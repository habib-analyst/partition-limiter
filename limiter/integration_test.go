package limiter

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"partitionlimiter/resp"
)

// These tests run against a real redis-server started by the test.
// Set REDIS_SERVER_BIN to the redis-server binary; without it they skip
// (the unit suite still runs fully offline).

func redisBin(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("REDIS_SERVER_BIN")
	if bin == "" {
		t.Skip("REDIS_SERVER_BIN not set; skipping integration test")
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("REDIS_SERVER_BIN %q not usable: %v", bin, err)
	}
	return bin
}

type testRedis struct {
	cmd  *exec.Cmd
	addr string
	dir  string
	port int
}

func startRedis(t *testing.T, bin string, port int, dir string) *testRedis {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin,
		"--port", fmt.Sprint(port),
		"--bind", "127.0.0.1",
		"--dir", dir,
		"--daemonize", "no",
		"--save", "",
		"--appendonly", "yes",
		"--protected-mode", "no",
		"--logfile", "",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	tr := &testRedis{cmd: cmd, addr: fmt.Sprintf("127.0.0.1:%d", port), dir: dir, port: port}
	c := resp.Dial(tr.addr, 200*time.Millisecond)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.Ping() {
			return tr
		}
		time.Sleep(50 * time.Millisecond)
	}
	cmd.Process.Kill()
	t.Fatalf("redis on %d did not come up", port)
	return nil
}

func (tr *testRedis) kill9() {
	tr.cmd.Process.Kill()
	tr.cmd.Wait()
}

func (tr *testRedis) stop() {
	c := resp.Dial(tr.addr, 200*time.Millisecond)
	_, _ = c.Do("SHUTDOWN", "NOSAVE")
	done := make(chan struct{})
	go func() { tr.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		tr.cmd.Process.Kill()
		tr.cmd.Wait()
	}
}

// Atomicity under concurrency: 32 limiter nodes share one store and one
// key. With Burst=50 and a refill of 1 token/s, concurrent hammering must
// admit at least the burst (no lost updates) and never more than
// burst + elapsed refill + 1 (no race over-admission — the failure mode
// of naive GET/INCR limiters).
func TestGlobalAtomicityConcurrent(t *testing.T) {
	bin := redisBin(t)
	tr := startRedis(t, bin, 7398, t.TempDir())
	defer tr.stop()

	cfg := Config{
		StoreAddr: tr.addr, NodeCount: 3,
		Rate: 3600, Period: time.Hour, Burst: 50,
		Fallback: FallbackBounded, OpTimeout: 2 * time.Second,
	}
	start := time.Now()
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for n := 0; n < 32; n++ {
		l, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				d := l.Allow("hot-key")
				if d.Source != SourceGlobal {
					t.Errorf("decision source = %q, want global (store is up)", d.Source)
					return
				}
				if d.Allowed {
					admitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	maxAllowed := int64(50) + int64(elapsed.Seconds()) + 1
	if got := admitted.Load(); got < 50 || got > maxAllowed {
		t.Fatalf("admitted = %d, want in [50, %d] (elapsed %v)", got, maxAllowed, elapsed)
	}
}

// Full lifecycle against a real store: global mode, kill -9 the store,
// bounded local fallback engages, store restarts (AOF state survives),
// node heals back to global mode, and the locally-admitted count is
// reconciled into pl:stats:<key> exactly.
func TestPartitionFallbackHealReconcile(t *testing.T) {
	bin := redisBin(t)
	dir := t.TempDir()
	tr := startRedis(t, bin, 7397, dir)

	cfg := Config{
		StoreAddr: tr.addr, NodeID: "t1", NodeCount: 3,
		Rate: 500, Period: 10 * time.Second, Burst: 50,
		Fallback: FallbackBounded, OpTimeout: 300 * time.Millisecond,
		ProbeInterval: 100 * time.Millisecond,
	}
	l, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if d := l.Allow("life"); d.Source != SourceGlobal {
		t.Fatalf("pre-kill decision source = %q, want global", d.Source)
	}

	tr.kill9()
	time.Sleep(100 * time.Millisecond)

	// Partition mode: decisions must come from the local bucket, and
	// its instant burst is exactly B_N = 17.
	localAdmitted := 0
	for i := 0; i < 40; i++ {
		d := l.Allow("life")
		if d.Source != SourceLocal {
			t.Fatalf("partition decision source = %q, want local", d.Source)
		}
		if d.Allowed {
			localAdmitted++
		}
	}
	if localAdmitted != 17 {
		t.Fatalf("local burst admissions = %d, want 17", localAdmitted)
	}

	// Heal: restart the same instance (same data dir).
	tr2 := startRedis(t, bin, 7397, dir)
	defer tr2.stop()

	deadline := time.Now().Add(10 * time.Second)
	healed := false
	for time.Now().Before(deadline) {
		if d := l.Allow("life"); d.Source == SourceGlobal {
			healed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !healed {
		t.Fatal("limiter did not return to global mode after store restart")
	}

	// Reconciliation: stats key must equal the local admissions count.
	c := resp.Dial(tr.addr, time.Second)
	v, err := c.Do("GET", "pl:stats:life")
	if err != nil {
		t.Fatalf("GET stats: %v", err)
	}
	got, ok := resp.Int64(v)
	if !ok || got != int64(localAdmitted) {
		t.Fatalf("pl:stats:life = %v, want %d", v, localAdmitted)
	}
}
