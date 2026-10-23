// Command chaos is the partition-limiter chaos harness.
//
// It boots a real 3-node Redis deployment (one primary on :7301, two
// replicas on :7302/:7303), runs three limiter nodes against it, and
// drives the system through healthy, partitioned and healed phases,
// measuring admitted vs the global limit in each phase and checking the
// measurements against the design's proven bounds.
//
// Partitions are induced two ways, both documented in results.json:
//   - kill -9 on the primary process (store loss for every node);
//   - client-subset isolation: one limiter node is pointed at a dead
//     address while its peers keep using the live primary (partial
//     partition, the mixed case of the bound).
//
// Usage:
//
//	go run ./cmd/chaos --redis-bin ~/workspace/tools/redis/bin/redis-server \
//	    --workdir ./chaos-run --out results.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"partitionlimiter/limiter"
	"partitionlimiter/resp"
)

// ---- experiment parameters (the headline configuration) ----
const (
	nodeCount = 3
	rate      = 500              // tokens per period, global
	period    = 10 * time.Second // replenishment period
	burst     = 50               // global burst
	// Global quota over a full period window: burst + rate = 550.
	quotaPeriod = burst + rate
)

// windowQuota returns the max admissions the global bucket allows over
// a window of length w starting rested.
func windowQuota(w time.Duration) int {
	return burst + int(float64(rate)*(float64(w)/float64(period)))
}

// fullPartitionBound returns the proven worst case for aggregate
// admissions over window w when no node can reach the store:
// N*B_N + Rate*w/Period + N (boundary term), B_N = ceil(Burst/N).
func fullPartitionBound(w time.Duration) int {
	bn := (burst + nodeCount - 1) / nodeCount
	return nodeCount*bn + int(float64(rate)*(float64(w)/float64(period))) + nodeCount
}

// partialPartitionBound: store side enforces the global bucket, each of
// the k isolated nodes adds at most its local quota.
func partialPartitionBound(w time.Duration, k int) int {
	bn := (burst + nodeCount - 1) / nodeCount
	local := bn + int(float64(rate)*(float64(w)/float64(period))/nodeCount) + 1
	return windowQuota(w) + k*local
}

// ---- redis process management ----

type redisProc struct {
	port int
	dir  string
	cmd  *exec.Cmd
}

func startRedis(bin, base string, port int, replicaOf int) (*redisProc, error) {
	// Redis chdir()s into --dir before opening --logfile, so every path
	// handed to it must be absolute.
	absBase, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	base = absBase
	dir := filepath.Join(base, fmt.Sprintf("n%d", port))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	args := []string{
		"--port", fmt.Sprint(port),
		"--bind", "127.0.0.1",
		"--dir", dir,
		"--daemonize", "no",
		"--save", "",
		"--appendonly", "yes",
		"--protected-mode", "no",
		"--logfile", filepath.Join(dir, "redis.log"),
	}
	if replicaOf > 0 {
		args = append(args, "--replicaof", "127.0.0.1", fmt.Sprint(replicaOf))
	}
	cmd := exec.Command(bin, args...)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	rp := &redisProc{port: port, dir: dir, cmd: cmd}
	c := resp.Dial(rp.addr(), 200*time.Millisecond)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if c.Ping() {
			return rp, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	rp.kill9()
	return nil, fmt.Errorf("redis on %d did not come up", port)
}

func (r *redisProc) addr() string { return fmt.Sprintf("127.0.0.1:%d", r.port) }

func (r *redisProc) kill9() {
	if r.cmd.Process != nil {
		r.cmd.Process.Kill()
		r.cmd.Wait()
	}
}

func (r *redisProc) stop() {
	c := resp.Dial(r.addr(), 200*time.Millisecond)
	_, _ = c.Do("SHUTDOWN", "NOSAVE")
	done := make(chan struct{})
	go func() { r.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		r.kill9()
	}
}

// ---- load generation ----

type counters struct {
	attempts       atomic.Int64
	admitted       atomic.Int64
	admittedGlobal atomic.Int64
	admittedLocal  atomic.Int64
	admittedOther  atomic.Int64 // fail-open admissions
	rejected       atomic.Int64
	perNode        [nodeCount]atomic.Int64
}

// runLoad drives attempts against nodes for dur. rates[i] is node i's
// attempt rate per second; attempts are paced per node.
func runLoad(nodes []*limiter.Limiter, key string, dur time.Duration, rates []float64) *counters {
	c := &counters{}
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)
	for i, node := range nodes {
		wg.Add(1)
		go func(node *limiter.Limiter, rps float64) {
			defer wg.Done()
			nodeIdx := -1
			for j, n := range nodes {
				if n == node {
					nodeIdx = j
				}
			}
			interval := time.Duration(float64(time.Second) / rps)
			next := time.Now()
			for {
				now := time.Now()
				if now.After(deadline) {
					return
				}
				if now.Before(next) {
					time.Sleep(next.Sub(now))
					continue
				}
				next = now.Add(interval)
				c.attempts.Add(1)
				d := node.Allow(key)
				if d.Allowed {
					c.admitted.Add(1)
					if nodeIdx >= 0 {
						c.perNode[nodeIdx].Add(1)
					}
					switch d.Source {
					case limiter.SourceGlobal:
						c.admittedGlobal.Add(1)
					case limiter.SourceLocal:
						c.admittedLocal.Add(1)
					default:
						c.admittedOther.Add(1)
					}
				} else {
					c.rejected.Add(1)
				}
			}
		}(node, rates[i])
	}
	wg.Wait()
	return c
}

// ---- results ----

type phaseResult struct {
	Name             string  `json:"name"`
	HowInduced       string  `json:"how_induced,omitempty"`
	DurationS        float64 `json:"duration_s"`
	Attempts         int64   `json:"attempts"`
	Admitted         int64   `json:"admitted"`
	AdmittedGlobal   int64   `json:"admitted_global"`
	AdmittedLocal    int64   `json:"admitted_local"`
	AdmittedOther    int64   `json:"admitted_failopen"`
	Rejected         int64   `json:"rejected"`
	AdmittedPerNode  []int64 `json:"admitted_per_node"`
	ExpectedQuota    int     `json:"expected_quota"`
	Bound            int     `json:"bound"`
	OverAdmissionPct float64 `json:"over_admission_pct_vs_quota"`
	WithinBound      bool    `json:"within_bound"`
}

type healResult struct {
	StoreReadyMS        float64 `json:"store_ready_ms"`
	TimeToExactnessMS   float64 `json:"time_to_exactness_ms"`
	StatsReported       int64   `json:"stats_reported_local_admissions"`
	StatsCounted        int64   `json:"harness_counted_local_admissions"`
	ReconciliationMatch bool    `json:"reconciliation_match"`
}

type runReport struct {
	StartedAt    string         `json:"started_at"`
	RedisVersion string         `json:"redis_version"`
	Config       map[string]any `json:"config"`
	Phases       []phaseResult  `json:"phases"`
	Heal         healResult     `json:"heal"`
	Notes        []string       `json:"notes"`
}

func pct(admitted int64, quota int) float64 {
	if quota == 0 {
		return 0
	}
	return math.Round((float64(admitted)-float64(quota))/float64(quota)*10000) / 100
}

func makeNodes(addr string, fb limiter.Fallback, probe time.Duration) []*limiter.Limiter {
	nodes := make([]*limiter.Limiter, nodeCount)
	for i := 0; i < nodeCount; i++ {
		l, err := limiter.New(limiter.Config{
			StoreAddr: addr, NodeID: fmt.Sprintf("node-%d", i+1),
			NodeCount: nodeCount, Rate: rate, Period: period, Burst: burst,
			Fallback: fb, OpTimeout: 300 * time.Millisecond, ProbeInterval: probe,
		})
		if err != nil {
			panic(err)
		}
		nodes[i] = l
	}
	return nodes
}

func closeNodes(nodes []*limiter.Limiter) {
	for _, n := range nodes {
		n.Close()
	}
}

func main() {
	redisBin := flag.String("redis-bin", "", "path to redis-server binary (required)")
	workdir := flag.String("workdir", "chaos-run", "directory for redis data + logs")
	out := flag.String("out", "results.json", "where to write results JSON")
	flag.Parse()
	if *redisBin == "" {
		fmt.Fprintln(os.Stderr, "chaos: --redis-bin is required")
		os.Exit(2)
	}
	if err := os.MkdirAll(*workdir, 0o755); err != nil {
		panic(err)
	}

	verOut, _ := exec.Command(*redisBin, "--version").Output()

	report := &runReport{
		StartedAt:    time.Now().Format(time.RFC3339),
		RedisVersion: string(bytesTrim(verOut)),
		Config: map[string]any{
			"nodes": nodeCount, "rate_per_period": rate,
			"period_s": period.Seconds(), "burst": burst,
			"attempt_rate_per_node_rps": 120,
			"topology":                  "primary :7301, replicas :7302/:7303 (replicaof 7301); all limiter nodes share the primary in healthy mode",
		},
		Notes: []string{
			"The three limiter nodes are objects in this one harness process (one host clock); multi-host clock skew is out of scope — the bound covers skew analytically at W + 2*delta.",
			"Each phase uses a fresh bucket key, so phases measure rested-bucket behaviour and do not contaminate each other.",
			"In-flight decisions at the instant of kill -9 can be lost either way by TCP semantics; the harness counts a decision only when the limiter returns one (local fallback included), so the kill instant contributes at most a few hundred ms of local-mode decisions, which are inside the bound by construction.",
		},
	}

	even := []float64{120, 120, 120}
	phaseDur := 10 * time.Second

	// ---------- cluster up ----------
	primary, err := startRedis(*redisBin, *workdir, 7301, 0)
	if err != nil {
		panic(err)
	}
	rep1, err := startRedis(*redisBin, *workdir, 7302, 7301)
	if err != nil {
		panic(err)
	}
	rep2, err := startRedis(*redisBin, *workdir, 7303, 7301)
	if err != nil {
		panic(err)
	}
	defer rep1.stop()
	defer rep2.stop()
	// Wait for replicas to link.
	{
		c := resp.Dial(rep1.addr(), time.Second)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			v, _ := c.Do("INFO", "replication")
			if s := resp.String(v); contains(s, "master_link_status:up") {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	time.Sleep(300 * time.Millisecond)

	nodesA := makeNodes(primary.addr(), limiter.FallbackBounded, 200*time.Millisecond)
	defer closeNodes(nodesA)

	record := func(name, how string, dur time.Duration, c *counters, quota, bound int) {
		perNode := make([]int64, nodeCount)
		for i := 0; i < nodeCount; i++ {
			perNode[i] = c.perNode[i].Load()
		}
		pr := phaseResult{
			Name: name, HowInduced: how, DurationS: dur.Seconds(),
			Attempts: c.attempts.Load(), Admitted: c.admitted.Load(),
			AdmittedGlobal: c.admittedGlobal.Load(), AdmittedLocal: c.admittedLocal.Load(),
			AdmittedOther: c.admittedOther.Load(), Rejected: c.rejected.Load(),
			AdmittedPerNode: perNode,
			ExpectedQuota:   quota, Bound: bound,
			OverAdmissionPct: pct(c.admitted.Load(), quota),
			WithinBound:      bound >= 0 && c.admitted.Load() <= int64(bound),
		}
		report.Phases = append(report.Phases, pr)
		fmt.Printf("[phase] %-28s admitted=%-5d quota=%-4d bound=%-4d over=%+.2f%% within=%v\n",
			name, pr.Admitted, quota, bound, pr.OverAdmissionPct, pr.WithinBound)
	}

	// ---------- phase 1: healthy ----------
	c1 := runLoad(nodesA, "exp:healthy", phaseDur, even)
	record("healthy", "no fault; all nodes on the primary", phaseDur, c1, windowQuota(phaseDur), windowQuota(phaseDur)+nodeCount)

	// ---------- phase 2: partial partition (k=1 isolated) ----------
	deadNode, err := limiter.New(limiter.Config{
		StoreAddr: "127.0.0.1:7399", NodeID: "node-3-isolated",
		NodeCount: nodeCount, Rate: rate, Period: period, Burst: burst,
		Fallback: limiter.FallbackBounded, OpTimeout: 200 * time.Millisecond,
		ProbeInterval: 200 * time.Millisecond,
	})
	if err != nil {
		panic(err)
	}
	partialNodes := []*limiter.Limiter{nodesA[0], nodesA[1], deadNode}
	c2 := runLoad(partialNodes, "exp:partial", phaseDur, even)
	record("partial_partition_k1", "client-subset isolation: node 3 pointed at dead address 127.0.0.1:7399; nodes 1-2 keep the live primary", phaseDur, c2, windowQuota(phaseDur), partialPartitionBound(phaseDur, 1))
	deadNode.Close()

	// ---------- kill the primary ----------
	primary.kill9()
	time.Sleep(300 * time.Millisecond) // let nodes observe the failure

	// ---------- phase 3: full partition, bounded fallback ----------
	c3 := runLoad(nodesA, "exp:full-bounded", phaseDur, even)
	record("full_partition_bounded", "kill -9 on the primary (7301); replicas hold no write path", phaseDur, c3, windowQuota(phaseDur), fullPartitionBound(phaseDur))

	// ---------- phase 4: full partition, skewed load (under-admission cost) ----------
	skewDur := 6 * time.Second
	c4 := runLoad(nodesA, "exp:full-skewed", skewDur, []float64{324, 18, 18})
	record("full_partition_skewed", "primary still down; 90% of attempts land on node 1, whose local cap binds", skewDur, c4, windowQuota(skewDur), fullPartitionBound(skewDur))

	countedLocal := c3.admittedLocal.Load() + c4.admittedLocal.Load()

	// ---------- heal ----------
	healStart := time.Now()
	primary2, err := startRedis(*redisBin, *workdir, 7301, 0)
	if err != nil {
		panic(err)
	}
	primary = primary2
	storeReady := time.Since(healStart)
	// Poll node 1 until its decisions come from the global bucket again.
	var tte time.Duration
	{
		probeStart := time.Now()
		for {
			d := nodesA[0].Allow("exp:probe")
			if d.Source == limiter.SourceGlobal {
				tte = time.Since(probeStart)
				break
			}
			if time.Since(probeStart) > 15*time.Second {
				tte = -1
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	fmt.Printf("[heal] store ready in %v; node back to exact global enforcement after %v\n", storeReady, tte)

	// ---------- phase 5: healed window ----------
	c5 := runLoad(nodesA, "exp:healed", phaseDur, even)
	record("healed", "primary restarted from the same data dir (AOF state survives)", phaseDur, c5, windowQuota(phaseDur), windowQuota(phaseDur)+nodeCount)

	// ---------- reconciliation check ----------
	{
		cc := resp.Dial(primary.addr(), 2*time.Second)
		get := func(k string) int64 {
			v, err := cc.Do("GET", k)
			if err != nil {
				return -1
			}
			n, _ := resp.Int64(v)
			return n
		}
		reported := get("pl:stats:exp:full-bounded") + get("pl:stats:exp:full-skewed")
		report.Heal = healResult{
			StoreReadyMS:        math.Round(float64(storeReady.Microseconds())/1000*100) / 100,
			TimeToExactnessMS:   math.Round(float64(tte.Microseconds())/1000*100) / 100,
			StatsReported:       reported,
			StatsCounted:        countedLocal,
			ReconciliationMatch: reported == countedLocal,
		}
		fmt.Printf("[heal] reconciled local admissions: store=%d harness=%d match=%v\n",
			reported, countedLocal, reported == countedLocal)
	}

	// ---------- contrast phases: fail-open / fail-closed ----------
	primary.kill9()
	time.Sleep(300 * time.Millisecond)

	openNodes := makeNodes(primary.addr(), limiter.FallbackOpen, 200*time.Millisecond)
	openDur := 6 * time.Second
	c6 := runLoad(openNodes, "exp:open", openDur, even)
	record("full_partition_failopen", "primary killed; fallback policy = fail-open (Envoy failure_mode_deny=false equivalent)", openDur, c6, windowQuota(openDur), -1)
	closeNodes(openNodes)

	closedNodes := makeNodes(primary.addr(), limiter.FallbackClosed, 200*time.Millisecond)
	closedDur := 4 * time.Second
	c7 := runLoad(closedNodes, "exp:closed", closedDur, even)
	record("full_partition_failclosed", "primary still down; fallback policy = fail-closed (Envoy failure_mode_deny=true equivalent)", closedDur, c7, windowQuota(closedDur), windowQuota(closedDur))
	closeNodes(closedNodes)

	// Leave a healthy cluster behind for inspection.
	primary3, err := startRedis(*redisBin, *workdir, 7301, 0)
	if err == nil {
		primary = primary3
		// replicas resync on their own; stop everything cleanly.
		primary.stop()
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("results written to %s\n", *out)

	// Exit non-zero if any bounded phase broke its bound, so CI / the
	// orchestrator sees a broken guarantee as a failed run.
	for _, p := range report.Phases {
		if p.Bound >= 0 && !p.WithinBound {
			fmt.Fprintf(os.Stderr, "BOUND VIOLATED in phase %s: admitted %d > bound %d\n", p.Name, p.Admitted, p.Bound)
			os.Exit(1)
		}
	}
}

func bytesTrim(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r' || b[len(b)-1] == ' ') {
		b = b[:len(b)-1]
	}
	return b
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
