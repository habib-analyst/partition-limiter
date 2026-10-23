package limiter

import (
	"testing"
	"time"
)

// testConfig: Rate 500 per 10s (50/s), Burst 50, N=3 nodes.
// Global interval T = 20ms. Derived local bucket: T_N = 60ms
// (16.67/s), B_N = ceil(50/3) = 17.
func testConfig() Config {
	return Config{
		StoreAddr: "127.0.0.1:1", // nothing listening: forces partition mode
		NodeID:    "test",
		NodeCount: 3,
		Rate:      500,
		Period:    10 * time.Second,
		Burst:     50,
		Fallback:  FallbackBounded,
	}
}

func TestDerivedSubLimit(t *testing.T) {
	l, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got := l.LocalInterval(); got != 60*time.Millisecond {
		t.Fatalf("local interval = %v, want 60ms (N * 20ms)", got)
	}
	if got := l.LocalBurst(); got != 17 {
		t.Fatalf("local burst = %d, want ceil(50/3)=17", got)
	}
}

// The local bucket must admit exactly B_N requests instantly from a
// rested state, then deny — this is the burst term of the bound.
func TestLocalBurstExact(t *testing.T) {
	l, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	now := base
	l.nowFunc = func() time.Time { return now }

	admitted := 0
	for i := 0; i < 100; i++ {
		if l.Allow("k").Allowed {
			admitted++
		}
	}
	if admitted != 17 {
		t.Fatalf("instant local admissions = %d, want exactly B_N=17", admitted)
	}
}

// Sustained local rate must be Rate/N: over a simulated 10s window one
// node admits at most B_N + (Rate/N)*10s = 17 + 166 (interval-quantised)
// requests — never more than the derived bound.
func TestLocalSustainedBound(t *testing.T) {
	l, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	now := base
	l.nowFunc = func() time.Time { return now }

	admitted := 0
	// Hammer the bucket every simulated millisecond for 10s.
	for step := 0; step < 10_000; step++ {
		now = base.Add(time.Duration(step) * time.Millisecond)
		if l.Allow("k").Allowed {
			admitted++
		}
	}
	// B_N + floor((W + tau_N)/T_N) with tau_N = B_N*T_N: the theoretical
	// max is 17 + 10000/60 = 183 (plus at most 1 boundary token).
	if admitted < 180 || admitted > 184 {
		t.Fatalf("10s local admissions = %d, want ~183 (bound 184)", admitted)
	}
}

// Aggregate across N=3 fresh nodes over the same window must stay
// within the full-partition bound: Q(10s) + (N*B_N - Burst) + N =
// 550 + 1 + 3 = 554.
func TestFullPartitionAggregateBound(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	total := 0
	for n := 0; n < 3; n++ {
		l, err := New(testConfig())
		if err != nil {
			t.Fatal(err)
		}
		now := base
		l.nowFunc = func() time.Time { return now }
		for step := 0; step < 10_000; step++ {
			now = base.Add(time.Duration(step) * time.Millisecond)
			if l.Allow("k").Allowed {
				total++
			}
		}
	}
	if total > 554 {
		t.Fatalf("aggregate partition admissions = %d, exceeds bound 554", total)
	}
	if total < 540 {
		t.Fatalf("aggregate partition admissions = %d, suspiciously below expected ~549", total)
	}
}

// Fail-open and fail-closed policies (the contrast baselines).
func TestFallbackPolicies(t *testing.T) {
	cfg := testConfig()
	cfg.Fallback = FallbackOpen
	l, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		d := l.Allow("k")
		if !d.Allowed || d.Source != SourceFailOpen {
			t.Fatalf("fail-open denied or mislabelled: %+v", d)
		}
	}

	cfg.Fallback = FallbackClosed
	l2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		d := l2.Allow("k")
		if d.Allowed || d.Source != SourceFailClose {
			t.Fatalf("fail-closed admitted or mislabelled: %+v", d)
		}
	}
}

// Local bucket unit behaviour, including retry-after sanity.
func TestLocalBucketMath(t *testing.T) {
	b := &localBucket{interval: 100 * time.Millisecond, burst: 3}
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < 3; i++ {
		if ok, _ := b.allow(now, 1); !ok {
			t.Fatalf("request %d should be admitted (burst=3)", i+1)
		}
	}
	ok, retry := b.allow(now, 1)
	if ok {
		t.Fatal("4th instant request must be denied")
	}
	if retry <= 0 || retry > 100*time.Millisecond {
		t.Fatalf("retry-after = %v, want in (0, 100ms]", retry)
	}
	// After one interval, exactly one more token.
	if ok, _ := b.allow(now.Add(100*time.Millisecond), 1); !ok {
		t.Fatal("request after one interval should be admitted")
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := testConfig()
	cfg.NodeCount = 0
	if _, err := New(cfg); err == nil {
		t.Fatal("NodeCount=0 must be rejected")
	}
	cfg = testConfig()
	cfg.Burst = 0
	if _, err := New(cfg); err == nil {
		t.Fatal("Burst=0 must be rejected")
	}
}
