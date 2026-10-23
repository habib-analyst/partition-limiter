// Package limiter implements a Redis-backed distributed rate limiter with
// a mathematically bounded partition mode.
//
// Healthy mode: every decision is a GCRA (Generic Cell Rate Algorithm)
// check executed atomically inside Redis by a Lua script, using the Redis
// server's own clock. All limiter nodes share one bucket per key, so the
// global limit is enforced exactly.
//
// Partition mode: when a node cannot reach the store, it does NOT fail
// open (unbounded over-admission) and does NOT fail closed (total
// outage). It falls back to a local GCRA bucket whose parameters are
// derived from the global ones:
//
//	local emission interval  T_N = N * T          (sustained rate = Rate/N)
//	local burst              B_N = ceil(Burst/N)
//
// where N is the configured number of limiter nodes. That derivation is
// what makes the worst case a theorem instead of a hope. Writing Q(W) =
// Burst + Rate*W/Period for the global quota over a window W:
//
//	FULL PARTITION (no node reaches the store):
//	  aggregate admissions over W  <=  Q(W) + (N*B_N - Burst) + N
//	  The sustained-rate over-admission is exactly 0; the burst
//	  over-admission is N*B_N - Burst <= N-1 tokens, plus a boundary
//	  term of at most one token per node from window alignment.
//
//	PARTIAL PARTITION (k of N nodes isolated, the rest on the store):
//	  aggregate admission rate  <=  Rate * (1 + k/N)
//	  because the store side still enforces the full global bucket
//	  while each isolated node adds at most Rate/N.
//
// Heal: when the store answers again, the node returns to exact global
// enforcement and reconciles by writing the number of requests it
// admitted locally during the partition into a stats key in the store,
// so the partition's cost stays auditable after the fact.
//
// Clock note: the bound is stated per node-local window. With node clock
// skew bounded by delta, evaluate the bound at W + 2*delta; skew shifts
// when tokens are available, never how many a node can admit per window.
package limiter

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"partitionlimiter/resp"
)

// Fallback selects what a node does while the store is unreachable.
type Fallback int

const (
	// FallbackBounded enforces the derived local sub-limit (the design).
	FallbackBounded Fallback = iota
	// FallbackOpen admits everything (measured as a contrast baseline).
	FallbackOpen
	// FallbackClosed admits nothing (measured as a contrast baseline).
	FallbackClosed
)

// Decision sources, reported on every Decision.
const (
	SourceGlobal    = "global"
	SourceLocal     = "local"
	SourceFailOpen  = "fail-open"
	SourceFailClose = "fail-closed"
)

// Config configures a Limiter node.
type Config struct {
	// StoreAddr is the Redis primary's address (host:port). All nodes
	// share the same primary in healthy mode.
	StoreAddr string
	// NodeID identifies this node in stats and logs.
	NodeID string
	// NodeCount N: total number of limiter nodes sharing the global
	// limit. The partition bound is derived from this number; it must
	// match the real fleet size for the bound to hold.
	NodeCount int
	// Rate is the global sustained limit: Rate tokens per Period.
	Rate int
	// Period is the window over which Rate is replenished.
	Period time.Duration
	// Burst is the global burst capacity (max instant admissions from
	// a rested bucket).
	Burst int
	// Fallback is the partition-mode policy. Default FallbackBounded.
	Fallback Fallback
	// OpTimeout bounds each store command. Default 300ms.
	OpTimeout time.Duration
	// ProbeInterval is how often a partitioned node retries the store.
	// Default 500ms.
	ProbeInterval time.Duration
}

// Decision is the outcome of one Allow call.
type Decision struct {
	Allowed    bool
	Source     string // one of the Source* constants
	RetryAfter time.Duration
	Remaining  int
}

// gcraScript is the healthy-mode decision. It runs atomically in Redis
// and takes time from the server, so gateway clock skew cannot mint
// extra admissions. ARGV: emission interval (us), burst, cost.
// Returns {allowed, retry_after_us, remaining}.
const gcraScript = `
local interval = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
if tat < now then tat = now end
local new_tat = tat + interval * cost
local allow_at = new_tat - burst * interval
if allow_at > now then
  return {0, allow_at - now, 0}
end
redis.call('SET', KEYS[1], new_tat, 'PX', math.ceil((new_tat - now) / 1000) + 1)
return {1, 0, math.floor((now - allow_at) / interval)}
`

// localBucket is an in-process GCRA bucket with the derived sub-limit.
type localBucket struct {
	interval time.Duration // T_N
	burst    int           // B_N
	tat      time.Time     // zero value = rested
}

func (b *localBucket) allow(now time.Time, cost int) (bool, time.Duration) {
	tat := b.tat
	if tat.Before(now) {
		tat = now
	}
	newTat := tat.Add(time.Duration(cost) * b.interval)
	allowAt := newTat.Add(-time.Duration(b.burst) * b.interval)
	if allowAt.After(now) {
		return false, allowAt.Sub(now)
	}
	b.tat = newTat
	return true, 0
}

// Limiter is one node of the distributed limiter.
type Limiter struct {
	cfg    Config
	client *resp.Client

	intervalUS int64 // global emission interval, microseconds (ceiled)
	localInt   time.Duration
	localBurst int

	mu            sync.Mutex
	global        bool
	lastProbe     time.Time
	buckets       map[string]*localBucket
	localAdmitted map[string]int64
	openAdmitted  map[string]int64
	heals         int64

	nowFunc func() time.Time // injectable for tests
}

// New builds a Limiter. It does not touch the store: the node starts in
// (optimistic) global mode and discovers a partition on first use.
func New(cfg Config) (*Limiter, error) {
	if cfg.NodeCount < 1 {
		return nil, fmt.Errorf("limiter: NodeCount must be >= 1")
	}
	if cfg.Rate < 1 || cfg.Period <= 0 {
		return nil, fmt.Errorf("limiter: Rate and Period must be positive")
	}
	if cfg.Burst < 1 {
		return nil, fmt.Errorf("limiter: Burst must be >= 1")
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = 300 * time.Millisecond
	}
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = 500 * time.Millisecond
	}
	// Emission interval in whole microseconds, ceiled: the enforced
	// rate is never above the configured rate.
	periodUS := cfg.Period.Microseconds()
	intervalUS := (periodUS + int64(cfg.Rate) - 1) / int64(cfg.Rate)
	// Derived local sub-limit: interval scales by N (rate divides by
	// N); burst divides by N, ceiled (the ceil is the <= N-1 token
	// slack in the full-partition bound).
	localBurst := (cfg.Burst + cfg.NodeCount - 1) / cfg.NodeCount
	return &Limiter{
		cfg:           cfg,
		client:        resp.Dial(cfg.StoreAddr, cfg.OpTimeout),
		intervalUS:    intervalUS,
		localInt:      time.Duration(intervalUS) * time.Microsecond * time.Duration(cfg.NodeCount),
		localBurst:    localBurst,
		global:        true,
		buckets:       make(map[string]*localBucket),
		localAdmitted: make(map[string]int64),
		openAdmitted:  make(map[string]int64),
		nowFunc:       time.Now,
	}, nil
}

// LocalInterval returns the derived local emission interval T_N.
func (l *Limiter) LocalInterval() time.Duration { return l.localInt }

// LocalBurst returns the derived local burst capacity B_N.
func (l *Limiter) LocalBurst() int { return l.localBurst }

// Mode reports "global" or "local" (partition mode).
func (l *Limiter) Mode() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.global {
		return "global"
	}
	return "local"
}

// Stats returns a snapshot: requests admitted from the local bucket per
// key since the last successful heal reconciliation, requests admitted
// by the fail-open policy per key, and the number of heals.
func (l *Limiter) Stats() (local map[string]int64, open map[string]int64, heals int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	local = make(map[string]int64, len(l.localAdmitted))
	for k, v := range l.localAdmitted {
		local[k] = v
	}
	open = make(map[string]int64, len(l.openAdmitted))
	for k, v := range l.openAdmitted {
		open[k] = v
	}
	return local, open, l.heals
}

// Close releases the store connection.
func (l *Limiter) Close() { l.client.Close() }

func storeKey(key string) string { return "pl:gcra:" + key }
func statsKey(key string) string { return "pl:stats:" + key }
func (l *Limiter) evalGCRA(key string, cost int) (Decision, error) {
	v, err := l.client.Do("EVAL", gcraScript, "1", storeKey(key),
		strconv.FormatInt(l.intervalUS, 10),
		strconv.Itoa(l.cfg.Burst),
		strconv.Itoa(cost))
	if err != nil {
		return Decision{}, err
	}
	if e, ok := v.(resp.Error); ok {
		return Decision{}, e
	}
	arr, ok := v.([]any)
	if !ok || len(arr) != 3 {
		return Decision{}, fmt.Errorf("limiter: unexpected EVAL reply %v", v)
	}
	allowed, _ := resp.Int64(arr[0])
	retryUS, _ := resp.Int64(arr[1])
	remaining, _ := resp.Int64(arr[2])
	return Decision{
		Allowed:    allowed == 1,
		Source:     SourceGlobal,
		RetryAfter: time.Duration(retryUS) * time.Microsecond,
		Remaining:  int(remaining),
	}, nil
}

// Allow decides whether one unit of the named bucket (tenant+resource)
// may proceed. Store failure is a mode, not an error: Allow always
// returns a decision.
func (l *Limiter) Allow(key string) Decision {
	return l.AllowN(key, 1)
}

// AllowN is Allow with a token cost.
func (l *Limiter) AllowN(key string, cost int) Decision {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.nowFunc()

	if l.global {
		if d, err := l.evalGCRA(key, cost); err == nil {
			return d
		}
		// Store lost (or errored): enter partition mode and serve
		// this request from the fallback policy.
		l.global = false
		l.lastProbe = now
		return l.serveLocalLocked(key, cost, now)
	}

	// Partition mode: probe lazily on the configured interval.
	if now.Sub(l.lastProbe) >= l.cfg.ProbeInterval {
		l.lastProbe = now
		if l.client.Ping() {
			l.global = true
			l.heals++
			l.reconcileLocked()
			if d, err := l.evalGCRA(key, cost); err == nil {
				return d
			}
			l.global = false // store answered PING but not EVAL: still partitioned for our purposes
		}
	}
	return l.serveLocalLocked(key, cost, now)
}

func (l *Limiter) serveLocalLocked(key string, cost int, now time.Time) Decision {
	switch l.cfg.Fallback {
	case FallbackOpen:
		l.openAdmitted[key]++
		return Decision{Allowed: true, Source: SourceFailOpen, Remaining: -1}
	case FallbackClosed:
		return Decision{Allowed: false, Source: SourceFailClose}
	default: // FallbackBounded
		b := l.buckets[key]
		if b == nil {
			b = &localBucket{interval: l.localInt, burst: l.localBurst}
			l.buckets[key] = b
		}
		ok, retry := b.allow(now, cost)
		if ok {
			l.localAdmitted[key] += int64(cost)
		}
		return Decision{Allowed: ok, Source: SourceLocal, RetryAfter: retry}
	}
}

// reconcileLocked reports locally-admitted counts to the store, best
// effort, and clears the counters that were successfully written. This
// is the audit trail: after a heal, pl:stats:<key> holds exactly how
// many requests the fleet admitted locally for that key while the store
// was down.
func (l *Limiter) reconcileLocked() {
	for key, n := range l.localAdmitted {
		if n == 0 {
			continue
		}
		if _, err := l.client.Do("INCRBY", statsKey(key), strconv.FormatInt(n, 10)); err == nil {
			l.localAdmitted[key] = 0
		}
	}
	if _, err := l.client.Do("INCR", "pl:heals"); err != nil {
		// best effort only
		_ = err
	}
}
