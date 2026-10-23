# partition-limiter

**A Redis-backed distributed rate limiter whose partition mode has a
provable worst case — measured, not hoped.**

Every Redis rate limiter has to answer one question: *what happens when
the store becomes unreachable?* The industry's answers are fail-open
(admit everything — over-admission is unbounded), fail-closed (admit
nothing — the limiter becomes the outage), or an operator-chosen local
cap with no stated relationship to the global limit. None of them can
tell you the worst case.

partition-limiter can. When a node loses the store, it falls back to a
local GCRA bucket whose parameters are *derived* from the global ones —
rate `Rate/N`, burst `ceil(Burst/N)`, where N is the configured node
count — so the worst-case over-admission during a partition is a small
theorem (below), and the included chaos harness partitions a real
3-node Redis deployment and verifies the theorem empirically.

## Measured results (Redis 7.4.2, chaos harness, `results.json`)

Global limit: 500 tokens / 10 s + burst 50 → **quota 550 per 10 s
window**. Three limiter nodes, ~330 attempts/s aggregate (demand ≈ 6×
quota in every phase).

| Phase | Admitted | vs quota | Bound | Verdict |
|---|---:|---:|---:|---|
| Healthy | 549 | −0.18% | 553 | exact |
| Partial partition (1 of 3 nodes isolated) | 732 | +33.09% | 734 | within bound |
| Full partition, bounded fallback | **549** | **−0.18%** | 554 | **zero over-admission** |
| Full partition, 90/5/5 skewed load | 322 | −8.00% | 354 | cost of static division (see paper) |
| Healed window (after primary restart) | 549 | −0.18% | 553 | exactness restored |
| Full partition, fail-open (contrast) | 1,935 | **+452.86%** | ∞ | every attempt admitted |
| Full partition, fail-closed (contrast) | 0 | −100% | — | total outage |

Heal: the primary answered PING 185 ms after the restart command; the
first node was back to exact global enforcement 2.5 ms later (worst
case is one probe interval, 200 ms here, plus a round trip). All **871**
requests admitted locally during the partition were reconciled into the
store's stats keys on heal — the store's count and the harness's count
match exactly.

## The bound

Notation: global rate R per period P, burst B, node count N, window W,
global quota `Q(W) = B + R·W/P`. Local bucket: emission interval
`T_N = N·(P/R)`, burst `B_N = ceil(B/N)`.

- **Full partition** (no node reaches the store). Aggregate admissions
  over any window W:
  `A(W) ≤ Q(W) + (N·B_N − B) + N`.
  The sustained-rate over-admission is exactly **0** — N nodes at R/N
  sum to R. The only slack is burst rounding, `N·B_N − B ≤ N−1` tokens,
  plus ≤ 1 boundary token per node. With the measured configuration
  that is a worst case of 554 against a quota of 550 (+0.7%); the
  measured value was 549.
- **Partial partition** (k of N nodes isolated; the rest still share the
  store, which keeps enforcing the full global bucket):
  aggregate admission rate `≤ R·(1 + k/N)`. Measured at k=1, N=3:
  +33.09% against a +33.3% rate bound (734-token window bound).

Clock skew: the bound is per node-local window; with skew bounded by δ,
evaluate at `W + 2δ`. Skew shifts *when* a node's tokens land, never how
many it can admit per window.

## Architecture

```
                ┌─────────────────────────────┐
   requests ───▶│ limiter node 1 ─┐           │
   requests ───▶│ limiter node 2 ─┼─ EVAL ───▶│ Redis primary :7301
   requests ───▶│ limiter node 3 ─┘  (GCRA    │   replicas :7302/:7303
                │                  Lua,      │   (state replication)
                │                  server    │
                │                  clock)    │
                └─────────────────────────────┘
   store lost ─▶ local GCRA bucket per node: rate R/N, burst ceil(B/N)
   store back ─▶ global mode + INCRBY pl:stats:<key> reconciliation
```

- **Healthy mode** — one Lua script implements GCRA atomically in
  Redis, timestamped by the *server's* clock (`TIME`), so client clock
  skew cannot mint admissions. State is a single key per bucket (the
  TAT), with a TTL so idle buckets cost no memory.
- **Partition mode** — entered on the first failed store command,
  exited by lazy probing every `ProbeInterval`. Fail-open and
  fail-closed policies are also implemented, as measured baselines.
- **Reconciliation** — on heal, each node writes how many requests it
  admitted locally per key into `pl:stats:<key>`. A partition leaves an
  audit trail, not a mystery.
- **Zero dependencies** — the module is stdlib-only Go, including a
  small hand-written RESP client (`resp/`).

## Prior art, and what is actually new here

GCRA in Redis is redis-cell's and go-redis/redis_rate's territory;
Envoy's ratelimit service offers `failure_mode_deny` (fail-open or
fail-closed, nothing between); token leasing and "degraded local
limits" exist as design-blog patterns with heuristic caps and no
measurement. What none of them provide — and what this project claims,
no more and no less — is the packaged guarantee: sub-limits *derived*
from the global limit and node count, a worst-case formula covering
both full and partial partitions, and a chaos harness that partitions
a real 3-node Redis deployment and measures over/under-admission
against that formula. Full analysis in `KILLGATE.md`.

## Run it

```bash
# tests: unit suite runs offline; integration tests start a real
# redis-server themselves when REDIS_SERVER_BIN is set
REDIS_SERVER_BIN=/path/to/redis-server go test ./...

# chaos experiments (boots the 3-node deployment itself, ~70 s)
go run ./cmd/chaos --redis-bin /path/to/redis-server \
    --workdir ./chaos-run --out results.json

# or run the cluster standalone for poking
scripts/run-cluster.sh start
```

## Honest limits

- **Static division is the price of the bound.** Under load skew, a hot
  node is capped at its 1/N share and cannot borrow idle capacity:
  measured utilisation was 92% of quota at 90/5/5 skew, and the analytic
  worst case (all traffic on one node) is ~1/N of quota. Token leasing
  would fix utilisation but gives up the static bound. This design
  chooses the bound.
- **N must be true.** The bound assumes the configured node count
  equals the real fleet size. If a (N+1)-th node joins with the same
  config, the full-partition bound scales accordingly — the formula
  tells you by how much, but nothing here elects or counts nodes.
- **Single primary.** Healthy mode depends on one primary (replicas
  hold state but take no writes). A primary loss is exactly the
  partition this design degrades through — but if you need the store
  itself highly available, you still need failover tooling (Sentinel /
  Cluster) in front of it, and a failover mid-flight replays as a
  short partition here.
- **One host in the harness.** The three limiter nodes run as objects
  in one process on one VM (the three Redis nodes are separate
  processes). Multi-host clock skew and real network latency are
  covered analytically, not measured.
- **In-flight ambiguity.** A decision in flight at the kill instant can
  be lost by TCP semantics in either direction; the bound covers every
  decision the limiter actually returns.

## Layout

```
limiter/   the limiter (GCRA Lua, bounded fallback, reconciliation)
resp/      minimal stdlib RESP client
cmd/chaos/ the chaos harness (this is the experiment runner)
scripts/   standalone cluster start/stop
KILLGATE.md  prior-art analysis   PAPER.md  experiment write-up
results.json measured output of the canonical run
```
