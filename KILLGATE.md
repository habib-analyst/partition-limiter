# KILL-GATE — partition-limiter

**Question:** does an available tool already implement a Redis-backed distributed
rate limiter with a *mathematically bounded partition mode* — automatic fallback
to a provable per-node sub-limit when the store is unreachable, a stated
worst-case over-admission formula, and a chaos harness that measures actual
over/under-admission against that bound on a real multi-node Redis setup?

## Prior art found (searched 2026-10-23)

| Tool / source | What it does | Partition behaviour |
|---|---|---|
| **redis-cell** (brandur) | Redis module; GCRA via `CL.THROTTLE`, one key per limit | None. If Redis is down, there is no limiter. No fallback, no bound. |
| **go-redis/redis_rate** | GCRA in Lua scripts on go-redis; `Allow`/`AllowN`/`AllowAtMost` | None. Store unreachable = error returned to caller; policy is the caller's problem. |
| **Envoy / Lyft ratelimit service** | gRPC service, Redis-backed counters | Binary config only: `failure_mode_deny` true (fail-closed = total outage) or false (fail-open = unbounded over-admission), or a percentage mix of the two. No bounded middle mode, no sub-limit derivation. |
| **Cloudflare's limiter** (blog-described) | Counter-based limiting at the edge | Per-data-centre counting with asynchronous propagation; over-admission during propagation is accepted, not bounded by a stated formula. Not an available tool. |
| **Token leasing** (multi-region system-design literature) | Central coordinator leases token budgets to regions; overage bounded by outstanding leases for one reconciliation period | A *design pattern* in interview/blog literature, not an available limiter. Coordinator-centric (needs its own consensus failover), aimed at cross-region latency, and the bound is "one lease period of overage", not a per-node sub-limit derived as limit/N with a mixed-partition formula. No chaos-measured validation found. |
| **"Degraded local limit" folk pattern** (design blogs, e.g. designgurus, sharpbyte) | "Fall back to local counters per instance… puts an upper cap on damage" | The cap is an operator-chosen heuristic. No derivation from the global limit and node count, no worst-case formula covering partial partitions, no measurement. |
| **"Overshoot, Retries and Clocks"** (empirical study, Zenodo DOI 10.5281/zenodo.23467402) | Measures how Redis limiters fail under *concurrency* (GET/INCR races, WATCH retries) | Measures races, not store partitions; proposes no bounded partition mode. |
| **ulule/limiter, uber-go/ratelimit, resilience4j** | Local or Redis-store limiters | Local ones have no global limit at all; store ones inherit the store's failure with no bounded fallback. |

## Verdict: PROCEED — with narrowed, honest claims

No available tool found that combines all four of:

1. **Automatic partition mode** — on store loss, each limiter node falls back to a
   local GCRA bucket capped at `global_rate / N` (rate) and `ceil(global_burst / N)`
   (burst), where N is the configured node count. No operator-chosen magic cap.
2. **A stated worst-case bound, in two cases** (proved from the GCRA math, not
   asserted):
   - *Full partition* (no node reaches the store): aggregate admissions over any
     window W ≤ `global_quota(W) + (N − 1)` tokens — over-admission ≤ N−1 tokens.
   - *Partial partition* (k of N nodes isolated, the rest still on the shared
     store): aggregate admission rate ≤ `global_rate × (1 + k/N)`.
   Fail-open's bound is ∞ and fail-closed's availability is 0; this design's
   bound is a small, explicit number.
3. **Heal reconciliation with measured time-to-exactness** — when the store
   returns, nodes resume exact global enforcement and report how many requests
   they admitted locally during the partition (a stats counter in the store),
   so the partition's cost is auditable after the fact.
4. **A chaos harness that partitions a real 3-node Redis deployment**
   (kill -9 on the primary; client-subset isolation) and measures admitted vs
   limit in healthy / partitioned / healed phases against the bound.

Explicitly **not** claimed: inventing GCRA (1990s ATM standard, via redis-cell),
inventing local fallback caps (folk practice), or inventing budget splitting
(multi-region designs do it statically). The novel position is the *packaged
guarantee*: an exact bound derived from the algorithm's own parameters, both
partition cases covered, proven in code comments and *measured* under induced
partitions on real Redis — a claim none of the tools above make or test.

**Kill condition not met** → build proceeds.
