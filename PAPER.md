# Bounded Partition Mode for Redis Rate Limiters: Design and Measured Validation

**System:** partition-limiter (Go, stdlib-only) · **Store:** Redis 7.4.2,
built from source · **Date of canonical run:** 2026-10-23 ·
**Raw data:** `results.json`

## 1. Summary

A distributed rate limiter that shares one Redis store enforces its
global limit exactly — until the store becomes unreachable. At that
moment every mainstream design picks one of two unbounded answers:
fail-open (over-admission without limit) or fail-closed (a self-inflicted
outage). This paper presents a third: an automatic partition mode whose
fallback parameters are *derived* from the global limit, giving a
worst-case over-admission bound that is proved from the algorithm's
parameters and then *measured* on a real 3-node Redis deployment under
induced partitions. Headline measurement: with the primary killed by
SIGKILL, the fleet admitted **549 requests against a healthy quota of
550 (−0.18%)** — the partition was, in aggregate admission terms,
free. With one of three nodes isolated instead, over-admission was
+33.09%, inside the predicted +33.3% bound. The fail-open baseline
admitted +452.86% on the same workload; fail-closed admitted 0.

## 2. Design

**Healthy mode.** Each decision executes a GCRA (Generic Cell Rate
Algorithm) check as a Lua script inside Redis, atomic and timestamped
by the server's clock. State per bucket is one integer (the theoretical
arrival time). Configuration for all experiments: rate R = 500 tokens
per P = 10 s, burst B = 50, so the rested-bucket quota over a 10 s
window is Q = B + R = 550.

**Partition mode.** A node that cannot complete a store command falls
back, for the affected decisions, to an in-process GCRA bucket with
emission interval T_N = N·(P/R) and burst B_N = ⌈B/N⌉, N = configured
node count (3). It probes the store every 200 ms (harness config) and
returns to global mode on the first successful probe, at which point it
writes its per-key local admission counts into `pl:stats:<key>` in the
store (reconciliation).

**The bound.** Let Q(W) = B + R·W/P.

- *Full partition.* Each node admits over W at most
  B_N + (R/N)·(W/P) + 1 (the +1 is window-alignment). Summing over N
  nodes: A(W) ≤ Q(W) + (N·B_N − B) + N. The sustained term is exact —
  N × R/N = R — so over-admission is confined to burst rounding
  (≤ N−1 tokens) plus the boundary term. Numerically: ≤ 554 vs quota
  550.
- *Partial partition, k isolated.* The connected side still enforces
  the full global bucket while each isolated node adds ≤ its local
  quota: admission rate ≤ R·(1 + k/N); for k=1, W=10 s: A ≤ 734.

## 3. Method

Topology: Redis primary on :7301 with two replicas (:7302, :7303,
`replicaof`), AOF persistence on, all on one 2-core VM. Three limiter
nodes (objects in the harness process) offer ≈120 attempts/s each
(≈360/s aggregate, ≈6× quota — every phase is supply-bound, never
demand-bound). Partitions were induced two ways: `kill -9` on the
primary process (store loss for all nodes), and client-subset
isolation (one node configured with a dead store address while its
peers keep the live primary). Each phase used a fresh bucket key
(rested-bucket semantics) and lasted 10 s unless noted. Fail-open and
fail-closed runs used the same harness with the fallback policy
switched — they are the Envoy `failure_mode_deny` = false/true
equivalents on identical workload.

## 4. Results

| Phase | Attempts | Admitted | Quota | Bound | Over-admission |
|---|---:|---:|---:|---:|---:|
| Healthy | 3,304 | 549 | 550 | 553 | −0.18% |
| Partial partition (k=1) | 3,168 | 732 (549 global + 183 local) | 550 | 734 | +33.09% |
| Full partition, bounded | 2,932 | 549 (183 × 3 local) | 550 | 554 | −0.18% |
| Full partition, skewed 90/5/5 (6 s) | 1,544 | 322 | 350 | 354 | −8.00% |
| Healed window | 3,119 | 549 | 550 | 553 | −0.18% |
| Full partition, fail-open | 1,935 | 1,935 | 350 | ∞ | +452.86% |
| Full partition, fail-closed (4 s) | 1,053 | 0 | 250 | — | −100% |

**Healthy exactness.** 549/550. The single missing token is a window
boundary effect (the final refill lands past the phase deadline); the
error is −0.18%, and concurrent atomicity is separately covered by an
integration test in which 32 nodes hammer one key and admissions stay
inside the GCRA envelope (a naive GET/INCR limiter over-admits by
~29% under the same conditions, per the "Overshoot, Retries and
Clocks" measurements).

**Full partition.** The three local buckets admitted exactly 183 each —
B_N + (R/N)·(W/P) = 17 + 166 — totalling 549, identical to the healthy
quota to within the same −1 boundary token. Measured over-admission
against the proven bound: none; against the quota: −0.18%.

**Partial partition.** The store side admitted 549 (the full global
bucket, shared by two nodes) and the isolated node admitted exactly
its local cap of 183, for 732 ≤ bound 734. The measured +33.09% sits
just under the analytic rate bound of +33.33% (k/N) — the mixed case
behaves exactly as the formula predicts, which is the case production
partitions actually look like.

**Skew cost (the honest price).** With 90% of attempts on one node
during a full partition, that node admitted 116 (≈ its 6 s cap of
117) and could not borrow the fleet's idle share; the light nodes
admitted everything they were offered (103 each). Fleet utilisation
was 322/350 = 92% of quota. The analytic worst case — all traffic on
one node — is utilisation ≈ 1/N of quota. Static division buys the
bound with worst-case utilisation; that is the trade, stated plainly.

**Heal.** Restarting the primary from the same data directory (AOF
state intact): PING answered 185 ms after the restart command; the
first probing node resumed global decisions 2.5 ms later (a probe was
due; the design's worst case is one probe interval, 200 ms, plus a
round trip). The healed 10 s window admitted 549, all global —
exactness fully restored, no residual local-mode leakage. The store's
reconciled stats total for the two bounded phases was **871** locally
admitted requests; the harness's independent count was **871**. Exact
agreement: every locally admitted request is accounted for after the
fact.

**Contrasts.** Fail-open admitted 100% of 1,935 attempts (+452.86%
over the 6 s quota) — protection simply absent for the duration.
Fail-closed admitted 0 of 1,053 — protection perfect, service gone.
Bounded mode is the only one of the three whose partition behaviour is
a number you can write in an SLO.

**Reproducibility.** The harness was run twice end-to-end. All
admission counts in bounded and healthy phases were identical across
runs (549 / 732 / 549 / 549); the skewed phase differed by 8 requests
(322 vs 330, pacing jitter on the light nodes), and fail-open differed
only in attempt volume. `results.json` holds the canonical second run.

## 5. Threats to validity

- Limiter nodes share one host clock in the harness; multi-host skew
  is handled analytically (bound at W + 2δ) but not measured.
- One VM, loopback networking: store RTTs are far below production
  cross-AZ values. Detection latency scales with the configured op
  timeout (300 ms), but every returned decision remains inside the
  bound regardless of when detection fires, because fallback
  admissions are capped from the first failure.
- AOF `everysec` can lose ≤ 1 s of bucket state in a crash; post-heal
  that can restore at most ~1 s of TAT error, i.e. a fraction of one
  burst, far inside the stated bounds.
- The bound depends on N being the true fleet size (§README, honest
  limits). Nothing here counts nodes dynamically.

## 6. Conclusion

Fail-open and fail-closed are not the only options, and "degraded
local limits" do not have to be folklore. Deriving the fallback from
the global limit turns partition behaviour into arithmetic: the
sustained over-admission is zero by construction, the burst slack is
≤ N−1 tokens, the partial-partition rate is ≤ R(1 + k/N) — and on real
Redis, under SIGKILL partitions, the measurements landed inside those
lines in every phase.
