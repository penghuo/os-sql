# Review of `finding_claude.md` (Alternate Angles)

Date: 2026-02-21
Reviewed doc: `docs/research/finding_claude.md`

## What This Review Is (and Is Not)
- This is a critique of the proposed native Trino-in-OpenSearch design from additional angles: OpenSearch runtime realities, Trino semantic parity, operability/perf, and packaging/maintenance.
- It is not a competing full design; it’s a list of gaps, risks, and concrete doc additions that would reduce execution risk.

## Executive Summary
`finding_claude.md` is a strong, coherent framing of “native integration” and correctly identifies the two hard blockers: (1) MPP vs shard-based architecture mismatch and (2) shuffle/exchange.

The main gaps are not in the high-level diagrams; they’re in the “last mile” engineering contracts:
- Exact semantic compatibility scope (what is supported vs rejected, and what differs from Trino).
- Concrete exchange protocol limits, failure modes, and backpressure/cancellation propagation.
- Concrete memory accounting integration with OpenSearch circuit breakers and parent breaker.
- Production operability requirements (metrics/SLOs) before distributed joins are attempted.
- Maintenance automation for shading + licensing + upgrade cadence.

## Strengths
- Clear definition of native integration and why connector-based approaches are insufficient.
- Correctly anchors security posture on “search API always” to preserve FLS/DLS.
- Phased plan is realistic: start with shard-local scan/filter/project, then partial aggregation, then joins.
- Identifies classpath/dependency conflicts early and proposes shading as the pragmatic baseline.

## Key Review Findings (By Angle)

### A. OpenSearch Runtime and Cluster-Health Angle
1. “Search API always” is necessary for FLS/DLS, but it is not automatically safe for cluster health.
- The doc proposes dedicated `trino-sql-worker` and `trino-sql-exchange` pools.
- Missing: explicit pool sizing guidance, queue bounds, rejection policies, and how these pools interact with search/index/bulk/management threads.

2. TransportService is request/response oriented; exchange needs a real streaming contract.
- The doc notes transport messages are fully materialized before sending.
- Missing: a concrete message framing/chunking protocol, bounded memory on both sender and receiver, timeouts/retries, and a clear rule for when to fail-fast vs spill.

3. Cancellation and plugin lifecycle need to be specified in OpenSearch terms.
- The doc mentions cancellation propagation.
- Missing: mapping to OpenSearch task management (`CancellableTask`), how cancellation interrupts shard-local operators, and cleanup of in-flight exchange buffers on cancellation and plugin disable.

4. Failure modes are acknowledged but not enumerated as “known-bad” scenarios.
- Examples that should be in the doc:
- Exchange buffer exhaustion during broadcast join.
- Analytics nodes (new role) becoming hotspots and saturating transport.
- Node restart mid-shuffle leaving downstream blocked.
- Spill directory full / slow disk turning into cluster-wide latency.

Concrete doc addition:
- Add a “Failure Modes and Recovery” section with table: symptom, root cause, signal/metric, automated mitigation, operator action.

### B. Trino Semantics/Correctness Angle
1. The doc implicitly promises Trino semantics by embedding parser/analyzer/optimizer, but it doesn’t define the support boundary.
- Trino includes advanced constructs (MATCH_RECOGNIZE, lambdas, table functions, JSON path, etc.).
- Missing: a compatibility matrix listing what is supported in Phase 1/2/3 and what returns an explicit error.

2. PIT-per-table is a correctness contract and must be treated as such.
- The doc accepts lack of cross-table snapshot isolation.
- Missing: explicit statement of isolation guarantees for single-table vs multi-table queries and how this differs from standalone Trino; include tests that pin this behavior.

3. Type mapping needs to be a strict contract, not a best-effort table.
- The mapping table includes `text -> VARCHAR`, `flattened -> MAP(VARCHAR,VARCHAR)`, `geo_point -> ROW(lat,lon)`.
- Missing: precise semantics for each mapping (sorting/collation, analyzers for text, multi-fields), and how conflicts across index patterns resolve.

4. NULL/timezone/decimal edge cases must be “definition first”, not “tests later”.
- The doc mentions golden results.
- Missing: explicit semantic definitions and a minimal conformance suite that must pass before enabling the endpoint by default.

Concrete doc addition:
- Add a “Semantic Compatibility Contract” section:
- Trino version pinned.
- Supported SQL grammar subset per phase.
- Divergences (PIT isolation, text analysis semantics, approximate aggregations).
- Error model (explicit errors vs silent behavior).

### C. Performance and Operability Angle
1. The 10-1000x claim is plausible for some aggregations, but it needs boundaries.
- It depends on translating GROUP BY into OpenSearch aggregations or at least reducing raw row materialization.
- If Phase 2 aggregates are done in Java operators over SearchHits, wins may be far smaller.

2. Cost model and stats strategy is under-specified.
- `_stats` + `sampler` caching is a start.
- Missing: how stats quality is validated, how stale stats are detected, and how “bad plans” are mitigated (timeouts, adaptive re-planning, conservative heuristics).

3. Shuffle amplification and skew need first-class treatment.
- Missing: skew detection (per-split/partition histograms), adaptive partitioning, and a safe default posture (avoid repartition unless necessary).

4. Observability is currently “Phase 5”; it needs to start in Phase 1.
- Without early metrics, debugging correctness/perf regressions will be slow and expensive.

Concrete doc addition:
- Add an “Observability Minimum Bar” section required for each phase:
- Query-level: planned vs actual rows/bytes, wall time breakdown.
- Exchange-level: bytes per exchange, buffer occupancy, backpressure time.
- Memory/spill: breaker usage, spilled bytes/files, spill latency.
- Cluster impact: thread pool utilization, transport saturation indicators.

### D. Packaging, Upgrades, and Compliance Angle
1. Shading is pragmatic but must be automated and validated.
- Missing: build checks for “no unshaded deps leaked”, runtime linkage smoke tests, and a reproducible relocation map.

2. Upgrade cadence mismatch (Trino biweekly vs OpenSearch quarterly) needs an explicit policy.
- The doc suggests quarterly pinning.
- Missing: security patch intake policy (when to cherry-pick) and a compatibility gate for each upgrade.

3. Licensing/NOTICE is not addressed.
- Shading bundles many transitive artifacts.
- Missing: dependency inventory, license harvesting, and NOTICE updates in the release process.

Concrete doc addition:
- Add a “Release Checklist” section:
- Update pinned Trino SHA/version.
- Regenerate shaded jar.
- Run linkage smoke tests + semantic regression suite.
- Emit third-party license report and update NOTICE.

## High-Value Questions to Answer Before Implementation
- What is the minimal supported SQL subset in Phase 1 and Phase 2, and what is the error contract for unsupported syntax?
- In Phase 2 aggregations, is the intent to push down into OpenSearch aggregations (preferred) or compute in Java operators over hits? If both, when does each apply?
- What is the exact exchange transport framing (chunk size, compression, checksums, retry semantics, timeouts)?
- How are memory allocations accounted for at every layer so OpenSearch breakers can reliably prevent OOM?
- What is the “safe default”: gather-at-coordinator only, no repartition, no joins, until metrics and controls prove stable?
- How is multi-tenant fairness handled (per user/role quotas) beyond a node-local semaphore?

## Suggested Doc Edits (Concrete)
1. Add “Semantic Compatibility Contract” section (version pin, SQL subset matrix, explicit divergences, error model).
2. Add “Failure Modes and Recovery” section (exchange/memory/spill/transport/node failure).
3. Move “Observability Minimum Bar” earlier and define phase gates.
4. Expand “Exchange” with a concrete protocol and bounded-resource guarantees.
5. Expand “Memory” with explicit breaker wiring and cancellation rules.
6. Add “Release Checklist” for shading/upgrades/licensing.

## Bottom Line
The overall direction is sound, but the plan should be treated as an OpenSearch distributed runtime project (exchange + governance + operability) as much as a SQL engine project. The success criteria should be defined as explicit semantic contracts and safety/operability gates, not only feature milestones.
