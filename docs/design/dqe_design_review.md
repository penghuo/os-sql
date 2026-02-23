# Review: `dqe_design.md` (Multi-Agent)

Date: 2026-02-21
Reviewed: `docs/design/dqe_design.md`

## Summary
- The design is substantially improved versus the earlier research: it has an explicit SQL support matrix (Section 3.2), error posture (Section 3.3), divergence list (Section 3.4), conformance expectations (Section 3.5), phase operability gates (Section 24), and Phase 1 observability as a hard requirement (Section 21.1).
- Remaining execution risk concentrates in the distributed runtime contract: exchange semantics, shard scheduling under relocation, memory accounting completeness, and client-stable error payloads.

## Owner Review Feedback (Consolidated)
- Trino pinning: pin to Trino 479 (not 439) and do not assume quarterly upgrades. If upgrades are rare, explicitly document the patch policy for Trino CVEs (cherry-pick or bump) so security fixes are not blocked by cadence.
- Split-to-shard mapping: "Trino Split = OpenSearch shard copy" should include either primary or replica. The scheduler must select exactly one shard copy per split to avoid double-scans and must define retry behavior (for example: retry the same split on a replica/new primary if the chosen copy fails).
- REST surface: prefer using the existing SQL REST endpoint (`/_sql` or OpenSearch's equivalent) rather than introducing a new `/_plugins/_dqe` endpoint. If DQE is a new engine behind that endpoint, the doc must specify routing/feature-flag behavior and how it coexists with existing SQL behavior.
- Doc hygiene: remove all editorial "Addresses review ..." callouts from the design doc. Keep review traceability in a separate review document, not embedded in the design itself.
- Build integration: keep DQE implemented within the existing `opensearch-sql` plugin repo/build system, but do not depend on existing engine modules (SQL/PPL/Calcite). The doc should explicitly define "allowed dependencies" (plugin framework, transport, settings, security integration, test harness) vs "disallowed dependencies" (existing query engine/planner/execution modules).
- Test plan: define whether we can reuse existing Trino OpenSearch connector test data and query cases as differential tests (native DQE vs standalone Trino/connector results) and how those datasets are materialized in CI.
- Phase plan quality: every phase must be testable with objective exit criteria (automated tests + measurable gates). Avoid ambiguous phase goals that can be satisfied by superficial implementations (for example, "works end-to-end" without correctness/perf assertions).

## Strengths To Keep
- Section 3.2 and 3.3: parse full Trino grammar, reject unsupported constructs in analysis with explicit errors.
- Section 10.2: Aggregation Path A (search aggregation) vs Path B (operator aggregation) is pragmatic and debuggable via `_explain`.
- Section 15: explicit integration with both a dedicated breaker and the parent breaker is the correct cluster-safety default.
- Section 19: failure modes table ties symptoms to metrics and operator actions.
- Section 21: metrics are phase-gated, not deferred to late hardening.
- Section 23: shading and release gates are called out explicitly (no-leak, linkage smoke test, conflict report, licensing).

## Must-Fix Gaps (Before Implementation Starts)
- Exchange protocol needs a channel state machine, ACK model, retry semantics, and a "do not block worker threads" backpressure strategy (Section 12).
- Scheduler needs a shard-routing contract and relocation behavior definition (Section 11).
- REST/API needs a stable error catalog (HTTP status, error code, structured fields) that is test-gated (Section 3.3 and Section 6).
- Type mapping needs deterministic rules for multi-fields and arrays/nested plus verification tests (Section 9).
- Memory accounting needs an explicit scope statement (what bytes are reserved) for decompression, page decode, operators, spill buffers, and exchange buffers (Section 15 and Section 12.3).

## Section Notes

## Section 3: Semantic Compatibility Contract
- Issue: Section 3.2 is a matrix, but it is not yet a "phase contract" with canonical queries that MUST succeed and MUST fail.
- Recommendation: Add a per-phase "feature contract" subsection listing representative queries and the expected rejection codes for unsupported constructs.
- Issue: Section 3.3 describes exceptions, but client compatibility depends on a stable error payload contract.
- Recommendation: Add an "Error Catalog" table with `error_code`, `http_status`, `message_template`, and fields like `construct`, `line`, `column`, `query_id`, `stage_id`.
- Issue: Section 3.5 lists conformance topics but not the precise semantics being asserted.
- Recommendation: Add micro-contracts for NULL semantics, timezone handling, and decimal overflow/rounding, and gate enabling `plugins.dqe.enabled` on these tests.

## Section 9: Type System and Mapping Contract
- Issue: Mapping `text -> VARCHAR` is ambiguous for filtering, sorting, grouping, and multi-field behavior.
- Recommendation: Specify deterministic rules for `.keyword` selection and explicit error behavior when users attempt to sort/aggregate on pure `text` without fielddata.
- Issue: Arrays, nested, and dynamic mappings need deterministic handling when documents violate assumptions.
- Recommendation: Document how arrays are detected (`_meta` vs inference), what the runtime does on schema violations, and add tests per mapping type that compare results against pinned Trino behavior where applicable.

## Section 11: Distributed Execution Model
- Issue: The design pins a shard via `_shards:<id>|_local`, but it does not state how the scheduler guarantees the fragment is launched on the node that owns that shard primary.
- Recommendation: Define the scheduling contract in terms of `ClusterState` routing: require `ShardRouting` is `STARTED`, route `stage/execute` to the node owning the target shard, and define behavior for relocation or initializing shards.
- Issue: Replica fallback policy is not explicit.
- Recommendation: State whether retries may target replicas (and under what correctness constraints with PIT), or whether DQE is primary-only and fail-fast in early phases.

## Section 12: Exchange Protocol
- Issue: Message fields are defined, but protocol versioning and serialization invariants are not.
- Recommendation: Add `protocol_version` and a binary encoding spec for `DqeDataPage` so upgrades can detect drift and fail safely.
- Issue: `sequenceNumber` is useful for dedup, but dedup without an ACK model is underspecified.
- Recommendation: Define ACK semantics (per chunk or batched), consumer state (last-acked seqno), and what happens on retransmit, gap, and close/abort races.
- Issue: Backpressure currently blocks producer driver threads (Section 12.3) which can strand `dqe_worker` threads under load.
- Recommendation: Use async/credit-based flow control so producers yield and resume, and ensure exchange send/receive runs primarily on `dqe_exchange` rather than `dqe_worker`.
- Issue: Buffer accounting (compressed vs uncompressed) is unclear.
- Recommendation: State what is breaker-reserved for each buffer (compressed bytes, decompression workspace, decoded page bytes) and make it consistent across producer and consumer.
- Issue: Transport-limit mapping is not explicit.
- Recommendation: Add required minimum transport settings and clarify how `chunk_size` interacts with transport framing and socket timeouts.
- Issue: Integrity checking is not described.
- Recommendation: Define a checksum strategy (either via LZ4 frame checksums or explicit checksum field) and the retry/abort behavior on checksum or decompression failures.

## Section 15: Memory Management and Circuit Breaker Integration
- Issue: The tracker interface is clear, but the "what must be accounted" scope is not fully enumerated.
- Recommendation: Add accounting invariants: every retained byte in operators, exchange buffers, decompression buffers, and spill buffers must go through `DqeMemoryTracker`, with labels that make metrics attributable.
- Issue: Breaker trip behavior is described, but cancellation timing under distributed exchange needs to be guaranteed.
- Recommendation: Make breaker-trip cancellation explicitly close/abort exchange channels on both sides and record a single terminal error cause for the query.

## Section 16: Resource Isolation and Thread Pools
- Issue: Default sizing uses `min(...)`, which underutilizes large analytics nodes and may serialize high shard fanout.
- Recommendation: Use a bounded `max(min(...), ...)` sizing formula, and document tuning guidance for pool vs queue changes.
- Issue: Rejection policy is "fail query" which is fine, but clients need stable error codes.
- Recommendation: Ensure pool rejections map to stable `error_code` and are surfaced in response and metrics.

## Section 18: Cancellation and Task Lifecycle
- Issue: The cancellation narrative is good, but idempotency and ACK-loss handling are not explicit.
- Recommendation: Define `stage/cancel` as idempotent, define coordinator `waitForCancelAck` timeout, and define cleanup rules for nodes that do not ACK.
- Issue: Exchange cleanup is mentioned but needs to be wired as an explicit protocol guarantee.
- Recommendation: Add explicit close/abort semantics for each channel on cancel, timeout, and node shutdown.

## Section 19: Failure Modes and Recovery
- Issue: Most severe failures end in "query fails" or "hang then timeout" which is acceptable for early phases but should be explicit.
- Recommendation: Add a Phase 3+ minimal retry policy for transient failures (for example: retry a failed shard task once on a replica/new primary, then fail), and add an "exchange channel broken" signal to fail fast instead of waiting for `query_timeout`.

## Section 21: Observability
- Issue: Metrics are good, but correlation across `_tasks`, response stats, and logs is not explicit.
- Recommendation: Ensure every metric/log includes `queryId` and stage identifiers, and add skew indicators for repartition/broadcast (top-N partitions by bytes/rows).

## Section 23: Packaging, Shading, and Release Process
- Issue: Relocations are enumerated, but relocation coverage can drift as dependencies change.
- Recommendation: Add a "shadowJar diff" gate that reports new packages/classes introduced by dependency changes and blocks merges until relocation/validation is updated.
- Issue: Licensing/NOTICE steps are described but may remain manual and stale.
- Recommendation: Make NOTICE/license generation a build artifact and add a check that the NOTICE reflects the current shaded dependency set.

## Open Questions To Resolve Early
- Are exchange retries allowed for transient transport errors, or is the engine strictly fail-fast on any chunk send failure?
- What is the minimal correctness policy for shard relocation mid-query (cancel/reschedule vs fail-fast vs wait)?
- Do we support cross-index patterns in Phase 1, and if so, what is the deterministic field-conflict behavior?
