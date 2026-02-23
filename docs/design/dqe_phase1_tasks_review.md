# Review: Phase 1 Tasks and Test Plan (Critical Issues Only)

Date: 2026-02-22
Reviewed:
- `docs/design/dqe_phase1_tasks.md`
- `docs/design/dqe_phase1_test_plan.md`
- `.claude/skills/dqe-phase1-execution.md`

Focus: issues that can cause false progress or "spec gaming" (implementations that pass tests without meeting intent).

## 1. Phase 1 Scope Drift (Spec Gaming Risk: Very High)

The Phase 1 task spec is "scan/filter/project/sort/limit" with a single-table FROM.
The Phase 1 test plan currently exercises functionality that is effectively Phase 2+.

Critical mismatches:
- `docs/design/dqe_phase1_tasks.md` analyzer scope requires `FROM` (single table), but `docs/design/dqe_phase1_test_plan.md` includes `Q013` (`SELECT 1 + 2 AS constant`) with no FROM.
- Phase 1 tasks do not define a function library, but the test plan includes function calls in Phase 1 integration queries (COALESCE, NULLIF, IF, CASE, TRY_CAST, TYPEOF) and timezone functions (EXTRACT) in Section 7.
- Phase 1 explicitly rejects subqueries, but `Q076` proposes `COUNT(*) FROM (SELECT ...)` and then adds "else count manually", which is not a testable contract.

Required fix:
- Decide and document one of:
  - Option A: Keep Phase 1 minimal (no functions, no SELECT-without-FROM, no subqueries). Remove or move these test cases to Phase 2+.
  - Option B: Expand Phase 1 tasks to include the minimal subset required by the test plan (function evaluation + SELECT-without-FROM). Update `docs/design/dqe_phase1_tasks.md` accordingly.
- Remove all "if supported, else ..." language from the test plan. Every Phase 1 exit criterion must be executable and objective.

## 2. Multi-Shard Correctness Can Be "Accidentally" Wrong (Spec Gaming Risk: High)

Phase 1 introduces "primary + replica selection" in splits and also requires multi-shard correctness.
This creates a common failure mode: scanning both primary and replica for a shard will duplicate rows and can still pass LIMIT-based tests.

Required fix:
- Make the split contract explicit in `docs/design/dqe_phase1_tasks.md` and `docs/design/dqe_design.md`:
  - Each logical shard contributes exactly one shard copy per query execution (primary OR one replica), never both.
  - Replica selection is for retry and locality, not for parallel double-reading.
- Add a Phase 1 integration test that fails on duplication:
  - Dataset must have a globally unique key (already `id`).
  - Query must return the full set (limit >= doc count) and validator must assert uniqueness of `id` for that case.

## 3. ORDER BY + LIMIT Planning Is Underspecified (Spec Gaming Risk: High)

`docs/design/dqe_phase1_tasks.md` requires an in-memory `SortOperator` (no spill) and also a `TopNOperator`.
`docs/design/dqe_phase1_test_plan.md` assumes some queries require full materialization ("sort requires full materialization"), which is not consistently true and can incentivize incorrect shortcuts.

Spec-gaming pitfall:
- Implementing ORDER BY + LIMIT by "sort only the first N rows seen" is wrong but can pass small test datasets.

Required fix:
- Add an explicit planner/operator selection rule for Phase 1:
  - ORDER BY without LIMIT uses SortOperator (full sort).
  - ORDER BY with LIMIT uses TopNOperator semantics (correct top-N), and should be applied as early as possible (preferably shard-local) to avoid gather-amplification.
- Add at least one adversarial integration test where the top-N rows are not present in the first few batches/shards, so "first N rows" shortcuts fail.

## 4. Differential Tests Are Valuable But Easy To Water Down (Spec Gaming Risk: High)

The differential runner (`diff-results.py`) normalizes results (sorting unless ORDER BY) and allows tolerances.
This can hide real bugs if the normalization is too permissive.

Required fix:
- Define hard caps for tolerances (epsilon, timestamp normalization) in the test framework, not per-test, and require explicit justification for exceptions.
- Require differential tests to also validate:
  - No duplicate rows when the reference engine has none (for keys declared unique).
  - Stable ordering for ORDER BY queries (no "sort before compare" for those).
- For every normalization rule, add a "negative test" that would fail if the normalization were mistakenly made more permissive.

## 5. Integration Test Expected Results Provenance Must Be Locked Down (Spec Gaming Risk: Very High)

Phase 1 integration tests use static `expect` blocks, but the plan does not state how expected results are generated.
If expected results are generated from DQE itself, tests can pass even if DQE is wrong.

Required fix:
- Add a rule (in `docs/design/dqe_phase1_test_plan.md` and `.claude/skills/dqe-phase1-execution.md`):
  - Static expected results must be generated from an independent reference (standalone Trino, or manual derivation), never from DQE output.
  - Store a small "expected results generation note" per test suite (reference engine version, command used) to make audits possible.

## 6. Test-Count Quotas Encourage Shallow Tests (Spec Gaming Risk: Medium)

Many tasks specify minimum test counts (e.g., "minimum 60 tests").
This can incentivize low-value tests that assert only "not null" style checks.

Required fix:
- Keep counts as secondary, but prioritize "must-have invariants" and "mutation-resistant tests":
  - For each operator, include at least one test that would fail if the operator became a no-op.
  - For predicate pushdown, include at least one test that asserts the exact Query DSL generated.

## 7. Phase Exit Criteria Must Be Mechanically Enforceable (Spec Gaming Risk: High)

Some exit criteria are strong (search latency delta, breaker usage, cancellation cleanup), but they depend on tool availability and reproducibility.

Required fix:
- Ensure every Phase 1 gate has:
  - a pinned toolchain (Rally invocation, Docker versions),
  - a deterministic pass/fail script,
  - and a "skip policy" (what happens in CI when hardware is not available).
- Avoid "optional" checks for gates that are labeled as exit criteria.

## 8. Skill File: Good Guardrails, Missing a Few Anti-Gaming Rules (Spec Gaming Risk: High)

`.claude/skills/dqe-phase1-execution.md` has strong reviewer rules and human checkpoints.
The biggest missing anti-gaming rule is expected-results provenance (see Section 5).

Required fix:
- Add explicit rules for QA:
  - Do not generate static expected results from DQE.
  - Keep `validate.py` strict by default (fail on extra fields/rows unless explicitly allowed by the test case schema).
  - Log and preserve raw HTTP responses for failures (already mentioned) and forbid "auto-fixing" expected results in response to failures without human approval.

## Minimal Changes That Unlock Confidence
- Align Phase 1 scope between tasks and test plan (functions and FROM-less SELECT are the top blockers).
- Replace `Q076` with a deterministic multi-shard completeness check that does not require subqueries.
- Add at least one adversarial test per operator that defeats "first batch" / "first shard" shortcuts.
- Lock down expected-results provenance and tolerance caps to prevent silent weakening of tests.
