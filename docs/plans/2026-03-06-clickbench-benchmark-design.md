# ClickBench Benchmark Integration Design

**Date**: 2026-03-06
**Status**: Approved

## Goal

Migrate the ClickBench benchmark framework from `wukong-benchmark` into the `wukong` repo, run correctness and performance benchmarks against the 43 ClickBench queries, fix query correctness issues in DQE, then optimize performance at the engine level.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Migration strategy | Direct copy + adapt | Scripts are proven; minimal changes needed |
| Target environment | EC2 instance | Keep existing AWS/EBS infrastructure |
| Workflow | Two-phase: correctness then perf | Must get correct results before optimizing speed |
| Ground truth | ClickHouse + saved expected results | ClickHouse for initial baseline; saved files for CI regression |
| Performance scope | DQE engine-level only | Predicate pushdown, aggregation, expression compilation |

## Migration

### File Structure

Copy `benchmarks/clickbench/` from `wukong-benchmark` into `wukong/benchmarks/clickbench/`:

```
wukong/benchmarks/clickbench/
├── README.md
├── setup/
│   ├── setup_common.sh           # Add REPO_ROOT auto-detection
│   ├── setup_clickhouse.sh       # Unchanged
│   └── setup_opensearch.sh       # Build plugin from local repo
├── data/
│   ├── download_data.sh          # Unchanged
│   ├── load_clickhouse.sh        # Unchanged
│   └── load_opensearch.sh        # Unchanged
├── queries/
│   ├── create_clickhouse.sql     # 98-field MergeTree schema
│   ├── queries.sql               # 43 ClickHouse-dialect queries
│   └── queries_trino.sql         # 43 Trino-dialect queries for DQE
├── run/
│   ├── run_all.sh                # Orchestrator
│   ├── run_clickhouse.sh         # 43 queries x 3 runs
│   └── run_opensearch.sh         # 43 queries x 3 runs via /_plugins/_trino_sql
├── correctness/
│   ├── check_correctness.sh      # Normalize + diff outputs
│   └── expected/                 # Saved ClickHouse baseline results
├── results/
│   ├── opensearch/
│   └── clickhouse/
└── viewer/
    └── setup_viewer.sh
```

Also copy `integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json` for the OpenSearch index schema.

### Key Adaptation: setup_opensearch.sh

Current behavior: downloads OpenSearch tarball + clones/builds plugin from remote branch.

New behavior:
1. Download OpenSearch 3.6.0-SNAPSHOT tarball (unchanged)
2. Build plugin from local repo: `cd $REPO_ROOT && ./gradlew :plugin:assemble`
3. Install locally-built zip into OpenSearch
4. Start OpenSearch

`REPO_ROOT` is auto-detected in `setup_common.sh` from the script's location:
```bash
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
```

## Phase 1: Correctness

### Step 1 — Baseline Run

Execute all 43 queries against both ClickHouse and OpenSearch. The existing `check_correctness.sh` produces a PASS/FAIL/SKIP report with row counts and diffs.

### Step 2 — Failure Categorization

Classify each failure:

| Category | Symptom | Example |
|----------|---------|---------|
| Parse error | HTTP 400 / syntax error | Missing function like `REGEXP_REPLACE` |
| Planning error | HTTP 500 / planning failed | Unsupported GROUP BY expression |
| Execution error | HTTP 500 / runtime error | Type mismatch in operator |
| Wrong results | Diff in output | Incorrect aggregation, precision loss |

### Step 3 — Systematic Fixes

Fix by category, simplest first:
1. **Parse errors** — Add missing functions or syntax to DQE's Trino parser integration
2. **Planning errors** — Fix optimizer rules, type resolution, plan generation
3. **Execution errors** — Fix operator implementations (Filter, HashAgg, Sort, etc.)
4. **Wrong results** — Debug with explain output, compare execution plans

### Step 4 — Save Expected Results

Once all 43 queries pass, save ClickHouse outputs as `correctness/expected/q{01-43}.expected` for regression testing without ClickHouse.

## Phase 2: Performance

### Step 1 — Baseline Performance

Run all 43 queries x 3 iterations on both engines. Capture cold/warm timings in ClickBench-compatible JSON format.

### Step 2 — Identify Bottlenecks

Rank queries by OpenSearch-to-ClickHouse time ratio (warm runs). Focus on worst performers — they indicate systematic engine issues.

### Step 3 — Engine-Level Optimizations

Target areas based on ClickBench query patterns:

- **Predicate pushdown**: Push WHERE clauses into PageSource/SearchAction
- **Aggregation strategies**: Hash vs sorted aggregation, partial aggregation on shards
- **Expression compilation**: Batch evaluation, vectorized operations
- **Sort optimization**: Top-N pushdown for ORDER BY ... LIMIT
- **Column pruning**: Only read columns referenced in the query

### Step 4 — Regression Tracking

After each optimization, re-run the full benchmark. Compare against baseline to measure improvement and catch regressions.
