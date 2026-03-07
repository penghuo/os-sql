# ClickBench: OpenSearch trino_sql vs ClickHouse

Benchmark framework comparing OpenSearch's `/_plugins/_trino_sql` endpoint against ClickHouse using the standard [ClickBench](https://benchmark.clickhouse.com/) 43-query dataset (~100M web analytics rows).

## 3-Tier Evaluation

| Tier | Dataset | Purpose | Command |
|------|---------|---------|---------|
| **Correctness** | 1M (1 parquet file) | Validate query results match ClickHouse | `run_all.sh correctness` |
| **Perf-lite** | 1M (1 parquet file) | Timing baseline for regression detection | `run_all.sh perf-lite` |
| **Performance** | 100M (full dataset) | Standard ClickBench comparison | `run_all.sh performance` |

All tiers apply a 60-second query timeout. Correctness and perf-lite share the same 1M dataset.

## Dev Iteration Loop

```bash
# One-time setup (if cluster is already running with full dataset):
./run/run_all.sh load-1m           # Load 1M subset into both engines

# After each code change:
./run/run_all.sh reload-plugin     # Rebuild SQL plugin + restart OpenSearch
./run/run_all.sh correctness       # Test all 43 queries on 1M
./run/run_all.sh --query 5 correctness  # Test single query
```

## Prerequisites

- EC2 instance (Amazon Linux 2023 or Ubuntu 22.04+)
- AWS CLI configured (for EBS snapshots)
- `jq`, `wget`, `curl`, `bc` installed
- ~50GB disk space (15GB parquet + loaded data in both engines)

## Full Setup (from scratch)

```bash
# Install engines, download data, load, run full benchmark
./run/run_all.sh setup
./run/run_all.sh load-full
./run/run_all.sh performance
```

## Commands

```
run_all.sh [OPTIONS] <command>

Commands:
  setup           Install OpenSearch + ClickHouse (idempotent)
  load-1m         Load 1M subset into both engines + create snapshot
  load-full       Download full dataset + load into both engines
  reload-plugin   Rebuild SQL plugin, restart OpenSearch (preserves data)
  correctness     Run 43 queries on 1M, compare results vs ClickHouse
  perf-lite       Run 43 queries x 3 on 1M, record timing + correctness
  performance     Run 43 queries x 3 on 100M, standard ClickBench benchmark
  restore-1m      Restore 1M index from snapshot
  snapshot        Create EBS snapshots of loaded data
  restore         Restore data from EBS snapshots

Aliases:
  load            Same as load-full
  benchmark       Same as performance

Options:
  --query N       Run only query N
  --dry-run       Print commands without executing
```

## Configuration

Override defaults via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `OS_HOST` | `localhost` | OpenSearch host |
| `OS_PORT` | `9200` | OpenSearch port |
| `CH_HOST` | `localhost` | ClickHouse host |
| `CH_PORT` | `9000` | ClickHouse native port |
| `NUM_TRIES` | `3` | Runs per query |
| `QUERY_TIMEOUT` | `60` | Per-query timeout in seconds |
| `BULK_SIZE` | `5000` | Docs per bulk request |

## Results

```
results/
├── correctness/          # Diffs + summary.json (PASS/FAIL/SKIP per query)
│   ├── expected_1m/      # ClickHouse ground truth for 1M
│   ├── diffs/            # Per-query diffs (failures only)
│   └── summary.json
├── perf-lite/            # ClickBench JSON + correctness flags
│   ├── clickhouse.json
│   ├── opensearch.json
│   └── summary.json
└── performance/          # Standard ClickBench format
    ├── clickhouse/
    └── opensearch/
```

View performance results with the ClickBench dashboard:
```bash
./viewer/setup_viewer.sh
```

## Directory Structure

```
setup/          Engine installation scripts
data/           Dataset download, loading, and EBS snapshots
queries/        43 ClickBench queries (ClickHouse + Trino SQL dialects)
run/            Benchmark runners and orchestrator
correctness/    Cross-engine result comparison
results/        Output JSON files
viewer/         ClickBench dashboard integration
```
