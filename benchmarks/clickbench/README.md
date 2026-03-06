# ClickBench: OpenSearch trino_sql vs ClickHouse

Benchmark framework comparing OpenSearch's `/_plugins/_trino_sql` endpoint against ClickHouse using the standard [ClickBench](https://benchmark.clickhouse.com/) 43-query dataset (~100M web analytics rows).

## Prerequisites

- EC2 instance (Amazon Linux 2023 or Ubuntu 22.04+)
- AWS CLI configured (for EBS snapshots)
- `jq`, `wget`, `curl`, `bc` installed
- ~50GB disk space (15GB parquet + loaded data in both engines)

## Quick Start

```bash
# Full run: install engines, download data, load, benchmark, check correctness
./run/run_all.sh all
```

## Step-by-Step

```bash
# 1. Install ClickHouse + OpenSearch (with trino_sql plugin)
./run/run_all.sh setup

# 2. Download dataset + load into both engines
./run/run_all.sh load

# 3. (Optional) Snapshot data so you don't reload next time
./run/run_all.sh snapshot

# 4. Run benchmark (43 queries × 3 runs each)
./run/run_all.sh benchmark

# 5. Verify correctness (ClickHouse = ground truth)
./run/run_all.sh correctness
```

## Resuming After Reboot

```bash
# Restore from EBS snapshot instead of reloading data
./run/run_all.sh restore
./run/run_all.sh benchmark
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
| `BULK_SIZE` | `5000` | Docs per bulk request |

## Results

Results are written in [ClickBench JSON format](https://github.com/ClickHouse/ClickBench) to:
- `results/clickhouse/<instance-type>.json`
- `results/opensearch/<instance-type>.json`

View with the ClickBench dashboard:
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
