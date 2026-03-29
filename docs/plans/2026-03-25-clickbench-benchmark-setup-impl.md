# ClickBench Benchmark Setup & Execution Plan (kiro-cli)

> **For kiro-cli:** Step-by-step execution plan to set up OpenSearch 3.6 benchmark environment, restore data, get baseline, and run ClickBench queries.

**Goal:** Set up a reproducible ClickBench benchmark environment on a fresh machine using OpenSearch 3.6.0-SNAPSHOT, restore the 100M-row dataset from S3 snapshot, and run all 43 queries against the official ClickHouse-Parquet baseline. Then iterate: root cause failing queries, fix DQE code, reinstall plugin, re-benchmark.

**Current Score:** 25/43 within 2x of ClickHouse-Parquet. Target: 43/43.

**Correctness Baseline:** 33/43 pass on 1M dataset. Must not regress.

**Environment:** Linux x64, 16+ vCPU, 64+ GB RAM. JDK 21 required.

**Repo:** `/local/home/penghuo/oss/os-sql` (branch: `wukong`)

**Key Files:**
- Setup scripts: `benchmarks/clickbench/setup/setup_opensearch.sh`, `setup_clickhouse.sh`
- Run scripts: `benchmarks/clickbench/run/run_all.sh`, `run/run_opensearch.sh`
- Queries: `benchmarks/clickbench/queries/queries_trino.sql` (43 queries, 1-based line numbers)
- Baseline: `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
- DQE code: `dqe/src/main/java/org/opensearch/sql/dqe/`

---

## Phase 1: Install OpenSearch 3.6.0-SNAPSHOT

### Task 1.1: Download and extract OpenSearch minimal distribution

```bash
wget -O /tmp/opensearch-snapshot.tar.gz \
  https://artifacts.opensearch.org/snapshots/core/opensearch/3.6.0-SNAPSHOT/opensearch-min-3.6.0-SNAPSHOT-linux-x64-latest.tar.gz
sudo mkdir -p /opt/opensearch
sudo tar -xzf /tmp/opensearch-snapshot.tar.gz -C /opt/opensearch --strip-components=1
rm -f /tmp/opensearch-snapshot.tar.gz
```

### Task 1.2: Configure single-node benchmark cluster

```bash
cat <<YAML | sudo tee /opt/opensearch/config/opensearch.yml > /dev/null
cluster.name: clickbench
node.name: node-1
network.host: 0.0.0.0
discovery.type: single-node
path.repo: ["$(cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && pwd)/snapshots"]
YAML
```

### Task 1.3: Set heap to 32GB

Default 16GB cap is too low for high-cardinality GROUP BY on 100M rows.

```bash
sudo mkdir -p /opt/opensearch/config/jvm.options.d
echo "-Xms32g" | sudo tee /opt/opensearch/config/jvm.options.d/heap.options
echo "-Xmx32g" | sudo tee -a /opt/opensearch/config/jvm.options.d/heap.options
```

### Task 1.4: Create opensearch user and set permissions

```bash
sudo useradd -r -s /bin/false opensearch 2>/dev/null || true
sudo chown -R opensearch:opensearch /opt/opensearch
```

### Task 1.5: Build and install SQL plugin

```bash
cd /local/home/penghuo/oss/os-sql
./gradlew :opensearch-sql-plugin:assemble
sudo /opt/opensearch/bin/opensearch-plugin install \
  file://$(pwd)/plugin/build/distributions/opensearch-sql-plugin-*.zip --batch
```

### Task 1.6: Start OpenSearch and verify

```bash
sudo -u opensearch /opt/opensearch/bin/opensearch -d -p /opt/opensearch/opensearch.pid
# Wait for startup
while ! curl -sf http://localhost:9200 >/dev/null 2>&1; do sleep 3; done

# Verify
curl -sf http://localhost:9200/_cluster/health | jq .status        # → "green" or "yellow"
curl -sf http://localhost:9200/_cat/plugins?v                       # → opensearch-sql listed
ps aux | grep opensearch | grep -o '\-Xms[0-9]*[gm]'              # → -Xms32g
```

---

## Phase 2: Restore 100M Dataset from S3 Snapshot

### Task 2.1: Install repository-s3 plugin

```bash
# Stop OpenSearch first
sudo -u opensearch kill "$(cat /opt/opensearch/opensearch.pid)" 2>/dev/null
while curl -sf http://localhost:9200 >/dev/null 2>&1; do sleep 2; done

sudo /opt/opensearch/bin/opensearch-plugin install repository-s3 --batch
```

### Task 2.2: Get AWS credentials via ada

```bash
ada credentials update --account 924196221507 --provider isengard --role Admin --once
```

S3 bucket: `s3://flint-data-dp-us-west-2-beta/data/clickbench/`

### Task 2.3: Restart OpenSearch and register S3 snapshot repository

```bash
sudo chown -R opensearch:opensearch /opt/opensearch
sudo -u opensearch /opt/opensearch/bin/opensearch -d -p /opt/opensearch/opensearch.pid
while ! curl -sf http://localhost:9200 >/dev/null 2>&1; do sleep 3; done

curl -sf -XPUT "http://localhost:9200/_snapshot/s3_clickbench" \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "s3",
    "settings": {
      "bucket": "flint-data-dp-us-west-2-beta",
      "base_path": "data/clickbench",
      "region": "us-west-2",
      "readonly": true
    }
  }' | jq .
# Expected: {"acknowledged": true}
```

### Task 2.4: Verify snapshot exists

```bash
curl -sf "http://localhost:9200/_snapshot/s3_clickbench/_all" | jq '.snapshots[] | {name, state, indices}'
# Expected: name="hits-snapshot", state="SUCCESS", indices=["hits"]
```

### Task 2.5: Restore the 4-shard hits index (~15-30 min)

```bash
# Delete existing index if any
curl -sf -XDELETE "http://localhost:9200/hits" | jq .

# Restore
curl -sf -XPOST "http://localhost:9200/_snapshot/s3_clickbench/hits-snapshot/_restore" \
  -H 'Content-Type: application/json' \
  -d '{"indices": "hits", "include_global_state": false}' | jq .
# Expected: {"accepted": true}

# Monitor progress
while true; do
    status=$(curl -sf "http://localhost:9200/_cat/recovery/hits?active_only=true" 2>/dev/null)
    count=$(curl -sf "http://localhost:9200/hits/_count" 2>/dev/null | jq -r '.count // 0')
    echo "$(date +%H:%M:%S) docs=$count active_recovery=$(echo "$status" | wc -l)"
    [ -z "$status" ] && break
    sleep 30
done
```

### Task 2.6: Verify restored index

```bash
curl -sf "http://localhost:9200/hits/_count" | jq .count                    # → ~99997497
curl -sf "http://localhost:9200/hits/_settings" | jq '.hits.settings.index | {number_of_shards, number_of_replicas}'
                                                                             # → shards: 4
curl -sf "http://localhost:9200/_cat/indices/hits?v"
```

---

## Phase 3: Load 1M Subset (for Correctness Testing)

### Task 3.1: Load 1M dataset into both ClickHouse and OpenSearch

Creates `hits_1m` in both engines (first 1M rows), generates expected query results from ClickHouse, and creates a local filesystem snapshot for fast restores.

```bash
cd benchmarks/clickbench
bash run/run_all.sh load-1m
```

### Task 3.2: Verify 1M index

```bash
curl -sf http://localhost:9200/hits_1m/_count | jq .count          # → 1000000
clickhouse-client -q "SELECT count() FROM hits_1m"                 # → 1000000
```

### Task 3.3: Restore 1M index (if lost)

```bash
bash run/run_all.sh restore-1m
```

---

## Phase 4: Get ClickHouse-Parquet Baseline

**CRITICAL:** Compare against official ClickHouse-Parquet results, NOT native MergeTree.

### Task 3.1: Verify baseline file exists in repo

```bash
cat benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json | jq '.system, .machine'
# Expected: "ClickHouse (Parquet, single)", "c6a.4xlarge"
```

Source: https://github.com/ClickHouse/ClickBench/tree/main/clickhouse-parquet/results

JSON format: 43 entries (0-indexed), each with 3 runs. Use `min()` per query as baseline.

### Task 3.2: Print baseline times

```bash
cd benchmarks/clickbench
python3 -c "
import json
with open('results/performance/clickhouse_parquet_official/c6a.4xlarge.json') as f:
    ch = json.load(f)
for i, runs in enumerate(ch['result']):
    best = min(runs)
    print(f'Q{i:02d}: {best:.3f}s')
"
```

---

## Phase 5: Run ClickBench Benchmark

### Task 5.1: Correctness check on 1M dataset (fast, ~2 min)

Always run correctness before performance. Uses `hits_1m` index.

```bash
cd benchmarks/clickbench
bash run/run_all.sh correctness
jq '.summary' results/correctness/summary.json
# Expected: pass count >= 33 (no regression)
```

Single query correctness:
```bash
bash run/run_all.sh correctness --query 17
```

### Task 5.2: Full 43-query performance benchmark (~25 min)

```bash
cd benchmarks/clickbench
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full
```

Single query benchmark:
```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 17 --output-dir /tmp/q17
```

### Task 5.3: Compare against baseline

```bash
cd benchmarks/clickbench
python3 -c "
import json, glob
with open('results/performance/clickhouse_parquet_official/c6a.4xlarge.json') as f:
    ch = json.load(f)
os_file = glob.glob('/tmp/full/*.json')[0]
with open(os_file) as f:
    os_data = json.load(f)
within = 0
for i in range(43):
    ch_best = min(ch['result'][i])
    os_times = [x for x in os_data['result'][i] if x is not None]
    if not os_times: continue
    os_best = min(os_times)
    ratio = os_best / ch_best if ch_best > 0 else 999
    if ratio <= 2.0: within += 1
    else: print(f'Q{i:02d}: {ratio:.1f}x ({os_best:.3f}s vs {ch_best:.3f}s)')
print(f'\n{within}/43 within 2x')
"
```

---

## Phase 6: Dev Iteration Loop (Root Cause → Fix → Reinstall → Benchmark)

### Task 6.1: Make code changes

Key DQE files (prefix: `dqe/src/main/java/org/opensearch/sql/dqe/`):

| File | Lines | Purpose |
|------|-------|---------|
| `shard/transport/TransportShardExecuteAction.java` | ~2200 | Shard dispatch, pattern detection |
| `shard/source/FusedGroupByAggregate.java` | ~11700 | All GROUP BY execution |
| `shard/source/FusedScanAggregate.java` | ~1600 | Scalar aggregation |
| `coordinator/transport/TransportTrinoSqlAction.java` | ~2000 | Coordinator, result merging |

### Task 6.2: Compile (~5s)

```bash
cd /local/home/penghuo/oss/os-sql
./gradlew :dqe:compileJava
```

### Task 6.3: Rebuild plugin + restart OpenSearch (~3 min)

```bash
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin
```

**CRITICAL: Never run reload-plugin while a benchmark is running.**

### Task 6.4: Correctness gate

```bash
bash run/run_all.sh correctness
# Must stay >= 33/43. If regression, fix before proceeding.
```

### Task 6.5: Benchmark the target query

```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query N --output-dir /tmp/qN
```

### Task 6.6: Full benchmark after batch of fixes

```bash
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full
```

---

## Query Numbering Reference

| Context | Indexing | Example: "Q17" |
|---------|----------|-----------------|
| `--query N` in run scripts | 1-based | `--query 18` |
| `queries_trino.sql` line | 1-based | line 18 |
| JSON `result[N]` array | 0-based | `result[17]` |
| Handover doc (Q00-Q42) | 0-based | Q17 |

---

## Remaining 18 Queries — Root Cause Summary

| Category | Queries | Root Cause | Fix Strategy |
|----------|---------|------------|--------------|
| A: COUNT(DISTINCT) decomposition | Q04,Q05,Q08,Q09,Q11,Q13 | Calcite 2-level agg doubles key space | Intercept at TransportShardExecuteAction, fused GROUP BY with per-group LongOpenHashSet |
| B: High-cardinality GROUP BY | Q15,Q16,Q18,Q32 | 17M-50M entry HashMap misses CPU cache | Parallelize single-key path, hash-partitioned aggregation |
| C: Full-table VARCHAR GROUP BY | Q35,Q36,Q39 | Millions of unique URL/IP groups | Per-doc hash overhead — fundamentally limited |
| D: Borderline | Q02,Q28,Q30,Q31,Q37 | Small gaps (Q31 needs 3ms, Q28 needs regex caching) | Targeted micro-optimizations |

**Recommended attack order:** A → B → D → C (highest leverage first).

---

## Pitfalls

1. **Plugin reload kills benchmarks** — never run concurrently
2. **JIT warmup** — always `--warmup 3`, first runs are 2-5x slower without it
3. **Query numbering** — `--query 17` = line 17 = `result[16]` = Q16 (see table above)
4. **1M vs 100M** — always benchmark on 100M (`hits`), 1M (`hits_1m`) is correctness only
5. **GC pressure** — sequential queries cause heap pressure, warmup helps
6. **OrdinalMap cost** — first VARCHAR query pays ~200ms-3s, cached afterward
7. **Result filename** — output is `<instance-type>.json`, varies by machine, use `glob` to find it
8. **ClickHouse baseline** — use Parquet official, NOT local native MergeTree times

---

## Time Estimates

| Phase | Duration |
|-------|----------|
| Phase 1: Install OpenSearch + SQL plugin | ~10 min |
| Phase 2: S3 snapshot restore (85GB, 4 shards) | ~15-30 min |
| Phase 3: Load 1M subset + ClickHouse | ~5 min |
| Phase 4: Verify baseline | ~1 min |
| Phase 5: Full benchmark (43 × 5 runs + 3 warmup) | ~25 min |
| Phase 6: Per iteration (compile + reload + single query) | ~5 min |
| **Total setup to first benchmark** | **~55-70 min** |
