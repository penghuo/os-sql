# ClickBench Full Dataset (100M) Benchmark Execution Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Run the full ClickBench 43-query performance benchmark on the 100M-row dataset with a 4-shard OpenSearch index restored from S3 snapshot, compared against official ClickBench ClickHouse results.

**Architecture:** Restore the pre-built 4-shard `hits` index from S3 snapshot, bump OpenSearch heap to 32GB, rebuild the SQL plugin with current DQE code, run correctness on 1M (fast), then performance on 100M. ClickHouse baseline comes from official ClickBench results (c6a.4xlarge, 16 vCPUs, 32GB RAM — matching our 32GB heap).

**Tech Stack:** OpenSearch 3.6.0-SNAPSHOT, repository-s3 plugin, AWS S3, bash scripts

**Environment:**
- Instance: r5.4xlarge (16 vCPUs, 124GB RAM, heap bumped to 32GB)
- Current `hits` index: 100M rows, 1 shard, 73.6GB (will be replaced)
- S3 snapshot: `hits` index, 4 shards, ~85GB, from OS 3.2.0
- ClickHouse baseline: official ClickBench c6a.4xlarge results (32GB RAM)
  - Saved to: `benchmarks/clickbench/results/performance/clickhouse_clickbench_c6a.4xlarge.json`

---

### Task 1: Bump Heap + Install repository-s3 Plugin

Combine into a single stop/start cycle to save time.

**Files:**
- Modify: `/opt/opensearch/config/jvm.options` (lines 31-32: `16g` → `32g`)

**Step 1: Stop OpenSearch**

```bash
sudo -u opensearch kill "$(cat /opt/opensearch/opensearch.pid)" 2>/dev/null
while curl -sf http://localhost:9200 >/dev/null 2>&1; do sleep 2; done
```

**Step 2: Update heap settings**

```bash
sudo sed -i 's/^-Xms16g$/-Xms32g/' /opt/opensearch/config/jvm.options
sudo sed -i 's/^-Xmx16g$/-Xmx32g/' /opt/opensearch/config/jvm.options
```

**Step 3: Verify heap change**

```bash
grep -E '^-Xm[sx]' /opt/opensearch/config/jvm.options
```

Expected:
```
-Xms32g
-Xmx32g
```

**Step 4: Install repository-s3 plugin**

```bash
sudo /opt/opensearch/bin/opensearch-plugin install repository-s3 --batch
```

Expected: `-> Installed repository-s3`

**Step 5: Configure AWS credentials for S3 access**

```
Used ada credentials update --account 924196221507 --provider isengard --role Admin --once (conduit provider didn't work, isengard did).

Contents of s3://flint-data-dp-us-west-2-beta/data/clickbench/:

Directories:
- 901001101001100/
- _00110100111100/
- g01100100011011/
- indices/
- k01010110111101/
- snapshot_shard_paths/

Files:
- index-0 — 509 bytes (2025-09-13)
- index.latest — 8 bytes (2025-09-13)
- meta-RvDchkmgTHaJJRedztc_jQ.dat — 193 bytes (2025-09-13)
- snap-RvDchkmgTHaJJRedztc_jQ.dat — 371 bytes (2025-09-13)
```

**Step 6: Start OpenSearch and verify**

```bash
sudo chown -R opensearch:opensearch /opt/opensearch
sudo -u opensearch /opt/opensearch/bin/opensearch -d -p /opt/opensearch/opensearch.pid
while ! curl -sf http://localhost:9200 >/dev/null 2>&1; do sleep 3; done

# Verify heap
ps aux | grep opensearch | grep -o '\-Xms[0-9]*[gm]'

# Verify plugin
curl -sf http://localhost:9200/_cat/plugins?v
```

Expected: `-Xms32g` and `repository-s3` in plugin list.

---

### Task 2: Restore Full Dataset from S3 Snapshot

**Step 1: Register S3 snapshot repository**

```bash
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
```

Expected: `{"acknowledged": true}`

**Step 2: Verify snapshot is visible**

```bash
curl -sf "http://localhost:9200/_snapshot/s3_clickbench/_all" | jq '.snapshots[] | {name, state, indices}'
```

Expected:
```json
{
  "name": "hits-snapshot",
  "state": "SUCCESS",
  "indices": ["hits"]
}
```

**Step 3: Delete existing 1-shard hits index**

```bash
curl -sf -XDELETE "http://localhost:9200/hits" | jq .
```

Expected: `{"acknowledged": true}`

**Step 4: Restore the 4-shard hits index**

```bash
curl -sf -XPOST "http://localhost:9200/_snapshot/s3_clickbench/hits-snapshot/_restore" \
  -H 'Content-Type: application/json' \
  -d '{
    "indices": "hits",
    "include_global_state": false
  }' | jq .
```

Expected: `{"accepted": true}`

**Step 5: Monitor restore progress (~15-30 min)**

```bash
# Poll every 30s until done
while true; do
    status=$(curl -sf "http://localhost:9200/_cat/recovery/hits?active_only=true" 2>/dev/null)
    count=$(curl -sf "http://localhost:9200/hits/_count" 2>/dev/null | jq -r '.count // 0')
    echo "$(date +%H:%M:%S) docs=$count active_recovery=$(echo "$status" | wc -l)"
    [ -z "$status" ] && break
    sleep 30
done
```

**Step 6: Verify restored index**

```bash
curl -sf "http://localhost:9200/hits/_count" | jq .count
curl -sf "http://localhost:9200/hits/_settings" | jq '.hits.settings.index | {number_of_shards, number_of_replicas}'
curl -sf "http://localhost:9200/_cat/indices/hits?v"
```

Expected:
- Count: ~99,997,497
- Shards: 4
- Health: green

---

### Task 3: Rebuild SQL Plugin with Current DQE Code

**Step 1: Rebuild and reload**

```bash
cd /home/ec2-user/oss/wukong
bash benchmarks/clickbench/run/run_all.sh reload-plugin
```

Expected: OpenSearch restarts with latest SQL plugin (~2-3 min).

**Step 2: Verify DQE endpoint works on full dataset**

```bash
curl -sf -XPOST "http://localhost:9200/_plugins/_trino_sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNT(*) FROM hits"}' | jq .
```

Expected: count ~99,997,497

---

### Task 4: Run Correctness Check (1M dataset)

Correctness runs on the 1M dataset (fast, already set up). Per CLAUDE.md: correctness before performance.

**Step 1: Run correctness suite**

```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
bash run/run_all.sh correctness
```

Expected: ~2 min. Results in `results/correctness/summary.json`. Baseline: 32/43 pass.

**Step 2: Verify no regressions**

```bash
jq '.summary' results/correctness/summary.json
```

Expected: pass count >= 32 (no regression from 1M report).

---

### Task 5: Run Performance Benchmark (100M, 4 shards)

OpenSearch only — ClickHouse baseline from official ClickBench results.

**Step 1: Run OpenSearch benchmark with warmup**

```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
bash run/run_opensearch.sh \
  --timeout 120 \
  --warmup 3 \
  --num-tries 3 \
  --output-dir results/performance/opensearch
```

Expected: ~15-30 min. Results in `results/performance/opensearch/m5.8xlarge.json`.

**Step 2: Generate comparison against ClickBench baseline**

```bash
# Extract min times from both
CH_FILE="results/performance/clickhouse_clickbench_c6a.4xlarge.json"
OS_FILE="results/performance/opensearch/m5.8xlarge.json"

echo "Q | OS (s) | CH (s) | Ratio"
echo "--|--------|--------|------"
paste \
  <(jq -r '.result | to_entries[] | "\(.key+1)\t\([.value[] | select(. != null)] | min // "null")"' "$OS_FILE") \
  <(jq -r '.result | to_entries[] | "\(.key+1)\t\([.value[] | select(. != null)] | min // "null")"' "$CH_FILE") \
| awk -F'\t' '{
    q=$1; os=$2; ch=$4;
    if (ch+0 > 0 && os+0 > 0) ratio=sprintf("%.2fx", os/ch); else ratio="N/A";
    printf "Q%d | %s | %s | %s\n", q, os, ch, ratio
  }'
```

---

### Task 6: Write Full-Dataset Report

**Files:**
- Create: `docs/reports/2026-03-18-clickbench-full-dataset.md`

**Content:**
- Same format as existing `docs/reports/2026-03-18-reports.md`
- Performance table: Q, OS (ms), CH (ms), Ratio, Status
- Note: CH baseline is official ClickBench c6a.4xlarge (16 vCPU, 32GB), OS is m5.8xlarge (32 vCPU, 32GB heap)
- Queries exceeding 2x ClickHouse — root cause analysis
- Comparison: 1M/8-shard vs 100M/4-shard — how DQE scales with data size
- Key observations about data scale impact (cardinality, I/O, memory)

---

## Execution Order

```
Task 1: Heap 32GB + repository-s3 install (single restart)
    │
    └─ Task 2: Restore from S3 (~15-30 min)
           │
           ├─ Task 3: Rebuild SQL plugin
           │
           ├─ Task 4: Correctness on 1M (fast, ~2 min)
           │
           ├─ Task 5: Performance on 100M (OS only, ~15-30 min)
           │
           └─ Task 6: Write report
```

## Time Estimates

| Task | Duration |
|------|----------|
| Task 1: Heap + plugin install + restart | ~5 min |
| Task 2: S3 restore (85GB) | ~15-30 min |
| Task 3: Rebuild SQL plugin | ~3 min |
| Task 4: Correctness (1M, 43 queries) | ~2 min |
| Task 5: Performance (100M, OS only, 43 × 3 tries) | ~15-30 min |
| Task 6: Report | ~10 min |
| **Total** | **~50-80 min** |

## ClickHouse Baseline Reference

Source: `https://github.com/ClickHouse/ClickBench/blob/main/clickhouse/results/c6a.4xlarge.json`

| Property | Value |
|----------|-------|
| Machine | c6a.4xlarge (16 vCPUs, 32GB RAM) |
| System | ClickHouse (native MergeTree) |
| Total (min per query) | ~33s across 43 queries |

Rationale for using this baseline: Our OpenSearch heap is 32GB, matching the c6a.4xlarge's total memory. ClickHouse on c6a.4xlarge uses 16 threads vs our 32 vCPUs, so this is a conservative comparison (ClickHouse has less compute).

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| S3 snapshot from OS 3.2.0 incompatible with 3.6.0 | Forward-compatible restore is standard; if fails, re-index from existing parquet files |
| S3 credentials expire mid-restore | If interrupted, delete partial index and retry with fresh creds |
| 100M queries timeout at 120s | Increase to 300s for initial run if needed |
| repository-s3 plugin version mismatch | Install from official repo; matches running OS version |
| ClickHouse baseline on different hardware | Note clearly in report; c6a.4xlarge has fewer vCPUs (conservative for us) |
