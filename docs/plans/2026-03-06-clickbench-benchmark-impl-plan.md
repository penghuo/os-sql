# ClickBench Benchmark Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Migrate ClickBench benchmark scripts from wukong-benchmark into wukong, adapt for local plugin builds, run correctness baseline, fix query failures, save expected results, and establish performance baseline.

**Architecture:** Copy shell scripts from `/home/ec2-user/oss/wukong-benchmark/benchmarks/clickbench/` into `/home/ec2-user/oss/wukong/benchmarks/clickbench/`. Adapt `setup_opensearch.sh` to build the SQL plugin from the local wukong repo instead of cloning a remote branch. Use `check_correctness.sh` to compare DQE trino_sql results against ClickHouse ground truth.

**Tech Stack:** Bash scripts, curl, jq, ClickHouse CLI, OpenSearch REST API (`/_plugins/_trino_sql`), Gradle

---

### Task 1: Copy clickbench directory from wukong-benchmark

**Files:**
- Create: `benchmarks/clickbench/` (entire directory tree)

**Step 1: Copy the directory**

```bash
cp -r /home/ec2-user/oss/wukong-benchmark/benchmarks/clickbench /home/ec2-user/oss/wukong/benchmarks/clickbench
```

**Step 2: Verify files are present**

```bash
find /home/ec2-user/oss/wukong/benchmarks/clickbench -type f | sort
```

Expected: All files listed below exist:
```
benchmarks/clickbench/README.md
benchmarks/clickbench/correctness/check_correctness.sh
benchmarks/clickbench/data/download_data.sh
benchmarks/clickbench/data/load_clickhouse.sh
benchmarks/clickbench/data/load_opensearch.sh
benchmarks/clickbench/data/snapshot.sh
benchmarks/clickbench/queries/create_clickhouse.sql
benchmarks/clickbench/queries/queries.sql
benchmarks/clickbench/queries/queries_trino.sql
benchmarks/clickbench/run/run_all.sh
benchmarks/clickbench/run/run_clickhouse.sh
benchmarks/clickbench/run/run_opensearch.sh
benchmarks/clickbench/setup/setup_clickhouse.sh
benchmarks/clickbench/setup/setup_common.sh
benchmarks/clickbench/setup/setup_opensearch.sh
benchmarks/clickbench/viewer/setup_viewer.sh
```

**Step 3: Ensure results and expected directories exist with .gitkeep**

```bash
mkdir -p /home/ec2-user/oss/wukong/benchmarks/clickbench/results/opensearch
mkdir -p /home/ec2-user/oss/wukong/benchmarks/clickbench/results/clickhouse
mkdir -p /home/ec2-user/oss/wukong/benchmarks/clickbench/correctness/expected
touch /home/ec2-user/oss/wukong/benchmarks/clickbench/results/opensearch/.gitkeep
touch /home/ec2-user/oss/wukong/benchmarks/clickbench/results/clickhouse/.gitkeep
touch /home/ec2-user/oss/wukong/benchmarks/clickbench/correctness/expected/.gitkeep
```

**Step 4: Commit**

```bash
cd /home/ec2-user/oss/wukong
git add benchmarks/clickbench/
git commit -m "chore: copy clickbench benchmark framework from wukong-benchmark"
```

---

### Task 2: Adapt setup_common.sh — add REPO_ROOT auto-detection

**Files:**
- Modify: `benchmarks/clickbench/setup/setup_common.sh`

The current `REPO_DIR` variable uses `$(cd "$BENCH_DIR/../.." && pwd)` which resolves to the repo root since `BENCH_DIR` is `benchmarks/clickbench`. This is already correct for the wukong repo structure. No change needed for `REPO_DIR`.

**Step 1: Verify REPO_DIR resolves correctly**

The path chain: `SCRIPT_DIR` → `setup/`, `BENCH_DIR` → `benchmarks/clickbench/`, `REPO_DIR` → `benchmarks/clickbench/../..` = repo root.

Run from the script's perspective:
```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench/setup && bash -c 'source setup_common.sh && echo "REPO_DIR=$REPO_DIR"'
```

Expected: `REPO_DIR=/home/ec2-user/oss/wukong`

**Step 2: Verify mapping file path resolves**

The mapping file is referenced in `setup_opensearch.sh` line 109 as:
```
$REPO_DIR/integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json
```

This file already exists in the wukong repo. No change needed.

**Step 3: No-op commit (skip if nothing changed)**

If REPO_DIR already works, move to Task 3.

---

### Task 3: Adapt setup_opensearch.sh — build plugin from local repo

**Files:**
- Modify: `benchmarks/clickbench/setup/setup_opensearch.sh` (lines 63-92, the `install_sql_plugin` function)

**Step 1: Replace the `install_sql_plugin` function**

The current function clones the SQL plugin from a remote git branch and builds it there. Replace it to build from the local wukong repo.

Replace lines 63-92 (the entire `install_sql_plugin` function) with:

```bash
# --- Build and install SQL plugin from local repo ---
install_sql_plugin() {
    local plugin_zip

    # Check if plugin already installed
    if sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" list 2>/dev/null | grep -q opensearch-sql; then
        log "SQL plugin already installed."
        return 0
    fi

    log "Building SQL plugin from local repo ($REPO_DIR)..."
    cd "$REPO_DIR"
    ./gradlew :plugin:assemble -x test -x integTest

    # Find the plugin zip
    plugin_zip=$(find "$REPO_DIR/plugin/build/distributions" -name "opensearch-sql-*.zip" | head -1)
    if [ -z "$plugin_zip" ]; then
        die "Plugin ZIP not found after build. Check ./gradlew :plugin:assemble output."
    fi

    log "Installing SQL plugin from $plugin_zip..."
    sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" install "file://$plugin_zip"

    cd "$BENCH_DIR"
}
```

**Step 2: Remove unused SQL_PLUGIN_REPO and SQL_PLUGIN_BRANCH from setup_common.sh**

In `benchmarks/clickbench/setup/setup_common.sh`, remove these two lines (lines 41-42):
```
SQL_PLUGIN_REPO="${SQL_PLUGIN_REPO:-https://github.com/opensearch-project/sql.git}"
SQL_PLUGIN_BRANCH="${SQL_PLUGIN_BRANCH:-trino-v5}"
```

**Step 3: Verify the script parses without errors**

```bash
bash -n /home/ec2-user/oss/wukong/benchmarks/clickbench/setup/setup_opensearch.sh
```

Expected: No output (clean parse).

**Step 4: Commit**

```bash
cd /home/ec2-user/oss/wukong
git add benchmarks/clickbench/setup/setup_opensearch.sh benchmarks/clickbench/setup/setup_common.sh
git commit -m "feat(clickbench): adapt setup to build plugin from local wukong repo"
```

---

### Task 4: Add .gitignore for benchmark data and results

**Files:**
- Create: `benchmarks/clickbench/.gitignore`

**Step 1: Create .gitignore**

```gitignore
# Downloaded parquet files (~15GB)
data/parquet/

# Benchmark result JSONs (machine-specific)
results/opensearch/*.json
results/clickhouse/*.json

# Correctness comparison outputs
correctness/*.out
correctness/*.txt
correctness/summary.json

# EBS snapshot config
.snapshot_config

# ClickBench viewer clone
viewer/clickbench-website/
```

**Step 2: Commit**

```bash
cd /home/ec2-user/oss/wukong
git add benchmarks/clickbench/.gitignore
git commit -m "chore(clickbench): add .gitignore for data, results, and viewer"
```

---

### Task 5: Make all shell scripts executable

**Files:**
- Modify: all `.sh` files under `benchmarks/clickbench/`

**Step 1: Set executable permissions**

```bash
chmod +x /home/ec2-user/oss/wukong/benchmarks/clickbench/setup/*.sh
chmod +x /home/ec2-user/oss/wukong/benchmarks/clickbench/data/*.sh
chmod +x /home/ec2-user/oss/wukong/benchmarks/clickbench/run/*.sh
chmod +x /home/ec2-user/oss/wukong/benchmarks/clickbench/correctness/*.sh
chmod +x /home/ec2-user/oss/wukong/benchmarks/clickbench/viewer/*.sh
```

**Step 2: Verify**

```bash
find /home/ec2-user/oss/wukong/benchmarks/clickbench -name "*.sh" -exec ls -la {} \;
```

Expected: All `.sh` files have `-rwxr-xr-x` permissions.

**Step 3: Commit**

```bash
cd /home/ec2-user/oss/wukong
git add benchmarks/clickbench/
git commit -m "chore(clickbench): make all shell scripts executable"
```

---

### Task 6: Run setup — install ClickHouse and OpenSearch with DQE plugin

This is an EC2-only task. Run on target EC2 instance.

**Step 1: Install ClickHouse**

```bash
cd /home/ec2-user/oss/wukong
bash benchmarks/clickbench/setup/setup_clickhouse.sh
```

Expected: ClickHouse installed and running. Verify with:
```bash
clickhouse-client -q "SELECT version()"
```

**Step 2: Install OpenSearch with DQE plugin**

```bash
bash benchmarks/clickbench/setup/setup_opensearch.sh
```

Expected:
- OpenSearch 3.6.0-SNAPSHOT downloaded and configured
- Plugin built from local repo via `./gradlew :plugin:assemble`
- Plugin installed and OpenSearch started
- `hits` index created

Verify:
```bash
curl -sf http://localhost:9200/ | jq .name
curl -sf http://localhost:9200/hits | jq .hits.mappings.properties | head -5
```

**Step 3: If setup_opensearch.sh fails building the plugin**

Check `./gradlew :plugin:assemble` output. Common issues:
- Missing JDK 21: `java -version` should show 21
- Build errors: run `./gradlew :plugin:assemble -x test -x integTest` manually from repo root

---

### Task 7: Download and load ClickBench data

EC2-only task. Run after Task 6.

**Step 1: Download 100 parquet files**

```bash
bash benchmarks/clickbench/data/download_data.sh
```

Expected: 100 parquet files in `benchmarks/clickbench/data/parquet/` (~15GB total).

**Step 2: Load into ClickHouse**

```bash
bash benchmarks/clickbench/data/load_clickhouse.sh
```

Expected: ~100M rows loaded. Verify:
```bash
clickhouse-client -q "SELECT count() FROM hits"
```

**Step 3: Load into OpenSearch**

```bash
bash benchmarks/clickbench/data/load_opensearch.sh
```

Expected: ~100M docs indexed. Verify:
```bash
curl -sf http://localhost:9200/hits/_count | jq .count
```

**Step 4: (Optional) Create EBS snapshots for fast re-runs**

```bash
bash benchmarks/clickbench/data/snapshot.sh create
```

---

### Task 8: Run correctness baseline — identify failures

EC2-only task. Run after Task 7.

**Step 1: Run correctness checker**

```bash
bash benchmarks/clickbench/correctness/check_correctness.sh
```

Expected output: PASS/FAIL/SKIP for each of the 43 queries, plus summary:
```
Summary: X/43 PASS, Y/43 FAIL, Z/43 SKIP
```

**Step 2: Save the baseline report**

```bash
cp benchmarks/clickbench/results/correctness/summary.json benchmarks/clickbench/correctness/baseline_summary.json
```

**Step 3: Categorize failures**

For each FAILed query, check the error output to categorize:

```bash
# For each failed query N, check the OpenSearch response:
for i in $(seq 1 43); do
    DIFF="benchmarks/clickbench/results/correctness/diff_q${i}.txt"
    if [ -f "$DIFF" ]; then
        echo "=== Q${i} ==="
        head -20 "$DIFF"
        echo ""
    fi
done
```

Also run individual queries to see HTTP status and error messages:

```bash
# Test a single query (replace N with query number, 1-indexed)
QUERY=$(sed -n 'Np' benchmarks/clickbench/queries/queries_trino.sql)
curl -sf -w '\nHTTP %{http_code}\n' -XPOST 'http://localhost:9200/_plugins/_trino_sql' \
    -H 'Content-Type: application/json' \
    -d "{\"query\": $(echo "$QUERY" | jq -Rs '.')}" | head -50
```

**Step 4: Create a failure tracking file**

Create `benchmarks/clickbench/correctness/failure_report.md` with the categorized results:

```markdown
# ClickBench Correctness Baseline - YYYY-MM-DD

## Summary
- PASS: X/43
- FAIL: Y/43
- SKIP: Z/43

## Parse Errors
| Query | Error |
|-------|-------|
| Q29   | REGEXP_REPLACE not supported |

## Planning Errors
| Query | Error |
|-------|-------|
| Q35   | GROUP BY 1 not supported |

## Execution Errors
| Query | Error |
|-------|-------|

## Wrong Results
| Query | Issue |
|-------|-------|
| Q4    | AVG(UserID) precision differs |
```

**Step 5: Commit the baseline**

```bash
cd /home/ec2-user/oss/wukong
git add benchmarks/clickbench/correctness/baseline_summary.json benchmarks/clickbench/correctness/failure_report.md
git commit -m "docs(clickbench): record correctness baseline results"
```

---

### Task 9: Fix parse errors (Category 1)

This task depends on the failure report from Task 8. Fix each parse error by adding missing function/syntax support to DQE.

**Step 1: For each parse error, identify the missing feature**

Common ClickBench queries that may cause parse errors:
- **Q29**: `REGEXP_REPLACE(Referer, '^https?://...', '\1')` — needs REGEXP_REPLACE function
- **Q28**: `length(URL)` — needs LENGTH function
- **Q43**: `DATE_TRUNC('minute', EventTime)` — needs DATE_TRUNC function
- **Q36**: `ClientIP - 1` — arithmetic on integer columns

**Step 2: For each missing function, add it to DQE's function registry**

Look in `/home/ec2-user/oss/wukong/dqe/` for the function registry. Typical location:
```
dqe/src/main/java/org/opensearch/sql/dqe/function/
```

**Step 3: Build and test each fix**

```bash
cd /home/ec2-user/oss/wukong
./gradlew :plugin:assemble -x test -x integTest
# Reinstall plugin (stop OS, remove old, install new, start OS)
sudo -u opensearch kill "$(cat /opt/opensearch/opensearch.pid)"
sudo /opt/opensearch/bin/opensearch-plugin remove opensearch-sql
sudo /opt/opensearch/bin/opensearch-plugin install "file://$(find plugin/build/distributions -name 'opensearch-sql-*.zip' | head -1)"
sudo -u opensearch /opt/opensearch/bin/opensearch -d -p /opt/opensearch/opensearch.pid
sleep 30  # wait for startup
# Re-test the specific query
```

**Step 4: Commit each fix**

```bash
git add -A
git commit -m "fix(dqe): add REGEXP_REPLACE function for ClickBench Q29"
```

---

### Task 10: Fix planning errors (Category 2)

**Step 1: For each planning error, identify the root cause**

Common issues:
- `GROUP BY 1` (positional grouping) — Q35
- `CASE WHEN ... THEN ... END` in GROUP BY — Q40
- `HAVING` clause — Q28, Q29
- `SELECT *` with large schemas — Q24

**Step 2: Fix optimizer rules or plan generation in DQE**

Look in:
```
dqe/src/main/java/org/opensearch/sql/dqe/planner/
dqe/src/main/java/org/opensearch/sql/dqe/coordinator/
```

**Step 3: Build, redeploy, test (same as Task 9 Step 3)**

**Step 4: Commit each fix**

---

### Task 11: Fix execution errors (Category 3)

**Step 1: For each execution error, identify the failing operator**

Common issues:
- Type mismatches in aggregation (SUM on short columns overflows)
- NULL handling in comparisons
- OFFSET/LIMIT order issues

**Step 2: Fix operator implementations**

Look in:
```
dqe/src/main/java/org/opensearch/sql/dqe/operator/
dqe/src/main/java/org/opensearch/sql/dqe/shard/
```

**Step 3: Build, redeploy, test**

**Step 4: Commit each fix**

---

### Task 12: Fix wrong results (Category 4)

**Step 1: For each wrong-result query, compare ClickHouse and OpenSearch output**

```bash
# View diff for query N
cat benchmarks/clickbench/results/correctness/diff_qN.txt
```

Common causes:
- Float precision (AVG returning different decimal places)
- NULL handling (NULL vs empty string)
- Sort order differences (unstable sorts)

**Step 2: Debug with explain endpoint**

```bash
# Get the execution plan
QUERY=$(sed -n 'Np' benchmarks/clickbench/queries/queries_trino.sql)
curl -sf -XPOST 'http://localhost:9200/_plugins/_trino_sql/_explain' \
    -H 'Content-Type: application/json' \
    -d "{\"query\": $(echo "$QUERY" | jq -Rs '.')}" | jq .
```

**Step 3: Fix and commit**

---

### Task 13: Re-run correctness check — verify all 43 pass

After completing Tasks 9-12.

**Step 1: Run full correctness check**

```bash
bash benchmarks/clickbench/correctness/check_correctness.sh
```

Expected: `Summary: 43/43 PASS, 0/43 FAIL, 0/43 SKIP`

**Step 2: If any failures remain, go back to Tasks 9-12 for that category**

**Step 3: Update failure report**

Update `benchmarks/clickbench/correctness/failure_report.md` with final status.

---

### Task 14: Save expected results from ClickHouse

After all 43 queries pass.

**Step 1: Generate expected result files**

```bash
QUERY_DIR="/home/ec2-user/oss/wukong/benchmarks/clickbench/queries"
EXPECTED_DIR="/home/ec2-user/oss/wukong/benchmarks/clickbench/correctness/expected"
mkdir -p "$EXPECTED_DIR"

QUERY_NUM=0
while IFS= read -r query; do
    QUERY_NUM=$((QUERY_NUM + 1))
    printf -v PADDED "%02d" "$QUERY_NUM"
    clickhouse-client -q "$query" > "$EXPECTED_DIR/q${PADDED}.expected" 2>/dev/null || echo "SKIP" > "$EXPECTED_DIR/q${PADDED}.expected"
    echo "Q${PADDED}: $(wc -l < "$EXPECTED_DIR/q${PADDED}.expected") rows"
done < "$QUERY_DIR/queries.sql"
```

**Step 2: Verify expected files**

```bash
ls -la /home/ec2-user/oss/wukong/benchmarks/clickbench/correctness/expected/
wc -l /home/ec2-user/oss/wukong/benchmarks/clickbench/correctness/expected/*.expected
```

Expected: 43 files, each with the correct row count matching ClickHouse output.

**Step 3: Commit expected results**

```bash
cd /home/ec2-user/oss/wukong
git add benchmarks/clickbench/correctness/expected/
git commit -m "docs(clickbench): save ClickHouse expected results for regression testing"
```

---

### Task 15: Run performance baseline

After correctness is established.

**Step 1: Run benchmark on both engines**

```bash
bash benchmarks/clickbench/run/run_all.sh benchmark
```

This runs all 43 queries × 3 iterations on both ClickHouse and OpenSearch.

**Step 2: Check results**

```bash
ls -la benchmarks/clickbench/results/clickhouse/*.json
ls -la benchmarks/clickbench/results/opensearch/*.json
```

**Step 3: Compare performance**

```bash
# Quick comparison: warm-run times (run 2) for each query
CH_FILE=$(ls benchmarks/clickbench/results/clickhouse/*.json | head -1)
OS_FILE=$(ls benchmarks/clickbench/results/opensearch/*.json | head -1)

echo "Query | ClickHouse(s) | OpenSearch(s) | Ratio"
echo "------|---------------|---------------|------"
for i in $(seq 0 42); do
    QN=$((i + 1))
    CH_TIME=$(jq -r ".result[$i][1] // \"null\"" "$CH_FILE")
    OS_TIME=$(jq -r ".result[$i][1] // \"null\"" "$OS_FILE")
    if [ "$CH_TIME" != "null" ] && [ "$OS_TIME" != "null" ]; then
        RATIO=$(echo "scale=1; $OS_TIME / $CH_TIME" | bc 2>/dev/null || echo "N/A")
        printf "Q%-4d | %13s | %13s | %sx\n" "$QN" "$CH_TIME" "$OS_TIME" "$RATIO"
    else
        printf "Q%-4d | %13s | %13s | N/A\n" "$QN" "$CH_TIME" "$OS_TIME"
    fi
done
```

**Step 4: Save baseline results**

```bash
cp "$CH_FILE" benchmarks/clickbench/results/clickhouse/baseline.json
cp "$OS_FILE" benchmarks/clickbench/results/opensearch/baseline.json
```

**Step 5: Commit baseline**

```bash
cd /home/ec2-user/oss/wukong
git add benchmarks/clickbench/results/
git commit -m "docs(clickbench): save performance baseline results"
```

---

### Task 16: Identify performance bottlenecks and prioritize optimizations

After Task 15.

**Step 1: Rank queries by performance ratio**

From the comparison in Task 15 Step 3, identify:
- Queries where OpenSearch is >10x slower than ClickHouse (critical)
- Queries where OpenSearch is >5x slower (important)
- Queries where OpenSearch is >2x slower (nice to have)

**Step 2: Categorize by query pattern**

Group the slowest queries by what they exercise:
- Full table scans (Q1-Q7): COUNT/SUM/AVG without GROUP BY
- GROUP BY with few groups (Q8-Q15): Aggregation strategies
- GROUP BY with high cardinality (Q16-Q18, Q31-Q33): Hash table efficiency
- String operations (Q21-Q29): LIKE, REGEXP_REPLACE
- Filtered queries (Q37-Q43): Predicate pushdown effectiveness

**Step 3: Create optimization plan**

Document findings in `benchmarks/clickbench/correctness/perf_analysis.md` with prioritized optimization targets.

**Step 4: Commit**

```bash
cd /home/ec2-user/oss/wukong
git add benchmarks/clickbench/correctness/perf_analysis.md
git commit -m "docs(clickbench): analyze performance bottlenecks and prioritize optimizations"
```
