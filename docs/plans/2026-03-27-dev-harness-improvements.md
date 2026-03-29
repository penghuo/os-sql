# Dev Harness Improvements — Implementation Plan

> **For kiro-cli:** Use `executing-plans` skill for sequential tasks.

**Goal:** Make dev_harness.sh production-ready — fix the correctness regression, add data-exists guard, and update AGENTS.md with new workflow.

**Architecture:** Incremental fixes to existing scripts. No new files.

**Tech Stack:** Bash, OpenSearch REST API

---

### Task 1: Add data-exists guard to dev_harness.sh

Before compile step, verify `hits` (100M) and `hits_1m` (1M) indices exist with expected row counts. Fail-fast if data is missing — don't waste time compiling and reloading.

**Files:**
- Modify: `benchmarks/clickbench/run/dev_harness.sh` (add guard before Step 1)

**Step 1: Add index check before compile**

Insert before the compile step:

```bash
# =============================================================================
# Pre-check: Verify data is indexed
# =============================================================================
log "Pre-check: Verifying indexed data..."
HITS_COUNT=$(curl -sf "${OS_URL}/hits/_count" | jq -r '.count // 0')
HITS_1M_COUNT=$(curl -sf "${OS_URL}/hits_1m/_count" | jq -r '.count // 0')

if [ "$HITS_COUNT" -lt 99000000 ]; then
    log "###FAILED: pre-check (hits index has ${HITS_COUNT} rows, expected ~99M)###"
    exit 1
fi
if [ "$HITS_1M_COUNT" -lt 1000000 ]; then
    log "###FAILED: pre-check (hits_1m index has ${HITS_1M_COUNT} rows, expected 1M)###"
    exit 1
fi
log "Data OK: hits=${HITS_COUNT}, hits_1m=${HITS_1M_COUNT}"
```

**Step 2: Test**

Run: `bash run/dev_harness.sh --help` or just verify the guard by stopping OpenSearch and running the script — should fail immediately.

**Step 3: Commit**

```bash
git add benchmarks/clickbench/run/dev_harness.sh
git commit -m "dev_harness: add data-exists guard before compile"
```

---

### Task 2: Investigate correctness regression (31/43 vs 38/43)

After plugin reload, correctness dropped from 38/43 to 31/43. 7 new FAILs: Q18, Q24, Q25, Q29, Q33, Q39, Q40. These may be non-deterministic (ordering, float precision on cold JVM).

**Files:**
- Read: `benchmarks/clickbench/results/correctness/diffs/` (diff files for failed queries)

**Step 1: Examine the diffs**

```bash
for q in 18 24 25 29 33 39 40; do
    echo "=== Q${q} ==="
    cat benchmarks/clickbench/results/correctness/diffs/q$(printf '%02d' $q).diff 2>/dev/null | head -20
done
```

**Step 2: Classify each failure**

- Float precision → normalization issue in check_correctness.sh
- Row ordering → missing ORDER BY or non-deterministic tie-breaking
- Actual wrong results → DQE bug

**Step 3: Fix check_correctness.sh if normalization issues, or document as known DQE bugs**

**Step 4: Re-run correctness and verify threshold is met**

```bash
bash run/dev_harness.sh --query 1 --correctness-threshold 38
```

---

### Task 3: Update AGENTS.md with dev_harness.sh workflow

Replace the manual 4-step dev iteration loop in AGENTS.md with the new single-command workflow.

**Files:**
- Modify: `AGENTS.md` (Dev Iteration Loop section)

**Step 1: Update the dev iteration loop section**

Replace the current manual steps with:

```markdown
## Dev Iteration Loop

1. Code change (edit Java files)
2. Run dev harness:
   ```bash
   # Full loop: compile → reload → correctness gate → benchmark
   nohup bash -c 'cd /local/home/penghuo/oss/os-sql/benchmarks/clickbench && bash run/dev_harness.sh --query 4,5,11 > /tmp/dev.log 2>&1' &>/dev/null &

   # Monitor:
   tail -5 /tmp/dev.log

   # Check completion:
   tail -1 /tmp/dev.log | grep '###COMPLETE\|###FAILED'
   ```
3. If correctness gate fails (below 38/43), fix before benchmarking.
```

**Step 2: Commit**

```bash
git add AGENTS.md
git commit -m "docs: update AGENTS.md with dev_harness.sh workflow"
```

---

### Task 4: Remove duplicate correctness/run_all.sh

`correctness/run_all.sh` is a near-copy of `run/run_all.sh` (2-line diff). This is fragile.

**Files:**
- Read: `benchmarks/clickbench/correctness/run_all.sh` and `benchmarks/clickbench/run/run_all.sh`
- Decide: delete the duplicate or make one source the other

**Step 1: Diff the two files**

```bash
diff benchmarks/clickbench/correctness/run_all.sh benchmarks/clickbench/run/run_all.sh
```

**Step 2: If safe, delete the duplicate and symlink or consolidate**

**Step 3: Test that `run_all.sh correctness` still works**

**Step 4: Commit**
