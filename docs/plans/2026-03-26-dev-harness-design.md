# Dev Harness Design — 2026-03-26

## Summary

New script `benchmarks/clickbench/run/dev_harness.sh` that chains the full dev iteration loop: compile → reload-plugin → correctness-gate → benchmark. Fail-fast on any step. Structured completion markers for sisyphus monitoring.

## Pipeline

```
compile (gradlew :dqe:compileJava)
  → reload-plugin (rebuild zip, restart OS, reinstall plugin)
    → correctness-gate (38/43 threshold, fail-fast if below)
      → benchmark (run_opensearch.sh with specified queries)
```

## Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--query 4,5,11` | all 43 | Benchmark specific queries only |
| `--skip-correctness` | false | Skip correctness gate |
| `--warmup N` | 1 | Warmup passes before benchmark |
| `--correctness-threshold N` | 38 | Minimum pass count to proceed |

## Output Markers

```
###COMPLETE: compile=OK reload=OK correctness=39/43 benchmark=done###
###FAILED: correctness=35/43 (below threshold 38)###
###FAILED: compile (BUILD FAILED)###
###FAILED: reload-plugin (timeout)###
```

## Sisyphus Monitoring

```bash
nohup bash -c 'cd /path/to/benchmarks/clickbench && bash run/dev_harness.sh --query 4,5 > /tmp/dev.log 2>&1' &>/dev/null &
# Poll:
tail -1 /tmp/dev.log | grep '###COMPLETE\|###FAILED'
```

## Implementation

- Sources `setup_common.sh` for shared functions
- Calls `reload_sql_plugin()` directly (not via run_all.sh)
- Calls `check_correctness.sh` directly, parses summary line
- Calls `run_opensearch.sh` with `--warmup` and `--query` flags
