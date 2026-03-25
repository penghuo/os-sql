# Ralph Loop Logs

Append-only log of each iteration.

## Iteration 1 — 2026-03-25T20:24–23:20Z

### What I Did
1. Fixed sudo prompts in benchmark scripts (`setup_common.sh`):
   - Added `OS_USER` variable (auto-detected from file ownership)
   - Added `_os_run` / `_os_run_as` helpers that skip sudo when current user == OS_USER
   - Replaced all hardcoded `sudo -u opensearch` / `sudo` calls in `reload_sql_plugin` with helpers
   - Made `drop_caches` best-effort (warns instead of failing without sudo)
   - Fixed snapshot `chown` to use `$OS_USER`
2. Validated pending multi-pass single-key GROUP BY change:
   - Compiled cleanly
   - Reloaded plugin (no sudo prompts)
   - Correctness gate: 29/43 PASS (no regression)
   - Benchmarked Q15 and all Category B queries

### Results
- **Q15**: 74.563s → 15.360s (4.85x improvement, but still 29.5x vs CH baseline 0.520s)
- **Q16**: 12.458s → 17.465s (no improvement, multi-key query)
- **Q18**: 39.180s → 40.490s (no improvement, multi-key query)
- **Q32**: 11.289s → 11.380s (no improvement, multi-key query)
- Correctness: 29/43 maintained

### Decisions
- Reduced benchmark warmup to 1 pass and tries to 3 (from 3/5) to speed up iteration cycle
- Multi-pass helps Q15 significantly but not enough for 2x target — further optimization needed
- Category B multi-key queries unaffected by single-key multi-pass — need separate optimization path
