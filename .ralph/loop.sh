#!/usr/bin/env bash
set -uo pipefail

RALPH_DIR="$(cd "$(dirname "$0")" && pwd)"
MAX_ITERATIONS="${RALPH_MAX_ITERATIONS:-50}"
MAX_REJECTIONS="${RALPH_MAX_REJECTIONS:-3}"
MAX_REPLANS="${RALPH_MAX_REPLANS:-3}"

while [[ $# -gt 0 ]]; do
  case $1 in
    --max-iterations) MAX_ITERATIONS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [ ! -f "$RALPH_DIR/PROMPT.md" ]; then
  echo "Error: $RALPH_DIR/PROMPT.md not found." >&2
  exit 1
fi

# --- Helpers ---

get_plan_count() {
  # Highest N from existing TASKS_{N}.md files
  local highest=0
  for f in "$RALPH_DIR"/TASKS_*.md; do
    [ -f "$f" ] || continue
    n=$(basename "$f" | sed 's/TASKS_//; s/\.md//')
    if [ "$n" -gt "$highest" ] 2>/dev/null; then
      highest=$n
    fi
  done
  echo "$highest"
}

count_pending() {
  grep -c '^\- \[ \] Done' "$RALPH_DIR/TASKS_${1}.md" 2>/dev/null || echo 0
}

log_plan() {
  echo "$1"
  { echo ""; echo "$1"; } >> "$RALPH_DIR/LOGS_PLAN.md"
}

log_execute() {
  echo "$1"
  { echo ""; echo "$1"; } >> "$RALPH_DIR/LOGS_EXECUTE.md"
}

# Build a string listing all previous TASKS files for planner context
previous_tasks_context() {
  local n=$1
  local ctx=""
  local i=1
  while [ "$i" -lt "$n" ]; do
    ctx="${ctx}
- .ralph/TASKS_${i}.md — previous plan #${i}"
    i=$((i + 1))
  done
  echo "$ctx"
}

echo "=== Ralph loop started (max $MAX_ITERATIONS iterations, $MAX_REPLANS re-plans, $MAX_REJECTIONS rejections) ==="
echo "Monitor: tail -f $RALPH_DIR/LOGS_PLAN.md $RALPH_DIR/LOGS_EXECUTE.md"
echo "---"

iteration=0
plan_count=$(get_plan_count)
rejection_count=0

while true; do

  # --- Guard: max iterations ---
  if [ "$iteration" -ge "$MAX_ITERATIONS" ]; then
    echo "🛑 Max iterations ($MAX_ITERATIONS) reached."
    exit 1
  fi

  # ==========================================================
  # PHASE 1: PLAN
  # Planner reads PROMPT.md, TASKS_{1..N-1}.md, LOGS_PLAN.md, LOGS_EXECUTE.md, STATUS.md
  # Planner writes TASKS_{N}.md, EXEC_PROMPT_{N}.md, appends to LOGS_PLAN.md, appends to STATUS.md
  # ==========================================================
  plan_count=$((plan_count + 1))

  if [ "$plan_count" -gt "$MAX_REPLANS" ]; then
    echo "🛑 Max re-plans ($MAX_REPLANS) reached."
    exit 1
  fi

  echo "=== Phase 1: Planning (plan #$plan_count) ==="

  PREV_TASKS=$(previous_tasks_context "$plan_count")

  PLAN_PROMPT="You are the PLANNER for an autonomous coding loop.

Read these files for context:
- .ralph/PROMPT.md — the original task
- .ralph/LOGS_PLAN.md — history of planning decisions and rejections
- .ralph/LOGS_EXECUTE.md — history of execution attempts
- .ralph/STATUS.md — append-only timeline (if exists)${PREV_TASKS}

Your job: RESEARCH first, then PLAN.

Step 1 — RESEARCH & EXPLORE (delegate to subagents in parallel):
- Spawn explorer: find relevant code, patterns, dependencies, test structure in the codebase
- Spawn librarian: find docs, examples, best practices for the technologies involved
- Synthesize their findings before planning

Step 2 — PLAN (based on research findings):
- Design approaches informed by what you actually found in the code and docs
- Each task's Approaches should reference specific files, functions, or patterns discovered

Write/append these files:

### .ralph/TASKS_${plan_count}.md (create new file)
\`\`\`markdown
# Plan ${plan_count}

## Success Criteria
- [ ] Criterion 1 — [measurable, testable]
- [ ] Criterion 2 — ...

## Anti-Criteria
- [ ] No placeholder/stub implementations
- [ ] No TODO comments left behind
- [ ] No removed tests or logging

## Task 1: [title]
- [ ] Done

### Goal
[What this task achieves]

### Approaches
1. [Primary approach] — [why preferred]
2. [Alternative approach] — [fallback if primary fails]

### Verification
[How to verify — specific commands/tests to run]

### Dependencies
[Which tasks must complete first, or 'none']
\`\`\`

### .ralph/EXEC_PROMPT_${plan_count}.md (create new file)
This is the execution prompt that Phase 2 will feed directly to the executor agent.
It must contain:
- Your distilled context from BFS exploration (key findings, relevant code locations, architecture understanding)
- Specific guidance for the executor: which directions to explore, what approaches to take, what failed in previous plans (if any)
- A reference to .ralph/TASKS_${plan_count}.md as the authoritative task list
- Instructions for the executor to: mark checkboxes as done, append to .ralph/LOGS_EXECUTE.md, append to .ralph/STATUS.md

\`\`\`markdown
# Execution Prompt — Plan ${plan_count}

## Context
[Distilled findings from your BFS exploration — code locations, patterns, dependencies discovered]

## Guidance
[Specific directions for the executor — what to try, what to avoid, lessons from previous plans]

## Task Reference
Execute tasks in .ralph/TASKS_${plan_count}.md in order. For each task:
- Read the task's Goal, Approaches, and Verification sections
- Implement the task
- Mark \\\`- [ ] Done\\\` → \\\`- [x] Done\\\` in .ralph/TASKS_${plan_count}.md
- Append execution details to .ralph/LOGS_EXECUTE.md
- Append task status updates to .ralph/STATUS.md (do NOT overwrite — append only)
\`\`\`

### .ralph/STATUS.md (APPEND — do NOT overwrite)
Append a new section:
\`\`\`markdown

## Plan ${plan_count} — [date]
### Planner
Created N tasks. Focus: [summary].
[If re-planning: explain what the previous plan got wrong and how this plan differs]
\`\`\`

### .ralph/LOGS_PLAN.md (APPEND)
Append your planning rationale and decisions.

Order tasks by dependency (independent first). Each task should be completable in one iteration."

  kiro-cli chat --agent sisyphus --no-interactive -a "$PLAN_PROMPT"
  rc=$?

  if [ "$rc" -ne 0 ]; then
    log_plan "## Phase 1 crashed (exit $rc) on plan #$plan_count"
    echo "Error: Planner crashed." >&2
    exit 1
  fi

  if [ ! -f "$RALPH_DIR/TASKS_${plan_count}.md" ]; then
    log_plan "## Phase 1 did not create TASKS_${plan_count}.md on plan #$plan_count"
    echo "Error: TASKS_${plan_count}.md was not created." >&2
    exit 1
  fi

  if [ ! -f "$RALPH_DIR/EXEC_PROMPT_${plan_count}.md" ]; then
    log_plan "## Phase 1 did not create EXEC_PROMPT_${plan_count}.md on plan #$plan_count"
    echo "Error: EXEC_PROMPT_${plan_count}.md was not created." >&2
    exit 1
  fi

  task_count=$(grep -c '^## Task [0-9]' "$RALPH_DIR/TASKS_${plan_count}.md" 2>/dev/null || echo 0)
  log_plan "## Plan #$plan_count produced $task_count tasks — $(date)"

  # ==========================================================
  # PHASE 2: EXECUTE
  # Executor works through TASKS_{N}.md one task at a time
  # Executor marks - [ ] Done → - [x] Done in TASKS_{N}.md
  # Executor appends to LOGS_EXECUTE.md and STATUS.md
  # Exit condition: pending=0 (all tasks checked off)
  # ==========================================================
  echo "=== Phase 2: Execution ==="

  while [ "$iteration" -lt "$MAX_ITERATIONS" ]; do
    iteration=$((iteration + 1))

    # Check no pending tasks left
    pending=$(count_pending "$plan_count")
    if [ "$pending" -eq 0 ]; then
      log_execute "## All tasks done — entering oracle review (iteration $iteration)"
      break
    fi

    echo "=== Iteration $iteration/$MAX_ITERATIONS — $pending pending ($(date)) ==="

    kiro-cli chat --agent sisyphus --no-interactive -a "$(cat "$RALPH_DIR/EXEC_PROMPT_${plan_count}.md")"
    rc=$?

    if [ "$rc" -ne 0 ]; then
      echo "--- Executor crashed (exit $rc), retrying in 5s ---"
      sleep 5
      continue
    fi

    sleep 2
  done

  # --- Max iterations guard ---
  if [ "$iteration" -ge "$MAX_ITERATIONS" ]; then
    echo "🛑 Max iterations ($MAX_ITERATIONS) reached."
    exit 1
  fi

  # ========================================================
  # PHASE 3: ORACLE REVIEW
  # Always runs after Phase 2 exits (pending=0)
  # ========================================================
  echo "=== Phase 3: Oracle Review ==="

  ORACLE_PROMPT="You are a skeptical QA reviewer. Assume the work is incomplete until evidence convinces you.

## Context
Read .ralph/PROMPT.md, .ralph/TASKS_${plan_count}.md, .ralph/STATUS.md, tail of .ralph/LOGS_EXECUTE.md.
Run git log --oneline -10 and git diff main to see actual changes.

## Per-Criterion Evaluation
For EACH success criterion in TASKS_${plan_count}.md:
\`\`\`
Criterion: [exact text]
Rating: MET | PARTIALLY MET | NOT MET
Evidence: [specific output]
\`\`\`

## Auto-REJECT rules
- Any criterion NOT MET → REJECTED
- PARTIALLY MET without clear progress → REJECTED
- Self-reported evidence without verification output → REJECTED
- FAILED tasks without justification → REJECTED

## Final Verdict
Append your verdict to .ralph/STATUS.md (do NOT overwrite — append only):
\`\`\`markdown
### Oracle
APPROVED — all criteria met with evidence
\`\`\`
or:
\`\`\`markdown
### Oracle
REJECTED: <specific unmet criteria and what to fix>
\`\`\`

Output exactly one line to stdout: APPROVED or REJECTED: <reason>"

  oracle_output=$(kiro-cli chat --agent oracle --no-interactive -a "$ORACLE_PROMPT" 2>&1) || true

  if echo "$oracle_output" | grep -qiE 'APPROVED'; then
    echo "✅ Oracle APPROVED. Done."
    exit 0
  fi

  # REJECTED
  rejection_count=$((rejection_count + 1))
  rejection_reason=$(echo "$oracle_output" | grep -iA 50 "REJECTED" | head -20)
  log_plan "## Oracle REJECTED plan #$plan_count (rejection #$rejection_count) — $(date)"
  { echo ""; echo "$rejection_reason"; } >> "$RALPH_DIR/LOGS_PLAN.md"
  echo "❌ Oracle REJECTED (#$rejection_count)."

  if [ "$rejection_count" -ge "$MAX_REJECTIONS" ]; then
    echo "🛑 Max rejections ($MAX_REJECTIONS) reached."
    exit 1
  fi

  # Do NOT reset STATUS.md — append-only. Just continue to next plan.
  continue

done
