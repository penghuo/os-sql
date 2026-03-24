#!/usr/bin/env bash
set -uo pipefail

RALPH_DIR="$(cd "$(dirname "$0")" && pwd)"
MAX_ITERATIONS="${RALPH_MAX_ITERATIONS:-50}"
MAX_REJECTIONS=3

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

get_status() {
  head -1 "$RALPH_DIR/STATUS.md" 2>/dev/null | sed 's/^status: *//; s/[[:space:]]*$//'
}

echo "Starting ralph loop (max $MAX_ITERATIONS iterations)..."
echo "Monitor: tail -f $RALPH_DIR/LOGS.md"
echo "Status:  cat $RALPH_DIR/STATUS.md"
echo "---"

iteration=0
consecutive_rejections=0

while [ "$iteration" -lt "$MAX_ITERATIONS" ]; do
  iteration=$((iteration + 1))
  echo "=== Iteration $iteration/$MAX_ITERATIONS ($(date)) ==="

  # --- Sisyphus iteration ---
  if kiro-cli chat --agent sisyphus --no-interactive -a "$(cat "$RALPH_DIR/PROMPT.md")"; then
    echo "--- Sisyphus completed iteration $iteration ---"
  else
    rc=$?
    echo "--- Sisyphus crashed (exit $rc), restarting in 5s ---"
    sleep 5
    continue
  fi

  # --- Check status ---
  STATUS=$(get_status)

  if [ -z "$STATUS" ]; then
    echo "⚠️  STATUS.md missing or malformed after iteration $iteration. Continuing..."
  fi

  if [ "$STATUS" = "COMPLETE" ]; then
    echo "=== Sisyphus says COMPLETE. Oracle review... ==="

    ORACLE_PROMPT="Review the work for the task in .ralph/PROMPT.md.
Read .ralph/STATUS.md for final state and evidence.
Read the tail of .ralph/LOGS.md for what was done.
Run git log --oneline -10 and git diff main to see actual changes.

Verify:
1. Are ALL success criteria from PROMPT.md met?
2. Is there concrete evidence (test output, benchmarks, etc.)?
3. Are there regressions or broken patterns?

Output exactly one of:
- APPROVED — all criteria met with evidence
- REJECTED: <reason> — what's missing or wrong"

    oracle_output=$(kiro-cli chat --agent oracle --no-interactive -a "$ORACLE_PROMPT" 2>&1) || true

    if echo "$oracle_output" | grep -qE '^-?\s*APPROVED'; then
      echo "✅ Oracle APPROVED. Done."
      exit 0
    else
      echo "❌ Oracle REJECTED. Continuing..."
      rejection_reason=$(echo "$oracle_output" | grep -A 50 "REJECTED" | head -20)
      # Append rejection to LOGS.md
      {
        echo ""
        echo "## Oracle Review — Iteration $iteration"
        echo ""
        echo "**REJECTED**"
        echo "$rejection_reason"
      } >> "$RALPH_DIR/LOGS.md"
      # Update STATUS.md to reflect rejection
      tmp_file="$RALPH_DIR/STATUS.md.tmp.$$"
      sed 's/^status: COMPLETE/status: WORKING/' "$RALPH_DIR/STATUS.md" > "$tmp_file"
      mv "$tmp_file" "$RALPH_DIR/STATUS.md"
      # Append oracle feedback to STATUS.md so sisyphus sees it
      {
        echo ""
        echo "## Oracle Rejection"
        echo "$rejection_reason"
      } >> "$RALPH_DIR/STATUS.md"
      consecutive_rejections=$((consecutive_rejections + 1))

      if [ "$consecutive_rejections" -ge "$MAX_REJECTIONS" ]; then
        echo "🛑 $MAX_REJECTIONS consecutive rejections — stopping."
        break
      fi
    fi
  else
    # WORKING — reset rejection counter on productive iterations
    consecutive_rejections=0
  fi

  sleep 2
done

if [ "$iteration" -ge "$MAX_ITERATIONS" ]; then
  echo "🛑 Max iterations ($MAX_ITERATIONS) reached."
fi

echo "--- Ralph loop finished. See .ralph/STATUS.md for final state. ---"
