#!/usr/bin/env bash
# run_check.sh — run arch-guard locally against this repo.
#
# Usage:
#   ./run_check.sh                  # checks all tracked files
#   ./run_check.sh <base> <head>    # checks only files changed between two commits
#
# Output:
#   findings.sarif     — upload to GitHub via codeql-action/upload-sarif
#   summary.md         — the job summary that would appear in GitHub Actions
#   stdout             — one line per finding

set -euo pipefail

ARCH_GUARD_DIR="$(cd "$(dirname "$0")/../arch-guard" && pwd)"
SARIF_OUT="findings.sarif"
SUMMARY_OUT="summary.md"

if [ ! -d "$ARCH_GUARD_DIR/arch_guard" ]; then
  echo "ERROR: arch-guard not found at $ARCH_GUARD_DIR"
  echo "Clone it alongside this repo: git clone <arch-guard-url> ../arch-guard"
  exit 1
fi

# If no args, diff all tracked files against the empty tree (checks everything)
if [ $# -eq 0 ]; then
  BASE=$(git hash-object -t tree /dev/null)
  HEAD=$(git rev-parse HEAD 2>/dev/null || echo "HEAD")
else
  BASE="$1"
  HEAD="$2"
fi

echo "arch-guard: checking $(git diff --name-only "$BASE".."$HEAD" 2>/dev/null | wc -l | tr -d ' ') changed file(s)..."
echo ""

rm -f "$SARIF_OUT" "$SUMMARY_OUT"

PYTHONPATH="$ARCH_GUARD_DIR" python3 -m arch_guard.check \
  --contract arch-contract.yaml \
  --diff-base "$BASE" \
  --diff-head "$HEAD" \
  --sarif-out "$SARIF_OUT" \
  --summary-out "$SUMMARY_OUT" \
  --advisory

echo ""
echo "── Output files ──────────────────────────────────"
echo "  $SARIF_OUT     upload to GitHub → inline PR annotations"
echo "  $SUMMARY_OUT   the job summary shown in the Actions tab"
echo ""
echo "── Summary ───────────────────────────────────────"
cat "$SUMMARY_OUT"
