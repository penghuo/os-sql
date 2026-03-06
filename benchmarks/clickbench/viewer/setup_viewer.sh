#!/usr/bin/env bash
# setup_viewer.sh — Clone ClickBench website and inject our results.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VIEWER_DIR="$SCRIPT_DIR/clickbench-website"

if [ -d "$VIEWER_DIR" ]; then
    echo "Viewer already cloned at $VIEWER_DIR"
else
    echo "Cloning ClickBench website..."
    git clone --depth 1 https://github.com/ClickHouse/ClickBench.git "$VIEWER_DIR"
fi

# Copy our results into the website's expected locations
echo "Copying results..."
mkdir -p "$VIEWER_DIR/opensearch-trino-sql/results"
cp "$BENCH_DIR"/results/opensearch/*.json "$VIEWER_DIR/opensearch-trino-sql/results/" 2>/dev/null || echo "  No OpenSearch results yet"

mkdir -p "$VIEWER_DIR/clickhouse/results"
cp "$BENCH_DIR"/results/clickhouse/*.json "$VIEWER_DIR/clickhouse/results/" 2>/dev/null || echo "  No ClickHouse results yet"

echo ""
echo "To view results:"
echo "  cd $VIEWER_DIR/website"
echo "  python3 -m http.server 8888"
echo "  Then open http://localhost:8888 in your browser"
echo ""
echo "Note: You may need to add 'opensearch-trino-sql' to the website's"
echo "hardware.json or results index for it to appear in the UI."
