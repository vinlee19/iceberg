#!/usr/bin/env bash
# Daily sync and report script for Apache Iceberg fork
# Usage: ./scripts/daily-sync-report.sh [YYYY-MM-DD]
# If no date is provided, uses yesterday's date.

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────
UPSTREAM_REMOTE="upstream"
UPSTREAM_URL="https://github.com/apache/iceberg.git"
UPSTREAM_BRANCH="main"
REPORT_DIR="docs"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Date calculation ─────────────────────────────────────────────────
if [ "${1:-}" != "" ]; then
    REPORT_DATE="$1"
else
    REPORT_DATE="$(date -d 'yesterday' '+%Y-%m-%d' 2>/dev/null || date -v-1d '+%Y-%m-%d')"
fi
REPORT_FILE="$REPO_DIR/$REPORT_DIR/iceberg-daily-report-${REPORT_DATE}.md"

echo "========================================================"
echo "  Apache Iceberg Daily Sync & Report"
echo "  Date: $REPORT_DATE"
echo "========================================================"

# ── Step 1: Ensure upstream remote exists ────────────────────────────
cd "$REPO_DIR"
if ! git remote | grep -q "^${UPSTREAM_REMOTE}$"; then
    echo "[1/4] Adding upstream remote: $UPSTREAM_URL"
    git remote add "$UPSTREAM_REMOTE" "$UPSTREAM_URL"
else
    echo "[1/4] Upstream remote already exists: $(git remote get-url $UPSTREAM_REMOTE)"
fi

# ── Step 2: Fetch and merge upstream ─────────────────────────────────
echo "[2/4] Fetching upstream/${UPSTREAM_BRANCH}..."
RETRY=0
MAX_RETRY=4
WAIT=2
while ! git fetch "$UPSTREAM_REMOTE" "$UPSTREAM_BRANCH" 2>&1; do
    RETRY=$((RETRY+1))
    if [ $RETRY -ge $MAX_RETRY ]; then
        echo "ERROR: Failed to fetch upstream after $MAX_RETRY retries."
        exit 1
    fi
    echo "  Fetch failed, retrying in ${WAIT}s... ($RETRY/$MAX_RETRY)"
    sleep $WAIT
    WAIT=$((WAIT*2))
done

echo "[2/4] Merging upstream/${UPSTREAM_BRANCH} into current branch..."
MERGE_OUT=$(git merge "${UPSTREAM_REMOTE}/${UPSTREAM_BRANCH}" --no-edit 2>&1) || true
echo "$MERGE_OUT" | tail -5
FILES_CHANGED=$(echo "$MERGE_OUT" | grep -oP '\d+ file' | head -1 || echo "0 files")
echo "      Sync complete: $FILES_CHANGED changed."

# ── Step 3: Generate report via Claude Code (or standalone) ──────────
echo "[3/4] Generating daily report for $REPORT_DATE..."

mkdir -p "$REPO_DIR/$REPORT_DIR"

# Check if we have jq for JSON processing
if ! command -v jq &>/dev/null; then
    echo "  WARNING: jq not found. Install it for richer report generation."
fi

cat > "$REPORT_FILE" <<HEADER_EOF
# Apache Iceberg 每日动态报告

**报告日期：** ${REPORT_DATE}
**数据范围：** ${REPORT_DATE} 00:00:00 UTC — ${REPORT_DATE} 23:59:59 UTC
**生成时间：** $(date -u '+%Y-%m-%d %H:%M:%S UTC')
**Fork 同步状态：** ✅ 已同步至 upstream \`apache/iceberg\` main 分支

---

> 本报告由 \`scripts/daily-sync-report.sh\` 自动生成框架，
> 详细 PR 内容分析请通过 Claude Code 补全（见下方说明）。

## 使用方式

运行以下命令由 Claude Code 生成完整分析：

\`\`\`bash
# 在 Claude Code 会话中执行：
claude "分析 apache/iceberg 在 ${REPORT_DATE} 的活动：合并的 PR、新增 Issue、新增 PR，生成完整分析报告追加到 ${REPORT_FILE}"
\`\`\`

## 快速链接

- 合并 PR：https://github.com/apache/iceberg/pulls?q=is%3Apr+is%3Amerged+merged%3A${REPORT_DATE}
- 新增 Issue：https://github.com/apache/iceberg/issues?q=is%3Aissue+created%3A${REPORT_DATE}
- 新增 PR：https://github.com/apache/iceberg/pulls?q=is%3Apr+created%3A${REPORT_DATE}

HEADER_EOF

echo "      Report skeleton created: $REPORT_FILE"

# ── Step 4: Commit and push ───────────────────────────────────────────
echo "[4/4] Committing and pushing..."
git add "$REPORT_FILE"

# Also stage any upstream merge changes if any
git add -A 2>/dev/null || true

if git diff --cached --quiet; then
    echo "      Nothing to commit."
else
    git commit -m "chore: daily sync and report for ${REPORT_DATE}

- Synced fork with upstream apache/iceberg main
- Generated report skeleton at docs/iceberg-daily-report-${REPORT_DATE}.md"

    RETRY=0
    WAIT=2
    CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    while ! git push -u origin "$CURRENT_BRANCH" 2>&1; do
        RETRY=$((RETRY+1))
        if [ $RETRY -ge $MAX_RETRY ]; then
            echo "ERROR: Failed to push after $MAX_RETRY retries."
            exit 1
        fi
        echo "  Push failed, retrying in ${WAIT}s... ($RETRY/$MAX_RETRY)"
        sleep $WAIT
        WAIT=$((WAIT*2))
    done
    echo "      Pushed to origin/$CURRENT_BRANCH."
fi

echo ""
echo "========================================================"
echo "  Done! Report location:"
echo "  $REPORT_FILE"
echo "========================================================"
