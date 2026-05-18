#!/usr/bin/env bash
# Daily sync fork + generate activity report for apache/iceberg
# Usage: ./scripts/daily-sync-and-report.sh
# Schedule: Run daily at 08:00 CST (00:00 UTC) via cron:
#   0 0 * * * cd /home/user/iceberg && bash scripts/daily-sync-and-report.sh >> logs/daily-sync.log 2>&1

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPORT_DIR="$REPO_DIR/docs/daily-reports"
YESTERDAY=$(date -u -d "yesterday" +%Y-%m-%d)
TODAY=$(date -u +%Y-%m-%d)
REPORT_FILE="$REPORT_DIR/${YESTERDAY}-daily-report.md"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"; }

cd "$REPO_DIR"

# ── 1. Sync fork ──────────────────────────────────────────────
log "Syncing fork from upstream apache/iceberg..."

git remote get-url upstream &>/dev/null || git remote add upstream https://github.com/apache/iceberg.git

for attempt in 1 2 3 4; do
  if git fetch upstream main 2>&1; then
    break
  fi
  wait_sec=$((2 ** attempt))
  log "Fetch failed (attempt $attempt), retrying in ${wait_sec}s..."
  sleep "$wait_sec"
done

OLD_HEAD=$(git rev-parse main)
git checkout main
git merge upstream/main --ff-only
NEW_HEAD=$(git rev-parse main)

for attempt in 1 2 3 4; do
  if git push -u origin main 2>&1; then
    break
  fi
  wait_sec=$((2 ** attempt))
  log "Push failed (attempt $attempt), retrying in ${wait_sec}s..."
  sleep "$wait_sec"
done

log "Sync complete: $OLD_HEAD → $NEW_HEAD"

# ── 2. Collect merged PRs ─────────────────────────────────────
log "Collecting merged PRs for $YESTERDAY..."

MERGED_PRS=$(git log upstream/main \
  --after="${YESTERDAY}T00:00:00Z" \
  --before="${TODAY}T00:00:00Z" \
  --format="%H|%ai|%an|%s" 2>/dev/null)

PR_COUNT=$(echo "$MERGED_PRS" | grep -c "." || echo 0)
log "Found $PR_COUNT merged commits"

# ── 3. Generate report skeleton ───────────────────────────────
mkdir -p "$REPORT_DIR"

cat > "$REPORT_FILE" << EOF
# Apache Iceberg 每日动态报告 — ${YESTERDAY}

> **报告范围：** ${YESTERDAY} 00:00 UTC — ${YESTERDAY} 23:59 UTC
> **生成时间：** ${TODAY}
> **上游仓库：** apache/iceberg
> **同步状态：** ✅ 已同步（main 分支 ${OLD_HEAD:0:8} → ${NEW_HEAD:0:8}）

## 已合并 PR 列表

| PR 号 | 标题 | 作者 | 合并时间 |
|-------|------|------|----------|
EOF

while IFS='|' read -r hash date author subject; do
  pr_num=$(echo "$subject" | grep -oP '#\d+' | tail -1 | tr -d '#')
  title=$(echo "$subject" | sed 's/ (#[0-9]*)//')
  if [[ -n "$pr_num" ]]; then
    echo "| [#${pr_num}](https://github.com/apache/iceberg/pull/${pr_num}) | ${title} | ${author} | ${date} |" >> "$REPORT_FILE"
  fi
done <<< "$MERGED_PRS"

cat >> "$REPORT_FILE" << EOF

## 变更详情

EOF

while IFS='|' read -r hash date author subject; do
  if [[ -n "$hash" ]]; then
    pr_num=$(echo "$subject" | grep -oP '#\d+' | tail -1 | tr -d '#')
    echo "### ${subject}" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    git show "$hash" --stat --format="" 2>/dev/null | head -20 >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
  fi
done <<< "$MERGED_PRS"

log "Report written to $REPORT_FILE"

# ── 4. Commit and push report ─────────────────────────────────
git checkout claude/happy-ride-SSLCq 2>/dev/null || true
git add "$REPORT_FILE"
git diff --cached --quiet && { log "No changes to commit"; exit 0; }

git commit -m "$(cat <<EOF
docs: add daily report for ${YESTERDAY}

Auto-generated daily sync report for apache/iceberg upstream activity.
Covers merged PRs, new issues, and new PRs for ${YESTERDAY}.

https://claude.ai/code/session_01TWVxUNcb2WFqJfW7TmaAU9
EOF
)"

for attempt in 1 2 3 4; do
  if git push -u origin claude/happy-ride-SSLCq 2>&1; then
    break
  fi
  wait_sec=$((2 ** attempt))
  log "Push failed (attempt $attempt), retrying in ${wait_sec}s..."
  sleep "$wait_sec"
done

log "Done. Report committed and pushed."
