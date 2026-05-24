#!/usr/bin/env python3
"""
Generate a daily Apache Iceberg activity report.

Usage:
    python3 generate_daily_report.py [DATE]

    DATE: YYYY-MM-DD format (defaults to yesterday)

Output:
    reports/YYYY-MM-DD.md
"""

import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

UPSTREAM_REPO = "apache/iceberg"
GITHUB_API = "https://api.github.com"
REPORT_DIR = Path(__file__).parent.parent / "reports"


def gh_api(path: str, token: str = "") -> dict | list:
    url = f"{GITHUB_API}{path}"
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        print(f"[WARN] GitHub API {url} → HTTP {e.code}", file=sys.stderr)
        return []
    except URLError as e:
        print(f"[WARN] GitHub API {url} → {e.reason}", file=sys.stderr)
        return []


def get_merged_prs(report_date: date, token: str) -> list[dict]:
    """Fetch PRs merged on report_date from apache/iceberg."""
    since = f"{report_date}T00:00:00Z"
    until = f"{report_date + timedelta(days=1)}T00:00:00Z"
    results = []
    page = 1
    while True:
        prs = gh_api(
            f"/repos/{UPSTREAM_REPO}/pulls?state=closed&sort=updated"
            f"&direction=desc&per_page=100&page={page}",
            token,
        )
        if not prs:
            break
        found_any = False
        for pr in prs:
            merged_at = pr.get("merged_at") or ""
            if since <= merged_at < until:
                found_any = True
                results.append(pr)
            elif merged_at and merged_at < since:
                return results
        if not found_any and len(prs) < 100:
            break
        page += 1
    return results


def get_new_issues(report_date: date, token: str) -> list[dict]:
    """Fetch issues (not PRs) created on report_date."""
    since = f"{report_date}T00:00:00Z"
    until = f"{report_date + timedelta(days=1)}T00:00:00Z"
    items = gh_api(
        f"/repos/{UPSTREAM_REPO}/issues?state=all&sort=created"
        f"&direction=desc&since={since}&per_page=100",
        token,
    )
    return [
        i for i in items
        if "pull_request" not in i and since <= i.get("created_at", "") < until
    ]


def get_new_prs(report_date: date, token: str) -> list[dict]:
    """Fetch PRs opened on report_date."""
    since = f"{report_date}T00:00:00Z"
    until = f"{report_date + timedelta(days=1)}T00:00:00Z"
    items = gh_api(
        f"/repos/{UPSTREAM_REPO}/pulls?state=all&sort=created"
        f"&direction=desc&per_page=100",
        token,
    )
    return [i for i in items if since <= i.get("created_at", "") < until]


def get_pr_files(pr_number: int, token: str) -> list[dict]:
    return gh_api(f"/repos/{UPSTREAM_REPO}/pulls/{pr_number}/files?per_page=100", token)


def classify_pr(pr: dict) -> str:
    title = pr.get("title", "").lower()
    labels = [lb["name"].lower() for lb in pr.get("labels", [])]
    for kw, cat in [
        ("spec", "📋 规范 (Spec)"),
        ("core", "🔧 核心 (Core)"),
        ("spark", "⚡ Spark"),
        ("flink", "🌊 Flink"),
        ("hive", "🐝 Hive"),
        ("kafka", "📨 Kafka Connect"),
        ("aws", "☁️ AWS"),
        ("azure", "☁️ Azure"),
        ("gcp", "☁️ GCP"),
        ("api", "🔌 API"),
        ("ci", "🤖 CI/CD"),
        ("doc", "📖 文档"),
        ("test", "🧪 测试"),
    ]:
        if kw in title or any(kw in lb for lb in labels):
            return cat
    return "🔨 其他"


def format_body(body: str | None, max_len: int = 400) -> str:
    if not body:
        return "_（无描述）_"
    body = body.strip()
    body = re.sub(r"\r\n", "\n", body)
    body = re.sub(r"<!--.*?-->", "", body, flags=re.DOTALL).strip()
    if len(body) > max_len:
        body = body[:max_len].rsplit(" ", 1)[0] + " …"
    return body


def files_summary(files: list[dict]) -> str:
    if not files:
        return ""
    rows = []
    for f in files[:20]:
        adds = f.get("additions", 0)
        dels = f.get("deletions", 0)
        rows.append(f"| `{f['filename']}` | +{adds} | -{dels} |")
    header = "| 文件 | 新增 | 删除 |\n|:-----|-----:|-----:|"
    if len(files) > 20:
        rows.append(f"| _…以及 {len(files) - 20} 个其他文件_ | | |")
    return header + "\n" + "\n".join(rows)


def label_badges(pr: dict) -> str:
    labels = pr.get("labels", [])
    if not labels:
        return ""
    return " ".join(f"`{lb['name']}`" for lb in labels)


def render_pr_section(pr: dict, files: list[dict], idx: int) -> str:
    num = pr["number"]
    title = pr["title"]
    author = pr.get("user", {}).get("login", "unknown")
    merged_at = pr.get("merged_at", "")[:19].replace("T", " ") + " UTC"
    url = pr.get("html_url", f"https://github.com/{UPSTREAM_REPO}/pull/{num}")
    category = classify_pr(pr)
    body = format_body(pr.get("body"))
    labels = label_badges(pr)
    file_table = files_summary(files)

    section = f"""
### {idx}. [{title}](#{num}) — PR #{num}

| 属性 | 详情 |
|:-----|:-----|
| **PR 编号** | [#{num}]({url}) |
| **作者** | [@{author}](https://github.com/{author}) |
| **合并时间** | {merged_at} |
| **类别** | {category} |
| **标签** | {labels or "无"} |

**描述摘要：**

{body}
"""
    if file_table:
        section += f"\n**变更文件 ({len(files)} 个)：**\n\n{file_table}\n"

    return section


def render_issue_row(issue: dict) -> str:
    num = issue["number"]
    title = issue["title"]
    author = issue.get("user", {}).get("login", "unknown")
    created = issue.get("created_at", "")[:10]
    url = issue.get("html_url", f"https://github.com/{UPSTREAM_REPO}/issues/{num}")
    labels = " ".join(f"`{lb['name']}`" for lb in issue.get("labels", []))
    return f"| [#{num}]({url}) | {title} | @{author} | {labels or '无'} | {created} |"


def render_new_pr_row(pr: dict) -> str:
    num = pr["number"]
    title = pr["title"]
    author = pr.get("user", {}).get("login", "unknown")
    created = pr.get("created_at", "")[:10]
    url = pr.get("html_url", f"https://github.com/{UPSTREAM_REPO}/pull/{num}")
    draft = " 🚧" if pr.get("draft") else ""
    category = classify_pr(pr)
    return f"| [#{num}]({url}) | {title}{draft} | @{author} | {category} | {created} |"


def generate_report(report_date: date, token: str) -> str:
    print(f"[INFO] Fetching data for {report_date} …")

    merged_prs = get_merged_prs(report_date, token)
    new_issues = get_new_issues(report_date, token)
    new_prs = get_new_prs(report_date, token)

    print(f"[INFO] merged_prs={len(merged_prs)}, new_issues={len(new_issues)}, new_prs={len(new_prs)}")

    contributors = set()
    for pr in merged_prs:
        if pr.get("user"):
            contributors.add(pr["user"]["login"])

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    date_str = str(report_date)

    lines = [
        f"# Apache Iceberg 社区日报 — {date_str}",
        "",
        f"> **数据来源**: [apache/iceberg](https://github.com/{UPSTREAM_REPO})  ",
        f"> **统计周期**: {date_str} 00:00 UTC — {date_str} 23:59 UTC  ",
        f"> **生成时间**: {now_str}",
        "",
        "---",
        "",
        "## 一、今日活动概览",
        "",
        "| 指标 | 数量 |",
        "|:-----|-----:|",
        f"| 🔀 合并的 PR | **{len(merged_prs)}** |",
        f"| 🐛 新增 Issue | **{len(new_issues)}** |",
        f"| 📬 新开 PR | **{len(new_prs)}** |",
        f"| 👥 贡献者 | **{len(contributors)}** |",
        "",
        "---",
        "",
    ]

    # ── Merged PRs ────────────────────────────────────────────────
    lines.append("## 二、合并 PR 详细分析")
    lines.append("")
    if not merged_prs:
        lines.append("_今日无合并的 PR。_")
        lines.append("")
    else:
        for idx, pr in enumerate(merged_prs, 1):
            files = get_pr_files(pr["number"], token)
            lines.append(render_pr_section(pr, files, idx))
            lines.append("")

    lines.append("---")
    lines.append("")

    # ── New Issues ────────────────────────────────────────────────
    lines.append("## 三、新增 Issue")
    lines.append("")
    if not new_issues:
        lines.append("_今日无新增 Issue。_")
    else:
        lines.append(f"共新增 **{len(new_issues)}** 个 Issue：")
        lines.append("")
        lines.append("| 编号 | 标题 | 作者 | 标签 | 创建时间 |")
        lines.append("|:-----|:-----|:-----|:-----|:---------|")
        for issue in new_issues:
            lines.append(render_issue_row(issue))
    lines.append("")
    lines.append("---")
    lines.append("")

    # ── New PRs ───────────────────────────────────────────────────
    lines.append("## 四、新开 PR")
    lines.append("")
    if not new_prs:
        lines.append("_今日无新开 PR。_")
    else:
        lines.append(f"共新开 **{len(new_prs)}** 个 PR：")
        lines.append("")
        lines.append("| 编号 | 标题 | 作者 | 类别 | 创建时间 |")
        lines.append("|:-----|:-----|:-----|:-----|:---------|")
        for pr in new_prs:
            lines.append(render_new_pr_row(pr))
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(
        "_本报告由 [daily-sync-analysis](/.github/workflows/daily-sync-analysis.yml) 自动生成_"
    )

    return "\n".join(lines)


def main():
    report_date_str = sys.argv[1] if len(sys.argv) > 1 else str(date.today() - timedelta(days=1))
    try:
        report_date = date.fromisoformat(report_date_str)
    except ValueError:
        print(f"[ERROR] Invalid date: {report_date_str}. Use YYYY-MM-DD.", file=sys.stderr)
        sys.exit(1)

    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        print("[WARN] GITHUB_TOKEN not set — unauthenticated API (60 req/hr limit)", file=sys.stderr)

    content = generate_report(report_date, token)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORT_DIR / f"{report_date}.md"
    out_path.write_text(content, encoding="utf-8")
    print(f"[INFO] Report written → {out_path}")


if __name__ == "__main__":
    main()
