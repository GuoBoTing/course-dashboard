#!/usr/bin/env python3
"""
Course Scraper（省 credit 版）
─────────────────────────────────────────────────────────────────
省 credit 策略：
  1. 課程清單（名稱 + URL）快取在 data/course_list.json，
     只有加 --discover 參數時才重新爬列表頁（每次消耗 ~2 credits）。
  2. Hahow 更新：用 LLM 從列表頁一次提取全部學生數（1 request），
     只有募資課（LLM 回傳 null）才進內頁。
  3. PressPlay 仍逐一爬個別頁（markdown + regex）。

使用方式：
  python scraper.py              # 只更新學生數（省 credit）
  python scraper.py --discover   # 重新爬列表頁取得課程清單（較貴）
"""
import os
import re
import sys
import json
import argparse
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional
from supabase import create_client
from firecrawl import FirecrawlApp
from firecrawl.v2.types import JsonFormat

load_dotenv()

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY")
COURSE_LIST_FILE  = Path("data/course_list.json")

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("錯誤：請在 .env 中設定 SUPABASE_URL 和 SUPABASE_KEY")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Schema ────────────────────────────────────────────────────────────────────

class CourseLink(BaseModel):
    course_name: str
    teacher: str
    price: Optional[float] = None
    url: str

class CourseLinkPage(BaseModel):
    courses: List[CourseLink]

# ── 平台設定 ──────────────────────────────────────────────────────────────────

PLATFORMS = {
    "hahow": {
        "list_url": "https://hahow.in/courses?page=1&sort=TRENDING",
        "list_prompt": (
            "This page shows Hahow trending courses written in Traditional Chinese. "
            "The page may have a '近期熱門' (recently trending) featured section at the top — SKIP IT. "
            "Only extract courses from the '全部結果' (all results) section. "
            "IMPORTANT: Only extract courses that actually appear on this page. "
            "Do NOT invent or guess any course names. "
            "If you cannot find any courses in the content, return an empty list. "
            "For each course extract: "
            "course_name (the exact Traditional Chinese title), "
            "teacher (instructor name in Chinese), "
            "price (NTD as plain number, 0 if free), "
            "url (full absolute URL, e.g. https://hahow.in/courses/slug)."
        ),
        "expect_chinese": True,
        # 內頁 regex（re.DOTALL；非貪婪跳過章節數等小數字）
        # Hahow 頁面學生數無千分位，使用 4 位以上純數字即可
        "student_patterns": [
            r"課程總人數.{0,100}?(\d{4,})",   # 一般課（≥1000 人）
            r"當前購買數.{0,100}?(\d{4,})",   # 預購課（≥1000 人）
        ],
    },
    "pressplay": {
        "list_url": "https://www.pressplay.cc/project",
        "list_prompt": (
            "This is the PressPlay project listing page in Traditional Chinese. "
            "IMPORTANT: Only extract projects that actually appear on this page. "
            "Do NOT invent or guess any project names. "
            "If you cannot find any projects in the content, return an empty list. "
            "For each project extract: "
            "course_name (the exact project title in Chinese), "
            "teacher (creator name), "
            "price (lowest subscription price in NTD as plain number, 0 if free), "
            "url (full absolute URL to that project's detail page)."
        ),
        "expect_chinese": True,
        # 匹配：「5,979 人學習」、「123 人預購」，排除「追蹤」
        "student_patterns": [
            r"([\d,]+)\s*人學習",
            r"([\d,]+)\s*人預購",
        ],
    },
}

# ── 工具函式 ──────────────────────────────────────────────────────────────────

def has_chinese(text: str) -> bool:
    return bool(re.search(r'[\u4e00-\u9fff]', text))

def parse_int(s: str) -> int:
    """把 '3,210' 之類的字串轉成整數。"""
    return int(s.replace(",", "").replace("，", ""))

def extract_students_from_markdown(md: str, patterns: list[str]) -> int | None:
    """用 regex 從 markdown 取出學生數（re.DOTALL 讓 . 跨行）。"""
    for pat in patterns:
        m = re.search(pat, md, re.DOTALL)
        if m:
            try:
                return parse_int(m.group(1))
            except ValueError:
                continue
    return None

# ── Discover 模式：爬列表頁取得課程清單（消耗 LLM credits）───────────────────

def discover_courses(app: FirecrawlApp) -> dict[str, list[dict]]:
    """爬各平台列表頁，回傳 {platform: [course_dict, ...]}。"""
    result: dict[str, list[dict]] = {}

    for platform, config in PLATFORMS.items():
        print(f"\n  [{platform}] 發現模式：爬列表頁 → {config['list_url']}")
        try:
            res = app.scrape(
                url=config["list_url"],
                formats=[
                    "markdown",
                    JsonFormat(
                        type="json",
                        prompt=config["list_prompt"],
                        schema=CourseLinkPage.model_json_schema(),
                    ),
                ],
                wait_for=8000,
                proxy="stealth",
            )
        except Exception as exc:
            print(f"  [{platform}] ✗ 列表頁爬取失敗：{exc}")
            result[platform] = []
            continue

        md = res.markdown or ""
        if md:
            print(f"  [{platform}] markdown 前 300 字：\n{md[:300]}\n  ---")
        else:
            print(f"  [{platform}] ⚠ markdown 為空")

        data = res.json
        if not data:
            print(f"  [{platform}] ✗ 無結構化資料")
            result[platform] = []
            continue

        courses = data.get("courses", []) if isinstance(data, dict) else []

        # 防幻覺：過濾無中文課程名
        if config.get("expect_chinese"):
            before = len(courses)
            courses = [c for c in courses if has_chinese(c.get("course_name", ""))]
            if before - len(courses):
                print(f"  [{platform}] ⚠ 過濾 {before - len(courses)} 筆幻覺課程")

        # 過濾 /services/ 頁面（工作坊/服務，無學生數欄位）
        before_svc = len(courses)
        courses = [c for c in courses if "/services/" not in c.get("url", "")]
        if before_svc - len(courses):
            print(f"  [{platform}] ⚠ 過濾 {before_svc - len(courses)} 筆服務/工作坊頁面")

        courses = courses[:20]
        print(f"  [{platform}] ✓ 發現 {len(courses)} 門課程")
        result[platform] = courses

    return result

# ── Update 模式 ───────────────────────────────────────────────────────────────

def update_student_counts(app: FirecrawlApp, course_list: dict[str, list[dict]]) -> list[dict]:
    """所有課程（Hahow & PressPlay）逐一進個別頁，用 markdown + regex 取學生數。"""
    rows: list[dict] = []
    scraped_at = datetime.now().isoformat(timespec="seconds")

    for platform, courses in course_list.items():
        config = PLATFORMS.get(platform, {})
        patterns = config.get("student_patterns", [])
        print(f"\n  [{platform}] 更新學生數（{len(courses)} 門）…")

        for rank, course in enumerate(courses, start=1):
            url = course.get("url", "").strip()
            name = course.get("course_name", f"課程{rank}")
            print(f"  [{rank}/{len(courses)}] {name}")

            students = None
            if url and url.startswith("http"):
                try:
                    res = app.scrape(
                        url=url,
                        formats=["markdown"],
                        wait_for=5000,
                    )
                    md = res.markdown or ""
                    students = extract_students_from_markdown(md, patterns)
                    print(f"    學生數：{students}")
                    if students is None:
                        preview = md[:400].replace("\n", " ")
                        print(f"    ⚠ 無法匹配，markdown 前 400 字：{preview}")
                except Exception as exc:
                    print(f"    ✗ 爬取失敗：{exc}")
            else:
                print(f"    ⚠ 無效 URL，跳過")

            rows.append({
                "platform":    platform,
                "rank":        rank,
                "course_name": course.get("course_name", ""),
                "teacher":     course.get("teacher", ""),
                "price":       course.get("price"),
                "students":    students,
                "course_url":  url,
                "scraped_at":  scraped_at,
            })

    return rows

# ── 主程式 ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--discover", action="store_true",
                        help="重新爬列表頁取得課程清單（會消耗較多 credits）")
    args = parser.parse_args()

    if not FIRECRAWL_API_KEY:
        print("錯誤：請在 .env 中設定 FIRECRAWL_API_KEY")
        sys.exit(1)

    COURSE_LIST_FILE.parent.mkdir(parents=True, exist_ok=True)
    sb  = get_supabase()
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)

    print(f"\n=== 開始爬取 {datetime.now().isoformat(timespec='seconds')} ===")

    # 取得課程清單
    if args.discover or not COURSE_LIST_FILE.exists():
        print("\n【發現模式】爬列表頁取得課程清單…")
        course_list = discover_courses(app)
        course_list = {k: v for k, v in course_list.items() if v}
        if not course_list:
            print("✗ 無法取得任何課程清單，程式結束。")
            sys.exit(1)
        COURSE_LIST_FILE.write_text(
            json.dumps(course_list, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"✓ 課程清單已存至 {COURSE_LIST_FILE}")
    else:
        course_list = json.loads(COURSE_LIST_FILE.read_text(encoding="utf-8"))
        total = sum(len(v) for v in course_list.values())
        print(f"\n【更新模式】使用快取清單（{total} 門課），只更新學生數")
        print(f"  提示：加 --discover 參數可重新發現新課程")

    # 更新學生數
    rows = update_student_counts(app, course_list)

    if not rows:
        print("\n✗ 未取得任何資料。")
        sys.exit(1)

    # 寫入 Supabase（None 值轉成 null）
    clean_rows = [
        {k: (None if v != v or v is None else v) for k, v in row.items()}
        for row in rows
    ]
    sb.table("course_scrapes").insert(clean_rows).execute()
    print(f"\n✓ 儲存 {len(rows)} 筆新資料 → Supabase")
    print(f"=== 爬取完成 ===\n")


if __name__ == "__main__":
    main()
