#!/usr/bin/env python3
"""
Course Scraper（省 credit 版）
─────────────────────────────────────────────────────────────────
省 credit 策略：
  1. 課程清單（名稱 + URL）快取在 data/course_list.json，
     只有加 --discover 參數時才重新爬列表頁（每次消耗 ~2 credits）。
  2. Hahow discover：LLM 取課程名/老師/URL，同時 HTML 解析列表頁取得
     類型（gkOCkQ）與學生數（dvCJUj）；非「課程」項目直接略過。
  3. Hahow 更新：列表已有學生數 → 直接使用；無（預購課）→ 才進內頁。
  4. PressPlay 仍逐一爬個別頁（markdown + regex）。

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
    students: Optional[int] = None   # 從列表頁 HTML 取得（None 表示需進內頁）

class CourseLinkPage(BaseModel):
    courses: List[CourseLink]

# ── 平台設定 ──────────────────────────────────────────────────────────────────

PLATFORMS = {
    "hahow": {
        "list_urls": [
            "https://hahow.in/courses?page=1&sort=TRENDING",
            "https://hahow.in/courses?page=2&sort=TRENDING",
            "https://hahow.in/courses?page=3&sort=TRENDING",
        ],
        "max_courses": 50,
        "list_prompt": (
            "This page shows Hahow trending courses written in Traditional Chinese. "
            "The page may have a '近期熱門' (recently trending) featured section at the top — SKIP IT. "
            "Only extract courses from the '全部結果' (all results) section. "
            "IMPORTANT: Each listing on Hahow is labeled as either '課程' (course), '服務' (service), or '工作坊' (workshop). "
            "Only extract items labeled as '課程'. SKIP any item labeled '服務', '工作坊', or any label other than '課程'. "
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
        # Hahow 頁面學生數固定格式為「X 位同學」，直接匹配即可
        # 當前購買數（募資/預購課）：context 夠精確，允許任意位數
        "student_patterns": [
            r"(\d+)\s*位同學",         # 一般課：「147 位同學」、「7380 位同學」
            r"當前購買數.{0,100}?(\d+)",  # 預購課
        ],
    },
    "pressplay": {
        "list_urls": [
            "https://www.pressplay.cc/project",
            "https://www.pressplay.cc/project?page=2",
            "https://www.pressplay.cc/project?page=3",
        ],
        "max_courses": 50,
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

def parse_pressplay_listing_html(html: str) -> dict[str, dict]:
    """
    解析 PressPlay 列表頁 HTML。
    回傳 {"/project/xxx": {"is_funding": bool, "students": int|None}}
    - is_funding=True：集資課（列表只顯示達標%），必須進內頁用「人預購」抓數
    - students：非集資課若列表已有「人學習」則直接使用
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, dict] = {}
    seen: set[str] = set()

    # 找出集資課 URL（有 data-type="funding" 屬性的卡片）
    funding_paths: set[str] = set()
    for el in soup.find_all(attrs={"data-type": "funding"}):
        ancestor = el
        for _ in range(8):
            if ancestor is None:
                break
            lnk = ancestor.find("a", href=re.compile(r"/project/"))
            if lnk:
                funding_paths.add(lnk.get("href", "").split("?")[0].rstrip("/"))
                break
            ancestor = getattr(ancestor, "parent", None)

    # 逐一處理卡片連結
    for link in soup.find_all("a", href=re.compile(r"/project/")):
        raw_path = link.get("href", "").split("?")[0].rstrip("/")
        if not raw_path or raw_path in seen:
            continue
        seen.add(raw_path)

        is_funding = raw_path in funding_paths
        students = None

        if not is_funding:
            card_text = link.get_text(separator=" ", strip=True)
            m = re.search(r"([\d,]+)\s*人學習", card_text)
            if m:
                try:
                    students = parse_int(m.group(1))
                except ValueError:
                    pass

        result[raw_path] = {"is_funding": is_funding, "students": students}

    return result

def parse_hahow_listing_html(html: str) -> dict[str, dict]:
    """
    解析 Hahow 列表頁 HTML，透過 CSS class substring 找出每張課程卡片的類型與學生數。
    回傳 {"/courses/<slug>": {"type": "課程"|..., "students": int|None}}
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, dict] = {}

    # 每張課程卡片包含一個 class 含 "gkOCkQ" 的元素（顯示類型：課程/補給/服務…）
    for type_el in soup.find_all(class_=re.compile(r"gkOCkQ")):
        # 往上找包含 /courses/ 連結的祖先元素
        ancestor = type_el.parent
        link_el = None
        for _ in range(12):
            if ancestor is None:
                break
            link_el = ancestor.find("a", href=re.compile(r"/courses/\w"))
            if link_el:
                break
            ancestor = ancestor.parent

        if not link_el:
            continue

        url_path = link_el.get("href", "")         # e.g. "/courses/5a211b15..."
        course_type = type_el.get_text(strip=True)  # "課程" / "補給" / "服務" …

        # 在同一卡片中找學生數（class 含 "dvCJUj"）
        students = None
        if ancestor:
            student_el = ancestor.find(class_=re.compile(r"dvCJUj"))
            if student_el:
                text = student_el.get_text(strip=True)
                m = re.search(r"([\d,]+)", text)
                if m:
                    try:
                        students = parse_int(m.group(1))
                    except ValueError:
                        pass

        result[url_path] = {"type": course_type, "students": students}

    return result

# ── Discover 模式：爬列表頁取得課程清單（消耗 LLM credits）───────────────────

def discover_courses(app: FirecrawlApp) -> dict[str, list[dict]]:
    """爬各平台列表頁（支援多頁），回傳 {platform: [course_dict, ...]}。"""
    result: dict[str, list[dict]] = {}

    for platform, config in PLATFORMS.items():
        list_urls   = config.get("list_urls", [])
        max_courses = config.get("max_courses", 50)
        seen_urls: set[str] = set()
        all_courses: list[dict] = []

        for page_url in list_urls:
            if len(all_courses) >= max_courses:
                break
            print(f"\n  [{platform}] 爬列表頁 → {page_url}")
            try:
                formats = ["markdown", JsonFormat(
                    type="json",
                    prompt=config["list_prompt"],
                    schema=CourseLinkPage.model_json_schema(),
                )]
                if platform in ("hahow", "pressplay"):
                    formats = ["html"] + formats  # 額外取 HTML 供 CSS 選擇器解析
                res = app.scrape(
                    url=page_url,
                    formats=formats,
                    wait_for=8000,
                    proxy="stealth",
                )
            except Exception as exc:
                print(f"  [{platform}] ✗ 列表頁爬取失敗：{exc}")
                continue

            md = res.markdown or ""
            if not md:
                print(f"  [{platform}] ⚠ markdown 為空")

            data = res.json
            if not data:
                print(f"  [{platform}] ✗ 無結構化資料")
                continue

            courses = data.get("courses", []) if isinstance(data, dict) else []

            # 防幻覺：過濾無中文課程名
            if config.get("expect_chinese"):
                before = len(courses)
                courses = [c for c in courses if has_chinese(c.get("course_name", ""))]
                if before - len(courses):
                    print(f"  [{platform}] ⚠ 過濾 {before - len(courses)} 筆幻覺課程")

            # 過濾非課程頁面
            before_svc = len(courses)
            url_blocklist = ["/services/", "/campaigns/"]
            # Hahow 課程 URL 必須包含 /courses/
            if platform == "hahow":
                courses = [
                    c for c in courses
                    if "/courses/" in c.get("url", "")
                    and all(b not in c.get("url", "") for b in url_blocklist)
                ]
            else:
                courses = [
                    c for c in courses
                    if all(b not in c.get("url", "") for b in url_blocklist)
                ]
            if before_svc - len(courses):
                print(f"  [{platform}] ⚠ 過濾 {before_svc - len(courses)} 筆非課程頁面")

            # Hahow：用 HTML 解析列表頁取得類型與學生數
            if platform == "hahow":
                html = getattr(res, "html", None) or ""
                if html:
                    from urllib.parse import urlparse
                    card_data = parse_hahow_listing_html(html)
                    # debug log
                    types_found = {v["type"] for v in card_data.values() if v.get("type")}
                    students_found = sum(1 for v in card_data.values() if v.get("students") is not None)
                    print(f"  [hahow] HTML 解析：{len(card_data)} 卡片，類型={types_found}，有學生數={students_found}")
                    enriched = []
                    for c in courses:
                        path = urlparse(c.get("url", "")).path  # "/courses/xxx"
                        info = card_data.get(path, {})
                        if info.get("type") and info["type"] != "課程":
                            print(f"  [hahow] ⚠ 跳過「{info['type']}」: {c.get('course_name')}")
                            continue
                        c["students"] = info.get("students")
                        enriched.append(c)
                    courses = enriched
                else:
                    print(f"  [hahow] ⚠ HTML 為空，略過 CSS 選擇器過濾")

            # PressPlay：偵測集資課 + 嘗試從列表取學生數
            if platform == "pressplay":
                html = getattr(res, "html", None) or ""
                if html:
                    from urllib.parse import urlparse
                    card_data = parse_pressplay_listing_html(html)
                    funding_count   = sum(1 for v in card_data.values() if v["is_funding"])
                    students_found  = sum(1 for v in card_data.values() if v.get("students") is not None)
                    print(f"  [pressplay] HTML 解析：{len(card_data)} 卡片，集資課={funding_count}，有學生數={students_found}")
                    for c in courses:
                        # 正規化 URL path（去掉 /about 尾綴）
                        raw = urlparse(c.get("url", "")).path.rstrip("/")
                        path = raw[:-6] if raw.endswith("/about") else raw
                        info = card_data.get(path, card_data.get(raw, {}))
                        c["is_funding"] = info.get("is_funding", False)
                        c["students"]   = info.get("students")  # None if funding or not found
                else:
                    print(f"  [pressplay] ⚠ HTML 為空，略過集資課偵測")

            # 跨頁去重（以 URL 為 key）
            for c in courses:
                url = c.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_courses.append(c)

            print(f"  [{platform}] 本頁新增 {len(courses)} 門，累計 {len(all_courses)} 門")

        all_courses = all_courses[:max_courses]
        print(f"  [{platform}] ✓ 最終 {len(all_courses)} 門課程")
        result[platform] = all_courses

    return result

# ── Update 模式 ───────────────────────────────────────────────────────────────

def update_student_counts(app: FirecrawlApp, course_list: dict[str, list[dict]]) -> list[dict]:
    """更新學生數：Hahow 優先用列表頁已知數值，無則進內頁；PressPlay 逐一進內頁。"""
    rows: list[dict] = []
    scraped_at = datetime.now().isoformat(timespec="seconds")

    for platform, courses in course_list.items():
        config = PLATFORMS.get(platform, {})
        patterns = config.get("student_patterns", [])
        print(f"\n  [{platform}] 更新學生數（{len(courses)} 門）…")

        for rank, course in enumerate(courses, start=1):
            url = course.get("url", "").strip()
            name = course.get("course_name", f"課程{rank}")

            # 若 discover 時已從列表頁取得學生數，直接使用，不進內頁
            existing = course.get("students")
            if existing is not None:
                print(f"  [{rank}/{len(courses)}] {name} → {existing}（列表已知，略過內頁）")
                rows.append({
                    "platform":    platform,
                    "rank":        rank,
                    "course_name": course.get("course_name", ""),
                    "teacher":     course.get("teacher", ""),
                    "price":       course.get("price"),
                    "students":    existing,
                    "course_url":  url,
                    "scraped_at":  scraped_at,
                })
                continue

            # 集資課只用「人預購」pattern，避免誤抓達標百分比
            if platform == "pressplay" and course.get("is_funding"):
                course_patterns = [r"([\d,]+)\s*人預購"]
                print(f"  [{rank}/{len(courses)}] {name}（集資課，進內頁抓人預購）")
            else:
                course_patterns = patterns
                print(f"  [{rank}/{len(courses)}] {name}")

            students = None
            if url and url.startswith("http"):
                # 策略：先用 stealth proxy；
                # 若 markdown 太短（<1500字，疑似被擋）→ 改用無 proxy 重試
                attempts = [
                    {"proxy": "stealth", "wait_for": 5000},
                    {"proxy": None,      "wait_for": 15000},  # 長等待 fallback（某些頁面需要 >5s 渲染）
                ]
                for attempt_no, opt in enumerate(attempts, start=1):
                    try:
                        scrape_kwargs = dict(
                            url=url,
                            formats=["markdown"],
                            wait_for=opt["wait_for"],
                        )
                        if opt["proxy"]:
                            scrape_kwargs["proxy"] = opt["proxy"]
                        res = app.scrape(**scrape_kwargs)
                        md = res.markdown or ""
                        if len(md) < 1500 and attempt_no < len(attempts):
                            print(f"    ⚠ 第{attempt_no}次 markdown 過短({len(md)}字)，改用無proxy重試…")
                            continue
                        students = extract_students_from_markdown(md, course_patterns)
                        if students is not None:
                            break
                        if attempt_no < len(attempts):
                            print(f"    ⚠ 第{attempt_no}次未匹配，改用無proxy重試…")
                    except Exception as exc:
                        print(f"    ✗ 爬取失敗（第{attempt_no}次）：{exc}")
                        break
                print(f"    學生數：{students}")
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
