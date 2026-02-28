#!/usr/bin/env python3
"""
快速測試：只爬 Hahow 前 N 門課的內頁，驗證學生數 regex 是否正確。
用法：
  python test_hahow.py        # 預設爬前 2 門
  python test_hahow.py 3      # 爬前 3 門
"""
import re
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent / ".env")

from firecrawl import FirecrawlApp

COURSE_LIST_FILE = Path("data/course_list.json")
PATTERNS = [
    r"課程總人數.{0,100}?(\d{4,})",   # 一般課（≥1000 人）
    r"當前購買數.{0,100}?(\d+)",       # 預購課（任意人數）
]

def parse_int(s: str) -> int:
    return int(s.replace(",", "").replace("，", ""))

def extract_students(md: str):
    for pat in PATTERNS:
        m = re.search(pat, md, re.DOTALL)
        if m:
            return parse_int(m.group(1)), pat
    return None, None

def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    if not COURSE_LIST_FILE.exists():
        print("✗ 找不到 data/course_list.json，請先執行 python scraper.py --discover")
        sys.exit(1)

    course_list = json.loads(COURSE_LIST_FILE.read_text(encoding="utf-8"))
    hahow_courses = course_list.get("hahow", [])[:n]

    if not hahow_courses:
        print("✗ course_list.json 裡沒有 hahow 資料")
        sys.exit(1)

    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("✗ 請在 .env 設定 FIRECRAWL_API_KEY")
        sys.exit(1)

    app = FirecrawlApp(api_key=api_key)
    print(f"=== Hahow 測試：前 {n} 門課 ===\n")

    matched_count = 0
    for i, course in enumerate(hahow_courses, 1):
        name = course.get("course_name", f"課程{i}")
        url  = course.get("url", "")

        if not url.startswith("http"):
            continue

        try:
            res = app.scrape(url=url, formats=["markdown"], wait_for=5000)
            md  = res.markdown or ""
        except Exception as e:
            continue

        students, matched_pat = extract_students(md)

        if students is not None:
            matched_count += 1
            print(f"[{i}] {name}")
            print(f"  學生數：{students}")
            print(f"  URL：{url}")
            print()

    print(f"=== 完成：{n} 門中有 {matched_count} 門成功抓到學生數 ===")

if __name__ == "__main__":
    main()
