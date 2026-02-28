#!/usr/bin/env python3
"""
快速測試：驗證學生數 regex 是否正確。
用法：
  python test_hahow.py                          # 預設爬前 2 門
  python test_hahow.py 5                        # 爬前 5 門
  python test_hahow.py --url https://...        # 直接測單一 URL
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
    r"課程總人數.{0,100}?(\d+)\s*位同學",  # 一般課（任意人數）
    r"當前購買數.{0,100}?(\d+)",            # 預購課（任意人數）
]

def parse_int(s: str) -> int:
    return int(s.replace(",", "").replace("，", ""))

def extract_students(md: str):
    for pat in PATTERNS:
        m = re.search(pat, md, re.DOTALL)
        if m:
            return parse_int(m.group(1)), pat
    return None, None

def test_url(app: FirecrawlApp, url: str, name: str = ""):
    print(f"URL：{url}")
    try:
        res = app.scrape(url=url, formats=["markdown"], wait_for=5000)
        md  = res.markdown or ""
    except Exception as e:
        print(f"✗ 爬取失敗：{e}")
        return

    students, matched_pat = extract_students(md)
    if students is not None:
        print(f"✓ 學生數：{students}")
        print(f"  匹配 pattern：{matched_pat}")
    else:
        print(f"✗ 未匹配任何 pattern（markdown 共 {len(md)} 字）")
        # 搜尋跟學生數相關的關鍵字，印出前後 80 字
        keywords = ["課程總人數", "當前購買數", "人學習", "人預購", "已購買", "同學", "學員", "購買數", "人已"]
        found_any = False
        for kw in keywords:
            idx = md.find(kw)
            if idx != -1:
                start = max(0, idx - 80)
                end = min(len(md), idx + 120)
                print(f"\n  關鍵字「{kw}」出現在位置 {idx}：")
                print(f"  ...{md[start:end]}...")
                found_any = True
        if not found_any:
            print("  ⚠ 頁面中找不到任何學生相關關鍵字")
            print("--- markdown 前 800 字 ---")
            print(md[:800])

def main():
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("✗ 請在 .env 設定 FIRECRAWL_API_KEY")
        sys.exit(1)

    app = FirecrawlApp(api_key=api_key)

    # --url 模式：直接測單一 URL
    if len(sys.argv) >= 3 and sys.argv[1] == "--url":
        url = sys.argv[2]
        print(f"=== 單一 URL 測試 ===\n")
        test_url(app, url)
        return

    # 一般模式：測前 N 門
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    if not COURSE_LIST_FILE.exists():
        print("✗ 找不到 data/course_list.json，請先執行 python scraper.py --discover")
        sys.exit(1)

    course_list = json.loads(COURSE_LIST_FILE.read_text(encoding="utf-8"))
    hahow_courses = course_list.get("hahow", [])[:n]

    if not hahow_courses:
        print("✗ course_list.json 裡沒有 hahow 資料")
        sys.exit(1)

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
