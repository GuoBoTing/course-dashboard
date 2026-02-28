#!/usr/bin/env python3
"""
課程趨勢儀表板 (Streamlit)
顯示 Hahow 與 PressPlay 的課程排名、成長率、成長速度，並標示近期成長快速的課程。
"""
import subprocess
import sys
import os
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
import plotly.express as px

load_dotenv(Path(__file__).parent / ".env")

def get_secret(key: str) -> str:
    """優先讀 Streamlit Secrets（Cloud），fallback 到 .env（本地）。"""
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key, "")

@st.cache_resource
def get_supabase():
    from supabase import create_client
    url = get_secret("SUPABASE_URL")
    key = get_secret("SUPABASE_KEY")
    if not url or not key:
        return None
    return create_client(url, key)

def get_last_scrape_date() -> date | None:
    """讀取 Supabase 中最新一筆的爬取日期。"""
    sb = get_supabase()
    if sb is None:
        return None
    try:
        res = sb.table("course_scrapes").select("scraped_at").order("scraped_at", desc=True).limit(1).execute()
        if not res.data:
            return None
        return pd.to_datetime(res.data[0]["scraped_at"]).date()
    except Exception:
        return None

PLATFORM_LABEL = {"hahow": "Hahow", "pressplay": "PressPlay"}
PLATFORM_COLOR = {"hahow": "#FF6B35", "pressplay": "#3A86FF"}

# ── 頁面設定 ──────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="課程趨勢儀表板",
    page_icon="📈",
    layout="wide",
)

st.title("📈 課程趨勢儀表板")
st.caption("追蹤 Hahow & PressPlay 熱門課程的成長率與成長速度")

# ── 爬取按鈕（最先定義，確保無資料時也能顯示）────────────────────────────────

with st.sidebar:
    st.header("資料更新")

    project_dir = Path(__file__).parent
    scraper_path = project_dir / "scraper.py"
    venv_python = project_dir / ".venv" / "bin" / "python"
    python_exec = str(venv_python) if venv_python.exists() else sys.executable

    # ── 管理員登入 ──
    if "is_admin" not in st.session_state:
        st.session_state.is_admin = False

    if not st.session_state.is_admin:
        with st.form("login_form"):
            pwd = st.text_input("管理員密碼", type="password")
            if st.form_submit_button("登入", width="stretch"):
                if pwd == get_secret("ADMIN_PASSWORD"):
                    st.session_state.is_admin = True
                    st.rerun()
                else:
                    st.error("密碼錯誤")
    else:
        st.caption(f"👤 管理員已登入")
        if st.button("登出", width="stretch"):
            st.session_state.is_admin = False
            st.rerun()

        st.divider()

        def run_scraper(extra_args: list, spinner_msg: str):
            with st.spinner(spinner_msg):
                proc = subprocess.run(
                    [python_exec, str(scraper_path)] + extra_args,
                    capture_output=True,
                    text=True,
                    cwd=str(project_dir),
                )
            if proc.returncode == 0:
                st.success("完成！")
                st.text(proc.stdout[-800:] if len(proc.stdout) > 800 else proc.stdout)
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("爬取失敗")
                if proc.stdout:
                    st.text("--- stdout ---\n" + (proc.stdout[-1200:] if len(proc.stdout) > 1200 else proc.stdout))
                if proc.stderr:
                    st.text("--- stderr ---\n" + (proc.stderr[-800:] if len(proc.stderr) > 800 else proc.stderr))

        last_scrape = get_last_scrape_date()
        if last_scrape:
            st.caption(f"上次更新：{last_scrape}")

        run_update   = st.button("🔄 更新學生數",   width="stretch", type="primary",
                                 help="只更新學生人數，markdown 模式（~20 credits）")
        run_discover = st.button("🔍 重新發現課程", width="stretch",
                                 help="重新爬列表頁取得最新排名，LLM 模式（~60 credits）")
        if run_update:
            run_scraper([], "更新學生數中（約 2～4 分鐘）…")
        if run_discover:
            run_scraper(["--discover"], "重新發現課程中（約 3～5 分鐘）…")

    st.divider()

# ── 載入資料 ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=120)
def load_data() -> pd.DataFrame:
    sb = get_supabase()
    if sb is None:
        st.error("尚未設定 Supabase 連線，請在 Secrets 中加入 SUPABASE_URL 和 SUPABASE_KEY。")
        return pd.DataFrame()
    try:
        res = sb.table("course_scrapes").select("*").order("scraped_at").execute()
        if not res.data:
            return pd.DataFrame()
        df = pd.DataFrame(res.data)
        df["scraped_at"] = pd.to_datetime(df["scraped_at"])
        df["students"]   = pd.to_numeric(df["students"], errors="coerce")
        df["price"]      = pd.to_numeric(df["price"],    errors="coerce")
        df["rank"]       = pd.to_numeric(df["rank"],     errors="coerce")
        return df
    except Exception as e:
        st.error(f"資料庫讀取失敗：{e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.info("尚無資料，請點擊左側「🔄 立即爬取」按鈕開始收集課程資料。")
    st.stop()

# ── 側邊欄篩選（有資料後才顯示）──────────────────────────────────────────────

with st.sidebar:
    st.header("篩選條件")

    platform_options = ["全部"] + [PLATFORM_LABEL[p] for p in sorted(df["platform"].unique())]
    selected_label = st.selectbox("平台", platform_options)

    all_dates = sorted(df["scraped_at"].dt.date.unique())
    if len(all_dates) >= 2:
        date_range = st.date_input(
            "日期範圍",
            value=(all_dates[0], all_dates[-1]),
            min_value=all_dates[0],
            max_value=all_dates[-1],
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = all_dates[-1]
    else:
        start_date = end_date = all_dates[0]
        st.info(f"目前僅有 {all_dates[0]} 的資料")

    growth_threshold = st.slider("成長率提示門檻（%）", min_value=1, max_value=50, value=5)

# ── 套用篩選 ──────────────────────────────────────────────────────────────────

df_f = df.copy()
if selected_label != "全部":
    inv_map = {v: k for k, v in PLATFORM_LABEL.items()}
    df_f = df_f[df_f["platform"] == inv_map[selected_label]]

df_f = df_f[
    (df_f["scraped_at"].dt.date >= start_date) &
    (df_f["scraped_at"].dt.date <= end_date)
]

# ── 計算成長指標 ──────────────────────────────────────────────────────────────

def compute_growth(df: pd.DataFrame) -> pd.DataFrame:
    """
    以「天」為單位計算成長指標：
    - 先將每門課按日期彙總（每天取最後一筆），避免同日多次爬取干擾
    - student_diff : 首日 → 末日的學生人數變化量
    - growth_rate  : 成長率 (%)
    - growth_speed : 每天成長人數 (人/天)，需至少跨越 2 個不同日期
    - days_elapsed : 觀察天數（日曆天數差）
    """
    rows = []
    for (platform, course_name), grp in df.groupby(["platform", "course_name"]):
        grp = grp.sort_values("scraped_at")

        # 按日彙總：每天取最後一筆（最新的爬取結果）
        grp["_date"] = grp["scraped_at"].dt.date
        daily = (
            grp.groupby("_date", sort=True)
               .last()
               .reset_index()
        )

        first = daily.iloc[0]
        last  = daily.iloc[-1]

        s0 = first["students"]
        s1 = last["students"]

        # 日曆天數差（不是小時差）
        day_diff = (last["_date"] - first["_date"]).days

        diff  = (s1 - s0)           if pd.notna(s0) and pd.notna(s1) else None
        rate  = (diff / s0 * 100)   if diff is not None and s0 and s0 > 0 else None
        # 成長速度只在有跨日資料時才計算
        speed = (diff / day_diff)   if diff is not None and day_diff >= 1 else None

        rows.append({
            "platform":        platform,
            "course_name":     course_name,
            "teacher":         last["teacher"],
            "latest_students": s1,
            "latest_price":    last["price"],
            "latest_rank":     last["rank"] if "rank" in grp.columns else None,
            "student_diff":    diff,
            "growth_rate":     rate,
            "growth_speed":    speed,
            "days_elapsed":    day_diff if day_diff >= 1 else None,
            "scrape_count":    len(daily),
            "course_url":      last.get("course_url", "") or "",
        })
    return pd.DataFrame(rows)

growth_df = compute_growth(df_f)

# ── 總覽指標 ──────────────────────────────────────────────────────────────────

latest_time    = df["scraped_at"].max()
unique_courses = df[["platform", "course_name"]].drop_duplicates().shape[0]
scrape_count   = df["scraped_at"].nunique()

c1, c2, c3 = st.columns(3)
c1.metric("追蹤課程總數", unique_courses)
c2.metric("累計爬取次數", scrape_count)
c3.metric("最後更新時間", latest_time.strftime("%Y-%m-%d %H:%M"))

st.divider()

# ── 成長快速提示 ──────────────────────────────────────────────────────────────

st.subheader("🚀 近期成長快速課程")

fast = (
    growth_df[
        growth_df["growth_rate"].notna() &
        (growth_df["growth_rate"] >= growth_threshold)
    ]
    .sort_values("growth_speed", ascending=False)
    .head(5)
)

if fast.empty:
    unique_days = df_f["scraped_at"].dt.date.nunique()
    if unique_days < 2:
        st.info("需要至少 **兩天** 的資料才能計算成長率。")
    else:
        st.info(f"目前無課程成長率超過 {growth_threshold}%，可調整側邊欄的門檻值。")
else:
    for _, row in fast.iterrows():
        plabel = PLATFORM_LABEL.get(row["platform"], row["platform"])
        rate_str  = f"+{row['growth_rate']:.1f}%"
        speed_str = f"+{row['growth_speed']:,.1f} 人/天" if pd.notna(row["growth_speed"]) else "—"
        diff_str  = f"+{int(row['student_diff']):,} 人"

        with st.container(border=True):
            col1, col2, col3, col4 = st.columns([4, 1, 1, 1])
            url = row.get("course_url", "") or ""
            name_md = f"[{row['course_name']}]({url})" if url else row['course_name']
            col1.markdown(
                f"**{name_md}**  \n"
                f"<span style='color:gray'>{row['teacher']} ｜ {plabel}</span>",
                unsafe_allow_html=True,
            )
            col2.metric("目前學生", f"{int(row['latest_students']):,}" if pd.notna(row['latest_students']) else "N/A")
            col3.metric("成長率",   rate_str,  delta=diff_str)
            col4.metric("成長速度", speed_str)

st.divider()

# ── 最新排名表 ────────────────────────────────────────────────────────────────

st.subheader("📋 最新課程排名")

latest_snap = df_f[df_f["scraped_at"] == df_f["scraped_at"].max()].copy()

if latest_snap.empty:
    st.warning("篩選後無最新資料。")
else:
    table = latest_snap.merge(
        growth_df[["platform", "course_name", "growth_rate", "growth_speed", "student_diff"]],
        on=["platform", "course_name"],
        how="left",
    )
    table["平台"]       = table["platform"].map(PLATFORM_LABEL)
    table["成長率(%)"]  = table["growth_rate"].apply(
        lambda x: f"+{x:.1f}" if pd.notna(x) and x > 0 else (f"{x:.1f}" if pd.notna(x) else "—")
    )
    table["成長速度(人/天)"] = table["growth_speed"].apply(
        lambda x: f"+{x:,.1f}" if pd.notna(x) and x > 0 else (f"{x:,.1f}" if pd.notna(x) else "—")
    )

    display = (
        table
        .sort_values("growth_rate", ascending=False, na_position="last")
        [[
            "平台", "rank", "course_name", "teacher",
            "price", "students", "成長率(%)", "成長速度(人/天)", "course_url"
        ]]
        .rename(columns={
            "rank":        "排名",
            "course_name": "課程名稱",
            "teacher":     "老師",
            "price":       "價格(NTD)",
            "students":    "學生數",
            "course_url":  "連結",
        })
    )

    st.dataframe(
        display,
        column_config={
            "連結": st.column_config.LinkColumn("連結", display_text="🔗 開啟"),
        },
        width="stretch",
        hide_index=True,
    )

st.divider()

# ── 趨勢折線圖 ────────────────────────────────────────────────────────────────

st.subheader("📉 學生人數趨勢（每日）")

unique_days = df_f["scraped_at"].dt.date.nunique()
if unique_days < 2:
    st.info("需要至少兩天的資料才能顯示趨勢圖。")
else:
    top_default = (
        growth_df[growth_df["latest_students"].notna()]
        .nlargest(5, "latest_students")["course_name"]
        .tolist()
    )
    all_course_names = sorted(growth_df["course_name"].unique().tolist())

    selected = st.multiselect(
        "選擇要比較的課程（最多 10 門）",
        options=all_course_names,
        default=[c for c in top_default if c in all_course_names],
        max_selections=10,
    )

    if selected:
        trend_raw = df_f[df_f["course_name"].isin(selected)].copy()
        trend_raw["date"] = trend_raw["scraped_at"].dt.date

        # 每天取最後一筆，以「天」為粒度顯示
        trend = (
            trend_raw.sort_values("scraped_at")
                     .groupby(["platform", "course_name", "date"], sort=True)
                     .last()
                     .reset_index()
        )
        trend["課程"] = trend["platform"].map(PLATFORM_LABEL) + " · " + trend["course_name"]

        fig = px.line(
            trend,
            x="date",
            y="students",
            color="課程",
            markers=True,
            title="學生人數歷史趨勢（每日）",
            labels={"date": "日期", "students": "學生人數"},
        )
        fig.update_layout(hovermode="x unified", legend_title="課程")
        st.plotly_chart(fig, width="stretch")

    # ── 成長速度長條圖 ────────────────────────────────────────────────────────

    st.subheader("⚡ 成長速度比較（每日新增學生）")

    top_speed = (
        growth_df[growth_df["growth_speed"].notna() & (growth_df["growth_speed"] > 0)]
        .nlargest(15, "growth_speed")
        .copy()
    )

    if top_speed.empty:
        st.info("尚無跨日正向成長資料可顯示。")
    else:
        top_speed["label"] = (
            top_speed["platform"].map(PLATFORM_LABEL) + " · " +
            top_speed["course_name"].str[:25]
        )
        fig2 = px.bar(
            top_speed,
            x="growth_speed",
            y="label",
            orientation="h",
            color="platform",
            color_discrete_map=PLATFORM_COLOR,
            title="每日學生成長速度 Top 15",
            labels={
                "growth_speed": "每日成長人數",
                "label": "",
                "platform": "平台",
            },
            text="growth_speed",
        )
        fig2.update_traces(texttemplate="%{text:,.1f}", textposition="outside")
        fig2.update_layout(
            yaxis={"categoryorder": "total ascending"},
            xaxis_title="每日新增學生人數",
            showlegend=True,
        )
        st.plotly_chart(fig2, width="stretch")

# ── 資料管理（管理員限定）────────────────────────────────────────────────────

if st.session_state.get("is_admin"):
    st.divider()
    st.subheader("🗂 資料管理")

    mgmt_platform = st.selectbox(
        "平台",
        options=sorted(df["platform"].unique()),
        format_func=lambda x: PLATFORM_LABEL.get(x, x),
        key="mgmt_platform",
    )
    courses_in_platform = sorted(
        df[df["platform"] == mgmt_platform]["course_name"].unique()
    )
    mgmt_course = st.selectbox("課程", options=courses_in_platform, key="mgmt_course")

    course_hist = (
        df[(df["platform"] == mgmt_platform) & (df["course_name"] == mgmt_course)]
        [["id", "scraped_at", "students", "rank", "price", "course_url"]]
        .sort_values("scraped_at")
        .reset_index(drop=True)
    )

    # 顯示時不顯示 id 欄，但保留在 DataFrame 供刪除使用
    st.dataframe(
        course_hist.drop(columns=["id"]),
        width="stretch",
        hide_index=False,
    )

    ts_options = course_hist["scraped_at"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
    to_delete = st.multiselect("選擇要刪除的紀錄（可多選）", options=ts_options, key="mgmt_delete")

    if to_delete:
        st.warning(f"將刪除 {len(to_delete)} 筆紀錄，此操作無法還原。")
        if st.button("🗑 確認刪除", type="primary"):
            delete_set = set(to_delete)
            ids_to_delete = course_hist[
                course_hist["scraped_at"].dt.strftime("%Y-%m-%d %H:%M:%S").isin(delete_set)
            ]["id"].tolist()
            sb = get_supabase()
            sb.table("course_scrapes").delete().in_("id", ids_to_delete).execute()
            st.success(f"已刪除 {len(ids_to_delete)} 筆紀錄")
            st.cache_data.clear()
            st.rerun()

    st.divider()
    st.markdown("**🧹 批次清除**")

    svc_ids = df[df["course_url"].str.contains("/services/", na=False)]["id"].tolist()
    null_ids = df[df["students"].isna()]["id"].tolist()

    col_a, col_b = st.columns(2)
    with col_a:
        st.caption(f"服務/工作坊頁面：{len(svc_ids)} 筆")
        if st.button("🗑 清除服務/工作坊資料", disabled=len(svc_ids) == 0):
            get_supabase().table("course_scrapes").delete().in_("id", svc_ids).execute()
            st.success(f"已清除 {len(svc_ids)} 筆")
            st.cache_data.clear()
            st.rerun()
    with col_b:
        st.caption(f"學生數為空：{len(null_ids)} 筆")
        if st.button("🗑 清除學生數空值資料", disabled=len(null_ids) == 0):
            get_supabase().table("course_scrapes").delete().in_("id", null_ids).execute()
            st.success(f"已清除 {len(null_ids)} 筆")
            st.cache_data.clear()
            st.rerun()

# ── 頁尾 ──────────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "資料來源：Hahow & PressPlay ｜ "
    "每日執行 `python scraper.py` 更新 ｜ "
    "啟動看板：`streamlit run app.py`"
)
