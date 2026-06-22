"""마켓 데이터 — main Streamlit entry.

세 개의 탭:
  1. 🌏 마켓 둘러보기  — 지수·시가총액 상위·지수 멤버 한눈에
  2. 🔎 종목 상세      — 검색·차트·외국인 보유·기준데이터
  3. ⬇ 데이터 추출    — 다종목·기간·빈도·포맷 묶음 export

진입:
  - 통합 런처: pages/marketdata.py → auth.run_legacy_app("marketdata_app", "app.py")
  - 단독: streamlit run marketdata_app/app.py --server.port 8520
"""
from __future__ import annotations

from datetime import date, timedelta
from io import StringIO

import pandas as pd
import streamlit as st

from marketdata_app import data as md
from marketdata_app import charts as ch
from marketdata_app import export as ex
from marketdata_app import brief as br


# ──────────────────────────────────────────────────────────────────────
# Page header
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
      .md-pill { display:inline-block; padding:2px 10px; border-radius:999px;
                 background:#faefe9; color:#6e3520; font-size:11px;
                 font-weight:600; letter-spacing:.03em; margin-right:6px; }
      .md-kpi-label { color:#666; font-size:11px; letter-spacing:.06em;
                      text-transform:uppercase; margin-bottom:2px; }
      .md-kpi-val { font-size:22px; font-weight:600; color:#111;
                    font-variant-numeric: tabular-nums; }
      .md-meta { color:#888; font-size:11.5px; }
      .md-sec-h { font-size:15px; font-weight:600; color:#111; margin:0 0 6px; }
    </style>
    """,
    unsafe_allow_html=True,
)

col_title, col_status = st.columns([3, 2])
with col_title:
    st.markdown("## 📈 마켓 데이터")
    st.markdown(
        "<div class='md-meta'>한국 주식 시세·외국인·기관 데이터 · "
        "검색하고, 보고, 추출하세요.</div>",
        unsafe_allow_html=True,
    )

with col_status:
    status = md.master_status()
    if not status.get("ok"):
        st.error(f"마스터 로드 실패: {status.get('error')}")
    else:
        st.markdown(
            f"<div style='text-align:right; padding-top:8px;'>"
            f"<span class='md-pill'>{status['n_equities']:,} listings</span>"
            f"<span class='md-pill'>KOSPI 200 · {status['n_kospi200']}</span>"
            f"<span class='md-pill'>KOSDAQ 150 · {status['n_kosdaq150']}</span>"
            f"<span class='md-pill'>KRX 300 · {status['n_krx300']}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

st.divider()


# ──────────────────────────────────────────────────────────────────────
# 진단 (접힌 상태로 항상 노출 — 데이터 안 뜰 때 첫 번째로 펴 보는 곳)
# ──────────────────────────────────────────────────────────────────────
with st.expander("🔧 KRX 연결 진단 / 캐시 초기화", expanded=False):
    diag_col, button_col = st.columns([3, 1])
    with diag_col:
        diag = md.diagnostics()

        def _line(label: str, ok: bool, ok_text: str = "", err_text: str = "") -> str:
            mark = "✅" if ok else "❌"
            tail = ok_text if ok else err_text
            return f"- **{label}**: {mark} {tail}"

        st.markdown(
            f"- **Python**: `{diag.get('python', '?')}`\n"
            f"- **pykrx**: `{diag.get('pykrx_version', '?')}` "
            f"{'✅' if diag.get('pykrx_ok') else '❌'}\n"
            f"- **영업일**: `{diag.get('latest_business_day') or diag.get('latest_business_day_error') or '—'}`"
        )
        st.markdown(_line(
            "샘플 호출 (005930 → 종목명)",
            bool(diag.get("sample_fetch_ok")),
            ok_text=f"→ `{diag.get('sample_value', '')}`",
            err_text=str(diag.get("sample_error") or ""),
        ))
        st.markdown(_line(
            "지수 OHLCV (KOSPI 1001, 최근 14일)",
            bool(diag.get("index_kospi_ok")),
            ok_text=f"{diag.get('index_kospi_rows', 0)} rows",
            err_text=str(diag.get("index_kospi_error") or "empty"),
        ))
        st.markdown(_line(
            "전종목 리스트 (KOSPI)",
            bool(diag.get("universe_kospi_ok")),
            ok_text=f"{diag.get('universe_kospi_n', 0)} tickers",
            err_text=str(diag.get("universe_kospi_error") or "empty"),
        ))
        st.markdown(_line(
            "시가총액 (KOSPI)",
            bool(diag.get("cap_kospi_ok")),
            ok_text=f"{diag.get('cap_kospi_rows', 0)} rows",
            err_text=str(diag.get("cap_kospi_error") or "empty"),
        ))

        if md.LAST_ERRORS:
            st.markdown("**최근 캐시된 호출 에러**")
            for k, v in list(md.LAST_ERRORS.items())[-8:]:
                st.code(f"{k}: {v}", language=None)
        else:
            st.caption("최근 fetch 에러 없음. (캐시된 빈 결과가 있다면 위 진단은 직접 호출이라 캐시 무관)")
    with button_col:
        if st.button("🗑 캐시 초기화", use_container_width=True):
            st.cache_data.clear()
            md.clear_errors()
            st.rerun()
        st.caption("호출 결과가 비어 있는 채로 캐시됐다면 클릭.")


# ──────────────────────────────────────────────────────────────────────
# Tabs
# ──────────────────────────────────────────────────────────────────────
tab_browse, tab_stock, tab_extract = st.tabs([
    "🌏 마켓 둘러보기", "🔎 종목 상세", "⬇ 데이터 추출"
])


# ============= TAB 1: 마켓 둘러보기 ====================================
with tab_browse:
    # KPI: KOSPI / KOSDAQ / KOSPI 200
    today = date.today()
    start = today - timedelta(days=7)

    indices = [
        ("KOSPI", "1001"),
        ("KOSDAQ", "2001"),
        ("KOSPI 200", "1028"),
        ("KRX 300", "5042"),
    ]
    kpi_cols = st.columns(len(indices))
    for col, (label, code) in zip(kpi_cols, indices):
        with col:
            df = md.index_ohlcv(code, start.isoformat(), today.isoformat())
            if df.empty or len(df) < 2:
                st.markdown(
                    f"<div class='md-kpi-label'>{label}</div>"
                    f"<div class='md-kpi-val'>—</div>"
                    f"<div class='md-meta'>데이터 없음</div>",
                    unsafe_allow_html=True,
                )
            else:
                last = df.iloc[-1]
                prev = df.iloc[-2]
                pct = (last["close"] / prev["close"] - 1) * 100
                color = "#2f7a3a" if pct >= 0 else "#b14a3a"
                sign = "+" if pct >= 0 else ""
                st.markdown(
                    f"<div class='md-kpi-label'>{label}</div>"
                    f"<div class='md-kpi-val'>{last['close']:,.2f}</div>"
                    f"<div style='color:{color}; font-size:12px; font-weight:600; "
                    f"font-variant-numeric: tabular-nums;'>{sign}{pct:.2f}%</div>",
                    unsafe_allow_html=True,
                )

    st.markdown("&nbsp;", unsafe_allow_html=True)

    # Index members table
    left, right = st.columns([3, 2])
    with left:
        st.markdown("<div class='md-sec-h'>지수 구성종목</div>", unsafe_allow_html=True)
        idx_label = st.radio(
            "Index", list(md.INDEX_CHOICES.keys()),
            horizontal=True, label_visibility="collapsed",
        )
        members = md.index_members(idx_label)
        if members.empty:
            st.info("멤버 데이터를 가져오지 못했습니다.")
        else:
            search = st.text_input("filter", placeholder="이름/티커로 필터…",
                                   label_visibility="collapsed")
            if search:
                mask = (
                    members["name_kr"].str.contains(search, case=False, na=False)
                    | members["name_en"].str.contains(search, case=False, na=False)
                    | members["ticker"].astype(str).str.contains(search, na=False)
                )
                view = members[mask]
            else:
                view = members
            st.caption(f"{len(view):,} / {len(members):,} 종목")
            st.dataframe(
                view, use_container_width=True, hide_index=True, height=420,
                column_config={
                    "ticker": st.column_config.TextColumn("Ticker", width="small"),
                    "name_kr": st.column_config.TextColumn("이름"),
                    "name_en": st.column_config.TextColumn("Name (EN)"),
                    "market": st.column_config.TextColumn("Market", width="small"),
                    "sector_gics": st.column_config.TextColumn("Sector (GICS)"),
                    "isin": st.column_config.TextColumn("ISIN", width="small"),
                    "bloomberg": st.column_config.TextColumn("BBG", width="small"),
                    "ric": st.column_config.TextColumn("RIC", width="small"),
                },
            )

    with right:
        st.markdown("<div class='md-sec-h'>시가총액 상위 (KOSPI)</div>", unsafe_allow_html=True)
        with st.spinner("Loading…"):
            uni = md.list_market_universe("KOSPI")

        # 시가총액 컬럼이 비어 있으면 (pykrx 깨져서 mandata_kr 폴백) 안내만
        has_cap = (
            "market_cap_krw" in uni.columns
            and pd.to_numeric(uni["market_cap_krw"], errors="coerce").notna().any()
        )

        if uni.empty:
            st.info("KRX 데이터를 가져오지 못했습니다. (네트워크 상태 확인)")
        elif not has_cap:
            st.info(
                f"📋 KOSPI 종목 마스터 **{len(uni):,}개** 로드됨 "
                "(시가총액은 pykrx 응답 깨짐으로 제외).\n\n"
                "현재 검증된 작동: 인덱스 KPI (yfinance), 지수 멤버 테이블 (좌측).\n"
                "다음: 'Top by market cap' 은 yfinance 기반으로 재구현 예정."
            )
        else:
            uni["market_cap_krw"] = pd.to_numeric(uni["market_cap_krw"], errors="coerce")
            top = uni.dropna(subset=["market_cap_krw"]).nlargest(20, "market_cap_krw")
            top["mcap_trn"] = (top["market_cap_krw"] / 1e12).round(1)
            st.dataframe(
                top[["ticker", "name", "mcap_trn", "last_close"]],
                use_container_width=True, hide_index=True, height=420,
                column_config={
                    "ticker": st.column_config.TextColumn("Ticker", width="small"),
                    "name": st.column_config.TextColumn("이름"),
                    "mcap_trn": st.column_config.NumberColumn("시총 (조)", format="%.1f"),
                    "last_close": st.column_config.NumberColumn("종가", format="%d"),
                },
            )


# ============= TAB 2: 종목 상세 ========================================
with tab_stock:
    # Search bar
    q_col, range_col = st.columns([3, 2])
    with q_col:
        query = st.text_input(
            "종목 검색",
            placeholder="예: 005930 · 삼성전자 · Samsung · KR7005930003 · 005930 KS Equity",
            help="이름·티커·ISIN·Bloomberg·RIC·DART 어떤 표기든 OK.",
        )
    with range_col:
        years = st.select_slider(
            "조회 기간", options=[1, 2, 3, 5, 10],
            value=1, format_func=lambda y: f"최근 {y}년",
        )

    # Resolve security
    sec: dict | None = None
    if query:
        sec = md.lookup_security(query)
        if not sec:
            # 부분 일치 후보 제시
            hits = md.search_securities(query, limit=8)
            if hits:
                st.warning("정확한 매칭이 없어요. 후보 중에 골라보세요:")
                labels = [
                    f"{h['ticker']} · {h['name_kr']} ({h['name_en']})" for h in hits
                ]
                pick = st.selectbox("후보", labels, index=0, label_visibility="collapsed")
                sec = hits[labels.index(pick)] if pick else None
            else:
                st.info("매칭되는 종목이 없습니다.")

    if sec:
        # Header
        h_left, h_right = st.columns([3, 2])
        with h_left:
            badges = ""
            if sec.get("kospi200"):
                badges += "<span class='md-pill'>KOSPI 200</span>"
            if sec.get("kosdaq150"):
                badges += "<span class='md-pill'>KOSDAQ 150</span>"
            if sec.get("krx300"):
                badges += "<span class='md-pill'>KRX 300</span>"
            st.markdown(
                f"### {sec['name_kr'] or '—'} "
                f"<span style='color:#666; font-weight:400; font-size:18px;'>"
                f"{sec['name_en'] or ''}</span>",
                unsafe_allow_html=True,
            )
            st.markdown(badges, unsafe_allow_html=True)

        with h_right:
            st.markdown(
                f"<div class='md-meta'>"
                f"<b>Ticker</b> {sec['ticker']} &nbsp; · &nbsp; "
                f"<b>ISIN</b> {sec['isin'] or '—'} &nbsp; · &nbsp; "
                f"<b>BBG</b> {sec['bloomberg'] or '—'} &nbsp; · &nbsp; "
                f"<b>RIC</b> {sec['ric'] or '—'}<br>"
                f"<b>Market</b> {sec['market'] or '—'} &nbsp; · &nbsp; "
                f"<b>Sector</b> {sec['sector_name_en'] or '—'} &nbsp; · &nbsp; "
                f"<b>Listed</b> {sec['listing_date'] or '—'}"
                f"</div>",
                unsafe_allow_html=True,
            )

        start_d, end_d = md.default_date_range(years=years)

        # Fetch
        with st.spinner("Fetching OHLCV from KRX…"):
            ohlcv_df = md.ohlcv(sec["ticker"], start_d.isoformat(), end_d.isoformat())
        with st.spinner("Fetching foreign ownership…"):
            fo_df = md.foreign_ownership(sec["ticker"], start_d.isoformat(), end_d.isoformat())

        if ohlcv_df.empty:
            st.warning("KRX에서 OHLCV를 가져오지 못했어요. 종목코드를 확인하거나 잠시 후 다시 시도해주세요.")
        else:
            # ✨ Hangang Brief — hero AI report. 가격 차트보다 먼저.
            #    Fetches KOSPI as benchmark for "vs benchmark" lines.
            with st.spinner("Generating Hangang Brief…"):
                kospi_df = md.index_ohlcv("1001",
                                           (end_d - timedelta(days=14)).isoformat(),
                                           end_d.isoformat())
                brief_obj = br.generate_brief(
                    sec=sec,
                    ohlcv_df=ohlcv_df,
                    benchmark_df=kospi_df,
                    fo_df=fo_df if not fo_df.empty else None,
                    benchmark_label="KOSPI",
                )
            br.render_brief(brief_obj)

            # Quick stats
            last = ohlcv_df.iloc[-1]
            first = ohlcv_df.iloc[0]
            ret_total = (last["close"] / first["close"] - 1) * 100
            avg_vol = ohlcv_df["volume"].mean()
            high_52w = ohlcv_df["high"].max()
            low_52w = ohlcv_df["low"].min()
            color = "#2f7a3a" if ret_total >= 0 else "#b14a3a"
            sign = "+" if ret_total >= 0 else ""

            s1, s2, s3, s4 = st.columns(4)
            with s1:
                st.markdown(
                    f"<div class='md-kpi-label'>Last close (KRW)</div>"
                    f"<div class='md-kpi-val'>{last['close']:,.0f}</div>",
                    unsafe_allow_html=True,
                )
            with s2:
                st.markdown(
                    f"<div class='md-kpi-label'>Period return</div>"
                    f"<div class='md-kpi-val' style='color:{color}'>{sign}{ret_total:.1f}%</div>",
                    unsafe_allow_html=True,
                )
            with s3:
                st.markdown(
                    f"<div class='md-kpi-label'>{years}y high / low</div>"
                    f"<div class='md-kpi-val'>{high_52w:,.0f} / {low_52w:,.0f}</div>",
                    unsafe_allow_html=True,
                )
            with s4:
                if not fo_df.empty:
                    last_fo = fo_df["foreign_pct"].iloc[-1]
                    st.markdown(
                        f"<div class='md-kpi-label'>Foreign ownership</div>"
                        f"<div class='md-kpi-val'>{last_fo:.2f}%</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f"<div class='md-kpi-label'>Avg daily volume</div>"
                        f"<div class='md-kpi-val'>{avg_vol:,.0f}</div>",
                        unsafe_allow_html=True,
                    )

            st.markdown("&nbsp;", unsafe_allow_html=True)

            # Chart selection
            chart_kind = st.radio(
                "Chart", ["Line + Value", "Candlestick"],
                horizontal=True, label_visibility="collapsed",
            )
            if chart_kind == "Line + Value":
                st.plotly_chart(ch.price_chart(ohlcv_df, name=sec["name_kr"] or sec["ticker"]),
                                use_container_width=True)
            else:
                st.plotly_chart(ch.candlestick(ohlcv_df, name=sec["name_kr"] or sec["ticker"]),
                                use_container_width=True)

            if not fo_df.empty:
                st.plotly_chart(ch.foreign_ownership_chart(fo_df), use_container_width=True)

            with st.expander("📋 OHLCV 원본 테이블"):
                st.dataframe(ohlcv_df.tail(60), use_container_width=True, hide_index=True)

            # Quick single-stock export
            st.markdown("<div class='md-sec-h'>이 종목만 빠르게 export</div>", unsafe_allow_html=True)
            qf1, qf2, qf3 = st.columns([2, 2, 6])
            with qf1:
                qfmt = st.selectbox("format", ["csv", "xlsx", "json"], key="single_fmt")
            with qf2:
                include_fo = st.checkbox("외국인 보유 포함", value=True)

            export_df = ohlcv_df.copy()
            if include_fo and not fo_df.empty:
                export_df = export_df.merge(
                    fo_df[["date", "foreign_pct", "foreign_shares", "limit_exhausted_pct"]],
                    on="date", how="left",
                )
            with qf3:
                st.markdown("&nbsp;", unsafe_allow_html=True)
                st.download_button(
                    "⬇ 다운로드",
                    data=ex.to_bytes(export_df, qfmt),
                    file_name=ex.filename(f"{sec['ticker']}_{sec['name_kr']}", qfmt),
                    mime=ex.MIME[qfmt],
                    use_container_width=True,
                )


# ============= TAB 3: 데이터 추출 ======================================
with tab_extract:
    st.markdown("<div class='md-sec-h'>원하는 종목을 묶음으로 추출</div>", unsafe_allow_html=True)
    st.caption(
        "여러 종목을 한 번에 받아 long-format 또는 wide-format 으로 export. "
        "다종목 백테스트·리서치 노트북·BI 도구에 그대로 import."
    )

    col_in, col_opt = st.columns([3, 2])

    with col_in:
        st.markdown("**1️⃣ 종목 입력**")
        mode = st.radio(
            "입력 방식",
            ["검색해서 추가", "지수 멤버 일괄", "티커 직접 붙여넣기", "CSV 업로드"],
            horizontal=True,
            label_visibility="collapsed",
        )

        if "extract_tickers" not in st.session_state:
            st.session_state.extract_tickers = []

        if mode == "검색해서 추가":
            q = st.text_input("종목 검색", key="extract_q",
                              placeholder="이름 또는 티커")
            if q:
                hits = md.search_securities(q, limit=8)
                if hits:
                    labels = [f"{h['ticker']} · {h['name_kr']}" for h in hits]
                    pick = st.multiselect("후보에서 선택", labels, key="extract_pick")
                    if st.button("➕ 추가", use_container_width=False):
                        for lbl in pick:
                            t = lbl.split(" · ")[0]
                            if t not in st.session_state.extract_tickers:
                                st.session_state.extract_tickers.append(t)

        elif mode == "지수 멤버 일괄":
            idx_pick = st.selectbox("지수 선택", list(md.INDEX_CHOICES.keys()))
            if st.button(f"➕ {idx_pick} 전체 추가", use_container_width=False):
                members = md.index_members(idx_pick)
                for t in members["ticker"].tolist():
                    if t not in st.session_state.extract_tickers:
                        st.session_state.extract_tickers.append(t)

        elif mode == "티커 직접 붙여넣기":
            raw = st.text_area("티커 (쉼표·줄바꿈 구분)",
                               placeholder="005930\n000660\n373220")
            if st.button("➕ 추가", use_container_width=False) and raw:
                cleaned = [t.strip().zfill(6) for t in raw.replace(",", "\n").splitlines()
                           if t.strip()]
                for t in cleaned:
                    if t not in st.session_state.extract_tickers:
                        st.session_state.extract_tickers.append(t)

        elif mode == "CSV 업로드":
            up = st.file_uploader("CSV (첫 컬럼이 ticker)", type=["csv"])
            if up is not None:
                try:
                    df_up = pd.read_csv(up, dtype=str)
                    col = df_up.columns[0]
                    for t in df_up[col].dropna().astype(str).str.strip().str.zfill(6):
                        if t not in st.session_state.extract_tickers:
                            st.session_state.extract_tickers.append(t)
                    st.success(f"{len(df_up)}개 티커 추가")
                except Exception as e:
                    st.error(f"CSV 파싱 실패: {e}")

        # current basket
        st.markdown(f"**선택된 종목 — {len(st.session_state.extract_tickers)}개**")
        if st.session_state.extract_tickers:
            cols_clear = st.columns([6, 1])
            with cols_clear[1]:
                if st.button("Clear", use_container_width=True):
                    st.session_state.extract_tickers = []
                    st.rerun()
            preview_rows = []
            for t in st.session_state.extract_tickers[:50]:
                rec = md.lookup_security(t)
                preview_rows.append({
                    "ticker": t,
                    "name_kr": rec["name_kr"] if rec else "(unknown)",
                    "market": rec["market"] if rec else "",
                })
            st.dataframe(pd.DataFrame(preview_rows), use_container_width=True,
                         hide_index=True, height=200)
            if len(st.session_state.extract_tickers) > 50:
                st.caption(f"+ {len(st.session_state.extract_tickers) - 50}개 더 (export에는 모두 포함)")

    with col_opt:
        st.markdown("**2️⃣ 기간·옵션**")
        today = date.today()
        d_start = st.date_input("시작일", value=today - timedelta(days=365))
        d_end = st.date_input("종료일", value=today)
        freq_label = st.selectbox("빈도", ["일봉 (d)", "주봉 (w)", "월봉 (m)"], index=0)
        freq_code = freq_label[freq_label.index("(") + 1: freq_label.index(")")]
        adjusted = st.checkbox("수정주가 (액면분할 반영)", value=True)
        shape = st.radio(
            "결과 형태",
            ["Long (종목+날짜 행)", "Wide (날짜 행 · 종목 열)"],
            help="Long은 분석/DB용, Wide는 엑셀 보기 좋음.",
        )

        st.markdown("**3️⃣ 포맷 & 다운로드**")
        fmt = st.selectbox("포맷", ["csv", "xlsx", "json"])

        ready = bool(st.session_state.extract_tickers) and d_end >= d_start
        if not ready:
            st.button("⬇ 추출 & 다운로드", disabled=True, use_container_width=True)
            st.caption("종목과 기간을 선택해주세요.")
        else:
            if st.button("⚙ 데이터 가져오기", type="primary", use_container_width=True):
                with st.spinner(f"{len(st.session_state.extract_tickers)}개 종목 fetching…"):
                    long_df = md.bulk_ohlcv(
                        st.session_state.extract_tickers,
                        d_start.isoformat(), d_end.isoformat(),
                        freq=freq_code, adjusted=adjusted,
                    )

                if long_df.empty:
                    st.error("어떤 종목도 데이터를 가져오지 못했어요.")
                else:
                    # join with names for human-readability
                    name_map = {t: (md.lookup_security(t) or {}).get("name_kr", "")
                                for t in long_df["ticker"].unique()}
                    long_df["name_kr"] = long_df["ticker"].map(name_map)

                    if shape.startswith("Wide"):
                        out = ex.wide_pivot(long_df, value_col="close")
                        out_label = "wide_close"
                    else:
                        out = long_df
                        out_label = "long_ohlcv"

                    st.success(
                        f"{long_df['ticker'].nunique():,}개 종목 · "
                        f"{len(long_df):,}행 가져왔어요."
                    )
                    st.dataframe(out.head(20), use_container_width=True, hide_index=True)

                    st.download_button(
                        f"⬇ {fmt.upper()} 다운로드 ({out_label})",
                        data=ex.to_bytes(out, fmt),
                        file_name=ex.filename(f"hangang_{out_label}", fmt),
                        mime=ex.MIME[fmt],
                        use_container_width=True,
                    )
