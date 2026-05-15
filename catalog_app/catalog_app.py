"""
Mandata Data Catalog — 외부 고객용 데이터 마켓플레이스 (Streamlit MVP).

워크플로우:
    1. 카탈로그 로드 (parquet 자동 / 업로드 / 데모)
    2. 필터: 섹터 multiselect + 시그널 점수 슬라이더 + 검색
    3. 카탈로그 테이블 — 회사별 ➕ 장바구니 추가
    4. 장바구니 — 선택 회사 목록 + 다운로드 버튼

배포: Streamlit Cloud에 별도 앱으로 등록
    Main file path: catalog_app/catalog_app.py
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

from catalog_app.data_loader import (
    load_latest_catalog, load_from_upload, demo_catalog, normalize_catalog,
    list_catalogs,
)
from catalog_app.cart import (
    get_cart, add_to_cart, remove_from_cart, clear_cart, cart_size, in_cart,
)
from catalog_app.export import build_export_xlsx, export_filename


# ── 페이지 설정 ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Mandata Data Catalog",
    page_icon="🛒",
    layout="wide",
)

# ── 헤더 ──────────────────────────────────────────────────────────────────
st.markdown(
    """
<div style='background:linear-gradient(135deg,#1E40AF 0%,#3B82F6 100%);
            color:white;padding:24px 28px;border-radius:12px;margin-bottom:16px'>
  <div style='font-size:12px;letter-spacing:0.08em;opacity:0.85;margin-bottom:4px'>
    MANDATA · ALT-DATA MARKETPLACE
  </div>
  <div style='font-size:24px;font-weight:800;letter-spacing:-0.02em'>
    🛒 Data Catalog — 회사·티커 장바구니
  </div>
  <div style='font-size:14px;opacity:0.9;margin-top:6px'>
    필터로 관심 회사를 추리고, 카트에 담아 데이터를 다운로드하세요.
  </div>
</div>
""",
    unsafe_allow_html=True,
)


# ── 데이터 소스 선택 ───────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def _load_catalog_cached(source: str, file_id: str | None) -> pd.DataFrame | None:
    """카탈로그 캐시 — 10분."""
    if source == "auto":
        return load_latest_catalog()
    return None


with st.sidebar:
    st.markdown("### 📦 카탈로그 소스")

    available = list_catalogs()
    source_options = []
    if available:
        source_options.append(f"자동 ({len(available)}개 파일)")
    source_options.append("직접 업로드")
    source_options.append("데모 데이터")

    source_choice = st.radio(
        "데이터 출처", source_options,
        label_visibility="collapsed",
    )

    catalog: pd.DataFrame | None = None
    if source_choice.startswith("자동"):
        catalog = _load_catalog_cached("auto", str(available[0]) if available else None)
        if catalog is not None:
            st.caption(f"📂 `{available[0].name}` 로드됨 ({len(catalog):,} 회사)")
    elif source_choice == "직접 업로드":
        up = st.file_uploader("parquet / xlsx / csv", type=["parquet", "xlsx", "csv"])
        if up:
            catalog = load_from_upload(up)
            if catalog is not None:
                st.caption(f"📂 `{up.name}` 로드됨 ({len(catalog):,} 회사)")
    else:
        catalog = demo_catalog()
        st.caption(f"🎭 데모 데이터 ({len(catalog)} 회사)")


# ── 카탈로그 로드 실패시 ───────────────────────────────────────────────────
if catalog is None or catalog.empty:
    st.info(
        "📭 카탈로그가 비어있습니다. 사이드바에서 다음 중 하나 선택:\n\n"
        "1. **자동** — `catalog/` 폴더에 분석 앱이 export한 parquet 파일이 있어야 함\n"
        "2. **직접 업로드** — 분석 앱에서 다운받은 catalog.parquet을 직접 업로드\n"
        "3. **데모 데이터** — 가짜 60개 회사로 UI 테스트"
    )
    st.stop()

catalog = normalize_catalog(catalog)


# ── 필터 영역 ─────────────────────────────────────────────────────────────
st.markdown("### 🔍 필터")
fcol1, fcol2, fcol3 = st.columns([2, 2, 3])

with fcol1:
    sectors_all = sorted(catalog["sector"].dropna().unique().tolist())
    sectors_sel = st.multiselect(
        "섹터", sectors_all, default=[],
        help="비워두면 전체. 여러 개 선택 가능.",
    )

with fcol2:
    sig_range = st.slider(
        "시그널 점수", 0.0, 1.0, (0.0, 1.0), 0.05,
        help="매출-주가 상관 강도 (0=무관, 1=완벽 상관).",
    )

with fcol3:
    search = st.text_input(
        "🔎 검색 (회사명 / 티커)",
        placeholder="예: 농심 / 004370",
    )

fcol4, fcol5 = st.columns([1, 1])
with fcol4:
    require_dart = st.checkbox("DART 매칭된 회사만", value=False)
with fcol5:
    require_stock = st.checkbox("주가 데이터 있는 회사만", value=False)


# ── 필터 적용 ─────────────────────────────────────────────────────────────
filt = catalog.copy()
if sectors_sel:
    filt = filt[filt["sector"].isin(sectors_sel)]
filt = filt[
    (filt["signal_score"] >= sig_range[0]) & (filt["signal_score"] <= sig_range[1])
]
if search:
    s = search.strip().lower()
    mask = (
        filt["company"].str.lower().str.contains(s, na=False)
        | filt["ticker"].astype(str).str.contains(s, na=False)
    )
    filt = filt[mask]
if require_dart:
    filt = filt[filt["has_dart"]]
if require_stock:
    filt = filt[filt["has_stock"]]


# ── 결과 요약 ─────────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
m1.metric("전체 회사", f"{len(catalog):,}")
m2.metric("필터 결과", f"{len(filt):,}", delta=f"{len(filt) - len(catalog):+,}")
m3.metric("🛒 카트", f"{cart_size()}")
m4.metric("평균 시그널 점수",
          f"{filt['signal_score'].mean():.2f}" if len(filt) else "—")

st.divider()


# ── 카탈로그 테이블 + 카트 추가 ───────────────────────────────────────────
tcol_left, tcol_right = st.columns([3, 2])

with tcol_left:
    st.markdown(f"### 📋 카탈로그 ({len(filt)}개)")
    if filt.empty:
        st.info("필터 결과 없음. 조건을 완화하세요.")
    else:
        # 인터랙티브 그리드 — Streamlit 1.40의 dataframe selection
        st.caption("아래 표에서 행을 클릭 → 우측 '카트 추가' 버튼으로 담기")
        show_df = filt[[
            "company", "ticker", "sector", "signal_score",
            "mom_growth", "coverage_months", "has_dart", "has_stock",
        ]].rename(columns={
            "company":         "회사",
            "ticker":          "티커",
            "sector":          "섹터",
            "signal_score":    "시그널",
            "mom_growth":      "MoM %",
            "coverage_months": "커버 (월)",
            "has_dart":        "DART",
            "has_stock":       "주가",
        })

        event = st.dataframe(
            show_df,
            hide_index=True,
            use_container_width=True,
            height=520,
            on_select="rerun",
            selection_mode="multi-row",
            key="catalog_table",
        )

        # 선택된 행 → 카트 추가 버튼
        selected_rows = event.selection.rows if hasattr(event, "selection") else []
        if selected_rows:
            picked = filt.iloc[selected_rows]["company"].tolist()
            b1, b2 = st.columns([2, 1])
            with b1:
                if st.button(
                    f"➕ 선택된 {len(picked)}개 회사 카트에 추가",
                    type="primary", use_container_width=True,
                ):
                    for c in picked:
                        add_to_cart(c)
                    st.success(f"{len(picked)}개 회사가 카트에 담겼습니다.")
                    st.rerun()
            with b2:
                if st.button("👁 미리보기 (선택)", use_container_width=True):
                    st.dataframe(filt.iloc[selected_rows], hide_index=True,
                                 use_container_width=True)


# ── 장바구니 패널 ─────────────────────────────────────────────────────────
with tcol_right:
    st.markdown(f"### 🛒 장바구니 ({cart_size()})")

    cart_set = get_cart()
    if not cart_set:
        st.info("카트가 비어있습니다.\n좌측 표에서 행을 선택하고 '카트에 추가' 클릭.")
    else:
        cart_df = catalog[catalog["company"].isin(cart_set)][
            ["company", "ticker", "sector", "signal_score"]
        ].rename(columns={
            "company": "회사", "ticker": "티커",
            "sector": "섹터", "signal_score": "시그널",
        })
        st.dataframe(cart_df, hide_index=True, use_container_width=True, height=320)

        # 다운로드 + 비우기
        filter_desc = []
        if sectors_sel:
            filter_desc.append(f"섹터={','.join(sectors_sel)}")
        if sig_range != (0.0, 1.0):
            filter_desc.append(f"시그널={sig_range[0]}~{sig_range[1]}")
        if require_dart:
            filter_desc.append("DART매칭")
        if require_stock:
            filter_desc.append("주가데이터")
        if search:
            filter_desc.append(f"검색='{search}'")
        filter_summary = " · ".join(filter_desc) if filter_desc else "(필터 미적용)"

        xlsx_bytes = build_export_xlsx(catalog, cart_set, filter_summary)
        st.download_button(
            "📥 선택 회사 데이터 다운로드 (xlsx)",
            data=xlsx_bytes,
            file_name=export_filename(),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True,
        )

        if st.button("🗑 카트 비우기", use_container_width=True):
            clear_cart()
            st.rerun()

        # 개별 제거
        with st.expander(f"📝 개별 제거 ({cart_size()})", expanded=False):
            for company in sorted(cart_set):
                c1, c2 = st.columns([4, 1])
                c1.markdown(f"<div style='padding:6px 0;font-size:13px'>{company}</div>",
                            unsafe_allow_html=True)
                if c2.button("✕", key=f"rm_{company}", use_container_width=True):
                    remove_from_cart(company)
                    st.rerun()


# ── Footer ────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "💡 Mandata Alt-Data Catalog · 카탈로그는 분석 앱 Step 6의 'Catalog Export' 버튼으로 생성. "
    "문의: yonghan@mandata.kr"
)
