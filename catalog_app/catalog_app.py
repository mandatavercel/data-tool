"""
Mandata Data Catalog — 글로벌 기관투자자용 Alt-Data Marketplace (Streamlit).

워크플로우:
    1. 카탈로그 로드 (parquet 자동 / 업로드 / 데모)
    2. 사이드바 — 12 카테고리 고도화 필터 (시장·시가총액·데이터품질·시그널·펀더 등)
    3. 메인 — 필터 칩 + Quick-add 버튼(Top 50/100/200/500) + 카탈로그 테이블
    4. 우측 컴팩트 카트 — 수량/합계/결제 버튼
    5. 결제 페이지 — 라인아이템 단가 breakdown + 묶음 할인 + VAT + 총액

배포: Streamlit Cloud
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
    get_cart, add_to_cart, remove_from_cart, clear_cart, cart_size,
)
from catalog_app.export import (
    build_export_xlsx, export_filename,
    build_paid_data_xlsx, paid_filename,
)
from catalog_app.sample_data import monthly_aggregates
from catalog_app.filters import (
    CATEGORY_REGISTRY, PRESETS,
    empty_selection, apply_filters, summarize_selection,
    active_chips, available_categories, category_columns_present,
    _col_range,
)
from catalog_app.pricing import (
    attach_unit_price, top_n_companies,
    build_checkout_lines, calc_totals, fmt_usd, VAT_RATE, VOLUME_TIERS,
)


# ── 페이지 설정 ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Mandata Data Catalog — Institutional",
    page_icon="🛒",
    layout="wide",
)

# ── 헤더 ──────────────────────────────────────────────────────────────────
st.markdown(
    """
<div style='background:linear-gradient(135deg,#0F172A 0%,#1E40AF 50%,#3B82F6 100%);
            color:white;padding:20px 26px;border-radius:12px;margin-bottom:14px'>
  <div style='font-size:12px;letter-spacing:0.08em;opacity:0.85;margin-bottom:4px'>
    MANDATA · INSTITUTIONAL ALT-DATA MARKETPLACE
  </div>
  <div style='font-size:22px;font-weight:800;letter-spacing:-0.02em'>
    🛒 Data Catalog — Global Coverage · 12 Filter Categories
  </div>
  <div style='font-size:13px;opacity:0.9;margin-top:4px'>
    Universe · Liquidity · Signal · Coverage · ESG. 기관 투자자용 정밀 필터링.
  </div>
</div>
""",
    unsafe_allow_html=True,
)


# ── 데이터 로드 ───────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def _load_catalog_cached(source: str, file_id: str | None) -> pd.DataFrame | None:
    if source == "auto":
        return load_latest_catalog()
    return None


# ── 카탈로그 소스 — 메인 헤더 바로 아래 compact bar ───────────────────────
available = list_catalogs()
source_options: list[str] = []
if available:
    source_options.append(f"자동 ({len(available)}개 파일)")
source_options.append("직접 업로드")
source_options.append("데모 데이터")

# 현재 선택 — session_state 에 보존
if "src_choice" not in st.session_state:
    st.session_state["src_choice"] = source_options[0]
# 옵션 목록이 바뀌면 default fallback
if st.session_state["src_choice"] not in source_options:
    st.session_state["src_choice"] = source_options[0]

with st.expander(
    f"📦 카탈로그 소스 — 현재: **{st.session_state['src_choice']}**  (변경하려면 클릭)",
    expanded=False,
):
    sc1, sc2 = st.columns([2, 3])
    with sc1:
        st.session_state["src_choice"] = st.radio(
            "데이터 출처",
            source_options,
            index=source_options.index(st.session_state["src_choice"]),
            label_visibility="collapsed",
        )
    with sc2:
        if st.session_state["src_choice"] == "직접 업로드":
            uploaded_file = st.file_uploader(
                "parquet / xlsx / csv",
                type=["parquet", "xlsx", "csv"],
                key="src_uploader",
            )
        else:
            uploaded_file = None
            st.caption(
                "💡 자동: `catalog/` 폴더의 parquet · "
                "직접 업로드: 분석 앱 export 파일 · "
                "데모: 글로벌 6개 시장 120개 가짜 데이터"
            )

source_choice = st.session_state["src_choice"]
catalog: pd.DataFrame | None = None
if source_choice.startswith("자동"):
    catalog = _load_catalog_cached("auto", str(available[0]) if available else None)
elif source_choice == "직접 업로드":
    if uploaded_file:
        catalog = load_from_upload(uploaded_file)
else:
    catalog = demo_catalog()


# ── 카탈로그 없음 → 안내 후 종료 ───────────────────────────────────────────
if catalog is None or catalog.empty:
    st.info(
        "📭 카탈로그가 비어있습니다. 사이드바에서 선택:\n\n"
        "1. **자동** — `catalog/` 폴더에 분석 앱이 export한 parquet\n"
        "2. **직접 업로드** — 분석 앱에서 다운받은 catalog.parquet\n"
        "3. **데모 데이터** — 120개 글로벌 가짜 회사 (KR/US/JP/CN/HK/EU)"
    )
    st.stop()

catalog = normalize_catalog(catalog)
catalog = attach_unit_price(catalog)


# ── 필터 선택 상태 (session) ──────────────────────────────────────────────
if "filter_sel" not in st.session_state:
    st.session_state["filter_sel"] = empty_selection()

sel: dict = st.session_state["filter_sel"]


# ── 사이드바 — 12 카테고리 필터 ───────────────────────────────────────────
def _render_numeric(col: str, label: str, kind: str) -> tuple[float, float] | None:
    s = pd.to_numeric(catalog[col], errors="coerce").dropna()
    if s.empty:
        return None
    lo, hi = float(s.min()), float(s.max())
    if lo == hi:
        st.caption(f"{label}: {lo:.2f} (단일값)")
        return None
    cur = sel["numeric"].get(col, (lo, hi))
    cur_lo, cur_hi = cur
    cur_lo = max(min(cur_lo, hi), lo)
    cur_hi = max(min(cur_hi, hi), lo)
    rng = hi - lo
    if kind == "numeric_0_1":
        step, fmt = 0.05, "%.2f"
    else:
        step, fmt = max(round(rng / 100, 2), 0.01), "%.2f"
    return st.slider(
        label, min_value=lo, max_value=hi,
        value=(cur_lo, cur_hi), step=step, format=fmt,
        key=f"sl_{col}",
    )


with st.sidebar:
    st.markdown(
        f"<div style='display:flex;align-items:baseline;justify-content:space-between;"
        f"margin:-4px 0 8px 0'>"
        f"<div style='font-size:18px;font-weight:800;color:#0F172A'>🎛 필터</div>"
        f"<div style='font-size:11px;color:#64748B'>"
        f"{len(catalog):,} 회사 · {len(catalog.columns)} 컬럼</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    preset_choice = st.selectbox(
        "💡 프리셋",
        ["(없음)"] + list(PRESETS.keys()),
        help="기관투자자 시나리오. 선택하면 관련 필터가 자동 적용.",
    )
    pcol1, pcol2 = st.columns(2)
    apply_preset = pcol1.button("적용", use_container_width=True, type="primary")
    reset_filters = pcol2.button("초기화", use_container_width=True)
    if apply_preset and preset_choice != "(없음)":
        new_sel = empty_selection()
        preset = PRESETS[preset_choice]
        for k, v in preset.items():
            new_sel[k] = v if isinstance(v, dict) else list(v)
        st.session_state["filter_sel"] = new_sel
        st.rerun()
    if reset_filters:
        st.session_state["filter_sel"] = empty_selection()
        st.rerun()

    st.markdown("#### 🔎 검색")
    sel["search"] = st.text_input(
        "회사 / 티커 / ISIN",
        value=sel.get("search", ""),
        placeholder="예: 농심 / AAPL / KR7005930003",
        label_visibility="collapsed",
    )

    available_cats = available_categories(catalog)
    for cat_key in available_cats:
        meta = CATEGORY_REGISTRY[cat_key]
        cols_present = category_columns_present(catalog, cat_key)
        if not cols_present:
            continue
        with st.expander(meta["label"], expanded=(cat_key in ("size_liquidity", "signal"))):
            for col, label, kind in cols_present:
                if kind == "categorical":
                    opts = sorted([v for v in catalog[col].dropna().astype(str).unique() if v])
                    if not opts: continue
                    cur = [c for c in sel["categorical"].get(col, []) if c in opts]
                    new = st.multiselect(label, opts, default=cur, key=f"ms_{col}")
                    if new: sel["categorical"][col] = new
                    else:   sel["categorical"].pop(col, None)
                elif kind == "categorical_multi":
                    tokens: set[str] = set()
                    for v in catalog[col].dropna().astype(str):
                        tokens.update(t.strip() for t in v.split(",") if t.strip())
                    opts = sorted(tokens)
                    if not opts: continue
                    cur = [c for c in sel["categorical_multi"].get(col, []) if c in opts]
                    new = st.multiselect(label, opts, default=cur, key=f"msm_{col}")
                    if new: sel["categorical_multi"][col] = new
                    else:   sel["categorical_multi"].pop(col, None)
                elif kind == "tag_list":
                    tokens: set[str] = set()
                    for v in catalog[col].dropna().astype(str):
                        tokens.update(t.strip() for t in v.split(",") if t.strip())
                    opts = sorted(tokens)
                    if not opts: continue
                    cur = [c for c in sel["tag_list"].get(col, []) if c in opts]
                    new = st.multiselect(label, opts, default=cur, key=f"tag_{col}")
                    if new: sel["tag_list"][col] = new
                    else:   sel["tag_list"].pop(col, None)
                elif kind in ("numeric", "numeric_0_1", "numeric_pm", "numeric_log"):
                    val = _render_numeric(col, label, kind)
                    if val is None: continue
                    lo, hi = val
                    full_lo, full_hi = _col_range(catalog, col)
                    if lo <= full_lo + 1e-9 and hi >= full_hi - 1e-9:
                        sel["numeric"].pop(col, None)
                    else:
                        sel["numeric"][col] = (float(lo), float(hi))
                elif kind == "bool":
                    state_now = ("required" if col in sel["bool_required"]
                                 else "excluded" if col in sel["bool_excluded"]
                                 else "any")
                    new_state = st.radio(
                        label, ["any", "required", "excluded"],
                        index=["any", "required", "excluded"].index(state_now),
                        horizontal=True,
                        format_func=lambda x: {"any": "전체", "required": "✓ 필수", "excluded": "✗ 제외"}[x],
                        key=f"bool_{col}",
                    )
                    sel["bool_required"] = [c for c in sel["bool_required"] if c != col]
                    sel["bool_excluded"] = [c for c in sel["bool_excluded"] if c != col]
                    if new_state == "required":  sel["bool_required"].append(col)
                    elif new_state == "excluded": sel["bool_excluded"].append(col)


# ── 필터 적용 ─────────────────────────────────────────────────────────────
filt = apply_filters(catalog, sel)

# ── 활성 필터 칩 ──────────────────────────────────────────────────────────
chips = active_chips(sel, catalog)
if chips:
    chip_html = "".join(
        f"<span style='display:inline-block;background:#EEF2FF;color:#1E3A8A;"
        f"padding:3px 10px;border-radius:14px;margin:2px 4px 2px 0;font-size:12px;"
        f"border:1px solid #C7D2FE'><b>{lbl}</b> · {val}</span>"
        for lbl, val in chips
    )
    st.markdown(
        f"<div style='margin:6px 0;padding:6px 0'>"
        f"<span style='font-size:12px;color:#64748B;margin-right:6px'>활성 필터:</span>"
        f"{chip_html}</div>",
        unsafe_allow_html=True,
    )

# ── 결과 요약 ─────────────────────────────────────────────────────────────
cart_set_now = get_cart()
cart_lines_now = build_checkout_lines(catalog, cart_set_now)
totals_now = calc_totals(cart_lines_now)

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("전체 회사", f"{len(catalog):,}")
m2.metric("필터 결과", f"{len(filt):,}",
          delta=f"{len(filt) - len(catalog):+,}", delta_color="off")
m3.metric("🛒 카트", f"{cart_size()}")
m4.metric("평균 시그널",
          f"{filt['signal_score'].mean():.2f}" if len(filt) and "signal_score" in filt else "—")
m5.metric("카트 합계", fmt_usd(totals_now.grand_total) if totals_now.qty else "—")

# ── Quick-Add 버튼 + Checkout 트리거 ──────────────────────────────────────
st.markdown(
    "<div style='font-size:13px;color:#475569;margin:6px 0 4px 0'>"
    "⚡ <b>Today's Quick-Add</b> · 현재 필터 결과의 품질 상위를 미리보기 → 검토 후 카트로"
    "</div>",
    unsafe_allow_html=True,
)
qa1, qa2, qa3, qa4, qa5, qa6 = st.columns([1, 1, 1, 1, 1, 1.6])
quick_sizes = [(qa1, 50), (qa2, 100), (qa3, 200), (qa4, 500)]
for col_widget, n in quick_sizes:
    label = f"Top {n}"
    disabled = (len(filt) == 0)
    if col_widget.button(label, use_container_width=True, disabled=disabled,
                         key=f"top_{n}",
                         help=f"필터 결과 상위 {n}개를 먼저 미리보기. 검토 후 카트 추가/취소 선택"):
        st.session_state["preview_topn"] = n
        st.rerun()

if qa5.button("⭐ 전체 결과", use_container_width=True, disabled=(len(filt) == 0),
              help=f"필터 결과 {len(filt)}개 전체 미리보기"):
    st.session_state["preview_topn"] = len(filt)
    st.rerun()

if qa6.button(
    f"💳 결제하기  ({cart_size()}개 · {fmt_usd(totals_now.grand_total)})",
    type="primary", use_container_width=True,
    disabled=(cart_size() == 0),
):
    st.session_state["show_checkout"] = True

st.divider()


# ── Top N 프리뷰 패널 ─────────────────────────────────────────────────────
preview_n = st.session_state.get("preview_topn")
if preview_n is not None and len(filt):
    requested = preview_n
    picks = top_n_companies(filt, requested)
    preview_df = filt[filt["company"].isin(picks)].copy()
    # 품질 점수 기준으로 정렬 (top_n_companies 와 동일)
    from catalog_app.pricing import quality_rank as _qrank
    preview_df = preview_df.assign(_q=_qrank(preview_df)).sort_values(
        "_q", ascending=False).drop(columns=["_q"])

    actual_n = len(preview_df)
    preview_total = float(preview_df["unit_price"].sum()) if "unit_price" in preview_df else 0.0

    # 배너
    st.markdown(
        f"<div style='background:#FFF7ED;border:1px solid #FED7AA;border-left:6px solid #F59E0B;"
        f"border-radius:8px;padding:12px 16px;margin-bottom:10px'>"
        f"<div style='font-size:11px;color:#9A3412;letter-spacing:0.05em'>"
        f"⚡ TOP {requested} 미리보기 — 필터 결과의 품질 상위 {actual_n}개</div>"
        f"<div style='font-size:14px;color:#7C2D12;margin-top:2px'>"
        f"signal·IC·Sharpe·Hit ratio 합성 점수 기준 · 예상 소계 "
        f"<b>{fmt_usd(preview_total)}</b> "
        f"({actual_n}건 묶음 할인 별도)</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # 프리뷰 테이블 컬럼 — 메인 테이블과 동일 우선순위에서 가용 컬럼만
    _PREVIEW_COL_MAP = [
        ("company",          "회사"),
        ("ticker",           "티커"),
        ("region",           "지역"),
        ("gics_sector",      "GICS"),
        ("market_cap_usd",   "MCap (M$)"),
        ("signal_score",     "시그널"),
        ("ic",               "IC"),
        ("backtest_sharpe",  "Sharpe"),
        ("hit_ratio_pct",    "Hit %"),
        ("data_latency_days","지연(일)"),
        ("unit_price",       "단가 (USD)"),
    ]
    _pcols = [(c, lbl) for c, lbl in _PREVIEW_COL_MAP if c in preview_df.columns]
    show_preview = preview_df[[c for c, _ in _pcols]].rename(
        columns={c: lbl for c, lbl in _pcols}
    )

    pcc = {}
    if "MCap (M$)" in show_preview.columns:
        pcc["MCap (M$)"] = st.column_config.NumberColumn(format="$%.0fM")
    if "시그널" in show_preview.columns:
        pcc["시그널"] = st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f")
    if "Hit %" in show_preview.columns:
        pcc["Hit %"] = st.column_config.NumberColumn(format="%.1f %%")
    if "단가 (USD)" in show_preview.columns:
        pcc["단가 (USD)"] = st.column_config.NumberColumn(format="$%,.0f")

    st.dataframe(
        show_preview, hide_index=True, use_container_width=True, height=420,
        column_config=pcc,
    )

    # 카트에 이미 있는 항목 카운트
    already = sum(1 for c in picks if c in get_cart())
    new_count = actual_n - already

    pb1, pb2, pb3 = st.columns([2.5, 1, 1])
    with pb1:
        if st.button(
            f"🛒 위 {actual_n}개 모두 카트에 추가" + (f"  (신규 {new_count})" if already else ""),
            type="primary", use_container_width=True, key="confirm_preview_add",
        ):
            for c in picks:
                add_to_cart(c)
            st.session_state["preview_topn"] = None
            st.toast(f"{new_count}개 신규 추가 · 카트 총 {cart_size()}개", icon="✅")
            st.rerun()
    with pb2:
        if st.button("✕ 취소", use_container_width=True, key="cancel_preview"):
            st.session_state["preview_topn"] = None
            st.rerun()
    with pb3:
        # 다른 사이즈로 빠르게 전환
        new_size = st.selectbox(
            "다른 사이즈",
            [50, 100, 200, 500, len(filt)],
            index=[50, 100, 200, 500, len(filt)].index(requested)
                if requested in [50, 100, 200, 500, len(filt)] else 0,
            label_visibility="collapsed",
            key="preview_size_switch",
        )
        if new_size != requested:
            st.session_state["preview_topn"] = new_size
            st.rerun()

    st.divider()


# ── 메인: 테이블 + 미니 카트 ──────────────────────────────────────────────
tcol_left, tcol_right = st.columns([5, 1.3])

_DISPLAY_PRIORITY = [
    ("company",          "회사"),
    ("ticker",           "티커"),
    ("region",           "지역"),
    ("gics_sector",      "GICS"),
    ("market_cap_usd",   "MCap (M$)"),
    ("signal_score",     "시그널"),
    ("ic",               "IC"),
    ("backtest_sharpe",  "Sharpe"),
    ("mom_growth",       "MoM %"),
    ("data_latency_days","지연(일)"),
    ("esg_score",        "ESG"),
    ("unit_price",       "단가 (USD)"),
]


def _display_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = [(c, lbl) for c, lbl in _DISPLAY_PRIORITY if c in df.columns]
    if not cols:
        return df.head(0)
    sub = df[[c for c, _ in cols]].rename(columns={c: lbl for c, lbl in cols})
    return sub


with tcol_left:
    st.markdown(f"### 📋 카탈로그 ({len(filt):,})")
    if filt.empty:
        st.info("필터 결과 없음. 사이드바 조건을 완화하거나 '초기화' 버튼 클릭.")
    else:
        st.caption("행 클릭(Shift/Cmd-Click) 으로 다중 선택 → 우측 ‘카트 추가’")
        show_df = _display_df(filt)
        col_config = {}
        if "MCap (M$)" in show_df.columns:
            col_config["MCap (M$)"] = st.column_config.NumberColumn(format="$%.0fM")
        if "시그널" in show_df.columns:
            col_config["시그널"] = st.column_config.ProgressColumn(min_value=0, max_value=1, format="%.2f")
        if "ESG" in show_df.columns:
            col_config["ESG"] = st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.0f")
        if "MoM %" in show_df.columns:
            col_config["MoM %"] = st.column_config.NumberColumn(format="%+.1f %%")
        if "단가 (USD)" in show_df.columns:
            col_config["단가 (USD)"] = st.column_config.NumberColumn(format="$%,.0f")

        event = st.dataframe(
            show_df, hide_index=True, use_container_width=True, height=560,
            on_select="rerun", selection_mode="multi-row", key="catalog_table",
            column_config=col_config,
        )

        selected_rows = event.selection.rows if hasattr(event, "selection") else []
        if selected_rows:
            picked = filt.iloc[selected_rows]["company"].tolist()
            picked_total = float(filt.iloc[selected_rows]["unit_price"].sum())
            b1, b2 = st.columns([2.5, 1])
            with b1:
                if st.button(
                    f"➕ 선택된 {len(picked)}개 카트에 추가 · {fmt_usd(picked_total)}",
                    type="primary", use_container_width=True, key="add_selected",
                ):
                    for c in picked:
                        add_to_cart(c)
                    st.toast(f"{len(picked)}개 회사 카트 추가", icon="✅")
                    st.rerun()
            with b2:
                if st.button("👁 미리보기", use_container_width=True, key="preview_selected"):
                    st.dataframe(filt.iloc[selected_rows], hide_index=True,
                                 use_container_width=True)

# ── 미니 카트 (우측 narrow column) ─────────────────────────────────────────
with tcol_right:
    st.markdown(
        f"<div style='background:#F8FAFC;border:1px solid #E2E8F0;border-radius:10px;"
        f"padding:14px;text-align:center'>"
        f"<div style='font-size:11px;color:#64748B;letter-spacing:0.05em;"
        f"margin-bottom:2px'>🛒 CART</div>"
        f"<div style='font-size:28px;font-weight:800;color:#0F172A;"
        f"line-height:1.1'>{cart_size()}</div>"
        f"<div style='font-size:12px;color:#94A3B8;margin-top:1px'>회사</div>"
        f"<div style='border-top:1px dashed #CBD5E1;margin:10px 0 6px 0'></div>"
        f"<div style='font-size:10px;color:#64748B;text-transform:uppercase;"
        f"letter-spacing:0.08em'>예상 결제액</div>"
        f"<div style='font-size:20px;font-weight:700;color:#1E40AF'>"
        f"{fmt_usd(totals_now.grand_total) if totals_now.qty else '—'}</div>"
        f"<div style='font-size:10px;color:#64748B;margin-top:2px'>"
        f"{totals_now.volume_tier_label if totals_now.qty else 'VAT 포함'}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    if cart_size():
        if st.button("💳 결제하기", type="primary", use_container_width=True,
                     key="open_checkout_side"):
            st.session_state["show_checkout"] = True
        if st.button("🗑 카트 비우기", use_container_width=True, key="clear_cart_side"):
            clear_cart()
            st.rerun()
        with st.expander(f"카트 상세 ({cart_size()})", expanded=False):
            mini = pd.DataFrame([{
                "회사": l.company, "티커": l.ticker,
                "단가": l.unit_price,
            } for l in cart_lines_now])
            st.dataframe(
                mini, hide_index=True, use_container_width=True, height=240,
                column_config={"단가": st.column_config.NumberColumn(format="$%,.0f")},
            )
            for company in sorted(cart_set_now):
                if st.button(f"✕ {company}", key=f"rm_{company}",
                             use_container_width=True):
                    remove_from_cart(company)
                    st.rerun()


# ── 결제하기 Dialog (결제 전 / 결제 후 2단계) ─────────────────────────────
@st.dialog("💳 결제 · 데이터 다운로드", width="large")
def _checkout_dialog():
    cart_now = get_cart()
    paid = st.session_state.get("payment_completed", False)

    # ── 결제 완료 후 — 다운로드 화면 ──────────────────────────────────
    if paid:
        paid_companies = st.session_state.get("paid_companies", [])
        paid_totals = st.session_state.get("paid_totals")
        paid_order_id = st.session_state.get("paid_order_id", "—")

        st.success(
            f"✅ 결제 완료 · {len(paid_companies)}개 회사 · "
            f"{fmt_usd(getattr(paid_totals, 'grand_total', 0))}",
            icon="🎉",
        )
        st.caption(f"Order ID: `{paid_order_id}`")

        st.markdown("#### 📥 데이터 다운로드")
        st.markdown(
            "다운로드 파일은 다음 시트를 포함합니다:\n"
            "- **invoice** — 영수증·결제 내역\n"
            "- **companies** — 구매 회사 메타·단가\n"
            "- **monthly_aggregates** — 회사별 24개월 매출·거래·이용자 시계열\n"
            "- **signal_history** — 회사별 시그널 점수 추이\n"
            "- **summary_by_month** — 전체 합산 월별\n"
            "- **notes** — 데이터 사용 가이드"
        )

        try:
            with st.spinner("데이터 패키지 생성 중..."):
                xlsx_bytes = build_paid_data_xlsx(
                    catalog, list(paid_companies), paid_totals,
                    filter_summary=summarize_selection(sel),
                    n_months=24,
                )
            st.download_button(
                f"📥 데이터 패키지 다운로드 (xlsx · {len(paid_companies)}개 회사 × 24개월)",
                data=xlsx_bytes,
                file_name=paid_filename(),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary", use_container_width=True,
                key="dl_paid_data",
            )
        except Exception as e:
            st.error(f"패키지 생성 실패: {type(e).__name__}: {e}")

        st.divider()

        # 영수증 미리보기
        with st.expander("🧾 영수증 보기", expanded=True):
            if paid_totals is not None:
                rows = [
                    ("Order ID",            paid_order_id),
                    ("회사 수",             str(getattr(paid_totals, "qty", len(paid_companies)))),
                    ("소계",                fmt_usd(getattr(paid_totals, "subtotal", 0))),
                    ("묶음 할인",
                     f"{getattr(paid_totals, 'volume_tier_label', '-')} · "
                     f"-{fmt_usd(getattr(paid_totals, 'volume_discount', 0))}"),
                    ("할인 후",             fmt_usd(getattr(paid_totals, "after_discount", 0))),
                    (f"VAT ({VAT_RATE*100:.0f}%)", fmt_usd(getattr(paid_totals, "tax", 0))),
                    ("총 결제액",           fmt_usd(getattr(paid_totals, "grand_total", 0))),
                ]
                st.dataframe(pd.DataFrame(rows, columns=["항목", "값"]),
                             hide_index=True, use_container_width=True)

        # 닫기 — 카트도 비움
        if st.button("닫기 (카트 비움)", use_container_width=True):
            clear_cart()
            st.session_state["show_checkout"] = False
            st.session_state["payment_completed"] = False
            st.session_state["paid_companies"] = []
            st.session_state["paid_totals"] = None
            st.session_state["paid_order_id"] = None
            st.rerun()
        return

    # ── 결제 전 — 라인아이템·샘플·합산 ────────────────────────────────
    if not cart_now:
        st.info("카트가 비어있습니다.")
        if st.button("닫기"):
            st.session_state["show_checkout"] = False
            st.rerun()
        return

    lines = build_checkout_lines(catalog, cart_now)
    totals = calc_totals(lines)

    # 상단 — 요약
    h1, h2, h3, h4 = st.columns(4)
    h1.metric("회사 수", f"{totals.qty}")
    h2.metric("소계", fmt_usd(totals.subtotal))
    h3.metric(
        f"할인 ({totals.volume_rate*100:.0f}%)",
        f"− {fmt_usd(totals.volume_discount)}",
        delta=totals.volume_tier_label, delta_color="off",
    )
    h4.metric("총 결제액", fmt_usd(totals.grand_total))

    st.divider()

    # 라인아이템
    st.markdown("#### 📑 라인 아이템")
    line_df = pd.DataFrame([{
        "회사":   l.company,
        "티커":   l.ticker,
        "지역":   l.region,
        "섹터":   l.sector,
        "단가":   l.unit_price,
    } for l in lines])
    st.dataframe(
        line_df, hide_index=True, use_container_width=True, height=240,
        column_config={"단가": st.column_config.NumberColumn(format="$%,.0f")},
    )

    # 회사 선택 — 단가 + 샘플 미리보기 (한 회사 선택해서 두 가지 보기)
    co_pick = st.selectbox(
        "🔎 회사 선택 — 단가 산정 내역 + 데이터 샘플 미리보기",
        [l.company for l in lines],
        index=0,
    )

    cc1, cc2 = st.columns([1, 1.4])

    with cc1:
        st.markdown("##### 🔬 단가 산정")
        chosen = next(l for l in lines if l.company == co_pick)
        bd_df = pd.DataFrame(chosen.breakdown, columns=["구성요소", "금액"])
        st.dataframe(
            bd_df, hide_index=True, use_container_width=True, height=260,
            column_config={"금액": st.column_config.NumberColumn(format="$%,.2f")},
        )
        st.caption(f"→ **{chosen.company}** 단가 합계: **{fmt_usd(chosen.unit_price)}**")

    with cc2:
        st.markdown("##### 📊 데이터 샘플 (최근 6개월)")
        chosen_row = catalog[catalog["company"] == co_pick].iloc[0]
        try:
            sample = monthly_aggregates(chosen_row, n_months=6)
            st.dataframe(
                sample, hide_index=True, use_container_width=True, height=260,
                column_config={
                    "revenue_usd_m": st.column_config.NumberColumn("매출 (M$)", format="$%.2fM"),
                    "transactions":  st.column_config.NumberColumn("거래", format="%,d"),
                    "unique_users":  st.column_config.NumberColumn("이용자", format="%,d"),
                    "mom_pct":       st.column_config.NumberColumn("MoM %", format="%+.1f %%"),
                    "yoy_pct":       st.column_config.NumberColumn("YoY %", format="%+.1f %%"),
                    "signal_score":  st.column_config.ProgressColumn(
                        "시그널", min_value=0.0, max_value=1.0, format="%.2f"),
                },
            )
            # 미니 라인 차트
            chart_df = sample.set_index("month")[["revenue_usd_m"]]
            st.caption("매출 추이 (USD M)")
            st.line_chart(chart_df, height=120)
            st.caption(
                "💡 결제 진행 시 **24개월 전체 시계열** 다운로드 (매출·거래·이용자·시그널 history)"
            )
        except Exception as e:
            st.warning(f"샘플 생성 실패: {e}")

    st.divider()

    # 결제 합산
    st.markdown("#### 🧮 결제 합산")
    summary_rows = [
        ("소계 (Subtotal)",                         totals.subtotal),
        (f"묶음 할인 ({totals.volume_tier_label}, "
         f"{totals.volume_rate*100:.0f}%)",          -totals.volume_discount),
        ("할인 후 (Net)",                            totals.after_discount),
        (f"VAT ({VAT_RATE*100:.0f}%)",               totals.tax),
        ("총 결제액 (Grand Total)",                  totals.grand_total),
    ]
    sum_df = pd.DataFrame(summary_rows, columns=["항목", "금액 (USD)"])
    st.dataframe(
        sum_df, hide_index=True, use_container_width=True,
        column_config={"금액 (USD)": st.column_config.NumberColumn(format="$%,.2f")},
    )

    with st.expander("📊 묶음 할인 테이블", expanded=False):
        vt = pd.DataFrame(
            [(qty, f"{rate*100:.0f}%", lbl) for qty, rate, lbl in VOLUME_TIERS],
            columns=["≥ 수량", "할인율", "Tier"],
        )
        st.dataframe(vt, hide_index=True, use_container_width=True)

    # 액션
    st.divider()
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        if st.button("취소", use_container_width=True, key="checkout_cancel"):
            st.session_state["show_checkout"] = False
            st.rerun()
    with c2:
        # 견적서 다운로드 (결제 전 단계)
        quote_bytes = build_export_xlsx(catalog, cart_now,
                                        summarize_selection(sel))
        st.download_button(
            "📋 견적서 (xlsx)",
            data=quote_bytes,
            file_name=export_filename(prefix="mandata_quote"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="dl_quote",
        )
    with c3:
        if st.button(f"✅ 결제 진행 · {fmt_usd(totals.grand_total)}",
                     type="primary", use_container_width=True,
                     key="confirm_payment"):
            # 결제 처리 (Mock) — 스냅샷 저장 후 결제 후 화면으로 전환
            from datetime import datetime as _dt
            st.session_state["payment_completed"] = True
            st.session_state["paid_companies"] = list(cart_now)
            st.session_state["paid_totals"] = totals
            st.session_state["paid_order_id"] = (
                f"MAN-{_dt.now().strftime('%Y%m%d-%H%M%S')}"
            )
            st.rerun()


if st.session_state.get("show_checkout"):
    # NOTE: show_checkout 리셋은 dialog 내부 버튼(취소·결제 진행)에서만 한다.
    # 여기서 즉시 False 로 두면 첫 rerun 후 dialog 가 닫혀버린다.
    _checkout_dialog()


# ── Footer ────────────────────────────────────────────────────────────────
st.divider()
with st.expander("ℹ️ 필터 사양 · 가격 정책", expanded=False):
    tab1, tab2 = st.tabs(["필터 컬럼", "가격 정책"])
    with tab1:
        st.caption("카탈로그에 컬럼이 있으면 자동으로 사이드바 필터 노출.")
        spec_rows: list[dict] = []
        for cat_key, meta in CATEGORY_REGISTRY.items():
            for col, (lbl, kind) in meta["cols"].items():
                spec_rows.append({
                    "카테고리": meta["label"], "컬럼": col, "라벨": lbl,
                    "타입": kind,
                    "카탈로그": "✓" if col in catalog.columns else "—",
                })
        st.dataframe(pd.DataFrame(spec_rows), hide_index=True, use_container_width=True)
    with tab2:
        st.markdown(
            "**단가 (USD)** = Base × Signal-mult × Size-mult + 보너스 항목\n\n"
            "- Base: $2,000\n"
            "- Signal multiplier: 0.5× ~ 2.5× (signal_score 0~1)\n"
            "- Size multiplier: Mega 1.50× / Large 1.30× / Mid 1.15× / Small 1.00× / Micro 0.85×\n"
            "- Data Completeness 보너스: 최대 +$400\n"
            "- Source Diversity 보너스: 추가 소스당 +$200\n"
            "- History Coverage 보너스: 1년당 +$150 (캡 3년)\n"
            "- Freshness: latency ≤ 7d → +$250, > 30d → −$200\n\n"
            "**묶음 할인** (qty 기준)"
        )
        vt = pd.DataFrame(
            [(qty, f"{rate*100:.0f}%", lbl) for qty, rate, lbl in VOLUME_TIERS],
            columns=["≥ 수량", "할인율", "Tier"],
        )
        st.dataframe(vt, hide_index=True, use_container_width=True)
        st.markdown(f"**세금**: VAT {VAT_RATE*100:.0f}%")

st.caption(
    "💡 Mandata Alt-Data Catalog · Institutional Grade. "
    "카탈로그는 분석 앱 Step 6 'Catalog Export' 로 생성. 문의: yonghan@mandata.kr"
)
