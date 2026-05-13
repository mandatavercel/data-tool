"""
Demand Intelligence — 거래건수 × 객단가 분해 + 매출 원인 분석 + Demand Signal Score
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from modules.common.foundation import _parse_dates
from modules.common.core.audit import compute_module_audit, check_growth_sanity, check_sample_size_sanity
from modules.common.core.result import enrich_result

from modules.analysis.guides import render_guide

# ── 색상 ─────────────────────────────────────────────────────────────────────
C_BLUE   = "#1e40af"
C_GREEN  = "#16a34a"
C_RED    = "#dc2626"
C_AMBER  = "#d97706"
C_GRAY   = "#6b7280"
C_PURPLE = "#7c3aed"
C_TEAL   = "#0d9488"

DRIVER_COLOR = {
    "📦 Volume-driven":         C_BLUE,
    "💰 Price-driven":          C_PURPLE,
    "🚀 Mixed (Volume+Price)":  C_TEAL,
    "🔴 동반 하락":             C_RED,
    "📉 Volume 감소 주도":       C_AMBER,
    "📉 Price 감소 주도":        "#db2777",
    "➡ 기타":                  C_GRAY,
    "—":                        "#e5e7eb",
}

SCORE_BANDS = [
    (70,  101, "🚀 Strong Growth", C_TEAL),
    (55,  70,  "🟢 Bullish",       C_GREEN),
    (45,  55,  "⚪ Neutral",        C_GRAY),
    (30,  45,  "🟠 Caution",       C_AMBER),
    (0,   30,  "🔴 Bearish",       C_RED),
]


def _score_label(score: float) -> tuple[str, str]:
    for lo, hi, label, color in SCORE_BANDS:
        if lo <= score < hi:
            return label, color
    return "🚀 Strong Growth", C_TEAL


def _norm(val, lo: float = -50.0, hi: float = 50.0) -> float:
    if pd.isna(val):
        return 50.0
    return max(0.0, min(100.0, (float(val) - lo) / (hi - lo) * 100.0))


# ══════════════════════════════════════════════════════════════════════════════
# 계산
# ══════════════════════════════════════════════════════════════════════════════

def run_demand_analysis(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    """
    매출을 거래건수 × 객단가로 분해하고 원인을 분류.

    Returns
    -------
    {
      "agg_df"   : pd.DataFrame  # 기간별 집계 + 성장률 + 분해 + score
      "name_col" : str
      "date_col" : str
      "sales_col": str
    }
    """
    date_col  = role_map.get("transaction_date")
    sales_col = role_map.get("sales_amount")
    name_col  = role_map.get("company_name") or role_map.get("brand_name")
    tx_col    = role_map.get("number_of_tx")

    agg_unit = params.get("agg_unit", "월")
    FREQ_MAP = {"일": "D", "주": "W", "월": "ME"}
    freq = FREQ_MAP.get(agg_unit, "ME")

    # ── 전처리 ────────────────────────────────────────────────────────────────
    n_original = len(df)
    df = df.copy()
    df[date_col]  = _parse_dates(df[date_col])
    df[sales_col] = pd.to_numeric(df[sales_col], errors="coerce")
    df = df.dropna(subset=[date_col, sales_col])
    n_valid   = len(df)
    _date_min = str(df[date_col].min().date()) if n_valid > 0 else None
    _date_max = str(df[date_col].max().date()) if n_valid > 0 else None
    df["__row__"] = 1
    df = df.set_index(date_col)

    agg_dict: dict = {sales_col: "sum", "__row__": "count"}
    if tx_col:
        df[tx_col] = pd.to_numeric(df[tx_col], errors="coerce")
        agg_dict[tx_col] = "sum"

    # ── 집계 ──────────────────────────────────────────────────────────────────
    if name_col:
        agg = df.groupby([name_col, pd.Grouper(freq=freq)]).agg(agg_dict).reset_index()
    else:
        agg = df.groupby(pd.Grouper(freq=freq)).agg(agg_dict).reset_index()
        agg["__all__"] = "전체"
        name_col = "__all__"

    agg["tx_count"]   = agg[tx_col] if (tx_col and tx_col in agg.columns) else agg["__row__"]
    agg["avg_ticket"] = np.where(agg["tx_count"] > 0,
                                  agg[sales_col] / agg["tx_count"], np.nan)
    agg = agg.sort_values([name_col, date_col]).reset_index(drop=True)

    # ── 성장률 (전기 대비 %) ──────────────────────────────────────────────────
    for src, dst in [(sales_col, "sales_chg"),
                     ("tx_count", "tx_chg"),
                     ("avg_ticket", "ticket_chg")]:
        agg[dst] = agg.groupby(name_col)[src].transform(
            lambda s: s.pct_change() * 100
        )

    # ── 가속도 (성장률 변화 pp) ───────────────────────────────────────────────
    for src, dst in [("sales_chg",  "sales_accel"),
                     ("tx_chg",     "tx_accel"),
                     ("ticket_chg", "ticket_accel")]:
        agg[dst] = agg.groupby(name_col)[src].transform(lambda s: s.diff())

    # ── 매출 원인 분해 (Laspeyres) ────────────────────────────────────────────
    # ΔRevenue ≈ vol_contrib + price_contrib + mix_contrib
    #   vol_contrib   = ATV_{t-1} × ΔTX
    #   price_contrib = TX_{t-1}  × ΔATV
    #   mix_contrib   = ΔTX       × ΔATV
    def _decompose(grp: pd.DataFrame) -> pd.DataFrame:
        grp = grp.copy().sort_values(date_col).reset_index(drop=True)
        tx  = grp["tx_count"]
        atv = grp["avg_ticket"]

        delta_tx  = tx  - tx.shift(1)
        delta_atv = atv - atv.shift(1)

        grp["vol_contrib"]   = atv.shift(1) * delta_tx
        grp["price_contrib"] = tx.shift(1)  * delta_atv
        grp["mix_contrib"]   = delta_tx * delta_atv
        grp["delta_revenue"] = grp[sales_col] - grp[sales_col].shift(1)
        return grp

    # groupby().apply() 대신 for 루프 사용 — pandas 2.x에서 apply 후 name_col 컬럼이
    # 사라지는 동작 변화를 피하기 위함
    frames = []
    for _, grp in agg.groupby(name_col, sort=False):
        frames.append(_decompose(grp))
    agg = pd.concat(frames, ignore_index=True)

    # ── Driver 분류 ───────────────────────────────────────────────────────────
    def _classify(row) -> str:
        vc = row.get("vol_contrib", np.nan)
        pc = row.get("price_contrib", np.nan)
        dr = row.get("delta_revenue", np.nan)

        if any(pd.isna(v) for v in [vc, pc, dr]):
            return "—"

        scale = max(abs(dr), 1.0)

        if dr > 0:
            vp = vc / scale
            pp = pc / scale
            if vp >= 0.6:
                return "📦 Volume-driven"
            if pp >= 0.6:
                return "💰 Price-driven"
            if vc > 0 and pc > 0:
                return "🚀 Mixed (Volume+Price)"
            return "➡ 기타"
        elif dr < 0:
            if vc < 0 and pc < 0:
                return "🔴 동반 하락"
            return "📉 Volume 감소 주도" if abs(vc) >= abs(pc) else "📉 Price 감소 주도"
        return "—"

    agg["demand_driver"] = agg.apply(_classify, axis=1)

    # ── Demand Signal Score (0-100) ───────────────────────────────────────────
    def _compute_score(grp: pd.DataFrame) -> pd.DataFrame:
        grp = grp.copy().sort_values(date_col)
        r   = grp.tail(3)
        score = (
            _norm(r["sales_chg"].mean())                  * 0.30 +
            _norm(r["tx_chg"].mean())                     * 0.25 +
            _norm(r["ticket_chg"].mean())                 * 0.20 +
            _norm(r["sales_accel"].mean(), lo=-20, hi=20) * 0.25
        )
        grp["demand_score"] = round(score, 1)
        return grp

    frames = []
    for _, grp in agg.groupby(name_col, sort=False):
        frames.append(_compute_score(grp))
    agg = pd.concat(frames, ignore_index=True)

    n_periods = int(agg.groupby(name_col).size().max()) if name_col in agg.columns else len(agg)

    bs  = check_sample_size_sanity(n_periods, min_required=12)
    bs += check_growth_sanity(agg["sales_chg"] if "sales_chg" in agg.columns else None)

    audit, conf = compute_module_audit(
        n_original=n_original,
        n_valid=n_valid,
        role_map=role_map,
        used_roles=["transaction_date", "sales_amount", "company_name", "number_of_tx"],
        date_min=_date_min,
        date_max=_date_max,
        formula="Laspeyres 분해: ΔRevenue = vol_contrib + price_contrib + mix_contrib",
        agg_unit=agg_unit,
        n_computable=n_periods,
        n_periods=n_periods,
        business_checks=bs,
    )

    result = {
        "status":    "success",
        "message":   f"{n_periods}개 기간 Demand 분해 완료",
        "data":      agg,
        "metrics":   {"n_periods": n_periods, "agg_unit": agg_unit},
        "agg_df":    agg,
        "name_col":  name_col,
        "date_col":  date_col,
        "sales_col": sales_col,
    }
    return enrich_result(result, audit, conf)


# ══════════════════════════════════════════════════════════════════════════════
# 렌더링 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _score_bar_html(score: float) -> str:
    label, color = _score_label(score)
    pct = int(score)
    return (
        f"<div style='margin:8px 0'>"
        f"<div style='display:flex;justify-content:space-between;"
        f"font-size:12px;color:{C_GRAY};margin-bottom:4px'>"
        f"<span>{label}</span><span style='font-weight:700;color:{color}'>{score:.1f} / 100</span>"
        f"</div>"
        f"<div style='background:#e5e7eb;border-radius:6px;height:10px'>"
        f"<div style='background:{color};width:{pct}%;height:100%;border-radius:6px'></div>"
        f"</div></div>"
    )


def _growth_triple(sub: pd.DataFrame, date_col: str, title: str) -> go.Figure:
    """sales_chg / tx_chg / ticket_chg 3선 차트."""
    fig = go.Figure()
    for col, name, color in [
        ("sales_chg",  "매출 성장률",   C_BLUE),
        ("tx_chg",     "거래건수 성장률", C_GREEN),
        ("ticket_chg", "객단가 성장률",  C_PURPLE),
    ]:
        if col in sub.columns:
            fig.add_scatter(x=sub[date_col], y=sub[col].round(1),
                            mode="lines+markers", name=name,
                            line=dict(color=color, width=2))
    fig.add_hline(y=0, line_dash="dash", line_color=C_GRAY, line_width=1)
    fig.update_layout(title=title, xaxis_title="기간", yaxis_title="%",
                      hovermode="x unified", legend=dict(orientation="h"),
                      margin=dict(t=40, b=0))
    return fig


def _decomp_chart(sub: pd.DataFrame, date_col: str, title: str) -> go.Figure:
    """Vol / Price / Mix 기여도 누적 막대 + delta_revenue 선."""
    fig = go.Figure()

    color_map = {"vol_contrib": C_BLUE, "price_contrib": C_PURPLE,
                 "mix_contrib": "#94a3b8"}
    label_map = {"vol_contrib": "Volume 기여", "price_contrib": "Price 기여",
                 "mix_contrib": "Mix 기여"}

    for col in ["vol_contrib", "price_contrib", "mix_contrib"]:
        if col not in sub.columns:
            continue
        fig.add_bar(
            x=sub[date_col], y=sub[col].round(0),
            name=label_map[col], marker_color=color_map[col],
            opacity=0.8,
        )

    if "delta_revenue" in sub.columns:
        fig.add_scatter(
            x=sub[date_col], y=sub["delta_revenue"].round(0),
            mode="lines+markers", name="실제 매출 변화",
            line=dict(color=C_AMBER, width=2, dash="dot"),
        )

    fig.add_hline(y=0, line_dash="dash", line_color=C_GRAY, line_width=1)
    fig.update_layout(
        title=title, barmode="relative",
        xaxis_title="기간", yaxis_title="기여액",
        hovermode="x unified", legend=dict(orientation="h"),
        margin=dict(t=40, b=0),
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# 메인 렌더러
# ══════════════════════════════════════════════════════════════════════════════

def _render(result: dict):
    render_guide("demand")
    # 실패/경고 결과 안전 처리
    if not isinstance(result, dict) or result.get("status") == "failed":
        st.error(result.get("message", "Demand 분석 실패") if isinstance(result, dict) else "결과 없음")
        return
    if "agg_df" not in result or result.get("agg_df") is None:
        st.info(result.get("message", "Demand 결과 데이터 부족"))
        return
    agg_df    = result["agg_df"]
    date_col  = result.get("date_col")
    sales_col = result.get("sales_col")
    name_col  = result.get("name_col")
    if not (date_col and sales_col and name_col):
        st.info("필수 컬럼 정보 누락 — date/sales/name col")
        return

    companies = sorted(agg_df[name_col].unique())

    # ── 회사 선택 ─────────────────────────────────────────────────────────────
    if len(companies) > 1:
        sel = st.selectbox("분석 대상 회사", companies, key="dm_co")
    else:
        sel = companies[0]

    sub = agg_df[agg_df[name_col] == sel].sort_values(date_col)

    # ── 헤더 메트릭 — 절대 규모 우선 ─────────────────────────────────────────
    st.markdown(f"### {sel} — Demand Intelligence")

    total_sales = sub[sales_col].sum() if sales_col in sub.columns else 0
    total_tx    = sub["tx_count"].sum() if "tx_count" in sub.columns else 0
    avg_ticket  = sub["avg_ticket"].mean() if "avg_ticket" in sub.columns else float("nan")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 매출",    f"{total_sales:,.0f}")
    c2.metric("총 거래건수", f"{total_tx:,.0f}")
    c3.metric("평균 객단가", f"{avg_ticket:,.0f}" if not pd.isna(avg_ticket) else "N/A")
    # 최신 Driver — 절대값과 함께
    if len(sub) >= 2:
        drv    = sub.iloc[-1].get("demand_driver", "—")
        dcolor = DRIVER_COLOR.get(drv, C_GRAY)
        c4.markdown(
            f"<div style='text-align:center;padding-top:4px'>"
            f"<div style='font-size:11px;color:{C_GRAY}'>최신 성장 동인</div>"
            f"<div style='font-size:13px;font-weight:700;color:{dcolor};margin-top:4px'>{drv}</div>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # ── 탭 ────────────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["📊 Sales Overview", "🔍 매출 분해", "📈 성장률 추이", "📊 Demand Signal", "📋 데이터"]
    )

    # ── TAB 1: Sales Overview — 절대값 중심 ──────────────────────────────────
    with tab1:
        # 절대 매출 추이 막대
        ma3 = sub[sales_col].rolling(3, min_periods=1).mean() if sales_col in sub.columns else None
        if sales_col in sub.columns:
            fig_sales = go.Figure()
            fig_sales.add_bar(x=sub[date_col], y=sub[sales_col].round(0),
                              name="매출", marker_color=C_BLUE, opacity=0.8)
            if ma3 is not None:
                fig_sales.add_scatter(x=sub[date_col], y=ma3.round(0),
                                      mode="lines", name="3기 이동평균",
                                      line=dict(color=C_AMBER, width=2))
            fig_sales.update_layout(
                title=f"{sel} — 기간별 매출 (절대값)",
                xaxis_title="기간", yaxis_title="매출액",
                hovermode="x unified", legend=dict(orientation="h"),
                margin=dict(t=40, b=0),
            )
            st.plotly_chart(fig_sales, key="demand_1")

        # 거래건수 × 객단가 분해 (절대값)
        if "tx_count" in sub.columns and "avg_ticket" in sub.columns:
            c1c, c2c = st.columns(2)
            with c1c:
                fig_tx = go.Figure()
                fig_tx.add_bar(x=sub[date_col], y=sub["tx_count"].round(0),
                               name="거래건수", marker_color=C_GREEN, opacity=0.8)
                fig_tx.update_layout(
                    title="거래건수 추이", xaxis_title="기간",
                    yaxis_title="건수", margin=dict(t=40, b=0),
                )
                st.plotly_chart(fig_tx, key="demand_2")
            with c2c:
                fig_atv = go.Figure()
                fig_atv.add_scatter(x=sub[date_col], y=sub["avg_ticket"].round(0),
                                    mode="lines+markers", name="객단가",
                                    line=dict(color=C_PURPLE, width=2))
                fig_atv.update_layout(
                    title="객단가 추이", xaxis_title="기간",
                    yaxis_title="객단가", margin=dict(t=40, b=0),
                )
                st.plotly_chart(fig_atv, key="demand_3")

        # 요약 테이블 (절대값 중심)
        with st.expander("기간별 절대값 요약"):
            abs_cols = [date_col, sales_col, "tx_count", "avg_ticket"]
            show = sub[[c for c in abs_cols if c in sub.columns]].copy()
            show = show.rename(columns={
                date_col: "기간", sales_col: "매출액",
                "tx_count": "거래건수", "avg_ticket": "객단가",
            })
            for c in ["매출액", "객단가"]:
                if c in show.columns:
                    show[c] = show[c].round(0)
            st.dataframe(show.sort_values("기간", ascending=False), hide_index=True)

    # ── TAB 2: 매출 원인 분해 ─────────────────────────────────────────────────
    with tab2:
        st.caption(
            "매출 변화 = Volume 기여(거래건수 변화 × 전기 객단가) "
            "+ Price 기여(객단가 변화 × 금기 거래건수) + Mix 기여(교차항)"
        )

        st.plotly_chart(
            _decomp_chart(sub, date_col, f"{sel} — 매출 변화 원인 분해"), key="demand_4"
        )

        # Driver 비율 파이 (최근 유효 기간)
        drv_valid = sub[sub["demand_driver"] != "—"]["demand_driver"]
        if not drv_valid.empty:
            drv_cnt = drv_valid.value_counts().reset_index()
            drv_cnt.columns = ["driver", "count"]
            drv_cnt["color"] = drv_cnt["driver"].map(
                lambda d: DRIVER_COLOR.get(d, C_GRAY)
            )

            fig_pie = go.Figure(go.Pie(
                labels=drv_cnt["driver"], values=drv_cnt["count"],
                marker_colors=drv_cnt["color"],
                hole=0.4, textinfo="label+percent",
            ))
            fig_pie.update_layout(
                title="분석 기간 내 Driver 분포",
                margin=dict(t=40, b=0),
                showlegend=False,
            )
            st.plotly_chart(fig_pie, key="demand_5")

        with st.expander("분해 데이터 테이블"):
            cols_d = [date_col, sales_col, "delta_revenue",
                      "vol_contrib", "price_contrib", "mix_contrib", "demand_driver"]
            show = sub[[c for c in cols_d if c in sub.columns]].copy()
            show = show.rename(columns={
                date_col: "기간", sales_col: "매출액",
                "delta_revenue": "매출 변화",
                "vol_contrib": "Volume 기여", "price_contrib": "Price 기여",
                "mix_contrib": "Mix 기여", "demand_driver": "Driver",
            })
            for c in ["매출액", "매출 변화", "Volume 기여", "Price 기여", "Mix 기여"]:
                if c in show.columns:
                    show[c] = show[c].round(0)
            st.dataframe(show.sort_values("기간", ascending=False), hide_index=True)

    # ── TAB 3: 성장률 추이 ────────────────────────────────────────────────────
    with tab3:
        st.plotly_chart(
            _growth_triple(sub, date_col, f"{sel} — 매출 · 거래건수 · 객단가 성장률"), key="demand_6"
        )
        if "sales_accel" in sub.columns and sub["sales_accel"].notna().sum() > 0:
            accel_colors = [C_GREEN if v >= 0 else C_RED
                            for v in sub["sales_accel"].fillna(0)]
            fig_accel = go.Figure()
            fig_accel.add_bar(x=sub[date_col], y=sub["sales_accel"].round(1),
                              marker_color=accel_colors, name="매출 성장률 가속도")
            fig_accel.add_hline(y=0, line_dash="dot", line_color=C_GRAY)
            fig_accel.update_layout(title="매출 성장 가속도 (전기 대비 성장률 변화 pp)",
                                    xaxis_title="기간", yaxis_title="pp",
                                    margin=dict(t=40, b=0))
            st.plotly_chart(fig_accel, key="demand_7")

        with st.expander("성장률 데이터 테이블"):
            show = sub[[date_col, sales_col, "tx_count", "avg_ticket",
                         "sales_chg", "tx_chg", "ticket_chg",
                         "sales_accel", "tx_accel", "ticket_accel"]].copy()
            show = show.rename(columns={
                date_col: "기간", sales_col: "매출액",
                "tx_count": "거래건수", "avg_ticket": "객단가",
                "sales_chg":  "매출 성장(%)",  "tx_chg":     "거래건수 성장(%)",
                "ticket_chg": "객단가 성장(%)",
                "sales_accel": "매출 가속(pp)", "tx_accel": "거래건수 가속(pp)",
                "ticket_accel": "객단가 가속(pp)",
            })
            for c in show.columns[1:]:
                show[c] = show[c].round(1)
            st.dataframe(show.sort_values("기간", ascending=False), hide_index=True)

    # ── TAB 4: Demand Signal ──────────────────────────────────────────────────
    with tab4:
        score = float(sub["demand_score"].iloc[-1]) if "demand_score" in sub.columns else 50.0
        label, scolor = _score_label(score)
        # Score 게이지 바
        st.markdown(_score_bar_html(score), unsafe_allow_html=True)
        st.caption("Score = 매출성장(30%) + 거래건수성장(25%) + 객단가성장(20%) + 가속도(25%) — 최근 3기간 평균 기준")

        st.divider()

        # Driver 타임라인 배지
        drv_sub = sub[[date_col, "demand_driver", "delta_revenue",
                        "vol_contrib", "price_contrib"]].iloc[::-1]

        for _, row in drv_sub.head(24).iterrows():
            drv   = row.get("demand_driver", "—")
            dclr  = DRIVER_COLOR.get(drv, C_GRAY)
            dt    = (pd.Timestamp(row[date_col]).strftime("%Y-%m")
                     if hasattr(row[date_col], "strftime") else str(row[date_col])[:7])
            dr_v  = row.get("delta_revenue", np.nan)
            vc    = row.get("vol_contrib", np.nan)
            pc    = row.get("price_contrib", np.nan)
            dr_s  = f"{dr_v:+,.0f}" if pd.notna(dr_v) else "—"
            vc_s  = f"{vc:+,.0f}"   if pd.notna(vc)   else "—"
            pc_s  = f"{pc:+,.0f}"   if pd.notna(pc)   else "—"

            st.markdown(
                f"<div style='display:flex;align-items:center;gap:12px;"
                f"padding:5px 0;border-bottom:1px solid #f3f4f6'>"
                f"<div style='width:60px;font-size:12px;color:{C_GRAY};flex-shrink:0'>{dt}</div>"
                f"<div style='background:{dclr}22;border:1px solid {dclr}88;"
                f"border-radius:20px;padding:3px 12px;font-size:12px;"
                f"white-space:nowrap;flex-shrink:0'>{drv}</div>"
                f"<div style='font-size:11px;color:{C_GRAY}'>"
                f"매출변화 {dr_s} &nbsp;|&nbsp; "
                f"Vol {vc_s} &nbsp; Price {pc_s}</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── TAB 5: 원시 데이터 ────────────────────────────────────────────────────
    with tab5:
        show_cols = [date_col, sales_col, "tx_count", "avg_ticket",
                     "sales_chg", "tx_chg", "ticket_chg",
                     "vol_contrib", "price_contrib", "demand_driver", "demand_score"]
        show = sub[[c for c in show_cols if c in sub.columns]].copy()
        show = show.rename(columns={
            date_col: "기간", sales_col: "매출액",
            "tx_count": "거래건수", "avg_ticket": "객단가",
            "sales_chg": "매출성장(%)", "tx_chg": "거래건수성장(%)",
            "ticket_chg": "객단가성장(%)",
            "vol_contrib": "Vol 기여", "price_contrib": "Price 기여",
            "demand_driver": "Driver", "demand_score": "Score",
        })
        for c in ["매출액", "객단가", "Vol 기여", "Price 기여"]:
            if c in show.columns:
                show[c] = show[c].round(0)
        for c in ["매출성장(%)", "거래건수성장(%)", "객단가성장(%)"]:
            if c in show.columns:
                show[c] = show[c].round(1)
        st.dataframe(show.sort_values("기간", ascending=False), hide_index=True)

    # ── 회사별 비교 ────────────────────────────────────────────────────────────
    if len(companies) <= 1:
        return

    st.divider()
    st.markdown("### 🏆 회사별 Demand 비교")

    latest = agg_df.sort_values(date_col).groupby(name_col).last().reset_index()

    # Score 랭킹
    score_df = latest[[name_col, "demand_score", "demand_driver"]].copy()
    score_df = score_df.rename(columns={name_col: "회사"})
    score_df = score_df.sort_values("demand_score", ascending=True)
    score_df["color"] = score_df["demand_score"].apply(
        lambda s: _score_label(s)[1]
    )

    fig_score = go.Figure(go.Bar(
        x=score_df["demand_score"],
        y=score_df["회사"],
        orientation="h",
        marker_color=score_df["color"],
        text=score_df["demand_score"].apply(lambda s: f"{s:.0f}"),
        textposition="outside",
    ))
    fig_score.update_layout(
        title="회사별 Demand Signal Score",
        xaxis=dict(title="Score (0-100)", range=[0, 105]),
        margin=dict(t=40, b=0),
    )
    st.plotly_chart(fig_score, key="demand_8")

    c1, c2 = st.columns(2)
    with c1:
        fig_tx = px.bar(
            latest.sort_values("tx_count", ascending=True),
            x="tx_count", y=name_col, orientation="h",
            title="최신 기간 거래건수", labels={"tx_count": "거래건수"},
        )
        fig_tx.update_layout(margin=dict(t=40, b=0))
        st.plotly_chart(fig_tx, key="demand_9")

    with c2:
        fig_tk = px.bar(
            latest.sort_values("avg_ticket", ascending=True),
            x="avg_ticket", y=name_col, orientation="h",
            title="최신 기간 객단가", labels={"avg_ticket": "객단가"},
        )
        fig_tk.update_layout(margin=dict(t=40, b=0))
        st.plotly_chart(fig_tk, key="demand_10")

    # 성장률 추이 비교 (멀티라인)
    fig_trend = go.Figure()
    for co in sorted(agg_df[name_col].unique()):
        co_df = agg_df[agg_df[name_col] == co].sort_values(date_col)
        fig_trend.add_scatter(
            x=co_df[date_col], y=co_df["sales_chg"].round(1),
            mode="lines+markers", name=co,
        )
    fig_trend.add_hline(y=0, line_dash="dash", line_color=C_GRAY)
    fig_trend.update_layout(
        title="회사별 매출 성장률 추이 (%)",
        xaxis_title="기간", yaxis_title="%",
        hovermode="x unified", legend=dict(orientation="h"),
        margin=dict(t=40, b=0),
    )
    st.plotly_chart(fig_trend, key="demand_11")

    # 요약 테이블
    with st.expander("회사별 요약 테이블"):
        tbl = latest[[name_col, sales_col, "tx_count", "avg_ticket",
                       "sales_chg", "tx_chg", "ticket_chg",
                       "demand_driver", "demand_score"]].copy()
        tbl = tbl.rename(columns={
            name_col: "회사", sales_col: "매출액",
            "tx_count": "거래건수", "avg_ticket": "객단가",
            "sales_chg":  "매출성장(%)", "tx_chg":    "거래건수성장(%)",
            "ticket_chg": "객단가성장(%)",
            "demand_driver": "Driver", "demand_score": "Score",
        })
        for c in ["매출액", "객단가"]:
            if c in tbl.columns:
                tbl[c] = tbl[c].round(0)
        for c in ["매출성장(%)", "거래건수성장(%)", "객단가성장(%)"]:
            if c in tbl.columns:
                tbl[c] = tbl[c].round(1)
        st.dataframe(tbl.sort_values("Score", ascending=False), hide_index=True)
