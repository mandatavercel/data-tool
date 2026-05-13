"""
Growth Analytics — YoY / QoQ / MoM 성장률 & 모멘텀 분석
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from modules.common.helpers import get_col, parse_dates

FREQ_MAP = {"일": "D", "주": "W", "월": "ME", "분기": "QE"}
SHIFT_MAP = {  # MoM / QoQ / YoY shift 주기 (해당 freq 기준)
    "일":   {"MoM": 30,  "QoQ": 91,  "YoY": 365},
    "주":   {"MoM": 4,   "QoQ": 13,  "YoY": 52},
    "월":   {"MoM": 1,   "QoQ": 3,   "YoY": 12},
    "분기": {"MoM": None, "QoQ": 1,  "YoY": 4},
}


def _agg(df, date_col, sales_col, name_col, freq):
    """회사(있으면) + 기간별 매출 집계"""
    df = df.copy()
    df[date_col] = parse_dates(df[date_col])
    df[sales_col] = pd.to_numeric(df[sales_col], errors="coerce")
    df = df.dropna(subset=[date_col, sales_col])
    df = df.set_index(date_col)

    if name_col:
        agg = (
            df.groupby([name_col, pd.Grouper(freq=freq)])[sales_col]
            .sum()
            .reset_index()
        )
    else:
        agg = (
            df.groupby(pd.Grouper(freq=freq))[sales_col]
            .sum()
            .reset_index()
        )
        agg["__all__"] = "전체"
        name_col = "__all__"
    return agg, name_col


def render(go_to):
    if "role_map" not in st.session_state:
        go_to(1); st.stop()

    df       = st.session_state["raw_df"].copy()
    role_map = st.session_state["role_map"]
    params   = st.session_state.get("analysis_params", {})

    date_col  = get_col(role_map, "transaction_date")
    sales_col = get_col(role_map, "sales_amount")
    name_col  = get_col(role_map, "company_name", "brand_name")

    agg_unit = params.get("agg_unit", "월")
    metrics  = params.get("metrics", ["MoM", "YoY"])

    st.subheader("③ Growth Analytics — 성장률 & 모멘텀")

    freq = FREQ_MAP[agg_unit]
    shifts = SHIFT_MAP[agg_unit]

    agg, name_col = _agg(df, date_col, sales_col, name_col, freq)
    agg = agg.sort_values([name_col, date_col])

    # 성장률 계산
    for m in metrics:
        n = shifts.get(m)
        if n is None:
            continue
        agg[m] = agg.groupby(name_col)[sales_col].transform(
            lambda s: s.pct_change(n) * 100
        )

    # 모멘텀 스코어: MoM 또는 QoQ의 3기간 이동평균 변화
    base_metric = "MoM" if "MoM" in metrics else (metrics[0] if metrics else None)
    if base_metric and base_metric in agg.columns:
        agg["Momentum"] = agg.groupby(name_col)[base_metric].transform(
            lambda s: s.rolling(3).mean().diff()
        )

    st.session_state["result_growth_df"] = agg

    # ── 회사 선택 ──────────────────────────────────────────────────────────────
    companies = sorted(agg[name_col].unique())
    sel_co = (
        st.selectbox("회사 선택", companies, key="gr_co")
        if len(companies) > 1
        else companies[0]
    )
    sub = agg[agg[name_col] == sel_co].sort_values(date_col)

    # ── 매출 추이 ──────────────────────────────────────────────────────────────
    st.markdown(f"### 📈 {sel_co} — {agg_unit}별 매출 추이")
    fig_sales = px.bar(
        sub, x=date_col, y=sales_col,
        title=f"{sel_co} {agg_unit}별 매출",
        labels={sales_col: "매출액", date_col: "기간"},
    )
    fig_sales.add_scatter(x=sub[date_col], y=sub[sales_col],
                          mode="lines+markers", name="추세", line=dict(color="#1e40af"))
    st.plotly_chart(fig_sales, key="growth_1")

    # ── 성장률 차트 ────────────────────────────────────────────────────────────
    avail_metrics = [m for m in metrics if m in sub.columns]
    if avail_metrics:
        st.markdown(f"### 📊 성장률 ({' / '.join(avail_metrics)})")
        fig_gr = go.Figure()
        colors = ["#1e40af", "#10b981", "#f59e0b"]
        for i, m in enumerate(avail_metrics):
            fig_gr.add_trace(go.Scatter(
                x=sub[date_col], y=sub[m],
                name=m, mode="lines+markers",
                line=dict(color=colors[i % len(colors)]),
            ))
        fig_gr.add_hline(y=0, line_dash="dash", line_color="gray")
        fig_gr.update_layout(
            title=f"{sel_co} 성장률",
            yaxis_title="성장률 (%)",
            xaxis_title="기간",
            hovermode="x unified",
        )
        st.plotly_chart(fig_gr, key="growth_2")

    # ── 모멘텀 차트 ────────────────────────────────────────────────────────────
    if "Momentum" in sub.columns:
        st.markdown("### 🚀 성장 모멘텀 (가속/감속)")
        sub_m = sub.dropna(subset=["Momentum"])
        fig_mom = go.Figure()
        colors_bar = ["#10b981" if v >= 0 else "#ef4444" for v in sub_m["Momentum"]]
        fig_mom.add_trace(go.Bar(
            x=sub_m[date_col], y=sub_m["Momentum"],
            marker_color=colors_bar, name="Momentum",
        ))
        fig_mom.add_hline(y=0, line_dash="dash", line_color="gray")
        fig_mom.update_layout(
            title=f"{sel_co} 성장 모멘텀 (양수=가속, 음수=감속)",
            yaxis_title="모멘텀",
        )
        st.plotly_chart(fig_mom, key="growth_3")

    # ── 전체 회사 성장률 비교 (최신 기간 기준) ─────────────────────────────────
    if len(companies) > 1 and avail_metrics:
        st.markdown("### 🏆 전체 회사 성장률 비교 (최신 기간)")
        latest = agg.sort_values(date_col).groupby(name_col).last().reset_index()
        fig_cmp = px.bar(
            latest.sort_values(avail_metrics[0], ascending=False),
            x=name_col, y=avail_metrics[0],
            color=avail_metrics[0],
            color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
            title=f"최신 {avail_metrics[0]} 비교",
            labels={avail_metrics[0]: f"{avail_metrics[0]} (%)"},
        )
        fig_cmp.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig_cmp, key="growth_4")

    # ── 요약 테이블 ────────────────────────────────────────────────────────────
    st.markdown("### 📋 상세 데이터")
    display_cols = [date_col, name_col, sales_col] + avail_metrics + (["Momentum"] if "Momentum" in agg.columns else [])
    st.dataframe(
        agg[display_cols].sort_values([name_col, date_col]),
        hide_index=True,
    )

    c_prev, _, c_next = st.columns([1, 3, 1])
    with c_prev:
        if st.button("← 분석 선택", key="gr_prev"):
            go_to(2)
    with c_next:
        if st.button("Signal Dashboard →", type="primary", key="gr_next"):
            go_to(4)
