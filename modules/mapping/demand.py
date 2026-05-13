"""
Demand Intelligence — 거래건수 vs 객단가 분해, Demand Signal 탐지
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from modules.common.helpers import get_col, parse_dates

FREQ_MAP = {"일": "D", "주": "W", "월": "ME"}


def _agg(df, date_col, sales_col, name_col, tx_col, freq):
    df = df.copy()
    df[date_col] = parse_dates(df[date_col])
    df[sales_col] = pd.to_numeric(df[sales_col], errors="coerce")
    df = df.dropna(subset=[date_col, sales_col])
    df = df.set_index(date_col)

    if name_col:
        groups = [name_col, pd.Grouper(freq=freq)]
        agg_dict = {sales_col: "sum"}
        if tx_col:
            df[tx_col] = pd.to_numeric(df[tx_col], errors="coerce")
            agg_dict[tx_col] = "sum"
        else:
            agg_dict["__row__"] = "count"
            df["__row__"] = 1

        agg = df.groupby(groups).agg(agg_dict).reset_index()
    else:
        agg_dict = {sales_col: "sum"}
        if tx_col:
            df[tx_col] = pd.to_numeric(df[tx_col], errors="coerce")
            agg_dict[tx_col] = "sum"
        else:
            agg_dict["__row__"] = "count"
            df["__row__"] = 1

        agg = df.groupby(pd.Grouper(freq=freq)).agg(agg_dict).reset_index()
        agg["__all__"] = "전체"
        name_col = "__all__"

    # tx_count 컬럼 통일
    if tx_col and tx_col in agg.columns:
        agg["tx_count"] = agg[tx_col]
    elif "__row__" in agg.columns:
        agg["tx_count"] = agg["__row__"]
        agg = agg.drop(columns=["__row__"])
    else:
        agg["tx_count"] = np.nan

    # 객단가 (avg_ticket)
    agg["avg_ticket"] = np.where(
        agg["tx_count"] > 0,
        agg[sales_col] / agg["tx_count"],
        np.nan,
    )

    return agg, name_col


def _classify_signal(vol_chg, price_chg, threshold=5.0):
    """볼륨/가격 변화율(%) 기반 Demand Signal 분류"""
    if pd.isna(vol_chg) or pd.isna(price_chg):
        return "데이터 부족"
    v, p = vol_chg, price_chg
    if v >= threshold and p >= threshold:
        return "🚀 Volume+Price 동반 성장"
    if v >= threshold and p < -threshold:
        return "📦 Volume-driven (가격↓)"
    if v >= threshold:
        return "📦 Volume-driven"
    if p >= threshold and v < -threshold:
        return "💰 Price-driven (거래↓)"
    if p >= threshold:
        return "💰 Price-driven"
    if v < -threshold and p < -threshold:
        return "🔴 Volume+Price 동반 하락"
    if v < -threshold:
        return "📉 Volume 감소"
    if p < -threshold:
        return "📉 Price 감소"
    return "➡ 횡보"


def render(go_to):
    if "role_map" not in st.session_state:
        go_to(1); st.stop()

    df       = st.session_state["raw_df"].copy()
    role_map = st.session_state["role_map"]
    params   = st.session_state.get("analysis_params", {})

    date_col  = get_col(role_map, "transaction_date")
    sales_col = get_col(role_map, "sales_amount")
    name_col  = get_col(role_map, "company_name", "brand_name")
    tx_col    = get_col(role_map, "number_of_tx")

    agg_unit = params.get("agg_unit", "월")
    selected_metrics = params.get("metrics", ["매출액", "거래건수", "건당평균단가"])

    st.subheader("③ Demand Intelligence — 거래 분해 & Demand Signal")

    freq = FREQ_MAP[agg_unit]
    agg, name_col = _agg(df, date_col, sales_col, name_col, tx_col, freq)
    agg = agg.sort_values([name_col, date_col])

    # 성장률 계산 (MoM 기준)
    for col, label in [(sales_col, "매출액_chg"), ("tx_count", "거래건수_chg"), ("avg_ticket", "객단가_chg")]:
        if col in agg.columns:
            agg[label] = agg.groupby(name_col)[col].transform(lambda s: s.pct_change() * 100)

    # Demand Signal 분류
    agg["demand_signal"] = agg.apply(
        lambda r: _classify_signal(r.get("거래건수_chg", np.nan), r.get("객단가_chg", np.nan)),
        axis=1,
    )

    # 변동성 (CV = std / mean, 낮을수록 안정)
    cv_df = (
        agg.groupby(name_col)[sales_col]
        .agg(["std", "mean"])
        .assign(CV=lambda x: x["std"] / x["mean"] * 100)
        .reset_index()
        .rename(columns={"CV": "변동성(CV%)"})
    )

    st.session_state["result_demand_df"] = agg

    # ── 회사 선택 ──────────────────────────────────────────────────────────────
    companies = sorted(agg[name_col].unique())
    sel_co = (
        st.selectbox("회사 선택", companies, key="dm_co")
        if len(companies) > 1
        else companies[0]
    )
    sub = agg[agg[name_col] == sel_co].sort_values(date_col)

    # ── 메트릭 카드 ────────────────────────────────────────────────────────────
    if len(sub) >= 2:
        last = sub.iloc[-1]
        prev = sub.iloc[-2]
        c1, c2, c3 = st.columns(3)
        with c1:
            delta = last[sales_col] - prev[sales_col]
            st.metric("최신 매출액", f"{last[sales_col]:,.0f}",
                      delta=f"{delta:+,.0f}")
        with c2:
            if not pd.isna(last.get("tx_count")):
                delta_tx = last["tx_count"] - prev["tx_count"]
                st.metric("최신 거래건수", f"{last['tx_count']:,.0f}",
                          delta=f"{delta_tx:+,.0f}")
        with c3:
            if not pd.isna(last.get("avg_ticket")):
                delta_at = last["avg_ticket"] - prev["avg_ticket"]
                st.metric("최신 객단가", f"{last['avg_ticket']:,.0f}",
                          delta=f"{delta_at:+,.0f}")

    # ── 멀티 지표 추이 ─────────────────────────────────────────────────────────
    metric_col_map = {
        "매출액": sales_col,
        "거래건수": "tx_count",
        "건당평균단가": "avg_ticket",
    }
    display_metrics = [m for m in selected_metrics if metric_col_map.get(m) in sub.columns]

    if display_metrics:
        st.markdown(f"### 📊 {sel_co} — {agg_unit}별 지표 추이")
        fig = go.Figure()
        colors = ["#1e40af", "#10b981", "#f59e0b"]
        for i, m in enumerate(display_metrics):
            col = metric_col_map[m]
            if col not in sub.columns:
                continue
            fig.add_trace(go.Scatter(
                x=sub[date_col], y=sub[col],
                name=m, mode="lines+markers",
                line=dict(color=colors[i % len(colors)]),
                yaxis="y" if i == 0 else "y2",
            ))
        fig.update_layout(
            title=f"{sel_co} 수요 지표 추이",
            xaxis_title="기간",
            yaxis=dict(title=display_metrics[0] if display_metrics else ""),
            yaxis2=dict(title=" / ".join(display_metrics[1:]), overlaying="y", side="right")
                   if len(display_metrics) > 1 else None,
            hovermode="x unified",
        )
        st.plotly_chart(fig, key="demand_1")

    # ── 매출 분해: Volume vs Price ─────────────────────────────────────────────
    if "거래건수_chg" in sub.columns and "객단가_chg" in sub.columns:
        st.markdown(f"### 🔬 {sel_co} — 매출 성장 분해 (Volume vs Price)")
        decomp = sub.dropna(subset=["거래건수_chg", "객단가_chg"])
        if not decomp.empty:
            fig_decomp = go.Figure()
            fig_decomp.add_trace(go.Bar(
                x=decomp[date_col], y=decomp["거래건수_chg"],
                name="거래건수 변화(%)", marker_color="#1e40af",
            ))
            fig_decomp.add_trace(go.Bar(
                x=decomp[date_col], y=decomp["객단가_chg"],
                name="객단가 변화(%)", marker_color="#10b981",
            ))
            fig_decomp.add_trace(go.Scatter(
                x=decomp[date_col], y=decomp["매출액_chg"],
                name="매출액 변화(%)", mode="lines+markers",
                line=dict(color="#f59e0b", width=2),
            ))
            fig_decomp.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_decomp.update_layout(
                barmode="group",
                title=f"{sel_co} 매출 성장 분해 (전기 대비 %)",
                yaxis_title="변화율 (%)",
                hovermode="x unified",
            )
            st.plotly_chart(fig_decomp, key="demand_2")

    # ── Demand Signal Timeline ─────────────────────────────────────────────────
    if "demand_signal" in sub.columns:
        st.markdown(f"### 🧭 {sel_co} — Demand Signal Timeline")
        sig_recent = sub[["demand_signal", date_col, sales_col]].tail(12).copy()
        sig_recent[date_col] = sig_recent[date_col].astype(str)
        st.dataframe(
            sig_recent.rename(columns={date_col: "기간", sales_col: "매출액", "demand_signal": "Demand Signal"}),
            hide_index=True,
        )

    # ── 전체 회사 비교 ─────────────────────────────────────────────────────────
    if len(companies) > 1:
        st.markdown("### 🏆 전체 회사 비교 (최신 기간)")
        latest = agg.sort_values(date_col).groupby(name_col).last().reset_index()
        cv_map = cv_df.set_index(name_col)["변동성(CV%)"].to_dict()
        latest["변동성(CV%)"] = latest[name_col].map(cv_map)

        fig_cmp = px.scatter(
            latest, x="tx_count", y="avg_ticket",
            size=sales_col, color=name_col,
            hover_name=name_col,
            title="거래건수 vs 객단가 (버블=매출액)",
            labels={"tx_count": "거래건수", "avg_ticket": "객단가"},
        )
        st.plotly_chart(fig_cmp, key="demand_3")

        col_show = [name_col, sales_col, "tx_count", "avg_ticket", "demand_signal", "변동성(CV%)"]
        col_show = [c for c in col_show if c in latest.columns]
        st.dataframe(
            latest[col_show].sort_values(sales_col, ascending=False),
            hide_index=True,
        )

    # ── 상세 테이블 ────────────────────────────────────────────────────────────
    st.markdown("### 📋 상세 데이터")
    detail_cols = [date_col, name_col, sales_col, "tx_count", "avg_ticket",
                   "거래건수_chg", "객단가_chg", "매출액_chg", "demand_signal"]
    detail_cols = [c for c in detail_cols if c in agg.columns]
    st.dataframe(
        agg[detail_cols].sort_values([name_col, date_col]),
        hide_index=True,
    )

    c_prev, _, c_next = st.columns([1, 3, 1])
    with c_prev:
        if st.button("← 분석 선택", key="dm_prev"):
            go_to(2)
    with c_next:
        if st.button("Signal Dashboard →", type="primary", key="dm_next"):
            go_to(4)
