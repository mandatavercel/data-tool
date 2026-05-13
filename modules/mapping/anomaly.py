"""
Anomaly Detection — 소비 급등·급락 시그널 탐지 (Z-score / IQR)
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from modules.common.helpers import get_col, parse_dates

FREQ_MAP = {"일": "D", "주": "W", "월": "ME"}


def _agg(df, date_col, sales_col, name_col, freq):
    df = df.copy()
    df[date_col] = parse_dates(df[date_col])
    df[sales_col] = pd.to_numeric(df[sales_col], errors="coerce")
    df = df.dropna(subset=[date_col, sales_col]).set_index(date_col)

    if name_col:
        agg = (
            df.groupby([name_col, pd.Grouper(freq=freq)])[sales_col]
            .sum().reset_index()
        )
    else:
        agg = df.groupby(pd.Grouper(freq=freq))[sales_col].sum().reset_index()
        agg["__all__"] = "전체"
        name_col = "__all__"
    return agg, name_col


def _detect(series: pd.Series, method: str, threshold: float):
    """Returns boolean mask where anomaly=True and z_score series."""
    z = (series - series.mean()) / series.std(ddof=1)
    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    iqr = q3 - q1
    iqr_lo, iqr_hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr

    if method == "Z-score":
        mask = z.abs() > threshold
    elif method == "IQR":
        mask = (series < iqr_lo) | (series > iqr_hi)
    else:  # 둘 다
        mask = (z.abs() > threshold) | (series < iqr_lo) | (series > iqr_hi)

    return mask, z


def _label_anomaly(val, mean, std):
    if val > mean + 2 * std:
        return "급등 🚨"
    if val < mean - 2 * std:
        return "급락 ⚠️"
    return "이상"


def render(go_to):
    if "role_map" not in st.session_state:
        go_to(1); st.stop()

    df       = st.session_state["raw_df"].copy()
    role_map = st.session_state["role_map"]
    params   = st.session_state.get("analysis_params", {})

    date_col  = get_col(role_map, "transaction_date")
    sales_col = get_col(role_map, "sales_amount")
    name_col  = get_col(role_map, "company_name", "brand_name")

    agg_unit  = params.get("agg_unit", "일")
    method    = params.get("method", "Z-score")
    threshold = float(params.get("threshold", 2.5))

    st.subheader("③ Anomaly Detection — 소비 급등·급락 시그널")

    freq = FREQ_MAP[agg_unit]
    agg, name_col = _agg(df, date_col, sales_col, name_col, freq)
    agg = agg.sort_values([name_col, date_col])

    # 이상치 탐지 per company
    anomaly_frames = []
    for co, grp in agg.groupby(name_col):
        grp = grp.copy().reset_index(drop=True)
        if len(grp) < 4:
            grp["z_score"] = np.nan
            grp["is_anomaly"] = False
            grp["anomaly_type"] = ""
        else:
            mask, z = _detect(grp[sales_col], method, threshold)
            grp["z_score"] = z.round(2)
            grp["is_anomaly"] = mask
            mean_, std_ = grp[sales_col].mean(), grp[sales_col].std(ddof=1)
            grp["anomaly_type"] = grp.apply(
                lambda r: _label_anomaly(r[sales_col], mean_, std_) if r["is_anomaly"] else "",
                axis=1,
            )
        anomaly_frames.append(grp)

    agg = pd.concat(anomaly_frames, ignore_index=True)
    st.session_state["result_anomaly_df"] = agg

    # ── 회사 선택 ──────────────────────────────────────────────────────────────
    companies = sorted(agg[name_col].unique())
    sel_co = (
        st.selectbox("회사 선택", companies, key="an_co")
        if len(companies) > 1
        else companies[0]
    )
    sub = agg[agg[name_col] == sel_co].sort_values(date_col)

    # ── 요약 메트릭 ────────────────────────────────────────────────────────────
    total = len(sub)
    n_anom = sub["is_anomaly"].sum()
    n_surge = (sub["anomaly_type"] == "급등 🚨").sum()
    n_drop  = (sub["anomaly_type"] == "급락 ⚠️").sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("전체 기간", f"{total}개")
    c2.metric("이상치 수", f"{n_anom}개", delta=f"비율 {n_anom/total*100:.1f}%")
    c3.metric("급등 🚨", f"{n_surge}개")
    c4.metric("급락 ⚠️", f"{n_drop}개")

    # ── 메인 차트: 매출 + 이상치 마킹 ─────────────────────────────────────────
    st.markdown(f"### 📉 {sel_co} — {agg_unit}별 매출 & 이상치")

    normal = sub[~sub["is_anomaly"]]
    anoms  = sub[sub["is_anomaly"]]

    fig = go.Figure()
    # 정상 구간 라인
    fig.add_trace(go.Scatter(
        x=sub[date_col], y=sub[sales_col],
        mode="lines", name="매출액", line=dict(color="#6b7280", width=1.5),
    ))
    # 정상 포인트
    fig.add_trace(go.Scatter(
        x=normal[date_col], y=normal[sales_col],
        mode="markers", name="정상", marker=dict(color="#1e40af", size=5),
    ))
    # 급등
    surge = anoms[anoms["anomaly_type"] == "급등 🚨"]
    if not surge.empty:
        fig.add_trace(go.Scatter(
            x=surge[date_col], y=surge[sales_col],
            mode="markers+text", name="급등 🚨",
            marker=dict(color="#ef4444", size=12, symbol="triangle-up"),
            text=surge["anomaly_type"], textposition="top center",
        ))
    # 급락
    drop = anoms[anoms["anomaly_type"] == "급락 ⚠️"]
    if not drop.empty:
        fig.add_trace(go.Scatter(
            x=drop[date_col], y=drop[sales_col],
            mode="markers+text", name="급락 ⚠️",
            marker=dict(color="#f59e0b", size=12, symbol="triangle-down"),
            text=drop["anomaly_type"], textposition="bottom center",
        ))
    # 나머지 이상 유형
    other_anom = anoms[~anoms["anomaly_type"].isin(["급등 🚨", "급락 ⚠️"])]
    if not other_anom.empty:
        fig.add_trace(go.Scatter(
            x=other_anom[date_col], y=other_anom[sales_col],
            mode="markers", name="이상치",
            marker=dict(color="#8b5cf6", size=10, symbol="diamond"),
        ))

    # 평균 ± 2σ 밴드
    mean_v = sub[sales_col].mean()
    std_v  = sub[sales_col].std(ddof=1)
    fig.add_hrect(
        y0=mean_v - 2 * std_v, y1=mean_v + 2 * std_v,
        fillcolor="rgba(16,185,129,0.08)", line_width=0,
        annotation_text="±2σ 정상 범위", annotation_position="top left",
    )
    fig.add_hline(y=mean_v, line_dash="dot", line_color="#10b981", annotation_text="평균")

    fig.update_layout(
        title=f"{sel_co} 이상치 탐지 ({method}, threshold={threshold})",
        xaxis_title="기간",
        yaxis_title="매출액",
        hovermode="x unified",
    )
    st.plotly_chart(fig, key="anomaly_1")

    # ── Z-score 차트 ──────────────────────────────────────────────────────────
    if "z_score" in sub.columns and sub["z_score"].notna().any():
        st.markdown(f"### 📊 {sel_co} — Z-score 추이")
        fig_z = go.Figure()
        colors_z = ["#ef4444" if v > threshold or v < -threshold else "#6b7280"
                    for v in sub["z_score"].fillna(0)]
        fig_z.add_trace(go.Bar(
            x=sub[date_col], y=sub["z_score"],
            marker_color=colors_z, name="Z-score",
        ))
        fig_z.add_hline(y=threshold,  line_dash="dash", line_color="#ef4444",
                        annotation_text=f"+{threshold}σ")
        fig_z.add_hline(y=-threshold, line_dash="dash", line_color="#f59e0b",
                        annotation_text=f"-{threshold}σ")
        fig_z.add_hline(y=0, line_color="gray", line_width=0.5)
        fig_z.update_layout(
            title=f"{sel_co} Z-score",
            yaxis_title="Z-score",
            xaxis_title="기간",
        )
        st.plotly_chart(fig_z, key="anomaly_2")

    # ── 이상치 목록 ────────────────────────────────────────────────────────────
    st.markdown(f"### 📋 {sel_co} — 이상치 목록")
    anom_list = sub[sub["is_anomaly"]].copy()
    if anom_list.empty:
        st.success("이상치가 감지되지 않았습니다.")
    else:
        show_cols = [date_col, sales_col, "z_score", "anomaly_type"]
        show_cols = [c for c in show_cols if c in anom_list.columns]
        st.dataframe(
            anom_list[show_cols].sort_values(date_col, ascending=False), hide_index=True,
        )

    # ── 전체 회사 이상치 빈도 비교 ────────────────────────────────────────────
    if len(companies) > 1:
        st.markdown("### 🏆 전체 회사 이상치 빈도 비교")
        freq_df = (
            agg[agg["is_anomaly"]]
            .groupby([name_col, "anomaly_type"])
            .size()
            .reset_index(name="count")
        )
        if not freq_df.empty:
            fig_freq = px.bar(
                freq_df, x=name_col, y="count", color="anomaly_type",
                barmode="group",
                color_discrete_map={"급등 🚨": "#ef4444", "급락 ⚠️": "#f59e0b", "이상": "#8b5cf6"},
                title="회사별 이상치 유형 빈도",
                labels={"count": "이상치 수", name_col: "회사"},
            )
            st.plotly_chart(fig_freq, key="anomaly_3")

    c_prev, _, c_next = st.columns([1, 3, 1])
    with c_prev:
        if st.button("← 분석 선택", key="an_prev"):
            go_to(2)
    with c_next:
        if st.button("Signal Dashboard →", type="primary", key="an_next"):
            go_to(4)
