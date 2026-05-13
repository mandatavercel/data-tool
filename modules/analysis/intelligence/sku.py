"""SKU Intelligence — Pareto analysis, top/bottom SKUs"""
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from modules.common.foundation import _parse_dates
from modules.common.core.audit import compute_module_audit, check_sample_size_sanity
from modules.common.core.result import enrich_result

from modules.analysis.guides import render_guide


def run_sku_analysis(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    sales_col   = role_map.get("sales_amount")
    sku_col     = role_map.get("sku_name")
    date_col    = role_map.get("transaction_date")
    company_col = role_map.get("company_name")

    if not sales_col:
        return {"status": "failed", "message": "sales_amount 역할 없음", "data": None, "metrics": {}}

    n_original = len(df)
    df = df.copy()
    df["_sales"] = pd.to_numeric(df[sales_col], errors="coerce")
    n_valid = int(df["_sales"].notna().sum())

    warnings: list[str] = []
    if not sku_col:
        warnings.append("sku_name 없음 — 행 인덱스를 SKU로 대체 (의미 제한)")
        df["_sku"] = "SKU_" + df.reset_index().index.astype(str)
        sku_col_use = "_sku"
    else:
        sku_col_use = sku_col

    _date_min = _date_max = None
    if date_col:
        df["_date"] = _parse_dates(df[date_col])
        valid_dates = df["_date"].dropna()
        if not valid_dates.empty:
            _date_min = str(valid_dates.min().date())
            _date_max = str(valid_dates.max().date())

    # SKU-level aggregation
    agg = (
        df.groupby(sku_col_use, as_index=False)["_sales"]
        .sum()
        .rename(columns={sku_col_use: "sku", "_sales": "sales"})
    )

    total = float(agg["sales"].sum())
    if total == 0:
        return {"status": "failed", "message": "매출 합계가 0입니다", "data": None, "metrics": {}}

    agg["share_pct"] = (agg["sales"] / total * 100).round(3)
    agg = agg.sort_values("sales", ascending=False).reset_index(drop=True)
    agg["rank"]           = agg.index + 1
    agg["cumulative_pct"] = agg["share_pct"].cumsum().round(2)

    # Pareto: how many SKUs cover 80%?
    pareto_80 = int((agg["cumulative_pct"] <= 80).sum()) + 1
    pareto_80 = min(pareto_80, len(agg))
    pareto_pct = round(pareto_80 / len(agg) * 100, 1)

    # Top 5 / Bottom 5
    top5    = agg.head(5)["sku"].tolist()
    bottom5 = agg.tail(5)["sku"].tolist()

    # Monthly trend for top SKUs (if date available)
    trend_df = pd.DataFrame()
    if date_col and "_date" in df.columns:
        df["_ym"] = df["_date"].dt.to_period("M")
        top3_skus = agg.head(3)["sku"].tolist()
        mask = df[sku_col_use].isin(top3_skus)
        trend = (
            df[mask]
            .groupby([sku_col_use, "_ym"], as_index=False)["_sales"]
            .sum()
            .rename(columns={sku_col_use: "sku", "_ym": "period", "_sales": "sales"})
        )
        trend["period_str"] = trend["period"].astype(str)
        trend_df = trend.copy()

    metrics = {
        "n_skus":       int(len(agg)),
        "pareto_80_n":  pareto_80,
        "pareto_80_pct": pareto_pct,
        "top_sku":      str(agg.iloc[0]["sku"]) if len(agg) > 0 else "",
        "top_share":    round(float(agg.iloc[0]["share_pct"]), 2) if len(agg) > 0 else 0,
        "total_sales":  total,
    }

    status  = "warning" if warnings else "success"
    message = " | ".join(warnings) if warnings else f"{len(agg)}개 SKU 분석 완료 (Pareto 80%: {pareto_80}개)"

    result = {
        "status":    status,
        "message":   message,
        "data":      agg,
        "metrics":   metrics,
        "_trend_df": trend_df,
        "_top5":     top5,
        "_bottom5":  bottom5,
    }

    bs = check_sample_size_sanity(len(agg), min_required=5)
    audit, conf = compute_module_audit(
        n_original=n_original,
        n_valid=n_valid,
        role_map=role_map,
        used_roles=["sales_amount", "sku_name", "transaction_date"],
        date_min=_date_min,
        date_max=_date_max,
        formula="SKU별 매출 합계 → Pareto / 점유율(%)",
        agg_unit="SKU",
        n_computable=len(agg),
        n_periods=len(agg),
        business_checks=bs,
    )
    return enrich_result(result, audit, conf)


def _render(result: dict):
    render_guide("sku")
    if result["status"] == "failed":
        st.error(result["message"])
        return
    if result["status"] == "warning":
        st.warning(result["message"])

    m        = result["metrics"]
    agg      = result.get("data")
    trend_df = result.get("_trend_df", pd.DataFrame())
    top5     = result.get("_top5", [])
    bottom5  = result.get("_bottom5", [])

    # ── 헤더 메트릭 — 절대 규모 우선 ─────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 매출",    f"{m.get('total_sales', 0):,.0f}")
    c2.metric("SKU 수",     f"{m.get('n_skus', 0):,}")
    c3.metric("Top SKU",    m.get("top_sku", "")[:20])
    c4.metric("Pareto 80%", f"{m.get('pareto_80_n', 0)}개",
              delta=f"전체의 {m.get('pareto_80_pct', 0):.1f}%",
              help="매출 80%를 커버하는 SKU 수")

    tab1, tab2, tab3, tab4 = st.tabs(
        ["📊 Sales Overview", "🏆 Top / Bottom SKU", "📅 월별 추이", "📋 데이터"]
    )

    # ── TAB 1: Sales Overview — 절대 매출 + Pareto ────────────────────────────
    with tab1:
        if agg is not None and len(agg) > 0:
            top_n   = min(20, len(agg))
            df_plot = agg.head(top_n).copy()

            # 절대 매출 수평 막대
            df_sorted = df_plot.sort_values("sales", ascending=True)
            fig_abs = go.Figure(go.Bar(
                x=df_sorted["sales"],
                y=df_sorted["sku"],
                orientation="h",
                marker_color="#3b82f6",
                text=[f"{v:,.0f}" for v in df_sorted["sales"]],
                textposition="outside",
            ))
            fig_abs.update_layout(
                title=f"SKU별 총 매출 (Top {top_n})",
                xaxis_title="매출액",
                height=max(320, len(df_sorted) * 26 + 80),
                margin=dict(t=40, b=0, r=100),
                showlegend=False,
            )
            st.plotly_chart(fig_abs, key="sku_1")

            # Pareto 차트 (절대값 막대 + 누적 점유율 선)
            fig = go.Figure()
            fig.add_bar(
                x=df_plot["sku"], y=df_plot["sales"],
                name="매출", marker_color="#93c5fd", opacity=0.8,
            )
            fig.add_scatter(
                x=df_plot["sku"], y=df_plot["cumulative_pct"],
                mode="lines+markers", name="누적 점유율(%)",
                yaxis="y2", line=dict(color="#ef4444", width=2),
            )
            fig.update_layout(
                title=f"Pareto 분석 — Top {top_n} SKU",
                height=360,
                yaxis=dict(title="매출액"),
                yaxis2=dict(overlaying="y", side="right",
                            title="누적 점유율(%)", range=[0, 110]),
                legend=dict(orientation="h"),
                margin=dict(t=40, b=0),
            )
            st.plotly_chart(fig, key="sku_2")

    # ── TAB 2: Top / Bottom SKU ───────────────────────────────────────────────
    with tab2:
        if agg is not None:
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Top 5 SKU**")
                for i, s in enumerate(top5, 1):
                    rows = agg[agg["sku"] == s]
                    if rows.empty:
                        continue
                    row = rows.iloc[0]
                    st.markdown(
                        f"<div style='padding:6px 10px;background:#dbeafe;border-radius:6px;"
                        f"margin:3px 0;font-size:13px'>"
                        f"<b>{i}.</b> {s}<br>"
                        f"<span style='color:#1e40af'>{row['sales']:,.0f}</span>"
                        f" &nbsp; ({row['share_pct']:.2f}%)</div>",
                        unsafe_allow_html=True,
                    )
            with c2:
                st.markdown("**Bottom 5 SKU**")
                for i, s in enumerate(reversed(bottom5), 1):
                    rows = agg[agg["sku"] == s]
                    if rows.empty:
                        continue
                    row = rows.iloc[0]
                    st.markdown(
                        f"<div style='padding:6px 10px;background:#fef2f2;border-radius:6px;"
                        f"margin:3px 0;font-size:13px'>"
                        f"<b>{i}.</b> {s}<br>"
                        f"<span style='color:#dc2626'>{row['sales']:,.0f}</span>"
                        f" &nbsp; ({row['share_pct']:.3f}%)</div>",
                        unsafe_allow_html=True,
                    )

    # ── TAB 3: 월별 추이 ──────────────────────────────────────────────────────
    with tab3:
        if not trend_df.empty:
            fig = px.line(
                trend_df, x="period_str", y="sales", color="sku",
                title="Top 3 SKU 월별 매출 추이", markers=True,
            )
            fig.update_layout(height=380)
            st.plotly_chart(fig, key="sku_3")
        else:
            st.info("날짜 컬럼이 없어 월별 추이를 표시할 수 없습니다.")

    # ── TAB 4: 데이터 ─────────────────────────────────────────────────────────
    with tab4:
        if agg is not None:
            st.dataframe(agg, hide_index=True)
