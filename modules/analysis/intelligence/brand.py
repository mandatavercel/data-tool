"""Brand Intelligence — brand share, HHI, MoM growth"""
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from modules.common.foundation import _parse_dates
from modules.common.core.audit import compute_module_audit, check_sample_size_sanity
from modules.common.core.result import enrich_result

from modules.analysis.guides import render_guide


def run_brand_analysis(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    sales_col   = role_map.get("sales_amount")
    brand_col   = role_map.get("brand_name")
    company_col = role_map.get("company_name")
    date_col    = role_map.get("transaction_date")

    if not sales_col:
        return {"status": "failed", "message": "sales_amount 역할 없음", "data": None, "metrics": {}}

    n_original = len(df)
    df = df.copy()
    df["_sales"] = pd.to_numeric(df[sales_col], errors="coerce")
    n_valid = int(df["_sales"].notna().sum())

    warnings: list[str] = []

    # Name column: brand_name → company_name → 전체
    if brand_col:
        name_col = brand_col
    elif company_col:
        name_col = company_col
        warnings.append("brand_name 없음 — company_name으로 대체")
    else:
        name_col = None
        warnings.append("brand_name / company_name 없음 — 전체 단일 집계")

    # Date handling
    _date_min = _date_max = None
    if date_col:
        df["_date"] = _parse_dates(df[date_col])
        valid_dates = df["_date"].dropna()
        if not valid_dates.empty:
            _date_min = str(valid_dates.min().date())
            _date_max = str(valid_dates.max().date())

    # Brand-level aggregation
    if name_col:
        df["_brand"] = df[name_col].fillna("(없음)")
        agg = (
            df.groupby("_brand", as_index=False)["_sales"]
            .sum()
            .rename(columns={"_brand": "brand", "_sales": "sales"})
        )
    else:
        agg = pd.DataFrame([{"brand": "전체", "sales": float(df["_sales"].sum())}])

    total = float(agg["sales"].sum())
    if total == 0:
        return {"status": "failed", "message": "매출 합계가 0입니다", "data": None, "metrics": {}}

    agg["share_pct"] = (agg["sales"] / total * 100).round(2)
    agg["rank"]      = agg["sales"].rank(ascending=False, method="min").astype(int)
    agg = agg.sort_values("rank").reset_index(drop=True)

    # HHI
    hhi = float((agg["share_pct"] ** 2).sum())

    # Monthly trend per brand
    mom_df = pd.DataFrame()
    if date_col and name_col and "_date" in df.columns:
        df["_ym"] = df["_date"].dt.to_period("M")
        monthly   = (
            df.groupby([name_col, "_ym"], as_index=False)["_sales"]
            .sum()
            .rename(columns={name_col: "brand", "_ym": "period", "_sales": "sales"})
        )
        monthly = monthly.sort_values(["brand", "period"])
        monthly["mom_pct"]    = monthly.groupby("brand")["sales"].pct_change() * 100
        monthly["period_str"] = monthly["period"].astype(str)
        mom_df = monthly.dropna(subset=["mom_pct"]).copy()

    metrics = {
        "n_brands":  int(len(agg)),
        "hhi":       round(hhi, 1),
        "top_brand": str(agg.iloc[0]["brand"]),
        "top_share": round(float(agg.iloc[0]["share_pct"]), 1),
        "total_sales": total,
    }

    status  = "warning" if warnings else "success"
    message = " | ".join(warnings) if warnings else f"{len(agg)}개 브랜드 분석 완료"

    result = {
        "status":  status,
        "message": message,
        "data":    agg,
        "metrics": metrics,
        "_mom_df": mom_df,
    }

    bs = check_sample_size_sanity(len(agg), min_required=3)
    audit, conf = compute_module_audit(
        n_original=n_original,
        n_valid=n_valid,
        role_map=role_map,
        used_roles=["sales_amount", "brand_name", "transaction_date"],
        date_min=_date_min,
        date_max=_date_max,
        formula="브랜드별 매출 합계 → 점유율(%) / HHI",
        agg_unit="브랜드",
        n_computable=len(agg),
        n_periods=len(agg),
        business_checks=bs,
    )
    return enrich_result(result, audit, conf)


def _render(result: dict):
    render_guide("brand")
    if result["status"] == "failed":
        st.error(result["message"])
        return
    if result["status"] == "warning":
        st.warning(result["message"])

    m      = result["metrics"]
    agg    = result.get("data")
    mom_df = result.get("_mom_df", pd.DataFrame())

    # ── 헤더 메트릭 — 절대 규모 우선 ─────────────────────────────────────────
    total_sales = m.get("total_sales", 0)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 매출",   f"{total_sales:,.0f}")
    c2.metric("브랜드 수", f"{m.get('n_brands', 0):,}")
    c3.metric("Top 브랜드", f"{m.get('top_brand', '')}  ({m.get('top_share', 0):.1f}%)")
    c4.metric("HHI", f"{m.get('hhi', 0):.0f}",
              help="허핀달-허쉬만 지수: 10,000=독점 / 1,500 미만=경쟁적")

    tab1, tab2, tab3 = st.tabs(["📊 Sales Overview", "📈 성장률 추이", "📋 데이터"])

    # ── TAB 1: Sales Overview ─────────────────────────────────────────────────
    with tab1:
        if agg is not None and len(agg) > 0:
            top_n   = min(15, len(agg))
            df_plot = agg.head(top_n).copy()

            # 절대 매출 수평 막대 (규모 순)
            df_sorted = df_plot.sort_values("sales", ascending=True)
            fig_bar = go.Figure(go.Bar(
                x=df_sorted["sales"],
                y=df_sorted["brand"],
                orientation="h",
                marker_color="#3b82f6",
                text=[f"{v:,.0f}" for v in df_sorted["sales"]],
                textposition="outside",
            ))
            fig_bar.update_layout(
                title=f"브랜드별 총 매출 (Top {top_n})",
                xaxis_title="매출액",
                height=max(300, len(df_sorted) * 32 + 80),
                margin=dict(t=40, b=0, r=100),
                showlegend=False,
            )
            st.plotly_chart(fig_bar, key="brand_1")

            col1, col2 = st.columns(2)
            with col1:
                fig2 = px.pie(df_plot, names="brand", values="sales",
                              title=f"매출 점유율 (Top {top_n})")
                fig2.update_layout(height=320)
                st.plotly_chart(fig2, key="brand_2")
            with col2:
                rank_df = df_plot[["rank", "brand", "sales", "share_pct"]].copy()
                rank_df = rank_df.rename(columns={
                    "rank": "순위", "brand": "브랜드",
                    "sales": "매출액", "share_pct": "점유율(%)",
                })
                rank_df["매출액"] = rank_df["매출액"].round(0)
                st.dataframe(rank_df, hide_index=True)

    # ── TAB 2: 성장률 추이 ────────────────────────────────────────────────────
    with tab2:
        if not mom_df.empty:
            brands = mom_df["brand"].unique()[:8]
            df_top = mom_df[mom_df["brand"].isin(brands)]
            fig = px.line(
                df_top, x="period_str", y="mom_pct", color="brand",
                title="브랜드별 MoM 성장률 (%)", markers=True,
            )
            fig.add_hline(y=0, line_dash="dot", line_color="gray")
            fig.update_layout(height=420)
            st.plotly_chart(fig, key="brand_3")

            with st.expander("브랜드별 최근 MoM 비교"):
                latest_mom = mom_df.sort_values("period").groupby("brand").last()[["mom_pct"]].reset_index()
                latest_mom = latest_mom.sort_values("mom_pct", ascending=True)
                bar_colors = ["#16a34a" if v >= 0 else "#dc2626" for v in latest_mom["mom_pct"]]
                fig2 = go.Figure(go.Bar(
                    x=latest_mom["mom_pct"].round(1),
                    y=latest_mom["brand"],
                    orientation="h", marker_color=bar_colors,
                    text=latest_mom["mom_pct"].round(1).astype(str) + "%",
                    textposition="outside",
                ))
                fig2.add_vline(x=0, line_color="gray", line_width=1)
                fig2.update_layout(
                    title="최근 기간 MoM 성장률",
                    height=max(280, len(latest_mom) * 28 + 80),
                    margin=dict(t=40, b=0, r=60),
                )
                st.plotly_chart(fig2, key="brand_4")
        else:
            st.info("날짜 또는 브랜드 컬럼이 없어 성장률 추이를 계산할 수 없습니다.")

    # ── TAB 3: 데이터 ─────────────────────────────────────────────────────────
    with tab3:
        if agg is not None:
            st.dataframe(agg, hide_index=True)
