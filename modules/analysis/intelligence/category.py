"""Category Intelligence — share, growth, trend"""
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from modules.common.foundation import _parse_dates
from modules.common.core.audit import compute_module_audit, check_growth_sanity, check_sample_size_sanity
from modules.common.core.result import enrich_result

from modules.analysis.guides import render_guide


def run_category_analysis(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    sales_col = role_map.get("sales_amount")
    date_col  = role_map.get("transaction_date")

    # ── 카테고리 계층 자동 선택 ─────────────────────────────────────────────
    # 분석 대상 우선순위 (medium이 가장 분석 친화적)
    _ANALYSIS_PRIORITY = [
        ("category_medium", "중분류"),
        ("category_large",  "대분류"),
        ("category_small",  "소분류"),
        ("category_name",   "카테고리"),
    ]
    # 계층 자연 순서 (UI 표시 + hierarchy breakdown용)
    _NATURAL_ORDER = [
        ("category_large",  "대분류"),
        ("category_medium", "중분류"),
        ("category_small",  "소분류"),
        ("category_name",   "카테고리"),
    ]

    chosen_level = (params or {}).get("category_level")
    cat_col = None
    cat_level_label = ""
    if chosen_level and role_map.get(chosen_level):
        cat_col = role_map[chosen_level]
        cat_level_label = dict(_ANALYSIS_PRIORITY).get(chosen_level, "카테고리")
    else:
        # 우선순위 순으로 첫 매핑된 컬럼 선택
        for lv_key, lv_label in _ANALYSIS_PRIORITY:
            if role_map.get(lv_key):
                cat_col = role_map[lv_key]
                cat_level_label = lv_label
                break

    # 사용 가능한 모든 계층 (자연 순서: 대→중→소→카테고리)
    # alias로 인한 중복 컬럼은 제외 (예: category_name이 medium과 같은 컬럼이면 스킵)
    levels_present = []
    _seen_cols: set[str] = set()
    for k, lbl in _NATURAL_ORDER:
        col = role_map.get(k)
        if col and col not in _seen_cols:
            levels_present.append({"key": k, "label": lbl, "column": col})
            _seen_cols.add(col)

    if not sales_col:
        return {"status": "failed", "message": "sales_amount 역할 없음", "data": None, "metrics": {}}

    n_original = len(df)
    df = df.copy()
    df["_sales"] = pd.to_numeric(df[sales_col], errors="coerce")
    n_valid = int(df["_sales"].notna().sum())

    warnings: list[str] = []
    if not cat_col:
        warnings.append(
            "카테고리 역할 미매핑 — 전체 단일 집계 "
            "(Step 2에서 category_large / category_medium / category_small 중 하나를 컬럼에 매핑하면 분석 가능)"
        )

    _date_min = _date_max = None
    if date_col:
        df["_date"] = _parse_dates(df[date_col])
        valid_dates = df["_date"].dropna()
        if not valid_dates.empty:
            _date_min = str(valid_dates.min().date())
            _date_max = str(valid_dates.max().date())

    # Category aggregation
    if cat_col:
        df["_cat"] = df[cat_col].fillna("(없음)")
        agg = (
            df.groupby("_cat", as_index=False)["_sales"]
            .sum()
            .rename(columns={"_cat": "category", "_sales": "sales"})
        )
    else:
        agg = pd.DataFrame([{"category": "전체", "sales": float(df["_sales"].sum())}])

    total = float(agg["sales"].sum())
    if total == 0:
        return {"status": "failed", "message": "매출 합계가 0입니다", "data": None, "metrics": {}}

    agg["share_pct"] = (agg["sales"] / total * 100).round(2)
    agg = agg.sort_values("sales", ascending=False).reset_index(drop=True)
    agg["rank"] = agg.index + 1

    # Monthly trend per category
    trend_df = pd.DataFrame()
    growth_df = pd.DataFrame()
    if date_col and cat_col and "_date" in df.columns:
        df["_ym"] = df["_date"].dt.to_period("M")
        monthly = (
            df.groupby([cat_col, "_ym"], as_index=False)["_sales"]
            .sum()
            .rename(columns={cat_col: "category", "_ym": "period", "_sales": "sales"})
        )
        monthly = monthly.sort_values(["category", "period"])
        monthly["mom_pct"]    = monthly.groupby("category")["sales"].pct_change() * 100
        monthly["period_str"] = monthly["period"].astype(str)
        trend_df  = monthly.copy()
        growth_df = monthly.dropna(subset=["mom_pct"]).copy()

    # Recent growth per category (last period vs prev)
    cat_growth: dict = {}
    if not growth_df.empty:
        for cat, grp in growth_df.groupby("category"):
            last_mom = float(grp.sort_values("period").iloc[-1]["mom_pct"])
            cat_growth[cat] = round(last_mom, 1)
        agg["recent_mom"] = agg["category"].map(cat_growth)

    # ── 계층 breakdown (대-중-소 모두 매핑되어 있는 경우 자동 생성) ────────
    hierarchy_data: dict = {}
    if cat_col and len(levels_present) >= 2:
        try:
            level_cols = [(lv["label"], lv["column"]) for lv in levels_present[:3]]
            # 각 상위 카테고리별 하위 카테고리 매출 분포
            for i, (lbl, col_name) in enumerate(level_cols):
                if i == 0:
                    continue
                parent_col = level_cols[i - 1][1]
                if parent_col not in df.columns or col_name not in df.columns:
                    continue
                hier = (
                    df.groupby([parent_col, col_name], as_index=False)["_sales"]
                    .sum()
                    .rename(columns={parent_col: "parent", col_name: "child", "_sales": "sales"})
                )
                hier = hier.sort_values(["parent", "sales"], ascending=[True, False])
                hierarchy_data[f"{level_cols[i-1][0]} → {lbl}"] = hier
        except Exception:
            pass

    metrics = {
        "n_categories":    int(len(agg)),
        "top_category":    str(agg.iloc[0]["category"]) if len(agg) > 0 else "",
        "top_share":       round(float(agg.iloc[0]["share_pct"]), 1) if len(agg) > 0 else 0,
        "total_sales":     total,
        "category_level":  cat_level_label,
        "levels_present":  [lv["label"] for lv in levels_present],
        "has_hierarchy":   len(hierarchy_data) > 0,
    }

    status  = "warning" if warnings else "success"
    if warnings:
        message = " | ".join(warnings)
    elif cat_level_label:
        hier_note = f" + {len(hierarchy_data)}개 계층 breakdown" if hierarchy_data else ""
        message   = f"{cat_level_label} 기준 {len(agg)}개 카테고리 분석 완료{hier_note}"
    else:
        message = f"{len(agg)}개 카테고리 분석 완료"

    result = {
        "status":            status,
        "message":           message,
        "data":              agg,
        "metrics":           metrics,
        "_trend_df":         trend_df,
        "_growth_df":        growth_df,
        "_hierarchy_data":   hierarchy_data,
        "_levels_present":   levels_present,
        "_active_level":     cat_level_label,
    }

    bs = check_sample_size_sanity(len(agg), min_required=3)
    if not growth_df.empty:
        bs += check_growth_sanity(growth_df["mom_pct"])
    audit, conf = compute_module_audit(
        n_original=n_original,
        n_valid=n_valid,
        role_map=role_map,
        used_roles=["sales_amount", "category_name", "category_large",
                    "category_medium", "category_small", "transaction_date"],
        date_min=_date_min,
        date_max=_date_max,
        formula=f"{cat_level_label or '카테고리'}별 매출 합계 → 점유율(%) + MoM 성장률",
        agg_unit=cat_level_label or "카테고리",
        n_computable=len(agg),
        n_periods=len(agg),
        business_checks=bs,
    )
    return enrich_result(result, audit, conf)


def _render(result: dict):
    render_guide("category")
    if result["status"] == "failed":
        st.error(result["message"])
        return
    if result["status"] == "warning":
        st.warning(result["message"])

    m            = result["metrics"]
    agg          = result.get("data")
    trend_df     = result.get("_trend_df", pd.DataFrame())
    hier_data    = result.get("_hierarchy_data", {}) or {}
    levels_pres  = result.get("_levels_present", []) or []
    active_level = result.get("_active_level", "") or "카테고리"

    # ── 현재 분석 중인 계층 + 매핑된 모든 계층 표시 ─────────────────────────
    if levels_pres:
        levels_str = " · ".join(
            f"**{lv['label']}** ({lv['column']})" + (" ← 현재 분석" if lv["label"] == active_level else "")
            for lv in levels_pres
        )
        st.markdown(
            f"<div style='background:#f0fdfa;border-left:3px solid #0d9488;"
            f"padding:8px 14px;border-radius:6px;font-size:13px;margin:8px 0'>"
            f"🌲 매핑된 카테고리 계층: {levels_str}</div>",
            unsafe_allow_html=True,
        )

    # ── 헤더 메트릭 — 절대 규모 우선 ─────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 매출",          f"{m.get('total_sales', 0):,.0f}")
    c2.metric(f"{active_level} 수", f"{m.get('n_categories', 0)}")
    c3.metric(f"Top {active_level}", m.get("top_category", ""))
    c4.metric("Top 점유율",        f"{m.get('top_share', 0):.1f}%")

    # 계층 breakdown 탭은 hierarchy_data가 있을 때만 추가
    if hier_data:
        tab1, tab2, tab3, tab_h, tab4 = st.tabs(
            ["📊 Sales Overview", "📈 성장률", "📅 월별 추이", "🌲 계층 Breakdown", "📋 데이터"]
        )
    else:
        tab1, tab2, tab3, tab4 = st.tabs(
            ["📊 Sales Overview", "📈 성장률", "📅 월별 추이", "📋 데이터"]
        )
        tab_h = None

    # ── TAB 1: Sales Overview ─────────────────────────────────────────────────
    with tab1:
        if agg is not None and len(agg) > 0:
            # 절대 매출 수평 막대 (규모 순)
            df_sorted = agg.sort_values("sales", ascending=True)
            fig_abs = go.Figure(go.Bar(
                x=df_sorted["sales"],
                y=df_sorted["category"],
                orientation="h",
                marker_color="#0d9488",
                text=[f"{v:,.0f}" for v in df_sorted["sales"]],
                textposition="outside",
            ))
            fig_abs.update_layout(
                title="카테고리별 총 매출",
                xaxis_title="매출액",
                height=max(280, len(df_sorted) * 36 + 80),
                margin=dict(t=40, b=0, r=100),
                showlegend=False,
            )
            st.plotly_chart(fig_abs, key="category_1")

            col1, col2 = st.columns(2)
            with col1:
                fig2 = px.pie(agg, names="category", values="sales",
                              title="카테고리 점유율")
                fig2.update_layout(height=320)
                st.plotly_chart(fig2, key="category_2")
            with col2:
                rank_df = agg[["rank", "category", "sales", "share_pct"]].copy()
                rank_df = rank_df.rename(columns={
                    "rank": "순위", "category": "카테고리",
                    "sales": "매출액", "share_pct": "점유율(%)",
                })
                rank_df["매출액"] = rank_df["매출액"].round(0)
                st.dataframe(rank_df, hide_index=True)

    # ── TAB 2: 성장률 ─────────────────────────────────────────────────────────
    with tab2:
        if agg is not None and "recent_mom" in agg.columns and agg["recent_mom"].notna().any():
            df_gr = agg.dropna(subset=["recent_mom"]).sort_values("recent_mom", ascending=True)
            bar_colors = ["#16a34a" if v >= 0 else "#dc2626" for v in df_gr["recent_mom"]]
            fig = go.Figure(go.Bar(
                x=df_gr["recent_mom"].round(1),
                y=df_gr["category"],
                orientation="h",
                marker_color=bar_colors,
                text=df_gr["recent_mom"].round(1).astype(str) + "%",
                textposition="outside",
            ))
            fig.add_vline(x=0, line_color="gray", line_dash="dot")
            fig.update_layout(
                title="카테고리별 최근 MoM 성장률(%)",
                xaxis_title="MoM 성장률(%)",
                height=max(280, len(df_gr) * 36 + 80),
                margin=dict(t=40, b=0, r=60),
            )
            st.plotly_chart(fig, key="category_3")
        else:
            st.info("날짜 컬럼이 없어 성장률 비교를 계산할 수 없습니다.")

    # ── TAB 3: 월별 추이 ──────────────────────────────────────────────────────
    with tab3:
        if not trend_df.empty:
            cats = agg["category"].tolist()[:6] if agg is not None else []
            df_top = trend_df[trend_df["category"].isin(cats)]
            fig = px.line(
                df_top, x="period_str", y="sales", color="category",
                title="카테고리별 월별 매출 추이", markers=True,
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, key="category_4")
        else:
            st.info("날짜 컬럼이 없어 월별 추이를 표시할 수 없습니다.")

    # ── TAB 3.5: 🌲 계층 Breakdown ───────────────────────────────────────────
    if tab_h is not None:
        with tab_h:
            st.caption("매핑된 모든 카테고리 계층에 대해 상위→하위 매출 분포를 분석합니다.")
            for pair_label, hier_df in hier_data.items():
                st.markdown(f"#### {pair_label}")
                if hier_df.empty:
                    st.info("데이터 없음")
                    continue
                # Sunburst chart (대→중 또는 중→소)
                try:
                    fig_sb = px.sunburst(
                        hier_df,
                        path=["parent", "child"],
                        values="sales",
                        title=None,
                    )
                    fig_sb.update_layout(height=420, margin=dict(t=10, b=10))
                    st.plotly_chart(fig_sb, key=f"category_sb_{pair_label}")
                except Exception:
                    pass

                # Stacked bar — 상위 카테고리별 하위 분포
                try:
                    # 너무 많으면 상위 10개 부모만
                    top_parents = (
                        hier_df.groupby("parent")["sales"].sum()
                        .sort_values(ascending=False).head(10).index.tolist()
                    )
                    df_top = hier_df[hier_df["parent"].isin(top_parents)]
                    fig_bar = px.bar(
                        df_top, x="parent", y="sales", color="child",
                        title=f"{pair_label} 매출 stacked",
                    )
                    fig_bar.update_layout(height=380, showlegend=False)
                    st.plotly_chart(fig_bar, key=f"category_stk_{pair_label}")
                except Exception:
                    pass

                # 데이터 표
                with st.expander(f"📋 {pair_label} 상세 데이터", expanded=False):
                    df_disp = hier_df.copy()
                    df_disp["sales"] = df_disp["sales"].round(0)
                    df_disp["share_in_parent_pct"] = (
                        df_disp.groupby("parent")["sales"]
                        .transform(lambda x: x / x.sum() * 100)
                        .round(2)
                    )
                    st.dataframe(
                        df_disp.rename(columns={
                            "parent": "상위", "child": "하위",
                            "sales": "매출액",
                            "share_in_parent_pct": "상위 내 비중(%)",
                        }),
                        hide_index=True, use_container_width=True,
                    )
                st.write("")

    # ── TAB 4: 데이터 ─────────────────────────────────────────────────────────
    with tab4:
        if agg is not None:
            st.dataframe(agg, hide_index=True)
