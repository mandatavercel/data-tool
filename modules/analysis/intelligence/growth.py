"""
Growth Analytics — 일별 / 주간 / 월간 매출 추이 & WoW / MoM / 가속도
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from modules.common.foundation import _parse_dates
from modules.common.core.audit import compute_module_audit, check_growth_sanity, check_sample_size_sanity
from modules.common.core.result import enrich_result

from modules.analysis.guides import render_guide
from modules.common.core.metrics import calculate_growth_rate

# ── 색상 팔레트 ────────────────────────────────────────────────────────────────
C_BLUE   = "#1e40af"
C_GREEN  = "#16a34a"
C_RED    = "#dc2626"
C_GRAY   = "#6b7280"
C_AMBER  = "#d97706"


# ══════════════════════════════════════════════════════════════════════════════
# 내부 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _agg_period(df: pd.DataFrame, sales_col: str, name_col: str, freq: str) -> pd.DataFrame:
    """name_col + 기간(freq)별 매출 합산."""
    out = (
        df.groupby([name_col, pd.Grouper(freq=freq)])[sales_col]
        .sum()
        .reset_index()
        .sort_values([name_col, df.index.name or "index"])
    )
    return out


def _add_growth(df: pd.DataFrame, sales_col: str, name_col: str,
                periods: int, col: str) -> pd.DataFrame:
    """groupby(name_col) 기준 성장률 컬럼 추가."""
    df = df.copy()
    df[col] = df.groupby(name_col)[sales_col].transform(
        lambda s: calculate_growth_rate(s, periods=periods)
    )
    return df


def _add_accel(df: pd.DataFrame, growth_col: str, name_col: str, accel_col: str) -> pd.DataFrame:
    """성장률의 1기간 변화(가속/감속) 컬럼 추가."""
    df = df.copy()
    df[accel_col] = df.groupby(name_col)[growth_col].transform(lambda s: s.diff())
    return df


def _bar_line(x, y_bar, y_line=None, bar_name="매출", line_name="추세",
              title="", x_label="기간", y_label="매출액") -> go.Figure:
    """막대(매출) + 선(이동평균/추세) 조합 차트."""
    fig = go.Figure()
    fig.add_bar(x=x, y=y_bar, name=bar_name,
                marker_color=C_BLUE, opacity=0.75)
    if y_line is not None:
        fig.add_scatter(x=x, y=y_line, mode="lines", name=line_name,
                        line=dict(color=C_AMBER, width=2))
    fig.update_layout(title=title, xaxis_title=x_label, yaxis_title=y_label,
                      hovermode="x unified", legend=dict(orientation="h"),
                      margin=dict(t=40, b=0))
    return fig


def _growth_bar(x, y, title="") -> go.Figure:
    """성장률 막대 차트 (양수=초록, 음수=빨강)."""
    colors = [C_GREEN if v >= 0 else C_RED
              for v in pd.Series(y).fillna(0)]
    fig = go.Figure()
    fig.add_bar(x=x, y=y, marker_color=colors, name="성장률")
    fig.add_hline(y=0, line_dash="dash", line_color=C_GRAY, line_width=1)
    fig.update_layout(title=title, xaxis_title="기간", yaxis_title="%",
                      hovermode="x unified", margin=dict(t=40, b=0))
    return fig


def _accel_bar(x, y, title="") -> go.Figure:
    """가속도 막대 차트 (양수=가속, 음수=감속)."""
    colors = [C_GREEN if v >= 0 else C_RED
              for v in pd.Series(y).fillna(0)]
    fig = go.Figure()
    fig.add_bar(x=x, y=y, marker_color=colors, name="가속도",
                opacity=0.8)
    fig.add_hline(y=0, line_dash="dot", line_color=C_GRAY, line_width=1)
    fig.update_layout(title=title, xaxis_title="기간",
                      yaxis_title="성장률 변화 (pp)",
                      hovermode="x unified", margin=dict(t=40, b=0))
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# 탭별 렌더러
# ══════════════════════════════════════════════════════════════════════════════

def _tab_daily(sub: pd.DataFrame, date_col: str, sales_col: str, company: str):
    if sub.empty:
        st.info("일별 데이터가 없습니다.")
        return

    sub = sub.copy().sort_values(date_col)
    ma7 = sub[sales_col].rolling(7, min_periods=1).mean()

    st.plotly_chart(
        _bar_line(sub[date_col], sub[sales_col], ma7,
                  bar_name="일매출", line_name="7일 이동평균",
                  title=f"{company} — 일별 매출 추이",
                  y_label="매출액"), key="growth_1"
    )

    with st.expander("일별 데이터 테이블"):
        st.dataframe(
            sub[[date_col, sales_col]].rename(columns={
                date_col: "날짜", sales_col: "매출액"
            }).sort_values("날짜", ascending=False), hide_index=True,
        )


def _section_company(monthly: pd.DataFrame, date_col: str,
                     sales_col: str, name_col: str):
    """회사별 성장률 비교 섹션."""
    st.markdown("### 🏆 회사별 성장률 비교")

    latest = (
        monthly.sort_values(date_col)
        .groupby(name_col)
        .last()
        .reset_index()
    )

    n_co  = len(latest)
    TOP_N = 10

    c1, c2 = st.columns(2)

    with c1:
        # 회사가 많으면 상위/하위 N개만, 적으면 전체 표시
        if n_co > TOP_N * 2:
            sorted_mom = latest.sort_values("MoM")
            bar_df = pd.concat([
                sorted_mom.head(TOP_N),
                sorted_mom.tail(TOP_N),
            ]).drop_duplicates().sort_values("MoM")
            mom_title = f"최신 MoM — 상위/하위 {TOP_N}개사"
        else:
            bar_df = latest.sort_values("MoM")
            mom_title = "최신 MoM (%)"

        bar_colors = [C_GREEN if v >= 0 else C_RED for v in bar_df["MoM"]]
        fig_mom = go.Figure(go.Bar(
            x=bar_df["MoM"].round(1), y=bar_df[name_col],
            orientation="h", marker_color=bar_colors,
            text=bar_df["MoM"].round(1).astype(str) + "%",
            textposition="outside",
        ))
        fig_mom.add_vline(x=0, line_color="#9ca3af", line_width=1)
        fig_mom.update_layout(
            title=mom_title, xaxis_title="MoM (%)",
            height=max(300, len(bar_df) * 28 + 80),
            margin=dict(t=40, b=20, l=10, r=70),
            showlegend=False,
        )
        st.plotly_chart(fig_mom, key="growth_8")

    with c2:
        # 매출 상위 N개사
        top_sales = (
            latest.sort_values(sales_col, ascending=True).tail(TOP_N)
            if n_co > TOP_N
            else latest.sort_values(sales_col, ascending=True)
        )
        sales_title = f"최신 월 매출 — 상위 {TOP_N}개사" if n_co > TOP_N else "최신 월 매출"
        fig_sales = go.Figure(go.Bar(
            x=top_sales[sales_col], y=top_sales[name_col],
            orientation="h", marker_color="#3b82f6",
        ))
        fig_sales.update_layout(
            title=sales_title, xaxis_title="매출액",
            height=max(300, len(top_sales) * 28 + 80),
            margin=dict(t=40, b=20, l=10, r=20),
            showlegend=False,
        )
        st.plotly_chart(fig_sales, key="growth_9")

    # ── 히트맵 (스파게티 라인 대체) ─────────────────────────────────────────
    # 매출 기준 내림차순으로 회사 정렬, 최근 24개월만 표시
    companies_order = latest.sort_values(sales_col, ascending=False)[name_col].tolist()
    pivot = monthly.pivot_table(index=name_col, columns=date_col, values="MoM", aggfunc="mean")
    pivot = pivot.reindex([c for c in companies_order if c in pivot.index])
    pivot = pivot.iloc[:, -24:]  # 최근 24개월

    col_labels = [str(c)[:7] for c in pivot.columns]
    z_vals     = pivot.values.tolist()

    # 셀 수가 적으면 수치 표시
    show_text  = len(pivot) <= 12 and len(pivot.columns) <= 18
    text_vals  = [[f"{v:.1f}" if pd.notna(v) else "" for v in row] for row in pivot.values]

    fig_heat = go.Figure(go.Heatmap(
        z=z_vals, x=col_labels, y=pivot.index.tolist(),
        colorscale=[[0, "#dc2626"], [0.5, "#f3f4f6"], [1, "#16a34a"]],
        zmid=0, zmin=-30, zmax=30,
        text=text_vals if show_text else None,
        texttemplate="%{text}" if show_text else "",
        textfont=dict(size=8),
        colorbar=dict(title="MoM (%)"),
        hoverongaps=False,
        hovertemplate="%{y}<br>%{x}<br>MoM: <b>%{z:.1f}%</b><extra></extra>",
    ))
    fig_heat.update_layout(
        title="회사별 월간 MoM 히트맵 (최근 24개월) — 매출 규모순",
        height=max(320, len(pivot) * 28 + 120),
        xaxis=dict(tickangle=-45, side="bottom"),
        yaxis=dict(autorange="reversed"),
        margin=dict(t=50, b=60, l=10, r=80),
    )
    st.plotly_chart(fig_heat, key="growth_10")

    # 요약 테이블
    with st.expander("회사별 요약 테이블"):
        tbl = latest[[name_col, sales_col, "MoM", "MoM_accel"]].copy()
        tbl = tbl.rename(columns={
            name_col: "회사", sales_col: "최신월 매출",
            "MoM": "MoM (%)", "MoM_accel": "가속도 (pp)",
        })
        tbl["MoM (%)"]    = tbl["MoM (%)"].round(1)
        tbl["가속도 (pp)"] = tbl["가속도 (pp)"].round(1)
        st.dataframe(tbl.sort_values("MoM (%)", ascending=False), hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# 메인 함수
# ══════════════════════════════════════════════════════════════════════════════

def run_growth_analysis(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    """
    일별 / 주간 / 월간 매출 추이와 WoW / MoM / 가속도를 계산하고 Streamlit에 표시.

    Returns:
        daily    : pd.DataFrame  일별 집계
        weekly   : pd.DataFrame  주간 집계 + WoW + WoW_accel
        monthly  : pd.DataFrame  월간 집계 + MoM + MoM_accel
        agg_df   : pd.DataFrame  (= monthly, Signal Dashboard 호환용)
        date_col, sales_col, name_col : str
    """
    date_col  = role_map.get("transaction_date")
    sales_col = role_map.get("sales_amount")
    name_col  = role_map.get("company_name") or role_map.get("brand_name")

    # ── 전처리 ────────────────────────────────────────────────────────────────
    n_original = len(df)
    df = df.copy()
    df[date_col]  = _parse_dates(df[date_col])
    df[sales_col] = pd.to_numeric(df[sales_col], errors="coerce")
    df = df.dropna(subset=[date_col, sales_col])
    n_valid   = len(df)
    _date_min = str(df[date_col].min().date()) if n_valid > 0 else None
    _date_max = str(df[date_col].max().date()) if n_valid > 0 else None

    if not name_col:
        df["__all__"] = "전체"
        name_col = "__all__"

    df = df.set_index(date_col)
    date_col_name = df.index.name          # reset_index 후 컬럼명 복원용

    # ── 집계 ──────────────────────────────────────────────────────────────────
    daily   = _agg_period(df, sales_col, name_col, "D")
    weekly  = _agg_period(df, sales_col, name_col, "W")
    monthly = _agg_period(df, sales_col, name_col, "ME")

    # reset_index 후 date 컬럼명 확인 (Grouper 결과는 index name 사용)
    # daily/weekly/monthly 의 date 컬럼은 date_col_name
    date_col = date_col_name

    # ── WoW ───────────────────────────────────────────────────────────────────
    weekly = _add_growth(weekly, sales_col, name_col, 1, "WoW")
    weekly = _add_accel(weekly, "WoW", name_col, "WoW_accel")

    # ── MoM ───────────────────────────────────────────────────────────────────
    monthly = _add_growth(monthly, sales_col, name_col, 1, "MoM")
    monthly = _add_accel(monthly, "MoM", name_col, "MoM_accel")

    n_months = int(monthly[name_col].value_counts().max()) if not monthly.empty else 0
    n_weeks  = int(weekly[name_col].value_counts().max())  if not weekly.empty  else 0

    bs  = check_sample_size_sanity(n_months, min_required=12)
    bs += check_growth_sanity(monthly["MoM"].dropna() if "MoM" in monthly.columns else None)

    audit, conf = compute_module_audit(
        n_original=n_original,
        n_valid=n_valid,
        role_map=role_map,
        used_roles=["transaction_date", "sales_amount", "company_name"],
        date_min=_date_min,
        date_max=_date_max,
        formula="월간 MoM = (이번달 / 전달 - 1) × 100 | WoW 동일 주기",
        agg_unit="월",
        window=1,
        n_computable=n_months,
        n_periods=n_months,
        business_checks=bs,
    )

    result = {
        "status":    "success",
        "message":   f"월 {n_months}개 / 주 {n_weeks}개 집계 완료",
        "data":      monthly,
        "metrics":   {"n_months": n_months, "n_weeks": n_weeks},
        "daily":     daily,
        "weekly":    weekly,
        "monthly":   monthly,
        "agg_df":    monthly,
        "date_col":  date_col,
        "sales_col": sales_col,
        "name_col":  name_col,
    }
    return enrich_result(result, audit, conf)


def _render(result: dict):
    render_guide("growth")
    # 실패/경고 결과 안전 처리
    if not isinstance(result, dict) or result.get("status") == "failed":
        st.error(result.get("message", "Growth 분석 실패") if isinstance(result, dict) else "결과 없음")
        return
    if "monthly" not in result or result.get("monthly") is None:
        st.info(result.get("message", "Growth 결과 데이터 부족"))
        return
    name_col  = result.get("name_col")
    date_col  = result.get("date_col")
    sales_col = result.get("sales_col")
    if not (name_col and date_col and sales_col):
        st.info("필수 컬럼 정보 누락 — date/sales/name col")
        return
    try:
        companies = sorted(result["monthly"][name_col].unique())
    except (KeyError, AttributeError):
        st.info("Growth 결과의 회사명 컬럼이 비어 있습니다.")
        return

    # ── 회사 선택 ──────────────────────────────────────────────────────────────
    if len(companies) > 1:
        sel = st.selectbox("분석 대상 회사", companies, key="gr_co")
    else:
        sel = companies[0]

    d_co = result["daily"][result["daily"][name_col] == sel]
    w_co = result["weekly"][result["weekly"][name_col] == sel]
    m_co = result["monthly"][result["monthly"][name_col] == sel].sort_values(date_col)

    # ── 헤더 메트릭 — 절대 규모 우선 ─────────────────────────────────────────
    st.markdown(f"### {sel}")
    total  = m_co[sales_col].sum()
    avg_m  = m_co[sales_col].mean()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("총 매출",     f"{total:,.0f}")
    c2.metric("월 평균 매출", f"{avg_m:,.0f}")

    if not m_co.empty:
        peak_idx   = m_co[sales_col].idxmax()
        trough_idx = m_co[sales_col].idxmin()
        peak_row   = m_co.loc[peak_idx]
        trough_row = m_co.loc[trough_idx]
        peak_label   = str(peak_row[date_col])[:7]
        trough_label = str(trough_row[date_col])[:7]
        c3.metric("최대 매출 기간", peak_label,
                  f"{peak_row[sales_col]:,.0f}")
        c4.metric("최저 매출 기간", trough_label,
                  f"{trough_row[sales_col]:,.0f}")

    # ── 탭 ────────────────────────────────────────────────────────────────────
    extra_tabs = ["🏆 회사 비교"] if len(companies) > 1 else []
    tab_labels = ["📊 Sales Overview", "📈 성장률 분석", "📅 일별 추이", "📋 데이터"] + extra_tabs
    tabs = st.tabs(tab_labels)

    tab_ov, tab_gr, tab_d, tab_data = tabs[0], tabs[1], tabs[2], tabs[3]
    tab_cmp = tabs[4] if len(companies) > 1 else None

    # ── TAB 1: Sales Overview — 절대 매출 추이 ────────────────────────────────
    with tab_ov:
        if not m_co.empty:
            # 월별 절대 매출 막대 + 이동평균
            ma3 = m_co[sales_col].rolling(3, min_periods=1).mean()
            st.plotly_chart(
                _bar_line(m_co[date_col], m_co[sales_col], ma3,
                          bar_name="월간 매출", line_name="3개월 이동평균",
                          title=f"{sel} — 월별 매출 추이",
                          y_label="매출액"), key="growth_11"
            )

            # 월별 매출 요약 테이블 (절대값 중심)
            with st.expander("월별 매출 요약"):
                tbl = m_co[[date_col, sales_col]].copy()
                tbl = tbl.rename(columns={date_col: "월", sales_col: "매출액"})
                tbl["누적합"] = tbl["매출액"].cumsum().round(0)
                tbl["매출액"] = tbl["매출액"].round(0)
                st.dataframe(tbl.sort_values("월", ascending=False), hide_index=True)

        if not d_co.empty:
            st.divider()
            d_sub = d_co.copy().sort_values(date_col)
            ma7 = d_sub[sales_col].rolling(7, min_periods=1).mean()
            st.plotly_chart(
                _bar_line(d_sub[date_col], d_sub[sales_col], ma7,
                          bar_name="일매출", line_name="7일 이동평균",
                          title=f"{sel} — 일별 매출 추이",
                          y_label="매출액"), key="growth_12"
            )

    # ── TAB 2: 성장률 분석 — MoM / WoW / 가속도 ─────────────────────────────
    with tab_gr:
        if not m_co.empty and len(m_co) >= 2:
            mom = m_co["MoM"].dropna()
            if not mom.empty:
                c1g, c2g = st.columns(2)
                with c1g:
                    last_mom  = float(m_co["MoM"].dropna().iloc[-1])
                    last_acc  = m_co["MoM_accel"].dropna()
                    last_acc  = float(last_acc.iloc[-1]) if not last_acc.empty else float("nan")
                    st.metric("최신 MoM", f"{last_mom:.1f}%",
                              delta=f"전월比 {last_acc:+.1f}pp" if not pd.isna(last_acc) else None)
                with c2g:
                    if not w_co.empty and len(w_co) >= 2:
                        last_wow = w_co.sort_values(date_col)["WoW"].dropna()
                        if not last_wow.empty:
                            st.metric("최신 WoW", f"{float(last_wow.iloc[-1]):.1f}%")

                c1, c2 = st.columns(2)
                with c1:
                    st.plotly_chart(
                        _growth_bar(m_co[date_col], m_co["MoM"],
                                    title="월간 MoM 성장률 (%)"), key="growth_13"
                    )
                with c2:
                    st.plotly_chart(
                        _accel_bar(m_co[date_col], m_co["MoM_accel"],
                                   title="MoM 가속도 (성장률 변화 pp)"), key="growth_14"
                    )

        if not w_co.empty and len(w_co) >= 2:
            st.divider()
            w_sub = w_co.copy().sort_values(date_col)
            if w_sub["WoW"].notna().any():
                c1, c2 = st.columns(2)
                with c1:
                    st.plotly_chart(
                        _growth_bar(w_sub[date_col], w_sub["WoW"],
                                    title="주간 WoW 성장률 (%)"), key="growth_15"
                    )
                with c2:
                    st.plotly_chart(
                        _accel_bar(w_sub[date_col], w_sub["WoW_accel"],
                                   title="WoW 가속도 (성장률 변화 pp)"), key="growth_16"
                    )

        with st.expander("성장률 데이터 테이블"):
            show = m_co[[date_col, sales_col, "MoM", "MoM_accel"]].copy()
            show = show.rename(columns={
                date_col: "월", sales_col: "매출액",
                "MoM": "MoM(%)", "MoM_accel": "가속도(pp)",
            })
            show["MoM(%)"]   = show["MoM(%)"].round(1)
            show["가속도(pp)"] = show["가속도(pp)"].round(1)
            st.dataframe(show.sort_values("월", ascending=False), hide_index=True)

    # ── TAB 3: 일별 추이 ──────────────────────────────────────────────────────
    with tab_d:
        _tab_daily(d_co, date_col, sales_col, sel)

    # ── TAB 4: 데이터 ─────────────────────────────────────────────────────────
    with tab_data:
        show = m_co[[date_col, sales_col, "MoM", "MoM_accel"]].copy()
        show = show.rename(columns={
            date_col: "월", sales_col: "매출액",
            "MoM": "MoM(%)", "MoM_accel": "가속도(pp)",
        })
        st.dataframe(show.sort_values("월", ascending=False), hide_index=True)

    # ── TAB 5: 회사 비교 (멀티컴퍼니) ────────────────────────────────────────
    if tab_cmp is not None:
        with tab_cmp:
            _section_company(result["monthly"], date_col, sales_col, name_col)
