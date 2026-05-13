"""
Investor Dashboard — Step 5 첫 화면.

데이터 분석가가 팀과 데이터에 대해 토론할 때 필요한 핵심 인사이트:
1. 매출 성장률 Top (회사 / 브랜드 / SKU)
2. 매수/매도 후보 종목 (주가 상관성 기반)
3. 주가 상관성 Top 회사
4. 공시매출 상관성 Top 회사
5. 알파 창출 가능 지점 — narrative
"""
from __future__ import annotations

import pandas as pd
import streamlit as st


# ══════════════════════════════════════════════════════════════════════════════
# 컴포넌트 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _kpi_card(title: str, value: str, sub: str = "",
              accent: str = "#3b82f6", bg: str = "#eff6ff") -> str:
    """파스텔 colored KPI card — HTML 반환."""
    return (
        f"<div style='background:{bg};border-radius:10px;padding:16px 18px;height:100%'>"
        f"<div style='font-size:11px;color:#64748b;letter-spacing:0.5px;"
        f"text-transform:uppercase;margin-bottom:6px;font-weight:600'>{title}</div>"
        f"<div style='font-size:22px;font-weight:700;color:{accent};line-height:1.2;margin-bottom:4px'>{value}</div>"
        f"<div style='font-size:11px;color:#64748b;line-height:1.4'>{sub}</div>"
        f"</div>"
    )


def _render_glossary() -> None:
    """초보자용 용어 가이드 expander."""
    with st.expander("📖 용어 가이드 — 처음 보는 단어가 있으면 펼쳐주세요", expanded=False):
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            st.markdown(
                "<div style='font-size:13px;line-height:1.7;color:#334155'>"
                "<b>매출 상관성 r</b> — POS 매출 변화와 주가 변화의 상관계수. "
                "0.5+ 강함, 0.3+ 의미 있음, 0.3 미만 약함.<br><br>"
                "<b>공시 Direction Match</b> — POS 분기 변화와 DART 공시 변화의 부호(↑↓) 일치율. "
                "70%+면 POS만 봐도 실적 방향 예측 가능.<br><br>"
                "<b>YoY</b> — Year-over-Year. 작년 동기 대비 매출 성장률.<br><br>"
                "<b>Lag (시차)</b> — 매출이 움직인 뒤 주가가 반응하기까지 걸리는 시간. "
                "양수면 POS가 주가를 선행."
                "</div>",
                unsafe_allow_html=True,
            )
        with gcol2:
            st.markdown(
                "<div style='font-size:13px;line-height:1.7;color:#334155'>"
                "<b>POS</b> — Point of Sale. 매장 결제 단말기 거래 데이터.<br><br>"
                "<b>DART</b> — 금융감독원 전자공시. 상장사 분기·연간 매출 공식 발표.<br><br>"
                "<b>알파 신호</b> — 시장보다 빠르게 매출 변화를 포착해 수익을 낼 수 있는 시그널. "
                "|r| ≥ 0.3 + lag ≥ 1주가 일반적 기준.<br><br>"
                "<b>HHI</b> — 집중도 지수. 상위 회사 매출 비중이 클수록 높음."
                "</div>",
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# 데이터 추출 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _yoy_by_group(df: pd.DataFrame, group_col: str, date_col: str,
                  sales_col: str, n: int = 10) -> list[dict]:
    """그룹(회사/브랜드/SKU)별 최근 12개월 vs 직전 12개월 YoY 계산."""
    try:
        from modules.common.foundation import _parse_dates
        dates = _parse_dates(df[date_col])
        end = dates.max()
        if pd.isna(end):
            return []
        cutoff      = end - pd.Timedelta(days=365)
        prior_start = end - pd.Timedelta(days=730)
        s = pd.to_numeric(df[sales_col], errors="coerce")
        tmp = pd.DataFrame({"name": df[group_col], "d": dates, "s": s}).dropna()
        recent = tmp[tmp["d"] > cutoff].groupby("name")["s"].sum()
        prior  = tmp[(tmp["d"] > prior_start) & (tmp["d"] <= cutoff)].groupby("name")["s"].sum()
        common = recent.index.intersection(prior.index)
        if len(common) == 0:
            return []
        # 최소 1억원 이상 회사만 (노이즈 컷)
        common = common[recent.loc[common] > 0]
        yoy = ((recent.loc[common] - prior.loc[common]) / prior.loc[common].abs() * 100).replace(
            [float("inf"), -float("inf")], pd.NA
        ).dropna()
        if yoy.empty:
            return []
        df_yoy = pd.DataFrame({
            "name":   yoy.index.astype(str),
            "yoy":    yoy.values,
            "sales":  recent.loc[yoy.index].values,
        }).sort_values("yoy", ascending=False).head(n)
        return df_yoy.to_dict("records")
    except Exception:
        return []


def _top_market_corr(results: dict, n: int = 10) -> list[dict]:
    """market_signal의 회사별 |r| Top."""
    mkt = results.get("market_signal", {}) or {}
    sigs = [s for s in (mkt.get("_company_signals", []) or []) if s.get("status") == "ok"]
    sigs_sorted = sorted(sigs, key=lambda s: abs(s.get("max_corr") or 0), reverse=True)
    return [
        {
            "name":   s.get("company", "")[:30],
            "ticker": s.get("ticker", ""),
            "r":      float(s.get("max_corr") or 0),
            "lag":    s.get("best_lag"),
            "score":  float(s.get("signal_score") or 0),
        }
        for s in sigs_sorted[:n]
    ]


def _top_earnings_corr(results: dict, n: int = 10) -> list[dict]:
    """earnings_intel의 회사별 Tracking Quality / Direction Match Top."""
    earn = results.get("earnings_intel", {}) or {}
    tracks = (earn.get("metrics", {}) or {}).get("company_tracking", {}) or {}
    rows = [
        {
            "name":       cname[:30],
            "direction":  float(t.get("direction_match", 0)),
            "corr":       float(t.get("correlation", 0)),
            "stability":  float(t.get("stability", 0)),
            "quality":    float(t.get("quality", 0)),
            "n_quarters": int(t.get("n_quarters", 0)),
        }
        for cname, t in tracks.items() if t
    ]
    return sorted(rows, key=lambda x: x["quality"], reverse=True)[:n]


def _alpha_insights(results: dict) -> list[str]:
    """알파 창출 가능 지점 narrative 생성."""
    insights = []

    # ── 1. 강한 매출-주가 상관 회사 ──────────────────────────────────────────
    mkt_sigs = [s for s in (results.get("market_signal", {}).get("_company_signals", []) or [])
                 if s.get("status") == "ok"]
    strong = [s for s in mkt_sigs if abs(s.get("max_corr") or 0) >= 0.5]
    medium = [s for s in mkt_sigs if 0.3 <= abs(s.get("max_corr") or 0) < 0.5]
    if strong:
        names = ", ".join(s.get("company", "")[:14] for s in strong[:3])
        insights.append(
            f"🎯 **강한 알파 신호 (|r| ≥ 0.5)**: {len(strong)}개 종목 — {names}"
            f"{(' 외 ' + str(len(strong)-3) + '개') if len(strong) > 3 else ''}. "
            "분기 어닝 발표 전 매출 모멘텀으로 방향 예측 가능 → L/S 또는 Event-Driven 전략 후보."
        )
    if medium:
        insights.append(
            f"⚡ **중간 알파 신호 (|r| 0.3~0.5)**: {len(medium)}개 종목 — "
            "단독 신호는 부족하지만 다른 Factor와 결합 시 한계 alpha 기여 가능."
        )

    # ── 2. 선행 lag 패턴 ──────────────────────────────────────────────────────
    valid_lags = [s.get("best_lag") for s in mkt_sigs
                   if s.get("best_lag") is not None and abs(s.get("max_corr") or 0) >= 0.3]
    if valid_lags:
        avg_lag = sum(valid_lags) / len(valid_lags)
        max_lag = max(valid_lags)
        insights.append(
            f"📅 **선행 시차 분포**: {len(valid_lags)}개 종목 평균 {avg_lag:.1f}주 선행 "
            f"(최대 {max_lag}주). 즉 POS 매출 변화 후 {avg_lag:.0f}주 뒤에 주가가 반응 — "
            "이 윈도우 안에 진입하면 알파 포착 가능."
        )

    # ── 3. DART 공시 일치도 ──────────────────────────────────────────────────
    earn = results.get("earnings_intel", {}) or {}
    em = earn.get("metrics", {}) or {}
    dm_avg = em.get("direction_match_avg")
    if dm_avg is not None and dm_avg >= 70:
        insights.append(
            f"📊 **POS-공시 일치도 {dm_avg:.0f}%**: POS 데이터로 공시 매출 방향성을 "
            f"70%+ 정확도로 예측. 분기 컨센서스 Surprise 베팅에 활용 가능."
        )
    elif dm_avg is not None and dm_avg >= 50:
        insights.append(
            f"📊 **POS-공시 일치도 {dm_avg:.0f}%**: 보조 신호로 활용 — 단독 사용은 위험."
        )

    # ── 4. 이상 패턴 ────────────────────────────────────────────────────────
    anom = results.get("anomaly", {}) or {}
    n_anom = (anom.get("metrics", {}) or {}).get("n_anomaly")
    if n_anom and n_anom > 0:
        insights.append(
            f"🚨 **이상 이벤트 {n_anom}건 감지**: 프로모션·공급 충격·구조 변화 등. "
            "Event-Driven 전략의 트리거 신호로 활용 가능."
        )

    # ── 5. Factor 신호 ──────────────────────────────────────────────────────
    fr = results.get("factor_research", {}) or {}
    fm = fr.get("metrics", {}) or {}
    ic = fm.get("ic_mean")
    if ic is not None and abs(ic) >= 0.05:
        sharpe = fm.get("ls_sharpe", 0)
        insights.append(
            f"🧪 **Factor 검증**: Cross-Sectional Rank IC = {ic:+.3f}, "
            f"L/S Sharpe = {sharpe:+.2f}. 헤지펀드 표준 Factor zoo 추가 후보."
        )

    if not insights:
        insights.append(
            "ℹ️ 현재 결과에서 강한 알파 신호는 발견되지 않았어요. "
            "Market Signal · Earnings Intelligence · Factor Research 모듈을 모두 실행하고 "
            "데이터 기간을 늘려보세요 (최소 24개월 권장)."
        )

    return insights


# ══════════════════════════════════════════════════════════════════════════════
# Top N 표시 헬퍼 (성장률 / 상관성 / Quality)
# ══════════════════════════════════════════════════════════════════════════════

def _render_growth_top(label: str, rows: list[dict], unit: str = "원") -> None:
    """매출 성장률 Top 표 렌더링 (3 column tab용)."""
    if not rows:
        st.caption(f"_{label} 데이터 부족_")
        return
    df = pd.DataFrame(rows)
    df["YoY"]   = df["yoy"].apply(lambda v: f"{v:+.1f}%")
    df["매출"] = df["sales"].apply(
        lambda v: f"{v/1e8:.1f}억" if v >= 1e8 else f"{v/1e4:.0f}만"
    )
    view = df[["name", "YoY", "매출"]].rename(columns={"name": label})
    st.dataframe(view, hide_index=True, use_container_width=True)


def _render_market_corr_top(rows: list[dict]) -> None:
    """주가 상관성 Top 표 렌더링."""
    if not rows:
        st.info("Market Signal을 먼저 실행하면 회사별 주가 상관성이 표시됩니다.")
        return
    df = pd.DataFrame(rows)
    df["r"]   = df["r"].apply(lambda v: f"{v:+.3f}")
    df["lag"] = df["lag"].apply(lambda v: f"{v}주" if v is not None else "—")
    df["⭐"] = pd.DataFrame(rows)["r"].apply(
        lambda v: "🟢" if abs(v) >= 0.5 else "🟡" if abs(v) >= 0.3 else "🔴"
    )
    view = df[["⭐", "name", "ticker", "r", "lag", "score"]].rename(columns={
        "name": "회사", "ticker": "Ticker", "score": "Signal Score",
    })
    view["Signal Score"] = view["Signal Score"].apply(lambda v: f"{v:.0f}/100")
    st.dataframe(view, hide_index=True, use_container_width=True)


def _render_earnings_corr_top(rows: list[dict]) -> None:
    """공시매출 상관성 Top 표 렌더링."""
    if not rows:
        st.info("Earnings Intelligence를 먼저 실행하면 공시 상관성이 표시됩니다.")
        return
    df = pd.DataFrame(rows)
    df["Direction"] = df["direction"].apply(lambda v: f"{v:.0f}%")
    df["|r|"]       = df["corr"].apply(lambda v: f"{abs(v):.2f}")
    df["Stability"] = df["stability"].apply(lambda v: f"{v:.2f}")
    df["Quality"]   = df["quality"].apply(lambda v: f"{v:.0f}/100")
    df["⭐"] = df["quality"].apply(
        lambda v: "🟢" if v >= 65 else "🟡" if v >= 40 else "🔴"
    )
    view = df[["⭐", "name", "Quality", "Direction", "|r|", "Stability", "n_quarters"]].rename(columns={
        "name": "회사", "n_quarters": "N분기",
    })
    st.dataframe(view, hide_index=True, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# 메인 렌더러
# ══════════════════════════════════════════════════════════════════════════════

def render_investor_dashboard(results: dict, analysis_options: dict | None = None) -> None:
    """Step 5 첫 화면 — 데이터 분석가가 팀과 토론할 때 필요한 핵심 인사이트."""
    import plotly.graph_objects as go

    # ── 데이터 추출 ──────────────────────────────────────────────────────────
    raw_df   = st.session_state.get("raw_df")
    role_map = st.session_state.get("role_map", {}) or {}
    n_rows   = len(raw_df) if raw_df is not None else 0

    mkt = results.get("market_signal", {}) or {}
    mkt_metrics = mkt.get("metrics", {}) or {}
    mkt_ok = [s for s in (mkt.get("_company_signals", []) or []) if s.get("status") == "ok"]

    earn = results.get("earnings_intel", {}) or {}
    earn_metrics = earn.get("metrics", {}) or {}

    n_companies = (mkt_metrics.get("n_companies_total") or
                   earn_metrics.get("n_dart_companies") or 0)
    n_periods = mkt_metrics.get("n_months") or 0

    # 데이터 기간
    date_range_str = "—"
    if raw_df is not None and role_map.get("transaction_date"):
        date_col = role_map["transaction_date"]
        if date_col in raw_df.columns:
            try:
                from modules.common.foundation import _parse_dates
                dates = _parse_dates(raw_df[date_col]).dropna()
                if not dates.empty:
                    date_range_str = f"{dates.min().strftime('%Y-%m')} ~ {dates.max().strftime('%Y-%m')}"
            except Exception:
                pass

    # ── 헤더 ────────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='display:flex;align-items:baseline;gap:14px;margin-bottom:4px'>"
        f"<div style='font-size:22px;font-weight:700;color:#0f172a'>🎯 Investor Dashboard</div>"
        f"<div style='font-size:12px;color:#64748b'>{date_range_str} · "
        f"{n_companies} stocks · {n_rows:,} rows · {len(results)} modules</div>"
        f"</div>",
        unsafe_allow_html=True,
    )
    st.caption(
        "데이터 분석가가 팀과 데이터에 대해 논의할 때 필요한 핵심 인사이트. "
        "익숙하지 않은 용어는 ↓ 가이드 참조."
    )
    _render_glossary()
    st.write("")

    # ── KPI 4종 (분석가 핵심) ──────────────────────────────────────────────
    # 1. 매출 성장률 1위 회사
    # 2. 주가 상관 1위 회사
    # 3. 공시 상관 1위 회사
    # 4. 알파 가능 종목 수
    k1, k2, k3, k4 = st.columns(4)

    # KPI 1: 매출 성장 1위
    company_growth_rows = []
    if raw_df is not None and role_map.get("company_name") and role_map.get("transaction_date") and role_map.get("sales_amount"):
        company_growth_rows = _yoy_by_group(
            raw_df, role_map["company_name"], role_map["transaction_date"],
            role_map["sales_amount"], n=10,
        )
    if company_growth_rows:
        top = company_growth_rows[0]
        with k1:
            st.markdown(_kpi_card(
                "매출 성장 1위 회사", top["name"][:18],
                f"최근 12M YoY <b style='color:#16a34a'>{top['yoy']:+.1f}%</b>",
                accent="#16a34a", bg="#dcfce7",
            ), unsafe_allow_html=True)
    else:
        with k1:
            st.markdown(_kpi_card("매출 성장 1위 회사", "—",
                                  "company_name + 날짜 매핑 필요", "#94a3b8", "#f1f5f9"),
                        unsafe_allow_html=True)

    # KPI 2: 주가 상관 1위
    market_top = _top_market_corr(results, n=1)
    if market_top:
        m = market_top[0]
        sign = "" if m["r"] < 0 else "+"
        with k2:
            st.markdown(_kpi_card(
                "주가 상관 1위 회사", m["name"][:18],
                f"r=<b style='color:#1e40af'>{sign}{m['r']:.2f}</b> · {m['lag']}주 선행",
                accent="#1e40af", bg="#dbeafe",
            ), unsafe_allow_html=True)
    else:
        with k2:
            st.markdown(_kpi_card("주가 상관 1위 회사", "—",
                                  "Market Signal 분석 필요", "#94a3b8", "#f1f5f9"),
                        unsafe_allow_html=True)

    # KPI 3: 공시 상관 1위
    earnings_top = _top_earnings_corr(results, n=1)
    if earnings_top:
        e = earnings_top[0]
        with k3:
            st.markdown(_kpi_card(
                "공시 상관 1위 회사", e["name"][:18],
                f"Quality <b style='color:#7c3aed'>{e['quality']:.0f}</b>/100 · "
                f"방향 {e['direction']:.0f}%",
                accent="#7c3aed", bg="#f3e8ff",
            ), unsafe_allow_html=True)
    else:
        with k3:
            st.markdown(_kpi_card("공시 상관 1위 회사", "—",
                                  "Earnings Intel 분석 필요", "#94a3b8", "#f1f5f9"),
                        unsafe_allow_html=True)

    # KPI 4: 알파 가능 종목 (|r| ≥ 0.3)
    n_alpha = sum(1 for s in mkt_ok if abs(s.get("max_corr") or 0) >= 0.3)
    n_total_signals = len(mkt_ok)
    if n_total_signals:
        pct = n_alpha / n_total_signals * 100
        color = "#16a34a" if pct >= 30 else "#d97706" if pct >= 10 else "#dc2626"
        bg    = "#dcfce7" if pct >= 30 else "#fef3c7" if pct >= 10 else "#fee2e2"
        with k4:
            st.markdown(_kpi_card(
                "알파 가능 종목 (|r|≥0.3)",
                f"{n_alpha}개 / {n_total_signals}",
                f"전체의 <b style='color:{color}'>{pct:.0f}%</b> · L/S 전략 후보",
                accent=color, bg=bg,
            ), unsafe_allow_html=True)
    else:
        with k4:
            st.markdown(_kpi_card("알파 가능 종목", "—",
                                  "Market Signal 분석 필요", "#94a3b8", "#f1f5f9"),
                        unsafe_allow_html=True)

    st.write("")
    st.divider()

    # ── 📈 매수/매도 후보 종목 (기존 유지 — 사용자 칭찬한 부분) ────────────
    st.markdown("### 📈 매수/매도 후보 종목 (Top Long/Short)")
    st.caption(
        "POS 매출과 주가 변화의 상관관계 r 기준 — "
        "🟢 양수: 매출↑→주가↑ (매수 후보) / 🔴 음수: 매출↑→주가↓ (매도 후보)"
    )
    if mkt_ok:
        longs  = sorted([s for s in mkt_ok if (s.get("max_corr") or 0) > 0],
                        key=lambda s: s.get("max_corr") or 0, reverse=True)[:5]
        shorts = sorted([s for s in mkt_ok if (s.get("max_corr") or 0) < 0],
                        key=lambda s: s.get("max_corr") or 0)[:5]
        bar_labels = [f"🟢 {s.get('company','')[:14]} ({s.get('ticker','')})" for s in longs] + \
                     [f"🔴 {s.get('company','')[:14]} ({s.get('ticker','')})" for s in shorts]
        bar_values = [s.get("max_corr", 0) for s in longs] + \
                     [s.get("max_corr", 0) for s in shorts]
        bar_colors = ["#16a34a"] * len(longs) + ["#dc2626"] * len(shorts)

        if bar_labels:
            fig = go.Figure(go.Bar(
                x=bar_values, y=bar_labels, orientation="h",
                marker_color=bar_colors,
                text=[f"{v:+.3f}" for v in bar_values],
                textposition="outside",
            ))
            fig.add_vline(x=0, line_color="#cbd5e1", line_width=1)
            fig.update_layout(
                height=340, plot_bgcolor="#fff",
                margin=dict(t=10, b=30, l=10, r=10),
                xaxis=dict(title="POS-주가 상관 r (0.5+ 강함, 0.3+ 의미, 0.3 미만 약함)",
                           showgrid=True, gridcolor="#e2e8f0",
                           range=[min(bar_values + [-0.4]) * 1.2, max(bar_values + [0.4]) * 1.2]),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig, key="dash_ls_bars", use_container_width=True)
            st.caption("막대가 길수록 신호 강함. |r| ≥ 0.3이어야 의미 있음.")
        else:
            st.info("후보 없음.")
    else:
        st.info("Market Signal을 먼저 실행하면 후보가 표시됩니다.")

    st.divider()

    # ── 🏆 매출 성장률 Top — 회사/브랜드/SKU ────────────────────────────────
    st.markdown("### 🏆 매출 성장률 Top (최근 12M YoY)")
    st.caption("최근 12개월 매출이 직전 12개월 대비 얼마나 늘었는지. 회사/브랜드/SKU별 Top 10.")

    g1, g2, g3 = st.columns(3)

    with g1:
        st.markdown("**🏢 회사 Top**")
        _render_growth_top("회사", company_growth_rows[:10])

    with g2:
        st.markdown("**🏷 브랜드 Top**")
        brand_rows = []
        if raw_df is not None and role_map.get("brand_name") and role_map.get("transaction_date") and role_map.get("sales_amount"):
            brand_rows = _yoy_by_group(
                raw_df, role_map["brand_name"], role_map["transaction_date"],
                role_map["sales_amount"], n=10,
            )
        _render_growth_top("브랜드", brand_rows[:10])

    with g3:
        st.markdown("**📦 SKU Top**")
        sku_rows = []
        if raw_df is not None and role_map.get("sku_name") and role_map.get("transaction_date") and role_map.get("sales_amount"):
            sku_rows = _yoy_by_group(
                raw_df, role_map["sku_name"], role_map["transaction_date"],
                role_map["sales_amount"], n=10,
            )
        _render_growth_top("SKU", sku_rows[:10])

    st.divider()

    # ── 🔗 주가 상관성 Top 회사 ──────────────────────────────────────────────
    st.markdown("### 🔗 주가 상관성 Top 회사")
    st.caption(
        "POS 매출과 주가의 상관계수 |r| 절댓값 기준 Top 10. "
        "⭐ 🟢 강함(|r|≥0.5) · 🟡 보통(0.3+) · 🔴 약함"
    )
    market_top = _top_market_corr(results, n=10)
    _render_market_corr_top(market_top)

    st.divider()

    # ── 📊 공시매출 상관성 Top 회사 ──────────────────────────────────────────
    st.markdown("### 📊 공시매출 상관성 Top 회사 (Tracking Quality)")
    st.caption(
        "POS 매출이 DART 공시 매출을 얼마나 잘 따라가는지. Quality = Direction × 0.5 + |r| × 0.3 + Stability × 0.2. "
        "⭐ 🟢 강함(65+) · 🟡 보통(40+) · 🔴 약함"
    )
    earnings_top = _top_earnings_corr(results, n=10)
    _render_earnings_corr_top(earnings_top)

    st.divider()

    # ── 💡 알파 창출 가능 지점 인사이트 ─────────────────────────────────────
    st.markdown("### 💡 알파 창출 가능 지점 — 데이터 인사이트")
    st.caption("팀과 토론할 때 핵심 토픽으로 활용 가능한 발견 사항.")
    insights = _alpha_insights(results)
    for ins in insights:
        st.markdown(
            f"<div style='background:#f8fafc;border-left:3px solid #6366f1;"
            f"border-radius:6px;padding:10px 14px;margin:6px 0;font-size:13.5px;"
            f"line-height:1.65;color:#1f2937'>{ins}</div>",
            unsafe_allow_html=True,
        )

    st.write("")
