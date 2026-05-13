"""Alpha Validation — composite alpha score from all available analyses"""
import math
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from modules.analysis.guides import render_guide
from modules.common.core.audit import compute_module_audit, check_sample_size_sanity
from modules.common.core.result import enrich_result



# ── Score helpers ──────────────────────────────────────────────────────────────

def _norm(val: float | None, lo: float = -30, hi: float = 30) -> float:
    """Normalize val to 0-100 range, clamped."""
    if val is None or not math.isfinite(val):
        return 50.0
    return max(0.0, min(100.0, (val - lo) / (hi - lo) * 100))


def _get_metric(result: dict | None, *keys: str, default=None):
    """Safely read nested metrics from a result dict."""
    if result is None:
        return default
    m = result.get("metrics", {})
    for k in keys:
        if k in m:
            return m[k]
    return default


def _read_growth(g_res: dict | None) -> tuple[float | None, float | None]:
    """Extract latest MoM and YoY from growth result (legacy or standard format)."""
    if g_res is None:
        return None, None
    # Standard format
    m = g_res.get("metrics", {})
    if m.get("latest_mom_pct") is not None:
        return m.get("latest_mom_pct"), m.get("latest_yoy_pct")
    # Legacy format: monthly DataFrame
    monthly = g_res.get("monthly")
    if monthly is None or monthly.empty:
        return None, None
    mom = yoy = None
    for col in ["MoM", "mom_pct"]:
        if col in monthly.columns:
            s = monthly[col].dropna()
            if not s.empty:
                mom = float(s.iloc[-1])
    for col in ["YoY", "yoy_pct"]:
        if col in monthly.columns:
            s = monthly[col].dropna()
            if not s.empty:
                yoy = float(s.iloc[-1])
    return mom, yoy


def _read_demand(d_res: dict | None) -> float | None:
    """Extract average demand score from demand result (legacy or standard)."""
    if d_res is None:
        return None
    m = d_res.get("metrics", {})
    if m.get("avg_demand_score") is not None:
        return m["avg_demand_score"]
    if m.get("demand_score") is not None:
        return m["demand_score"]
    # Legacy: agg_df has demand_score column
    agg = d_res.get("agg_df")
    if agg is not None and "demand_score" in agg.columns:
        s = agg["demand_score"].dropna()
        if not s.empty:
            return float(s.mean())
    return None


def _read_anomaly(a_res: dict | None) -> tuple[int | None, int]:
    """Extract (n_anomaly, n_periods) from anomaly result (legacy or standard)."""
    if a_res is None:
        return None, 12
    # Both legacy and (future) standard have n_anomaly at top level or in metrics
    n_anom = a_res.get("n_anomaly") or a_res.get("metrics", {}).get("n_anomaly")
    agg    = a_res.get("agg_df")
    n_per  = int(len(agg)) if agg is not None else 12
    return n_anom, max(1, n_per)


def _compute_alpha(all_results: dict) -> tuple[float, dict]:
    """
    Rule-based Alpha Score (0-100).

    Sources:
        Growth   → growth_score  (0-40 pts)
        Demand   → demand_score  (0-35 pts)
        Anomaly  → safety_score  (0-25 pts)
        Market   → bonus         (+10 if strong positive lag corr)
    Confidence multiplier based on how many core analyses ran.
    """
    g_res = all_results.get("growth")
    d_res = all_results.get("demand")
    a_res = all_results.get("anomaly")
    m_res = all_results.get("market_signal")

    breakdown: dict = {}
    n_avail = 0

    # ── Growth score (0-40) ─────────────────────────────────────────────────
    growth_raw = 0.0
    mom, yoy = _read_growth(g_res)
    if mom is not None:
        n_avail += 1
        growth_raw = min(40.0, max(0.0, _norm(mom, lo=-20, hi=30) * 0.4))
        if yoy is not None:
            yoy_pts = min(40.0, _norm(yoy, lo=-20, hi=40) * 0.4)
            growth_raw = round((growth_raw + yoy_pts) / 2 * 2, 1)
        else:
            growth_raw = round(growth_raw, 1)
    breakdown["growth"] = growth_raw

    # ── Demand score (0-35) ─────────────────────────────────────────────────
    demand_raw = 0.0
    dscore = _read_demand(d_res)
    if dscore is not None:
        n_avail += 1
        demand_raw = round(_norm(dscore, lo=20, hi=80) * 0.35, 1)
    breakdown["demand"] = demand_raw

    # ── Safety score (0-25) — inverse anomaly ──────────────────────────────
    safety_raw = 0.0
    n_anom, n_per = _read_anomaly(a_res)
    if n_anom is not None:
        n_avail += 1
        anom_rate  = min(1.0, n_anom / n_per)
        safety_raw = round((1 - anom_rate) * 25, 1)
    breakdown["safety"] = safety_raw

    # ── Market bonus (0-10) ─────────────────────────────────────────────────
    bonus = 0.0
    best_corr = _get_metric(m_res, "best_corr")
    if best_corr is not None and best_corr > 0.5:
        bonus = round(min(10.0, (best_corr - 0.5) * 20), 1)
    breakdown["market_bonus"] = bonus

    # ── Confidence multiplier ────────────────────────────────────────────────
    conf = {0: 0.5, 1: 0.65, 2: 0.82, 3: 1.0}.get(n_avail, 1.0)

    raw_total   = growth_raw + demand_raw + safety_raw
    alpha_score = round(min(100.0, (raw_total + bonus) * conf), 1)
    breakdown["confidence_mult"] = conf
    breakdown["raw_total"]       = round(raw_total, 1)
    breakdown["final"]           = alpha_score

    return alpha_score, breakdown


def run_alpha_validation(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    all_results = params.get("all_results", {})

    alpha_score, breakdown = _compute_alpha(all_results)

    # Signals summary — handle both legacy and standard result formats
    signals = []
    LEGACY_DEFAULTS = {
        "growth":  "성장률 분석 완료",
        "demand":  "Demand 분석 완료",
        "anomaly": "이상 탐지 완료",
    }
    for key, label in [
        ("growth",        "📈 Growth"),
        ("demand",        "🔥 Demand"),
        ("anomaly",       "🚨 Anomaly"),
        ("market_signal", "📉 Market"),
        ("brand",         "🏷 Brand"),
        ("sku",           "📦 SKU"),
        ("category",      "🗂 Category"),
        ("earnings_intel","📊 Earnings"),
    ]:
        res = all_results.get(key)
        if res is not None:
            status  = res.get("status") or "success"    # legacy has no status
            message = res.get("message") or LEGACY_DEFAULTS.get(key, "")
            signals.append({
                "module":  label,
                "status":  status,
                "message": message,
            })

    interpretation = _interpret(alpha_score, breakdown)

    metrics = {
        "alpha_score": alpha_score,
        "growth_pts":  breakdown.get("growth", 0),
        "demand_pts":  breakdown.get("demand", 0),
        "safety_pts":  breakdown.get("safety", 0),
        "bonus_pts":   breakdown.get("market_bonus", 0),
        "conf_mult":   breakdown.get("confidence_mult", 1.0),
        "n_modules":   len(signals),
    }

    n_core = sum(1 for k in ["growth", "demand", "anomaly"] if k in all_results)
    if n_core == 0:
        status  = "warning"
        message = "핵심 분석(Growth/Demand/Anomaly) 없음 — 점수 신뢰도 낮음"
    else:
        status  = "success"
        message = f"Alpha Score: {alpha_score:.0f}/100 (신뢰도 {breakdown.get('confidence_mult', 1)*100:.0f}%)"

    n_modules = len(signals)
    bs = check_sample_size_sanity(n_modules, min_required=3)

    audit, conf = compute_module_audit(
        n_original=n_modules,
        n_valid=n_modules,
        role_map={},
        used_roles=[],
        formula="Alpha = Growth(30) + Demand(25) + Safety(25) + MarketBonus(20) × conf_mult",
        agg_unit="모듈",
        n_computable=n_modules,
        n_periods=n_modules,
        business_checks=bs,
    )

    result = {
        "status":          status,
        "message":         message,
        "data":            pd.DataFrame([breakdown]),
        "metrics":         metrics,
        "_signals":        signals,
        "_breakdown":      breakdown,
        "_interpretation": interpretation,
    }
    return enrich_result(result, audit, conf)


def _interpret(score: float, bd: dict) -> list[str]:
    lines = []
    if score >= 75:
        lines.append("🟢 **강한 알파 신호**: 소비 선행 데이터가 긍정적 방향성 제시")
    elif score >= 55:
        lines.append("🟡 **중립 신호**: 일부 긍정적 지표 존재, 추가 검증 권장")
    else:
        lines.append("🔴 **약한 신호**: 소비 지표가 부진 또는 데이터 불충분")

    if bd.get("growth", 0) >= 25:
        lines.append("• 성장률 양호 (매출 모멘텀 확인됨)")
    if bd.get("demand", 0) >= 20:
        lines.append("• Demand 신호 긍정적 (거래 건수·객단가 상승 기여)")
    if bd.get("safety", 0) >= 18:
        lines.append("• 이상 신호 낮음 (안정적 소비 패턴)")
    if bd.get("market_bonus", 0) > 0:
        lines.append("• 주가-매출 선행 상관 확인 (Market Signal 보너스)")
    if bd.get("confidence_mult", 1) < 0.8:
        lines.append("⚠️ 분석 모듈이 적어 신뢰도 조정 적용됨")
    return lines


def _render(result: dict):
    render_guide("alpha_validation")
    if result["status"] == "failed":
        st.error(result["message"])
        return

    if result["status"] == "warning":
        st.warning(result["message"])

    m  = result["metrics"]
    bd = result.get("_breakdown", {})

    alpha = m.get("alpha_score", 0)
    if alpha >= 75:
        color, label = "#16a34a", "강한 신호"
    elif alpha >= 55:
        color, label = "#d97706", "중립 신호"
    else:
        color, label = "#dc2626", "약한 신호"

    # ── 무엇을 보고 있는가 (초보자 설명) ─────────────────────────────────
    with st.expander("❓ 이 화면이 뭐예요?", expanded=(alpha == 0)):
        st.markdown(
            """
**Alpha Validation**은 다른 모든 분석 결과를 종합해서 **"이 데이터가 투자 신호로 쓸만한가"**를 한 줄 점수로 답하는 모듈입니다.

🎯 **Alpha Score = Growth(40점) + Demand(35점) + Safety(25점) + Bonus(10점) = 총 100점**

| 구성요소 | 무엇을 봄 | 어디서 가져옴 |
|---|---|---|
| **Growth** (40점) | 매출 성장률 (MoM·YoY) — 회사가 잘 크고 있는가 | Growth Analytics 모듈 |
| **Demand** (35점) | 거래량·객단가 신호 — 진짜 수요인가 | Demand Intelligence 모듈 |
| **Safety** (25점) | 이상 신호 적은가 — 안정적인가 | Anomaly Detection 모듈 |
| **Bonus** (10점) | 주가 선행성·DART 매칭 등 추가 신호 | Market Signal · Earnings Intel |

📊 **점수 해석**
- **75+ 강함** 🟢 — 모든 구성요소가 양호. 자신 있게 활용 가능
- **55~74 중립** 🟡 — 일부만 양호. 보조 신호로 사용
- **<55 약함** 🔴 — 부족한 데이터·약한 신호. 단독 사용 비권장

💡 **점수가 0이거나 너무 낮을 때**
- 핵심 분석(Growth·Demand·Anomaly)을 **실행 안 했을** 가능성 큼
- Step 4로 돌아가 **Growth Analytics, Demand Intelligence, Anomaly Detection** 체크 후 다시 실행
- 신뢰도 배수(`conf_mult`)는 활성 모듈 수에 비례 — 더 많이 실행할수록 점수가 신뢰할 수 있음
            """
        )

    # Hero gauge
    st.markdown(
        f"""<div style="text-align:center;padding:20px 0">
        <div style="font-size:72px;font-weight:900;color:{color};line-height:1">{alpha:.0f}</div>
        <div style="font-size:16px;color:{color};font-weight:600">Alpha Score / 100 · {label}</div>
        <div style="font-size:12px;color:#9ca3af;margin-top:4px">
          신뢰도 배수 {m.get('conf_mult', 1)*100:.0f}% · {m.get('n_modules', 0)}개 모듈 활성</div>
        </div>""",
        unsafe_allow_html=True,
    )

    # ── 0점 / 낮은 점수일 때 액션 가이드 ──────────────────────────────────
    n_active = m.get("n_modules", 0)
    if alpha < 30 or n_active < 3:
        missing = []
        sigs = result.get("_signals", [])
        sig_modules = {s.get("module", "") for s in sigs if s.get("status") == "success"}
        for mod_name in ("Growth Analytics", "Demand Intelligence", "Anomaly Detection"):
            if mod_name not in sig_modules:
                missing.append(mod_name)
        if missing:
            st.markdown(
                f"<div style='background:#fef3c7;border-left:4px solid #d97706;border-radius:6px;"
                f"padding:12px 16px;font-size:13px;color:#854d0e;margin:10px 0'>"
                f"<b>💡 점수를 올리려면</b> — Step 4로 돌아가 핵심 분석 추가 실행:<br>"
                f"&nbsp;&nbsp;{' · '.join('▶ ' + m for m in missing)}<br>"
                f"<span style='font-size:11px;color:#a16207'>"
                f"이 세 개가 점수의 100점 중 75점(Growth 40 + Demand 35)을 차지합니다.</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # Score breakdown bars
    st.write("")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Growth",  f"{m.get('growth_pts', 0):.1f} / 40",
              help="매출 성장률(MoM·YoY) — Growth Analytics 모듈에서 계산")
    c2.metric("Demand",  f"{m.get('demand_pts', 0):.1f} / 35",
              help="거래량·객단가 신호 — Demand Intelligence 모듈에서 계산")
    c3.metric("Safety",  f"{m.get('safety_pts', 0):.1f} / 25",
              help="이상 신호 부재 — Anomaly Detection 모듈에서 계산. 이상 적을수록 점수↑")
    c4.metric("Bonus",   f"{m.get('bonus_pts', 0):.1f} / 10",
              help="주가 선행성·DART 매칭 — Market Signal · Earnings Intel에서 가져옴")
    st.caption("각 항목은 해당 분석 모듈을 실행해야 점수가 채워집니다. "
               "전부 0이면 Growth/Demand/Anomaly를 실행해주세요.")

    tab1, tab2, tab3 = st.tabs(["📊 점수 분해", "🧩 모듈 상태", "📝 해석"])

    with tab1:
        st.caption("각 구성요소가 만점 중 몇 점을 획득했는지. 회색 = 잔여 가능 점수.")
        cats = ["Growth", "Demand", "Safety", "Bonus"]
        vals = [
            m.get("growth_pts", 0),
            m.get("demand_pts", 0),
            m.get("safety_pts", 0),
            m.get("bonus_pts", 0),
        ]
        maxs = [40, 35, 25, 10]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=cats, y=vals, name="획득 점수",
            marker_color=[color if v / mx >= 0.6 else "#94a3b8" for v, mx in zip(vals, maxs)],
        ))
        fig.add_trace(go.Bar(
            x=cats, y=[mx - v for v, mx in zip(vals, maxs)],
            name="잔여 가능", marker_color="#f1f5f9",
        ))
        fig.update_layout(
            barmode="stack", height=360, title="Alpha Score 구성요소 (만점 100)",
            yaxis_title="점수",
        )
        st.plotly_chart(fig, key="alpha_1")
        st.markdown(
            "<div style='background:#f8fafc;border-left:3px solid #94a3b8;padding:10px 14px;"
            "font-size:13px;line-height:1.7;color:#475569;border-radius:4px'>"
            "<b style='color:#0f172a'>읽는 법</b> — "
            "막대가 색깔(회색 아닌)이면 해당 구성요소가 60% 이상 채워짐. "
            "회색이면 0~60% — 그 모듈을 다시 보거나 데이터 보강이 필요합니다.<br>"
            "<b>왜 가중치가 다른가?</b> — Growth(40)·Demand(35)가 alt data alpha의 핵심 source라서 "
            "비중이 높고, Safety(25)는 위험 관리, Bonus(10)는 보조 시그널."
            "</div>",
            unsafe_allow_html=True,
        )

        # Radar chart
        sigs = result.get("_signals", [])
        if len(sigs) >= 3:
            radar_cats = ["Growth", "Demand", "Safety"] + ["Market"]
            radar_vals = [
                _norm(m.get("growth_pts", 0), lo=0, hi=40),
                _norm(m.get("demand_pts", 0), lo=0, hi=35),
                _norm(m.get("safety_pts", 0), lo=0, hi=25),
                _norm(m.get("bonus_pts", 0), lo=0, hi=10),
            ]
            radar_cats_closed = radar_cats + [radar_cats[0]]
            radar_vals_closed = radar_vals + [radar_vals[0]]
            def _hex_rgba(h: str, a: float = 0.2) -> str:
                h = h.lstrip("#")
                return f"rgba({int(h[0:2],16)},{int(h[2:4],16)},{int(h[4:6],16)},{a})"

            fig2 = go.Figure(go.Scatterpolar(
                r=radar_vals_closed, theta=radar_cats_closed,
                fill="toself", fillcolor=_hex_rgba(color, 0.2),
                line=dict(color=color),
            ))
            fig2.update_layout(
                polar=dict(radialaxis=dict(range=[0, 100])),
                height=360, title="신호 레이더",
            )
            st.plotly_chart(fig2, key="alpha_2")

    with tab2:
        st.caption("Alpha Score 계산에 참여한 분석 모듈 상태. ✅ 성공 / ⚠️ 부분 / ❌ 실패 / — 미실행.")
        sigs = result.get("_signals", [])
        if sigs:
            STATUS_ICON = {"success": "✅", "warning": "⚠️", "failed": "❌", "unknown": "—"}
            STATUS_BG   = {"success": "#f0fdf4", "warning": "#fffbeb", "failed": "#fef2f2", "unknown": "#f9fafb"}
            for sig in sigs:
                bg = STATUS_BG.get(sig["status"], "#f9fafb")
                icon = STATUS_ICON.get(sig["status"], "—")
                st.markdown(
                    f"<div style='background:{bg};border-radius:8px;padding:10px 14px;margin:4px 0;font-size:13px'>"
                    f"{icon} &nbsp; <b>{sig['module']}</b> &nbsp;·&nbsp; {sig['message']}</div>",
                    unsafe_allow_html=True,
                )
            st.markdown(
                "<div style='background:#f8fafc;border-left:3px solid #94a3b8;padding:10px 14px;"
                "font-size:12px;color:#475569;margin-top:8px;border-radius:4px'>"
                "💡 ❌·— 모듈은 Step 4로 돌아가서 체크 후 실행하면 점수 신뢰도가 올라갑니다."
                "</div>",
                unsafe_allow_html=True,
            )
        else:
            st.info("실행된 분석 모듈이 없습니다. Step 4에서 Growth/Demand/Anomaly를 추가하세요.")

    with tab3:
        st.caption("점수와 모듈 상태를 종합한 해석 — 다음 액션을 고려하는 데 활용하세요.")
        lines = result.get("_interpretation", [])
        if lines:
            for line in lines:
                st.markdown(line)
        else:
            st.info("해석 가능한 데이터 없음 — 분석 모듈을 더 실행해주세요.")
