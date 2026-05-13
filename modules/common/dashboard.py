"""
Signal Dashboard — Growth · Demand · Anomaly 결과 통합 + Alpha Score
"""
import io
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

# ── 색상 ─────────────────────────────────────────────────────────────────────
C_BLUE   = "#1e40af"
C_GREEN  = "#16a34a"
C_RED    = "#dc2626"
C_AMBER  = "#d97706"
C_GRAY   = "#6b7280"
C_TEAL   = "#0d9488"
C_PURPLE = "#7c3aed"

ALPHA_BANDS = [
    (75, 101, "🚀 Strong Buy",  C_TEAL,   "#ccfbf1"),
    (60,  75, "🟢 Buy Signal",  C_GREEN,  "#dcfce7"),
    (45,  60, "⚪ Neutral",     C_GRAY,   "#f3f4f6"),
    (30,  45, "🟡 Caution",    C_AMBER,  "#fef9c3"),
    ( 0,  30, "🔴 Risk Signal", C_RED,    "#fee2e2"),
]


def _alpha_band(score: float) -> tuple[str, str, str]:
    """(label, text_color, bg_color)"""
    for lo, hi, label, tc, bg in ALPHA_BANDS:
        if lo <= score < hi:
            return label, tc, bg
    return "🚀 Strong Buy", C_TEAL, "#ccfbf1"


# ══════════════════════════════════════════════════════════════════════════════
# Alpha Score — rule-based
# ══════════════════════════════════════════════════════════════════════════════

def _alpha_score(
    mom: float | None       = None,   # latest MoM %
    demand_score: float | None = None, # 0-100
    n_crit: int = 0,
    n_high: int = 0,
    n_med:  int = 0,
    n_avail: int = 3,                  # 몇 개 분석이 있는지
) -> float:
    """
    Rule-based Alpha Score (0 – 100).

    Growth  component  (0-40): MoM 기반, -30%→0pt  +30%→40pt
    Demand  component  (0-35): demand_score / 100 × 35
    Safety  component  (0-25): Critical -10, High -5, Medium -2 차감

    분석이 없는 축은 중립값(절반)으로 대체하되, 신뢰도에 따라 가중치 축소.
    """
    # Growth (0-40)
    if mom is not None and not np.isnan(float(mom)):
        g = max(0.0, min(40.0, (float(mom) + 30.0) / 60.0 * 40.0))
    else:
        g = 20.0  # neutral

    # Demand (0-35)
    if demand_score is not None and not np.isnan(float(demand_score)):
        d = float(demand_score) * 35.0 / 100.0
    else:
        d = 17.5  # neutral

    # Safety (0-25)
    deduction = min(25, n_crit * 10 + n_high * 5 + n_med * 2)
    s = 25.0 - deduction

    raw = g + d + s  # 0-100

    # 분석 수가 적을수록 신뢰도 조정 (중립 50점 쪽으로 당김)
    confidence = {1: 0.6, 2: 0.8, 3: 1.0}.get(n_avail, 1.0)
    adjusted = 50.0 + (raw - 50.0) * confidence

    return round(max(0.0, min(100.0, adjusted)), 1)


# ══════════════════════════════════════════════════════════════════════════════
# 회사별 신호 추출
# ══════════════════════════════════════════════════════════════════════════════

def _extract_signals(g_res, d_res, a_res) -> pd.DataFrame:
    """
    Growth / Demand / Anomaly 결과에서 회사별 최신 신호를 추출해
    하나의 DataFrame으로 합칩니다.
    """
    companies: set = set()
    for res, key in [(g_res, "monthly"), (d_res, "agg_df"), (a_res, "agg_df")]:
        if res:
            nc = res.get("name_col", "")
            df = res.get(key) if key == "monthly" else res.get("agg_df")
            if df is not None and nc in df.columns:
                companies.update(df[nc].unique())

    if not companies:
        return pd.DataFrame()

    event_df = a_res.get("event_df", pd.DataFrame()) if a_res else pd.DataFrame()
    n_avail  = sum(1 for r in [g_res, d_res, a_res] if r)

    rows = []
    for co in sorted(companies):
        row: dict = {"회사": co}

        # ── Growth ────────────────────────────────────────────────────────
        if g_res:
            monthly = g_res.get("monthly")
            nc, dc  = g_res["name_col"], g_res["date_col"]
            if monthly is not None and nc in monthly.columns:
                sub = monthly[monthly[nc] == co].sort_values(dc)
                if not sub.empty:
                    last = sub.iloc[-1]
                    mom  = last.get("MoM", np.nan)
                    accel = last.get("MoM_accel", np.nan)
                    row["MoM (%)"]      = round(mom,   1) if pd.notna(mom)   else None
                    row["가속도 (pp)"]  = round(accel, 1) if pd.notna(accel) else None
                    row["Growth 추세"]  = (
                        "🚀 가속 성장" if pd.notna(accel) and accel > 2  else
                        "📈 성장"      if pd.notna(mom)   and mom  > 5   else
                        "📉 둔화"      if pd.notna(accel) and accel < -2 else
                        "➡ 횡보"
                    )

        # ── Demand ────────────────────────────────────────────────────────
        if d_res:
            agg = d_res.get("agg_df")
            nc, dc = d_res["name_col"], d_res["date_col"]
            if agg is not None and nc in agg.columns:
                sub = agg[agg[nc] == co].sort_values(dc)
                if not sub.empty:
                    last = sub.iloc[-1]
                    row["Demand Driver"] = last.get("demand_driver", "—")
                    ds = last.get("demand_score", np.nan)
                    row["Demand Score"]  = round(ds, 1) if pd.notna(ds) else None

        # ── Anomaly ───────────────────────────────────────────────────────
        n_crit = n_high = n_med = 0
        if a_res:
            if not event_df.empty and "company" in event_df.columns:
                co_ev  = event_df[event_df["company"] == co]
                n_crit = int((co_ev["severity"] == "🔴 CRITICAL").sum())
                n_high = int((co_ev["severity"] == "🟠 HIGH").sum())
                n_med  = int((co_ev["severity"] == "🟡 MEDIUM").sum())
                row["이상 이벤트"]   = len(co_ev)
                row["CRITICAL"]      = n_crit
            else:
                agg = a_res.get("agg_df")
                nc  = a_res["name_col"]
                if agg is not None and nc in agg.columns:
                    co_df = agg[agg[nc] == co]
                    row["이상 이벤트"] = int(co_df["is_anomaly"].sum()) if "is_anomaly" in co_df.columns else 0
                    row["CRITICAL"]    = 0

        # ── Alpha Score ───────────────────────────────────────────────────
        row["Alpha Score"] = _alpha_score(
            mom          = row.get("MoM (%)"),
            demand_score = row.get("Demand Score"),
            n_crit       = n_crit,
            n_high       = n_high,
            n_med        = n_med,
            n_avail      = n_avail,
        )
        rows.append(row)

    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# UI 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _alpha_gauge_html(score: float) -> str:
    label, tc, bg = _alpha_band(score)
    pct = int(score)
    return (
        f"<div style='background:{bg};border-radius:10px;padding:14px 18px;"
        f"text-align:center;height:100%'>"
        f"<div style='font-size:11px;color:{C_GRAY};font-weight:600;letter-spacing:.05em'>"
        f"ALPHA SCORE</div>"
        f"<div style='font-size:42px;font-weight:900;color:{tc};line-height:1.1'>{score:.0f}</div>"
        f"<div style='font-size:13px;font-weight:700;color:{tc};margin-bottom:8px'>{label}</div>"
        f"<div style='background:#e5e7eb;border-radius:4px;height:8px'>"
        f"<div style='background:{tc};width:{pct}%;height:100%;border-radius:4px'></div>"
        f"</div></div>"
    )


def _signal_card_html(icon: str, title: str, value: str,
                      sub: str = "", color: str = C_BLUE, bg: str = "#eff6ff") -> str:
    return (
        f"<div style='background:{bg};border-radius:10px;padding:14px 18px;"
        f"border-top:3px solid {color};height:100%'>"
        f"<div style='font-size:11px;color:{C_GRAY};font-weight:600;letter-spacing:.05em'>"
        f"{icon} {title}</div>"
        f"<div style='font-size:20px;font-weight:800;color:{color};"
        f"margin:6px 0 2px;line-height:1.2'>{value}</div>"
        f"<div style='font-size:11px;color:{C_GRAY}'>{sub}</div>"
        f"</div>"
    )


def _composite_text(g_res, d_res, a_res, alpha: float) -> list[str]:
    """각 신호에 대한 자연어 해석 문장 리스트."""
    lines = []

    if g_res:
        monthly = g_res.get("monthly")
        nc, dc, sc = g_res["name_col"], g_res["date_col"], g_res["sales_col"]
        if monthly is not None:
            last_by_co = monthly.sort_values(dc).groupby(nc).last()
            top_mom = last_by_co["MoM"].max() if "MoM" in last_by_co else np.nan
            if pd.notna(top_mom):
                trend = "가속 중" if (last_by_co.get("MoM_accel", pd.Series([np.nan])).max() or 0) > 0 else "둔화 중"
                lines.append(
                    f"**📈 Growth** — 최고 MoM **{top_mom:+.1f}%**, 성장세 {trend}."
                )

    if d_res:
        agg = d_res.get("agg_df")
        nc, dc = d_res["name_col"], d_res["date_col"]
        if agg is not None and "demand_score" in agg.columns:
            latest = agg.sort_values(dc).groupby(nc).last()
            top_score = latest["demand_score"].max()
            top_driver = latest.loc[latest["demand_score"].idxmax(), "demand_driver"] if "demand_driver" in latest.columns else "—"
            lines.append(
                f"**🔥 Demand** — 최고 Demand Score **{top_score:.0f}/100**, "
                f"주요 동인: {top_driver}."
            )

    if a_res:
        n = a_res.get("n_anomaly", 0)
        ev = a_res.get("event_df", pd.DataFrame())
        n_crit = int((ev["severity"] == "🔴 CRITICAL").sum()) if not ev.empty else 0
        if n == 0:
            lines.append("**🚨 Anomaly** — 이상 이벤트 없음. 데이터 패턴 안정적.")
        else:
            crit_s = f" (CRITICAL {n_crit}건)" if n_crit else ""
            lines.append(
                f"**🚨 Anomaly** — {n}건 이상 이벤트 감지{crit_s}. 집중 모니터링 권장."
            )

    # Alpha 해석
    label, tc, _ = _alpha_band(alpha)
    interp = {
        "🚀 Strong Buy":  "강한 매수 신호. 성장·수요 모두 강세, 리스크 낮음.",
        "🟢 Buy Signal":  "매수 관심 구간. 모멘텀 양호, 추가 확인 후 진입.",
        "⚪ Neutral":     "중립 구간. 방향성 불분명, 관망 유지.",
        "🟡 Caution":    "주의 구간. 일부 지표 약화, 리스크 관리 필요.",
        "🔴 Risk Signal": "위험 신호. 성장 둔화 또는 이상 이벤트 다수 감지.",
    }.get(label, "")
    lines.append(f"**🎯 Alpha {alpha:.0f}** — {label}. {interp}")

    return lines


# ══════════════════════════════════════════════════════════════════════════════
# 메인 렌더러
# ══════════════════════════════════════════════════════════════════════════════

def render_signal_dashboard():
    results    = st.session_state.get("results", {})
    role_map   = st.session_state.get("role_map", {})
    qual_score = st.session_state.get("quality_score", "—")

    g_res = results.get("growth")
    d_res = results.get("demand")
    a_res = results.get("anomaly")

    st.subheader("📡 Signal Dashboard")

    # ── 실행 현황 배지 ────────────────────────────────────────────────────────
    _GRADE_COLOR = {"A": C_GREEN, "B": C_AMBER, "C": "#f97316", "D": C_RED}
    _GRADE_BG    = {"A": "#dcfce7", "B": "#fef3c7", "C": "#fff7ed", "D": "#fee2e2"}
    _GRADE_WARN  = {"C": "참고용", "D": "해석 주의"}

    badge_html = ""
    conf_html  = ""
    for key, icon, label in [("growth","📈","Growth"),
                               ("demand","🔥","Demand"),
                               ("anomaly","🚨","Anomaly")]:
        done = key in results
        c = C_GREEN if done else "#9ca3af"
        bg = "#dcfce7" if done else "#f3f4f6"
        mark = "✓" if done else "—"
        badge_html += (
            f"<span style='background:{bg};color:{c};border:1px solid {c}44;"
            f"border-radius:20px;padding:3px 12px;font-size:12px;"
            f"font-weight:600;margin-right:6px'>{icon} {label} {mark}</span>"
        )
        if done:
            res = results[key]
            grade = res.get("confidence", {}).get("grade", "?")
            gc    = _GRADE_COLOR.get(grade, C_GRAY)
            gbg   = _GRADE_BG.get(grade, "#f3f4f6")
            warn  = _GRADE_WARN.get(grade, "")
            warn_span = (
                f"<span style='color:{C_RED};font-size:10px;margin-left:4px'>⚠ {warn}</span>"
                if warn else ""
            )
            conf_html += (
                f"<span style='background:{gbg};color:{gc};border:1px solid {gc}44;"
                f"border-radius:20px;padding:3px 10px;font-size:11px;"
                f"font-weight:600;margin-right:6px'>{icon} {grade}{warn_span}</span>"
            )

    qs_color = C_GREEN if isinstance(qual_score, int) and qual_score >= 70 else C_AMBER
    st.markdown(
        f"<div style='display:flex;align-items:center;gap:16px;margin-bottom:8px'>"
        f"{badge_html}"
        f"<span style='font-size:12px;color:{C_GRAY}'>Data Quality: "
        f"<b style='color:{qs_color}'>{qual_score}/100</b></span>"
        f"</div>",
        unsafe_allow_html=True,
    )
    if conf_html:
        st.markdown(
            f"<div style='margin-bottom:14px'>"
            f"<span style='font-size:11px;color:{C_GRAY};margin-right:8px'>신뢰도:</span>"
            f"{conf_html}"
            f"</div>",
            unsafe_allow_html=True,
        )

    if not results:
        st.info("Step 5에서 분석을 실행하면 결과가 여기에 표시됩니다.")
        return

    # ── 전체 Alpha Score 계산 ─────────────────────────────────────────────────
    # 전체 평균 지표 기준
    mom_all = demand_score_all = None
    n_crit_all = n_high_all = n_med_all = 0

    if g_res:
        monthly = g_res.get("monthly")
        dc = g_res["date_col"]
        nc = g_res["name_col"]
        if monthly is not None and "MoM" in monthly.columns:
            latest_mom = monthly.sort_values(dc).groupby(nc)["MoM"].last()
            mom_all = float(latest_mom.mean())

    if d_res:
        agg = d_res.get("agg_df")
        dc  = d_res["date_col"]
        nc  = d_res["name_col"]
        if agg is not None and "demand_score" in agg.columns:
            latest_ds = agg.sort_values(dc).groupby(nc)["demand_score"].last()
            demand_score_all = float(latest_ds.mean())

    if a_res:
        ev = a_res.get("event_df", pd.DataFrame())
        if not ev.empty:
            n_crit_all = int((ev["severity"] == "🔴 CRITICAL").sum())
            n_high_all = int((ev["severity"] == "🟠 HIGH").sum())
            n_med_all  = int((ev["severity"] == "🟡 MEDIUM").sum())

    n_avail  = sum(1 for r in [g_res, d_res, a_res] if r)
    alpha    = _alpha_score(mom_all, demand_score_all, n_crit_all, n_high_all, n_med_all, n_avail)
    al_label, al_tc, al_bg = _alpha_band(alpha)

    # ── 4 Signal Cards ───────────────────────────────────────────────────────
    col_a, col_g, col_d, col_an = st.columns(4)

    with col_a:
        st.markdown(_alpha_gauge_html(alpha), unsafe_allow_html=True)

    with col_g:
        if g_res:
            monthly = g_res.get("monthly")
            dc, nc  = g_res["date_col"], g_res["name_col"]
            if monthly is not None and "MoM" in monthly.columns:
                latest = monthly.sort_values(dc).groupby(nc).last()
                best_co  = latest["MoM"].idxmax()
                best_mom = latest.loc[best_co, "MoM"]
                accel    = latest.loc[best_co].get("MoM_accel", np.nan)
                sub_txt  = f"{best_co} | 가속 {accel:+.1f}pp" if pd.notna(accel) else str(best_co)
                mom_color = C_GREEN if best_mom >= 0 else C_RED
                st.markdown(
                    _signal_card_html("📈", "TOP GROWTH SIGNAL",
                                      f"MoM {best_mom:+.1f}%",
                                      sub_txt, mom_color, "#eff6ff"),
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                _signal_card_html("📈", "TOP GROWTH SIGNAL", "—", "미실행", C_GRAY, "#f9fafb"),
                unsafe_allow_html=True,
            )

    with col_d:
        if d_res:
            agg = d_res.get("agg_df")
            dc, nc = d_res["date_col"], d_res["name_col"]
            if agg is not None and "demand_score" in agg.columns:
                latest = agg.sort_values(dc).groupby(nc).last()
                best_co = latest["demand_score"].idxmax()
                best_ds = latest.loc[best_co, "demand_score"]
                driver  = latest.loc[best_co].get("demand_driver", "—")
                ds_color = C_TEAL if best_ds >= 60 else (C_AMBER if best_ds >= 45 else C_RED)
                st.markdown(
                    _signal_card_html("🔥", "TOP DEMAND SIGNAL",
                                      f"Score {best_ds:.0f}/100",
                                      f"{best_co} | {driver}", ds_color, "#fff7ed"),
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                _signal_card_html("🔥", "TOP DEMAND SIGNAL", "—", "미실행", C_GRAY, "#f9fafb"),
                unsafe_allow_html=True,
            )

    with col_an:
        if a_res:
            n = a_res.get("n_anomaly", 0)
            ev = a_res.get("event_df", pd.DataFrame())
            nc = int((ev["severity"] == "🔴 CRITICAL").sum()) if not ev.empty else 0
            an_color = C_RED if nc > 0 else (C_AMBER if n > 0 else C_GREEN)
            an_bg    = "#fee2e2" if nc > 0 else ("#fef9c3" if n > 0 else "#dcfce7")
            an_val   = f"{n}건 이상 감지" if n > 0 else "이상 없음"
            an_sub   = f"CRITICAL {nc}건 포함" if nc > 0 else ("모니터링 권장" if n > 0 else "데이터 안정적")
            st.markdown(
                _signal_card_html("🚨", "ANOMALY ALERT", an_val, an_sub, an_color, an_bg),
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                _signal_card_html("🚨", "ANOMALY ALERT", "—", "미실행", C_GRAY, "#f9fafb"),
                unsafe_allow_html=True,
            )

    st.write("")

    # ── Composite Signal Summary ──────────────────────────────────────────────
    st.markdown("#### 📋 Composite Signal Summary")

    lines = _composite_text(g_res, d_res, a_res, alpha)
    summary_html = "".join(
        f"<div style='padding:6px 0;border-bottom:1px solid #f3f4f6;font-size:13px'>"
        f"{line}</div>"
        for line in lines
    )
    st.markdown(
        f"<div style='background:{al_bg};border:1px solid {al_tc}44;"
        f"border-radius:10px;padding:14px 18px;margin-bottom:16px'>"
        f"{summary_html}"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── 회사별 Signal Table ───────────────────────────────────────────────────
    sig_df = _extract_signals(g_res, d_res, a_res)

    if not sig_df.empty and len(sig_df) > 0:
        st.markdown("#### 🏢 회사별 Composite Signal")

        # Alpha Score 색상 수평 막대 차트
        if "Alpha Score" in sig_df.columns and len(sig_df) > 1:
            sorted_df = sig_df.sort_values("Alpha Score", ascending=True)
            colors = [_alpha_band(s)[1] for s in sorted_df["Alpha Score"]]
            fig = go.Figure(go.Bar(
                x=sorted_df["Alpha Score"],
                y=sorted_df["회사"],
                orientation="h",
                marker_color=colors,
                text=sorted_df["Alpha Score"].apply(lambda s: f"{s:.0f}"),
                textposition="outside",
            ))
            fig.update_layout(
                title="회사별 Alpha Score",
                xaxis=dict(title="Score (0–100)", range=[0, 105]),
                margin=dict(t=40, b=0),
            )
            st.plotly_chart(fig, key="dashboard_1")

        # Spider / Radar 차트 (회사별 3축 비교) — 데이터가 충분한 경우
        has_3 = all(col in sig_df.columns for col in ["MoM (%)", "Demand Score", "Alpha Score"])
        if has_3 and len(sig_df) >= 2:
            fig_r = go.Figure()
            categories = ["성장 (MoM)", "수요 Score", "Alpha Score"]
            for _, row in sig_df.iterrows():
                mom_n   = max(0, min(100, (row.get("MoM (%)", 0) or 0 + 30) / 60 * 100))
                d_score = row.get("Demand Score", 50) or 50
                a_score = row.get("Alpha Score",  50) or 50
                fig_r.add_scatterpolar(
                    r=[mom_n, d_score, a_score, mom_n],
                    theta=categories + [categories[0]],
                    fill="toself", name=str(row["회사"]), opacity=0.6,
                )
            fig_r.update_layout(
                polar=dict(radialaxis=dict(range=[0, 100], visible=True)),
                title="회사별 신호 Radar",
                legend=dict(orientation="h"),
                margin=dict(t=50, b=0),
            )
            st.plotly_chart(fig_r, key="dashboard_2")

        # 요약 테이블
        disp_cols = [c for c in [
            "회사", "MoM (%)", "가속도 (pp)", "Growth 추세",
            "Demand Driver", "Demand Score",
            "이상 이벤트", "CRITICAL", "Alpha Score",
        ] if c in sig_df.columns]
        disp = sig_df[disp_cols].copy()
        st.dataframe(
            disp.sort_values("Alpha Score", ascending=False), hide_index=True,
        )

    # ── 다운로드 ──────────────────────────────────────────────────────────────
    st.divider()
    st.markdown("#### 📥 결과 다운로드")

    dl_frames: dict[str, pd.DataFrame] = {}
    if g_res and g_res.get("monthly") is not None:
        dl_frames["growth_monthly"] = g_res["monthly"]
    if d_res and d_res.get("agg_df") is not None:
        dl_frames["demand"] = d_res["agg_df"]
    if a_res:
        if a_res.get("event_df") is not None and not a_res["event_df"].empty:
            ev = a_res["event_df"].drop(columns=["_tc", "_bg"], errors="ignore")
            dl_frames["anomaly_events"] = ev
        if a_res.get("agg_df") is not None:
            dl_frames["anomaly_raw"] = a_res["agg_df"]
    if not sig_df.empty:
        dl_frames["composite_signal"] = sig_df

    c1, c2 = st.columns(2)

    # Excel (멀티시트)
    with c1:
        if dl_frames:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                for sheet, frame in dl_frames.items():
                    frame.to_excel(w, index=False, sheet_name=sheet[:31])
            st.download_button(
                "📥 전체 결과 Excel",
                data=buf.getvalue(),
                file_name="signal_dashboard.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    # Composite CSV
    with c2:
        if not sig_df.empty:
            csv = sig_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "📥 Composite Signal CSV",
                data=csv,
                file_name="composite_signal.csv",
                mime="text/csv",
            )
