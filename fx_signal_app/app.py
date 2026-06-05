"""
FX Signal — USD/KRW 환전 타이밍 대시보드.

질문: "오늘 USD를 KRW로 환전, 지금 해야 하나 기다려야 하나?"
답:   단기(1~2주) + 중기(1~3개월) 신호 점수 + 매크로 드라이버 breakdown
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

# 패키지 import (legacy launcher 에서도 동작)
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for p in (str(_ROOT), str(_HERE.parent)):
    if p not in sys.path:
        sys.path.insert(0, p)

import streamlit as st

try:
    st.set_page_config(page_title="FX Signal — USD/KRW", page_icon="💱", layout="wide")
except Exception:
    pass

# 패키지 상대 import (run_legacy_app으로 진입 시에도 동작)
try:
    from fx_signal_app import data as fx_data
    from fx_signal_app import signals as fx_signals
    from fx_signal_app import events as fx_events
except ImportError:
    import data as fx_data           # type: ignore
    import signals as fx_signals     # type: ignore
    import events as fx_events       # type: ignore


# ─────────────────────────────────────────────────────────────
# 헤더
# ─────────────────────────────────────────────────────────────
st.markdown(
    """
    <div style="margin: 6px 0 18px 0;">
      <div style="font-size: 1.6rem; font-weight: 700; color: #F1F5F9; letter-spacing: -0.02em;">
        💱 FX Signal — USD/KRW
      </div>
      <div style="font-size: 0.85rem; color: rgba(241,245,249,0.65); margin-top: 4px;">
        오늘 USD → KRW 환전, 지금 해야 할까 기다려야 할까? 단기(1~2주) · 중기(1~3개월) 신호로 판단.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────
# 데이터 로드
# ─────────────────────────────────────────────────────────────
ALL_KEYS = ["USDKRW", "DXY", "UST10Y", "KOSPI", "BRENT", "WTI", "CNY", "JPY"]

with st.spinner("📡 시장 데이터를 받아오는 중 (yfinance)…"):
    snaps = fx_data.fetch_snapshots(ALL_KEYS, period="1y")

usd_snap = snaps.get("USDKRW")
if usd_snap is None:
    st.error(
        "USD/KRW 시계열을 가져오지 못했어요. yfinance가 응답하지 않거나 "
        "Yahoo Finance가 클라우드 IP를 차단했을 수 있습니다."
    )
    with st.expander("🔧 디버그 정보 (어떤 ticker가 막혔는지)", expanded=False):
        st.write("각 ticker별로 빠르게 health check를 수행합니다…")
        with st.spinner("…"):
            hc = fx_data.health_check()
        st.write({k: ("✅ OK" if v else "❌ FAIL") for k, v in hc.items()})
        st.caption(
            "전부 ❌면 클라우드 IP가 Yahoo Finance에 차단된 상태. "
            "잠시(15분~1시간) 후 자동 풀리거나, requirements.txt 의 yfinance 버전을 더 최신으로 올려보세요."
        )
        if st.button("🔄 다시 시도", type="primary"):
            st.cache_data.clear()
            st.rerun()
    st.stop()


# 신호 계산
short = fx_signals.compute_short_term(snaps)
mid = fx_signals.compute_mid_term(snaps)
verdict = fx_signals.combined_verdict(short, mid)
narrative = fx_signals.market_narrative(short, mid)


# ─────────────────────────────────────────────────────────────
# 0) 최상단 — 종합 환전 판정 (가장 큰 카드)
# ─────────────────────────────────────────────────────────────
st.markdown(
    f"""
    <div style="background: linear-gradient(135deg, {verdict.color}18 0%, {verdict.color}05 100%);
                border: 1px solid {verdict.color}55; border-left: 5px solid {verdict.color};
                border-radius: 14px; padding: 22px 26px; margin: 4px 0 22px 0;">
      <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:24px;">
        <div style="flex:1; min-width:0;">
          <div style="font-size:0.72rem; color:rgba(241,245,249,0.55);
                      text-transform:uppercase; letter-spacing:0.12em; font-weight:600;">
            오늘의 환전 판정 · USD → KRW
          </div>
          <div style="font-size:2.0rem; font-weight:700; color:{verdict.color};
                      margin-top:6px; line-height:1.1; letter-spacing:-0.02em;">
            {verdict.emoji} {verdict.headline}
          </div>
          <div style="font-size:0.95rem; color:rgba(241,245,249,0.78);
                      margin-top:10px; line-height:1.5;">
            {verdict.detail}
          </div>
        </div>
        <div style="text-align:right; min-width:160px;">
          <div style="font-size:0.72rem; color:rgba(241,245,249,0.55);
                      text-transform:uppercase; letter-spacing:0.1em; font-weight:600;">
            권장 행동
          </div>
          <div style="font-size:1.05rem; font-weight:600; color:#F1F5F9;
                      margin-top:6px; line-height:1.3;">
            {verdict.action}
          </div>
          <div style="font-size:0.78rem; color:rgba(241,245,249,0.5); margin-top:14px;
                      padding-top:12px; border-top:1px solid rgba(255,255,255,0.08);">
            단기 <span style="font-family:'JetBrains Mono', monospace; color:{short.verdict_color};
                              font-weight:600;">{short.score:+.0f}</span>
            &nbsp;·&nbsp;
            중기 <span style="font-family:'JetBrains Mono', monospace; color:{mid.verdict_color};
                              font-weight:600;">{mid.score:+.0f}</span>
          </div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────
# 0.5) "지금 왜 오르고/떨어지는지" 시장 요약
# ─────────────────────────────────────────────────────────────
st.markdown(
    f"""
    <div style="background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.08);
                border-radius: 12px; padding: 16px 20px; margin: 0 0 18px 0;">
      <div style="font-size:0.72rem; color:rgba(241,245,249,0.55);
                  text-transform:uppercase; letter-spacing:0.1em; font-weight:600; margin-bottom:8px;">
        지금 왜 오르고 · 왜 떨어지는지
      </div>
      <div style="font-size:0.95rem; color:rgba(241,245,249,0.85); line-height:1.55;">
        {narrative.summary}
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# 두 컬럼: 끌어올리는 요인 / 끌어내리는 요인
def _driver_card(col, title: str, drivers: list, color: str, empty_msg: str):
    with col:
        with st.container(border=True):
            st.markdown(
                f"<div style='font-size:0.78rem; color:{color}; "
                f"text-transform:uppercase; letter-spacing:0.08em; font-weight:700; margin-bottom:10px;'>"
                f"{title}</div>",
                unsafe_allow_html=True,
            )
            if not drivers:
                st.markdown(
                    f"<div style='color:rgba(241,245,249,0.45); font-size:0.85rem; padding:6px 0;'>{empty_msg}</div>",
                    unsafe_allow_html=True,
                )
                return
            for d in drivers:
                friendly_html = (
                    f"<div style='font-size:0.82rem; color:rgba(241,245,249,0.65); "
                    f"line-height:1.45; margin-top:6px; padding:6px 10px; "
                    f"background:rgba(255,255,255,0.025); border-left:2px solid {color}; "
                    f"border-radius:4px;'>💡 {d.friendly}</div>"
                ) if d.friendly else ""
                st.markdown(
                    f"""
                    <div style="padding:10px 0 12px 0; border-bottom:1px solid rgba(255,255,255,0.04);">
                      <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                        <div style="flex:1; min-width:0;">
                          <div style="font-size:0.7rem; color:rgba(241,245,249,0.5);
                                      text-transform:uppercase; letter-spacing:0.06em; font-weight:600;">
                            {d.label}
                          </div>
                          <div style="font-size:0.88rem; color:rgba(241,245,249,0.85); margin-top:2px;">
                            {d.detail}
                          </div>
                        </div>
                        <div style="font-family:'JetBrains Mono', monospace; font-weight:700;
                                    color:{color}; font-size:1.0rem; margin-left:12px;">
                          {d.contribution:+.1f}
                        </div>
                      </div>
                      {friendly_html}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


narrative_cols = st.columns(2, gap="medium")
_driver_card(
    narrative_cols[0],
    "📈 USD/KRW 끌어올리는 요인 (오르는 이유)",
    narrative.up_drivers,
    "#EF4444",
    "현재 USD/KRW를 끌어올리는 매크로 요인이 약합니다.",
)
_driver_card(
    narrative_cols[1],
    "📉 USD/KRW 끌어내리는 요인 (떨어지는 이유)",
    narrative.down_drivers,
    "#22C55E",
    "현재 USD/KRW를 끌어내리는 매크로 요인이 약합니다.",
)

st.markdown("")  # spacer


# ─────────────────────────────────────────────────────────────
# 1) Top KPI Row — 현재 환율
# ─────────────────────────────────────────────────────────────
kpi_cols = st.columns([1.3, 1, 1, 1, 1])
with kpi_cols[0]:
    st.metric(
        "USD/KRW (종가)",
        f"{usd_snap.last:,.2f}",
        f"{usd_snap.delta:+,.2f} ({usd_snap.pct_1d:+.2f}%)",
        delta_color="off",
    )
with kpi_cols[1]:
    if usd_snap.series.shape[0] >= 20:
        st.metric("20일 평균", f"{usd_snap.ma20:,.2f}",
                  f"{(usd_snap.last - usd_snap.ma20):+.2f}", delta_color="off")
with kpi_cols[2]:
    if usd_snap.series.shape[0] >= 60:
        st.metric("60일 평균", f"{usd_snap.ma60:,.2f}",
                  f"{(usd_snap.last - usd_snap.ma60):+.2f}", delta_color="off")
with kpi_cols[3]:
    hi52 = float(usd_snap.series.tail(252).max()) if len(usd_snap.series) else float("nan")
    st.metric("52주 고점", f"{hi52:,.2f}", f"{(usd_snap.last - hi52):+.2f}", delta_color="off")
with kpi_cols[4]:
    lo52 = float(usd_snap.series.tail(252).min()) if len(usd_snap.series) else float("nan")
    st.metric("52주 저점", f"{lo52:,.2f}", f"{(usd_snap.last - lo52):+.2f}", delta_color="off")

st.divider()


# ─────────────────────────────────────────────────────────────
# 2) Signal Cards — 단기 / 중기
# ─────────────────────────────────────────────────────────────
def _signal_card(col, result: fx_signals.SignalResult):
    """단기 또는 중기 신호 카드."""
    with col:
        with st.container(border=True):
            st.markdown(
                f"""
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:8px;">
                  <div>
                    <div style="font-size:0.78rem; color:rgba(241,245,249,0.55);
                                text-transform:uppercase; letter-spacing:0.08em; font-weight:600;">
                      {result.horizon} · {result.horizon_desc}
                    </div>
                    <div style="font-size:1.35rem; font-weight:700; color:{result.verdict_color}; margin-top:4px;">
                      {result.verdict_emoji} {result.verdict}
                    </div>
                  </div>
                  <div style="text-align:right;">
                    <div style="font-size:0.7rem; color:rgba(241,245,249,0.45);
                                text-transform:uppercase; letter-spacing:0.08em;">점수</div>
                    <div style="font-size:2rem; font-weight:700; color:{result.verdict_color}; line-height:1;">
                      {result.score:+.0f}
                    </div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # 점수 척도 막대 (-100 ~ +100)
            pos = (result.score + 100) / 2.0  # 0 ~ 100
            st.markdown(
                f"""
                <div style="background:rgba(255,255,255,0.06); height:8px; border-radius:4px;
                            position:relative; margin:8px 0 14px 0; overflow:hidden;">
                  <!-- 0점 표시 -->
                  <div style="position:absolute; left:50%; top:-2px; bottom:-2px; width:1px;
                              background:rgba(255,255,255,0.25);"></div>
                  <!-- 점수 위치 -->
                  <div style="position:absolute; left:{pos:.1f}%; top:-3px; bottom:-3px; width:3px;
                              background:{result.verdict_color}; border-radius:2px;
                              box-shadow:0 0 8px {result.verdict_color}80;"></div>
                </div>
                <div style="display:flex; justify-content:space-between; font-size:0.7rem;
                            color:rgba(241,245,249,0.4); margin-top:-8px;">
                  <span>지금 환전 ←</span>
                  <span>중립</span>
                  <span>→ 대기</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Top 3 기여 컴포넌트
            comps_sorted = sorted(result.components, key=lambda c: abs(c.value), reverse=True)
            top3 = comps_sorted[:3]
            if top3:
                st.markdown(
                    "<div style='font-size:0.72rem; color:rgba(241,245,249,0.55); "
                    "text-transform:uppercase; letter-spacing:0.08em; margin:6px 0 4px 0; font-weight:600;'>"
                    "주요 근거</div>",
                    unsafe_allow_html=True,
                )
                for c in top3:
                    sign_color = "#22C55E" if c.value < 0 else ("#EF4444" if c.value > 0 else "#94A3B8")
                    st.markdown(
                        f"""
                        <div style="display:flex; justify-content:space-between; align-items:center;
                                    padding:4px 0; font-size:0.85rem;">
                          <span style="color:rgba(241,245,249,0.78);">{c.detail}</span>
                          <span style="color:{sign_color}; font-weight:600; font-family:'JetBrains Mono', monospace;">
                            {c.value:+.1f}
                          </span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )


sig_cols = st.columns(2, gap="medium")
_signal_card(sig_cols[0], short)
_signal_card(sig_cols[1], mid)

st.markdown("")  # spacer


# ─────────────────────────────────────────────────────────────
# 3) USD/KRW 차트 + 50/200MA
# ─────────────────────────────────────────────────────────────
with st.container(border=True):
    st.markdown(
        "<div style='font-size:0.78rem; color:rgba(241,245,249,0.55); "
        "text-transform:uppercase; letter-spacing:0.08em; font-weight:600; margin-bottom:8px;'>"
        "USD/KRW · 12개월 추이</div>",
        unsafe_allow_html=True,
    )

    try:
        import plotly.graph_objects as go
        s = usd_snap.series.tail(252)  # 1년 거래일
        ma20 = s.rolling(20).mean()
        ma60 = s.rolling(60).mean()
        ma200 = s.rolling(200).mean()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=s.index, y=s.values, name="USD/KRW",
            line=dict(color="#F59E0B", width=2.2),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=ma20.index, y=ma20.values, name="20MA",
            line=dict(color="#60A5FA", width=1.2, dash="dot"),
            hovertemplate="20MA: %{y:.2f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=ma60.index, y=ma60.values, name="60MA",
            line=dict(color="#A78BFA", width=1.2, dash="dot"),
            hovertemplate="60MA: %{y:.2f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=ma200.index, y=ma200.values, name="200MA",
            line=dict(color="#94A3B8", width=1.4, dash="dash"),
            hovertemplate="200MA: %{y:.2f}<extra></extra>",
        ))
        fig.update_layout(
            height=380,
            margin=dict(l=8, r=8, t=8, b=8),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#F1F5F9", family="Inter, sans-serif", size=12),
            xaxis=dict(gridcolor="rgba(255,255,255,0.06)", showline=False),
            yaxis=dict(gridcolor="rgba(255,255,255,0.06)", showline=False, tickformat=",.0f"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    except ImportError:
        st.line_chart(usd_snap.series.tail(252), height=320)


# ─────────────────────────────────────────────────────────────
# 4) Macro Drivers Table
# ─────────────────────────────────────────────────────────────
st.markdown(
    "<div style='margin: 18px 0 8px 0; font-size:0.78rem; color:rgba(241,245,249,0.55); "
    "text-transform:uppercase; letter-spacing:0.08em; font-weight:600;'>"
    "매크로 드라이버 · 변화율과 USD/KRW 영향</div>",
    unsafe_allow_html=True,
)

import pandas as pd

driver_rows = []
for k in ALL_KEYS:
    if k == "USDKRW":
        continue
    snap = snaps.get(k)
    if snap is None:
        continue
    sign = fx_data.USDKRW_SIGN.get(k, 0)
    arrow = "↑KRW약세" if sign > 0 else ("↓KRW강세" if sign < 0 else "—")
    driver_rows.append({
        "지표": snap.label,
        "현재": f"{snap.last:,.2f}" if k != "UST10Y" else f"{snap.last:.2f}%",
        "1일": f"{snap.pct_1d:+.2f}%" if not pd.isna(snap.pct_1d) else "—",
        "5일": f"{snap.pct_5d:+.2f}%" if not pd.isna(snap.pct_5d) else "—",
        "20일": f"{snap.pct_20d:+.2f}%" if not pd.isna(snap.pct_20d) else "—",
        "60일": f"{snap.pct_60d:+.2f}%" if not pd.isna(snap.pct_60d) else "—",
        "USD/KRW 영향": arrow,
    })

if driver_rows:
    df_drivers = pd.DataFrame(driver_rows)
    st.dataframe(
        df_drivers,
        hide_index=True,
        use_container_width=True,
    )
else:
    st.info("매크로 지표를 가져오지 못했습니다.")


# ─────────────────────────────────────────────────────────────
# 5) 신호 컴포넌트 상세 breakdown
# ─────────────────────────────────────────────────────────────
exp_cols = st.columns(2, gap="medium")

def _components_table(col, result: fx_signals.SignalResult):
    with col:
        with st.expander(f"📋 {result.horizon} 신호 컴포넌트 ({result.horizon_desc}) — 점수 {result.score:+.0f}", expanded=False):
            rows = []
            for c in result.components:
                rows.append({
                    "항목": c.name,
                    "최대 기여": f"±{c.weight:.0f}",
                    "실제 기여": f"{c.value:+.1f}",
                    "설명": c.detail,
                })
            if rows:
                st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
            else:
                st.info("컴포넌트가 계산되지 않았습니다 (데이터 부족).")

_components_table(exp_cols[0], short)
_components_table(exp_cols[1], mid)


# ─────────────────────────────────────────────────────────────
# 6) 다가오는 매크로 이벤트
# ─────────────────────────────────────────────────────────────
st.markdown(
    "<div style='margin: 22px 0 8px 0; font-size:0.78rem; color:rgba(241,245,249,0.55); "
    "text-transform:uppercase; letter-spacing:0.08em; font-weight:600;'>"
    "다가오는 매크로 이벤트 · 45일 이내</div>",
    unsafe_allow_html=True,
)

events_30d = fx_events.upcoming(45)
if not events_30d:
    st.info(
        "📅 등록된 매크로 이벤트가 없습니다. `fx_signal_app/events.json` 을 편집해 "
        "다가오는 FOMC, BOK MPC, 미국 CPI, 한국 GDP 등을 추가하면 여기에 표시돼요. "
        "스키마는 `fx_signal_app/events.py` 상단 docstring 참고."
    )
else:
    today = date.today()
    for ev in events_30d:
        d_left = (ev.date - today).days
        urgency = "🔥" if d_left <= 3 else ("⚡" if d_left <= 7 else "📍")
        with st.container(border=True):
            cols = st.columns([0.6, 4, 1.2])
            with cols[0]:
                st.markdown(f"<div style='font-size:1.4rem; text-align:center;'>{ev.icon}</div>",
                            unsafe_allow_html=True)
            with cols[1]:
                st.markdown(
                    f"<div style='font-weight:600; color:#F1F5F9;'>{ev.title}</div>"
                    f"<div style='font-size:0.78rem; color:rgba(241,245,249,0.55); margin-top:2px;'>"
                    f"{ev.category}{' · ' + ev.note if ev.note else ''}</div>",
                    unsafe_allow_html=True,
                )
            with cols[2]:
                day_str = "오늘" if d_left == 0 else ("내일" if d_left == 1 else f"D-{d_left}")
                st.markdown(
                    f"<div style='text-align:right;'>"
                    f"<div style='font-size:1.1rem; font-weight:600; color:{ev.color};'>{urgency} {day_str}</div>"
                    f"<div style='font-size:0.75rem; color:rgba(241,245,249,0.5); margin-top:2px;'>"
                    f"{ev.date.strftime('%m/%d (%a)')}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )


# ─────────────────────────────────────────────────────────────
# 6.5) 백테스트 — 지난 N년 신호 따랐다면 얼마나 더 받았을까?
# ─────────────────────────────────────────────────────────────
st.markdown("")
with st.expander("🧪 백테스트 — 신호 따랐다면 환전을 얼마나 더 잘 했을까?", expanded=False):
    st.caption(
        "지난 N년 매월 가상으로 USD가 입금됐다고 가정하고, "
        "**즉시 환전(no-timing)** vs **신호 기반 환전**을 비교합니다. "
        "→ 신호 기반이 outperform 했다면 실제 환전 정책으로 채택 검토."
    )

    # ─── 점수 부호 규약 안내 (슬라이더 위에 먼저) ────────────────
    st.markdown(
        """
        <div style="background: rgba(255,255,255,0.025); border: 1px solid rgba(255,255,255,0.08);
                    border-radius: 8px; padding: 10px 14px; margin: 4px 0 14px 0;
                    display:flex; gap:24px; flex-wrap:wrap; font-size:0.85rem;">
          <div style="color:rgba(241,245,249,0.55); font-size:0.72rem;
                      text-transform:uppercase; letter-spacing:0.08em; font-weight:600;">
            점수 부호
          </div>
          <div><span style="color:#22C55E; font-weight:600;">◀ 음수 (-100~0)</span>
               <span style="color:rgba(241,245,249,0.65);"> = USD/KRW 하락 압력 → <b>환전 유리</b></span></div>
          <div><span style="color:#EF4444; font-weight:600;">▶ 양수 (0~+100)</span>
               <span style="color:rgba(241,245,249,0.65);"> = USD/KRW 상승 압력 → <b>대기 유리</b> (환전 trigger 안 됨)</span></div>
        </div>
        <div style="font-size:0.78rem; color:rgba(241,245,249,0.5); margin:-6px 0 10px 0; line-height:1.5;">
          💡 환전 신호는 <b>음수 영역</b>에서만 발생하므로 슬라이더는 <code>−80 ~ 0</code> 범위로 제한.
          실제 점수는 보통 <code>−60 ~ +60</code> 사이에서 움직이고, <code>−50 미만</code>은 매우 드문 극단입니다.
        </div>
        """,
        unsafe_allow_html=True,
    )

    bt_cols = st.columns([1, 1, 2, 1])
    with bt_cols[0]:
        bt_period_years = st.selectbox("백테스트 기간", [1, 2, 3], index=1, key="bt_years",
                                        format_func=lambda x: f"{x}년")
    with bt_cols[1]:
        bt_monthly = st.number_input("월 입금 USD", min_value=1000, max_value=1_000_000,
                                      value=10_000, step=1000, key="bt_monthly")
    with bt_cols[2]:
        # 단일 RangeSlider — 왼쪽 핸들 = 강한, 오른쪽 핸들 = 약한
        # 강한 ≤ 약한 자동 보장 (Streamlit이 두 핸들 순서 유지)
        bt_thr_strong, bt_thr_weak = st.slider(
            "환전 trigger 점수 구간  (← 100% 환전 | 50% 환전 →)",
            min_value=-80, max_value=0, value=(-35, -20), step=5,
            key="bt_thr_range",
            help=(
                "점수가 왼쪽 값 이하 → 풀 전부 환전 (강한 신호). "
                "두 값 사이 → 풀의 50% 환전 (약한 신호). "
                "오른쪽 값보다 크면 환전 안 함."
            ),
        )
    with bt_cols[3]:
        bt_max_hold = st.slider("강제 환전 한도 (일)", 60, 365, 180, step=30, key="bt_max_hold")

    # ─── 슬라이더 즉시 해석 — "이 설정의 의미" ─────────────────────
    # 예시: 한도 일수 / 30 만큼 입금이 누적 후 강제 환전 발생 시점.
    # 풀에 쌓이는 최대 USD는 대략 (한도_일/30) × 월 입금.
    typical_pool_months = max(1, int(bt_max_hold / 30))
    typical_pool_usd = bt_monthly * typical_pool_months
    weak_usd = typical_pool_usd * 0.5
    strong_usd = typical_pool_usd * 1.0

    # 오늘의 적용 (현재 단기·중기 평균)
    today_score = (short.score + mid.score) / 2.0
    if today_score <= bt_thr_strong:
        today_color = "#22C55E"
        today_action = f"🟢 보유 USD 100% 환전 (점수 {today_score:+.0f} ≤ {bt_thr_strong})"
    elif today_score <= bt_thr_weak:
        today_color = "#84CC16"
        today_action = f"🟢 보유 USD 50% 환전 (점수 {today_score:+.0f} ≤ {bt_thr_weak})"
    else:
        today_color = "#94A3B8"
        today_action = f"⚪ 환전 안 함 / 보유 (점수 {today_score:+.0f} > {bt_thr_weak})"

    st.markdown(
        f"""
        <div style="background: rgba(255,255,255,0.025); border: 1px solid rgba(255,255,255,0.08);
                    border-radius: 10px; padding: 14px 18px; margin: 14px 0 8px 0;">
          <div style="font-size:0.72rem; color:rgba(241,245,249,0.55);
                      text-transform:uppercase; letter-spacing:0.1em; font-weight:600; margin-bottom:10px;">
            이 설정의 의미 · 매월 ${bt_monthly:,} 입금 가정
          </div>
          <table style="width:100%; border-collapse:collapse; font-size:0.88rem;">
            <thead>
              <tr style="color:rgba(241,245,249,0.55); font-size:0.75rem;
                         text-transform:uppercase; letter-spacing:0.06em;">
                <th style="text-align:left; padding:6px 8px; border-bottom:1px solid rgba(255,255,255,0.08);">점수 구간</th>
                <th style="text-align:left; padding:6px 8px; border-bottom:1px solid rgba(255,255,255,0.08);">행동</th>
                <th style="text-align:right; padding:6px 8px; border-bottom:1px solid rgba(255,255,255,0.08);">예시 환전 액수</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style="padding:8px; color:#22C55E; font-family:'JetBrains Mono',monospace; font-weight:600;">≤ {bt_thr_strong}</td>
                <td style="padding:8px; color:#F1F5F9;">🟢 <b>풀의 100% 환전</b> (강한 신호)</td>
                <td style="padding:8px; text-align:right; color:#22C55E; font-family:'JetBrains Mono',monospace; font-weight:600;">~ ${strong_usd:,.0f}</td>
              </tr>
              <tr>
                <td style="padding:8px; color:#84CC16; font-family:'JetBrains Mono',monospace; font-weight:600;">{bt_thr_strong + 1} ~ {bt_thr_weak}</td>
                <td style="padding:8px; color:#F1F5F9;">🟢 <b>풀의 50% 환전</b> (약한 신호)</td>
                <td style="padding:8px; text-align:right; color:#84CC16; font-family:'JetBrains Mono',monospace; font-weight:600;">~ ${weak_usd:,.0f}</td>
              </tr>
              <tr>
                <td style="padding:8px; color:#94A3B8; font-family:'JetBrains Mono',monospace; font-weight:600;">{bt_thr_weak + 1} ~ +100</td>
                <td style="padding:8px; color:#F1F5F9;">⚪ <b>보유 / 환전 안 함</b></td>
                <td style="padding:8px; text-align:right; color:#94A3B8; font-family:'JetBrains Mono',monospace;">$0</td>
              </tr>
              <tr>
                <td style="padding:8px; color:#F59E0B; font-family:'JetBrains Mono',monospace; font-weight:600;">한도 초과</td>
                <td style="padding:8px; color:#F1F5F9;">🟡 <b>강제 환전</b> ({bt_max_hold}일 ≈ {typical_pool_months}개월 묵힌 USD)</td>
                <td style="padding:8px; text-align:right; color:#F59E0B; font-family:'JetBrains Mono',monospace; font-weight:600;">~ ${bt_monthly:,}/회</td>
              </tr>
            </tbody>
          </table>
          <div style="margin-top:12px; padding:10px 14px; background:rgba(255,255,255,0.025);
                      border-left:3px solid {today_color}; border-radius:6px;">
            <div style="font-size:0.72rem; color:rgba(241,245,249,0.55);
                        text-transform:uppercase; letter-spacing:0.08em; font-weight:600; margin-bottom:4px;">
              오늘 이 설정으로는
            </div>
            <div style="font-size:0.95rem; color:{today_color}; font-weight:600;">
              {today_action}
            </div>
          </div>
          <div style="font-size:0.78rem; color:rgba(241,245,249,0.45); margin-top:10px; line-height:1.5;">
            💡 풀(pool) = 아직 환전 안 한 누적 보유 USD.
            한도 일수가 길수록 풀이 더 많이 쌓여서 한 번에 환전하는 액수도 커집니다.
            예시 액수는 풀이 평균 {typical_pool_months}개월치(${typical_pool_usd:,}) 쌓였다고 가정한 것.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("🚀 백테스트 실행", type="primary", use_container_width=False, key="bt_run"):
        with st.spinner("과거 시계열 가져오는 중 + 시뮬레이션…"):
            from fx_signal_app import backtest as fx_backtest

            # 더 긴 시리즈 로드 (200MA 위해 백테스트 시작 전 추가 1년)
            extra_years = 1
            total_period = f"{bt_period_years + extra_years}y"
            full_map: dict[str, pd.Series] = {}
            for k in ALL_KEYS:
                s = fx_data.fetch_long_series(k, period=total_period)
                if not s.empty:
                    full_map[k] = s

            if "USDKRW" not in full_map:
                st.error("USD/KRW 시계열을 못 받았어요. 백테스트 불가.")
                st.stop()

            usdkrw = full_map["USDKRW"]
            end_dt = usdkrw.index[-1]
            start_dt = end_dt - pd.Timedelta(days=bt_period_years * 365)

            try:
                bt_result = fx_backtest.run_backtest(
                    full_series_map=full_map,
                    start_date=start_dt,
                    end_date=end_dt,
                    params=fx_backtest.BacktestParams(
                        monthly_deposit_usd=float(bt_monthly),
                        max_hold_days=int(bt_max_hold),
                        threshold_strong=float(bt_thr_strong),
                        threshold_weak=float(bt_thr_weak),
                    ),
                )
                st.session_state["_bt_result"] = bt_result
            except Exception as e:
                st.error(f"백테스트 실패: {e}")
                st.stop()

    # 결과 표시
    bt_result = st.session_state.get("_bt_result")
    if bt_result is not None:
        st.markdown("---")

        imm = bt_result.summary[bt_result.summary["시나리오"] == "즉시 환전"].iloc[0]
        sig = bt_result.summary[bt_result.summary["시나리오"] == "신호 기반"].iloc[0]

        # KPI 3개 — 비교 대상 명시 (신호 vs 즉시)
        kpi_cols = st.columns(3)
        with kpi_cols[0]:
            st.metric(
                "🅰 즉시 환전 (baseline)",
                f"{imm['평균 실효 환율']:,.2f} 원/$",
                f"누적 {imm['누적 KRW']/1e6:.1f}M원",
                delta_color="off",
                help="매월 입금된 USD를 그 날 바로 100% 환전했을 때의 평균 환율. 비교 기준선.",
            )
        with kpi_cols[1]:
            outperf = bt_result.outperformance_pct
            st.metric(
                "🅱 신호 기반",
                f"{sig['평균 실효 환율']:,.2f} 원/$",
                f"누적 {sig['누적 KRW']/1e6:.1f}M원",
                delta_color="off",
                help="신호 점수에 따라 환전 시점을 조절했을 때의 평균 환율. (음수 점수일 때만 환전 + 6개월 한도)",
            )
        with kpi_cols[2]:
            extra_krw = sig["누적 KRW"] - imm["누적 KRW"]
            verdict_color = "#22C55E" if outperf > 0.3 else ("#EF4444" if outperf < -0.3 else "#94A3B8")
            verdict_text = (
                "🟢 신호 채택 검토" if outperf > 0.3
                else ("🔴 신호 손해" if outperf < -0.3
                else "⚪ 효과 미미")
            )
            st.metric(
                "🅱 vs 🅰  Outperformance",
                f"{outperf:+.2f}%",
                f"신호 기반이 {extra_krw/1e3:+,.0f}천원 더 받음",
                delta_color="normal",
                help="신호 기반이 즉시 환전 대비 KRW를 얼마나 더(또는 덜) 받았는지. 양수면 신호 유리, 음수면 그냥 즉시 환전이 나음.",
            )
            st.markdown(
                f"<div style='font-size:0.85rem; color:{verdict_color}; font-weight:600; margin-top:-12px;'>{verdict_text}</div>",
                unsafe_allow_html=True,
            )

        # 누적 평균 환율 차트 — 제목은 plotly 밖으로 빼서 겹침 방지
        st.markdown(
            "<div style='margin: 22px 0 6px 0; font-size:0.85rem; color:rgba(241,245,249,0.7); font-weight:600;'>"
            "📈 누적 평균 실효 환율 — 낮을수록 같은 USD로 KRW를 더 잘 받은 것</div>",
            unsafe_allow_html=True,
        )
        try:
            import plotly.graph_objects as go
            fig_bt = go.Figure()
            cum_df = bt_result.cumulative_rate.copy()
            for col, color in [("즉시 환전", "#94A3B8"), ("신호 기반", "#F59E0B")]:
                if col in cum_df.columns:
                    fig_bt.add_trace(go.Scatter(
                        x=cum_df.index, y=cum_df[col].values, name=col,
                        line=dict(color=color, width=2.2),
                        hovertemplate="%{x|%Y-%m-%d}<br>" + col + ": %{y:,.2f}<extra></extra>",
                    ))
            fig_bt.update_layout(
                height=340,
                margin=dict(l=8, r=8, t=44, b=8),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#F1F5F9", family="Inter, sans-serif", size=12),
                xaxis=dict(gridcolor="rgba(255,255,255,0.06)", showline=False),
                yaxis=dict(gridcolor="rgba(255,255,255,0.06)", showline=False, tickformat=",.0f"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                hovermode="x unified",
            )
            st.plotly_chart(fig_bt, use_container_width=True, config={"displayModeBar": False})
        except ImportError:
            st.line_chart(bt_result.cumulative_rate)

        # 신호 점수 시계열 (작게)
        st.markdown(
            "<div style='margin: 14px 0 4px 0; font-size:0.85rem; color:rgba(241,245,249,0.7); font-weight:600;'>"
            "📊 백테스트 기간 일별 종합 점수 (단기+중기 평균) — 음수일 때 환전 trigger</div>",
            unsafe_allow_html=True,
        )
        st.line_chart(bt_result.score_series, height=180)

        # 환전 액션 로그 — expander 중첩 안 되므로 토글 버튼으로
        st.markdown("")
        show_log = st.toggle("📋 환전 액션 로그 보기", key="bt_show_log")
        if show_log:
            trades_show = bt_result.trades.copy()
            trades_show["date"] = pd.to_datetime(trades_show["date"]).dt.strftime("%Y-%m-%d")
            trades_show["usd"] = trades_show["usd"].apply(lambda x: f"{x:,.0f}")
            trades_show["rate"] = trades_show["rate"].apply(lambda x: f"{x:,.2f}")
            trades_show["krw"] = trades_show["krw"].apply(lambda x: f"{x:,.0f}")
            trades_show = trades_show.rename(columns={
                "date": "날짜", "usd": "USD", "rate": "환율", "krw": "KRW", "scenario": "시나리오", "reason": "사유",
            })
            st.dataframe(trades_show, hide_index=True, use_container_width=True)

        # 면책
        st.caption(
            "⚠️ 백테스트는 과거 데이터 기반. 과거 outperformance가 미래 outperformance를 보장하지 않습니다. "
            "또 매월 같은 액수 입금이라는 가정이며, 실제 매출 변동 / 입금 일자 / 환전 스프레드는 미반영."
        )


# ─────────────────────────────────────────────────────────────
# 7) 점수 계산 로직 설명
# ─────────────────────────────────────────────────────────────
st.markdown("")
with st.expander("📖 점수는 어떻게 계산되나요? (방법론 · 가중치 · 임계값)", expanded=False):
    st.markdown(
        """
        ### 부호 규약 (USD → KRW 환전자 관점)

        모든 점수는 **-100 ~ +100** 사이의 정수로 표시되며, 부호는 다음과 같습니다:

        | 점수 부호 | 의미 | 환전 시사점 |
        | --- | --- | --- |
        | **음수** | USD/KRW **하락** 압력 → 곧 USD가 싸질 듯 | 🟢 지금 환전 유리 |
        | **0 근처** | 방향성 약함 | ⚪ 중립 |
        | **양수** | USD/KRW **상승** 압력 → 곧 USD가 비싸질 듯 | 🔴 환전 대기 유리 |

        ### 단일 호라이즌 판정 임계값

        | 점수 구간 | 단기/중기 판정 |
        | --- | --- |
        | ≤ −35 | 🟢 **지금 환전 권장** (강한 신호) |
        | −35 ~ −20 | 🟢 약한 환전 신호 |
        | −20 ~ +20 | ⚪ 중립 |
        | +20 ~ +35 | 🟡 약한 대기 신호 |
        | ≥ +35 | 🔴 **환전 대기 권장** (강한 신호) |

        ### 종합 판정 (5단계, 우선순위 순)

        | # | 조건 (단기 S, 중기 M) | 판정 | 권장 행동 |
        | --- | --- | --- | --- |
        | 1 | S ≤ −35 **AND** M ≤ −20 | 🟢 지금 즉시 환전 권장 | 큰 비중 환전 (60~80%) |
        | 2 | (S ≤ −20 **OR** M ≤ −20) 반대편 +20 미만 | 🟢 분할 환전 시작 | 일부 환전 (30~50%) |
        | 3 | S ≥ +35 **AND** M ≥ +20 | 🔴 환전 대기 권장 | 보류 (필수 자금만) |
        | 4 | (S ≥ +20 **OR** M ≥ +20) 반대편 −20 초과 | 🟡 환전 보류 (소량만) | 필수 자금만 (10~20%) |
        | 5 | 그 외 (혼조 또는 중립) | ⚪ 중립 — 필요 만큼만 | DCA / 자금 사정에 맞춰 |

        ### 단기 신호 (1~2주) 컴포넌트 — 명목 ±100점

        단기는 **기술적 mean-reversion + 단기 매크로 모멘텀** 비중을 둡니다.

        | 항목 | 최대 기여 | 부호 의미 |
        | --- | --- | --- |
        | USD/KRW RSI(14) | ±25 | 과매수(≥70) → −, 과매도(≤30) → + |
        | USD/KRW vs 20일 이평선 | ±15 | 위(+), 아래(−) |
        | USD/KRW 5일 모멘텀 | ±15 | 상승 추세 → + |
        | DXY (달러 인덱스) 5일 | ±15 | 강세 → + (USD 강세 = KRW 약세) |
        | 미국 10Y 국채금리 5일 변화 | ±10 | 상승 → + |
        | KOSPI 5일 모멘텀 | ±10 | 강세 → − (외인 유입 = KRW 강세) |
        | USD/CNY 5일 (위안 동조) | ±10 | 위안 약세 → + (KRW 동조 약세) |

        ### 중기 신호 (1~3개월) 컴포넌트 — 명목 ±100점

        중기는 **장기 추세 추종 + 누적 매크로 변화** 비중을 둡니다.

        | 항목 | 최대 기여 | 부호 의미 |
        | --- | --- | --- |
        | USD/KRW vs 200일 이평선 | ±20 | 위(+), 아래(−) |
        | USD/KRW 60MA vs 200MA (cross) | ±15 | 골든크로스(+), 데드크로스(−) |
        | DXY 60일 모멘텀 | ±20 | 강세 → + |
        | 미국 10Y 60일 변화 | ±15 | 상승 → + |
        | KOSPI 60일 모멘텀 | ±10 | 강세 → − |
        | 원유 60일 (WTI·Brent 평균) | ±10 | 상승 → + (한국 수입 부담) |
        | USD/CNY 60일 | ±10 | 위안 약세 → + |

        ### 데이터 소스

        - **Yahoo Finance** (yfinance 라이브러리, 캐시 15분)
          - USD/KRW = `KRW=X`, DXY = `DX-Y.NYB`, 미국 10Y = `^TNX`
          - KOSPI = `^KS11`, Brent/WTI = `BZ=F`/`CL=F`
          - USD/CNY = `CNY=X`, USD/JPY = `JPY=X`
        - **매크로 이벤트** = `fx_signal_app/events.json` (직접 편집)

        ### 한계점 (꼭 읽어주세요)

        - 신호는 **휴리스틱 점수**이며 머신러닝/계량 예측이 아닙니다. 가중치는 경험적으로 설정.
        - **갑작스러운 정책 변화 / 지정학 리스크 / FX 개입**은 모델에 반영되지 않습니다.
        - 매크로 이벤트(FOMC, BOK)는 발생 **전·후** 변동성을 키우므로 이벤트 직전 환전은 신중하게.
        - 본 도구는 **참고용**입니다. 실제 환전은 본인 자금 사정과 판단에 따라 결정하세요.
        """
    )


# ─────────────────────────────────────────────────────────────
# 푸터 — 데이터 source / 면책
# ─────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "📡 데이터: Yahoo Finance (yfinance) · 캐시 15분. "
    "💡 신호 점수는 휴리스틱 기반(기술적 + 단기 매크로) 의사결정 보조 도구이며, "
    "투자/환전 추천이 아닙니다. 실제 환전은 본인의 판단과 자금 사정에 맞춰 결정하세요."
)
