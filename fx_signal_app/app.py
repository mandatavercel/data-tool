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

import pandas as pd
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
# 0.25) 📧 이메일 발송 — 세션 단위 로그인 + HTML 본문
# ─────────────────────────────────────────────────────────────
try:
    from fx_signal_app import email_report as fx_email
except ImportError:
    import email_report as fx_email  # type: ignore

_events_7d = fx_events.upcoming(7)
_mail_html, _mail_plain = fx_email.build_html_report(
    usdkrw_last=usd_snap.last,
    usdkrw_delta_pct=usd_snap.pct_1d if not pd.isna(usd_snap.pct_1d) else 0.0,
    verdict=verdict, narrative=narrative, short=short, mid=mid,
    upcoming_events=_events_7d,
)
_mail_subject = (
    f"[FX Signal] USD/KRW {usd_snap.last:,.2f} · "
    f"{verdict.emoji} {verdict.headline} · "
    f"{date.today().strftime('%Y-%m-%d')}"
)

# 세션에서 로그인 상태 조회
sender_email = st.session_state.get("_fx_sender_email", "")
sender_pwd = st.session_state.get("_fx_sender_pwd", "")
sender_host = st.session_state.get("_fx_sender_host", "")
sender_port = int(st.session_state.get("_fx_sender_port", 587))
logged_in = bool(sender_email and sender_pwd and sender_host)

with st.container(border=True):
    if not logged_in:
        # ── 로그인 UI ───────────────────────────────────────
        st.markdown(
            "<div style='font-size:0.85rem; color:#F1F5F9; font-weight:600;'>"
            "📧 HTML 이메일 발송 — 한 번 로그인하면 이 세션 내내 사용</div>",
            unsafe_allow_html=True,
        )
        st.caption(
            "💡 Gmail은 [App Password](https://myaccount.google.com/apppasswords) 가 필요해요 "
            "(2단계 인증 ON 후 발급되는 16자리). 일반 비밀번호로는 SMTP 인증 안 됨."
        )
        with st.form("fx_email_login_form", clear_on_submit=False):
            email_in = st.text_input(
                "보내는 사람 이메일",
                value="",
                placeholder="you@gmail.com",
                key="fx_login_email",
            )
            pwd_in = st.text_input(
                "앱 비밀번호 (또는 SMTP 비밀번호)",
                value="",
                type="password",
                placeholder="xxxx xxxx xxxx xxxx",
                key="fx_login_pwd",
            )

            # 도메인 자동 추정
            guess_host, guess_port = fx_email.guess_smtp(email_in)
            cols_smtp = st.columns([3, 1])
            with cols_smtp[0]:
                host_in = st.text_input(
                    "SMTP 서버",
                    value=guess_host or "",
                    placeholder="smtp.gmail.com",
                    help="이메일 도메인으로 자동 추정. 회사 메일은 직접 입력.",
                    key="fx_login_host",
                )
            with cols_smtp[1]:
                port_in = st.number_input(
                    "포트",
                    value=guess_port,
                    min_value=1, max_value=65535,
                    help="465 = SSL · 587 = STARTTLS",
                    key="fx_login_port",
                )

            submit_login = st.form_submit_button("🔐 로그인 (이 세션만)", type="primary")
            if submit_login:
                if not email_in.strip() or not pwd_in or not host_in.strip():
                    st.error("이메일, 비밀번호, SMTP 서버는 모두 필요.")
                else:
                    st.session_state["_fx_sender_email"] = email_in.strip()
                    st.session_state["_fx_sender_pwd"] = pwd_in
                    st.session_state["_fx_sender_host"] = host_in.strip()
                    st.session_state["_fx_sender_port"] = int(port_in)
                    st.rerun()

        st.caption(
            "🔒 비밀번호는 이 세션 메모리에만 잠시 보관. 페이지 닫거나 새로고침하면 사라집니다. "
            "Streamlit Cloud 디스크나 git에 저장되지 않아요."
        )
    else:
        # ── 로그인 완료 — 발송 UI ──────────────────────────
        head_cols = st.columns([4, 1])
        with head_cols[0]:
            st.markdown(
                f"<div style='font-size:0.85rem; color:#F1F5F9; font-weight:600;'>"
                f"📧 HTML 이메일 발송 — 보내는 사람: "
                f"<span style='color:#F59E0B;'>{sender_email}</span> "
                f"<span style='color:rgba(241,245,249,0.5); font-weight:400;'>"
                f"({sender_host}:{sender_port})</span></div>",
                unsafe_allow_html=True,
            )
        with head_cols[1]:
            if st.button("🔒 로그아웃", key="fx_logout", use_container_width=True):
                for k in ("_fx_sender_email", "_fx_sender_pwd", "_fx_sender_host", "_fx_sender_port"):
                    st.session_state.pop(k, None)
                st.rerun()

        send_cols = st.columns([3, 1])
        with send_cols[0]:
            mail_to = st.text_input(
                "받는 사람",
                value="yonghan@mandata.kr",
                placeholder="you@example.com",
                label_visibility="collapsed",
                key="fx_mail_to",
            )
        with send_cols[1]:
            do_send = st.button("📧 발송", type="primary", use_container_width=True,
                                 key="fx_do_send", disabled=not mail_to.strip())

        if do_send:
            try:
                with st.spinner("📤 발송 중…"):
                    cfg = fx_email.EmailConfig(
                        smtp_host=sender_host,
                        smtp_port=sender_port,
                        smtp_user=sender_email,
                        smtp_password=sender_pwd,
                        from_addr=sender_email,
                        to_addr=mail_to.strip(),
                    )
                    fx_email.send_email(
                        cfg=cfg,
                        to_addr=mail_to.strip(),
                        subject=_mail_subject,
                        html=_mail_html,
                        plain=_mail_plain,
                    )
                st.success(f"✅ {mail_to.strip()} 로 발송 완료")
            except Exception as e:
                st.error(f"❌ 발송 실패: {type(e).__name__}: {e}")
                with st.expander("⚠️ 디버그 정보 / 흔한 원인", expanded=False):
                    st.code(str(e))
                    st.markdown(
                        """
                        흔한 원인:
                        - **Gmail App Password 가 아니라 일반 비밀번호 사용** — Gmail은 2FA 활성화 후 [App Password](https://myaccount.google.com/apppasswords) 발급 필요
                        - **2단계 인증이 꺼져있음** — 켜야 App Password 발급 가능
                        - **회사 메일 SMTP가 외부 접속 차단** — IT 담당자에게 SMTP 외부 사용 허용 요청
                        - **SMTP 호스트/포트 오타** — Gmail: `smtp.gmail.com:587`, Naver: `smtp.naver.com:587`, Daum: `smtp.daum.net:465`
                        - **포트 587 → STARTTLS, 465 → SSL** — 자동 감지하지만 호스트와 안 맞으면 실패
                        """
                    )

        with st.expander("📄 보낼 본문 미리보기 (HTML 렌더링은 메일 클라이언트에서)", expanded=False):
            st.components.v1.html(_mail_html, height=500, scrolling=True)

st.markdown("")


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

    # ─── 🎯 최적 조합 자동 탐색 (사용자가 슬라이더 만지기 전에 추천) ──
    sweep_cols = st.columns([3, 1])
    with sweep_cols[0]:
        st.markdown(
            "<div style='font-size:0.95rem; color:#F1F5F9; font-weight:600;'>"
            "🎯 최적 조합 자동 찾기</div>"
            "<div style='font-size:0.8rem; color:rgba(241,245,249,0.6); margin-top:2px;'>"
            "수십 개 슬라이더 조합을 한 번에 시도해서 outperformance가 가장 좋은 조합 추천. "
            "월 입금액 정하고 버튼만 누르면 끝.</div>",
            unsafe_allow_html=True,
        )
    with sweep_cols[1]:
        bt_sweep_run = st.button("🎯 최적 조합 찾기", type="secondary",
                                  use_container_width=True, key="bt_sweep_run")

    if bt_sweep_run:
        with st.spinner("과거 시계열 + 점수 시계열 계산 → 수백 조합 시뮬레이션 중…"):
            from fx_signal_app import backtest as fx_backtest_sweep

            # 데이터 로드
            try:
                bt_period_years_sweep = st.session_state.get("bt_years", 2)
            except Exception:
                bt_period_years_sweep = 2
            try:
                bt_monthly_sweep = st.session_state.get("bt_monthly", 10_000)
            except Exception:
                bt_monthly_sweep = 10_000

            extra_years = 1
            total_period = f"{bt_period_years_sweep + extra_years}y"
            full_map_sweep: dict[str, pd.Series] = {}
            for k in ALL_KEYS:
                s = fx_data.fetch_long_series(k, period=total_period)
                if not s.empty:
                    full_map_sweep[k] = s

            if "USDKRW" not in full_map_sweep:
                st.error("USD/KRW 시계열 못 받아서 sweep 불가.")
            else:
                usdkrw_sweep = full_map_sweep["USDKRW"]
                end_dt = usdkrw_sweep.index[-1]
                start_dt = end_dt - pd.Timedelta(days=bt_period_years_sweep * 365)
                mask = (usdkrw_sweep.index >= start_dt) & (usdkrw_sweep.index <= end_dt)
                usdkrw_sub = usdkrw_sweep.loc[mask]

                # 점수 시계열 한 번만 계산 (가장 비싼 부분)
                score_series_sweep = fx_backtest_sweep.build_score_series(
                    full_map_sweep, usdkrw_sub.index
                )

                # sweep 실행
                sweep_df = fx_backtest_sweep.parameter_sweep(
                    usdkrw_sub,
                    score_series_sweep,
                    monthly_deposit_usd=float(bt_monthly_sweep),
                )
                st.session_state["_bt_sweep_df"] = sweep_df

    # sweep 결과 표시
    sweep_df = st.session_state.get("_bt_sweep_df")
    if sweep_df is not None and not sweep_df.empty:
        # 신호 비중이 5% 이상인 조합 (의미 있는 통계) vs 그 외
        meaningful = sweep_df[sweep_df["신호 비중 %"] >= 5.0].copy()

        if not meaningful.empty:
            best = meaningful.iloc[0]
            best_label = "신호 비중 5% 이상 조합 중 신호 실력 최고"
            best_metric = best["신호 실력 %"]
        else:
            best = sweep_df.iloc[0]
            best_label = "신호 비중 5% 미만 (참고용)"
            best_metric = best["신호 실력 %"]

        # 추천 카드 — 신호 실력 메트릭 기준
        best_color = "#22C55E" if best_metric > 0.3 else (
            "#EF4444" if best_metric < -0.3 else "#94A3B8"
        )
        st.markdown(
            f"""
            <div style="background: linear-gradient(135deg, {best_color}15 0%, {best_color}05 100%);
                        border: 1px solid {best_color}55; border-left: 4px solid {best_color};
                        border-radius: 10px; padding: 14px 18px; margin: 10px 0;">
              <div style="font-size:0.72rem; color:rgba(241,245,249,0.55);
                          text-transform:uppercase; letter-spacing:0.1em; font-weight:600;">
                🏆 추천 조합 — {best_label}
              </div>
              <div style="display:flex; gap:24px; align-items:center; margin-top:8px; flex-wrap:wrap;">
                <div>
                  <div style="font-size:0.7rem; color:rgba(241,245,249,0.55);">신호 실력 (vs 시장 평균)</div>
                  <div style="font-size:1.45rem; font-weight:700; color:{best_color}; line-height:1;">
                    {best_metric:+.2f}%
                  </div>
                </div>
                <div>
                  <div style="font-size:0.7rem; color:rgba(241,245,249,0.55);">파라미터</div>
                  <div style="font-size:0.95rem; color:rgba(241,245,249,0.85); font-weight:600;">
                    약한 {int(best['약한'])} · 강한 {int(best['강한'])} · 한도 {int(best['한도(일)'])}일
                  </div>
                </div>
                <div>
                  <div style="font-size:0.7rem; color:rgba(241,245,249,0.55);">신호 비중 / 환전 횟수</div>
                  <div style="font-size:0.95rem; color:rgba(241,245,249,0.85);">
                    {best['신호 비중 %']:.1f}% / 신호 {int(best['신호 환전'])} · 강제 {int(best['강제 환전'])}회
                  </div>
                </div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        apply_cols = st.columns([1, 3])
        with apply_cols[0]:
            if st.button("✅ 이 조합 적용", key="bt_apply_best", type="primary",
                         use_container_width=True):
                st.session_state["bt_thr_range"] = (int(best["강한"]), int(best["약한"]))
                st.session_state["bt_max_hold"] = int(best["한도(일)"])
                st.rerun()

        # Top 표 — 신호 실력 기준 정렬
        st.markdown(
            "<div style='font-size:0.78rem; color:rgba(241,245,249,0.55); "
            "text-transform:uppercase; letter-spacing:0.08em; font-weight:600; "
            "margin: 14px 0 6px 0;'>📊 상위 조합 — 신호 실력 기준 정렬</div>",
            unsafe_allow_html=True,
        )
        if not meaningful.empty:
            st.dataframe(meaningful.head(10), hide_index=True, use_container_width=True)
            st.caption(
                "⭐ **신호 실력 %** = 신호 환전 평균 환율이 시장 단순 평균보다 얼마나 높은지. "
                "양수 = 진짜 timing 실력 / 음수 = 신호가 오히려 안 좋은 시점에 환전. "
                "**전체 outperf %** 는 청산 효과까지 포함된 결과(운빨 섞임)."
            )
        else:
            st.warning(
                "신호 비중 5% 이상인 조합이 없습니다. → 임계값이 너무 가혹해서 신호가 거의 trigger 안 됨. "
                "임계값을 0에 더 가깝게 (예: 약한 -10, 강한 -20) 조정해보세요."
            )

        show_forced = st.toggle("🔒 신호 비중 5% 미만 조합도 보기 (참고)", key="bt_show_forced")
        if show_forced:
            below = sweep_df[sweep_df["신호 비중 %"] < 5.0].copy()
            if not below.empty:
                st.dataframe(below.head(10), hide_index=True, use_container_width=True)

        # 면책 — 더 강화
        st.caption(
            "⚠️ 과거 데이터 최적화의 한계: 백테스트에서 최적이었던 조합이 미래에도 최적이라는 보장은 없습니다. "
            "Overfitting 위험을 줄이려면 여러 기간(1년 / 2년 / 3년)에서 **모두 양수의 신호 실력**을 가진 조합을 채택하세요. "
            "어떤 기간에도 신호 실력이 +0.3% 이상 안 나오면 그건 신호 자체가 작동 안 한다는 신호입니다."
        )

    st.markdown("---")

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
                "🟢 표면적으로 신호 우위" if outperf > 0.3
                else ("🔴 표면적으로 신호 손해" if outperf < -0.3
                else "⚪ 효과 미미")
            )
            st.metric(
                "🅱 vs 🅰  전체 Outperformance",
                f"{outperf:+.2f}%",
                f"신호 기반이 {extra_krw/1e3:+,.0f}천원 더 받음",
                delta_color="normal",
                help="신호 + 강제 청산 + 종료 청산 모두 포함된 결과. 종료 청산 효과(운빨)가 섞여있어 신호 실력의 정확한 지표는 아님.",
            )
            st.markdown(
                f"<div style='font-size:0.85rem; color:{verdict_color}; font-weight:600; margin-top:-12px;'>{verdict_text}</div>",
                unsafe_allow_html=True,
            )

        # ─── 정직한 분리 — 신호 실력 vs 청산 운빨 ─────────────────
        st.markdown(
            "<div style='margin: 18px 0 8px 0; font-size:0.78rem; color:rgba(241,245,249,0.55); "
            "text-transform:uppercase; letter-spacing:0.08em; font-weight:600;'>"
            "🔬 정직한 분리 — 신호 실력 vs 청산 운빨</div>",
            unsafe_allow_html=True,
        )

        honest_cols = st.columns(3)
        with honest_cols[0]:
            mkt_avg = bt_result.market_avg_rate
            st.metric(
                "📐 시장 단순 평균 (참조)",
                f"{mkt_avg:,.2f} 원/$" if mkt_avg > 0 else "—",
                "기간 USDKRW 평균",
                delta_color="off",
                help="백테스트 기간 USD/KRW의 일별 단순 평균. 신호 실력의 fair한 비교 기준선.",
            )
        with honest_cols[1]:
            sig_share = bt_result.signal_trades_usd_share * 100
            sig_only_outperf = bt_result.signal_only_outperf_pct
            sig_avg_rate = bt_result.signal_avg_rate
            label_color = "#22C55E" if sig_only_outperf > 0.3 else ("#EF4444" if sig_only_outperf < -0.3 else "#94A3B8")
            if sig_share <= 0.5:
                # 신호 환전 거의 없으면 의미 없음
                st.metric(
                    "🎯 신호 환전만의 평균",
                    "— (신호 거의 없음)",
                    f"전체 USD 중 신호 환전 {sig_share:.1f}%",
                    delta_color="off",
                    help="신호가 거의 trigger 안 됨 → 신호 실력 판정 불가. 임계값을 더 완화하거나 백테스트 기간 늘려보세요.",
                )
            else:
                st.metric(
                    "🎯 신호 환전만의 평균",
                    f"{sig_avg_rate:,.2f} 원/$",
                    f"전체 USD 중 신호 환전 {sig_share:.1f}%",
                    delta_color="off",
                    help="신호가 trigger 된 trade들만의 평균 환율. 시장 평균과 비교해야 신호의 진짜 실력.",
                )
        with honest_cols[2]:
            if sig_share <= 0.5:
                st.metric(
                    "🏅 신호 실력 (vs 시장 평균)",
                    "측정 불가",
                    "신호 환전 부족",
                    delta_color="off",
                )
            else:
                st.metric(
                    "🏅 신호 실력 (vs 시장 평균)",
                    f"{sig_only_outperf:+.2f}%",
                    "양수면 진짜 실력, 음수면 timing 실패",
                    delta_color="normal",
                    help="신호 환전 trade들의 평균 환율이 시장 단순 평균보다 얼마나 높은지. 종료 청산 운빨이 제거된 순수 신호 효과.",
                )
                st.markdown(
                    f"<div style='font-size:0.85rem; color:{label_color}; font-weight:600; margin-top:-12px;'>"
                    + (
                        "🟢 진짜 신호 실력 있음" if sig_only_outperf > 0.3
                        else ("🔴 신호가 오히려 안 좋은 timing" if sig_only_outperf < -0.3
                        else "⚪ 신호 실력 미미")
                    )
                    + "</div>",
                    unsafe_allow_html=True,
                )

        # 해석 박스 — 어떻게 읽어야 하나
        with st.container(border=True):
            st.markdown(
                """
                **📖 결과 해석 가이드**

                - **전체 Outperformance** = 신호 환전 + 강제 환전 + 종료 청산 *모두 합친* 결과 → 청산 시점이 환율 고/저점 어디에 떨어지냐에 따라 운빨에 흔들림
                - **신호 실력** = 신호 trigger 된 분만 시장 평균과 비교 → **진짜 신호의 timing skill**
                - **신호 환전 비중이 5% 미만**이면 통계적으로 의미 없음 → 임계값 완화 또는 백테스트 기간 확장
                - 차트의 마지막 부분이 갑자기 크게 점프하면 → **종료 청산이 우연히 좋은(나쁜) 환율에 떨어진 것**. 그건 신호 실력 아님

                → "이 도구를 실제 채택할까?" 의 답은 **🏅 신호 실력** 메트릭으로만 판단하세요.
                """
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

        # 환전 액션 로그 — 시나리오 필터 추가
        st.markdown("")
        show_log = st.toggle("📋 환전 액션 로그 보기", key="bt_show_log")
        if show_log:
            log_cols = st.columns([1.5, 3])
            with log_cols[0]:
                log_filter = st.selectbox(
                    "시나리오 필터",
                    ["📊 두 시나리오 모두", "🅰 즉시 환전만 (baseline)", "🅱 신호 기반만"],
                    key="bt_log_filter",
                )
            with log_cols[1]:
                st.caption(
                    "💡 두 시나리오를 같이 시뮬레이션해서 비교해요. "
                    "**즉시 환전** = 매월 입금일에 그냥 환전한 baseline. "
                    "**신호 기반** = 신호 점수에 따라 환전 시점 조절한 시나리오."
                )

            trades_show = bt_result.trades.copy()
            if log_filter == "🅰 즉시 환전만 (baseline)":
                trades_show = trades_show[trades_show["scenario"] == "즉시 환전"]
            elif log_filter == "🅱 신호 기반만":
                trades_show = trades_show[trades_show["scenario"] == "신호 기반"]

            trades_show = trades_show.sort_values("date")
            trades_show["date"] = pd.to_datetime(trades_show["date"]).dt.strftime("%Y-%m-%d")
            trades_show["usd"] = trades_show["usd"].apply(lambda x: f"{x:,.0f}")
            trades_show["rate"] = trades_show["rate"].apply(lambda x: f"{x:,.2f}")
            trades_show["krw"] = trades_show["krw"].apply(lambda x: f"{x:,.0f}")
            trades_show = trades_show.rename(columns={
                "date": "날짜", "usd": "USD", "rate": "환율", "krw": "KRW", "scenario": "시나리오", "reason": "사유",
            })
            st.dataframe(trades_show, hide_index=True, use_container_width=True)

            # 시나리오별 요약 메트릭
            st.caption(
                f"📊 표시된 {len(trades_show)}건 · "
                f"전체 환전 액션 {len(bt_result.trades)}건 "
                f"(즉시 {len(bt_result.trades[bt_result.trades['scenario']=='즉시 환전'])} · "
                f"신호 {len(bt_result.trades[bt_result.trades['scenario']=='신호 기반'])})"
            )

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
