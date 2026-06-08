"""
FX Signal — 이메일 리포트.

현재 신호 화면의 핵심 내용을 정리해서:
  1) mailto: 링크 — 셋업 없이 기본 메일 클라이언트로 발송 (Gmail web/Apple Mail/Outlook)
  2) SMTP — secrets.toml 설정 시 자동 발송 (선택)

기본은 mailto. SMTP는 future use (정기 자동 발송 등)를 위해 살려둠.
"""
from __future__ import annotations

import smtplib
import ssl
from dataclasses import dataclass
from datetime import date
from email.message import EmailMessage
from typing import Any, Optional
from urllib.parse import quote


@dataclass
class EmailConfig:
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    from_addr: str
    to_addr: str = ""

    @property
    def is_complete(self) -> bool:
        return bool(self.smtp_host and self.smtp_user and self.smtp_password and self.from_addr)


def load_email_config() -> Optional[EmailConfig]:
    """Streamlit secrets에서 이메일 설정 로드. 없으면 None."""
    try:
        import streamlit as st
        cfg = st.secrets.get("email", {})
    except Exception:
        return None
    if not cfg:
        return None
    try:
        return EmailConfig(
            smtp_host=str(cfg.get("smtp_host", "")).strip(),
            smtp_port=int(cfg.get("smtp_port", 587)),
            smtp_user=str(cfg.get("smtp_user", "")).strip(),
            smtp_password=str(cfg.get("smtp_password", "")).strip(),
            from_addr=str(cfg.get("from_addr", cfg.get("smtp_user", ""))).strip(),
            to_addr=str(cfg.get("to_addr", "")).strip(),
        )
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# HTML 본문 생성
# ─────────────────────────────────────────────────────────────
def _strip_tags(s: str) -> str:
    """HTML 태그 제거 (plain-text fallback용)."""
    import re
    return re.sub(r"<[^>]+>", "", s or "").replace("&nbsp;", " ")


def build_html_report(
    *,
    usdkrw_last: float,
    usdkrw_delta_pct: float,
    verdict: Any,           # CombinedVerdict
    narrative: Any,         # MarketNarrative
    short: Any,             # SignalResult
    mid: Any,               # SignalResult
    upcoming_events: list,  # list[MacroEvent], 7일 이내
    report_date: Optional[date] = None,
    app_url: str = "",      # 대시보드 base URL (있으면 CTA 추가)
) -> tuple[str, str]:
    """
    HTML 본문 + plain-text fallback 반환.

    Returns (html, plain_text).
    """
    rd = report_date or date.today()
    weekday_kr = "월화수목금토일"[rd.weekday()]
    date_str = f"{rd.strftime('%Y-%m-%d')} ({weekday_kr})"

    delta_sign = "▲" if usdkrw_delta_pct >= 0 else "▼"
    delta_color = "#16A34A" if usdkrw_delta_pct < 0 else "#DC2626"  # 환전자: 떨어지면 손해

    # Drivers 렌더
    def _driver_rows(drivers, color):
        if not drivers:
            return f"<div style='color:#94A3B8; font-size:0.9rem; padding:8px 0;'>해당 요인 없음</div>"
        rows = []
        for d in drivers[:3]:
            friendly = d.friendly or d.detail
            rows.append(f"""
              <div style="padding:10px 12px; background:#F8FAFC; border-left:3px solid {color};
                          border-radius:6px; margin-bottom:6px;">
                <div style="font-size:0.72rem; color:#64748B; text-transform:uppercase;
                            letter-spacing:0.06em; font-weight:600;">{d.label}</div>
                <div style="font-size:0.9rem; color:#1E293B; margin-top:3px;">{friendly}</div>
                <div style="font-size:0.78rem; color:#475569; margin-top:4px;">
                  <span style="font-family:'JetBrains Mono', monospace; color:{color}; font-weight:700;">
                    {d.contribution:+.1f}점
                  </span>
                  <span style="margin-left:8px;">· {d.detail}</span>
                </div>
              </div>
            """)
        return "\n".join(rows)

    up_html = _driver_rows(narrative.up_drivers, "#DC2626")
    down_html = _driver_rows(narrative.down_drivers, "#16A34A")

    # Events
    events_section = ""
    if upcoming_events:
        ev_rows = []
        for e in upcoming_events[:5]:
            d_left = (e.date - rd).days
            day_str = "오늘" if d_left == 0 else ("내일" if d_left == 1 else f"D-{d_left}")
            urgency = "🔥" if d_left <= 1 else ("⚡" if d_left <= 3 else "📍")
            ev_rows.append(f"""
              <tr>
                <td style="padding:8px; border-bottom:1px solid #F1F5F9; font-size:0.85rem;">
                  {urgency} <b>{day_str}</b> · {e.date.strftime('%m/%d')}
                </td>
                <td style="padding:8px; border-bottom:1px solid #F1F5F9; font-size:0.85rem; color:#1E293B;">
                  {e.icon} {e.title}
                </td>
                <td style="padding:8px; border-bottom:1px solid #F1F5F9; font-size:0.8rem; color:#64748B;">
                  {e.note or '—'}
                </td>
              </tr>
            """)
        events_section = f"""
        <div style="margin-bottom:20px;">
          <div style="font-size:0.78rem; color:#64748B; text-transform:uppercase;
                      letter-spacing:0.08em; font-weight:600; margin-bottom:10px;">
            📅 다가오는 매크로 이벤트 (7일 이내)
          </div>
          <table style="width:100%; border-collapse:collapse; background:#FFF;
                        border:1px solid #F1F5F9; border-radius:8px; overflow:hidden;">
            <tbody>{''.join(ev_rows)}</tbody>
          </table>
        </div>
        """

    # 최종 HTML
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0; padding:0; background:#F1F5F9; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;">
  <div style="max-width:640px; margin:0 auto; padding:24px 16px;">
    <div style="background:#FFFFFF; border-radius:14px; padding:28px; box-shadow:0 1px 3px rgba(15,23,42,0.08);">

      <!-- Header -->
      <div style="border-bottom:1px solid #F1F5F9; padding-bottom:18px; margin-bottom:20px;">
        <div style="font-size:0.72rem; color:#94A3B8; text-transform:uppercase;
                    letter-spacing:0.12em; font-weight:600;">
          Mandata Data Intelligence
        </div>
        <div style="font-size:1.5rem; font-weight:700; color:#0F172A; margin-top:8px; letter-spacing:-0.02em;">
          💱 FX Signal — USD/KRW
        </div>
        <div style="font-size:0.85rem; color:#64748B; margin-top:4px;">
          {date_str} · 현재
          <b style="color:#0F172A;">{usdkrw_last:,.2f} 원/$</b>
          <span style="color:{delta_color}; margin-left:6px;">{delta_sign} {abs(usdkrw_delta_pct):.2f}%</span>
        </div>
      </div>

      <!-- 종합 판정 -->
      <div style="background:{verdict.color}15; border-left:4px solid {verdict.color};
                  padding:18px; border-radius:8px; margin-bottom:22px;">
        <div style="font-size:0.7rem; color:#64748B; text-transform:uppercase;
                    letter-spacing:0.1em; font-weight:600;">
          오늘의 환전 판정
        </div>
        <div style="font-size:1.55rem; font-weight:700; color:{verdict.color};
                    margin-top:6px; line-height:1.2;">
          {verdict.emoji} {verdict.headline}
        </div>
        <div style="font-size:0.92rem; color:#334155; margin-top:10px; line-height:1.5;">
          {verdict.detail}
        </div>
        <div style="font-size:0.85rem; color:#475569; margin-top:12px; padding-top:10px;
                    border-top:1px solid {verdict.color}33;">
          권장 행동: <b style="color:{verdict.color};">{verdict.action}</b>
        </div>
      </div>

      <!-- 시장 narrative -->
      <div style="background:#F8FAFC; border-radius:8px; padding:16px; margin-bottom:22px;
                  font-size:0.92rem; color:#334155; line-height:1.65;">
        {narrative.summary}
      </div>

      <!-- 단기/중기 점수 -->
      <table style="width:100%; border-collapse:separate; border-spacing:8px; margin-bottom:22px;">
        <tr>
          <td style="width:50%; background:#F8FAFC; padding:14px; border-radius:8px; vertical-align:top;">
            <div style="font-size:0.72rem; color:#94A3B8; text-transform:uppercase;
                        letter-spacing:0.08em; font-weight:600;">단기 (1~2주)</div>
            <div style="font-size:1.35rem; font-weight:700; color:{short.verdict_color}; margin-top:4px;">
              {short.score:+.0f} {short.verdict_emoji}
            </div>
            <div style="font-size:0.82rem; color:#475569; margin-top:4px;">{short.verdict}</div>
          </td>
          <td style="width:50%; background:#F8FAFC; padding:14px; border-radius:8px; vertical-align:top;">
            <div style="font-size:0.72rem; color:#94A3B8; text-transform:uppercase;
                        letter-spacing:0.08em; font-weight:600;">중기 (1~3개월)</div>
            <div style="font-size:1.35rem; font-weight:700; color:{mid.verdict_color}; margin-top:4px;">
              {mid.score:+.0f} {mid.verdict_emoji}
            </div>
            <div style="font-size:0.82rem; color:#475569; margin-top:4px;">{mid.verdict}</div>
          </td>
        </tr>
      </table>

      <!-- 매크로 드라이버 -->
      <div style="margin-bottom:20px;">
        <div style="font-size:0.78rem; color:#64748B; text-transform:uppercase;
                    letter-spacing:0.08em; font-weight:600; margin-bottom:10px;">
          📈 USD/KRW 끌어올리는 요인 (오르는 이유)
        </div>
        {up_html}
      </div>
      <div style="margin-bottom:20px;">
        <div style="font-size:0.78rem; color:#64748B; text-transform:uppercase;
                    letter-spacing:0.08em; font-weight:600; margin-bottom:10px;">
          📉 USD/KRW 끌어내리는 요인 (떨어지는 이유)
        </div>
        {down_html}
      </div>

      {events_section}

      <!-- CTA: 대시보드 링크 (본문 끝 — 모든 분석 확인 후) -->
      {f'''
      <div style="text-align:center; margin:28px 0 20px 0; padding:20px;
                  background:linear-gradient(135deg, #FEF3C7 0%, #FDE68A 100%);
                  border-radius:10px; border:1px solid #F59E0B33;">
        <div style="font-size:0.78rem; color:#92400E; text-transform:uppercase;
                    letter-spacing:0.08em; font-weight:700; margin-bottom:10px;">
          더 자세한 분석은 대시보드에서
        </div>
        <a href="{app_url}" target="_blank" style="
          display:inline-block; background:#F59E0B; color:#0A0A0B;
          padding:14px 32px; border-radius:8px; text-decoration:none;
          font-weight:700; font-size:1.0rem; letter-spacing:-0.01em;
          box-shadow:0 2px 8px rgba(245,158,11,0.3);">
          🔗 FX Signal 대시보드 열기 →
        </a>
        <div style="font-size:0.78rem; color:#78350F; margin-top:12px; line-height:1.5;">
          12개월 차트 · 백테스트 · 전체 매크로 드라이버 표 · 이벤트별 상세 시나리오
        </div>
      </div>
      ''' if app_url else ''}

      <!-- Footer -->
      <div style="border-top:1px solid #E2E8F0; padding-top:16px; margin-top:8px;
                  font-size:0.78rem; color:#94A3B8; text-align:center; line-height:1.7;">
        <div style="font-weight:600; color:#475569; font-size:0.85rem; letter-spacing:-0.01em;">
          Andrew Yoon by Mandata
        </div>
        <div style="margin-top:4px;">Mandata Data Intelligence · 매크로 신호 기반 의사결정 보조 도구</div>
        {f'<div style="margin-top:6px;"><a href="{app_url}" style="color:#F59E0B; text-decoration:none;">대시보드로 가기 →</a></div>' if app_url else ''}
        <div style="margin-top:6px; font-size:0.72rem; color:#CBD5E1;">
          신호는 휴리스틱이며 투자/환전 추천이 아닙니다. 실제 환전은 본인 판단으로.
        </div>
      </div>
    </div>
  </div>
</body>
</html>"""

    # Plain-text fallback
    plain = f"""FX Signal — USD/KRW
{date_str} · 현재 {usdkrw_last:,.2f} 원/$ ({delta_sign} {abs(usdkrw_delta_pct):.2f}%)

──────────────────────────────
{verdict.emoji} {verdict.headline}
{_strip_tags(verdict.detail)}
권장 행동: {verdict.action}
──────────────────────────────

[시장 요약]
{_strip_tags(narrative.summary)}

[점수]
단기 (1~2주): {short.score:+.0f} · {short.verdict}
중기 (1~3개월): {mid.score:+.0f} · {mid.verdict}

[오르는 요인]
"""
    for d in narrative.up_drivers[:3]:
        plain += f"  • {d.label} ({d.contribution:+.1f}점)\n    {d.friendly or d.detail}\n"
    plain += "\n[내리는 요인]\n"
    for d in narrative.down_drivers[:3]:
        plain += f"  • {d.label} ({d.contribution:+.1f}점)\n    {d.friendly or d.detail}\n"

    if upcoming_events:
        plain += "\n[다가오는 매크로 이벤트 (7일 이내)]\n"
        for e in upcoming_events[:5]:
            d_left = (e.date - rd).days
            day_str = "오늘" if d_left == 0 else ("내일" if d_left == 1 else f"D-{d_left}")
            plain += f"  • {day_str} ({e.date.strftime('%m/%d')}) {e.title}\n"

    if app_url:
        plain += f"\n🔗 대시보드에서 자세히 보기:\n   {app_url}\n"

    plain += (
        "\n──────────────────────────────\n"
        "Andrew Yoon by Mandata\n"
        "Mandata Data Intelligence · 매크로 신호 기반 의사결정 보조 도구\n"
        "신호는 휴리스틱이며 투자/환전 추천이 아닙니다."
    )

    return html, plain


# ─────────────────────────────────────────────────────────────
# SMTP 발송
# ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
# 도메인별 SMTP 자동 추정
# ─────────────────────────────────────────────────────────────
SMTP_DEFAULTS: dict[str, tuple[str, int]] = {
    "gmail.com":      ("smtp.gmail.com",   587),
    "googlemail.com": ("smtp.gmail.com",   587),
    "naver.com":      ("smtp.naver.com",   587),
    "daum.net":       ("smtp.daum.net",    465),
    "hanmail.net":    ("smtp.daum.net",    465),
    "kakao.com":      ("smtp.kakao.com",   465),
    "nate.com":       ("smtp.mail.nate.com", 465),
    "outlook.com":    ("smtp-mail.outlook.com", 587),
    "hotmail.com":    ("smtp-mail.outlook.com", 587),
    "live.com":       ("smtp-mail.outlook.com", 587),
    "yahoo.com":      ("smtp.mail.yahoo.com", 587),
    "icloud.com":     ("smtp.mail.me.com", 587),
    "me.com":         ("smtp.mail.me.com", 587),
}


def guess_smtp(email_addr: str) -> tuple[str, int]:
    """이메일 도메인 → (smtp_host, smtp_port). 모르는 도메인이면 (빈 host, 587)."""
    if "@" not in email_addr:
        return ("", 587)
    domain = email_addr.split("@", 1)[1].lower().strip()
    return SMTP_DEFAULTS.get(domain, ("", 587))


def get_app_url() -> str:
    """
    현재 Streamlit 앱의 base URL 추정.
    우선순위: secrets[app_url] > 환경변수 STREAMLIT_APP_URL > request headers > 빈 문자열.
    """
    import os
    # 1) secrets
    try:
        import streamlit as st
        url = str(st.secrets.get("app_url", "")).strip()
        if url:
            return url.rstrip("/")
    except Exception:
        pass
    # 2) 환경변수
    url = os.environ.get("STREAMLIT_APP_URL", "").strip()
    if url:
        return url.rstrip("/")
    # 3) request headers (Streamlit Cloud는 x-forwarded-host 제공)
    try:
        import streamlit as st
        headers = st.context.headers
        host = (headers.get("x-forwarded-host") or headers.get("host") or "").strip()
        proto = (headers.get("x-forwarded-proto") or "https").strip()
        if host:
            return f"{proto}://{host}".rstrip("/")
    except Exception:
        pass
    return ""


# ─────────────────────────────────────────────────────────────
# mailto: 링크 — 셋업 없이 메일 클라이언트로 발송 (백업)
# ─────────────────────────────────────────────────────────────
def build_mailto_url(to_addr: str, subject: str, body: str) -> str:
    """
    mailto: URL 생성. 사용자가 클릭하면 기본 메일 클라이언트가 열리고
    받는 사람/제목/본문이 미리 채워짐.

    주의: 대부분의 메일 클라이언트가 본문 ~2KB 까지 안정적. 그 이상은 잘릴 수 있음.
    """
    return (
        f"mailto:{quote(to_addr)}"
        f"?subject={quote(subject)}"
        f"&body={quote(body)}"
    )


# ─────────────────────────────────────────────────────────────
# SMTP 발송 (옵션 — 자동 발송용)
# ─────────────────────────────────────────────────────────────
def send_email(
    cfg: EmailConfig,
    to_addr: str,
    subject: str,
    html: str,
    plain: str,
) -> None:
    """
    SMTP로 이메일 발송. 포트 465 = SSL, 587 = STARTTLS.
    실패 시 예외 raise (UI에서 catch).
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.from_addr
    msg["To"] = to_addr
    msg.set_content(plain)
    msg.add_alternative(html, subtype="html")

    context = ssl.create_default_context()

    if cfg.smtp_port == 465:
        # SSL 직접
        with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port, context=context, timeout=20) as server:
            server.login(cfg.smtp_user, cfg.smtp_password)
            server.send_message(msg)
    else:
        # STARTTLS (587 등)
        with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=20) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(cfg.smtp_user, cfg.smtp_password)
            server.send_message(msg)
