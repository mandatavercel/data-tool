"""Hangang Brief — 종목 상세 페이지 상단 hero 리포트.

투자자가 "raw OHLCV 보다 여기서 받는 게 낫네" 라고 느끼게 만드는 자리.
실데이터로 계산된 수치(주간 수익률·벤치마크 대비·외국인 보유 변동 등) +
한국 시장 도메인 컨텍스트(거버넌스·치보 구조·membership)를 결합.

설계 원칙:
- 모든 숫자는 *실데이터*에서 계산. 모르는 값은 빈 칸 또는 "—" 로.
- 섹션마다 "데이터 출처" 명시 → 신뢰의 시그니처.
- 4섹션 고정:
    1) This week         — HF 관심사 (가격/플로우/이벤트)
    2) Valuation context — Fundamental 관심사 (밸류/피어/ADR)
    3) Catalysts ahead   — 일정 (HF 관심사)
    4) Structural notes  — 거버넌스/멤버십 (Fundamental 관심사)
- DART 또는 Anthropic 키 없는 섹션은 "needs connection" 배지.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import streamlit as st


# ──────────────────────────────────────────────────────────────────────
# 데이터 모델
# ──────────────────────────────────────────────────────────────────────

@dataclass
class BriefSection:
    heading: str
    lines: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    needs: list[str] = field(default_factory=list)   # ex: ["DART", "Anthropic"]


@dataclass
class Brief:
    ticker: str
    name: str
    generated_kst: str
    headline: str                 # 1-line TL;DR (italic serif)
    sections: list[BriefSection]


# ──────────────────────────────────────────────────────────────────────
# 빌더
# ──────────────────────────────────────────────────────────────────────

def generate_brief(
    sec: dict,
    ohlcv_df: pd.DataFrame,
    benchmark_df: Optional[pd.DataFrame] = None,
    fo_df: Optional[pd.DataFrame] = None,
    *,
    benchmark_label: str = "KOSPI",
) -> Brief:
    """종목 데이터를 받아 Brief 객체 반환.

    Parameters
    ----------
    sec          : ``data.lookup_security()`` 결과 dict
    ohlcv_df     : 종목 OHLCV (column: date, close, ...)
    benchmark_df : 벤치마크 인덱스 OHLCV (선택)
    fo_df        : 외국인 보유 시계열 (선택)
    """
    generated = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M KST")
    headline = _headline(sec, ohlcv_df, benchmark_df, benchmark_label)
    sections = [
        _section_this_week(sec, ohlcv_df, benchmark_df, fo_df, benchmark_label),
        _section_valuation(sec, ohlcv_df),
        _section_catalysts(sec),
        _section_structural(sec, fo_df),
    ]
    return Brief(
        ticker=sec.get("ticker", ""),
        name=sec.get("name_kr") or sec.get("name_en") or sec.get("ticker", ""),
        generated_kst=generated,
        headline=headline,
        sections=sections,
    )


# ─── 헤드라인 ─────────────────────────────────────────────────────────

def _headline(sec: dict, ohlcv: pd.DataFrame,
              bm: Optional[pd.DataFrame], bm_label: str) -> str:
    if ohlcv is None or ohlcv.empty:
        return f"{sec.get('name_kr','')}의 최근 마켓 흐름을 한눈에 보여드릴게요."

    last5 = ohlcv.tail(5)
    if len(last5) >= 2:
        wow = (last5["close"].iloc[-1] / last5["close"].iloc[0] - 1) * 100
    else:
        wow = 0.0
    direction = "rallied" if wow > 0.5 else "drifted lower" if wow < -0.5 else "treaded water"
    bm_phrase = ""
    if bm is not None and not bm.empty:
        # Align by tail
        bm_tail = bm.tail(5)
        if len(bm_tail) >= 2:
            bm_wow = (bm_tail["close"].iloc[-1] / bm_tail["close"].iloc[0] - 1) * 100
            spread = wow - bm_wow
            comp = "ahead of" if spread > 0 else "behind"
            bm_phrase = f", {abs(spread):.1f}pp {comp} {bm_label}"
    return f"{sec.get('name_kr','')} {direction} {abs(wow):.1f}% over 5 sessions{bm_phrase}."


# ─── 1) This week ─────────────────────────────────────────────────────

def _section_this_week(sec: dict, ohlcv: pd.DataFrame,
                       bm: Optional[pd.DataFrame],
                       fo: Optional[pd.DataFrame],
                       bm_label: str) -> BriefSection:
    s = BriefSection(heading="📅 This week")
    if ohlcv is None or ohlcv.empty:
        s.lines.append("최근 시세 데이터를 가져오지 못했어요.")
        return s

    last = ohlcv.tail(5)
    if len(last) >= 2:
        wow_pct = (last["close"].iloc[-1] / last["close"].iloc[0] - 1) * 100
        s.lines.append(
            f"5거래일 누적 **{_signed_pct(wow_pct)}**, "
            f"종가 **{last['close'].iloc[-1]:,.0f} KRW**."
        )

    if bm is not None and not bm.empty and len(bm) >= 2:
        bm_pct = (bm["close"].iloc[-1] / bm["close"].iloc[0] - 1) * 100
        wow_pct = (last["close"].iloc[-1] / last["close"].iloc[0] - 1) * 100
        spread = wow_pct - bm_pct
        s.lines.append(
            f"{bm_label} 대비 **{_signed_pp(spread)}** "
            f"({bm_label} {_signed_pct(bm_pct)})."
        )

    if "value" in ohlcv.columns:
        avg5 = ohlcv["value"].tail(5).mean()
        avg20 = ohlcv["value"].tail(20).mean() if len(ohlcv) >= 20 else avg5
        if avg20 > 0:
            ratio = avg5 / avg20
            if ratio > 1.3:
                s.lines.append(f"거래대금 5일 평균이 20일 평균의 **{ratio:.1f}배** — 관심 집중.")
            elif ratio < 0.7:
                s.lines.append(f"거래대금 5일 평균이 20일 평균의 **{ratio:.1f}배** — 관심 식음.")

    if fo is not None and not fo.empty and "foreign_pct" in fo.columns:
        fo_recent = fo.tail(5)
        if len(fo_recent) >= 2:
            delta = fo_recent["foreign_pct"].iloc[-1] - fo_recent["foreign_pct"].iloc[0]
            if abs(delta) >= 0.05:
                dir_word = "↑" if delta > 0 else "↓"
                s.lines.append(
                    f"외국인 지분율 5일간 **{dir_word} {abs(delta):.2f}pp** "
                    f"(현재 {fo_recent['foreign_pct'].iloc[-1]:.2f}%)."
                )

    s.sources = ["yfinance/pykrx (price)", "KRX (foreign ownership)"]
    s.needs = ["DART (material events)", "KIND (news catalysts)"]
    return s


# ─── 2) Valuation context ─────────────────────────────────────────────

def _section_valuation(sec: dict, ohlcv: pd.DataFrame) -> BriefSection:
    s = BriefSection(heading="📊 Valuation in context")

    last_close = None
    high_52w = low_52w = None
    if ohlcv is not None and not ohlcv.empty:
        last_close = ohlcv["close"].iloc[-1]
        recent_year = ohlcv.tail(252)
        high_52w = recent_year["high"].max() if "high" in recent_year.columns else recent_year["close"].max()
        low_52w = recent_year["low"].min() if "low" in recent_year.columns else recent_year["close"].min()

    if last_close is not None:
        s.lines.append(f"현재가 **{last_close:,.0f} KRW**.")
        if high_52w and low_52w and high_52w > low_52w:
            pos_in_range = (last_close - low_52w) / (high_52w - low_52w) * 100
            s.lines.append(
                f"52주 레인지 **{low_52w:,.0f}–{high_52w:,.0f}** 중 "
                f"**{pos_in_range:.0f}%** 위치."
            )

    sector = sec.get("sector_name_en") or ""
    if sector:
        s.lines.append(f"섹터: **{sector}** (GICS).")

    # ADR / GDR awareness (mandata_kr에 BBG ticker가 있으니 노출)
    bbg = sec.get("bloomberg") or ""
    if bbg:
        s.lines.append(f"Bloomberg: `{bbg}` · ISIN `{sec.get('isin') or '—'}` · RIC `{sec.get('ric') or '—'}`")

    s.sources = ["mandata_kr (sector/identifiers)", "yfinance (price)"]
    s.needs = ["FnGuide consensus (P/E, target price)", "DART (fundamentals)"]
    return s


# ─── 3) Catalysts ahead ──────────────────────────────────────────────

def _section_catalysts(sec: dict) -> BriefSection:
    s = BriefSection(heading="🎯 Catalysts ahead")
    s.lines.append(
        "*(데이터 소스 연결 후 — 어닝 콜, DART 정기보고 마감, "
        "MSCI/FTSE 리밸런스, BOK FOMC 등 자동 트리거.)*"
    )
    s.needs = ["DART (filing calendar)", "KRX (earnings calendar)", "Anthropic API (catalyst narrative)"]
    return s


# ─── 4) Structural notes ──────────────────────────────────────────────

def _section_structural(sec: dict, fo: Optional[pd.DataFrame]) -> BriefSection:
    s = BriefSection(heading="🏗 Structural notes")

    # Index membership
    mems = []
    if sec.get("kospi200"): mems.append("KOSPI 200")
    if sec.get("kosdaq150"): mems.append("KOSDAQ 150")
    if sec.get("krx300"): mems.append("KRX 300")
    if mems:
        s.lines.append("지수 편입: **" + " · ".join(mems) + "**")

    if sec.get("listing_date"):
        s.lines.append(f"상장일: **{sec['listing_date']}** ({sec.get('market') or 'KOSPI'})")

    if fo is not None and not fo.empty and "foreign_pct" in fo.columns:
        latest_fo = fo["foreign_pct"].iloc[-1]
        s.lines.append(f"외국인 지분율 **{latest_fo:.2f}%**")
        if "limit_exhausted_pct" in fo.columns:
            limit = fo["limit_exhausted_pct"].iloc[-1]
            if limit > 0:
                s.lines.append(f"외국인 한도 소진률 **{limit:.1f}%**")

    if sec.get("share_class"):
        s.lines.append(f"주식 분류: {sec['share_class']}")

    s.sources = ["mandata_kr (membership, listing, identifiers)"]
    s.needs = ["DART (chaebol cross-holdings)", "Custom KR research (governance score)"]
    return s


# ──────────────────────────────────────────────────────────────────────
# 헬퍼
# ──────────────────────────────────────────────────────────────────────

def _signed_pct(p: float) -> str:
    sign = "+" if p > 0 else ""
    return f"{sign}{p:.2f}%"


def _signed_pp(p: float) -> str:
    sign = "+" if p > 0 else ""
    return f"{sign}{p:.2f}pp"


# ──────────────────────────────────────────────────────────────────────
# Streamlit 렌더
# ──────────────────────────────────────────────────────────────────────

ACCENT = "#c96442"
ACCENT_50 = "#faefe9"
ACCENT_900 = "#6e3520"
EDGE = "#d6d6d0"
INK = "#111111"
MUTED = "#666666"
SOFT = "#888888"


def render_brief(brief: Brief) -> None:
    """Brief 객체를 Streamlit 컴포넌트로 렌더."""
    # 카드 컨테이너 (옅은 clay 배경 + 헤어라인)
    st.markdown(
        f"""
        <div style="
            background: {ACCENT_50};
            border: 1px solid #f5d3c2;
            border-radius: 12px;
            padding: 20px 22px 6px;
            margin: 0 0 14px;">
          <div style="display:flex; align-items:center; gap:8px;
                      font-size:11px; font-weight:600;
                      color:{ACCENT_900}; letter-spacing:.08em;
                      text-transform:uppercase; margin-bottom:6px;">
            ✨ Hangang Brief
            <span style="color:{ACCENT}; font-weight:400; text-transform:none;
                         letter-spacing:0; font-size:11px;">
              · {brief.generated_kst}
            </span>
          </div>
          <div style="
              font-family: ui-serif, Georgia, 'Source Serif 4', serif;
              font-style: italic; font-size:20px; line-height:1.3;
              color:{INK}; margin: 4px 0 16px;">
            "{brief.headline}"
          </div>
        """,
        unsafe_allow_html=True,
    )

    # 4 sections in 2x2 grid
    cols = st.columns(2)
    for i, sec in enumerate(brief.sections):
        with cols[i % 2]:
            _render_section(sec)

    # Footer: data lineage
    all_sources = sorted({src for s in brief.sections for src in s.sources})
    all_needs = sorted({n for s in brief.sections for n in s.needs})
    src_line = " · ".join(all_sources) if all_sources else "—"
    need_line = " · ".join(all_needs) if all_needs else ""

    st.markdown(
        f"""
        <div style="border-top: 1px solid #e8d4c5; padding: 12px 0 4px;
                    font-size: 11px; color:{MUTED};
                    background: {ACCENT_50}; margin-top:-14px;
                    border-radius: 0 0 12px 12px;
                    padding-left: 22px; padding-right: 22px;
                    margin-bottom: 14px;">
          <b style="color:{ACCENT_900};">Live sources</b> · {src_line}<br>
          {'<b style="color:'+SOFT+';">Enhance with</b> · ' + need_line if need_line else ''}
        </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_section(s: BriefSection) -> None:
    """단일 섹션 렌더 — 헤딩 + 라인 + 미니 needs 배지."""
    if not s.lines and not s.needs:
        return
    body_html = ""
    for line in s.lines:
        body_html += f'<div style="font-size:13px; color:#222; line-height:1.6; margin:2px 0;">{line}</div>'

    st.markdown(
        f"""
        <div style="background:white; border:1px solid #f0e2d6;
                    border-radius:8px; padding:14px 16px; margin-bottom:10px;">
          <div style="font-size:12px; font-weight:600; color:{INK};
                      margin-bottom:8px;">{s.heading}</div>
          {body_html}
        </div>
        """,
        unsafe_allow_html=True,
    )
