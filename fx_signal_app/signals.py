"""
FX Signal — 단기·중기 환전 신호 계산.

부호 규약 (사용자 관점: USD → KRW 환전):
  점수 > 0  : USD/KRW 상승 압력 → 기다림(WAIT)이 유리   (앞으로 USD가 더 비싸질 듯)
  점수 < 0  : USD/KRW 하락 압력 → 지금 환전(CONVERT NOW)이 유리
  점수 ≈ 0  : 중립 / DCA

호라이즌:
  단기 (1~2주)  : 모멘텀·기술적 + 단기 매크로 변화
  중기 (1~3개월): 추세 추종 + 중기 매크로 누적 변화

판정 기준:
  |score| ≥ 35 : 강한 신호
  20 ≤ |score| < 35 : 약한 신호
  |score| < 20 : 중립
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from .data import SeriesSnapshot, USDKRW_SIGN


# ─────────────────────────────────────────────────────────────
# 데이터 클래스
# ─────────────────────────────────────────────────────────────
@dataclass
class SignalComponent:
    """단일 신호 컴포넌트의 기여도와 설명."""
    name: str
    weight: float          # 명목 최대 기여 (참고용)
    value: float           # 실제 점수 기여 (-가중치 ~ +가중치)
    detail: str            # 사람이 읽을 설명 ("DXY 5일 +1.2% → +15")


@dataclass
class SignalResult:
    horizon: str               # "단기" / "중기"
    horizon_desc: str          # "1~2주" / "1~3개월"
    score: float               # -100 ~ +100 (음수 = 지금 환전, 양수 = 대기)
    verdict: str               # "지금 환전" / "약한 환전 신호" / "중립" / "약한 대기 신호" / "기다림"
    verdict_color: str         # CSS color (#hex)
    verdict_emoji: str         # 🟢 / 🟡 / 🔴
    components: list[SignalComponent] = field(default_factory=list)

    @property
    def is_convert(self) -> bool:
        return self.score <= -20

    @property
    def is_wait(self) -> bool:
        return self.score >= 20


@dataclass
class CombinedVerdict:
    """단기 + 중기 를 종합한 최종 환전 권고."""
    headline: str        # "지금 즉시 환전 권장" / "분할 환전 시작" / "중립 — 필요 만큼만" / "환전 일부 보류" / "환전 대기"
    detail: str          # 한 줄 근거 ("단기 -42, 중기 -18 — 둘 다 환전 우호적")
    color: str           # #hex
    emoji: str           # 🟢 / 🟡 / ⚪ / 🔴
    action: str          # "큰 비중 환전" / "일부 환전" / "DCA / 필요 만큼만" / "필수 자금만" / "보류"


@dataclass
class DriverItem:
    """"지금 왜 USD/KRW가 오르는지/떨어지는지"의 한 요인."""
    label: str         # "단기 · DXY 5일" 또는 "중기 · 60MA / 200MA"
    detail: str        # "DXY 5일 +0.30%"
    contribution: float  # 점수 기여 (부호 보존)


@dataclass
class MarketNarrative:
    """"지금 USD/KRW가 왜 오르는지/떨어지는지" 한눈 요약."""
    up_drivers: list[DriverItem]    # USD/KRW 끌어올리는 (양수 기여) 요인 TOP N
    down_drivers: list[DriverItem]  # USD/KRW 끌어내리는 (음수 기여) 요인 TOP N
    summary: str                     # 한 줄 자연어 요약
    net_score: float                 # 모든 컴포넌트 점수 합 (-200 ~ +200)


# ─────────────────────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────────────────────
def _rsi(s: pd.Series, n: int = 14) -> float:
    """RSI(14). NaN 안전. 빈/짧으면 50."""
    s = s.dropna()
    if len(s) < n + 1:
        return 50.0
    delta = s.diff().dropna()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ru = up.rolling(n).mean().iloc[-1]
    rd = down.rolling(n).mean().iloc[-1]
    if rd == 0 or np.isnan(rd):
        return 100.0 if ru > 0 else 50.0
    rs = ru / rd
    return float(100 - (100 / (1 + rs)))


def _clip(x: float, lo: float, hi: float) -> float:
    if np.isnan(x):
        return 0.0
    return max(lo, min(hi, x))


def _verdict(score: float) -> tuple[str, str, str]:
    """(verdict, color, emoji) 매핑."""
    if score <= -35:
        return ("지금 환전 권장", "#22C55E", "🟢")     # green
    if score <= -20:
        return ("약한 환전 신호", "#84CC16", "🟢")     # lime
    if score < 20:
        return ("중립", "#94A3B8", "⚪")              # slate
    if score < 35:
        return ("약한 대기 신호", "#F59E0B", "🟡")     # amber
    return ("환전 대기 권장", "#EF4444", "🔴")          # red


# ─────────────────────────────────────────────────────────────
# 단기 신호 (1~2주)
#   비중 — 총합 명목 ±100
#     USDKRW RSI(14) mean reversion        : ±25
#     USDKRW 5일 모멘텀 (추세)              : ±15
#     DXY 5일 모멘텀                        : ±15
#     UST10Y 5일 변화 (절대 bp)             : ±10
#     KOSPI 5일 모멘텀 (역방향)             : ±10
#     CNY 5일 모멘텀                        : ±10
#     USDKRW vs 20MA 위치                   : ±15
# ─────────────────────────────────────────────────────────────
def compute_short_term(snaps: dict[str, SeriesSnapshot]) -> SignalResult:
    comps: list[SignalComponent] = []
    score = 0.0

    # --- USDKRW RSI (mean reversion) ---
    usd = snaps.get("USDKRW")
    if usd is not None:
        rsi = _rsi(usd.series, 14)
        # RSI > 70 (과매수): 단기 하락 압력 = -25 (지금 환전)
        # RSI < 30 (과매도): 단기 상승 압력 = +25 (대기)
        if rsi >= 70:
            v = -25.0 * ((rsi - 70) / 30 + 1.0) / 2.0  # 70→-12.5, 100→-25
            v = _clip(v, -25, 0)
            detail = f"USD/KRW RSI(14) = {rsi:.0f} · 과매수 → 단기 조정 압력"
        elif rsi <= 30:
            v = 25.0 * ((30 - rsi) / 30 + 1.0) / 2.0
            v = _clip(v, 0, 25)
            detail = f"USD/KRW RSI(14) = {rsi:.0f} · 과매도 → 단기 반등 압력"
        else:
            # 중립대: 50 기준으로 -25~+25 선형 (영향력 약함)
            v = (50 - rsi) * (25 / 50) * 0.4  # 절대값 약화
            v = _clip(v, -10, 10)
            detail = f"USD/KRW RSI(14) = {rsi:.0f} · 중립"
        comps.append(SignalComponent("USD/KRW RSI(14)", 25, v, detail))
        score += v

    # --- USDKRW 5일 모멘텀 (단기 추세) ---
    if usd is not None and not np.isnan(usd.pct_5d):
        # +1% → +5, +3%+ → +15 (clipped)
        v = _clip(usd.pct_5d * 5.0, -15, 15)
        detail = f"USD/KRW 5일 모멘텀 {usd.pct_5d:+.2f}%"
        comps.append(SignalComponent("USD/KRW 5일 모멘텀", 15, v, detail))
        score += v

    # --- DXY 5일 모멘텀 ---
    dxy = snaps.get("DXY")
    if dxy is not None and not np.isnan(dxy.pct_5d):
        v = _clip(dxy.pct_5d * 15.0, -15, 15) * USDKRW_SIGN["DXY"]
        detail = f"DXY 5일 {dxy.pct_5d:+.2f}%"
        comps.append(SignalComponent("DXY 5일", 15, v, detail))
        score += v

    # --- UST10Y 5일 변화 (절대 변화 in %) ---
    ust = snaps.get("UST10Y")
    if ust is not None and len(ust.series) >= 6:
        chg = ust.last - float(ust.series.iloc[-6])  # 5 영업일 전과 비교
        # +0.10% (10bp) → +10
        v = _clip(chg * 100.0, -10, 10) * USDKRW_SIGN["UST10Y"]
        detail = f"미국 10Y 5일 변화 {chg*100:+.0f}bp ({ust.last:.2f}%)"
        comps.append(SignalComponent("US 10Y 5일 변화", 10, v, detail))
        score += v

    # --- KOSPI 5일 모멘텀 (역방향) ---
    kospi = snaps.get("KOSPI")
    if kospi is not None and not np.isnan(kospi.pct_5d):
        v = _clip(kospi.pct_5d * 5.0, -10, 10) * USDKRW_SIGN["KOSPI"]
        detail = f"KOSPI 5일 {kospi.pct_5d:+.2f}% (역상관)"
        comps.append(SignalComponent("KOSPI 5일", 10, v, detail))
        score += v

    # --- CNY 5일 모멘텀 ---
    cny = snaps.get("CNY")
    if cny is not None and not np.isnan(cny.pct_5d):
        v = _clip(cny.pct_5d * 10.0, -10, 10) * USDKRW_SIGN["CNY"]
        detail = f"USD/CNY 5일 {cny.pct_5d:+.2f}% (위안 동조)"
        comps.append(SignalComponent("USD/CNY 5일", 10, v, detail))
        score += v

    # --- USDKRW vs 20MA 위치 ---
    if usd is not None and not np.isnan(usd.ma20) and usd.ma20 > 0:
        dev = (usd.last / usd.ma20 - 1.0) * 100.0  # %
        # 20MA 위 = 단기 추세 위 = 상승 압력 잔존
        # 너무 벗어나면 mean reversion 우려 → 부호 약화
        if abs(dev) <= 1.5:
            v = _clip(dev * 10.0, -15, 15)
        else:
            v = _clip(dev * 5.0, -15, 15)  # 큰 이탈 시 둔감
        detail = f"USD/KRW 20일 이평선 대비 {dev:+.2f}%"
        comps.append(SignalComponent("vs 20MA", 15, v, detail))
        score += v

    score = float(_clip(score, -100, 100))
    verdict, color, emoji = _verdict(score)

    return SignalResult(
        horizon="단기",
        horizon_desc="1~2주",
        score=score,
        verdict=verdict,
        verdict_color=color,
        verdict_emoji=emoji,
        components=comps,
    )


# ─────────────────────────────────────────────────────────────
# 중기 신호 (1~3개월)
#   비중 — 총합 명목 ±100
#     USDKRW vs 200MA 위치 (장기 추세)      : ±20
#     USDKRW 50MA / 200MA 골든·데드크로스    : ±15
#     DXY 60일 모멘텀                       : ±20
#     UST10Y 60일 변화                      : ±15
#     KOSPI 60일 모멘텀 (역)                : ±10
#     원유(WTI/Brent 평균) 60일             : ±10
#     CNY 60일 모멘텀                       : ±10
# ─────────────────────────────────────────────────────────────
def compute_mid_term(snaps: dict[str, SeriesSnapshot]) -> SignalResult:
    comps: list[SignalComponent] = []
    score = 0.0

    # --- USDKRW vs 200MA (장기 추세 위치) ---
    usd = snaps.get("USDKRW")
    if usd is not None and not np.isnan(usd.ma200) and usd.ma200 > 0:
        dev = (usd.last / usd.ma200 - 1.0) * 100.0
        v = _clip(dev * 4.0, -20, 20)  # 5% 이탈 = 최대치
        detail = f"USD/KRW 200일 이평선 대비 {dev:+.2f}%"
        comps.append(SignalComponent("vs 200MA", 20, v, detail))
        score += v

    # --- 50MA vs 200MA (cross) ---
    if usd is not None and not np.isnan(usd.ma60) and not np.isnan(usd.ma200) and usd.ma200 > 0:
        # ma60을 50MA 대신 사용 (가장 가까운 중기 평균)
        gap = (usd.ma60 / usd.ma200 - 1.0) * 100.0
        v = _clip(gap * 6.0, -15, 15)
        if gap > 0.5:
            label = f"60MA > 200MA · 골든크로스 ({gap:+.2f}%)"
        elif gap < -0.5:
            label = f"60MA < 200MA · 데드크로스 ({gap:+.2f}%)"
        else:
            label = f"60MA ≈ 200MA ({gap:+.2f}%)"
        comps.append(SignalComponent("60MA / 200MA", 15, v, label))
        score += v

    # --- DXY 60일 모멘텀 ---
    dxy = snaps.get("DXY")
    if dxy is not None and not np.isnan(dxy.pct_60d):
        v = _clip(dxy.pct_60d * 4.0, -20, 20) * USDKRW_SIGN["DXY"]
        detail = f"DXY 60일 {dxy.pct_60d:+.2f}%"
        comps.append(SignalComponent("DXY 60일", 20, v, detail))
        score += v

    # --- UST10Y 60일 변화 ---
    ust = snaps.get("UST10Y")
    if ust is not None and len(ust.series) >= 61:
        chg = ust.last - float(ust.series.iloc[-61])
        # +0.5% (50bp) → +15
        v = _clip(chg * 30.0, -15, 15) * USDKRW_SIGN["UST10Y"]
        detail = f"미국 10Y 60일 변화 {chg*100:+.0f}bp"
        comps.append(SignalComponent("US 10Y 60일", 15, v, detail))
        score += v

    # --- KOSPI 60일 모멘텀 (역방향) ---
    kospi = snaps.get("KOSPI")
    if kospi is not None and not np.isnan(kospi.pct_60d):
        v = _clip(kospi.pct_60d * 1.5, -10, 10) * USDKRW_SIGN["KOSPI"]
        detail = f"KOSPI 60일 {kospi.pct_60d:+.2f}% (역상관)"
        comps.append(SignalComponent("KOSPI 60일", 10, v, detail))
        score += v

    # --- 원유 60일 (WTI/Brent 평균) ---
    oil_vals = []
    for k in ("WTI", "BRENT"):
        snap = snaps.get(k)
        if snap is not None and not np.isnan(snap.pct_60d):
            oil_vals.append(snap.pct_60d)
    if oil_vals:
        avg_oil = float(np.mean(oil_vals))
        v = _clip(avg_oil * 1.0, -10, 10) * USDKRW_SIGN["BRENT"]
        detail = f"원유(WTI·Brent 평균) 60일 {avg_oil:+.2f}%"
        comps.append(SignalComponent("원유 60일", 10, v, detail))
        score += v

    # --- CNY 60일 모멘텀 ---
    cny = snaps.get("CNY")
    if cny is not None and not np.isnan(cny.pct_60d):
        v = _clip(cny.pct_60d * 4.0, -10, 10) * USDKRW_SIGN["CNY"]
        detail = f"USD/CNY 60일 {cny.pct_60d:+.2f}% (위안 동조)"
        comps.append(SignalComponent("USD/CNY 60일", 10, v, detail))
        score += v

    score = float(_clip(score, -100, 100))
    verdict, color, emoji = _verdict(score)

    return SignalResult(
        horizon="중기",
        horizon_desc="1~3개월",
        score=score,
        verdict=verdict,
        verdict_color=color,
        verdict_emoji=emoji,
        components=comps,
    )


# ─────────────────────────────────────────────────────────────
# 단기 + 중기 종합 판정
#
# 5-단계 매핑 (우선순위 순):
#
#   1. 둘 다 강한 환전(녹색)         → "🟢 지금 즉시 환전 권장"
#      short ≤ -35 AND mid ≤ -20         큰 비중 환전
#
#   2. 한쪽 환전 + 다른 쪽 환전/중립    → "🟢 분할 환전 시작"
#      (short ≤ -20 OR mid ≤ -20) AND   단계적 환전
#       반대편이 +20 미만
#
#   3. 둘 다 강한 대기(빨강)         → "🔴 환전 대기 권장"
#      short ≥ +35 AND mid ≥ +20         보류, 필수 자금만
#
#   4. 한쪽 대기 + 다른 쪽 대기/중립    → "🟡 환전 보류 (소량만)"
#      (short ≥ +20 OR mid ≥ +20) AND   필수 자금만 환전
#       반대편이 -20 초과
#
#   5. 그 외 (혼조 또는 중립)        → "⚪ 중립 — 필요 만큼만"
#      DCA 또는 자금 사정에 맞춰 분할
# ─────────────────────────────────────────────────────────────
def combined_verdict(short: SignalResult, mid: SignalResult) -> CombinedVerdict:
    s = short.score
    m = mid.score

    s_convert = s <= -20
    s_wait = s >= 20
    m_convert = m <= -20
    m_wait = m >= 20

    s_strong_convert = s <= -35
    m_strong_convert = m <= -20
    s_strong_wait = s >= 35
    m_strong_wait = m >= 20

    # 1) 둘 다 강한 환전
    if s_strong_convert and m_strong_convert:
        return CombinedVerdict(
            headline="지금 즉시 환전 권장",
            detail=f"단기 {s:+.0f} · 중기 {m:+.0f} — 두 호라이즌 모두 USD/KRW 하락 압력. 보유 USD의 큰 비중을 지금 환전.",
            color="#16A34A",
            emoji="🟢",
            action="큰 비중 환전 (예: 60~80%)",
        )

    # 2) 한쪽 환전 + 반대편 비대기
    if (s_convert or m_convert) and not (s_wait or m_wait):
        primary = "단기" if abs(s) > abs(m) and s_convert else ("중기" if m_convert else "단기")
        return CombinedVerdict(
            headline="분할 환전 시작",
            detail=f"단기 {s:+.0f} · 중기 {m:+.0f} — {primary}에서 환전 신호. 일부(예: 30~50%) 먼저 환전 후 추가 신호 대기.",
            color="#22C55E",
            emoji="🟢",
            action="일부 환전 (예: 30~50%)",
        )

    # 3) 둘 다 강한 대기
    if s_strong_wait and m_strong_wait:
        return CombinedVerdict(
            headline="환전 대기 권장",
            detail=f"단기 {s:+.0f} · 중기 {m:+.0f} — 두 호라이즌 모두 USD/KRW 상승 압력. 필수 자금 외엔 보류.",
            color="#DC2626",
            emoji="🔴",
            action="보류 (필수 자금만)",
        )

    # 4) 한쪽 대기 + 반대편 비환전
    if (s_wait or m_wait) and not (s_convert or m_convert):
        primary = "단기" if s_wait else "중기"
        return CombinedVerdict(
            headline="환전 보류 (소량만)",
            detail=f"단기 {s:+.0f} · 중기 {m:+.0f} — {primary} 상승 압력. 필수 자금만 환전하고 추가 신호 대기.",
            color="#F59E0B",
            emoji="🟡",
            action="필수 자금만 (예: 10~20%)",
        )

    # 5) 혼조 또는 중립 (단기·중기 부호가 충돌하거나 둘 다 중립)
    return CombinedVerdict(
        headline="중립 — 필요 만큼만 환전",
        detail=(
            f"단기 {s:+.0f} · 중기 {m:+.0f} — 신호가 불분명. "
            "월별 자금 수요에 맞춰 DCA(분할 매도)로 환전 시점 리스크를 분산."
        ),
        color="#94A3B8",
        emoji="⚪",
        action="DCA / 자금 사정에 맞춰",
    )


# ─────────────────────────────────────────────────────────────
# "지금 왜 오르는지/떨어지는지" — 자연어 요약
# ─────────────────────────────────────────────────────────────
def market_narrative(
    short: SignalResult,
    mid: SignalResult,
    max_each: int = 3,
    min_abs: float = 2.0,
) -> MarketNarrative:
    """
    단기·중기의 모든 컴포넌트를 모아 부호별로 정렬, 상위 요인을 추출.

    - up_drivers: 양수 기여 (USD/KRW 상승 압력) 상위 max_each
    - down_drivers: 음수 기여 (USD/KRW 하락 압력) 상위 max_each
    - summary: 한 줄 자연어
    """
    all_items: list[DriverItem] = []
    for c in short.components:
        if abs(c.value) >= min_abs:
            all_items.append(DriverItem(
                label=f"단기 · {c.name}",
                detail=c.detail,
                contribution=c.value,
            ))
    for c in mid.components:
        if abs(c.value) >= min_abs:
            all_items.append(DriverItem(
                label=f"중기 · {c.name}",
                detail=c.detail,
                contribution=c.value,
            ))

    ups = sorted([d for d in all_items if d.contribution > 0],
                 key=lambda x: -x.contribution)[:max_each]
    downs = sorted([d for d in all_items if d.contribution < 0],
                   key=lambda x: x.contribution)[:max_each]

    net = sum(d.contribution for d in all_items)

    # 한 줄 요약 — 두 큰 축을 한 문장에
    def _short(d: DriverItem) -> str:
        # "DXY 5일 +0.30%" 같은 detail 그대로 사용
        return d.detail

    if ups and downs:
        top_up = ups[0]
        top_down = downs[0]
        if net > 5:
            summary = (
                f"📈 USD/KRW에 <b>상승 압력</b>이 우세. "
                f"가장 큰 요인: <b>{_short(top_up)}</b>. "
                f"반대 방향: {_short(top_down)}."
            )
        elif net < -5:
            summary = (
                f"📉 USD/KRW에 <b>하락 압력</b>이 우세. "
                f"가장 큰 요인: <b>{_short(top_down)}</b>. "
                f"반대 방향: {_short(top_up)}."
            )
        else:
            summary = (
                f"⚖️ <b>방향성 혼조</b>. "
                f"오르는 힘({_short(top_up)})과 내리는 힘({_short(top_down)})이 균형."
            )
    elif ups and not downs:
        summary = (
            f"📈 USD/KRW에 <b>상승 압력만</b> 보이고 반대 힘이 약합니다. "
            f"핵심: <b>{_short(ups[0])}</b>."
        )
    elif downs and not ups:
        summary = (
            f"📉 USD/KRW에 <b>하락 압력만</b> 보이고 반대 힘이 약합니다. "
            f"핵심: <b>{_short(downs[0])}</b>."
        )
    else:
        summary = "⚪ 모든 매크로 지표가 잠잠한 구간. 단기적으로는 큰 방향성을 잡기 어렵습니다."

    return MarketNarrative(
        up_drivers=ups,
        down_drivers=downs,
        summary=summary,
        net_score=float(net),
    )
