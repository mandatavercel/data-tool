"""
FX Signal — 매크로 지표 → 자연어 설명 layer.

목적: "DXY 5일 +2.05%" 같이 전문 용어 + 숫자만 있으면 일반 사용자는 무슨 뜻인지 모름.
이 모듈은 각 컴포넌트를 "왜냐하면 ~ 그래서 USD/KRW에 어떻게 영향" 친화 한국어로 풀어줌.

규약:
- 각 컴포넌트 이름(SignalComponent.name) 별로 (up_text, down_text) 쌍.
  * up_text   : contribution > 0 일 때 (USD/KRW 상승 압력 = 환전 대기 유리)
  * down_text : contribution < 0 일 때 (USD/KRW 하락 압력 = 지금 환전 유리)
"""
from __future__ import annotations

from typing import Optional


# (down_text, up_text) — contribution 부호로 인덱스
# down_text: 음수 기여(USD/KRW 하락 압력) 시 설명
# up_text  : 양수 기여(USD/KRW 상승 압력) 시 설명
_EXPLAIN: dict[str, tuple[str, str]] = {
    # ─── USD/KRW 자체 기술적 ──────────────────────────────
    "USD/KRW RSI(14)": (
        "단기 과매수 구간이라 곧 식는 흐름이 자연스러워요. 환율이 잠시 내려갈 수 있습니다.",
        "단기 과매도 구간이라 곧 반등이 자연스러워요. 환율이 다시 오를 수 있습니다.",
    ),
    "USD/KRW 5일 모멘텀": (
        "최근 5일간 환율이 내려가는 흐름이에요. 단기 하락 추세가 이어질 가능성.",
        "최근 5일간 환율이 오르는 흐름이에요. 단기 상승 추세가 이어질 가능성.",
    ),
    "vs 20MA": (
        "단기 평균선 아래에 있어요. 단기적으로 약세 분위기.",
        "단기 평균선 위에 있어요. 단기 상승세가 유효합니다.",
    ),
    "vs 200MA": (
        "장기 평균선 아래에 있어요. 큰 그림에선 하락 사이클 진행 중.",
        "장기 평균선 위에 있어요. 큰 그림에선 여전히 상승 사이클입니다.",
    ),
    "60MA / 200MA": (
        "중기 평균이 장기 평균보다 낮아요(데드크로스 신호). 중기적 약세 확인.",
        "중기 평균이 장기 평균보다 높아요(골든크로스). 중기적 상승 추세 확인.",
    ),

    # ─── DXY (달러 인덱스) ───────────────────────────────
    "DXY 5일": (
        "달러가 최근 다른 통화에 비해 약해지고 있어요. 원화에도 한숨 돌릴 여유.",
        "달러가 최근 다른 통화에 비해 강해지고 있어요. 원화도 같이 끌려가 환율 상승 압력.",
    ),
    "DXY 60일": (
        "지난 두 달 달러가 다른 통화 대비 약세 추세. 원화엔 우호적.",
        "지난 두 달 달러가 다른 통화 대비 강세 추세. 원화 약세 압력이 누적되고 있어요.",
    ),

    # ─── 미국 10년물 국채금리 ────────────────────────────
    "US 10Y 5일 변화": (
        "미국 금리가 내리면 달러 예금/채권 매력이 줄어요. 자금이 미국에서 빠져나가 원화 강세에 우호적.",
        "미국 금리가 오르면 달러 예금/채권 매력이 커져요. 자금이 미국으로 몰리면서 원화에서 빠져나가 환율 상승 압력.",
    ),
    "US 10Y 60일": (
        "지난 두 달 미국 금리가 내리는 추세. 달러 매력 ↓ → 원화엔 우호적.",
        "지난 두 달 미국 금리가 오르는 추세. 달러 매력 ↑ → 원화 약세 압력 누적.",
    ),

    # ─── KOSPI ────────────────────────────────────────
    "KOSPI 5일": (
        "한국 증시가 단기 강세예요. 외국인이 KOSPI 사려면 원화를 사야 해서, 원화 수요 증가 → 환율 하락 압력.",
        "한국 증시가 단기 약세예요. 외국인 자금이 빠져나가면 원화 매도 → 환율 상승 압력.",
    ),
    "KOSPI 60일": (
        "지난 두 달 한국 증시가 강한 흐름. 외국인 자금 유입 추세 = 원화 강세 우호.",
        "지난 두 달 한국 증시가 약한 흐름. 외국인 자금 이탈 우려 = 원화 약세 압력.",
    ),

    # ─── 위안 (USD/CNY) ───────────────────────────────
    "USD/CNY 5일": (
        "위안화가 단기 강세예요. 원화도 같이 강세 동조하는 경향 → 환율 하락 우호.",
        "위안화가 단기 약세예요. 원화도 동조해서 약세로 끌려가는 경향 → 환율 상승 압력.",
    ),
    "USD/CNY 60일": (
        "지난 두 달 위안화가 강세 추세. 원화도 동조 강세 흐름.",
        "지난 두 달 위안화가 약세 추세. 원화도 동조 약세 흐름.",
    ),

    # ─── 원유 ──────────────────────────────────────────
    "원유 60일": (
        "유가가 내리면 한국이 수입대금을 덜 내요. 무역수지에 우호적 → 원화 강세 압력.",
        "유가가 오르면 한국이 수입대금을 더 내야 해요. 달러 수요 ↑ → 원화 약세 압력.",
    ),
}


def friendly_explanation(component_name: str, contribution: float) -> str:
    """
    매크로 컴포넌트 이름 + 기여 점수 부호로 친화 한국어 한 줄 설명 반환.
    매핑이 없거나 컨트리뷰션이 ~0이면 빈 문자열.
    """
    if abs(contribution) < 0.5:
        return ""
    pair = _EXPLAIN.get(component_name)
    if not pair:
        return ""
    down_text, up_text = pair
    return up_text if contribution > 0 else down_text


# ─────────────────────────────────────────────────────────────
# 종합 narrative — 친절한 문단형 설명
# ─────────────────────────────────────────────────────────────
def build_friendly_summary(
    net_score: float,
    top_up_name: Optional[str],
    top_up_explain: Optional[str],
    top_down_name: Optional[str],
    top_down_explain: Optional[str],
    upcoming_event_text: Optional[str] = None,
) -> str:
    """
    "지금 USD/KRW가 왜 오르고/떨어지는지" 친화 한국어 문단.
    HTML 호환 (br 태그 사용).
    """
    lines: list[str] = []

    if net_score > 5:
        lines.append(
            "📈 <b>지금은 USD/KRW가 오르는 쪽에 힘이 더 실리고 있어요.</b>"
        )
        if top_up_explain:
            lines.append(f"가장 큰 이유: {top_up_explain}")
        if top_down_explain:
            lines.append(
                f"반대로 작용하는 힘도 있긴 해요 — {top_down_explain}"
            )
        lines.append(
            "<i>→ 이런 상황에선 환전을 서두르지 말고 조금 더 기다려보는 게 유리합니다.</i>"
        )
    elif net_score < -5:
        lines.append(
            "📉 <b>지금은 USD/KRW가 내려가는 쪽에 힘이 더 실리고 있어요.</b>"
        )
        if top_down_explain:
            lines.append(f"가장 큰 이유: {top_down_explain}")
        if top_up_explain:
            lines.append(
                f"반대로 작용하는 힘도 있긴 해요 — {top_up_explain}"
            )
        lines.append(
            "<i>→ 이런 상황에선 더 기다리면 환율이 더 내려가서 환전 손해. 지금 환전이 유리합니다.</i>"
        )
    else:
        lines.append(
            "⚖️ <b>오르는 힘과 내리는 힘이 거의 비슷해서 방향성이 약해요.</b>"
        )
        if top_up_explain:
            lines.append(f"오르는 쪽: {top_up_explain}")
        if top_down_explain:
            lines.append(f"내리는 쪽: {top_down_explain}")
        lines.append(
            "<i>→ 이럴 땐 한 번에 환전하지 말고, 자금 필요할 때마다 나눠서 환전(DCA)하는 게 안전해요.</i>"
        )

    if upcoming_event_text:
        lines.append(
            f"<span style='color:rgba(245,158,11,0.95);'>👀 {upcoming_event_text}</span>"
        )

    return "<br>".join(lines)
