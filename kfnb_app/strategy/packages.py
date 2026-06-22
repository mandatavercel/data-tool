"""
kfnb_app/strategy/packages.py — Korea F&B Consumption Intelligence 상품 카탈로그.

데이터 결핍을 '약점'이 아니라 '목적별 깊이 설계(Breadth·Investability·Granularity)'로
포지셔닝하는 3-티어 구조. 데이터셋 제작 이전에 "무엇을 팔 것인가"를 고정한다.

  레이어(빌딩블록)
    L1 Company Monitor   — 전체 회사 매출(시장 지도)         [Breadth]
    L2 Brand Tracker     — 시총 상위 20사 × 대표 브랜드 5개  [Investability]
    L3 Category DeepDive — 라면·주류 SKU 레벨(알파)          [Granularity]
    L4 Trade Add-on      — HS코드 기반 수출입 모멘텀(보조)

  판매 패키지
    Basic        = L1
    Professional = L1 + L2      (대표 상품)
    Premium      = L1 + L2 + L3 + L4(옵션)  (프리미엄 업셀)

순수 데이터(상수). streamlit 비의존.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


SUITE_NAME = "Korea F&B Consumption Intelligence"
SUITE_TAGLINE = "Breadth · Investability · Granularity"


@dataclass(frozen=True)
class Layer:
    code: str
    name: str
    value_pillar: str           # Breadth / Investability / Granularity
    unit: str                   # 분석 단위
    coverage: str
    grain: str
    use_cases: tuple
    limits: str
    pitch_en: str


LAYERS: dict[str, Layer] = {
    "L1": Layer(
        code="L1", name="Company Monitor", value_pillar="Breadth",
        unit="회사 전체 매출", coverage="F&B 섹터 전체(상장+주요 비상장)",
        grain="company × month",
        use_cases=("섹터 모멘텀 모니터링", "peer comparison", "시장 점유율 추정",
                   "투자 아이디어 스크리닝"),
        limits="브랜드/SKU 단위 분석 불가",
        pitch_en=("Broad coverage of Korea's listed and major F&B companies, "
                  "letting investors monitor sector-wide consumption momentum and "
                  "company-level relative performance.")),
    "L2": Layer(
        code="L2", name="Brand Tracker", value_pillar="Investability",
        unit="회사 × 대표 브랜드 5개", coverage="시총 상위 20개 상장사",
        grain="company × brand × month",
        use_cases=("상장사 실적 예측", "브랜드 모멘텀", "핵심 제품군 추적",
                   "롱숏 아이디어"),
        limits="브랜드 선정 기준(매출기여) 명시 필요",
        pitch_en=("For the most investable listed names, we track the momentum of "
                  "their revenue-driving brands — the layer closest to an equity "
                  "investment decision.")),
    "L3": Layer(
        code="L3", name="Category Deep Dive", value_pillar="Granularity",
        unit="SKU", coverage="고신호 카테고리(라면·주류)",
        grain="sku × month (sales/qty/ASP)",
        use_cases=("제품단위 경쟁력", "가격·믹스 변화", "신제품 반응 조기탐지",
                   "실적 서프라이즈 조기신호"),
        limits="카테고리 커버리지 제한적(전체 F&B 주장 불가)",
        pitch_en=("For selected high-signal categories we provide SKU-level "
                  "granularity to capture product-level competition, price/mix "
                  "changes, and early signs of brand momentum.")),
    "L4": Layer(
        code="L4", name="Trade Momentum Add-on", value_pillar="External demand",
        unit="HS코드 수출입", coverage="라면·과자·소스·김치·음료·주류 HS코드",
        grain="hs6/hs10 × country × month",
        use_cases=("국내+해외 수요 동시확인", "수출주 모멘텀", "수입맥주 흐름"),
        limits="브랜드/SKU 직접매핑 아님 — 'HS코드 카테고리 모멘텀'으로 표현",
        pitch_en=("Selected HS-code based export/import indicators for key K-food "
                  "categories (ramen, snacks, sauces, kimchi, beverages, alcohol).")),
}


@dataclass(frozen=True)
class Package:
    tier: str                   # Basic / Professional / Premium
    name: str
    layers: tuple               # 포함 레이어 코드
    optional_layers: tuple
    target: str                 # 타깃 고객
    positioning: str            # 가격/포지션
    asp_analysis: str           # 포함/제한적/-
    newproduct_analysis: str
    pitch_en: str


PACKAGES: list[Package] = [
    Package(
        tier="Basic", name="Korea F&B Sector Monitor",
        layers=("L1",), optional_layers=("L4",),
        target="한국 F&B를 처음 테스트하는 신규 고객",
        positioning="엔트리(최저가) — 커버리지 신뢰도 베이스",
        asp_analysis="-", newproduct_analysis="-",
        pitch_en="Entry layer for sector-wide monitoring and peer screening."),
    Package(
        tier="Professional", name="Korea F&B Listed Leaders Tracker",
        layers=("L1", "L2"), optional_layers=("L4",),
        target="일반 롱온리/헤지펀드 (대표 고객)",
        positioning="대표 상품(Core) — 글로벌 IR 1순위 제안",
        asp_analysis="제한적", newproduct_analysis="제한적",
        pitch_en=("Core package: sector breadth plus brand-level tracking of the "
                  "most investable listed leaders.")),
    Package(
        tier="Premium", name="Korea F&B Category Alpha",
        layers=("L1", "L2", "L3"), optional_layers=("L4",),
        target="알파를 찾는 헤지펀드/섹터 전문 투자자",
        positioning="프리미엄 업셀 — 차별화 최대",
        asp_analysis="포함", newproduct_analysis="포함",
        pitch_en=("Premium: adds SKU-level deep dive in ramen & alcohol plus "
                  "HS-code trade momentum for alpha and earnings prediction.")),
]


# ── 브랜드 선정 기준(IR 대응) ─────────────────────────────────────────────────
BRAND_SELECTION_CRITERIA = [
    ("1순위", "회사 내 매출 기여도가 큰 브랜드"),
    ("2순위", "POS/편의점/마트에서 식별 가능한 브랜드"),
    ("3순위", "상장사 실적과 연결 가능한 브랜드"),
    ("4순위", "해외 투자자가 이해하기 쉬운 대표 브랜드"),
    ("제외", "너무 작은 브랜드, 외식/비유통 브랜드, 식별 어려운 브랜드"),
]


# ── 분석 질문 → 필요한 레이어/패키지 매핑 ─────────────────────────────────────
QUESTION_TO_PRODUCT = [
    ("한국 F&B 소비가 전체적으로 좋은가?", "L1", "Basic"),
    ("어떤 회사가 상대적으로 강한가?", "L1+L2", "Professional"),
    ("특정 회사의 핵심 브랜드가 좋아지나?", "L2", "Professional"),
    ("삼양/농심/오뚜기 라면 경쟁은?", "L3(라면)", "Premium"),
    ("하이트진로/롯데칠성/OB 주류 경쟁은?", "L3(주류)", "Premium"),
    ("가격 인상 효과가 보이나?", "L3 ASP", "Premium"),
    ("신제품 반응이 빠르게 보이나?", "L3", "Premium"),
    ("수출주 모멘텀과 국내 소비가 같이 가나?", "L3+L4", "Premium"),
]


def package_matrix() -> pd.DataFrame:
    """고객 제안용 패키지 비교 매트릭스 (Basic/Professional/Premium)."""
    rows = [
        ("전체 회사 매출 (L1 Company Monitor)", "포함", "포함", "포함"),
        ("시총 상위 20개사 (L2)", "-", "포함", "포함"),
        ("회사별 대표 브랜드 5개 (L2)", "-", "포함", "포함"),
        ("라면 SKU (L3)", "-", "-", "포함"),
        ("주류 SKU (L3)", "-", "-", "포함"),
        ("ASP 분석", "-", "제한적", "포함"),
        ("신제품/제품믹스 분석", "-", "제한적", "포함"),
        ("HS 수출입 모멘텀 (L4)", "옵션", "옵션", "포함/옵션"),
        ("주요 용도", "시장 모니터링", "상장사 분석", "알파/실적 예측"),
        ("타깃 고객", "신규 고객", "일반 투자기관", "헤지펀드/섹터 전문"),
    ]
    return pd.DataFrame(rows, columns=["항목", "Basic", "Professional", "Premium"])


def layer_table() -> pd.DataFrame:
    rows = [{"레이어": l.code, "상품": l.name, "가치": l.value_pillar,
             "단위": l.unit, "커버리지": l.coverage, "한계": l.limits}
            for l in LAYERS.values()]
    return pd.DataFrame(rows)


def question_table() -> pd.DataFrame:
    return pd.DataFrame(QUESTION_TO_PRODUCT,
                        columns=["분석 질문", "필요 레이어", "최소 패키지"])
