"""
Foundation — Schema Intelligence, Validation, Capability Map
"""
import re
import pandas as pd
import streamlit as st

ROLE_OPTIONS = [
    # ── 시간 ───────────────────────────────────────────────────
    "transaction_date",
    # ── 식별 (Identity) ────────────────────────────────────────
    "company_name", "brand_name", "sku_name",
    # ── 카테고리 계층 (Hierarchy) ─────────────────────────────
    "category_large", "category_medium", "category_small",
    "category_name",          # 단일 카테고리 컬럼 (계층 없음)
    # ── 매출 메트릭 ─────────────────────────────────────────────
    "sales_amount",           # 거래금액
    "sales_quantity",         # 거래량 (수량)
    "sales_count",            # 거래건수 (결제·주문 건수)
    "unit_price",             # 단가
    # ── 이용자 메트릭 (신규 — 거래건수와 의미가 다름) ──────────
    "active_users",           # 활성 이용자수 (DAU/MAU/방문자/유저)
    # ── 호환용 alias (기존 모듈 호환 — 사용자 selectbox에선 숨김) ─
    "quantity", "number_of_tx",
    # ── 데모그래픽 ──────────────────────────────────────────────
    "gender", "age_group", "region",
    # ── 채널·점포·고객 ──────────────────────────────────────────
    "channel", "store_id", "customer_id",
    # ── 리텐션 ──────────────────────────────────────────────────
    "retention_flag",
    # ── 종목 매핑 (Stock Mapping) ──────────────────────────────
    "stock_code", "security_code",
    # ── Fallback ───────────────────────────────────────────────
    "unknown",
]

# 백엔드 alias 전용 — 사용자가 직접 선택할 필요 없는 role (selectbox에서 숨김).
# sales_quantity → quantity, sales_count → number_of_tx 자동 alias 등록되므로
# 사용자는 sales_quantity / sales_count만 선택하면 충분.
_HIDDEN_ALIAS_ROLES = {"quantity", "number_of_tx"}

# Alias → 사용자 노출 부모 role 매핑 (단일 출처 — UI 정규화 전용).
# step2_schema·foundation의 selectbox 둘 다 이 dict를 import해서 사용.
ALIAS_TO_PARENT = {
    "quantity":      "sales_quantity",
    "number_of_tx":  "sales_count",
}


def user_role_options() -> list[str]:
    """사용자 selectbox에 표시할 role 목록 — 백엔드 alias 숨김."""
    return [r for r in ROLE_OPTIONS if r not in _HIDDEN_ALIAS_ROLES]


def normalize_to_user_role(role: str | None, opts: list[str] | None = None) -> str:
    """alias·invalid role을 사용자 노출 옵션 안 값으로 정규화.

    사용처: step2_schema, foundation의 selectbox 직전 정규화.
    옛 session_state가 'quantity' 같은 alias를 들고 있어도 안전하게 'sales_quantity'로.
    옵션 밖이면 'unknown' 반환.
    """
    if role is None:
        return "unknown"
    v = ALIAS_TO_PARENT.get(role, role)
    if opts is None:
        opts = user_role_options()
    return v if v in opts else "unknown"


# UI selectbox에 표시할 한국어 라벨 (내부 값은 영문 유지)
# 사용자 명시 라벨: 거래일자, ISIN, 회사명, 종목코드(법인등록번호), 브랜드, SKU,
#                 카테고리 대/중/소, 거래금액, 거래수량, 거래건수
ROLE_LABEL = {
    # ── 시간 ─────────────────────────
    "transaction_date":  "📅 거래일자",
    # ── 식별 ─────────────────────────
    "company_name":      "🏢 회사명",
    "brand_name":        "🏷 브랜드",
    "sku_name":          "📦 SKU",
    # ── 카테고리 계층 ────────────────
    "category_large":    "🗂 카테고리 (대)",
    "category_medium":   "🗃 카테고리 (중)",
    "category_small":    "🗄 카테고리 (소)",
    "category_name":     "🗂 카테고리 (단일)",
    # ── 매출 메트릭 ──────────────────
    "sales_amount":      "💰 거래금액",
    "sales_quantity":    "🔢 거래수량",
    "sales_count":       "🧾 거래건수",
    "unit_price":        "🏷 단가",
    "active_users":      "👥 이용자수 (DAU·MAU·방문자)",
    # ── 백엔드 alias (사용자 selectbox에선 숨김 — 옛 session_state 표시용) ─
    "quantity":          "🔢 거래수량 (sales_quantity와 동일)",
    "number_of_tx":      "🧾 거래건수 (sales_count와 동일)",
    # ── 데모그래픽 ───────────────────
    "gender":            "🚻 성별",
    "age_group":         "🎂 연령대",
    "region":            "📍 지역",
    # ── 채널·점포·고객 ───────────────
    "channel":           "🛒 판매채널",
    "store_id":          "🏪 점포 ID",
    "customer_id":       "👤 고객 ID",
    # ── 리텐션 ───────────────────────
    "retention_flag":    "🔁 리텐션 (신규/재방문)",
    # ── 종목 매핑 ───────────────────
    "stock_code":        "📈 종목코드 (법인등록번호)",
    "security_code":     "🆔 ISIN",
    # ── 미지정 ──────────────────────
    "unknown":           "— 미지정 / 사용 안 함",
}


# UI 카드 색상 (role 그룹별)
ROLE_COLOR = {
    # 시간
    "transaction_date":  "#dbeafe",
    # 매출 메트릭 (그린)
    "sales_amount":      "#dcfce7",
    "sales_quantity":    "#dcfce7",
    "sales_count":       "#dcfce7",
    "unit_price":        "#dcfce7",
    "quantity":          "#d1fae5",
    "number_of_tx":      "#d1fae5",
    "active_users":      "#bae6fd",   # 이용자는 청록 — 매출(green)과 시각 구분
    # 식별 (노랑)
    "company_name":      "#fef9c3",
    "brand_name":        "#fef3c7",
    "sku_name":          "#ffedd5",
    # 카테고리 (오렌지)
    "category_large":    "#ffedd5",
    "category_medium":   "#fed7aa",
    "category_small":    "#fdba74",
    "category_name":     "#ffedd5",
    # 데모그래픽 (보라)
    "gender":            "#f3e8ff",
    "age_group":         "#e9d5ff",
    "region":            "#ddd6fe",
    # 채널·점포·고객 (티얼)
    "channel":           "#ccfbf1",
    "store_id":          "#99f6e4",
    "customer_id":       "#a5f3fc",
    # 리텐션 (핑크)
    "retention_flag":    "#fce7f3",
    # 종목 매핑 (인디고)
    "stock_code":        "#e0e7ff",
    "security_code":     "#c7d2fe",
    # 미지정
    "unknown":           "#f3f4f6",
}


def role_label(role: str) -> str:
    """role 코드를 한국어 라벨로 변환 (selectbox format_func에 사용)."""
    return ROLE_LABEL.get(role, role)


# 역할별 → 이 역할로 매핑하면 어떤 분석에 활용되는지
# 구조: {role: {"what": 한 줄 의미, "for": "이 역할로 매핑하면 가능해지는 분석"}}
# 사용자 관점: 매핑 결정을 도울 정보 (내부 구현 디테일 제외)
ROLE_DESCRIPTION: dict[str, dict[str, str]] = {
    # ── 시간 ────────────────────────────────────────────────────────────
    "transaction_date": {
        "what": "거래가 발생한 날짜 (YYYY-MM-DD 또는 YYYYMMDD).",
        "for":  "📈 시계열 분석 전부 활성화 — 매출 성장률(MoM·QoQ·YoY), 이상치 탐지, "
                "주가와의 시차 상관(Market Signal), 분기 집계(Earnings Intel). "
                "이 컬럼이 없으면 시간 기반 모듈 전체 비활성.",
    },
    # ── 식별 ────────────────────────────────────────────────────────────
    "company_name": {
        "what": "회사명 (거래/매출 데이터의 어느 상장사 매출인지).",
        "for":  "🏢 회사별 분석이 가능해집니다 — 주가 상관성, 공시 매출 매칭(DART), "
                "Long/Short 후보 종목 추출, Investor Dashboard 매출 Top 5, "
                "Factor Research universe 정의.",
    },
    "brand_name": {
        "what": "브랜드명 (한 회사 내 여러 브랜드 운영 시).",
        "for":  "🏷 브랜드별 매출·점유율 분해 — Brand Intelligence(HHI·MoM 추이), "
                "Investor Dashboard 브랜드 Top, 신규 브랜드 침투율 추적, "
                "프리미엄화 추세 검증.",
    },
    "sku_name": {
        "what": "SKU·상품명 (가장 세분화된 매출 단위).",
        "for":  "📦 상품별 분석 가능 — Pareto 80/20 분석(상위 20% SKU가 매출의 몇 %?), "
                "신제품 침투율, lifecycle(도입·성장·성숙·쇠퇴), "
                "Investor Dashboard SKU Top 성장률.",
    },
    # ── 카테고리 계층 ──────────────────────────────────────────────────
    "category_large": {
        "what": "카테고리 대분류 (예: 식품 / 음료 / 스낵 / 생활용품).",
        "for":  "🗂 Category Intelligence 활성화 — 대분류별 매출 점유율, MoM 성장률, "
                "월별 추이 비교. 중/소분류와 함께 매핑하면 자동 drill-down(sunburst chart).",
    },
    "category_medium": {
        "what": "카테고리 중분류 (예: 라면 / 김밥 / 과자 / 우유).",
        "for":  "🗃 카테고리 분석의 가장 정보성 높은 레벨로 자동 선택. "
                "대분류와 함께 매핑하면 '식품 → 라면 → 신라면' 계층 breakdown 가능.",
    },
    "category_small": {
        "what": "카테고리 소분류 (예: 컵라면 / 봉지라면 / 비빔라면).",
        "for":  "🗄 최종 drill-down 레벨 — SKU와 묶어 세부 카테고리 침투율 분석. "
                "신제품이 어떤 소분류에 영향을 주는지 정량 추적.",
    },
    "category_name": {
        "what": "단일 카테고리 (계층 구분 없을 때).",
        "for":  "🗂 Category Intelligence (단일 차원). 매출 점유율 + 성장률 + 월별 추이. "
                "대/중/소 계층 매핑 권장.",
    },
    # ── 매출 메트릭 ────────────────────────────────────────────────────
    "sales_amount": {
        "what": "거래 금액 (원). 매출 분석의 핵심 메트릭.",
        "for":  "💰 모든 분석의 입력값 — 회사·브랜드·SKU·카테고리별 매출 집계, "
                "성장률(MoM/QoQ/YoY) 계산, 주가·공시 상관 분석. "
                "이 컬럼 없으면 어떤 분석도 불가.",
    },
    "sales_quantity": {
        "what": "거래 수량 (몇 개 팔렸나).",
        "for":  "🔢 Demand Intelligence의 P/Q 분해가 가능해집니다 — "
                "매출 증가가 '가격 상승' 때문인지 '판매량 증가' 때문인지 분리해서 "
                "성장 동인 정량 파악. 프리미엄화 vs 볼륨 확대 식별.",
    },
    "sales_count": {
        "what": "거래 건수 (몇 번의 결제·주문이 발생했나). 한 명이 여러 번 거래 가능.",
        "for":  "🧾 거래 빈도 분석 — Demand Intelligence(건수 vs 객단가 분해), "
                "Anomaly Detection(평소 대비 거래량 급증·급감 자동 감지). "
                "💡 active_users도 함께 매핑되면 자동으로 '결제 빈도'(거래건수/이용자수) 파생.",
    },
    "active_users": {
        "what": "활성 이용자수 — 해당 기간 서비스를 이용한 unique 사용자 수 "
                "(DAU·MAU·방문자수·구독자수 등). 거래건수와 다름: "
                "한 명이 여러 번 거래해도 1명으로 카운트.",
        "for":  "👥 이용자 기반 분석 — ARPU(매출/이용자수, 1인당 매출력), "
                "Penetration(이용자 성장 vs 매출 성장 — 어느게 driver?), "
                "결제 빈도(거래건수/이용자수, 1인당 결제 횟수). "
                "💡 sales_amount와 함께 매핑되면 ARPU 자동 계산. "
                "💡 핀테크·구독·모바일앱·SaaS·이커머스 분석의 핵심 변수.",
    },
    "unit_price": {
        "what": "단가 / 객단가 (ATV — Average Transaction Value).",
        "for":  "🏷 Demand Intelligence(가격 동인 분석), "
                "브랜드 프리미엄화 추세 검증 (ATV↑면 mix 효과 좋음), "
                "할인·프로모션 효과 측정.",
    },
    # ── 자동 등록 (사용자가 직접 매핑할 필요 없음) ─────────────────────
    "quantity": {
        "what": "거래 수량과 동일 (자동 등록되므로 직접 선택 불필요).",
        "for":  "⚙️ sales_quantity 매핑 시 자동으로 같은 컬럼에 등록됩니다. "
                "신경 쓰지 마세요 — 사용자는 sales_quantity만 매핑하면 됩니다.",
    },
    "number_of_tx": {
        "what": "거래 건수와 동일 (자동 등록되므로 직접 선택 불필요).",
        "for":  "⚙️ sales_count 매핑 시 자동으로 같은 컬럼에 등록됩니다. "
                "신경 쓰지 마세요.",
    },
    # ── 데모그래픽 ─────────────────────────────────────────────────────
    "gender": {
        "what": "고객 성별 (M/F · 남/여).",
        "for":  "🚻 성별 매출 분포 분석 — Investor Dashboard에 성별 mix 차트 표시. "
                "어느 회사·브랜드가 어느 성별 의존도 높은지 정량 확인. "
                "ESG·소비 트렌드 보조 시그널.",
    },
    "age_group": {
        "what": "고객 연령대 (20대 / 30대 / 25-29 등).",
        "for":  "🎂 연령대별 매출 분포 분석 — 신제품이 어느 세그먼트에 침투했는지, "
                "고령화·MZ 트렌드가 매출에 어떤 영향을 주는지 정량화.",
    },
    "region": {
        "what": "거래 발생 지역 (서울 / 경기 / 부산 / 광역시·도).",
        "for":  "📍 지역별 매출 분포 — 수도권 집중도, 지역 확장 전략 검증, "
                "신규 매장 출점 효과 측정.",
    },
    # ── 채널·점포·고객 ─────────────────────────────────────────────────
    "channel": {
        "what": "판매 채널 (온라인 / 오프라인 / 편의점 / 마트 / 백화점).",
        "for":  "🛒 채널 mix 분석 — 한 회사 매출이 어떤 채널에서 발생하는지 추적. "
                "예: 'BGF리테일 = 편의점 채널 슬라이스'임을 명시하면 글로벌 투자자가 "
                "매출 coverage 한계를 정확히 이해 가능.",
    },
    "store_id": {
        "what": "점포 ID / 매장 코드.",
        "for":  "🏪 점포 단위 분석 — 매장별 매출 quality 검증, 이상 점포 자동 식별, "
                "PE Due Diligence에서 매장 분포 평가. 동일점포 매출 성장 추적.",
    },
    "customer_id": {
        "what": "고객 ID / 회원 번호.",
        "for":  "👤 고객 단위 분석 — Retention(신규 vs 재방문 비율), "
                "ARPU(회원당 평균 매출), Cohort 분석. CRM·LTV 추정의 기반.",
    },
    # ── 리텐션 ─────────────────────────────────────────────────────────
    "retention_flag": {
        "what": "신규 / 재방문 플래그 ('신규' vs '재방문' / 0·1).",
        "for":  "🔁 고객 quality 정량 분해 — 매출 성장이 '새 고객 유입' 때문인지 "
                "'기존 고객 재구매' 때문인지 분리. 충성도·marketing CAC 효율 평가.",
    },
    # ── 종목 매핑 ──────────────────────────────────────────────────────
    "stock_code": {
        "what": "KRX 6자리 종목코드 (예: 005930 = 삼성전자).",
        "for":  "📈 주가 연동 활성화 — Market Signal이 yfinance로 주가 호출 가능, "
                "매출과 주가의 시차 상관 분석, Long/Short 후보 추출. "
                "⚠️ 법인등록번호(13자리)와 다릅니다.",
    },
    "security_code": {
        "what": "ISIN — 국제 증권 식별 코드 (예: KR7005930003).",
        "for":  "🆔 stock_code 대체 가능 — 12자리 ISIN에서 중간 6자리(005930)를 "
                "자동 추출해 yfinance 호출. stock_code가 잘못 매핑돼도 ISIN으로 자동 fallback.",
    },
    # ── 미지정 ─────────────────────────────────────────────────────────
    "unknown": {
        "what": "이 컬럼은 분석에 사용하지 않음 (제외).",
        "for":  "분석과 무관한 컬럼 (예: 내부 ID, 메모, 빈 컬럼 등)을 'unknown'으로 두면 자동 무시.",
    },
}


def role_help_text(role: str) -> str:
    """role의 활용처와 의미를 selectbox help tooltip 텍스트로 반환.

    활용처가 먼저 — 매핑 결정을 위한 핵심 정보.
    """
    info = ROLE_DESCRIPTION.get(role)
    if not info:
        return ""
    return f"🎯 이 역할로 매핑하면:\n{info['for']}\n\n📌 의미: {info['what']}"


# Alias 정규화 — 새 역할 → 기존 모듈이 사용하는 alias도 함께 등록
# 예: sales_quantity 컬럼이 있으면 quantity 키로도 매핑하여 기존 모듈 호환
_ROLE_ALIASES = {
    "sales_quantity":  ["quantity"],
    "sales_count":     ["number_of_tx"],
    # 카테고리 계층 → 단일 category_name으로도 자동 매핑
    # 우선순위: medium → small → large (medium이 보통 가장 분석 친화적)
    # normalize_role_map의 setdefault 동작으로 medium이 있으면 medium, 없으면 small, 없으면 large 사용
    "category_medium": ["category_name"],
    "category_small":  ["category_name"],
    "category_large":  ["category_name"],
}


def normalize_role_map(role_map: dict) -> dict:
    """role_map에 alias 키를 자동 등록 (이미 있으면 덮어쓰지 않음).

    예) {"sales_quantity": "qty_col"} → {"sales_quantity": "qty_col", "quantity": "qty_col"}
    """
    out = dict(role_map)
    for src, aliases in _ROLE_ALIASES.items():
        if src in out:
            for a in aliases:
                out.setdefault(a, out[src])
    return out

# ── 분석 모듈 카탈로그 ─────────────────────────────────────────────────────────
# requires      : 필수 역할 (없으면 failed_requirement 상태)
# optional      : 선택 역할 (있으면 더 정확)
# warn_if_missing : 없으면 executable_with_warning 상태 (기능 제한)
CATALOG: dict[str, dict] = {
    # ── Intelligence Hub ───────────────────────────────────────────────────────
    "growth": {
        "name":             "📈 Growth Analytics",
        "desc":             "기간별 매출 성장률(MoM/QoQ/YoY) 및 모멘텀 분석",
        "layer":            "Intelligence",
        "requires":         ["transaction_date", "sales_amount"],
        "optional":         ["company_name", "brand_name"],
        "warn_if_missing":  [],
    },
    "demand": {
        "name":             "🔥 Demand Intelligence",
        "desc":             "거래건수 × 객단가로 매출 성장 동인 분해",
        "layer":            "Intelligence",
        "requires":         ["transaction_date", "sales_amount"],
        "optional":         ["number_of_tx"],
        "warn_if_missing":  [],
    },
    "anomaly": {
        "name":             "🚨 Anomaly Detection",
        "desc":             "소비 급등·급락 이상 신호 자동 탐지 (Z-score / IQR)",
        "layer":            "Intelligence",
        "requires":         ["transaction_date", "sales_amount"],
        "optional":         ["company_name", "brand_name"],
        "warn_if_missing":  [],
    },
    "brand": {
        "name":             "🏷 Brand Intelligence",
        "desc":             "브랜드별 매출 점유율 및 Momentum 비교",
        "layer":            "Intelligence",
        "requires":         ["sales_amount"],
        "optional":         ["brand_name", "company_name", "transaction_date"],
        "warn_if_missing":  ["brand_name"],     # company_name으로 fallback 가능
    },
    "sku": {
        "name":             "📦 SKU Intelligence",
        "desc":             "상품별 기여도, 신흥 SKU 탐지, 라이프사이클 분석",
        "layer":            "Intelligence",
        "requires":         ["sales_amount"],
        "optional":         ["sku_name", "transaction_date", "company_name"],
        "warn_if_missing":  ["sku_name"],
    },
    "category": {
        "name":             "🗂 Category Intelligence",
        "desc":             "카테고리 성장률 및 시장 점유율 이동 분석",
        "layer":            "Intelligence",
        "requires":         ["sales_amount"],
        "optional":         ["category_name", "transaction_date"],
        "warn_if_missing":  ["category_name"],
    },
    # ── Signal Layer ───────────────────────────────────────────────────────────
    "market_signal": {
        "name":             "📉 Market Signal",
        "desc":             "매출 성장률 vs 주가 수익률 시차 상관분석 (선행 신호 탐지)",
        "layer":            "Signal",
        "requires":         ["transaction_date", "sales_amount"],
        "optional":         ["stock_code", "security_code", "company_name"],
        "warn_if_missing":  ["stock_code"],     # stock_code OR security_code 필요
    },
    "earnings_intel": {
        "name":             "📊 Earnings Intelligence",
        "desc":             "매출 분기별 집계 및 DART 공시매출 비교 (API key 선택)",
        "layer":            "Signal",
        "requires":         ["transaction_date", "sales_amount"],
        "optional":         ["company_name"],
        "warn_if_missing":  [],
    },
    "alpha_validation": {
        "name":             "🎯 Alpha Validation",
        "desc":             "매출 선행성 기반 알파 수익 실현 가능성 종합 검증",
        "layer":            "Signal",
        "requires":         ["transaction_date", "sales_amount"],
        "optional":         ["stock_code", "security_code"],
        "warn_if_missing":  [],
    },
    # ── Factor Layer ──────────────────────────────────────────────────────────
    "factor_research": {
        "name":             "🧪 Factor Research",
        "desc":             "Cross-sectional Rank IC + Quintile Backtest (헤지펀드 표준 factor 검증)",
        "layer":            "Factor",
        "requires":         ["transaction_date", "sales_amount"],
        "optional":         ["company_name", "number_of_tx"],
        "warn_if_missing":  ["stock_code"],     # stock_code 또는 security_code 필요
    },
}


# 대용량 DataFrame 추론 시 패턴 매칭 sample 크기 (이상이면 head로 잘라 사용)
_INFER_SAMPLE_N    = 5_000     # 정규식·apply lambda 비용을 상한
_NUNIQUE_SAMPLE_N  = 100_000   # nunique 계산 상한 (그 이상은 표본 nunique로 추정)


def infer_schema(df: pd.DataFrame, sample_n: int = _INFER_SAMPLE_N) -> list[dict]:
    """
    컬럼명 + dtype + 샘플값 패턴으로 역할을 추론한다.

    대용량 대응: 정규식·apply 기반 패턴 검사는 상위 `sample_n` 행만 사용.
    null_pct는 전체 df 기준, n_unique는 100k 행 이하에서만 정확값 (그 이상은 표본 추정).

    Returns list of dicts:
      column_name, dtype, sample, null_pct, n_unique,
      inferred_role, confidence (0-100), reason, final_role
    """

    # ── 키워드 맵 (role, keywords, base_score) ─────────────────────────────────
    # base_score 가 높을수록 우선 — 동일 컬럼에 여러 역할이 후보로 매칭될 때 큰 점수 승리
    KW_MAP = [
        # ── 시간 ─────────────────────────────────────────────────
        ("transaction_date", [
            "transaction_date", "tx_date", "거래일자", "거래일",
            "일자", "date", "day", "날짜", "dt", "ymd",
            "transaction", "time", "tm", "yyyymmdd", "기간",
            # 월 단위 데이터 (YYYYMM)
            "transaction_month", "tx_month", "year_month", "yearmonth",
            "yyyymm", "ym", "month", "월", "년월", "거래월",
            # 분기 단위
            "quarter", "yyyyq", "분기",
        ], 55),

        # ── 매출 메트릭 ─────────────────────────────────────────
        ("sales_amount", [
            "sales_amount", "sales_amt", "amount", "거래금액", "거래액",
            "매출", "금액", "revenue", "sales", "value", "amt",
            "합계", "sum", "수익", "money", "판매금액", "결제금액",
        ], 60),
        ("sales_quantity", [
            "sales_quantity", "sales_qty", "거래량", "판매량",
            "qty_sold", "quantity_sold",
        ], 65),
        ("quantity", [
            "qty", "quantity", "수량", "ea", "개수", "pcs", "unit_qty",
        ], 60),
        ("sales_count", [
            # 결제·거래·주문 카운트 — 한 명이 여러 번 결제 가능 (>= active_users)
            "sales_count", "tx_count", "tx_cnt", "거래건수",
            "거래수", "건수", "횟수", "transaction_count",
            "num_tx", "number_of_tx", "txn_count", "트랜잭션수",
            "n_orders", "order_count", "orders_count", "주문건수", "주문수",
            "payment_count", "결제건수", "결제수",
            # 구매자수는 sales_count에 가깝지만 ambiguous → 약하게
            "구매자수", "구매자_수", "buyer_count",
        ], 65),
        ("active_users", [
            # 활성 이용자수 — 서비스를 사용한 unique 사용자 (< sales_count)
            "active_users", "active_user", "n_users", "n_user",
            "num_users", "num_user", "user_count", "users_count",
            "number_of_users", "number_of_user",
            "unique_users", "unique_visitors", "uniq_users",
            "uu", "uv",  # unique users / unique visitors
            "dau", "mau", "wau",   # daily/monthly/weekly active users
            "visitor_count", "visitors_count", "number_of_visitors",
            # 한국어 — unique 사람 수
            "이용자수", "이용자_수", "사용자수", "사용자_수",
            "유저수", "유저_수", "방문자수", "방문자_수",
            "거래자수", "거래자_수",   # unique 거래자(사람) — 결제 1회/N회 모두 1명
            "활성이용자", "활성사용자", "월활성", "일활성",
            "가입자수", "회원수", "회원_수",
            "고객수", "고객_수",
            "subscriber_count", "구독자수", "구독자_수",
        ], 70),  # sales_count(65)보다 약간 높게 — "이용자수"가 명시되면 정확히 잡음
        ("number_of_tx", [
            "number_of_tx", "tx_n", "n_tx", "n_transactions",
        ], 55),
        ("unit_price", [
            "unit_price", "단가", "price_per_unit", "asp",
            "atv", "객단가", "avg_price",
        ], 60),

        # ── 식별 ────────────────────────────────────────────────
        ("company_name", [
            "company", "corp", "업체", "회사", "법인", "거래처",
            "client", "vendor", "거래사",
            # 매핑 후 회사명 / 사업자명 컬럼
            "mapped_mbn", "mbn", "company_name", "corp_name",
            "회사명", "기업명", "법인명",
        ], 60),
        # 사업자번호 (10자리) — company_name과는 구별, ID 성격
        ("unknown", [
            "mapped_mbc", "mbc", "사업자번호", "사업자_번호", "biz_no",
            "bizr_no", "bizno", "business_no", "business_number",
        ], 30),    # unknown으로 약하게 — 분석 미사용 (회사명이 따로 있으면 충분)
        ("brand_name", [
            "brand", "브랜드", "상표", "maker", "제조사", "제조원",
        ], 65),
        ("sku_name", [
            "sku", "item", "product", "상품", "제품", "품목",
            "goods", "아이템", "상품명", "제품명", "barcode", "바코드",
        ], 60),

        # ── 카테고리 계층 (large > medium > small > generic) ───
        ("category_large", [
            "category_large", "category_l", "cat_l", "cat_1", "cat1",
            "대분류", "1차분류", "lg_cate", "lgcate",
            "category_lvl1", "level1_category", "parent_category",
        ], 75),
        ("category_medium", [
            "category_medium", "category_m", "cat_m", "cat_2", "cat2",
            "중분류", "2차분류", "md_cate", "mdcate",
            "category_lvl2", "level2_category", "sub_category", "subcategory",
        ], 75),
        ("category_small", [
            "category_small", "category_s", "cat_s", "cat_3", "cat3",
            "소분류", "3차분류", "sm_cate", "smcate",
            "category_lvl3", "level3_category", "leaf_category",
        ], 75),
        ("category_name", [
            "category", "cate", "카테고리", "분류", "구분",
            "genre", "type", "class",
        ], 55),

        # ── 데모그래픽 ─────────────────────────────────────────
        ("gender", [
            "gender", "sex", "성별", "남여", "남녀",
        ], 70),
        ("age_group", [
            "age_group", "age_grp", "age_band", "age_bracket",
            "연령대", "연령", "나이대", "age",
        ], 65),
        ("region", [
            "region", "지역", "도시", "city", "시도", "광역",
            "province", "시군구", "district", "지역명", "권역",
        ], 65),

        # ── 채널·점포·고객 ─────────────────────────────────────
        ("channel", [
            "channel", "판매채널", "유통채널", "채널",
            "online_offline", "store_type",
        ], 65),
        ("store_id", [
            "store_id", "store", "점포", "매장", "shop", "매장코드", "점포번호",
            "store_code", "branch", "branch_id",
        ], 65),
        ("customer_id", [
            # ID 형식만 강하게 매칭 — bare "user"/"customer"는 너무 broad해서
            # "number_of_users", "user_count" 같은 카운트 컬럼에도 매칭됨 → 제거.
            "customer_id", "customer_no", "고객id", "고객번호", "고객_id",
            "user_id", "user_no", "userid", "uid",
            "member_id", "member_no", "회원id", "회원번호", "회원_id",
            "buyer_id", "buyer_no",
        ], 65),

        # ── 리텐션 ────────────────────────────────────────────
        ("retention_flag", [
            "retention", "리텐션", "재방문", "재구매",
            "new_returning", "is_new", "is_returning",
            "신규여부", "신규구분", "신규재방문",
            "churn_flag", "loyal", "loyalty",
        ], 70),

        # ── 종목 매핑 ─────────────────────────────────────────
        ("stock_code", [
            "stock_code", "stock", "종목", "ticker", "stk", "종목코드",
            "krx", "krx_code", "isu_srt_cd",
        ], 55),
        ("security_code", [
            "isin", "security", "sec_code", "isu", "isu_cd",
            "증권코드", "표준코드",
        ], 55),
    ]

    scores:  dict[str, dict[str, int]]    = {col: {} for col in df.columns}
    reasons: dict[str, dict[str, list]]   = {col: {} for col in df.columns}
    meta:    dict[str, dict]              = {}

    def add(col: str, role: str, sc: int, rsn: str):
        scores[col][role]  = scores[col].get(role, 0) + sc
        reasons[col].setdefault(role, []).append(rsn)

    n_rows = len(df)
    for col in df.columns:
        series  = df[col]
        dtype   = str(series.dtype)
        nl      = col.lower().replace(" ", "_")

        # ── 통계 (전체) — null_pct는 isna().sum()으로 빠르게 ─────────────
        n_na      = int(series.isna().sum())
        null_pct  = round(n_na / max(n_rows, 1) * 100, 1)

        # ── n_unique: 100k행 초과는 sample-based 추정 (정확도보다 속도) ──
        if n_rows <= _NUNIQUE_SAMPLE_N:
            n_unique = int(series.nunique())
        else:
            n_unique = int(series.head(_NUNIQUE_SAMPLE_N).nunique())  # 표본 추정

        # ── 패턴 추론용 sample: 상위 sample_n 행만 (정규식·apply 비용 차단) ─
        sample        = series.dropna().head(sample_n)
        top3          = sample.unique()[:3]
        sample_str    = " / ".join(str(v) for v in top3)

        meta[col] = {
            "dtype":    dtype,
            "sample":   sample_str,
            "null_pct": null_pct,
            "n_unique": n_unique,
        }

        # ── 1. 컬럼명 키워드 매칭 ──────────────────────────────────────────
        for role, kws, base_sc in KW_MAP:
            for kw in kws:
                if kw in nl:
                    add(col, role, base_sc, f"컬럼명 '{kw}'")
                    break

        # ── 2. dtype 패턴 ───────────────────────────────────────────────────
        if "datetime" in dtype:
            add(col, "transaction_date", 40, "datetime 타입")

        is_numeric = any(d in dtype for d in ["int", "float"])
        is_object  = "object" in dtype

        if is_numeric and sample.size > 0:
            num      = pd.to_numeric(sample, errors="coerce").dropna()
            pos_rate = float((num > 0).mean())
            mean_val = float(num.mean()) if len(num) > 0 else 0

            # ── count-like 컬럼명 휴리스틱 (sales_amount 오추론 방지) ──
            # "거래자수", "user_count", "n_users", "DAU" 같은 카운트 컬럼명이면
            # sales_amount는 약하게, count 류는 강하게.
            col_lc = col.lower().strip()
            # 잘 알려진 count 약어 정확 매칭
            count_acronyms = {"dau", "mau", "wau", "uu", "uv"}  # daily/monthly/weekly active users
            is_count_name = (
                col_lc in count_acronyms
                or col_lc.endswith("수")
                or col_lc.endswith("_count") or col_lc.endswith("count")
                or col_lc.startswith("n_") or col_lc.startswith("num_")
                or col_lc.startswith("number_of_") or col_lc.startswith("cnt_")
                or "유저" in col or "거래자" in col or "구매자" in col
                or "방문자" in col or "사용자" in col or "이용자" in col
                or "user" in col_lc or "buyer" in col_lc or "visitor" in col_lc
                or "customer" in col_lc
            )
            # 정수면서 작은 값(<100k)이면 count 가능성 더 높음
            is_small_int = "int" in dtype and 0 < mean_val < 100_000

            if is_count_name:
                # sales_amount 점수는 약하게 (기존 20 → 5)
                add(col, "sales_amount", 5, "숫자형 (count-like 이름)")
                # count 후보들에 강한 boost
                add(col, "sales_count",  30, f"이름에 count 시그널 ('{col}')")
                add(col, "number_of_tx", 25, f"이름에 count 시그널 ('{col}')")
            else:
                add(col, "sales_amount", 20, "숫자형")
                if pos_rate > 0.8:
                    add(col, "sales_amount", 10, f"양수 {pos_rate*100:.0f}%")
                if mean_val > 5_000:
                    add(col, "sales_amount", 10, f"평균 {mean_val:,.0f}")

            if "int" in dtype:
                add(col, "number_of_tx", 12, "정수형")
                add(col, "quantity",     10, "정수형")
            # 작은 정수 (count 가능성) — count 신호가 이미 있으면 부스트
            if is_small_int and is_count_name:
                add(col, "sales_count",  10, "작은 정수 + count 이름")

        if is_object:
            # 고유값 수로 역할 암시 (계층 카테고리 추론에 핵심)
            if n_unique == 2:
                add(col, "gender",          18, f"고유값 2개 (남/여 가능성)")
                add(col, "retention_flag",  15, f"고유값 2개 (신규/재방문)")
            if 2 <= n_unique <= 10:
                add(col, "category_large",  18, f"고유값 {n_unique}개 (대분류)")
                add(col, "channel",         12, f"고유값 {n_unique}개")
                add(col, "age_group",       10, f"고유값 {n_unique}개")
                add(col, "region",           8, f"고유값 {n_unique}개")
            if 5 <= n_unique <= 30:
                add(col, "region",          12, f"고유값 {n_unique}개 (지역)")
            if 10 <= n_unique <= 50:
                add(col, "category_medium", 18, f"고유값 {n_unique}개 (중분류)")
                add(col, "category_name",   12, f"고유값 {n_unique}개")
            if 2 <= n_unique <= 50:
                add(col, "category_name",   10, f"고유값 {n_unique}개")
            if 30 <= n_unique <= 500:
                add(col, "category_small",  15, f"고유값 {n_unique}개 (소분류)")
            if 2 <= n_unique <= 2_000:
                add(col, "company_name",    10, f"고유값 {n_unique}개")
                add(col, "brand_name",       5, f"고유값 {n_unique}개")
            if n_unique > 50:
                add(col, "sku_name",         8, f"고유값 {n_unique}개")
            if n_unique > 500:
                add(col, "store_id",         8, f"고유값 {n_unique}개 (점포)")
                add(col, "customer_id",      8, f"고유값 {n_unique}개 (고객)")

        # ── 3. 샘플값 패턴 ──────────────────────────────────────────────────
        if sample.size > 0:
            s0    = str(sample.iloc[0]).strip()
            s_str = sample.astype(str)
            s_low = s_str.str.lower().str.strip()

            # YYYYMMDD (정수 8자리)
            if len(s0) == 8 and s0.isdigit():
                try:
                    v = int(s0)
                    if 19_000_101 <= v <= 21_001_231:
                        add(col, "transaction_date", 45, "YYYYMMDD 형식")
                except ValueError:
                    pass

            # YYYYMM (정수 6자리) — 월 단위 데이터 (예: 202110)
            if len(s0) == 6 and s0.isdigit():
                try:
                    v = int(s0)
                    yr = v // 100
                    mo = v % 100
                    if 1900 <= yr <= 2100 and 1 <= mo <= 12:
                        add(col, "transaction_date", 45, "YYYYMM 형식 (월 단위)")
                except ValueError:
                    pass

            # YYYY-MM-DD / YYYY/MM/DD
            elif re.search(r'^\d{4}[-/]\d{2}[-/]\d{2}', s0):
                add(col, "transaction_date", 35, "YYYY-MM-DD 형식")

            # 그 외 날짜 파싱 시도 (10개 중 80% 이상 성공)
            else:
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    parsed = pd.to_datetime(sample.head(10), errors="coerce")
                if parsed.notna().mean() >= 0.8:
                    add(col, "transaction_date", 12, "날짜 파싱 가능")

            # ── 종목/증권 코드 ────────────────────────────────────────────
            six_rate = float(s_str.str.match(r'^\d{5,6}$').mean())
            if six_rate > 0.5:
                add(col, "stock_code", 45, f"5-6자리 숫자코드 {six_rate*100:.0f}%")

            isin_rate = float(s_str.str.match(r'^[A-Z]{2}\d{10}$').mean())
            kr_rate   = float(s_str.str.startswith("KR").mean())
            if isin_rate > 0.3:
                add(col, "security_code", 45, f"ISIN 형식 {isin_rate*100:.0f}%")
            elif kr_rate > 0.3:
                add(col, "security_code", 25, f"KR 접두사 {kr_rate*100:.0f}%")

            # 사업자번호 (10자리 정수) — sales_amount 오추론 방지
            biz_rate = float(s_str.str.match(r'^\d{10}$').mean())
            if biz_rate > 0.5:
                add(col, "unknown", 40, f"10자리 사업자번호 {biz_rate*100:.0f}%")

            # ── 성별 (M/F/남/여/MALE/FEMALE) ──────────────────────────────
            gender_tokens = {"m", "f", "male", "female", "남", "여",
                             "남자", "여자", "남성", "여성", "1", "2"}
            gender_match  = float(s_low.isin(gender_tokens).mean())
            if gender_match > 0.7 and n_unique <= 5:
                add(col, "gender", 50, f"성별 토큰 매칭 {gender_match*100:.0f}%")

            # ── 연령대 (20대, 30대 / 20s / 25-29 / 정수 0-100) ───────────
            age_band_rate = float(s_str.str.match(r"^\d{1,2}대$|^\d{1,2}s$|^\d{2}-\d{2}$").mean())
            if age_band_rate > 0.5:
                add(col, "age_group", 50, f"연령대 형식 {age_band_rate*100:.0f}%")
            elif is_numeric:
                num_age = pd.to_numeric(sample, errors="coerce").dropna()
                if len(num_age) > 0 and num_age.between(0, 100).mean() > 0.9 and num_age.mean() < 70:
                    add(col, "age_group", 25, f"정수 나이 0~100 범위")

            # ── 지역 (한국 광역시·도 + 주요 시군구) ──────────────────────
            kr_regions = {
                "서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종",
                "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주",
                "수도권", "영남", "호남", "충청", "강원도",
            }
            region_match = float(
                s_str.apply(lambda x: any(r in str(x) for r in kr_regions)).mean()
            )
            if region_match > 0.5:
                add(col, "region", 55, f"한국 지역명 매칭 {region_match*100:.0f}%")

            # ── 판매채널 (온라인/오프라인/편의점/마트/백화점 등) ─────────
            channel_tokens = {
                "online", "offline", "온라인", "오프라인",
                "편의점", "마트", "백화점", "이커머스", "이커머",
                "convenience", "mart", "department", "supermarket",
                "직영", "가맹", "프랜차이즈",
            }
            channel_match = float(
                s_low.apply(lambda x: any(t in str(x) for t in channel_tokens)).mean()
            )
            if channel_match > 0.5:
                add(col, "channel", 55, f"채널 토큰 {channel_match*100:.0f}%")

            # ── 리텐션 (신규/재방문 등) ───────────────────────────────────
            retention_tokens = {
                "new", "returning", "신규", "재방문", "재구매",
                "loyal", "충성", "이탈", "churn",
                "y", "n", "true", "false", "0", "1",
            }
            retention_match = float(s_low.isin(retention_tokens).mean())
            if retention_match > 0.7 and n_unique <= 4:
                add(col, "retention_flag", 50, f"리텐션 플래그 {retention_match*100:.0f}%")

            # ── 식품/뷰티/패션 등 대분류 카테고리 토큰 매칭 ───────────────
            kr_cat_large = {
                "식품", "음료", "주류", "패션", "뷰티", "화장품", "가전",
                "생활", "유아", "스포츠", "건강", "의약", "유제품", "스낵",
                "가공식품", "신선식품", "냉장", "냉동", "베이커리", "디저트",
            }
            cat_match = float(
                s_str.apply(lambda x: any(t in str(x) for t in kr_cat_large)).mean()
            )
            if cat_match > 0.5 and n_unique <= 20:
                add(col, "category_large", 35, f"대분류 토큰 {cat_match*100:.0f}%")

            # ── 단가 / 객단가 추정 (숫자 + 평균 100~100,000) ─────────────
            if is_numeric:
                num = pd.to_numeric(sample, errors="coerce").dropna()
                if len(num) > 0:
                    mean_val = float(num.mean())
                    if 100 <= mean_val <= 100_000 and (num > 0).mean() > 0.9:
                        add(col, "unit_price", 15, f"평균 {mean_val:,.0f} (단가 범위)")

    # ── Greedy Assignment: 점수 높은 순으로 역할 1:1 배정 ─────────────────────
    ranked = sorted(
        [(col, role, sc)
         for col, rd in scores.items()
         for role, sc in rd.items()],
        key=lambda x: -x[2],
    )
    assigned: set[str]         = set()
    col_role: dict[str, tuple] = {}
    for col, role, sc in ranked:
        if col not in col_role and role not in assigned:
            col_role[col] = (role, sc)
            assigned.add(role)

    # ── 결과 조립 ─────────────────────────────────────────────────────────────
    result = []
    for col in df.columns:
        role, sc = col_role.get(col, ("unknown", 0))
        rsn = " / ".join(reasons.get(col, {}).get(role, [])) or "패턴 없음"
        m   = meta[col]
        result.append({
            "column_name":   col,
            "dtype":         m["dtype"],
            "sample":        m["sample"],
            "null_pct":      m["null_pct"],
            "n_unique":      m["n_unique"],
            "inferred_role": role,
            "confidence":    min(100, int(sc)),
            "reason":        rsn,
            "final_role":    role,          # 사용자가 덮어쓸 수 있는 필드
        })

    # ── LLM 스마트 보강 — 패턴이 약한 컬럼만 (confidence < 40) Claude에 위임 ──
    # API key 없거나 LLM 실패시 패턴 결과 그대로 사용 (graceful fallback)
    try:
        from modules.common.schema_ai import enhance_schema
        result = enhance_schema(result, confidence_threshold=40)
    except Exception:
        pass    # LLM 보강 실패해도 패턴 결과는 보존

    return result


def _parse_dates(series: pd.Series) -> pd.Series:
    """
    날짜 파싱 우선순위:
    1. 이미 datetime → 그대로 반환
    2. YYYYMMDD 8자리 정수/문자열 → format='%Y%m%d'
    3. YYYYMM 6자리 정수/문자열 (월 단위) → 해당 월 1일로 파싱
    4. YYYY-MM-DD / YYYY/MM/DD 문자열 → format 지정
    5. 그 외 → pd.to_datetime 자동 추론
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        return series

    sample = series.dropna()
    if sample.empty:
        return pd.to_datetime(series, errors="coerce")

    # 대표 샘플값을 문자열로 변환해서 형식 판별
    s0 = str(sample.iloc[0]).strip()

    # YYYYMMDD 감지: 순수 정수("20200101") 또는 NaN 혼재 float("20200101.0") 모두 처리
    s0_int = s0.split(".")[0]  # "20200101.0" → "20200101"
    if len(s0_int) == 8 and s0_int.isdigit():
        try:
            v = int(s0_int)
            if 19_000_101 <= v <= 21_001_231:
                # 벡터라이즈: float/int → Int64 nullable → 문자열로 변환 (apply 대비 100배 빠름)
                try:
                    int_s = pd.to_numeric(series, errors="coerce").astype("Int64")
                    str_s = int_s.astype("string")  # NA는 그대로 NA로 유지
                    return pd.to_datetime(str_s, format="%Y%m%d", errors="coerce")
                except Exception:
                    # fallback — 그래도 apply 사용
                    str_s = series.where(series.notna(), None).map(
                        lambda x: str(int(x)) if pd.notna(x) else None
                    )
                    return pd.to_datetime(str_s, format="%Y%m%d", errors="coerce")
        except (ValueError, OverflowError):
            pass

    # YYYYMM 감지 (6자리 — 월 단위 데이터 예: 202110 = 2021년 10월)
    if len(s0_int) == 6 and s0_int.isdigit():
        try:
            v = int(s0_int)
            yr, mo = v // 100, v % 100
            if 1900 <= yr <= 2100 and 1 <= mo <= 12:
                try:
                    int_s = pd.to_numeric(series, errors="coerce").astype("Int64")
                    # YYYYMM → "YYYYMM01" (해당 월 1일)
                    str_s = int_s.astype("string") + "01"
                    return pd.to_datetime(str_s, format="%Y%m%d", errors="coerce")
                except Exception:
                    str_s = series.where(series.notna(), None).map(
                        lambda x: f"{int(x)}01" if pd.notna(x) else None
                    )
                    return pd.to_datetime(str_s, format="%Y%m%d", errors="coerce")
        except (ValueError, OverflowError):
            pass

    # YYYY-MM-DD or YYYY/MM/DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s0):
        return pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
    if re.match(r"^\d{4}/\d{2}/\d{2}$", s0):
        return pd.to_datetime(series, format="%Y/%m/%d", errors="coerce")
    # YYYY-MM (월 단위 문자열)
    if re.match(r"^\d{4}-\d{2}$", s0):
        return pd.to_datetime(series + "-01", format="%Y-%m-%d", errors="coerce")

    # 그 외 형식 — 경고 억제
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return pd.to_datetime(series, errors="coerce")


def validate_data(df: pd.DataFrame, role_map: dict) -> dict:
    """
    9가지 항목 데이터 품질 검사 + Streamlit 렌더링.

    Returns:
        score      : int  0-100
        stats      : dict (date_min/max/days, sales_mean/total, valid_rows, total_rows)
        checks     : list[dict] (label, severity, detail, deduction)
        valid_rows : int
    """
    date_col  = role_map.get("transaction_date")
    sales_col = role_map.get("sales_amount")
    tx_col    = role_map.get("number_of_tx")

    total = len(df)
    score  = 100
    stats  = {"total_rows": total}
    checks: list[dict] = []

    def _add(label: str, severity: str, detail: str, cut: int = 0):
        nonlocal score
        score -= cut
        checks.append({"label": label, "severity": severity, "detail": detail, "cut": cut})

    # ── 1. 날짜 결측 ──────────────────────────────────────────────────────────
    dates: pd.Series | None = None
    if date_col:
        dates = _parse_dates(df[date_col])
        na_n  = int(dates.isna().sum())
        na_pct = na_n / total * 100
        if na_n:
            _add("날짜 결측", "error", f"{na_n:,}행 ({na_pct:.1f}%)",
                 cut=int(min(30, na_pct * 0.5)))
        else:
            _add("날짜 결측", "ok", "없음")
        vd = dates.dropna()
        if not vd.empty:
            stats.update({
                "date_min": vd.min().strftime("%Y-%m-%d"),
                "date_max": vd.max().strftime("%Y-%m-%d"),
                "days":     (vd.max() - vd.min()).days,
            })
    else:
        _add("날짜 컬럼", "critical", "transaction_date 역할 미지정", cut=30)

    # ── 2. 매출 결측 ──────────────────────────────────────────────────────────
    sales: pd.Series | None = None
    if sales_col:
        sales = pd.to_numeric(df[sales_col], errors="coerce")
        na_n  = int(sales.isna().sum())
        na_pct = na_n / total * 100
        if na_n:
            _add("매출 결측", "error", f"{na_n:,}행 ({na_pct:.1f}%)",
                 cut=int(min(25, na_pct * 0.4)))
        else:
            _add("매출 결측", "ok", "없음")
        stats.update({
            "sales_total": float(sales.sum()),
            "sales_mean":  float(sales.mean()),
        })
    else:
        _add("매출 컬럼", "critical", "sales_amount 역할 미지정", cut=25)

    # ── 3. 거래건수 결측 (컬럼이 있을 때만) ───────────────────────────────────
    if tx_col:
        tx    = pd.to_numeric(df[tx_col], errors="coerce")
        na_n  = int(tx.isna().sum())
        na_pct = na_n / total * 100
        if na_n:
            _add("거래건수 결측", "warning", f"{na_n:,}행 ({na_pct:.1f}%)",
                 cut=int(min(5, na_pct * 0.05)))
        else:
            _add("거래건수 결측", "ok", "없음")

    # ── 4. 중복 행 ────────────────────────────────────────────────────────────
    # 50만 행 초과 시 표본 추정으로 전환 (전체 hash는 매우 비쌈)
    _DUP_FULL_THRESHOLD = 500_000
    if total <= _DUP_FULL_THRESHOLD:
        dup_n   = int(df.duplicated().sum())
        dup_pct = dup_n / total * 100
        dup_note = f"{dup_n:,}행 ({dup_pct:.1f}%)"
    else:
        # 표본 100k 행만 검사 → 비율을 전체로 외삽
        _samp_n = min(100_000, total)
        dup_samp = int(df.head(_samp_n).duplicated().sum())
        dup_pct  = dup_samp / _samp_n * 100
        dup_n    = int(dup_pct / 100 * total)
        dup_note = f"~{dup_n:,}행 (표본 추정 {dup_pct:.1f}%)"
    if dup_n:
        _add("중복 행", "warning", dup_note,
             cut=10 if dup_pct > 5 else 5)
    else:
        _add("중복 행", "ok", "없음")

    # ── 5. 음수 매출 ──────────────────────────────────────────────────────────
    if sales is not None:
        neg_n = int((sales < 0).sum())
        if neg_n:
            _add("음수 매출", "warning",
                 f"{neg_n:,}행 ({neg_n/total*100:.1f}%) — 환불/취소 포함 가능성", cut=5)
        else:
            _add("음수 매출", "ok", "없음")

    # ── 6. 0원 매출 ───────────────────────────────────────────────────────────
    if sales is not None:
        zero_n   = int((sales == 0).sum())
        zero_pct = zero_n / total * 100
        if zero_n:
            _add("0원 매출", "info", f"{zero_n:,}행 ({zero_pct:.1f}%)",
                 cut=5 if zero_pct > 5 else 2)
        else:
            _add("0원 매출", "ok", "없음")

    # ── 7. 이상값 (IQR 1.5×) ─────────────────────────────────────────────────
    if sales is not None:
        valid_s = sales.dropna()
        if len(valid_s) >= 4:
            q1, q3 = valid_s.quantile(0.25), valid_s.quantile(0.75)
            iqr = q3 - q1
            out_mask = (valid_s < q1 - 1.5 * iqr) | (valid_s > q3 + 1.5 * iqr)
            out_n    = int(out_mask.sum())
            out_pct  = out_n / total * 100
            stats["outlier_n"] = out_n
            if out_n:
                _add("IQR 이상값", "info", f"{out_n:,}행 ({out_pct:.1f}%)",
                     cut=5 if out_pct > 10 else (3 if out_pct > 5 else 0))
            else:
                _add("IQR 이상값", "ok", "없음")

    # ── 8. 날짜 정렬 문제 ────────────────────────────────────────────────────
    if dates is not None and dates.notna().any():
        if not dates.dropna().is_monotonic_increasing:
            _add("날짜 정렬", "info", "시간 순 정렬이 아닌 행 존재 (분석엔 무관)", cut=0)
        else:
            _add("날짜 정렬", "ok", "시간 순 정렬")

    # ── 9. 분석 가능 행 수 ────────────────────────────────────────────────────
    masks = []
    if sales is not None:
        masks.append(sales.notna())
    if dates is not None:
        masks.append(dates.notna())
    if masks:
        valid_mask = masks[0]
        for m in masks[1:]:
            valid_mask = valid_mask & m
        valid_rows = int(valid_mask.sum())
    else:
        valid_rows = total

    stats["valid_rows"] = valid_rows

    if valid_rows < 10:
        _add("분석 가능 행", "critical", f"{valid_rows}행 — 분석 불가 (최소 10행)", cut=30)
    elif valid_rows < 30:
        _add("분석 가능 행", "warning", f"{valid_rows}행 — 권장: 30행 이상", cut=15)
    elif valid_rows < 100:
        _add("분석 가능 행", "info", f"{valid_rows:,}행 — 권장: 100행 이상", cut=5)
    else:
        _add("분석 가능 행", "ok", f"{valid_rows:,}행")

    final_score = max(0, min(100, score))

    # ── Streamlit 렌더링 ──────────────────────────────────────────────────────
    _render_validation(final_score, stats, checks, role_map, total)

    return {"score": final_score, "stats": stats, "checks": checks, "valid_rows": valid_rows}


def _render_validation(score: int, stats: dict, checks: list, role_map: dict, total: int):
    """validate_data() 전용 렌더러."""
    # ── 스코어 + 핵심 지표 ────────────────────────────────────────────────────
    if score >= 80:
        score_color, score_label = "#16a34a", "양호"
    elif score >= 60:
        score_color, score_label = "#d97706", "점검 권장"
    else:
        score_color, score_label = "#dc2626", "문제 있음"

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
        f"<div style='background:{score_color}1a;border:2px solid {score_color};"
        f"border-radius:10px;padding:14px;text-align:center'>"
        f"<div style='font-size:32px;font-weight:800;color:{score_color}'>{score}</div>"
        f"<div style='font-size:12px;color:{score_color};font-weight:600'>/ 100 · {score_label}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )
    c2.metric("전체 행", f"{total:,}")
    c3.metric("분석 가능 행", f"{stats.get('valid_rows', total):,}")
    if stats.get("date_min"):
        c4.metric("데이터 기간",
                  f"{stats['date_min']} ~",
                  delta=f"{stats['date_max']} ({stats['days']:,}일)")

    st.write("")

    # ── 날짜 기간 배너 ─────────────────────────────────────────────────────────
    if stats.get("date_min"):
        st.info(
            f"📅 **{stats['date_min']} ~ {stats['date_max']}** "
            f"({stats['days']:,}일 · {stats['days']//30}개월)"
        )

    # ── 검사 항목 목록 ─────────────────────────────────────────────────────────
    ICON = {
        "critical": "🔴",
        "error":    "🟠",
        "warning":  "🟡",
        "info":     "🔵",
        "ok":       "✅",
    }
    BG = {
        "critical": "#fef2f2",
        "error":    "#fff7ed",
        "warning":  "#fefce8",
        "info":     "#eff6ff",
        "ok":       "#f0fdf4",
    }

    failed  = [c for c in checks if c["severity"] != "ok"]
    passed  = [c for c in checks if c["severity"] == "ok"]

    if failed:
        st.markdown("#### 발견된 문제")
        for chk in failed:
            sev  = chk["severity"]
            icon = ICON[sev]
            bg   = BG[sev]
            cut  = f"  **−{chk['cut']}점**" if chk["cut"] > 0 else ""
            st.markdown(
                f"<div style='background:{bg};border-radius:8px;padding:10px 16px;"
                f"margin-bottom:6px;font-size:14px'>"
                f"{icon} &nbsp; <b>{chk['label']}</b> &nbsp;·&nbsp; {chk['detail']}{cut}"
                f"</div>",
                unsafe_allow_html=True,
            )
    else:
        st.success("모든 항목 정상")

    if passed:
        with st.expander(f"✅ 통과 항목 ({len(passed)}개)", expanded=False):
            for chk in passed:
                st.markdown(
                    f"<div style='font-size:13px;color:#6b7280;padding:2px 0'>"
                    f"✅ &nbsp; {chk['label']} &nbsp;·&nbsp; {chk['detail']}</div>",
                    unsafe_allow_html=True,
                )


def _has_stock(role_map: dict) -> bool:
    """stock_code 또는 security_code 중 하나라도 있으면 True."""
    return "stock_code" in role_map or "security_code" in role_map


def _eval_caps(role_map: dict) -> list[dict]:
    """렌더링 없이 capability 평가만 수행. build_capability_map / Step 5 공용.

    cap_status:
        executable              — 필수+권장 역할 모두 충족
        executable_with_warning — 필수 역할 충족, 일부 권장 역할 부재 (기능 제한)
        failed_requirement      — 필수 역할 누락 (실행 시 failed 반환)
    모든 모듈은 runnable=True (실행 시도 가능).
    """
    result: list[dict] = []
    for key, info in CATALOG.items():
        missing = [r for r in info["requires"] if r not in role_map]
        opt_missing = [r for r in info["optional"] if r not in role_map]

        # warn_if_missing 체크 (stock_code는 security_code로 대체 가능)
        warn_missing: list[str] = []
        for w in info.get("warn_if_missing", []):
            if w == "stock_code":
                if not _has_stock(role_map):
                    warn_missing.append("stock_code 또는 security_code")
            elif w == "brand_name":
                # company_name이 있으면 fallback 가능 — 경고만
                if "brand_name" not in role_map and "company_name" not in role_map:
                    warn_missing.append("brand_name (company_name 대체도 없음)")
                elif "brand_name" not in role_map:
                    warn_missing.append("brand_name (→ company_name 대체)")
            elif w not in role_map:
                warn_missing.append(w)

        if missing:
            cap_status = "failed_requirement"
            reason = "필수 역할 없음: " + ", ".join(missing)
        elif warn_missing:
            cap_status = "executable_with_warning"
            reason = "실행 가능 (제한): " + " · ".join(f"{w} 없음" for w in warn_missing)
        else:
            cap_status = "executable"
            extras = [f"{r} 없음 (선택)" for r in opt_missing if r not in info.get("warn_if_missing", [])]
            reason = "분석 가능" + (f" · {' · '.join(extras)}" if extras else "")

        result.append({
            "key":              key,
            "name":             info["name"],
            "desc":             info["desc"],
            "layer":            info["layer"],
            "cap_status":       cap_status,
            "runnable":         True,           # 모든 모듈 실행 가능
            "missing":          missing,
            "optional_missing": opt_missing,
            "warn_missing":     warn_missing,
            "reason":           reason,
        })
    return result


def get_runnable(role_map: dict) -> list[dict]:
    """Step 5에서 렌더링 없이 모든 모듈 목록 반환 (전체 실행 가능)."""
    return _eval_caps(role_map)


def build_capability_map(role_map: dict) -> list[dict]:
    """
    role_map 기준으로 9개 분석 모듈의 가능 여부를 판단하고 Streamlit에 렌더링.

    Returns list[dict]:
        key, name, desc, layer,
        cap_status, runnable,
        missing, optional_missing, warn_missing, reason
    """
    caps = _eval_caps(role_map)
    _render_capability_map(caps)
    return caps


def _render_capability_map(caps: list[dict]):
    """build_capability_map() 전용 렌더러."""

    n_exec  = sum(1 for c in caps if c["cap_status"] == "executable")
    n_warn  = sum(1 for c in caps if c["cap_status"] == "executable_with_warning")
    n_fail  = sum(1 for c in caps if c["cap_status"] == "failed_requirement")

    b1, b2, b3, _ = st.columns([1, 1, 1, 3])
    b1.metric("완전 실행 가능", f"{n_exec}개")
    b2.metric("제한적 실행", f"{n_warn}개")
    b3.metric("데이터 부족", f"{n_fail}개")
    st.write("")

    LAYER_META = {
        "Intelligence": ("Intelligence Hub", "🧠"),
        "Signal":       ("Signal Layer",     "📡"),
    }

    for layer_key, (layer_label, layer_icon) in LAYER_META.items():
        layer_caps = [c for c in caps if c["layer"] == layer_key]
        if not layer_caps:
            continue

        st.markdown(f"#### {layer_icon} {layer_label}")
        cols = st.columns(len(layer_caps))
        for col, cap in zip(cols, layer_caps):
            _render_cap_card(col, cap)
        st.write("")


def render(go_to):
    """Step 1 — 파일 업로드 + 스키마 추론 + 역할 매핑 + 검증 + capability map."""
    st.subheader("① Foundation — 데이터 업로드 & 스키마 매핑")

    uploaded = st.file_uploader(
        "CSV 또는 Excel 파일 업로드",
        type=["csv", "xlsx", "xls"],
        key="foundation_uploader",
    )

    # ── 새 파일이 올라오면 세션 상태 재설정 ────────────────────────────────────
    if uploaded is not None:
        file_id = f"{uploaded.name}__{uploaded.size}"
        if st.session_state.get("foundation_file_id") != file_id:
            size_mb = uploaded.size / (1024 * 1024)
            progress = st.progress(0.0)
            status   = st.empty()
            try:
                # ── 1) 파일 읽기 ─────────────────────────────────────────
                status.info(f"📥 파일 읽는 중... ({size_mb:.1f} MB) — 큰 파일은 1~3분 소요될 수 있습니다.")
                progress.progress(0.10)
                import time as _t; _t0 = _t.time()
                if uploaded.name.lower().endswith(".csv"):
                    df = pd.read_csv(uploaded, low_memory=False)
                else:
                    df = pd.read_excel(uploaded)
                progress.progress(0.55)
                status.info(
                    f"✓ {len(df):,}행 × {len(df.columns)}열 로드 ({_t.time()-_t0:.1f}초). "
                    f"🧠 스키마 추론 중... (최대 {min(_INFER_SAMPLE_N, len(df)):,}행 샘플 사용)"
                )
            except Exception as e:
                progress.empty(); status.empty()
                st.error(f"파일 읽기 실패: {e}")
                st.stop()

            # ── 2) 스키마 추론 (sample 기반) ───────────────────────────────
            _t1 = _t.time()
            try:
                schema = infer_schema(df)
            except Exception as e:
                progress.empty(); status.empty()
                st.error(f"스키마 추론 실패: {e}")
                st.stop()
            progress.progress(0.95)
            status.info(f"✓ 스키마 추론 완료 ({_t.time()-_t1:.1f}초). 화면 구성 중...")

            st.session_state["raw_df"]            = df
            st.session_state["foundation_file_id"] = file_id
            st.session_state["schema"]            = schema
            # 자동 추론 결과로 role_map 초기값 구성 + alias 정규화
            initial_map = {
                row["inferred_role"]: row["column_name"]
                for row in schema
                if row["inferred_role"] != "unknown"
            }
            st.session_state["role_map"] = normalize_role_map(initial_map)
            progress.progress(1.0)
            status.success(f"🎉 준비 완료 — {len(df):,}행 × {len(df.columns)}열")
            progress.empty()
            # 이전 매핑 위젯 상태 클리어 (컬럼명이 바뀌었을 수 있으므로)
            for k in list(st.session_state.keys()):
                if isinstance(k, str) and k.startswith("found_role_"):
                    del st.session_state[k]

    # ── 데이터 없으면 대기 ────────────────────────────────────────────────────
    if "raw_df" not in st.session_state:
        st.info("위에서 데이터 파일을 업로드하세요. (CSV, XLSX 지원)")
        st.stop()

    df:    pd.DataFrame = st.session_state["raw_df"]
    schema: list[dict]  = st.session_state["schema"]

    st.success(f"**파일 로드 완료** — {len(df):,}행 × {len(df.columns)}열")

    with st.expander("📋 데이터 미리보기 (상위 5행)", expanded=False):
        st.dataframe(df.head(5), use_container_width=True)

    # ── 역할 매핑 UI ──────────────────────────────────────────────────────────
    st.markdown("#### 1) 컬럼 역할 매핑")
    st.caption("자동 추론된 역할입니다. 필요시 직접 수정하세요.")

    role_map: dict[str, str] = {}
    duplicates: dict[str, list[str]] = {}

    # 사용자 노출 옵션 — 백엔드 alias 숨김
    user_opts = user_role_options()

    grid_cols = st.columns(2)
    for i, row in enumerate(schema):
        container = grid_cols[i % 2]
        with container:
            col_name = row["column_name"]
            current  = row["inferred_role"]
            session_key = f"found_role_{col_name}"

            # 세션 정규화 — selectbox 호출 전에 옵션 안 값으로 정렬.
            # normalize_to_user_role 단일 helper 사용 — 중복 dict 제거.
            existing = st.session_state.get(session_key, current)
            st.session_state[session_key] = normalize_to_user_role(existing, user_opts)

            new_role = st.selectbox(
                f"**{col_name}**  ·  *{row['dtype']}*",
                options=user_opts,
                key=session_key,             # session_state 기반 — index 불필요
                format_func=lambda r: ROLE_LABEL.get(r, r),
                help=(
                    f"샘플: {row['sample']}\n\n"
                    f"결측 {row['null_pct']}% · 고유값 {row['n_unique']:,}개\n\n"
                    f"추론 신뢰도: {row['confidence']}% — {row['reason']}"
                ),
            )
            if new_role != "unknown":
                if new_role in role_map:
                    duplicates.setdefault(new_role, [role_map[new_role]]).append(col_name)
                else:
                    role_map[new_role] = col_name

    if duplicates:
        for role, cols in duplicates.items():
            st.warning(
                f"⚠️ '{role}' 역할이 여러 컬럼에 할당됨: {', '.join(cols)} — "
                f"각 역할은 컬럼 하나에만 지정할 수 있습니다."
            )

    # alias 자동 등록 — sales_quantity ↔ quantity, sales_count ↔ number_of_tx 등
    role_map = normalize_role_map(role_map)
    st.session_state["role_map"] = role_map

    # ── 검증 ──────────────────────────────────────────────────────────────────
    st.markdown("#### 2) 데이터 품질 검사")
    with st.spinner("데이터 품질 검사 중..."):
        validation = validate_data(df, role_map)

    # ── Capability Map ────────────────────────────────────────────────────────
    st.markdown("#### 3) 분석 가능 범위")
    build_capability_map(role_map)

    # ── 진행 버튼 ─────────────────────────────────────────────────────────────
    st.divider()

    has_required = (
        "transaction_date" in role_map and "sales_amount" in role_map
    )
    enough_rows  = validation["valid_rows"] >= 10
    can_proceed  = has_required and enough_rows and not duplicates

    msg_col, btn_col = st.columns([5, 1])
    with msg_col:
        if not has_required:
            st.warning(
                "필수 역할(**transaction_date**, **sales_amount**)을 모두 매핑해야 다음 단계로 갈 수 있습니다."
            )
        elif not enough_rows:
            st.warning(
                f"분석 가능 행이 {validation['valid_rows']}개로 부족합니다 (최소 10행)."
            )
        elif duplicates:
            st.warning("역할 중복을 먼저 해결하세요.")
    with btn_col:
        if st.button("다음 →", type="primary", disabled=not can_proceed, key="found_next"):
            go_to(2)


def _render_cap_card(container, cap: dict):
    """카드 한 개 렌더링 — 3-state."""
    status = cap["cap_status"]

    if status == "executable":
        border, bg = "#16a34a", "#f0fdf4"
        badge, badge_bg = "✅ 실행 가능", "#16a34a"
    elif status == "executable_with_warning":
        border, bg = "#d97706", "#fffbeb"
        badge, badge_bg = "⚠️ 제한 실행", "#d97706"
    else:  # failed_requirement
        border, bg = "#d1d5db", "#f9fafb"
        badge, badge_bg = "❌ 데이터 부족", "#9ca3af"

    missing_html = ""
    if cap["missing"]:
        tags = "".join(
            f"<span style='background:#fee2e2;color:#dc2626;border-radius:4px;"
            f"padding:1px 6px;font-size:10px;margin:1px;display:inline-block'>"
            f"필요: {r}</span>"
            for r in cap["missing"]
        )
        missing_html = f"<div style='margin-top:6px'>{tags}</div>"

    warn_html = ""
    if cap.get("warn_missing") and status != "failed_requirement":
        tags = "".join(
            f"<span style='background:#fef3c7;color:#92400e;border-radius:4px;"
            f"padding:1px 6px;font-size:10px;margin:1px;display:inline-block'>"
            f"권장: {r}</span>"
            for r in cap["warn_missing"]
        )
        warn_html = f"<div style='margin-top:4px'>{tags}</div>"

    container.markdown(
        f"""<div style="border:1.5px solid {border};border-radius:10px;
        padding:14px;background:{bg};min-height:140px;margin-bottom:4px">
        <div style="font-size:13px;font-weight:700;margin-bottom:4px">{cap['name']}</div>
        <div style="font-size:11px;color:#6b7280;margin-bottom:8px;line-height:1.4">
          {cap['desc']}</div>
        <span style="background:{badge_bg};color:#fff;border-radius:5px;
          padding:2px 8px;font-size:11px;font-weight:600">{badge}</span>
        {missing_html}{warn_html}
        </div>""",
        unsafe_allow_html=True,
    )
