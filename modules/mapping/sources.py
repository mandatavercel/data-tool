"""
modules/mapping/sources.py

매핑 앱 — 표준 컬럼이 어떤 '소스'에서 값을 받을지에 대한 정의 모음.

소스 종류:
  1) raw 컬럼명 (사용자가 업로드한 원천 데이터의 컬럼)
  2) VIRTUAL_SOURCES (이 모듈의 VSRC_* 상수들):
      - [KRX] ISIN / 단축코드 / 시장   ← ③ ISIN 매칭 결과에서
      - [DART] 법인등록번호 / 사업자등록번호 / 영문명 / 한글정식명 / corp_code
                                       ← ④ DART 매칭 결과에서
      - [변환] 회사명 영문 (DART 우선, 폴백: 한글)
                                       ← DART 영문명 있으면 영문, 없으면 raw 한글명

KIND_DEFAULT_VSRC: 표준 컬럼 kind → 자동 default 가상 소스.
  '회사' kind 는 영문 변환을 default 로 둔다(분석·국제 보고서 호환).
"""
from __future__ import annotations


# ── 매핑 안 함 ────────────────────────────────────────────────────────────────
NO_MAP = "— (매핑 안 함) —"


# ── KRX 매칭 결과 ─────────────────────────────────────────────────────────────
VSRC_KRX_ISIN = "[KRX] ISIN"
VSRC_KRX_STK  = "[KRX] 단축코드"
VSRC_KRX_MKT  = "[KRX] 시장"

KRX_SOURCES = [VSRC_KRX_ISIN, VSRC_KRX_STK, VSRC_KRX_MKT]


# ── DART 매칭 결과 ────────────────────────────────────────────────────────────
VSRC_DART_JR = "[DART] 법인등록번호 (jurir_no)"
VSRC_DART_BZ = "[DART] 사업자등록번호 (bizr_no)"
VSRC_DART_EN = "[DART] 영문회사명"
VSRC_DART_KR = "[DART] 한글회사명 (정식)"
VSRC_DART_CC = "[DART] corp_code"

DART_SOURCES = [VSRC_DART_JR, VSRC_DART_BZ, VSRC_DART_EN, VSRC_DART_KR, VSRC_DART_CC]


# ── 변환형 (raw + 외부 정보 결합) ─────────────────────────────────────────────
# 한글 회사명을 영문으로 변환. DART 매칭 성공 시 영문명, 실패 시 raw 한글명 유지.
VSRC_NAME_EN_FALLBACK = "[변환] 회사명 영문 (DART 우선, 폴백: 한글)"

# ⑤ 브랜드·제품·카테고리 영문화 파이프라인 결과 → 표준 컬럼에 반영
VSRC_BRAND_EN    = "[번역] 브랜드 영문 (영문화 파이프라인)"
VSRC_SKU_EN      = "[번역] 제품 영문 (영문화 파이프라인)"
VSRC_CATEGORY_EN = "[번역] 카테고리 영문 (영문화 파이프라인)"

# ⑤ '자유 LLM 영문화' — raw 컬럼별 동적 가상 소스 ([번역::컬럼명])
# 사용자가 ⑤ 에서 raw 컬럼을 자유롭게 골라 영문화하면, 그 컬럼이 동적으로
# 가상 소스로 등록되어 ⑥ 최종 매핑에서 selectbox 옵션으로 보임.
# 모든 번역값은 SQLite category 테이블에 공용 저장.
VSRC_TRANSLATE_PREFIX = "[번역::"


def make_translate_source(raw_col: str) -> str:
    """raw 컬럼명 → 동적 번역 가상 소스 문자열."""
    return f"{VSRC_TRANSLATE_PREFIX}{raw_col}]"


def is_translate_source(src: str | None) -> bool:
    return bool(src) and src.startswith(VSRC_TRANSLATE_PREFIX) and src.endswith("]")


def extract_translate_col(src: str) -> str | None:
    if not is_translate_source(src):
        return None
    return src[len(VSRC_TRANSLATE_PREFIX):-1]


TRANSFORM_SOURCES = [
    VSRC_NAME_EN_FALLBACK, VSRC_BRAND_EN, VSRC_SKU_EN, VSRC_CATEGORY_EN,
]


# ── 통합 리스트 ───────────────────────────────────────────────────────────────
VIRTUAL_SOURCES: list[str] = KRX_SOURCES + DART_SOURCES + TRANSFORM_SOURCES


# ── kind → 자동 default ────────────────────────────────────────────────────────
# 표준 컬럼의 kind 가 아래 표에 있으면 매핑 UI 가 default 로 그 가상 소스 선택.
#  - isin/stock_code/corp_code/name_eng : 매칭 결과 그대로 출력
#  - company/brand : 한글로 찾고 영문으로 출력하는 게 일반적 → 변환형 default
KIND_DEFAULT_VSRC: dict[str, str] = {
    "isin":       VSRC_KRX_ISIN,
    "stock_code": VSRC_KRX_STK,
    "corp_code":  VSRC_DART_JR,
    "name_eng":   VSRC_DART_EN,
    "company":    VSRC_NAME_EN_FALLBACK,   # 최종 영문 변환 (DART)
    "brand":      VSRC_BRAND_EN,           # ⑤ 영문화 파이프라인 결과 사용
    "sku":        VSRC_SKU_EN,             # ⑤ 영문화 파이프라인 결과 사용
    "category":   VSRC_CATEGORY_EN,        # ⑤ 영문화 파이프라인 결과 사용
}


# ── helper ────────────────────────────────────────────────────────────────────
def is_virtual(src: str | None) -> bool:
    """selectbox 의 선택값이 가상 소스인지 (고정 + 동적 번역 포함)."""
    if not src:
        return False
    return src in VIRTUAL_SOURCES or is_translate_source(src)


def is_krx_source(src: str | None) -> bool:
    return bool(src) and src in KRX_SOURCES


def is_dart_source(src: str | None) -> bool:
    return bool(src) and (src in DART_SOURCES or src == VSRC_NAME_EN_FALLBACK)
