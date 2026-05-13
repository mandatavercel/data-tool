"""
제품명 속성 분해 — 한글 raw → {brand, base_product, flavor, format, package_size, variant}.

규칙 기반 (사전 + 정규식). LLM 폴백은 llm.py 에서 별도 호출.

예:
  "농심 신라면 큰사발면 86g"
    → brand="농심" base_product="신라면" format="큰사발면" package_size="86g"
  "오뚜기 진라면 매운맛 5입"
    → brand="오뚜기" base_product="진라면" flavor="매운맛" variant="5입"
  "비비고 왕교자 1kg"
    → brand="비비고" base_product="왕교자" package_size="1kg"
"""
from __future__ import annotations

import re
from typing import Iterable


# ── 패키지 크기 정규식 ────────────────────────────────────────────────────────
_PACKAGE_SIZE_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(g|kg|ml|L|l|ℓ|개|입|팩|박스|포)\b",
    re.IGNORECASE,
)

# ── format (포장 형태) ────────────────────────────────────────────────────────
FORMAT_DICT: dict[str, str] = {
    "큰사발면": "Big Bowl",
    "큰사발":   "Big Bowl",
    "사발면":   "Bowl Noodle",
    "사발":     "Bowl",
    "컵라면":   "Cup Noodle",
    "컵":       "Cup",
    "봉지":     "Bag",
    "용기":     "Container",
    "팩":       "Pack",
    "박스":     "Box",
    "캔":       "Can",
    "병":       "Bottle",
}

# ── flavor / variant ──────────────────────────────────────────────────────────
# ⚠️ '라면' / '교자' 같은 일반 카테고리 단어는 base_product 에 속하므로 제외.
FLAVOR_DICT: dict[str, str] = {
    "매운맛":   "Spicy",
    "순한맛":   "Mild",
    "오리지널": "Original",
    "김치":     "Kimchi",
    "치즈":     "Cheese",
    "불고기":   "Bulgogi",
    "해물":     "Seafood",
    "치킨":     "Chicken",
    "양념":     "Seasoned",
    "짜장":     "Jajang",
    "카레":     "Curry",
    "된장":     "Doenjang",
    "고추장":   "Gochujang",
    "굴":       "Oyster",
    "새우":     "Shrimp",
}


def parse_product_name(
    name_kr: str,
    known_brands: Iterable[str] | None = None,
) -> dict:
    """제품명 분해 → 속성 dict.

    known_brands 가 주어지면 prefix 매칭으로 브랜드 추출. 없으면 brand=None.
    """
    result = {
        "brand":        None,
        "base_product": None,
        "flavor":       None,
        "format":       None,
        "package_size": None,
        "variant":      None,
    }
    s = (name_kr or "").strip()
    if not s:
        return result

    # 1) brand — 사전 prefix 매칭 (긴 이름부터)
    if known_brands:
        sorted_brands = sorted({b for b in known_brands if b}, key=len, reverse=True)
        for b in sorted_brands:
            if s.startswith(b):
                result["brand"] = b
                s = s[len(b):].strip()
                break

    # 2) package_size — 정규식
    m = _PACKAGE_SIZE_RE.search(s)
    if m:
        result["package_size"] = m.group(0)
        s = (s[: m.start()] + s[m.end():]).strip()

    # 3) format — 사전 매칭 (긴 것부터)
    for kr in sorted(FORMAT_DICT.keys(), key=len, reverse=True):
        if kr in s:
            result["format"] = kr
            s = s.replace(kr, "").strip()
            break

    # 4) flavor — 사전 매칭
    for kr in sorted(FLAVOR_DICT.keys(), key=len, reverse=True):
        if kr in s:
            result["flavor"] = kr
            s = s.replace(kr, "").strip()
            break

    # 5) 잔여 = base_product
    leftover = re.sub(r"\s+", " ", s).strip()
    result["base_product"] = leftover or None

    # 6) variant 보조: 'N입' 같이 package_size 정규식에 포함됐다면 별도 처리 불필요
    return result


def assemble_en(
    attrs: dict,
    brand_en: str | None = None,
    base_en: str | None = None,
) -> str:
    """속성 영문값으로 제품 영문명 조립.

    brand_en / base_en 는 외부에서 따로 매핑(예: 브랜드 사전, LLM)된 값.
    flavor/format 는 내부 사전에서 영문 자동 변환.
    """
    parts: list[str] = []
    if brand_en:
        parts.append(brand_en)
    elif attrs.get("brand"):
        parts.append(attrs["brand"])

    if base_en:
        parts.append(base_en)
    elif attrs.get("base_product"):
        parts.append(attrs["base_product"])

    if attrs.get("flavor"):
        parts.append(FLAVOR_DICT.get(attrs["flavor"], attrs["flavor"]))
    if attrs.get("format"):
        parts.append(FORMAT_DICT.get(attrs["format"], attrs["format"]))
    if attrs.get("package_size"):
        parts.append(attrs["package_size"])
    if attrs.get("variant"):
        parts.append(attrs["variant"])

    return " ".join(p for p in parts if p)
