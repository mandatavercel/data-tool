"""
한글 → 로마자 표기 (Revised Romanization of Korean, 간단 구현).

KIPRIS·DART·LLM 모두 실패한 최후의 폴백. 공식 영문 표기와는 다를 수 있으므로
신뢰도는 낮게(0.4) 매겨진다.
"""
from __future__ import annotations


# 초성·중성·종성 매핑 (RR 표준 — 일부 보수적)
INITIAL = [
    "g", "kk", "n", "d", "tt", "r", "m", "b", "pp", "s",
    "ss", "", "j", "jj", "ch", "k", "t", "p", "h",
]
MEDIAL = [
    "a", "ae", "ya", "yae", "eo", "e", "yeo", "ye", "o", "wa",
    "wae", "oe", "yo", "u", "wo", "we", "wi", "yu", "eu", "ui", "i",
]
FINAL = [
    "", "k", "k", "ks", "n", "nj", "nh", "t", "l", "lk",
    "lm", "lp", "ls", "lt", "lp", "lh", "m", "p", "ps", "t",
    "t", "ng", "j", "ch", "k", "t", "p", "h",
]


def hangul_to_roman(s: str) -> str:
    """한글 음절을 로마자로. 비한글 문자는 그대로 유지."""
    if not s:
        return ""
    out: list[str] = []
    for ch in s:
        code = ord(ch)
        if 0xAC00 <= code <= 0xD7A3:
            base = code - 0xAC00
            i = base // 588
            m = (base % 588) // 28
            f = base % 28
            out.append(INITIAL[i] + MEDIAL[m] + FINAL[f])
        else:
            out.append(ch)
    return "".join(out)


def romanize_brand(name_kr: str) -> str:
    """브랜드 — 단어별 capitalize. 예: '신라면' → 'Sinramyeon', '농심' → 'Nongsim'."""
    if not name_kr:
        return ""
    parts = name_kr.split()
    return " ".join(hangul_to_roman(p).capitalize() for p in parts if p)


def romanize_product(name_kr: str) -> str:
    """제품 — 브랜드와 동일 규칙."""
    return romanize_brand(name_kr)
