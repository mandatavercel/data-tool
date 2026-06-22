"""
kfnb_app/standardization/text_cleaning.py — 한글 상품/브랜드명 정규화.

표기 흔들림(공백·특수문자·팩 표현·단위)을 통일해 매핑 일관성을 높인다.
원본은 절대 버리지 않는다 — 호출부에서 *_ko 원본 컬럼을 별도 보존한다.
"""
from __future__ import annotations

import re

_WS_RE = re.compile(r"\s+")
_PACK_RE = re.compile(r"(\d+)\s*(?:개입|입|개|p|pack|팩)\b", re.IGNORECASE)
_PROMO_TOKENS = ["행사", "증정", "기획", "묶음", "할인", "단독", "특가"]


def normalize_spaces(s: str) -> str:
    return _WS_RE.sub(" ", str(s)).strip()


def standardize_punctuation(s: str) -> str:
    """구분자(/ _ - ·) → 공백, 괄호 정리."""
    s = re.sub(r"[\/_\-·]+", " ", str(s))
    s = re.sub(r"[()\[\]{}]", " ", s)
    return normalize_spaces(s)


def standardize_pack(s: str) -> str:
    """'4개입','4입','4P' → '4-Pack' 로 통일."""
    return _PACK_RE.sub(lambda m: f"{m.group(1)}-Pack", str(s))


def strip_promo_tokens(s: str) -> tuple[str, list[str]]:
    """프로모션성 단어를 본문에서 떼어내고 태그로 반환."""
    found = [t for t in _PROMO_TOKENS if t in str(s)]
    out = str(s)
    for t in found:
        out = out.replace(t, " ")
    return normalize_spaces(out), found


def clean_korean_name(s: str) -> str:
    """공백·특수문자·팩 표현 통일한 정제 한글명 (원본은 별도 보존 전제)."""
    s = standardize_punctuation(s)
    s = standardize_pack(s)
    return normalize_spaces(s)
