"""
kfnb_app/standardization/normalize.py — ② 정규화 스테이지.

SKU 차원(distinct_skus) 테이블을 받아 투자분석용으로 정리:
  - 회사명 prefix 제거 ("농심)신라면5입" → product 파싱)
  - pack_count 추출 ("5입", "*6", "20입")
  - size 추출 ("500ml", "1.8L")
  - package_format (cat_l3 또는 SKU명 토큰 → Bag/Cup/Can/Bottle …, 한글 미반환)
  - product_family / variant 분리
  - asp(₩) = sales_amt / sales_qty
streamlit 비의존, 순수 pandas.
"""
from __future__ import annotations

import re

import pandas as pd

from kfnb_app import config

_PREFIX_RE = re.compile(r"^[^)]*\)")              # "농심)" 제거
_PARN_RE = re.compile(r"\((구|신)\)")              # (구)/(신)
_PACK_IP = re.compile(r"(\d+)\s*(?:개입|입)")       # 5입 / 4개입
_PACK_X = re.compile(r"\*\s*(\d+)")               # *6 / *20
_SIZE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(ml|l|리터|g|kg)", re.IGNORECASE)
_UNIT_NORM = {"ml": "ml", "l": "L", "리터": "L", "g": "g", "kg": "kg"}


def parse_pack_count(name: str) -> int:
    """SKU명에서 멀티팩 수량 추출 (입/개입/*N). 없으면 1."""
    s = str(name)
    m = _PACK_IP.search(s)
    if m:
        return int(m.group(1))
    m = _PACK_X.search(s)
    if m:
        return int(m.group(1))
    return 1


def extract_size(name: str) -> tuple[str, str]:
    """SKU명에서 용량 추출 → (값, 단위). 없으면 ('','')."""
    m = _SIZE_RE.search(str(name))
    if not m:
        return "", ""
    return m.group(1), _UNIT_NORM.get(m.group(2).lower(), m.group(2))


def strip_company_prefix(name: str) -> str:
    return _PREFIX_RE.sub("", str(name)).strip()


def package_format(cat_l3: str, sku_name: str = "") -> str:
    """포장형태 추론. cat_l3 매핑 → SKU명 토큰 → 'Unknown'. 절대 한글 미반환."""
    c = str(cat_l3 or "")
    if c in config.PACKAGE_FORMAT_MAP:
        return config.PACKAGE_FORMAT_MAP[c]
    nm = str(sku_name)
    for tok, en in config.PACKAGE_NAME_TOKENS.items():   # 긴 토큰 먼저
        if tok in nm:
            return en
    return "Unknown"


def product_family(name: str, brand_kr: str) -> str:
    return str(brand_kr) if brand_kr else strip_company_prefix(name)


def variant(name: str, brand_kr: str) -> str:
    """브랜드·포장·용량·팩 토큰을 제거한 나머지 → variant(맛/타입) 라벨."""
    base = strip_company_prefix(name)
    if brand_kr and str(brand_kr) in base:
        base = base.replace(str(brand_kr), "", 1)
    base = _PACK_IP.sub("", base)
    base = _PACK_X.sub("", base)
    base = _SIZE_RE.sub("", base)
    # 포장 토큰 제거 (긴 것 먼저)
    for tok in list(config.PACKAGE_NAME_TOKENS) + ["사발면", "용기", "큰", "면"]:
        base = base.replace(tok, "")
    base = _PARN_RE.sub("", base)
    base = re.sub(r"\d+", " ", base)        # 잔여 숫자 제거
    return re.sub(r"\s+", " ", base).strip()


def normalize_skus(sku_df: pd.DataFrame) -> pd.DataFrame:
    """distinct_skus DataFrame → 정규화 컬럼 추가."""
    df = sku_df.copy()
    df["pack_count"] = df["sku_name_kr"].map(parse_pack_count)
    df["is_multipack"] = df["pack_count"] > 1
    sizes = [extract_size(nm) for nm in df["sku_name_kr"]]
    df["size_value"] = [v for v, _ in sizes]
    df["size_unit"] = [u for _, u in sizes]
    df["package_format"] = [
        package_format(c3, nm)
        for c3, nm in zip(df.get("cat_l3", ""), df["sku_name_kr"])
    ]
    df["product_family"] = [
        product_family(nm, b)
        for nm, b in zip(df["sku_name_kr"], df.get("brand_kr", ""))
    ]
    df["variant"] = [
        variant(nm, b)
        for nm, b in zip(df["sku_name_kr"], df.get("brand_kr", ""))
    ]
    qty = pd.to_numeric(df["sales_qty"], errors="coerce")
    amt = pd.to_numeric(df["sales_amt"], errors="coerce")
    df["asp_won"] = (amt / qty.where(qty > 0)).round(0)
    return df
