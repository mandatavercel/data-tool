"""
kfnb_app/tagging.py — ③ 투자 테마 태깅 스테이지.

정규화된 SKU 테이블에 config.THEME_RULES 기반 boolean 태그를 부착하고,
사람이 읽기 좋은 단일 'investment_theme' 라벨 컬럼을 생성한다.
streamlit 비의존.
"""
from __future__ import annotations

import pandas as pd

from kfnb_app import config

# 신제품 판정 기준일 (이 날짜 이후 첫 등장 = 신제품)
NEW_SINCE = 20230101


def _all_tag_names() -> list[str]:
    names: list[str] = []
    for rules in config.THEME_RULES.values():
        for tag in rules:
            if tag not in names:
                names.append(tag)
    return names


def tag_skus(sku_df: pd.DataFrame, new_since: int = NEW_SINCE) -> pd.DataFrame:
    """SKU 테이블 → 테마 boolean 컬럼 + investment_theme 라벨 추가."""
    df = sku_df.copy()
    tag_names = _all_tag_names()
    for t in tag_names:
        df[f"tag_{t}"] = False

    cats = df.get("cat_l2", pd.Series([""] * len(df)))
    names = df["sku_name_kr"].astype(str)
    for i, (cat, nm) in enumerate(zip(cats, names)):
        rules = config.THEME_RULES.get(str(cat), {})
        for tag, kws in rules.items():
            if any(k in nm for k in kws):
                df.iat[i, df.columns.get_loc(f"tag_{tag}")] = True

    # 신제품 플래그
    fd = pd.to_numeric(df.get("first_date"), errors="coerce")
    df["tag_new"] = fd >= new_since

    # 사람이 읽는 단일 라벨
    label_order = ["spicy", "stir_fried", "black_bean", "premium",
                   "imported", "non_alcohol", "fruit", "low_abv"]
    pretty = {"spicy": "Spicy", "stir_fried": "Stir-fried",
              "black_bean": "Black-bean", "premium": "Premium",
              "imported": "Imported", "non_alcohol": "Non-alcohol",
              "fruit": "Fruit", "low_abv": "Low-ABV"}

    def _label(row) -> str:
        tags = [pretty[t] for t in label_order
                if f"tag_{t}" in row.index and row[f"tag_{t}"]]
        if row.get("tag_new"):
            tags.append("New")
        return "/".join(tags) if tags else "Standard"

    df["investment_theme"] = df.apply(_label, axis=1)
    return df


def theme_coverage(tagged_df: pd.DataFrame) -> dict[str, float]:
    """태그별 매출 비중(%) — 검증/리포트용."""
    total = pd.to_numeric(tagged_df["sales_amt"], errors="coerce").sum()
    out: dict[str, float] = {}
    if not total:
        return out
    for col in tagged_df.columns:
        if col.startswith("tag_"):
            s = pd.to_numeric(
                tagged_df.loc[tagged_df[col], "sales_amt"], errors="coerce").sum()
            out[col[4:]] = round(s / total * 100, 1)
    return out
