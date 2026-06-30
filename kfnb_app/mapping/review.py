"""
kfnb_app/mapping/review.py — 매출 가중 영문명 검수 큐.

"미검증(로마자 폴백) 매핑 중 매출이 큰 것부터" 띄워서, 적은 노력으로 최대 커버리지를
확정하게 한다. 상위 몇 개만 손보면 보통 매출의 80~90%가 검증된다.

  brand_review_queue() : 미큐레이션 브랜드 (매출순 + 누적%)
  sku_review_queue()   : 미검증 SKU (매출순 + 누적%)
  coverage_summary()   : 검증/미검증 매출 비중
  apply_brand_overrides() : 사람이 확정한 영문명을 sku_master 에 반영
  overrides_to_master_csv(): 확정분을 brand_master.csv 에 추가할 형태로 내보냄
streamlit 비의존.
"""
from __future__ import annotations

import pandas as pd

from kfnb_app import config


def _amt(df, col="sales_amt"):
    return pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0.0)


def coverage_summary(sku_master: pd.DataFrame, amount_col: str = "sales_amt") -> dict:
    """브랜드/SKU 검증 매출 비중. (curated=사전 등록 기준)"""
    sm = sku_master.copy()
    sm["_amt"] = _amt(sm, amount_col)
    total = sm["_amt"].sum() or 1.0
    sm["_brand_curated"] = sm.apply(
        lambda r: config.brand_known(r["company_kr"], r["brand_kr"]), axis=1)
    conf = sm.get("mapping_confidence", pd.Series("", index=sm.index)).astype(str)
    sm["_sku_verified"] = conf.eq("high")
    return {
        "brand_verified_pct": round(sm.loc[sm["_brand_curated"], "_amt"].sum() / total * 100, 1),
        "sku_verified_pct": round(sm.loc[sm["_sku_verified"], "_amt"].sum() / total * 100, 1),
        "total_sales": float(total),
    }


def brand_review_queue(sku_master: pd.DataFrame, amount_col: str = "sales_amt",
                       top: int | None = None) -> pd.DataFrame:
    """미큐레이션 브랜드 — 매출 내림차순 + 누적 비중(%). 큰 것부터 확정."""
    sm = sku_master.copy()
    sm["_amt"] = _amt(sm, amount_col)
    g = (sm.groupby(["company_kr", "brand_kr"])
         .agg(sales=("_amt", "sum"),
              brand_en=("brand_name_en", "first"),
              brand_id=("brand_id", "first")).reset_index())
    g["curated"] = g.apply(
        lambda r: config.brand_known(r["company_kr"], r["brand_kr"]), axis=1)
    total = g["sales"].sum() or 1.0
    q = g[~g["curated"]].sort_values("sales", ascending=False).copy()
    q["sales_pct"] = (q["sales"] / total * 100).round(2)
    q["cum_pct"] = q["sales_pct"].cumsum().round(2)
    cols = ["company_kr", "brand_kr", "brand_en", "sales", "sales_pct",
            "cum_pct", "brand_id"]
    return (q.head(top) if top else q)[cols].reset_index(drop=True)


def sku_review_queue(sku_master: pd.DataFrame, amount_col: str = "sales_amt",
                     top: int | None = None) -> pd.DataFrame:
    """미검증 SKU(로마자/저신뢰) — 매출 내림차순 + 누적 비중(%)."""
    sm = sku_master.copy()
    sm["_amt"] = _amt(sm, amount_col)
    conf = sm.get("mapping_confidence", pd.Series("", index=sm.index)).astype(str)
    q = sm[~conf.eq("high")].sort_values("_amt", ascending=False).copy()
    total = sm["_amt"].sum() or 1.0
    q["sales_pct"] = (q["_amt"] / total * 100).round(3)
    q["cum_pct"] = q["sales_pct"].cumsum().round(2)
    cols = [c for c in ["company_kr", "brand_kr", "sku_name_kr", "sku_name_en",
                        "barcode", "_amt", "sales_pct", "cum_pct",
                        "mapping_confidence"] if c in q.columns]
    out = (q.head(top) if top else q)[cols].rename(columns={"_amt": "sales"})
    return out.reset_index(drop=True)


def apply_brand_overrides(sku_master: pd.DataFrame, overrides: dict) -> pd.DataFrame:
    """{(company_kr, brand_kr): brand_en} 확정값을 sku_master 에 반영(이번 런)."""
    if not overrides:
        return sku_master
    sm = sku_master.copy()
    for (co, br), en in overrides.items():
        if not str(en).strip():
            continue
        m = (sm["company_kr"] == co) & (sm["brand_kr"] == br)
        sm.loc[m, "brand_name_en"] = str(en).strip()
        if "mapping_confidence" in sm.columns:
            sm.loc[m, "mapping_confidence"] = "high"
    return sm


def overrides_to_master_csv(sku_master: pd.DataFrame, overrides: dict) -> pd.DataFrame:
    """확정분을 brand_master.csv 에 append 할 형태로 변환(영속 큐레이션용).

    컬럼: company_kr, brand_kr, brand_id, brand_en, aliases
    """
    rows = []
    bm = sku_master.drop_duplicates(["company_kr", "brand_kr"]).set_index(
        ["company_kr", "brand_kr"])
    for (co, br), en in (overrides or {}).items():
        if not str(en).strip():
            continue
        bid = ""
        if (co, br) in bm.index:
            bid = str(bm.loc[(co, br)].get("brand_id", "") or "")
        rows.append({"company_kr": co, "brand_kr": br, "brand_id": bid,
                     "brand_en": str(en).strip(), "aliases": br})
    return pd.DataFrame(rows, columns=["company_kr", "brand_kr", "brand_id",
                                       "brand_en", "aliases"])
