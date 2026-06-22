"""
kfnb_app/mapping/coverage.py — 매핑 커버리지 (매출 기준).

투자기관은 SKU '개수'보다 '매출의 몇 %가 정확히 매핑됐는가'를 본다.
회사/브랜드/SKU/고신뢰 커버리지를 매출 기준으로 계산한다.
"""
from __future__ import annotations

import pandas as pd


def _amt(df: pd.DataFrame, mask=None) -> float:
    s = pd.to_numeric(df["sales_amt"], errors="coerce")
    return float(s[mask].sum() if mask is not None else s.sum())


def coverage_by_sales(sku_master: pd.DataFrame) -> dict:
    """매출기준 커버리지 요약 dict.

    주의(감사 D4 반영): 'verified' 커버리지는 사람이 검수한 큐레이션 매핑 매출%이며,
    'named'(영문명 비어있지 않음)는 로마자 폴백 포함이라 항상 ~100% 이므로 별도 표기한다.
    """
    total = _amt(sku_master)
    if not total:
        return {"total_sales": 0.0}

    company_ok = sku_master["company_en"].astype(str).str.len() > 0 \
        if "company_en" in sku_master else pd.Series(False, index=sku_master.index)
    status = sku_master.get("mapping_status", pd.Series("", index=sku_master.index))
    verified = status == "verified"          # 큐레이션 검수 완료
    named = sku_master.get("sku_name_en", pd.Series("", index=sku_master.index)).astype(str).str.len() > 0
    high_ok = sku_master.get("mapping_confidence", pd.Series("", index=sku_master.index)) == "high"
    listed_ok = sku_master.get("listed", pd.Series(False, index=sku_master.index)) == True  # noqa: E712

    def pct(mask):
        return round(_amt(sku_master, mask) / total * 100, 1)

    n_sku = sku_master["sku_id"].nunique() if "sku_id" in sku_master else len(sku_master)
    return {
        "total_sales": total,
        "company_coverage_pct": pct(company_ok),         # 회사 매핑(미매핑=0)
        "sku_verified_pct": pct(verified),               # ★ 검수 완료 매출%
        "sku_named_pct": pct(named),                     # 영문명 부여(로마자 포함, 참고)
        "sku_coverage_pct": pct(verified),               # 기본 커버리지 = verified
        "high_confidence_pct": pct(high_ok),
        "listed_coverage_pct": pct(listed_ok),
        "sku_count": n_sku,
        "needs_review_sku_count": int((~verified).sum()),
    }


def coverage_table(sku_master: pd.DataFrame) -> pd.DataFrame:
    """리포트용 한 줄 테이블 (mapping_coverage.csv)."""
    c = coverage_by_sales(sku_master)
    rows = [
        ("company_mapped_sales_coverage", c.get("company_coverage_pct", 0)),
        ("sku_verified_sales_coverage", c.get("sku_verified_pct", 0)),
        ("sku_named_sales_coverage_incl_romanized", c.get("sku_named_pct", 0)),
        ("high_confidence_sales_coverage", c.get("high_confidence_pct", 0)),
        ("listed_company_sales_coverage", c.get("listed_coverage_pct", 0)),
    ]
    return pd.DataFrame(rows, columns=["metric", "coverage_pct"])
