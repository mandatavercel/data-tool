"""
kfnb_app/panel.py — ⑤ 집계/패널 스테이지.

Source 에서 월별·연별 집계를 받아 ASP·YoY 를 계산하고 회사 식별자(④ mapping)를
부착한다. 백테스트용 PIT 패널의 기반.
streamlit 비의존.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

from kfnb_app.mapping import company as mapping
from kfnb_app.ingest.dataio import Source


def build_monthly_panel(src: Source, cat_l2: Optional[str] = None) -> pd.DataFrame:
    """월별 회사·브랜드 패널 + ASP + 티커 매핑."""
    p = src.monthly_panel(cat_l2)
    if p.empty:
        return p
    p = mapping.map_companies(p)
    qty = pd.to_numeric(p["sales_qty"], errors="coerce")
    p["asp_won"] = (pd.to_numeric(p["sales_amt"], errors="coerce")
                    / qty.where(qty > 0)).round(0)
    return p


def build_annual_company(src: Source, cat_l2: Optional[str] = None) -> pd.DataFrame:
    """연도별 회사 매출 + YoY% (회사별)."""
    a = src.annual_company(cat_l2)
    if a.empty:
        return a
    a = mapping.map_companies(a)
    a = a.sort_values(["company_kr", "yr"])
    a["yoy_pct"] = (a.groupby("company_kr")["sales_amt"]
                    .pct_change() * 100).round(1)
    return a.reset_index(drop=True)


def build_brand_trend(src: Source, brand_kr: str) -> pd.DataFrame:
    """단일 브랜드 연도별 추세 + YoY% (모멘텀 분석용)."""
    b = src.annual_brand(brand_kr)
    if b.empty:
        return b
    b = b.sort_values("yr")
    b["yoy_pct"] = (b["sales_amt"].pct_change() * 100).round(1)
    qty = pd.to_numeric(b["sales_qty"], errors="coerce")
    b["asp_won"] = (pd.to_numeric(b["sales_amt"], errors="coerce")
                    / qty.where(qty > 0)).round(0)
    return b.reset_index(drop=True)


def asp_outliers(panel_df: pd.DataFrame, lo: float, hi: float) -> pd.DataFrame:
    """ASP sanity 범위를 벗어난 행 (검증용)."""
    if "asp_won" not in panel_df:
        return panel_df.iloc[0:0]
    a = pd.to_numeric(panel_df["asp_won"], errors="coerce")
    return panel_df[(a < lo) | (a > hi)]
