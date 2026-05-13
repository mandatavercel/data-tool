"""Feature Engineering — sales panel → factor features.

핵심 features (헤지펀드 표준):
  - sales_yoy           : 12개월 전 대비 % 성장률 (계절성 제거된 모멘텀)
  - sales_yoy_delta     : YoY의 변화 (acceleration proxy)
  - sales_yoy_smooth3   : YoY의 3개월 이동평균 (smoothing)
  - sales_acceleration  : YoY의 1차 차분
  - sales_yoy_z6m       : YoY의 6M rolling z-score (이상치 정규화)
  - sales_yoy_rank      : 동일 시점 cross-sectional rank percentile

거래건수 (있으면):
  - tx_yoy              : 거래수 YoY
  - atv (avg ticket)    : sales / tx_count
  - atv_yoy             : ATV YoY
  - price_effect        : sales_yoy - tx_yoy (가격 효과 vs 수량 효과)
"""
from __future__ import annotations
import pandas as pd
import numpy as np


# 단일 출처 of truth — UI 표시용
FEATURE_DEFS = {
    "sales_yoy":          ("매출 YoY (%)",                 "전년 동월 대비 매출 성장률. 계절성 제거된 모멘텀."),
    "sales_yoy_delta":    ("매출 YoY ΔYoY (acceleration)", "YoY의 1차 차분. 성장이 가속/감속하는 변곡점 포착."),
    "sales_yoy_smooth3":  ("매출 YoY (3M 평활)",            "YoY 3개월 이동평균. 노이즈 제거한 추세."),
    "sales_yoy_z6m":      ("매출 YoY z-score (6M)",        "최근 6개월 평균 대비 표준화. 이상 가속 탐지."),
    "tx_yoy":             ("거래건수 YoY (%)",              "거래수 기준 성장률. ATV 효과 제거된 순수 수요."),
    "atv":                ("평균 객단가 (ATV)",             "sales / tx_count. 가격대 변동 추적."),
    "atv_yoy":            ("ATV YoY (%)",                   "객단가 YoY. 가격 효과만 분리."),
    "price_effect":       ("Price effect (sales − tx YoY)", "매출 YoY와 거래수 YoY의 차이. >0이면 단가 상승 주도."),
}


def build_features(panel: pd.DataFrame) -> pd.DataFrame:
    """PIT panel → feature 추가.

    Parameters
    ----------
    panel : build_pit_panel() 결과. 컬럼: stock_code, sales_month, sales, [tx_count], ...

    Returns
    -------
    panel + feature 컬럼들
    """
    p = panel.copy().sort_values(["stock_code", "sales_month"]).reset_index(drop=True)
    g = p.groupby("stock_code", group_keys=False)

    # 매출 YoY (12개월 전 대비)
    p["sales_yoy"] = g["sales"].pct_change(periods=12) * 100

    # YoY의 변화 = acceleration
    p["sales_yoy_delta"] = g["sales_yoy"].diff()

    # YoY 3M 평활
    p["sales_yoy_smooth3"] = (
        g["sales_yoy"]
        .transform(lambda s: s.rolling(3, min_periods=2).mean())
    )

    # YoY 6M z-score
    p["sales_yoy_z6m"] = g["sales_yoy"].transform(
        lambda s: (s - s.rolling(6, min_periods=3).mean())
                  / s.rolling(6, min_periods=3).std().replace(0, np.nan)
    )

    # 거래수 features (있으면)
    if "tx_count" in p.columns:
        p["tx_yoy"]  = g["tx_count"].pct_change(periods=12) * 100
        p["atv"]     = p["sales"] / p["tx_count"].replace(0, np.nan)
        p["atv_yoy"] = g["atv"].pct_change(periods=12) * 100
        p["price_effect"] = p["sales_yoy"] - p["tx_yoy"]

    # ±inf → NaN
    p = p.replace([float("inf"), float("-inf")], pd.NA)

    return p


def available_features(panel_with_features: pd.DataFrame) -> list[str]:
    """현재 panel에 실제로 데이터가 있는 feature만 리턴 (UI selector용)."""
    return [f for f in FEATURE_DEFS if f in panel_with_features.columns
            and panel_with_features[f].notna().any()]
