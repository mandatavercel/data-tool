"""Cross-sectional Neutralization — winsorize / sector z-score / mcap-quintile.

매 signal_date 단면에서 처리. 시계열은 건드리지 않음.
"""
from __future__ import annotations
import pandas as pd
import numpy as np


def neutralize(
    panel: pd.DataFrame,
    feature: str,
    methods: tuple[str, ...] = ("winsorize", "sector_z"),
    out_col: str | None = None,
    sector_col: str = "sector_gics",
    mcap_col: str = "market_cap",
    winsor_pct: tuple[float, float] = (0.01, 0.99),
) -> pd.DataFrame:
    """단계적 neutralization. 각 method를 같은 컬럼에 in-place 적용 → out_col 또는 feature.

    methods 옵션:
      - "winsorize"    : 매 단면에서 1%/99% 클립
      - "sector_z"     : sector 내 z-score
      - "rank_pct"     : sector 내 percentile rank (0~1)
      - "mcap_neutral" : sector × mcap quintile 내 평균 차감
      - "log"          : sign-preserving log1p (heavy-tail 압축)
    """
    if feature not in panel.columns:
        return panel
    p = panel.copy()
    work = out_col or feature

    # 입력 컬럼 → 작업 컬럼
    if work != feature:
        p[work] = p[feature]

    if "log" in methods:
        p[work] = np.sign(p[work]) * np.log1p(p[work].abs())

    if "winsorize" in methods:
        lo, hi = winsor_pct
        p[work] = p.groupby("signal_date")[work].transform(
            lambda s: s.clip(s.quantile(lo), s.quantile(hi))
        )

    if "sector_z" in methods and sector_col in p.columns:
        g = p.groupby(["signal_date", sector_col])[work]
        p[work] = (p[work] - g.transform("mean")) / g.transform("std").replace(0, np.nan)

    if "rank_pct" in methods and sector_col in p.columns:
        p[work] = p.groupby(["signal_date", sector_col])[work].rank(pct=True)

    if "mcap_neutral" in methods and sector_col in p.columns and mcap_col in p.columns:
        # sector × mcap quintile 내 평균 차감
        try:
            p["_mcap_q"] = p.groupby("signal_date")[mcap_col].transform(
                lambda s: pd.qcut(s, 5, labels=False, duplicates="drop")
            )
            g = p.groupby(["signal_date", sector_col, "_mcap_q"])[work]
            p[work] = p[work] - g.transform("mean")
            p = p.drop(columns=["_mcap_q"])
        except Exception:
            pass

    return p
