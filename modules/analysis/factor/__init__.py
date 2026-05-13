"""Factor Research Pipeline — POS 등 alt data로 미래 수익률 예측 가능성 검증.

핵심 흐름:
  1. PIT Panel        — sales(stock × month) + available_date
  2. Features         — YoY, ΔYoY, acceleration
  3. Targets          — forward 1m/3m/6m return (raw / market-adj / sector-rel)
  4. CS Rank IC       — Spearman, sector-neutral
  5. Quintile Backtest — long-short portfolio with Sharpe/turnover/maxDD
"""
from modules.analysis.factor.sector    import fetch_sector_master, GICS_SECTORS
from modules.analysis.factor.panel     import build_pit_panel
from modules.analysis.factor.features  import build_features, FEATURE_DEFS
from modules.analysis.factor.targets   import build_forward_returns
from modules.analysis.factor.ic        import cross_sectional_rank_ic, time_series_ic
from modules.analysis.factor.neutralize import neutralize
from modules.analysis.factor.backtest  import quintile_backtest

__all__ = [
    "fetch_sector_master", "GICS_SECTORS",
    "build_pit_panel",
    "build_features", "FEATURE_DEFS",
    "build_forward_returns",
    "cross_sectional_rank_ic", "time_series_ic",
    "neutralize",
    "quintile_backtest",
]
