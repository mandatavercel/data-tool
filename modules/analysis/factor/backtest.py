"""Quintile Backtest — feature 기준 분위 long-short 포트폴리오 검증.

매 signal_date에서 feature 분위로 종목 분류 → quintile별 평균 forward return →
top quintile (Q5) - bottom quintile (Q1) = long-short 수익률.
"""
from __future__ import annotations
import math
import numpy as np
import pandas as pd


def quintile_backtest(
    panel: pd.DataFrame,
    feature: str,
    target: str = "fwd_1m",
    n_groups: int = 5,
    sector_neutral: bool = True,
    sector_col: str = "sector_gics",
    cost_bps: float = 15.0,
    rebalance_freq: str = "1m",
) -> dict:
    """Quintile sort backtest.

    Returns dict:
      portfolio    : DataFrame[date, q1..q5, LS, LS_after_cost, cum_LS]
      sharpe       : float (annualized)
      sharpe_after : float
      max_dd       : float (%)
      annual_ret   : float (%)
      hit_rate     : float
      turnover     : float (avg rank change ratio)
      n_periods    : int
    """
    if panel.empty or feature not in panel.columns or target not in panel.columns:
        return _empty_bt()

    # sector_gics 컬럼 부재 시 sector_neutral 자동 off
    can_sector_neutral = sector_neutral and (sector_col in panel.columns)

    required_cols = ["signal_date", "stock_code", feature, target]
    if can_sector_neutral:
        required_cols.append(sector_col)
    df = panel[required_cols].dropna(subset=[feature, target])
    if df.empty:
        return _empty_bt()

    # 분위 분류
    if can_sector_neutral:
        df["q"] = df.groupby(["signal_date", sector_col])[feature].transform(
            lambda s: pd.qcut(s, n_groups, labels=False, duplicates="drop") + 1
            if s.notna().sum() >= n_groups else np.nan
        )
    else:
        df["q"] = df.groupby("signal_date")[feature].transform(
            lambda s: pd.qcut(s, n_groups, labels=False, duplicates="drop") + 1
            if s.notna().sum() >= n_groups else np.nan
        )
    df = df.dropna(subset=["q"])
    if df.empty:
        return _empty_bt()
    df["q"] = df["q"].astype(int)

    # 분위별 평균 forward return (단순 equal-weight)
    portfolio = (
        df.groupby(["signal_date", "q"])[target]
        .mean()
        .unstack("q")
        .sort_index()
    )
    portfolio.columns = [f"q{int(c)}" for c in portfolio.columns]

    if f"q{n_groups}" not in portfolio.columns or "q1" not in portfolio.columns:
        return _empty_bt()
    portfolio["LS"] = portfolio[f"q{n_groups}"] - portfolio["q1"]

    # Turnover (분위 변경 빈도)
    rank_t = df.pivot_table(index="signal_date", columns="stock_code", values="q", aggfunc="first")
    if len(rank_t) > 1:
        rank_changed = (rank_t.diff().abs() > 0).mean(axis=1).fillna(0)
        turnover_avg = float(rank_changed.mean())
    else:
        rank_changed = pd.Series(0, index=rank_t.index)
        turnover_avg = 0.0

    # 비용 차감 (양 매도매수 → cost_bps × 2 × turnover)
    portfolio["turnover"] = rank_changed.reindex(portfolio.index).fillna(0)
    portfolio["LS_after_cost"] = portfolio["LS"] - portfolio["turnover"] * (cost_bps / 1e4) * 2 * 100

    # 누적 수익률 (단순 합산 — annualized 비교 의미용)
    portfolio["cum_LS"]            = portfolio["LS"].cumsum()
    portfolio["cum_LS_after_cost"] = portfolio["LS_after_cost"].cumsum()

    # Performance metrics (rebalance_freq 기준 연환산)
    periods_per_year = {"1m": 12, "3m": 4, "6m": 2}.get(rebalance_freq, 12)
    ls = portfolio["LS"].dropna()
    ls_ac = portfolio["LS_after_cost"].dropna()

    sharpe       = (ls.mean() / ls.std() * math.sqrt(periods_per_year)) if ls.std() > 0 else 0.0
    sharpe_after = (ls_ac.mean() / ls_ac.std() * math.sqrt(periods_per_year)) if ls_ac.std() > 0 else 0.0
    annual_ret   = float(ls.mean() * periods_per_year)
    hit_rate     = float((ls > 0).mean())

    # Max drawdown of cum_LS (단순 합산 기준)
    cum   = portfolio["cum_LS"].dropna()
    peak  = cum.cummax()
    dd    = cum - peak
    max_dd = float(dd.min()) if not dd.empty else 0.0

    return {
        "portfolio":    portfolio.reset_index(),
        "sharpe":       round(sharpe, 2),
        "sharpe_after": round(sharpe_after, 2),
        "max_dd":       round(max_dd, 2),
        "annual_ret":   round(annual_ret, 2),
        "hit_rate":     round(hit_rate, 3),
        "turnover":     round(turnover_avg, 3),
        "n_periods":    int(len(ls)),
        "cost_bps":     cost_bps,
    }


def _empty_bt() -> dict:
    return {
        "portfolio":    pd.DataFrame(),
        "sharpe":       0.0, "sharpe_after": 0.0,
        "max_dd":       0.0, "annual_ret":   0.0,
        "hit_rate":     0.0, "turnover":     0.0,
        "n_periods":    0,   "cost_bps":     0.0,
    }
