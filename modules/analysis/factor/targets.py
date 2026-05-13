"""Forward Returns Target Builder.

PIT 가드:
  - signal_date를 받아서, 그 다음 영업일 가격 기준으로 forward N-day return 계산
  - 시작가는 signal_date+1d, 종가는 signal_date+1d+N
  - market-adjusted = stock 누적 수익률 - KOSPI 누적 수익률
  - sector-relative = stock 수익률 - 동일 sector 중간값
"""
from __future__ import annotations
import pandas as pd


# 분석 horizon (거래일 기준)
HORIZONS = {
    "1m": 21,
    "3m": 63,
    "6m": 126,
}


def _forward_compound_return(prices: pd.Series, n_days: int) -> pd.Series:
    """t+1 시작가 ~ t+1+n 종가 누적 수익률. PIT-safe."""
    # shift(-1) = t+1 가격, shift(-(n+1)) = t+n+1 가격
    p_start = prices.shift(-1)
    p_end   = prices.shift(-(n_days + 1))
    return (p_end / p_start - 1) * 100


def _norm_dt(s: pd.Series) -> pd.Series:
    """datetime 정규화 — tz-aware/naive·precision 통일 (datetime64[ns] tz-naive)."""
    s = pd.to_datetime(s, errors="coerce")
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        s = s.dt.tz_localize(None)
    return s.astype("datetime64[ns]")


def build_forward_returns(
    daily_prices: pd.DataFrame,
    bench_prices: pd.DataFrame | None = None,
    sector_master: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """daily prices → forward returns at signal_date.

    Parameters
    ----------
    daily_prices : DataFrame[stock_code, date, adj_close]
    bench_prices : DataFrame[date, adj_close] (KOSPI). 있으면 market-adjusted 계산.
    sector_master : DataFrame[stock_code, sector_gics]. 있으면 sector-relative 계산.

    Returns
    -------
    DataFrame[stock_code, date, fwd_1m, fwd_3m, fwd_6m,
              (fwd_excess_*), (fwd_sector_rel_*)]
    """
    if daily_prices.empty:
        return pd.DataFrame()

    p = daily_prices.copy().sort_values(["stock_code", "date"]).reset_index(drop=True)
    p["date"] = _norm_dt(p["date"])  # yfinance datetime[s] → datetime[ns]

    # Per-stock forward returns
    out_parts = []
    for code, grp in p.groupby("stock_code"):
        grp = grp.sort_values("date").copy()
        for h_label, n_days in HORIZONS.items():
            grp[f"fwd_{h_label}"] = _forward_compound_return(grp["adj_close"], n_days)
        out_parts.append(grp[["stock_code", "date"] + [f"fwd_{h}" for h in HORIZONS]])
    out = pd.concat(out_parts, ignore_index=True) if out_parts else pd.DataFrame()

    # Market-adjusted (vs KOSPI)
    if bench_prices is not None and not bench_prices.empty:
        bp = bench_prices.copy().sort_values("date").reset_index(drop=True)
        bp["date"] = _norm_dt(bp["date"])
        for h_label, n_days in HORIZONS.items():
            bp[f"bench_fwd_{h_label}"] = _forward_compound_return(bp["adj_close"], n_days)
        bench_keep = ["date"] + [f"bench_fwd_{h}" for h in HORIZONS]
        # out["date"]도 동일 정규화
        out["date"] = _norm_dt(out["date"])
        out = out.merge(bp[bench_keep], on="date", how="left")
        for h in HORIZONS:
            out[f"fwd_excess_{h}"] = out[f"fwd_{h}"] - out[f"bench_fwd_{h}"]

    # Sector-relative (sector median 차감 — 매 date × sector마다)
    if (sector_master is not None and not sector_master.empty
            and "sector_gics" in sector_master.columns
            and "stock_code" in sector_master.columns):
        sm = sector_master[["stock_code", "sector_gics"]].drop_duplicates("stock_code")
        out = out.merge(sm, on="stock_code", how="left")
        for h in HORIZONS:
            if f"fwd_{h}" not in out.columns:
                continue
            sec_med = out.groupby(["date", "sector_gics"])[f"fwd_{h}"].transform("median")
            out[f"fwd_sector_rel_{h}"] = out[f"fwd_{h}"] - sec_med

    return out


def join_signals_with_targets(
    panel: pd.DataFrame,
    forward_returns: pd.DataFrame,
) -> pd.DataFrame:
    """PIT panel(features 포함) ↔ forward returns at signal_date inner join.

    signal_date가 거래일이 아닐 수 있으므로, signal_date 이상인 가장 가까운 다음 거래일과 매칭.
    yfinance 1.3.0의 datetime[s] 단위와 pandas datetime[ns] 충돌 방지를 위해 명시적 정규화.
    """
    if panel.empty or forward_returns.empty:
        return pd.DataFrame()

    p = panel.copy().sort_values(["stock_code", "signal_date"]).reset_index(drop=True)
    fr = forward_returns.copy().sort_values(["stock_code", "date"]).reset_index(drop=True)

    # 단위 통일: datetime64[ns] tz-naive로 강제
    p["signal_date"] = _norm_dt(p["signal_date"])
    fr["date"]       = _norm_dt(fr["date"])

    # asof merge per stock_code (forward search to next trading day)
    parts = []
    for code, grp_p in p.groupby("stock_code"):
        grp_fr = fr[fr["stock_code"] == code].sort_values("date")
        if grp_fr.empty:
            continue
        merged = pd.merge_asof(
            grp_p.sort_values("signal_date"),
            grp_fr.drop(columns=["stock_code"]).sort_values("date"),
            left_on="signal_date", right_on="date",
            direction="forward",       # signal_date 이후 첫 거래일
            tolerance=pd.Timedelta(days=10),
        )
        parts.append(merged)

    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).dropna(subset=["date"])
