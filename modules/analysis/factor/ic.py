"""IC Engines — TS IC (per-stock) + CS Rank IC (factor 검증의 핵심).

CS Rank IC가 main metric. TS IC는 진단용 (특정 회사가 universe와 다른지 확인).
"""
from __future__ import annotations
import math
import pandas as pd
from scipy.stats import spearmanr, pearsonr


def cross_sectional_rank_ic(
    panel: pd.DataFrame,
    feature: str,
    target: str,
    method: str = "spearman",
    min_stocks: int = 5,
) -> dict:
    """매 signal_date에서 feature rank vs target rank의 Spearman 상관.

    Returns dict with:
      ic_mean, ic_std, ic_t, icir, hit_rate, n_periods, ic_series (DataFrame)
    """
    if feature not in panel.columns or target not in panel.columns:
        return _empty_result()

    df = panel[["signal_date", "stock_code", feature, target]].dropna()
    if df.empty:
        return _empty_result()

    daily_ic = []
    for date, grp in df.groupby("signal_date"):
        if len(grp) < min_stocks:
            continue
        if method == "spearman":
            ic, _ = spearmanr(grp[feature], grp[target], nan_policy="omit")
        else:
            ic, _ = pearsonr(grp[feature], grp[target])
        if not (ic is None or (isinstance(ic, float) and math.isnan(ic))):
            daily_ic.append({"date": date, "ic": float(ic), "n": len(grp)})

    if not daily_ic:
        return _empty_result()

    ic_series = pd.DataFrame(daily_ic).sort_values("date").reset_index(drop=True)
    ic_mean   = float(ic_series["ic"].mean())
    ic_std    = float(ic_series["ic"].std()) if len(ic_series) > 1 else 0.0
    n_periods = len(ic_series)
    # t-statistic of IC mean (assumes IC iid across periods)
    ic_t  = (ic_mean / (ic_std / math.sqrt(n_periods))) if ic_std > 1e-9 else 0.0
    # ICIR — IC mean / IC std (per-period). 연환산 필요시 caller가 곱함.
    icir  = ic_mean / ic_std if ic_std > 1e-9 else 0.0
    hit   = float((ic_series["ic"] > 0).mean())

    # Rolling IC (12-period)
    ic_series["ic_rolling12"] = ic_series["ic"].rolling(12, min_periods=4).mean()

    return {
        "ic_mean":   ic_mean,
        "ic_std":    ic_std,
        "ic_t":      ic_t,
        "icir":      icir,
        "hit_rate":  hit,
        "n_periods": n_periods,
        "ic_series": ic_series,
        "method":    method,
    }


def time_series_ic(
    panel: pd.DataFrame,
    feature: str,
    target: str,
    method: str = "spearman",
    min_obs: int = 8,
) -> pd.DataFrame:
    """기업별 시계열 상관 — 진단용. CS IC가 약하면서 TS IC만 강하면 spurious 의심."""
    if feature not in panel.columns or target not in panel.columns:
        return pd.DataFrame()

    df = panel[["stock_code", feature, target]].dropna()
    rows = []
    for code, grp in df.groupby("stock_code"):
        if len(grp) < min_obs:
            continue
        if method == "spearman":
            ic, p = spearmanr(grp[feature], grp[target], nan_policy="omit")
        else:
            ic, p = pearsonr(grp[feature], grp[target])
        if ic is None or (isinstance(ic, float) and math.isnan(ic)):
            continue
        rows.append({
            "stock_code": code,
            "ts_ic":      float(ic),
            "p_value":    float(p),
            "n":          len(grp),
        })
    return pd.DataFrame(rows).sort_values("ts_ic", ascending=False).reset_index(drop=True)


def lag_decay_ics(
    panel: pd.DataFrame,
    feature: str,
    target_horizons: tuple[str, ...] = ("1m", "3m", "6m"),
    method: str = "spearman",
    target_kind: str = "raw",  # "raw" | "excess" | "sector_rel"
) -> pd.DataFrame:
    """여러 horizon에서 CS Rank IC 계산해서 decay curve로 리턴."""
    rows = []
    prefix = {"raw": "fwd_", "excess": "fwd_excess_", "sector_rel": "fwd_sector_rel_"}.get(
        target_kind, "fwd_"
    )
    for h in target_horizons:
        target = f"{prefix}{h}"
        if target not in panel.columns:
            continue
        r = cross_sectional_rank_ic(panel, feature, target, method=method)
        if r["n_periods"] == 0:
            continue
        rows.append({
            "horizon":   h,
            "ic_mean":   r["ic_mean"],
            "ic_t":      r["ic_t"],
            "icir":      r["icir"],
            "hit_rate":  r["hit_rate"],
            "n_periods": r["n_periods"],
        })
    return pd.DataFrame(rows)


def _empty_result() -> dict:
    return {
        "ic_mean": 0.0, "ic_std": 0.0, "ic_t": 0.0,
        "icir": 0.0, "hit_rate": 0.0, "n_periods": 0,
        "ic_series": pd.DataFrame(columns=["date", "ic", "n"]),
        "method": "spearman",
    }
