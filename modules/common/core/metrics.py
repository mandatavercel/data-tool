"""Core metrics — pure calculation functions, no Streamlit, no side effects.

All functions:
- Accept pandas Series or numpy arrays
- Return Series, DataFrame, or scalar float
- Never raise on empty input — return NaN / empty DataFrame instead
- Are independently testable
"""
import math
import numpy as np
import pandas as pd


# ── Growth rates ───────────────────────────────────────────────────────────────

def calculate_growth_rate(series: pd.Series, periods: int = 1) -> pd.Series:
    """Period-over-period percent change.

    Parameters
    ----------
    series  : numeric Series (already aggregated per period)
    periods : look-back window; 1 = consecutive, 4 = same quarter last year, etc.

    Returns
    -------
    Series of % growth values (NaN at the first `periods` positions)
    """
    return series.pct_change(periods=periods) * 100


def calculate_mom(series: pd.Series) -> pd.Series:
    """Month-over-month growth rate (%). Expects a monthly-indexed series."""
    return calculate_growth_rate(series, periods=1)


def calculate_qoq(series: pd.Series) -> pd.Series:
    """Quarter-over-quarter growth rate (%). Expects a quarterly-indexed series."""
    return calculate_growth_rate(series, periods=1)


def calculate_yoy(series: pd.Series, freq: str = "Q") -> pd.Series:
    """Year-over-year growth rate (%).

    Parameters
    ----------
    series : aggregated periodic series
    freq   : "Q" (quarterly, periods=4) | "M" (monthly, periods=12)
    """
    periods = 4 if freq.upper() == "Q" else 12
    return calculate_growth_rate(series, periods=periods)


# ── Correlation ────────────────────────────────────────────────────────────────

def calculate_correlation(
    a: "pd.Series | np.ndarray",
    b: "pd.Series | np.ndarray",
) -> float:
    """Pearson r between two series after dropping NaN pairs.

    Returns
    -------
    float in [-1, 1], or NaN when:
    - fewer than 2 valid pairs
    - either series has zero variance
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    valid = ~(np.isnan(a) | np.isnan(b))
    if valid.sum() < 2:
        return float("nan")
    a_v, b_v = a[valid], b[valid]
    if np.std(a_v) == 0 or np.std(b_v) == 0:
        return float("nan")
    r = float(np.corrcoef(a_v, b_v)[0, 1])
    return r if math.isfinite(r) else float("nan")


def calculate_lag_correlation(
    a: "pd.Series | np.ndarray",
    b: "pd.Series | np.ndarray",
    max_lag: int = 4,
    min_lag: int = -2,
    name_a: str = "a",
    name_b: str = "b",
) -> pd.DataFrame:
    """Pearson r at each integer lag between a and b.

    Convention
    ----------
    lag > 0  →  a leads b  (a observed lag periods before b)
    lag = 0  →  concurrent
    lag < 0  →  b leads a  (b observed −lag periods before a)

    Parameters
    ----------
    name_a, name_b : label strings for generated 'label' column

    Returns
    -------
    DataFrame: lag (int), label (str), r (float | None), n (int)
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    n = len(a)
    rows = []
    for lag in range(min_lag, max_lag + 1):
        if lag > 0:
            a_sl, b_sl = a[:n - lag], b[lag:]
            label = f"{name_a} {lag}기간 선행"
        elif lag < 0:
            a_sl, b_sl = a[-lag:], b[:n + lag]
            label = f"{name_b} {-lag}기간 선행"
        else:
            a_sl, b_sl = a, b
            label = "동행 (lag 0)"

        valid = ~(np.isnan(a_sl) | np.isnan(b_sl))
        n_v = int(valid.sum())
        if n_v >= 3 and np.std(a_sl[valid]) > 0 and np.std(b_sl[valid]) > 0:
            r = float(np.corrcoef(a_sl[valid], b_sl[valid])[0, 1])
            r = r if math.isfinite(r) else float("nan")
        else:
            r = float("nan")

        rows.append({
            "lag":   lag,
            "label": label,
            "r":     round(r, 3) if not math.isnan(r) else None,
            "n":     n_v,
        })
    return pd.DataFrame(rows)


# ── Ratio ──────────────────────────────────────────────────────────────────────

def calculate_tracking_ratio(
    pos_series: pd.Series,
    dart_series: pd.Series,
) -> pd.Series:
    """POS / DART * 100, element-wise.  Inf → NaN (division by zero guard)."""
    ratio = pos_series / dart_series * 100
    return ratio.replace([float("inf"), float("-inf")], float("nan"))
