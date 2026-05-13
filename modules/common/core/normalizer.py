"""Core normalizer — unit-safe transformations, date/numeric parsing.

No Streamlit.  No side effects.  Returns new Series / scalars.
"""
import math
import pandas as pd


# ── Date ──────────────────────────────────────────────────────────────────────

def normalize_date(series: pd.Series) -> pd.Series:
    """Parse a heterogeneous date column to datetime64[ns].

    Handles: YYYYMMDD (int or str), YYYY-MM-DD, YYYY/MM/DD,
    and any format parseable by pandas.  Delegates to foundation._parse_dates
    so that the detection logic stays in a single place.
    """
    from modules.common.foundation import _parse_dates
    return _parse_dates(series)


# ── Numeric ───────────────────────────────────────────────────────────────────

def normalize_numeric(
    series: pd.Series,
    fill_value: "float | None" = None,
) -> pd.Series:
    """Coerce to float64.  Optionally fill NaN with fill_value."""
    result = pd.to_numeric(series, errors="coerce")
    if fill_value is not None:
        result = result.fillna(fill_value)
    return result


# ── Amount unit conversion ────────────────────────────────────────────────────

_UNIT_MULTIPLIERS: dict[str, int] = {
    "원":     1,
    "천원":   1_000,
    "만원":   10_000,
    "백만원": 1_000_000,
    "억원":   100_000_000,
    "십억원": 1_000_000_000,
}


def normalize_amount_to_krw(series: pd.Series, unit: str) -> pd.Series:
    """Scale a monetary series to 원(KRW) using a named unit string.

    Parameters
    ----------
    unit : one of 원 / 천원 / 만원 / 백만원 / 억원 / 십억원
           Unknown units pass through unchanged (multiplier = 1).
    """
    multiplier = _UNIT_MULTIPLIERS.get(unit, 1)
    return series * multiplier


def infer_amount_unit(pos_mean: float, dart_mean: float) -> dict:
    """Compare average POS and DART amounts to infer POS unit mismatch.

    DART API output is always in 원(KRW).  If POS uses a different unit the
    ratio will be far from 1×.

    Returns
    -------
    dict
        ratio        : float   pos_mean / dart_mean
        unit_type    : str     "원" | "천원" | "백만원" | "억원" | "unknown" | "ok"
        note         : str     human-readable diagnosis
        is_mismatch  : bool
    """
    _nan = {"ratio": float("nan"), "unit_type": "unknown", "note": "", "is_mismatch": False}
    if dart_mean == 0 or math.isnan(dart_mean) or math.isnan(pos_mean):
        return _nan

    ratio = pos_mean / dart_mean

    if 8_000 < ratio < 12_000:
        return {"ratio": ratio, "unit_type": "억원",
                "note": f"POS/DART 비율 ≈{ratio:.0f}× → POS 억원 vs DART 원 단위 의심",
                "is_mismatch": True}
    if 800 < ratio < 1_200:
        return {"ratio": ratio, "unit_type": "백만원",
                "note": f"POS/DART 비율 ≈{ratio:.0f}× → POS 백만원 vs DART 원 단위 의심",
                "is_mismatch": True}
    if 80 < ratio < 120:
        return {"ratio": ratio, "unit_type": "천원",
                "note": f"POS/DART 비율 ≈{ratio:.0f}× → POS 천원 vs DART 원 단위 의심",
                "is_mismatch": True}
    if 0.1 < ratio < 10:
        return {"ratio": ratio, "unit_type": "원",
                "note": "단위 일치 (원 KRW 기준)",
                "is_mismatch": False}
    return {"ratio": ratio, "unit_type": "unknown",
            "note": f"POS/DART 비율 ≈{ratio:.0f}× — 비표준 단위 또는 집계 범위 차이 가능",
            "is_mismatch": ratio > 10}


# ── Score normalisation ───────────────────────────────────────────────────────

def normalize_score(val: float, lo: float = -50.0, hi: float = 50.0) -> float:
    """Clamp-normalize val to [0, 100].  NaN / Inf → 50 (neutral midpoint).

    Used by Demand Intelligence and Alpha Validation to convert raw metric
    values into 0-100 component scores.
    """
    import math as _m
    if pd.isna(val) or not _m.isfinite(float(val)):
        return 50.0
    return max(0.0, min(100.0, (float(val) - lo) / (hi - lo) * 100.0))
