"""Core validators — structured data-quality checks.

Return plain dicts (no Streamlit).  Each function answers one narrow question
so callers can decide how to display or act on the result.
"""
import math
import pandas as pd


# ── Column presence ───────────────────────────────────────────────────────────

def validate_required_columns(role_map: dict, required: list) -> dict:
    """Check that every role in `required` is present in role_map.

    Returns
    -------
    dict
        ok      : bool
        missing : list[str]
        message : str
    """
    missing = [r for r in required if r not in role_map]
    ok = len(missing) == 0
    message = "모든 필수 컬럼 확인" if ok else f"필수 역할 없음: {', '.join(missing)}"
    return {"ok": ok, "missing": missing, "message": message}


# ── Numeric quality ───────────────────────────────────────────────────────────

def validate_numeric_values(series: pd.Series) -> dict:
    """Compute numeric-quality statistics for a column.

    Returns
    -------
    dict
        n_total, n_null, n_negative, n_zero, n_outlier_iqr,
        null_pct, negative_pct, zero_pct,
        mean, std, min, max
    """
    numeric   = pd.to_numeric(series, errors="coerce")
    n_total   = len(numeric)
    n_null    = int(numeric.isna().sum())
    valid     = numeric.dropna()
    n_valid   = len(valid)
    n_negative = int((valid < 0).sum())
    n_zero     = int((valid == 0).sum())

    n_outlier = 0
    if n_valid >= 4:
        q1, q3 = valid.quantile(0.25), valid.quantile(0.75)
        iqr = q3 - q1
        n_outlier = int(((valid < q1 - 1.5 * iqr) | (valid > q3 + 1.5 * iqr)).sum())

    return {
        "n_total":       n_total,
        "n_null":        n_null,
        "n_negative":    n_negative,
        "n_zero":        n_zero,
        "n_outlier_iqr": n_outlier,
        "null_pct":      round(n_null    / n_total  * 100, 1) if n_total  > 0 else 0.0,
        "negative_pct":  round(n_negative / n_valid * 100, 1) if n_valid  > 0 else 0.0,
        "zero_pct":      round(n_zero    / n_valid  * 100, 1) if n_valid  > 0 else 0.0,
        "mean": float(valid.mean()) if n_valid > 0 else float("nan"),
        "std":  float(valid.std())  if n_valid > 1 else float("nan"),
        "min":  float(valid.min())  if n_valid > 0 else float("nan"),
        "max":  float(valid.max())  if n_valid > 0 else float("nan"),
    }


# ── Date quality ──────────────────────────────────────────────────────────────

def validate_date_values(series: pd.Series) -> dict:
    """Parse and validate a date column.

    Returns
    -------
    dict
        n_total, n_null, null_pct,
        date_min (str | None), date_max (str | None), n_days,
        is_sorted (bool), ok (bool)
    """
    from modules.common.core.normalizer import normalize_date
    parsed  = normalize_date(series)
    n_total = len(parsed)
    n_null  = int(parsed.isna().sum())
    valid   = parsed.dropna()

    date_min = valid.min() if not valid.empty else None
    date_max = valid.max() if not valid.empty else None
    n_days   = int((date_max - date_min).days) if (date_min and date_max) else 0
    is_sorted = bool(valid.is_monotonic_increasing) if not valid.empty else True

    return {
        "n_total":  n_total,
        "n_null":   n_null,
        "null_pct": round(n_null / n_total * 100, 1) if n_total > 0 else 0.0,
        "date_min": date_min.strftime("%Y-%m-%d") if date_min else None,
        "date_max": date_max.strftime("%Y-%m-%d") if date_max else None,
        "n_days":   n_days,
        "is_sorted": is_sorted,
        "ok": n_null == 0,
    }


# ── Sample size ───────────────────────────────────────────────────────────────

def validate_sample_size(n: int, min_required: int = 10) -> dict:
    """Check whether sample size meets minimum threshold.

    Returns
    -------
    dict
        n, min_required, ok (bool),
        severity ("ok" | "warning" | "critical"), message
    """
    if n >= min_required:
        severity, message = "ok", f"{n:,}개 — 분석 가능"
    elif n >= max(3, min_required // 3):
        severity = "warning"
        message  = f"{n}개 — 최소 {min_required}개 권장 (제한적 분석)"
    else:
        severity = "critical"
        message  = f"{n}개 — 최소 {min_required}개 필요 (분석 불가)"
    return {
        "n": n, "min_required": min_required,
        "ok": severity == "ok", "severity": severity, "message": message,
    }


# ── Ratio sanity ──────────────────────────────────────────────────────────────

def validate_ratio_sanity(ratio: float) -> dict:
    """Classify a single ratio value (e.g. POS/DART %) into severity.

    Returns
    -------
    dict
        ratio, severity ("ok"|"caution"|"warning"|"critical"),
        label (str), message (str)
    """
    if math.isnan(ratio) or math.isinf(ratio):
        return {"ratio": ratio, "severity": "warning",
                "label": "계산 불가", "message": "비율이 무한대 또는 NaN"}
    if ratio < 0:
        return {"ratio": ratio, "severity": "critical",
                "label": "음수 비율", "message": f"{ratio:.1f}% — 데이터 변환 오류 가능"}
    if ratio > 200:
        return {"ratio": ratio, "severity": "critical",
                "label": "극단 초과", "message": f"{ratio:.1f}% — 단위 불일치 또는 이중 집계 의심"}
    if ratio > 150:
        return {"ratio": ratio, "severity": "warning",
                "label": "심각 초과", "message": f"{ratio:.1f}% — 채널 범위 또는 연결/별도 차이 확인 필요"}
    if ratio > 100:
        return {"ratio": ratio, "severity": "caution",
                "label": "범위 초과", "message": f"{ratio:.1f}% > 100% — 집계 범위 차이 가능성"}
    if ratio < 5:
        return {"ratio": ratio, "severity": "caution",
                "label": "매우 낮음", "message": f"{ratio:.1f}% — POS 추적 범위가 매우 좁음"}
    return {"ratio": ratio, "severity": "ok",
            "label": "정상", "message": f"{ratio:.1f}%"}


def validate_tracking_ratio(tr_series: pd.Series) -> dict:
    """Aggregate tracking-ratio validation across multiple quarters.

    Parameters
    ----------
    tr_series : Series of POS/DART ratio values (%)

    Returns
    -------
    dict
        avg, max, std, n_above_100, n_extreme,
        severity ("ok"|"caution"|"warning"|"critical"),
        issues (list[str])
    """
    tr = tr_series.replace([float("inf"), float("-inf")], float("nan")).dropna()
    if tr.empty:
        return {"avg": float("nan"), "max": float("nan"), "std": float("nan"),
                "n_above_100": 0, "n_extreme": 0, "severity": "ok", "issues": []}

    avg   = float(tr.mean())
    max_  = float(tr.max())
    std   = float(tr.std()) if len(tr) > 1 else float("nan")
    n_above   = int((tr > 100).sum())
    n_extreme = int(((tr > 200) | (tr < 0)).sum())

    issues: list[str] = []
    if avg > 150:
        issues.append(f"평균 추적률 {avg:.0f}% — 단위 불일치 또는 채널 범위 초과")
    elif avg > 100:
        issues.append(f"평균 추적률 {avg:.0f}% > 100%")
    if n_extreme > 0:
        issues.append(f"{n_extreme}개 분기에서 추적률 200% 초과 또는 음수")
    if not math.isnan(std) and std > 40:
        issues.append(f"추적률 표준편차 {std:.0f}pp — 분기별 변동 과대")

    if avg > 150 or n_extreme > 0:
        severity = "critical"
    elif avg > 100 or (not math.isnan(std) and std > 40):
        severity = "warning"
    elif issues:
        severity = "caution"
    else:
        severity = "ok"

    return {
        "avg": avg, "max": max_, "std": std,
        "n_above_100": n_above, "n_extreme": n_extreme,
        "severity": severity, "issues": issues,
    }
