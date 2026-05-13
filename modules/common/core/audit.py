"""Unified Audit / Confidence engine — applies to every analysis module.

Every run_* function should call compute_module_audit() at the end and
attach the returned (audit, confidence) dicts to its result.

No Streamlit.  Pure Python / pandas / numpy.
"""
from __future__ import annotations

import math
import pandas as pd

from modules.common.core.validators import validate_numeric_values, validate_date_values


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Input Audit
# ═══════════════════════════════════════════════════════════════════════════════

def build_input_audit(
    n_original: int,
    n_valid: int,
    role_map: dict,
    used_roles: list[str],
    date_min: str | None = None,
    date_max: str | None = None,
    exclude_reasons: list[str] | None = None,
) -> dict:
    """Describe what rows / columns were fed into the calculation."""
    used_cols = {role: role_map.get(role) for role in used_roles if role in role_map}
    return {
        "total_rows":      n_original,
        "used_rows":       n_valid,
        "excluded_rows":   n_original - n_valid,
        "exclude_reasons": exclude_reasons or [],
        "roles_used":      used_cols,
        "date_range":      {"start": date_min, "end": date_max},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Data Quality
# ═══════════════════════════════════════════════════════════════════════════════

def build_data_quality(
    df: pd.DataFrame,
    sales_col: str | None = None,
    date_col: str | None = None,
) -> dict:
    """Compute standard data quality metrics from the cleaned DataFrame."""
    result: dict = {"n_rows": len(df)}

    result["n_duplicates"] = int(df.duplicated().sum())
    result["dup_pct"] = round(result["n_duplicates"] / max(1, len(df)) * 100, 1)

    if sales_col and sales_col in df.columns:
        nv = validate_numeric_values(df[sales_col])
        result["sales_null_pct"]     = nv["null_pct"]
        result["sales_negative_n"]   = nv["n_negative"]
        result["sales_zero_n"]       = nv["n_zero"]
        result["sales_outlier_n"]    = nv["n_outlier_iqr"]
        result["sales_outlier_pct"]  = round(nv["n_outlier_iqr"] / max(1, len(df)) * 100, 1)
        result["sales_mean"]         = nv["mean"]
        result["sales_max"]          = nv["max"]
    else:
        result["sales_null_pct"]    = 0.0
        result["sales_negative_n"]  = 0
        result["sales_outlier_pct"] = 0.0

    if date_col and date_col in df.columns:
        dv = validate_date_values(df[date_col])
        result["date_null_pct"] = dv["null_pct"]
        result["date_min"]      = dv["date_min"]
        result["date_max"]      = dv["date_max"]
        result["date_sorted"]   = dv["is_sorted"]
    else:
        result["date_null_pct"] = 0.0
        result["date_min"]      = None
        result["date_max"]      = None
        result["date_sorted"]   = True

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Calculation Audit
# ═══════════════════════════════════════════════════════════════════════════════

def build_calculation_audit(
    formula: str,
    agg_unit: str | None = None,
    window: int | None = None,
    growth_base: str | None = None,
    n_computable: int | None = None,
    n_total: int | None = None,
    skipped: list[str] | None = None,
) -> dict:
    """Describe how the calculation was performed."""
    return {
        "formula":      formula,
        "agg_unit":     agg_unit,
        "window":       window,
        "growth_base":  growth_base,
        "n_computable": n_computable,
        "n_total":      n_total,
        "skipped":      skipped or [],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Business Sanity Checks
# ═══════════════════════════════════════════════════════════════════════════════

# Issue level definitions
#   "critical" → result likely wrong / misleading
#   "warning"  → result may be off, interpret carefully
#   "info"     → note worth knowing but not a problem

def _sanity_check(level: str, code: str, message: str) -> dict:
    return {"level": level, "code": code, "message": message}


def check_growth_sanity(
    growth_series: "pd.Series | None" = None,
    threshold_warn: float = 200.0,
    threshold_crit: float = 1000.0,
) -> list[dict]:
    """Flag unrealistically large growth rates."""
    checks = []
    if growth_series is None:
        return checks
    valid = growth_series.replace([float("inf"), float("-inf")], float("nan")).dropna()
    if valid.empty:
        return checks
    max_g = float(valid.abs().max())
    if max_g >= threshold_crit:
        checks.append(_sanity_check(
            "critical", "GROWTH_EXTREME",
            f"최대 성장률 {max_g:.0f}% — 데이터 오류 또는 베이스 기간 이상 가능성",
        ))
    elif max_g >= threshold_warn:
        checks.append(_sanity_check(
            "warning", "GROWTH_HIGH",
            f"최대 성장률 {max_g:.0f}% — 베이스 기간이 매우 낮거나 비교 기준 확인 필요",
        ))
    return checks


def check_tracking_ratio_sanity(avg_tr: float, n_extreme: int = 0) -> list[dict]:
    """POS/DART ratio plausibility check."""
    checks = []
    if math.isnan(avg_tr):
        return checks
    if n_extreme > 0:
        checks.append(_sanity_check(
            "critical", "TR_EXTREME",
            f"{n_extreme}개 분기에서 추적률 200% 초과 또는 음수 — DART YTD 변환 오류 가능",
        ))
    if avg_tr > 150:
        checks.append(_sanity_check(
            "critical", "TR_OVER_150",
            f"평균 Tracking Ratio {avg_tr:.0f}% — 단위 불일치 또는 이중 집계 의심",
        ))
    elif avg_tr > 100:
        checks.append(_sanity_check(
            "warning", "TR_OVER_100",
            f"평균 Tracking Ratio {avg_tr:.0f}% > 100% — 집계 범위 또는 연결/별도 차이",
        ))
    return checks


def check_correlation_sanity(r: float, n: int, min_n: int = 6) -> list[dict]:
    """Flag low-sample or near-zero correlations."""
    checks = []
    if n < 3:
        checks.append(_sanity_check(
            "critical", "CORR_TOO_FEW",
            f"상관 계산 관측 수 {n}개 — 결과 신뢰 불가 (최소 3개)",
        ))
    elif n < min_n:
        checks.append(_sanity_check(
            "warning", "CORR_FEW_SAMPLES",
            f"상관 계산 관측 수 {n}개 — 최소 {min_n}개 권장",
        ))
    return checks


def check_anomaly_rate_sanity(n_anomaly: int, n_total: int) -> list[dict]:
    """Flag if too many periods are anomalous."""
    checks = []
    if n_total < 1:
        return checks
    rate = n_anomaly / n_total
    if rate > 0.5:
        checks.append(_sanity_check(
            "warning", "ANOMALY_RATE_HIGH",
            f"이상 탐지 비율 {rate*100:.0f}% > 50% — threshold 또는 window 재조정 권장",
        ))
    return checks


def check_sample_size_sanity(n: int, min_required: int = 12) -> list[dict]:
    """Flag insufficient aggregated periods."""
    checks = []
    if n < 3:
        checks.append(_sanity_check(
            "critical", "SAMPLE_TOO_FEW",
            f"집계 기간 {n}개 — 분석 불가 (최소 3개)",
        ))
    elif n < min_required:
        checks.append(_sanity_check(
            "warning", "SAMPLE_LOW",
            f"집계 기간 {n}개 — 최소 {min_required}개 권장",
        ))
    return checks


# ═══════════════════════════════════════════════════════════════════════════════
# 5 & 6. Confidence Score and Grade
# ═══════════════════════════════════════════════════════════════════════════════

def compute_confidence_score(
    n_periods: int,
    null_pct: float = 0.0,
    outlier_pct: float = 0.0,
    dup_pct: float = 0.0,
    n_warnings: int = 0,
    n_criticals: int = 0,
    signal_stability: float = 1.0,
    extra_penalty: float = 0.0,
) -> float:
    """Compute 0-100 confidence score.

    Deductions
    ----------
    Sample size  : graduated -5 to -30 for n < 30
    Null %       : -0.5 per % (max -25)
    Outlier %    : -0.3 per % (max -10)
    Duplicate %  : -0.2 per % (max -5)
    Warnings     : -5 each
    Criticals    : -15 each
    Instability  : -(1-stability)*20  (stability in [0,1])
    Extra        : caller-specified additional penalty
    """
    score = 100.0

    # Sample size
    if n_periods < 4:
        score -= 30
    elif n_periods < 10:
        score -= 20
    elif n_periods < 20:
        score -= 10
    elif n_periods < 30:
        score -= 5

    # Data quality
    score -= min(25, null_pct * 0.5)
    score -= min(10, outlier_pct * 0.3)
    score -= min(5,  dup_pct * 0.2)

    # Business sanity
    score -= n_warnings  * 5
    score -= n_criticals * 15

    # Signal stability (0 = unstable, 1 = stable)
    stability = max(0.0, min(1.0, float(signal_stability)))
    score -= (1.0 - stability) * 20

    score -= extra_penalty

    return round(max(0.0, min(100.0, score)), 1)


def grade_confidence(score: float) -> str:
    """A ≥ 85 / B 70-84 / C 50-69 / D < 50"""
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 50:
        return "C"
    return "D"


GRADE_LABEL = {
    "A": "신뢰 가능",
    "B": "일반적 참고 가능",
    "C": "참고용 — 해석 주의",
    "D": "신뢰 낮음 — 데이터/표본 재확인",
}

GRADE_COLOR = {
    "A": "#16a34a",
    "B": "#d97706",
    "C": "#f97316",
    "D": "#dc2626",
}


def build_confidence(score: float, reason: list[str]) -> dict:
    """Assemble the standard confidence sub-dict."""
    grade = grade_confidence(score)
    return {
        "score": score,
        "grade": grade,
        "label": GRADE_LABEL[grade],
        "color": GRADE_COLOR[grade],
        "reason": reason,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Master convenience wrapper
# ═══════════════════════════════════════════════════════════════════════════════

def compute_module_audit(
    *,
    # Input
    n_original: int,
    n_valid: int,
    role_map: dict,
    used_roles: list[str],
    date_min: str | None = None,
    date_max: str | None = None,
    exclude_reasons: list[str] | None = None,
    # Data quality (pass pre-computed, or pass df + cols)
    dq: dict | None = None,
    df_clean: "pd.DataFrame | None" = None,
    sales_col: str | None = None,
    date_col: str | None = None,
    # Calculation
    formula: str = "",
    agg_unit: str | None = None,
    window: int | None = None,
    growth_base: str | None = None,
    n_computable: int | None = None,
    # Business sanity
    business_checks: list[dict] | None = None,
    # Confidence tuning
    n_periods: int | None = None,          # defaults to n_valid if omitted
    signal_stability: float = 1.0,
    extra_penalty: float = 0.0,
) -> tuple[dict, dict]:
    """Build (audit_dict, confidence_dict) for any analysis module.

    Returns
    -------
    audit : dict  — with input_audit, data_quality, calculation_audit, business_sanity
    confidence : dict — with score, grade, label, color, reason
    """
    # ── Input audit ────────────────────────────────────────────────────────────
    ia = build_input_audit(
        n_original=n_original,
        n_valid=n_valid,
        role_map=role_map,
        used_roles=used_roles,
        date_min=date_min,
        date_max=date_max,
        exclude_reasons=exclude_reasons,
    )

    # ── Data quality ───────────────────────────────────────────────────────────
    if dq is not None:
        dq_out = dq
    elif df_clean is not None:
        dq_out = build_data_quality(df_clean, sales_col, date_col)
    else:
        dq_out = {"n_rows": n_valid}

    # ── Calculation audit ──────────────────────────────────────────────────────
    ca = build_calculation_audit(
        formula=formula,
        agg_unit=agg_unit,
        window=window,
        growth_base=growth_base,
        n_computable=n_computable,
        n_total=n_valid,
    )

    # ── Business sanity ────────────────────────────────────────────────────────
    bs = business_checks or []

    # ── Confidence ─────────────────────────────────────────────────────────────
    n_pts        = n_periods if n_periods is not None else n_valid
    null_pct     = dq_out.get("sales_null_pct", 0.0)
    outlier_pct  = dq_out.get("sales_outlier_pct", 0.0)
    dup_pct      = dq_out.get("dup_pct", 0.0)
    n_warn       = sum(1 for c in bs if c.get("level") == "warning")
    n_crit       = sum(1 for c in bs if c.get("level") == "critical")

    score = compute_confidence_score(
        n_periods=n_pts,
        null_pct=null_pct,
        outlier_pct=outlier_pct,
        dup_pct=dup_pct,
        n_warnings=n_warn,
        n_criticals=n_crit,
        signal_stability=signal_stability,
        extra_penalty=extra_penalty,
    )

    # Build reason list
    reason: list[str] = []
    if n_pts < 10:
        reason.append(f"집계 기간 {n_pts}개 — 표본 부족")
    if null_pct > 10:
        reason.append(f"결측값 {null_pct:.0f}%")
    if outlier_pct > 10:
        reason.append(f"이상값 {outlier_pct:.0f}%")
    if signal_stability < 0.6:
        reason.append(f"신호 안정성 낮음 ({signal_stability*100:.0f}%)")
    for c in bs:
        if c.get("level") in ("warning", "critical"):
            reason.append(c["message"])
    if not reason:
        reason.append("데이터 품질 및 표본 수 양호")

    conf = build_confidence(score, reason)

    audit = {
        "input_audit":       ia,
        "data_quality":      dq_out,
        "calculation_audit": ca,
        "business_sanity":   bs,
    }
    return audit, conf
