"""Unified result building utilities.

Every analysis module returns a dict with these guaranteed keys:

    status     : "success" | "warning" | "failed"
    message    : str
    data       : pd.DataFrame | None
    metrics    : dict
    audit      : {input_audit, data_quality, calculation_audit, business_sanity}
    confidence : {score, grade, label, color, reason}

Extra module-private keys (prefixed with _) may also be present.
"""
from __future__ import annotations

import pandas as pd


# ── Sentinel: empty audit/confidence so callers never get KeyError ──────────

_EMPTY_AUDIT = {
    "input_audit":       {},
    "data_quality":      {},
    "calculation_audit": {},
    "business_sanity":   [],
}

_EMPTY_CONFIDENCE = {
    "score":  0.0,
    "grade":  "D",
    "label":  "평가 불가",
    "color":  "#9ca3af",
    "reason": ["audit 정보 없음"],
}


def make_result(
    status: str,
    message: str,
    data: "pd.DataFrame | None",
    metrics: dict,
    audit: dict | None = None,
    confidence: dict | None = None,
    **extra,
) -> dict:
    """Create a fully structured result dict.

    Parameters
    ----------
    **extra : any additional module-private keys (e.g. _dart_by_company)
    """
    return {
        "status":     status,
        "message":    message,
        "data":       data,
        "metrics":    metrics,
        "audit":      audit      or _EMPTY_AUDIT,
        "confidence": confidence or _EMPTY_CONFIDENCE,
        **extra,
    }


def enrich_result(result: dict, audit: dict, confidence: dict) -> dict:
    """Non-destructively add audit and confidence to an existing result dict.

    Safe to call even if audit/confidence keys already exist — they will be
    overwritten with the new values.
    """
    return {**result, "audit": audit, "confidence": confidence}


def failed_result(message: str, **extra) -> dict:
    """Convenience builder for early-exit failure results."""
    return make_result(
        status="failed",
        message=message,
        data=None,
        metrics={},
        **extra,
    )


def get_confidence_grade(result: dict) -> str:
    """Safely extract grade from any result dict. Returns '?' if absent."""
    return result.get("confidence", {}).get("grade", "?")


def get_confidence_score(result: dict) -> float:
    """Safely extract score from any result dict. Returns 0.0 if absent."""
    return float(result.get("confidence", {}).get("score", 0.0))
