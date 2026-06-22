"""
kfnb_app/qc/checks.py — Quality Control 체크 + 리포트.

투자등급 데이터의 마지막 관문. 매핑 누락·이상치·커버리지를 점검하고
qc_summary / unmapped_items / outlier_report / mapping_coverage 테이블을 만든다.
streamlit 비의존, 순수 pandas.
"""
from __future__ import annotations

import pandas as pd

from kfnb_app import config
from kfnb_app.mapping import coverage as cov

SEVERITY_RANK = {"ok": 0, "info": 1, "warning": 2, "error": 3, "critical": 4}


def run_qc(sku_master: pd.DataFrame, monthly_panel: pd.DataFrame,
           profile: dict | None = None) -> dict:
    """QC 실행 → {checks, max_severity, halt, summary, tables}."""
    t = config.THRESHOLDS
    profile = profile or {}
    checks: list[dict] = []
    total = pd.to_numeric(sku_master["sales_amt"], errors="coerce").sum()

    # 1) 필수 매핑 누락 (company/brand/sku id)
    for col, label in [("company_en", "회사"), ("brand_id", "브랜드"),
                       ("sku_id", "SKU")]:
        if col in sku_master:
            miss = sku_master[col].astype(str).str.len() == 0
            n = int(miss.sum())
            if n:
                amt = pd.to_numeric(sku_master.loc[miss, "sales_amt"],
                                    errors="coerce").sum()
                checks.append({"label": f"{label} 매핑 누락", "severity": "error",
                               "detail": f"{n}건 (매출 {amt/total*100 if total else 0:.1f}%)"})
            else:
                checks.append({"label": f"{label} 매핑", "severity": "ok",
                               "detail": "누락 없음"})

    # 2) unmapped/needs_review SKU
    need = sku_master[sku_master.get("mapping_status", "") == "needs_review"] \
        if "mapping_status" in sku_master else sku_master.iloc[0:0]
    if len(need):
        amt = pd.to_numeric(need["sales_amt"], errors="coerce").sum()
        checks.append({"label": "needs_review SKU", "severity": "warning",
                       "detail": f"{len(need)}개 (매출 {amt/total*100 if total else 0:.1f}%) "
                                 "— 검수 큐"})
    else:
        checks.append({"label": "needs_review SKU", "severity": "ok", "detail": "없음"})

    # 3) sku_id 중복 (안정 ID 무결성)
    if "sku_id" in sku_master:
        dup = int(sku_master["sku_id"].duplicated().sum())
        sev = "warning" if dup else "ok"
        checks.append({"label": "SKU ID 중복", "severity": sev,
                       "detail": f"{dup}건" if dup else "없음 (고유)"})

    # 4) ASP 이상치 (panel)
    outliers = pd.DataFrame()
    if monthly_panel is not None and "asp_won" in monthly_panel:
        a = pd.to_numeric(monthly_panel["asp_won"], errors="coerce")
        outliers = monthly_panel[(a < t.asp_min_won) | (a > t.asp_max_won)]
        sev = "warning" if len(outliers) else "ok"
        checks.append({"label": "ASP 이상치", "severity": sev,
                       "detail": f"{len(outliers)}행" if len(outliers)
                                 else "전 행 정상 범위"})

    # 5) 음수/0 매출 (profile 기반)
    q = profile.get("quality", {})
    if q:
        np_pct = q.get("nonpos_pct", 0.0)
        sev = "warning" if np_pct > t.nonpos_amt_warn_pct else "info"
        checks.append({"label": "음수/0 매출", "severity": sev,
                       "detail": f"{q.get('nonpos_amt', 0):,}행 ({np_pct:.2f}%)"})

    # 6) 매출기준 커버리지
    cov_sum = cov.coverage_by_sales(sku_master)
    sku_cov = cov_sum.get("sku_coverage_pct", 0)
    sev = "warning" if sku_cov < 80 else "info"
    checks.append({"label": "SKU 커버리지(매출%)", "severity": sev,
                   "detail": f"{sku_cov}% · 고신뢰 {cov_sum.get('high_confidence_pct', 0)}%"})

    max_sev = "ok"
    for c in checks:
        if SEVERITY_RANK.get(c["severity"], 0) > SEVERITY_RANK[max_sev]:
            max_sev = c["severity"]

    tables = {
        "qc_summary": _summary_table(checks, cov_sum, total),
        "unmapped_items": _unmapped_table(sku_master),
        "outlier_report": _outlier_table(outliers),
        "mapping_coverage": cov.coverage_table(sku_master),
    }
    return {
        "checks": checks,
        "max_severity": max_sev,
        "halt": max_sev in config.HALT_SEVERITIES,
        "summary": cov_sum,
        "tables": tables,
        "n_warnings": sum(1 for c in checks if c["severity"] == "warning"),
        "n_critical": sum(1 for c in checks if c["severity"] in ("error", "critical")),
    }


def _summary_table(checks, cov_sum, total) -> pd.DataFrame:
    rows = [{"item": c["label"], "severity": c["severity"], "detail": c["detail"]}
            for c in checks]
    rows.append({"item": "total_sales_amount_krw", "severity": "info",
                 "detail": f"{total:,.0f}"})
    return pd.DataFrame(rows)


def _unmapped_table(sku_master: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in ["sku_id", "sku_name_kr", "brand_name_ko", "company_kr",
                        "barcode", "mapping_confidence", "mapping_status",
                        "sales_amt"] if c in sku_master]
    mask = sku_master.get("mapping_status", "") != "verified" \
        if "mapping_status" in sku_master else pd.Series(False, index=sku_master.index)
    out = sku_master.loc[mask, cols].copy()
    return out.sort_values("sales_amt", ascending=False) if "sales_amt" in out else out


def _outlier_table(outliers: pd.DataFrame) -> pd.DataFrame:
    if outliers is None or outliers.empty:
        return pd.DataFrame(columns=["ym", "company_kr", "brand_kr", "asp_won"])
    cols = [c for c in ["ym", "company_kr", "brand_kr", "sales_amt", "asp_won"]
            if c in outliers]
    return outliers[cols].copy()
