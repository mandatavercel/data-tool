"""
kfnb_app/validation.py — 단계별 검증 게이트.

플랫폼 컨벤션(mapping_app/validation.py)을 따라 각 검증은
{label, severity, detail} dict 의 리스트를 만들고, severity ∈
{ok, info, warning, error, critical}. config.HALT_SEVERITIES 에 해당하는
항목이 하나라도 있으면 파이프라인이 중단된다.
streamlit 비의존, 순수 pandas.
"""
from __future__ import annotations

import pandas as pd

from kfnb_app import config

SEVERITY_RANK = {"ok": 0, "info": 1, "warning": 2, "error": 3, "critical": 4}


def _result(checks: list[dict]) -> dict:
    """checks 리스트 → {checks, max_severity, halt} 로 포장."""
    max_sev = "ok"
    for c in checks:
        if SEVERITY_RANK.get(c["severity"], 0) > SEVERITY_RANK[max_sev]:
            max_sev = c["severity"]
    return {
        "checks": checks,
        "max_severity": max_sev,
        "halt": max_sev in config.HALT_SEVERITIES,
    }


# ── ① profile 검증 ─────────────────────────────────────────────────────────
def validate_profile(prof: dict, present_cols: list[str]) -> dict:
    checks: list[dict] = []
    t = config.THRESHOLDS

    # 필수 컬럼 — canonical 기준
    canon_present = set(present_cols)
    required_canon = {config.RAW_COLUMNS[r] for r in config.REQUIRED_RAW}
    missing = sorted(required_canon - canon_present)
    if missing:
        checks.append({"label": "필수 컬럼", "severity": "critical",
                       "detail": f"누락: {', '.join(missing)}"})
    else:
        checks.append({"label": "필수 컬럼", "severity": "ok",
                       "detail": "모두 존재"})

    rows = prof["summary"]["rows"]
    if rows < t.min_rows:
        checks.append({"label": "행 수", "severity": "critical",
                       "detail": f"{rows:,}행 — 최소 {t.min_rows} 미만"})
    else:
        checks.append({"label": "행 수", "severity": "ok", "detail": f"{rows:,}행"})

    checks.append({"label": "데이터 기간", "severity": "info",
                   "detail": prof["summary"]["period"]})
    checks.append({"label": "커버리지", "severity": "info",
                   "detail": f"회사 {prof['summary']['companies']} · "
                             f"브랜드 {prof['summary']['brands']} · "
                             f"SKU {prof['summary']['skus']} · "
                             f"지역 {prof['summary']['regions']}"})

    q = prof["quality"]
    if q["null_company"] or q["null_sku"]:
        checks.append({"label": "핵심 결측", "severity": "error",
                       "detail": f"회사 null {q['null_company']:,} · "
                                 f"SKU null {q['null_sku']:,}"})
    else:
        checks.append({"label": "핵심 결측", "severity": "ok",
                       "detail": "회사/SKU null 없음"})

    if q["nonpos_pct"] > t.nonpos_amt_warn_pct:
        checks.append({"label": "음수/0 매출", "severity": "warning",
                       "detail": f"{q['nonpos_amt']:,}행 ({q['nonpos_pct']:.2f}%)"})
    else:
        checks.append({"label": "음수/0 매출", "severity": "info",
                       "detail": f"{q['nonpos_amt']:,}행 ({q['nonpos_pct']:.2f}%, 반품성)"})

    if q["barcode_ok_pct"] >= 99.9:
        checks.append({"label": "바코드(EAN-13)", "severity": "ok",
                       "detail": f"{q['barcode_ok_pct']:.1f}% 13자리"})
    else:
        checks.append({"label": "바코드(EAN-13)", "severity": "warning",
                       "detail": f"{q['barcode_ok_pct']:.1f}%만 13자리"})
    return _result(checks)


# ── ② normalize 검증 ───────────────────────────────────────────────────────
def validate_normalize(norm_df: pd.DataFrame) -> dict:
    checks: list[dict] = []
    n = len(norm_df)
    unknown_pkg = int((norm_df["package_format"] == "Unknown").sum())
    if unknown_pkg:
        checks.append({"label": "포장형태 미상", "severity": "warning",
                       "detail": f"{unknown_pkg}/{n} SKU — cat_l3/SKU명에서 추론 실패"})
    else:
        checks.append({"label": "포장형태", "severity": "ok",
                       "detail": "전 SKU 분류됨"})

    # '입' 표기가 있는데 pack_count=1 로 남은 케이스 (파싱 누락)
    has_ip = norm_df["sku_name_kr"].astype(str).str.contains("입")
    miss = int((has_ip & (norm_df["pack_count"] == 1)).sum())
    if miss:
        checks.append({"label": "멀티팩 파싱", "severity": "warning",
                       "detail": f"{miss}개 SKU 에 '입' 있으나 수량 미추출"})
    else:
        checks.append({"label": "멀티팩 파싱", "severity": "ok", "detail": "정상"})

    asp = pd.to_numeric(norm_df["asp_won"], errors="coerce")
    checks.append({"label": "ASP 범위", "severity": "info",
                   "detail": f"{asp.min():,.0f} ~ {asp.max():,.0f}원"})
    return _result(checks)


# ── ③ tagging 검증 ─────────────────────────────────────────────────────────
def validate_tagging(coverage: dict) -> dict:
    checks: list[dict] = []
    if not coverage:
        checks.append({"label": "태깅", "severity": "warning",
                       "detail": "태그 매출 비중 계산 불가 (매출 0)"})
        return _result(checks)
    top = sorted(coverage.items(), key=lambda kv: kv[1], reverse=True)[:5]
    detail = ", ".join(f"{k} {v}%" for k, v in top)
    checks.append({"label": "테마 커버리지(매출%)", "severity": "info",
                   "detail": detail})
    return _result(checks)


# ── ④ mapping 검증 ─────────────────────────────────────────────────────────
def validate_mapping(rep: dict) -> dict:
    checks: list[dict] = []
    t = config.THRESHOLDS

    if rep["unmapped"]:
        # 매핑 사전에 아예 없는 회사 = 사람 검수 필요 (error 게이트)
        checks.append({"label": "미매핑 회사", "severity": "error",
                       "detail": f"{', '.join(rep['unmapped'])} "
                                 f"(매출 {rep['unmapped_amt_pct']:.1f}%) — 검수 필요"})
    else:
        checks.append({"label": "미매핑 회사", "severity": "ok",
                       "detail": "사전에 모두 존재"})

    checks.append({"label": "상장사", "severity": "info",
                   "detail": f"{', '.join(rep['listed']) or '없음'} "
                             f"(매출 {rep['listed_amt_pct']:.1f}%)"})
    if rep["private"]:
        checks.append({"label": "비상장사", "severity": "info",
                       "detail": ", ".join(rep["private"])})

    if rep["listed_amt_pct"] < t.map_coverage_warn * 100:
        checks.append({"label": "상장 커버리지", "severity": "warning",
                       "detail": f"{rep['listed_amt_pct']:.1f}% < "
                                 f"{t.map_coverage_warn*100:.0f}% (비상장 비중 큼)"})
    return _result(checks)


# ── 마스터링 검증 ───────────────────────────────────────────────────────────
def validate_mastering(summary: dict) -> dict:
    checks: list[dict] = []
    checks.append({"label": "SKU 마스터", "severity": "ok",
                   "detail": f"{summary['n_skus']} SKU 표준화"})
    vp = summary["verified_amt_pct"]
    sev = "ok" if vp >= 60 else "info"
    checks.append({"label": "검수완료(verified) 매출비중", "severity": sev,
                   "detail": f"{vp:.1f}%"})
    nr = summary["needs_review_skus"]
    if nr:
        brands = ", ".join(summary["needs_review_brands"]) or "?"
        checks.append({"label": "needs_review SKU", "severity": "warning",
                       "detail": f"{nr}개 (브랜드 미큐레이션: {brands}) — 로마자 폴백/검수 큐"})
    else:
        checks.append({"label": "needs_review", "severity": "ok",
                       "detail": "없음 (전 브랜드 표준명 보유)"})
    return _result(checks)


# ── ⑤ panel 검증 ───────────────────────────────────────────────────────────
def validate_panel(panel_df: pd.DataFrame, outliers: pd.DataFrame) -> dict:
    checks: list[dict] = []
    t = config.THRESHOLDS
    n = len(panel_df)
    checks.append({"label": "패널 행", "severity": "ok" if n else "error",
                   "detail": f"{n:,}행"})
    n_out = len(outliers)
    if n_out:
        checks.append({"label": "ASP 이상치", "severity": "warning",
                       "detail": f"{n_out}행이 {t.asp_min_won:.0f}~"
                                 f"{t.asp_max_won:.0f}원 범위 밖"})
    else:
        checks.append({"label": "ASP sanity", "severity": "ok",
                       "detail": "전 행 정상 범위"})
    return _result(checks)


# ── ⑥ export 검증 ──────────────────────────────────────────────────────────
def validate_export(formula_errors: int, sheets: int) -> dict:
    checks: list[dict] = []
    if formula_errors:
        checks.append({"label": "수식 오류", "severity": "error",
                       "detail": f"{formula_errors}건 — 재생성 필요"})
    else:
        checks.append({"label": "수식 오류", "severity": "ok", "detail": "0건"})
    checks.append({"label": "시트", "severity": "info", "detail": f"{sheets}개"})
    return _result(checks)
