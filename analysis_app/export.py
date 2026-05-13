"""
Excel 멀티시트 export — session_state의 가용 분석 결과를 한 파일로 묶음.

사용:
    from analysis_app.export import build_export_excel
    result = build_export_excel()
    if result:
        excel_bytes, sheet_names = result
        st.download_button("📥 Excel 다운로드", data=excel_bytes, file_name="report.xlsx")

내부에서 streamlit session_state를 직접 읽어 다음 키를 사용:
    schema_rows / validation_result / capability_map / results
"""
from __future__ import annotations

import io
import pandas as pd
import streamlit as st

from modules.common.dashboard import _extract_signals


# ── 유틸 ──────────────────────────────────────────────────────────────────────

def _safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """Excel writer가 처리하기 어려운 타입(list, timezone-aware datetime)을 정리.

    - object 컬럼 안의 list/set 타입은 ", ".join 문자열로 변환
    - timezone-aware datetime은 tz 제거
    - "_" 접두사 내부 전용 컬럼 제거
    """
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna()
            if not sample.empty and isinstance(sample.iloc[0], (list, set)):
                df[col] = df[col].apply(
                    lambda v: ", ".join(str(i) for i in v) if isinstance(v, (list, set)) else v
                )
        if hasattr(df[col], "dt") and df[col].dtype.kind == "M":
            try:
                df[col] = df[col].dt.tz_localize(None)
            except Exception:
                try:
                    df[col] = df[col].dt.tz_convert(None)
                except Exception:
                    pass
    drop = [c for c in df.columns if str(c).startswith("_")]
    return df.drop(columns=drop, errors="ignore")


# ── Excel 빌더 ────────────────────────────────────────────────────────────────

def build_export_excel() -> tuple[bytes, list[str]] | None:
    """session_state의 가용 결과만 모아 멀티시트 Excel을 생성.

    Returns:
        (excel_bytes, sheet_names) — 가용 시트가 하나라도 있을 때
        None — 가용 결과 없음

    포함 시트 (가용 여부에 따라):
        schema_profile     — 컬럼 역할 추론 결과
        validation_report  — 데이터 품질 검사 결과
        capability_map     — 분석 모듈 가용 여부
        growth_analysis    — 월간 성장률 집계
        demand_analysis    — Demand Intelligence 집계
        anomaly_detection  — 이상 이벤트 로그
        brand_analysis / sku_analysis / category_analysis / market_signal /
        earnings_intel / alpha_validation — 표준 포맷 모듈 결과
        signal_dashboard   — 회사별 복합 신호 요약
    """
    sheets: dict[str, pd.DataFrame] = {}

    # ── 1. schema_profile ─────────────────────────────────────────────────────
    schema_rows = st.session_state.get("schema_rows", [])
    if schema_rows:
        df_schema = pd.DataFrame(schema_rows)
        keep = ["column_name", "dtype", "sample", "null_pct", "n_unique",
                "final_role", "confidence", "reason"]
        if "included" in df_schema.columns:
            keep.append("included")
        sheets["schema_profile"] = _safe_df(df_schema[[c for c in keep if c in df_schema.columns]])

    # ── 2. validation_report ──────────────────────────────────────────────────
    val = st.session_state.get("validation_result")
    if val:
        checks = val.get("checks", [])
        if checks:
            df_val = pd.DataFrame(checks).rename(columns={
                "label": "검사 항목", "severity": "등급",
                "detail": "내용",    "cut":      "감점",
            })
            stats = val.get("stats", {})
            summary = pd.DataFrame([{
                "검사 항목": "▶ Data Quality Score",
                "등급":     f"{val['score']}/100",
                "내용":     (f"데이터 기간: {stats.get('date_min','?')} ~ {stats.get('date_max','?')}"
                             f" | 분석 가능 행: {stats.get('valid_rows','?'):,}"),
                "감점":     "",
            }])
            sheets["validation_report"] = _safe_df(pd.concat([summary, df_val], ignore_index=True))

    # ── 3. capability_map ─────────────────────────────────────────────────────
    caps = st.session_state.get("capability_map")
    if caps:
        df_cap = pd.DataFrame(caps)
        keep = ["name", "layer", "runnable", "available", "ready", "reason"]
        for lc in ["missing", "optional_missing"]:
            if lc in df_cap.columns:
                df_cap[lc] = df_cap[lc].apply(lambda v: ", ".join(v) if isinstance(v, list) else v)
                keep.append(lc)
        df_cap = df_cap.rename(columns={
            "name": "모듈명", "layer": "레이어",
            "runnable": "실행 가능", "available": "데이터 충족",
            "ready": "구현 완료", "reason": "사유",
            "missing": "부족 역할", "optional_missing": "선택 역할 부족",
        })
        sheets["capability_map"] = _safe_df(df_cap[[c for c in df_cap.columns]])

    # ── 4–6. 분석 결과 (Intelligence Hub) ────────────────────────────────────
    results = st.session_state.get("results", {})
    g_res = results.get("growth")
    d_res = results.get("demand")
    a_res = results.get("anomaly")

    if g_res:
        monthly = g_res.get("monthly")
        if monthly is not None:
            sheets["growth_analysis"] = _safe_df(monthly)

    if d_res:
        agg = d_res.get("agg_df")
        if agg is not None:
            drop = ["__row__", "__all__"]
            sheets["demand_analysis"] = _safe_df(agg.drop(columns=drop, errors="ignore"))

    if a_res:
        ev = a_res.get("event_df", pd.DataFrame())
        if not ev.empty:
            sheets["anomaly_detection"] = _safe_df(ev)
        else:
            agg_a = a_res.get("agg_df")
            if agg_a is not None:
                sheets["anomaly_detection"] = _safe_df(agg_a)

    # ── 표준 포맷 모듈 (result["data"] = DataFrame) ──────────────────────────
    SHEET_MAP = {
        "brand":            "brand_analysis",
        "sku":              "sku_analysis",
        "category":         "category_analysis",
        "market_signal":    "market_signal",
        "earnings_intel":   "earnings_intel",
        "alpha_validation": "alpha_validation",
    }
    for key, sheet_name in SHEET_MAP.items():
        res = results.get(key)
        if res and isinstance(res.get("data"), pd.DataFrame) and not res["data"].empty:
            sheets[sheet_name] = _safe_df(res["data"])

    # ── 7. signal_dashboard ───────────────────────────────────────────────────
    sig_df = _extract_signals(g_res, d_res, a_res)
    if not sig_df.empty:
        sheets["signal_dashboard"] = _safe_df(sig_df)

    if not sheets:
        return None

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, frame in sheets.items():
            frame.to_excel(writer, index=False, sheet_name=sheet_name[:31])

    return buf.getvalue(), list(sheets.keys())
