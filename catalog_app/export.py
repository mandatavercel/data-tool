"""
선택된 ticker들의 데이터를 xlsx 패키지로 export.

시트 구성:
    1. summary       — 선택 회사 메타 + 시그널 점수
    2. monthly_aggs  — 회사별 월별 매출·거래건수·이용자수 (catalog에 있으면)
    3. notes         — 다운로드 시점·필터 조건 메모
"""
from __future__ import annotations

import io
from datetime import datetime
import pandas as pd


def build_export_xlsx(catalog: pd.DataFrame, selected: set[str],
                      filter_summary: str = "") -> bytes:
    """선택된 회사들의 xlsx 바이트 생성. xlsxwriter 또는 openpyxl 사용."""
    sub = catalog[catalog["company"].isin(selected)].copy()

    buf = io.BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            # 1) summary 시트 — 선택 회사 메타
            sub.to_excel(writer, index=False, sheet_name="summary")

            # 2) notes 시트 — 다운로드 컨텍스트
            notes = pd.DataFrame({
                "항목": [
                    "Export 시각",
                    "선택 회사 수",
                    "필터 조건",
                    "데이터 출처",
                    "주의",
                ],
                "값": [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    str(len(sub)),
                    filter_summary or "(필터 미적용)",
                    "Mandata Alt-Data Catalog",
                    "본 데이터는 분석 결과 메타이며, 원천 거래 데이터는 별도 계약 시 제공",
                ],
            })
            notes.to_excel(writer, index=False, sheet_name="notes")

            # 컬럼 너비 자동 조정
            for sheet_name, df in [("summary", sub), ("notes", notes)]:
                worksheet = writer.sheets[sheet_name]
                for col_idx, col in enumerate(df.columns):
                    try:
                        max_len = max(
                            df[col].astype(str).str.len().max() if len(df) else 10,
                            len(str(col)),
                        )
                        worksheet.set_column(col_idx, col_idx, min(max_len + 2, 40))
                    except Exception:
                        pass
    except ImportError:
        # xlsxwriter 미설치 시 openpyxl로 fallback
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            sub.to_excel(writer, index=False, sheet_name="summary")

    buf.seek(0)
    return buf.getvalue()


def export_filename(prefix: str = "mandata_catalog") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.xlsx"
