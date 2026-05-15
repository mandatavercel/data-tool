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


# ── 결제 완료 후 — 풀 데이터 패키지 (다중 시트) ───────────────────────────
def build_paid_data_xlsx(
    catalog: pd.DataFrame,
    purchased: list[str],
    totals: object,
    filter_summary: str = "",
    n_months: int = 24,
    selected_sources: list[str] | None = None,
) -> bytes:
    """결제 완료 후 다운로드되는 풀 데이터 xlsx.

    시트 구성:
        1. invoice          — 결제 영수증 (qty, subtotal, discount, tax, total)
        2. companies        — 구매 회사 메타 + 단가
        3. monthly_aggregates — 회사별 N개월 매출·거래·이용자 시계열 (long)
        4. signal_history   — 회사별 시그널 점수 추이
        5. summary_by_month — 모든 회사 합산 월별
        6. notes            — 컨텍스트/면책
    """
    from catalog_app.sample_data import monthly_aggregates_multi, monthly_by_source_multi
    from catalog_app.pricing import calc_unit_price

    selected_sources = selected_sources or []
    sub = catalog[catalog["company"].isin(purchased)].copy()

    # 단가 계산 (선택 소스 기준)
    unit_prices = []
    matched_n = []
    coverages = []
    for _, r in sub.iterrows():
        up = calc_unit_price(r, selected_sources)
        unit_prices.append(up.unit_price)
        matched_n.append(len(up.matched_sources))
        coverages.append(round(up.combined_coverage, 1))
    sub["unit_price_usd"]        = unit_prices
    sub["matched_sources_n"]     = matched_n
    sub["combined_coverage_pct"] = coverages

    # 시계열 (long format)
    series = monthly_aggregates_multi(sub, sub["company"].tolist(), n_months=n_months)
    # 소스별 분해 시계열
    src_series = monthly_by_source_multi(sub, sub["company"].tolist(),
                                         selected_sources, n_months=n_months)

    buf = io.BytesIO()
    engine = "xlsxwriter"
    try:
        import xlsxwriter  # noqa: F401
    except ImportError:
        engine = "openpyxl"

    with pd.ExcelWriter(buf, engine=engine) as writer:
        # 1) invoice
        invoice_rows = [
            ("Order ID",       f"MAN-{datetime.now().strftime('%Y%m%d-%H%M%S')}"),
            ("Order Time",     datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("Companies",      str(len(purchased))),
            ("Selected Sources", ", ".join(selected_sources) if selected_sources else "(none)"),
            ("Subtotal (USD)", f"${getattr(totals, 'subtotal', 0):,.2f}"),
            ("Volume Tier",    str(getattr(totals, 'volume_tier_label', '-'))),
            ("Volume Discount Rate", f"{getattr(totals, 'volume_rate', 0)*100:.0f}%"),
            ("Volume Discount (USD)", f"-${getattr(totals, 'volume_discount', 0):,.2f}"),
            ("After Discount", f"${getattr(totals, 'after_discount', 0):,.2f}"),
            ("VAT (10%)",      f"${getattr(totals, 'tax', 0):,.2f}"),
            ("Grand Total (USD)", f"${getattr(totals, 'grand_total', 0):,.2f}"),
            ("Filter Summary", filter_summary or "(no filter)"),
        ]
        pd.DataFrame(invoice_rows, columns=["Item", "Value"]).to_excel(
            writer, index=False, sheet_name="invoice",
        )

        # 2) companies (메타 + 소스 매칭/커버리지)
        meta_cols = [c for c in [
            "company", "ticker", "isin", "region", "country", "exchange",
            "currency", "gics_sector", "sector", "market_cap_usd",
            "adv_usd", "signal_score", "ic", "backtest_sharpe",
            "coverage_months", "data_latency_days", "update_frequency",
            "n_sources", "data_sources",
            "matched_sources_n", "combined_coverage_pct",
            "esg_score", "unit_price_usd",
        ] if c in sub.columns]
        sub[meta_cols].to_excel(writer, index=False, sheet_name="companies")

        # 3) monthly_aggregates (long)
        if not series.empty:
            series.to_excel(writer, index=False, sheet_name="monthly_aggregates")

            # 4) signal_history (pivot)
            sig_pivot = series.pivot(index="month", columns="company",
                                     values="signal_score").reset_index()
            sig_pivot.to_excel(writer, index=False, sheet_name="signal_history")

            # 5) summary_by_month (전체 합산)
            agg = series.groupby("month", as_index=False).agg({
                "revenue_usd_m": "sum",
                "transactions":  "sum",
                "unique_users":  "sum",
                "signal_score":  "mean",
            })
            agg.columns = ["month", "total_revenue_usd_m", "total_transactions",
                           "total_unique_users", "avg_signal_score"]
            agg.to_excel(writer, index=False, sheet_name="summary_by_month")

        # 6) source_breakdown — 소스별 월별 매출 (long)
        if not src_series.empty:
            src_series.to_excel(writer, index=False, sheet_name="source_breakdown")

        # 6) notes
        notes = pd.DataFrame({
            "Section": [
                "Data Coverage", "Data Frequency", "Currency", "Units",
                "Methodology", "Disclaimer", "Support",
            ],
            "Detail": [
                f"{n_months} months trailing (latest = {series['month'].iloc[-1] if not series.empty else '-'})",
                "Monthly aggregates",
                "USD (revenue), local (transactions/users)",
                "Revenue in USD millions; transactions·users in counts",
                "Aggregated from card/web/app/foot-traffic panel data, normalized to monthly",
                "Mandata 대안데이터는 패널 추정치이며 실측 실적과 차이가 있을 수 있습니다. "
                "투자 의사결정의 단독 근거로 사용하지 마세요.",
                "support@mandata.kr",
            ],
        })
        notes.to_excel(writer, index=False, sheet_name="notes")

        # 컬럼 폭 자동 조정 (xlsxwriter only)
        if engine == "xlsxwriter":
            for sheet_name in writer.sheets:
                ws = writer.sheets[sheet_name]
                try:
                    ws.set_column(0, 30, 18)
                except Exception:
                    pass

    buf.seek(0)
    return buf.getvalue()


def paid_filename(prefix: str = "mandata_data") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.xlsx"
