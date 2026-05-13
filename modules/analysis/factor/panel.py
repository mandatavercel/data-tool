"""PIT Panel Builder — 거래/매출 데이터 → (stock × month) panel + signal_date.

Look-ahead bias 가드:
  - sales는 sales_month 단위로 집계
  - available_date = sales_month_end + N business days (default N=5)
  - signal_date = available_date (그 시점에 만들 수 있던 신호)
  - forward returns는 signal_date 다음 영업일부터 측정
"""
from __future__ import annotations
import pandas as pd

from modules.common.foundation import _parse_dates


def build_pit_panel(
    sales_df: pd.DataFrame,
    sales_col: str,
    date_col: str,
    company_col: str,
    stock_col: str,
    tx_col: str | None = None,
    available_lag_days: int = 5,
) -> pd.DataFrame:
    """원시 거래 데이터 → 월별 (stock × month) panel.

    Parameters
    ----------
    sales_df : 원시 거래 데이터 (행 = 거래)
    sales_col, date_col, company_col, stock_col : role-mapped 컬럼명
    tx_col : 거래건수 컬럼 (있으면)
    available_lag_days : 매출월 종료 후 데이터가 가용해지는 영업일 수 (default 5)

    Returns
    -------
    DataFrame with columns:
      stock_code, company, sales_month (Period[M]), month_end (Timestamp),
      sales, [tx_count], available_date, signal_date
    """
    df = sales_df.copy()
    df["_sales"] = pd.to_numeric(df[sales_col], errors="coerce")
    # YYYYMMDD 정수 등 다양한 포맷 정확히 처리하는 foundation 헬퍼 사용
    df["_date"]  = _parse_dates(df[date_col])
    if tx_col and tx_col in df.columns:
        df["_tx"] = pd.to_numeric(df[tx_col], errors="coerce")
    df = df.dropna(subset=["_sales", "_date", stock_col])
    df["_month"] = df["_date"].dt.to_period("M")

    agg_spec = {"_sales": "sum"}
    if tx_col:
        agg_spec["_tx"] = "sum"

    # 회사명·종목코드 (회사별 첫 값 — 한 stock_code에 여러 row면 대표)
    panel = (
        df.groupby([stock_col, "_month"], as_index=False)
        .agg(agg_spec)
    )
    panel.columns = ["stock_code", "sales_month", "sales"] + (
        ["tx_count"] if tx_col else []
    )

    # 회사명 매핑 (stock_code → 가장 빈도 높은 company name)
    name_map = (
        df.groupby(stock_col)[company_col]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else "")
        .to_dict()
    )
    panel["company"] = panel["stock_code"].map(name_map)

    # 종목코드 6자리 정규화
    panel["stock_code"] = panel["stock_code"].astype(str).str.extract(r"(\d{6})")[0]
    panel = panel.dropna(subset=["stock_code"])

    # PIT 처리: month_end → available_date (영업일 기준 +N) → signal_date
    panel["month_end"] = panel["sales_month"].dt.to_timestamp(how="end").dt.normalize()
    panel["available_date"] = panel["month_end"] + pd.tseries.offsets.BDay(available_lag_days)
    panel["signal_date"]    = panel["available_date"]

    return panel.sort_values(["stock_code", "sales_month"]).reset_index(drop=True)
