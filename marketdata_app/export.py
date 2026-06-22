"""Export helpers — DataFrame → CSV / Excel / JSON bytes for st.download_button."""
from __future__ import annotations

import io
import json
from datetime import datetime
from typing import Literal

import pandas as pd

Format = Literal["csv", "xlsx", "json"]


MIME = {
    "csv": "text/csv",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "json": "application/json",
}

EXT = {"csv": "csv", "xlsx": "xlsx", "json": "json"}


def to_bytes(df: pd.DataFrame, fmt: Format) -> bytes:
    """DataFrame → bytes in chosen format."""
    if fmt == "csv":
        return df.to_csv(index=False).encode("utf-8-sig")  # BOM for Excel-on-Mac
    if fmt == "xlsx":
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="data")
        return buf.getvalue()
    if fmt == "json":
        # date를 ISO string으로 직렬화
        return df.to_json(orient="records", date_format="iso", force_ascii=False).encode("utf-8")
    raise ValueError(f"unsupported format: {fmt}")


def filename(prefix: str, fmt: Format) -> str:
    """단일 종목 등 prefix와 timestamp를 합친 파일명."""
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in prefix)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    return f"{safe}_{ts}.{EXT[fmt]}"


def wide_pivot(long_df: pd.DataFrame, value_col: str = "close") -> pd.DataFrame:
    """다종목 long → wide pivot. date × ticker matrix."""
    if long_df.empty:
        return long_df
    wide = long_df.pivot_table(index="date", columns="ticker", values=value_col)
    wide = wide.reset_index()
    return wide
