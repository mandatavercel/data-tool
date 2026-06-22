"""
kfnb_app/dataio.py — 데이터 접근 계층 (엔진 추상화).

대용량(수백 MB, 수백만 행) CSV는 duckdb 로 직접 집계하고, 이미 메모리에
올라온 DataFrame 이나 duckdb 미설치 환경은 pandas 로 폴백한다.
호출부(파이프라인)는 엔진을 신경 쓰지 않고 Source 인터페이스만 사용한다.

표준화: 원천 헤더를 config.RAW_COLUMNS / COLUMN_ALIASES 로 표준 컬럼명
(date, company_kr, brand_kr, cat_l1/2/3, barcode, sku_name_kr,
 sales_amt, sales_qty, sales_cnt, region) 으로 통일.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd

from kfnb_app import config

try:  # duckdb 는 권장 가속기. 없으면 pandas 폴백.
    import duckdb  # type: ignore
    _HAS_DUCKDB = True
except Exception:  # pragma: no cover
    _HAS_DUCKDB = False


CANONICAL = list(dict.fromkeys(config.RAW_COLUMNS.values()))


def canonical_rename(columns: list[str]) -> dict[str, str]:
    """원천 헤더 목록 → {원천명: 표준명} 매핑. 별칭/공백/BOM 흡수."""
    out: dict[str, str] = {}
    for c in columns:
        key = str(c).strip().lstrip("﻿")
        raw = config.COLUMN_ALIASES.get(key, key)
        if raw in config.RAW_COLUMNS:
            out[c] = config.RAW_COLUMNS[raw]
    return out


# ──────────────────────────────────────────────────────────────────────────
# Source 인터페이스
# ──────────────────────────────────────────────────────────────────────────
class Source:
    """집계 메서드 공통 인터페이스."""

    canonical_cols: list[str]
    raw_columns: list[str]

    def total_rows(self) -> int: raise NotImplementedError
    def category_options(self) -> pd.DataFrame: raise NotImplementedError
    def profile_stats(self) -> dict: raise NotImplementedError
    def distinct_skus(self) -> pd.DataFrame: raise NotImplementedError
    def monthly_panel(self, cat_l2: Optional[str] = None) -> pd.DataFrame: raise NotImplementedError
    def annual_company(self, cat_l2: Optional[str] = None) -> pd.DataFrame: raise NotImplementedError
    def annual_brand(self, brand_kr: str) -> pd.DataFrame: raise NotImplementedError
    def export_daily_en(self, keys, out_csv, cat_l2=None, lag_days=None) -> int: raise NotImplementedError


def open_source(src: Union[str, Path, pd.DataFrame],
                prefer_duckdb: bool = True) -> Source:
    """경로/DataFrame → 적절한 Source. duckdb 가능하면 우선 사용."""
    if prefer_duckdb and _HAS_DUCKDB:
        return _DuckSource(src)
    if isinstance(src, pd.DataFrame):
        return _PandasSource(src)
    return _PandasSource(_read_csv_any(src))


_ENCODINGS = ["utf-8-sig", "utf-8", "cp949", "euc-kr", "latin1"]
_CODE_DTYPES = {"ITEM_CD": str, "GRP_ACNT_CD": str, "GRP_ITEM_CD": str,
                "MDCL_CD": str, "SMCL_CD": str}


def detect_encoding(path: Union[str, Path]) -> str:
    """파일 앞부분으로 인코딩 추정 (한국어 CSV 의 cp949/euc-kr 대응)."""
    try:
        with open(path, "rb") as f:
            raw = f.read(200000)
    except Exception:
        return "utf-8-sig"
    for enc in _ENCODINGS:
        try:
            raw.decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return "latin1"


def _sniff_sep(path: Union[str, Path], enc: str) -> str:
    """헤더(첫 비어있지 않은 줄)에서 가장 빈번한 구분자 추정 (, ; \\t |)."""
    try:
        with open(path, encoding=enc, errors="replace") as f:
            for line in f:
                if line.strip():
                    return max([",", ";", "\t", "|"], key=line.count)
    except Exception:
        pass
    return ","


def _read_any(path: Union[str, Path], nrows: int | None = None) -> pd.DataFrame:
    """CSV/XLSX 견고 로더: 인코딩·구분자 자동감지 + 불량행 허용."""
    p = str(path)
    if p.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(p, dtype=_CODE_DTYPES, nrows=nrows)
    enc = detect_encoding(p)
    sep = _sniff_sep(p, enc)
    try:
        return pd.read_csv(p, dtype=_CODE_DTYPES, encoding=enc, sep=sep, nrows=nrows)
    except Exception:                                            # noqa: BLE001
        # 불량행(필드수 불일치) 스킵
        return pd.read_csv(p, dtype=_CODE_DTYPES, encoding=enc, sep=sep,
                           nrows=nrows, engine="python", on_bad_lines="skip")


# 하위호환 별칭
_read_csv_any = _read_any


def _fill_select_sql(present: dict) -> str:
    """{canon: raw} → 모든 CANONICAL 을 출력하는 SELECT 절. 누락은 가용 입자도로 보완.

    brand_kr←company, sku_name←brand/company, barcode=가용 식별자 concat, qty/cnt=NULL.
    (회사 단위/브랜드 단위/SKU 단위 어떤 데이터든 동일 파이프라인으로 처리)
    """
    def raw(c):
        return f'"{present[c]}"' if c in present else None
    exprs = []
    for canon in CANONICAL:
        if canon in present:
            e = f'"{present[canon]}"'
        elif canon == "region":
            e = "'(unknown)'"
        elif canon in ("region_code", "company_code", "brand_code"):
            e = "''"
        elif canon == "brand_kr":
            e = raw("company_kr")
        elif canon in ("cat_l1", "cat_l2", "cat_l3"):
            e = "'Uncategorized'"
        elif canon == "sku_name_kr":
            e = raw("brand_kr") or raw("company_kr")
        elif canon == "barcode":
            parts = [present["company_kr"]]
            if "brand_kr" in present:
                parts.append(present["brand_kr"])
            if "sku_name_kr" in present:
                parts.append(present["sku_name_kr"])
            inner = ", ".join(f'CAST("{p}" AS VARCHAR)' for p in parts)
            e = f"concat_ws('|', {inner})"
        else:                                   # sales_qty, sales_cnt 등
            e = "NULL"
        exprs.append(f"{e} AS {canon}")
    return ", ".join(exprs)


def _fill_canonical(df: pd.DataFrame, present: set) -> pd.DataFrame:
    """pandas 버전 표준컬럼 보완 (DuckDB _fill_select_sql 과 동일 규칙)."""
    out = df.copy()
    if "brand_kr" not in out:
        out["brand_kr"] = out["company_kr"]
    if "sku_name_kr" not in out:
        out["sku_name_kr"] = out["brand_kr"]
    if "barcode" not in out:
        parts = [out["company_kr"].astype(str)]
        if "brand_kr" in present:
            parts.append(out["brand_kr"].astype(str))
        if "sku_name_kr" in present:
            parts.append(out["sku_name_kr"].astype(str))
        bc = parts[0]
        for p in parts[1:]:
            bc = bc + "|" + p
        out["barcode"] = bc
    for c in ("cat_l1", "cat_l2", "cat_l3"):
        if c not in out:
            out[c] = "Uncategorized"
    if "region" not in out:
        out["region"] = "(unknown)"
    for c in ("region_code", "company_code", "brand_code"):
        if c not in out:
            out[c] = ""
    for c in ("sales_qty", "sales_cnt"):
        if c not in out:
            out[c] = np.nan
    return out


def peek_columns(src: Union[str, Path, pd.DataFrame]) -> list[str]:
    """전체 적재 없이 헤더 컬럼만 빠르게 읽는다 (스키마 사전 검증용)."""
    if isinstance(src, pd.DataFrame):
        return [str(c) for c in src.columns]
    try:
        return [str(c) for c in _read_any(src, nrows=5).columns]
    except Exception:
        return []


# ──────────────────────────────────────────────────────────────────────────
# duckdb 엔진
# ──────────────────────────────────────────────────────────────────────────
class _DuckSource(Source):
    def __init__(self, src: Union[str, Path, pd.DataFrame]):
        self.con = duckdb.connect()
        if isinstance(src, pd.DataFrame):
            self._df_ref = src  # 등록 유지
            self.con.register("src_df", src)
            base = "src_df"
            cols = list(src.columns)
        elif str(src).lower().endswith((".csv", ".txt", ".tsv")) and \
                detect_encoding(src) in ("utf-8-sig", "utf-8"):
            p = str(src).replace("'", "''")
            base = f"read_csv_auto('{p}', header=true, all_varchar=false)"
            cols = [r[0] for r in self.con.execute(
                f"DESCRIBE SELECT * FROM {base}").fetchall()]
        else:
            # xlsx/cp949/비정형 구분자 → 견고한 pandas 로더로 읽어 등록
            self._df_ref = _read_any(src)
            self.con.register("src_df", self._df_ref)
            base = "src_df"
            cols = list(self._df_ref.columns)
        self.raw_columns = list(cols)
        ren = canonical_rename(cols)            # {raw: canon}
        present = {canon: raw for raw, canon in ren.items()}   # {canon: raw}
        self.present_canon = set(present)
        # 누락 표준컬럼을 가용 입자도로 자동 보완 (회사/브랜드/SKU 어디든 처리)
        sel = _fill_select_sql(present)
        self.canonical_cols = list(CANONICAL)
        self.con.execute(f"CREATE VIEW t AS SELECT {sel} FROM {base}")

    def total_rows(self) -> int:
        return int(self.con.execute("SELECT COUNT(*) FROM t").fetchone()[0])

    def category_options(self) -> pd.DataFrame:
        return self.con.execute("""
            SELECT cat_l2, COUNT(DISTINCT barcode) skus, SUM(sales_amt) sales_amt
            FROM t GROUP BY 1 ORDER BY sales_amt DESC""").df()

    def profile_stats(self) -> dict:
        q = """SELECT
            COUNT(*) n_rows,
            MIN(date) min_d, MAX(date) max_d,
            COUNT(DISTINCT company_kr) n_co,
            COUNT(DISTINCT brand_kr) n_brand,
            COUNT(DISTINCT barcode) n_sku,
            COUNT(DISTINCT region) n_region,
            COUNT(DISTINCT date) n_days,
            SUM(CASE WHEN company_kr IS NULL THEN 1 ELSE 0 END) null_co,
            SUM(CASE WHEN sku_name_kr IS NULL THEN 1 ELSE 0 END) null_sku,
            SUM(CASE WHEN sales_amt<=0 THEN 1 ELSE 0 END) nonpos_amt,
            SUM(sales_amt) tot_amt
          FROM t"""
        r = self.con.execute(q).fetchone()
        keys = ["n_rows","min_d","max_d","n_co","n_brand","n_sku","n_region",
                "n_days","null_co","null_sku","nonpos_amt","tot_amt"]
        d = dict(zip(keys, r))
        d["barcode_len_ok"] = int(self.con.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT barcode FROM t "
            "WHERE LENGTH(CAST(barcode AS VARCHAR))=?)",
            [config.THRESHOLDS.barcode_len]).fetchone()[0])
        d["cat_l1"] = [row[0] for row in self.con.execute(
            "SELECT cat_l1 FROM t GROUP BY 1 ORDER BY SUM(sales_amt) DESC").fetchall()]
        return d

    def distinct_skus(self) -> pd.DataFrame:
        return self.con.execute("""
            SELECT company_kr, brand_kr, cat_l1, cat_l2, cat_l3,
                   CAST(barcode AS VARCHAR) barcode, sku_name_kr,
                   SUM(sales_amt) sales_amt, SUM(sales_qty) sales_qty,
                   MIN(date) first_date, MAX(date) last_date
            FROM t GROUP BY 1,2,3,4,5,6,7
            ORDER BY sales_amt DESC""").df()

    def monthly_panel(self, cat_l2: Optional[str] = None) -> pd.DataFrame:
        w, p = ("WHERE cat_l2=?", [cat_l2]) if cat_l2 else ("", [])
        return self.con.execute(f"""
            SELECT CAST(date/100 AS INT) ym, company_kr, brand_kr,
                   SUM(sales_amt) sales_amt, SUM(sales_qty) sales_qty,
                   SUM(sales_cnt) receipts
            FROM t {w} GROUP BY 1,2,3 ORDER BY 1, sales_amt DESC""", p).df()

    def annual_company(self, cat_l2: Optional[str] = None) -> pd.DataFrame:
        w, p = ("WHERE cat_l2=?", [cat_l2]) if cat_l2 else ("", [])
        return self.con.execute(f"""
            SELECT CAST(date/10000 AS INT) yr, company_kr,
                   SUM(sales_amt) sales_amt, SUM(sales_qty) sales_qty
            FROM t {w} GROUP BY 1,2 ORDER BY 1,3 DESC""", p).df()

    def annual_brand(self, brand_kr: str) -> pd.DataFrame:
        b = brand_kr.replace("'", "''")
        return self.con.execute(f"""
            SELECT CAST(date/10000 AS INT) yr,
                   SUM(sales_amt) sales_amt, SUM(sales_qty) sales_qty
            FROM t WHERE brand_kr='{b}' GROUP BY 1 ORDER BY 1""").df()

    def export_daily_en(self, keys: pd.DataFrame, out_csv: str,
                        cat_l2: Optional[str] = None, lag_days=None) -> int:
        """일별 거래 데이터에 영문 마스터(keys)를 barcode 로 조인해 CSV 직출력."""
        k = keys.copy(); k["barcode"] = k["barcode"].astype(str)
        self.con.register("keys_df", k)
        rmap = pd.DataFrame(list(config.REGION_EN.items()),
                            columns=["region", "region_en"])
        self.con.register("region_map", rmap)
        cmap = pd.DataFrame(list(config.CATEGORY_EN.items()),
                            columns=["cat_ko", "cat_en"])
        self.con.register("cat_map", cmap)
        w = "WHERE t.cat_l2=?" if cat_l2 else ""
        params = [cat_l2] if cat_l2 else []
        lag = int(config.THRESHOLDS.pos_release_lag_days if lag_days is None else lag_days)
        self.con.execute(f"""
            COPY (
              SELECT t.date,
                     strftime(strptime(CAST(t.date AS VARCHAR), '%Y%m%d')
                              + INTERVAL '{lag}' DAY, '%Y-%m-%d') AS available_date,
                     t.region, COALESCE(rm.region_en, t.region) AS region_en,
                     k.isin, k.company_en_official AS company_name_en,
                     k.brand_id, k.brand_name_en, k.sku_id, k.sku_name_en,
                     t.cat_l1, COALESCE(c1.cat_en, t.cat_l1) AS cat_l1_en,
                     t.cat_l2, COALESCE(c2.cat_en, t.cat_l2) AS cat_l2_en,
                     t.cat_l3, COALESCE(c3.cat_en, t.cat_l3) AS cat_l3_en,
                     t.sales_amt, t.sales_qty, t.sales_cnt
              FROM t LEFT JOIN keys_df k
                ON CAST(t.barcode AS VARCHAR)=k.barcode
              LEFT JOIN region_map rm ON t.region=rm.region
              LEFT JOIN cat_map c1 ON t.cat_l1=c1.cat_ko
              LEFT JOIN cat_map c2 ON t.cat_l2=c2.cat_ko
              LEFT JOIN cat_map c3 ON t.cat_l3=c3.cat_ko
              {w} ORDER BY t.date
            ) TO '{out_csv}' (HEADER, DELIMITER ',')""", params)
        n = int(self.con.execute(
            f"SELECT COUNT(*) FROM t {w}", params).fetchone()[0])
        self.con.unregister("keys_df")
        self.con.unregister("region_map")
        self.con.unregister("cat_map")
        return n


# ──────────────────────────────────────────────────────────────────────────
# pandas 엔진 (폴백)
# ──────────────────────────────────────────────────────────────────────────
class _PandasSource(Source):
    def __init__(self, df: pd.DataFrame):
        self.raw_columns = [str(c) for c in df.columns]
        ren = canonical_rename(list(df.columns))
        self.df = df.rename(columns=ren)
        self.present_canon = set(ren.values())
        # 누락 표준컬럼을 가용 입자도로 자동 보완
        self.df = _fill_canonical(self.df, self.present_canon)
        self.canonical_cols = list(CANONICAL)
        if "date" in self.df:
            self.df["date"] = pd.to_numeric(self.df["date"], errors="coerce")
        for c in ("sales_amt", "sales_qty", "sales_cnt"):
            if c in self.df:
                self.df[c] = pd.to_numeric(self.df[c], errors="coerce").fillna(0)
        if "barcode" in self.df:
            self.df["barcode"] = self.df["barcode"].astype(str)

    def total_rows(self) -> int:
        return len(self.df)

    def category_options(self) -> pd.DataFrame:
        if "cat_l2" not in self.df or "barcode" not in self.df:
            return pd.DataFrame(columns=["cat_l2", "skus", "sales_amt"])
        return (self.df.groupby("cat_l2")
                .agg(skus=("barcode", "nunique"), sales_amt=("sales_amt", "sum"))
                .reset_index().sort_values("sales_amt", ascending=False))

    def profile_stats(self) -> dict:
        d = self.df
        bl = config.THRESHOLDS.barcode_len
        return {
            "n_rows": len(d),
            "min_d": int(d["date"].min()) if "date" in d else None,
            "max_d": int(d["date"].max()) if "date" in d else None,
            "n_co": d["company_kr"].nunique() if "company_kr" in d else 0,
            "n_brand": d["brand_kr"].nunique() if "brand_kr" in d else 0,
            "n_sku": d["barcode"].nunique() if "barcode" in d else 0,
            "n_region": d["region"].nunique() if "region" in d else 0,
            "n_days": d["date"].nunique() if "date" in d else 0,
            "null_co": int(d["company_kr"].isna().sum()) if "company_kr" in d else 0,
            "null_sku": int(d["sku_name_kr"].isna().sum()) if "sku_name_kr" in d else 0,
            "nonpos_amt": int((d["sales_amt"] <= 0).sum()) if "sales_amt" in d else 0,
            "tot_amt": float(d["sales_amt"].sum()) if "sales_amt" in d else 0.0,
            "barcode_len_ok": int(d.loc[d["barcode"].str.len() == bl, "barcode"].nunique())
                if "barcode" in d else 0,
            "cat_l1": (d.groupby("cat_l1")["sales_amt"].sum()
                       .sort_values(ascending=False).index.tolist())
                if "cat_l1" in d else [],
        }

    def distinct_skus(self) -> pd.DataFrame:
        g = (self.df.groupby(
                ["company_kr","brand_kr","cat_l1","cat_l2","cat_l3","barcode","sku_name_kr"],
                dropna=False)
             .agg(sales_amt=("sales_amt","sum"), sales_qty=("sales_qty","sum"),
                  first_date=("date","min"), last_date=("date","max"))
             .reset_index().sort_values("sales_amt", ascending=False))
        return g

    def monthly_panel(self, cat_l2: Optional[str] = None) -> pd.DataFrame:
        d = self.df if cat_l2 is None else self.df[self.df["cat_l2"] == cat_l2]
        d = d.assign(ym=(d["date"] // 100).astype(int))
        return (d.groupby(["ym","company_kr","brand_kr"])
                 .agg(sales_amt=("sales_amt","sum"), sales_qty=("sales_qty","sum"),
                      receipts=("sales_cnt","sum"))
                 .reset_index().sort_values(["ym","sales_amt"], ascending=[True, False]))

    def annual_company(self, cat_l2: Optional[str] = None) -> pd.DataFrame:
        d = self.df if cat_l2 is None else self.df[self.df["cat_l2"] == cat_l2]
        d = d.assign(yr=(d["date"] // 10000).astype(int))
        return (d.groupby(["yr","company_kr"])
                 .agg(sales_amt=("sales_amt","sum"), sales_qty=("sales_qty","sum"))
                 .reset_index().sort_values(["yr","sales_amt"], ascending=[True, False]))

    def annual_brand(self, brand_kr: str) -> pd.DataFrame:
        d = self.df[self.df["brand_kr"] == brand_kr]
        d = d.assign(yr=(d["date"] // 10000).astype(int))
        return (d.groupby("yr")
                 .agg(sales_amt=("sales_amt","sum"), sales_qty=("sales_qty","sum"))
                 .reset_index().sort_values("yr"))

    def export_daily_en(self, keys: pd.DataFrame, out_csv: str,
                        cat_l2: Optional[str] = None, lag_days=None) -> int:
        k = keys.copy(); k["barcode"] = k["barcode"].astype(str)
        d = self.df if cat_l2 is None else self.df[self.df["cat_l2"] == cat_l2]
        d = d.copy(); d["barcode"] = d["barcode"].astype(str)
        m = d.merge(k, on="barcode", how="left")
        if "region" in m.columns:
            m["region_en"] = m["region"].map(config.REGION_EN).fillna(m["region"])
        for k_ in ("cat_l1", "cat_l2", "cat_l3"):
            if k_ in m.columns:
                m[f"{k_}_en"] = m[k_].map(config.CATEGORY_EN).fillna(m[k_])
        lag = int(config.THRESHOLDS.pos_release_lag_days if lag_days is None else lag_days)
        m["available_date"] = (pd.to_datetime(m["date"], format="%Y%m%d", errors="coerce")
                               + pd.Timedelta(days=lag)).dt.strftime("%Y-%m-%d")
        out_cols = ["date", "available_date", "region", "region_en", "isin",
                    "company_en_official", "brand_id", "brand_name_en", "sku_id",
                    "sku_name_en", "cat_l1", "cat_l1_en", "cat_l2", "cat_l2_en",
                    "cat_l3", "cat_l3_en", "sales_amt", "sales_qty", "sales_cnt"]
        out_cols = [c for c in out_cols if c in m.columns]
        m = m[out_cols].rename(columns={"company_en_official": "company_name_en"})
        m.sort_values("date").to_csv(out_csv, index=False, encoding="utf-8-sig")
        return len(m)
