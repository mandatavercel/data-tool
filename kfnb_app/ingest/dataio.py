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

import os
import tempfile
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


def canonical_rename(columns: list[str],
                     extra_rename: dict | None = None) -> dict[str, str]:
    """원천 헤더 목록 → {원천명: 표준명} 매핑. 별칭/공백/BOM 흡수.

    extra_rename: 사용자 수동 매핑 {원천컬럼: 표준컬럼}. 자동 매핑을 덮어쓴다.
    """
    out: dict[str, str] = {}
    for c in columns:
        key = str(c).strip().lstrip("﻿")
        raw = config.COLUMN_ALIASES.get(key, key)
        if raw in config.RAW_COLUMNS:
            out[c] = config.RAW_COLUMNS[raw]
    for raw, canon in (extra_rename or {}).items():
        if raw in columns and canon:
            out[raw] = canon
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
    def sku_monthly(self, cat_l2: Optional[str] = None) -> pd.DataFrame: raise NotImplementedError
    def annual_company(self, cat_l2: Optional[str] = None) -> pd.DataFrame: raise NotImplementedError
    def annual_brand(self, brand_kr: str) -> pd.DataFrame: raise NotImplementedError
    def export_daily_en(self, keys, out_csv, cat_l2=None, lag_days=None) -> int: raise NotImplementedError


def open_source(src: Union[str, Path, pd.DataFrame],
                prefer_duckdb: bool = True,
                extra_rename: dict | None = None, progress=None) -> Source:
    """경로/DataFrame → 적절한 Source. duckdb 가능하면 우선 사용.

    extra_rename: 자동 인식이 놓친 컬럼의 수동 매핑 {원천컬럼: 표준컬럼}.
    progress(done, total): 변환/적재 진행 콜백(UI 진행바).
    """
    if prefer_duckdb and _HAS_DUCKDB:
        return _DuckSource(src, extra_rename=extra_rename, progress=progress)
    # duckdb 없이 대용량 파일을 pandas 로 통째 적재하면 메모리 초과(특히 16GB RAM).
    if not isinstance(src, pd.DataFrame):
        try:
            big = Path(src).is_dir() or os.path.getsize(src) > 500 * 1024 * 1024
        except Exception:                          # noqa: BLE001
            big = False
        if big:
            raise RuntimeError(
                "대용량 파일은 duckdb 엔진이 필요합니다(현재 미설치). 터미널에서 "
                "`pip install duckdb` 후 다시 실행하세요. duckdb 없이는 전체를 메모리에 "
                "올려야 해 16GB RAM 에선 처리가 어렵습니다.")
    if isinstance(src, pd.DataFrame):
        return _PandasSource(src, extra_rename=extra_rename)
    return _PandasSource(_read_csv_any(src), extra_rename=extra_rename)


_ENCODINGS = ["utf-8-sig", "utf-8", "cp949", "euc-kr", "latin1"]
_CODE_DTYPES = {"ITEM_CD": str, "GRP_ACNT_CD": str, "GRP_ITEM_CD": str,
                "MDCL_CD": str, "SMCL_CD": str}


def detect_encoding(path: Union[str, Path]) -> str:
    """파일 앞부분으로 인코딩 추정 (한국어 CSV 의 cp949/euc-kr 대응).

    ⚠️ 샘플 경계에서 멀티바이트 글자가 잘리면 utf-8 디코드가 실패해 cp949 로
    오판하기 쉽다(→ UTF-8 파일을 cp949 로 잘못 변환해 한글 깨짐). 이를 막기 위해
    utf-8 은 '증분 디코더'로 검사해 *말미 잘림*은 무시하고 진짜 깨진 바이트만 거른다.
    """
    try:
        with open(path, "rb") as f:
            raw = f.read(1_000_000)
    except Exception:
        return "utf-8-sig"
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    import codecs
    try:
        # final=False → 마지막 불완전 멀티바이트(샘플 경계 잘림)는 에러 아님.
        codecs.getincrementaldecoder("utf-8")("strict").decode(raw, False)
        return "utf-8"
    except UnicodeDecodeError:
        pass                                       # 진짜 비-utf8 → 아래 후보
    for enc in ("cp949", "euc-kr"):
        try:
            # 말미 잘림 무시(final=False) — cp949 도 멀티바이트라 경계 잘림 가능.
            codecs.getincrementaldecoder(enc)("strict").decode(raw, False)
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


def _transcode_to_utf8(path: Union[str, Path], enc: str,
                       chunk_bytes: int = 16 * 1024 * 1024, progress=None) -> str:
    """대용량 비-UTF8 CSV 를 *스트리밍*(상수 메모리)으로 UTF-8 임시파일로 변환.

    바이트 단위 디코드/인코드 — pandas CSV 파싱보다 훨씬 빠르고 메모리 일정. 변환 후
    duckdb 네이티브 파서로 빠르게 적재한다. progress(done_bytes, total_bytes) 콜백 지원.
    """
    fd, out = tempfile.mkstemp(suffix=".utf8.csv", prefix="kfnb_")
    os.close(fd)
    total = os.path.getsize(path) or 1
    done = 0
    import codecs
    dec = codecs.getincrementaldecoder(enc)(errors="replace")
    with open(path, "rb") as fin, open(out, "w", encoding="utf-8", newline="") as fout:
        while True:
            b = fin.read(chunk_bytes)
            if not b:
                break
            fout.write(dec.decode(b))
            done += len(b)
            if progress is not None:
                try:
                    progress(done, total)
                except Exception:                  # noqa: BLE001
                    pass
        fout.write(dec.decode(b"", final=True))
    return out


def cleanup_temp(older_than_sec: int = 1800) -> int:
    """이전 실행이 남긴 임시파일(kfnb_*.utf8.csv / kfnb_*.duckdb)을 정리해 디스크 회수.

    older_than_sec 보다 오래된 것만 삭제(현재 사용 중 파일 보호). 삭제 개수 반환.
    """
    import glob
    import time
    n = 0
    tmp = tempfile.gettempdir()
    pats = ["kfnb_*.utf8.csv", "kfnb_*.duckdb", "kfnb_*.duckdb.wal"]
    now = time.time()
    for pat in pats:
        for f in glob.glob(os.path.join(tmp, pat)):
            try:
                if now - os.path.getmtime(f) > older_than_sec:
                    os.remove(f); n += 1
            except Exception:                      # noqa: BLE001
                pass
    return n


def _gather_csv_files(src: Union[str, Path]) -> list[str]:
    """입력이 폴더면 내부 CSV류 파일들을, 단일 파일이면 그 파일을 리스트로 반환.

    - 폴더: *.csv/*.txt/*.tsv 재귀 수집 → 없으면 숨김/엑셀 제외 일반 파일.
    - 단일(확장자 무관, xlsx 제외): 그 파일.
    """
    p = Path(src)
    if p.is_dir():
        exts = (".csv", ".txt", ".tsv")
        files = sorted(str(f) for f in p.rglob("*")
                       if f.is_file() and f.suffix.lower() in exts)
        if not files:                              # 확장자 없는 회사별 파일 대비
            files = sorted(str(f) for f in p.rglob("*")
                           if f.is_file() and not f.name.startswith(".")
                           and f.suffix.lower() not in (".xlsx", ".xls"))
        return files
    return [str(p)]


def _xlsx(path: str) -> bool:
    return str(path).lower().endswith((".xlsx", ".xls"))


def _configure_duckdb(con, mem_gb: float = 4.0) -> None:
    """duckdb 메모리 한도 + 디스크 스필 설정 (저사양 RAM 보호)."""
    try:
        con.execute(f"SET memory_limit='{mem_gb:.1f}GB'")
        con.execute(f"SET temp_directory='{tempfile.gettempdir()}'")
        con.execute("SET preserve_insertion_order=false")
    except Exception:                              # noqa: BLE001 — 일부 버전 옵션차
        pass


def _read_any(path: Union[str, Path], nrows: int | None = None) -> pd.DataFrame:
    """CSV/XLSX 견고 로더: 인코딩·구분자 자동감지 + 불량행 허용. 폴더면 파일들 결합."""
    # 폴더 입력: 내부 CSV류를 모두 읽어 결합(pandas 폴백 경로)
    if Path(path).is_dir():
        files = _gather_csv_files(path)
        frames = [_read_any(f, nrows=nrows) for f in files if not _xlsx(f)]
        frames = [f for f in frames if f is not None and len(f.columns)]
        if not frames:
            raise ValueError(f"읽을 CSV 파일을 찾지 못했습니다: {path}")
        return pd.concat(frames, ignore_index=True)
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
    def __init__(self, src: Union[str, Path, pd.DataFrame],
                 extra_rename: dict | None = None, progress=None):
        # 온디스크 duckdb 파일에 연결 — 대용량 테이블을 RAM 대신 디스크에 적재(16GB RAM
        # 보호). DataFrame(소규모/테스트)은 인메모리로 충분.
        self._db_path: str | None = None
        if isinstance(src, pd.DataFrame):
            self.con = duckdb.connect()
        else:
            fd, self._db_path = tempfile.mkstemp(suffix=".duckdb", prefix="kfnb_")
            os.close(fd); os.remove(self._db_path)   # duckdb 가 새로 생성하도록
            self.con = duckdb.connect(self._db_path)
        _configure_duckdb(self.con)
        self._tmp_utf8: list[str] = []             # 변환 임시파일(정리용)
        self._progress = progress
        if isinstance(src, pd.DataFrame):
            self._df_ref = src  # 등록 유지
            self.con.register("src_df", src)
            base = "src_df"
            cols = list(src.columns)
        elif Path(src).is_dir() or not _xlsx(src):
            files = [f for f in _gather_csv_files(src) if not _xlsx(f)]
            if not files:
                raise ValueError(f"읽을 CSV 파일을 찾지 못했습니다: {src}")
            # 비-UTF8(cp949 등)은 바이트 단위 스트리밍 변환(빠름·상수 메모리) 후
            # duckdb 네이티브 파서로 적재. UTF-8 은 바로 네이티브 적재.
            read_paths = []
            ntot = len(files)
            for i, f in enumerate(files):
                enc = detect_encoding(f)
                if enc not in ("utf-8-sig", "utf-8"):
                    def _p(done, total, _i=i):
                        if self._progress:
                            # 파일별 변환 진행(전체 파일 수로 분할)
                            self._progress((_i + done / max(total, 1)) / ntot,
                                           f"인코딩 변환 중… {done//(1024*1024)}MB")
                    read_paths.append(_transcode_to_utf8(f, enc, progress=_p))
                    self._tmp_utf8.append(read_paths[-1])
                else:
                    read_paths.append(str(f))
            lst = ", ".join("'" + p.replace("'", "''") + "'" for p in read_paths)
            base = (f"read_csv_auto([{lst}], header=true, all_varchar=false, "
                    f"ignore_errors=true, sample_size=200000, union_by_name=true)")
            cols = [r[0] for r in self.con.execute(
                f"DESCRIBE SELECT * FROM {base}").fetchall()]
        else:
            # xlsx 등은 스트리밍 불가 → pandas 로더(대용량 xlsx 는 비권장)
            self._df_ref = _read_any(src)
            self.con.register("src_df", self._df_ref)
            base = "src_df"
            cols = list(self._df_ref.columns)
        self.raw_columns = list(cols)
        ren = canonical_rename(cols, extra_rename)   # {raw: canon}
        present = {canon: raw for raw, canon in ren.items()}   # {canon: raw}
        self.present_canon = set(present)
        # 누락 표준컬럼을 가용 입자도로 자동 보완 (회사/브랜드/SKU 어디든 처리)
        sel = _fill_select_sql(present)
        self.canonical_cols = list(CANONICAL)
        # ⚡ 1회 스캔으로 물리 테이블에 적재(materialize). 뷰로 두면 단계마다 CSV 를
        #    재파싱해 대용량에서 치명적으로 느려진다. 테이블은 컬럼형·압축 + memory_limit
        #    초과 시 디스크 스필로 큰 데이터도 처리.
        try:
            self.con.execute(f"CREATE TABLE t AS SELECT {sel} FROM {base}")
            self._post_materialize()               # 테이블 적재 성공 → 정리
        except Exception:                          # noqa: BLE001 — 폴백(뷰, 백킹 유지)
            self.con.execute(f"CREATE VIEW t AS SELECT {sel} FROM {base}")

    def _post_materialize(self):
        """적재 후 임시파일/인메모리 df 정리(디스크·RAM 회수)."""
        for tmp in getattr(self, "_tmp_utf8", []):
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:                      # noqa: BLE001
                pass
        self._tmp_utf8 = []
        if getattr(self, "_df_ref", None) is not None:
            try:
                self.con.unregister("src_df")
            except Exception:                      # noqa: BLE001
                pass
            self._df_ref = None

    def __del__(self):                              # 임시파일/DB 정리(세션 종료 시)
        for t in getattr(self, "_tmp_utf8", []) or []:
            try:
                if os.path.exists(t):
                    os.remove(t)
            except Exception:                      # noqa: BLE001
                pass
        try:
            if getattr(self, "con", None) is not None:
                self.con.close()
        except Exception:                          # noqa: BLE001
            pass
        for ext in ("", ".wal"):                    # duckdb 파일 + WAL
            try:
                p = (self._db_path or "") + ext
                if self._db_path and os.path.exists(p):
                    os.remove(p)
            except Exception:                      # noqa: BLE001
                pass

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

    def sku_monthly(self, cat_l2: Optional[str] = None) -> pd.DataFrame:
        w, p = ("WHERE cat_l2=?", [cat_l2]) if cat_l2 else ("", [])
        return self.con.execute(f"""
            SELECT CAST(date/100 AS INT) ym, company_kr, brand_kr,
                   CAST(barcode AS VARCHAR) barcode,
                   SUM(sales_amt) sales_amt, SUM(sales_qty) sales_qty
            FROM t {w} GROUP BY 1,2,3,4 ORDER BY 1""", p).df()

    def daily_panel(self, cat_l2: Optional[str] = None) -> pd.DataFrame:
        """거래일(YYYYMMDD)×회사×브랜드 집계 — 전달 템플릿 '기본매출'용."""
        w, p = ("WHERE cat_l2=?", [cat_l2]) if cat_l2 else ("", [])
        return self.con.execute(f"""
            SELECT date, company_kr, brand_kr,
                   SUM(sales_amt) sales_amt, SUM(sales_qty) sales_qty,
                   SUM(sales_cnt) sales_cnt
            FROM t {w} GROUP BY 1,2,3 ORDER BY 1""", p).df()

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
    def __init__(self, df: pd.DataFrame, extra_rename: dict | None = None):
        self.raw_columns = [str(c) for c in df.columns]
        ren = canonical_rename(list(df.columns), extra_rename)
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

    def sku_monthly(self, cat_l2: Optional[str] = None) -> pd.DataFrame:
        d = self.df if cat_l2 is None else self.df[self.df["cat_l2"] == cat_l2]
        d = d.copy()
        d["ym"] = (pd.to_numeric(d["date"], errors="coerce") // 100).astype("Int64")
        d["barcode"] = d["barcode"].astype(str)
        return (d.groupby(["ym", "company_kr", "brand_kr", "barcode"])
                 .agg(sales_amt=("sales_amt", "sum"), sales_qty=("sales_qty", "sum"))
                 .reset_index().sort_values("ym"))

    def daily_panel(self, cat_l2: Optional[str] = None) -> pd.DataFrame:
        d = self.df if cat_l2 is None else self.df[self.df["cat_l2"] == cat_l2]
        agg = {"sales_amt": ("sales_amt", "sum"), "sales_qty": ("sales_qty", "sum")}
        if "sales_cnt" in d.columns:
            agg["sales_cnt"] = ("sales_cnt", "sum")
        return (d.groupby(["date", "company_kr", "brand_kr"]).agg(**agg)
                 .reset_index().sort_values("date"))

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
