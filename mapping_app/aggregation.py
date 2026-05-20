"""
mapping_app/aggregation.py — 회사·브랜드 합산 변환.

원천 raw (회사·브랜드 분리 컬럼) → 표준 레이아웃 (mandata_brand_name 형식).

표준 형식 (회사당):
  - 농심_all       = 회사 전체 합
  - 농심_신라면     = 명시한 브랜드 1
  - 농심_새우깡     = 명시한 브랜드 2
  - …
  - 농심_기타       = (전체 - 명시한 브랜드들의 합)

mandata_brand_code = jurir_no + 001/002/003 순번 (회사 내부)
"""
from __future__ import annotations

import io
import re

import pandas as pd


# ── 정규화 헬퍼 ───────────────────────────────────────────────────────────────
def _norm_token(s: str) -> str:
    """브랜드/회사명 토큰 정규화 — 공백·특수문자→underscore."""
    if not s:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s*&\s*", "&", s)
    s = re.sub(r"[.,;:!?]", " ", s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def _brand_name(company_kr: str, brand_label: str) -> str:
    """mandata_brand_name 빌드 — '회사_브랜드' (정규화)."""
    c = _norm_token(company_kr)
    b = _norm_token(brand_label)
    return f"{c}_{b}" if b else c


# ══════════════════════════════════════════════════════════════════════════════
# 핵심 집계
# ══════════════════════════════════════════════════════════════════════════════

def build_aggregated_layout(
    raw_df: pd.DataFrame,
    company_col: str,
    brand_col: str | None,
    group_dim_cols: list[str],
    value_cols: list[str],
    keep_top_brands_per_company: int = 5,
    brand_keep_per_company: dict[str, list[str]] | None = None,
    jurir_no_map: dict[str, str] | None = None,
    include_all_row: bool = True,
    include_other_row: bool = True,
    all_label: str = "all",
    other_label: str = "기타",
) -> pd.DataFrame:
    """raw_df 를 표준 레이아웃으로 집계.

    Args:
        raw_df: 원천 데이터
        company_col: 회사명 컬럼 (예 'GRP_ACNT_NM')
        brand_col:   브랜드 컬럼 (예 'GRP_ITEM_NM') — None 이면 _all 1행만
        group_dim_cols: 그룹화 차원 (예 ['YMD_CD','SIDO_NM','channel'])
                         transaction_date 등 raw 행마다 다른 차원들.
        value_cols: 집계할 값 컬럼 (sales_amount, sales_qty 등; numeric)
        keep_top_brands_per_company: 회사별 브랜드 합계 기준 상위 N개 명시
        brand_keep_per_company: {회사: [지정 브랜드 리스트]} — top-N 무시하고 직접 지정
        jurir_no_map: {회사: 법인등록번호} — code 빌드용. 없으면 빈 문자열
        include_all_row: 회사_all 행 포함 여부
        include_other_row: 회사_기타 행 포함 여부 (브랜드 split 후 잔차)

    Returns: 표준 레이아웃 DataFrame
        cols = group_dim_cols + ['mandata_brand_name', 'mandata_brand_code'] + value_cols
    """
    if company_col not in raw_df.columns:
        raise ValueError(f"회사 컬럼 없음: {company_col}")
    for c in group_dim_cols:
        if c not in raw_df.columns:
            raise ValueError(f"그룹 차원 컬럼 없음: {c}")
    for c in value_cols:
        if c not in raw_df.columns:
            raise ValueError(f"값 컬럼 없음: {c}")

    df = raw_df.copy()
    # 값 컬럼 numeric 변환
    for c in value_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    jurir_no_map = jurir_no_map or {}

    # 1) 회사_all 집계
    all_rows = (
        df.groupby([*group_dim_cols, company_col], dropna=False)[value_cols]
          .sum().reset_index()
    )
    all_rows["_kind"]       = "all"
    all_rows["_brand_label"] = all_label

    # 2) 브랜드별 집계 (brand_col 있을 때)
    brand_rows = pd.DataFrame()
    if brand_col and brand_col in df.columns:
        brand_rows = (
            df.groupby([*group_dim_cols, company_col, brand_col], dropna=False)[value_cols]
              .sum().reset_index()
        )
        # 회사별 brand 합계 (top N 선정)
        brand_total = (
            brand_rows.groupby([company_col, brand_col])[value_cols[0] if value_cols else brand_col]
                       .sum().reset_index()
                       .sort_values([company_col, value_cols[0] if value_cols else brand_col],
                                    ascending=[True, False])
        )
        # brand_keep_per_company override
        keep_set: dict[str, set] = {}
        for comp, sub in brand_total.groupby(company_col):
            if brand_keep_per_company and comp in brand_keep_per_company:
                keep_set[comp] = set(brand_keep_per_company[comp])
            else:
                keep_set[comp] = set(sub.head(keep_top_brands_per_company)[brand_col].tolist())

        # 명시 브랜드만 유지, 나머지는 _기타로 합산
        is_kept = brand_rows.apply(
            lambda r: r[brand_col] in keep_set.get(r[company_col], set()), axis=1,
        ) if not brand_rows.empty else pd.Series([], dtype=bool)
        kept_rows  = brand_rows[is_kept].copy()
        other_rows = brand_rows[~is_kept].copy()

        kept_rows["_kind"]       = "brand"
        kept_rows["_brand_label"] = kept_rows[brand_col]
        kept_rows = kept_rows.drop(columns=[brand_col])

        if include_other_row and not other_rows.empty:
            other_agg = (
                other_rows.groupby([*group_dim_cols, company_col], dropna=False)[value_cols]
                          .sum().reset_index()
            )
            other_agg["_kind"]        = "other"
            other_agg["_brand_label"] = other_label
            brand_rows_final = pd.concat([kept_rows, other_agg], ignore_index=True)
        else:
            brand_rows_final = kept_rows
    else:
        brand_rows_final = pd.DataFrame()

    # 3) 결합 + 회사·종류별 순번
    pieces = []
    if include_all_row:
        pieces.append(all_rows)
    if not brand_rows_final.empty:
        pieces.append(brand_rows_final)
    if not pieces:
        return pd.DataFrame(columns=[*group_dim_cols, "mandata_brand_name",
                                      "mandata_brand_code", *value_cols])
    combined = pd.concat(pieces, ignore_index=True)

    # mandata_brand_name
    combined["mandata_brand_name"] = combined.apply(
        lambda r: _brand_name(str(r[company_col]), str(r["_brand_label"])), axis=1,
    )

    # mandata_brand_code = jurir_no + 회사·종류 순서 (회사 1=all → 2=brand1 → ... → N=기타)
    # 회사 내 정렬: kind 'all' → 'brand' (이름순) → 'other'
    _KIND_ORDER = {"all": 0, "brand": 1, "other": 9}
    combined["_kind_ord"] = combined["_kind"].map(_KIND_ORDER).fillna(5)
    combined = combined.sort_values(
        by=[company_col, "_kind_ord", "_brand_label", *group_dim_cols],
        kind="mergesort",
    ).reset_index(drop=True)

    # 회사·종류 단위 유니크 라벨 → 순번
    # 같은 (company, brand_label) 은 같은 code (날짜·지역 등 dim 만 다름)
    code_map: dict[tuple[str, str], int] = {}
    seq_by_company: dict[str, int] = {}
    codes = []
    for _, r in combined.iterrows():
        comp = str(r[company_col])
        lbl  = str(r["_brand_label"])
        key  = (comp, lbl)
        if key not in code_map:
            seq_by_company[comp] = seq_by_company.get(comp, 0) + 1
            code_map[key] = seq_by_company[comp]
        seq = code_map[key]
        jurir = str(jurir_no_map.get(comp, "")).strip()
        codes.append(f"{jurir}{seq:03d}" if jurir else f"{seq:03d}")
    combined["mandata_brand_code"] = codes

    # 4) 최종 컬럼 정렬
    out_cols = [*group_dim_cols, "mandata_brand_name", "mandata_brand_code", *value_cols]
    out = combined[out_cols].reset_index(drop=True)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 직렬화 (공용 헬퍼)
# ══════════════════════════════════════════════════════════════════════════════

def df_to_xlsx_bytes(df: pd.DataFrame, sheet: str = "data",
                     max_rows: int = 1_048_575) -> tuple[bytes, bool]:
    truncated = False
    safe = df
    if safe is None or safe.empty:
        safe = pd.DataFrame({"info": ["(no data)"]})
    elif len(safe) > max_rows:
        safe = safe.head(max_rows); truncated = True
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        safe.to_excel(w, index=False, sheet_name=sheet)
        ws = w.book[sheet]; ws.sheet_state = "visible"
    return buf.getvalue(), truncated


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    if df is None or df.empty:
        df = pd.DataFrame({"info": ["(no data)"]})
    return df.to_csv(index=False).encode("utf-8-sig")
