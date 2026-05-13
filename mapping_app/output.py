"""
mapping_app/output.py — 표준 레이아웃 DataFrame 생성 + XLSX/CSV 직렬화.

⑦ 데이터 검증·⑧ 변환·다운로드 step 에서 사용.
streamlit 비의존 (순수 pandas) — 단위 테스트 가능.
"""
from __future__ import annotations

import io

import pandas as pd

from modules.mapping.sources import (
    VIRTUAL_SOURCES,
    VSRC_KRX_ISIN, VSRC_KRX_STK, VSRC_KRX_MKT,
    VSRC_DART_JR, VSRC_DART_BZ, VSRC_DART_EN, VSRC_DART_KR, VSRC_DART_CC,
    VSRC_NAME_EN_FALLBACK,
    VSRC_BRAND_EN, VSRC_SKU_EN, VSRC_CATEGORY_EN,
    is_translate_source, extract_translate_col,
)
from modules.mapping import translation_db as _trans_db


# ── 상수 ─────────────────────────────────────────────────────────────────────
XLSX_MAX_ROWS = 1_048_576       # Excel 한 시트 최대 행 수 (헤더 1 + 데이터)


# ── 회사명 raw 컬럼 찾기 (출력 함수가 직접 사용) ─────────────────────────────
def _raw_col_for_kind(
    target_kind: str,
    std_to_raw: dict[str, str],
    std_cols: list[str],
    std_kinds: list[str],
    raw_df: pd.DataFrame,
) -> str | None:
    raw_columns = list(raw_df.columns)
    for std, kind in zip(std_cols, std_kinds):
        if kind == target_kind:
            src = std_to_raw.get(std)
            if src in raw_columns:
                return src
    from modules.mapping.column_mapper import infer_column_kind
    for c in raw_columns:
        if infer_column_kind(c) == target_kind:
            return c
    return None


def _company_raw_col(
    std_to_raw: dict[str, str],
    std_cols: list[str],
    std_kinds: list[str],
    raw_df: pd.DataFrame,
    override: str | None = None,
) -> str | None:
    """회사명 raw 컬럼 — override 우선, 없으면 company → brand 추론."""
    raw_columns = list(raw_df.columns)
    if override and override in raw_columns:
        return override
    return (_raw_col_for_kind("company", std_to_raw, std_cols, std_kinds, raw_df)
            or _raw_col_for_kind("brand", std_to_raw, std_cols, std_kinds, raw_df))


# ══════════════════════════════════════════════════════════════════════════════
# 표준 레이아웃 DataFrame 빌드
# ══════════════════════════════════════════════════════════════════════════════

def build_output_df(
    raw_df: pd.DataFrame,
    std_cols: list[str],
    std_kinds: list[str],
    std_to_raw: dict[str, str],
    extra_cols: list[str],
    isin_match: pd.DataFrame | None = None,
    dart_match: pd.DataFrame | None = None,
    dart_company_info: dict | None = None,
    company_override: str | None = None,
    brand_key_col: str | None = None,
    sku_key_col: str | None = None,
    category_key_col: str | None = None,
) -> pd.DataFrame:
    """
    표준 레이아웃 형식의 DataFrame.

    각 표준 컬럼의 소스는 std_to_raw[std] 값으로 결정:
      - raw 컬럼명 → raw_df[col]
      - VSRC_* (가상 소스) → KRX/DART/SQLite 영문화 결과
    매핑 안 된 표준 컬럼은 빈 문자열. 보조 컬럼(__) 자동 추가 없음.
    """
    out = pd.DataFrame()
    comp_col = _company_raw_col(std_to_raw, std_cols, std_kinds, raw_df,
                                override=company_override)

    # ── 사전 1회 빌드 — 행마다 .get() 대신 Series.map(dict) 로 벡터화 ───────
    isin_lookup: dict = {}
    if isin_match is not None and "input_name" in isin_match.columns:
        isin_lookup = isin_match.set_index("input_name")[
            ["isin", "stock_code", "matched_name", "market"]
        ].to_dict(orient="index")

    dart_lookup: dict = {}
    if dart_match is not None and "input_name" in dart_match.columns:
        dart_lookup = dart_match.set_index("input_name")[
            ["corp_code", "corp_name", "corp_name_eng"]
        ].to_dict(orient="index")
    info_map = dart_company_info or {}

    # 회사명 → 단일 값 dict 들 (회사명 키 가상 소스용)
    isin_map = {k: v.get("isin", "")        for k, v in isin_lookup.items()}
    stk_map  = {k: v.get("stock_code", "")  for k, v in isin_lookup.items()}
    mkt_map  = {k: v.get("market", "")      for k, v in isin_lookup.items()}
    cc_map   = {k: v.get("corp_code", "")   for k, v in dart_lookup.items()}
    nkr_map  = {k: v.get("corp_name", "")   for k, v in dart_lookup.items()}
    neng_map = {k: v.get("corp_name_eng", "") for k, v in dart_lookup.items()}
    # 법인등록·사업자번호 — corp_code 한 단계 더 거침
    jr_map = {
        k: (info_map.get(v.get("corp_code", "")) or {}).get("jurir_no", "")
        for k, v in dart_lookup.items()
    }
    bz_map = {
        k: (info_map.get(v.get("corp_code", "")) or {}).get("bizr_no", "")
        for k, v in dart_lookup.items()
    }

    comp_series = (
        raw_df[comp_col].astype(str).str.strip()
        if comp_col is not None else None
    )

    # brand·sku·category 가상 소스는 각 raw 컬럼을 키로 사용
    # ⑤ 에서 사용자가 명시 지정한 컬럼(override)이 있으면 그것 우선, 없으면 자동 추론.
    def _resolve_key(override: str | None, kind: str) -> str | None:
        if override and override in raw_df.columns:
            return override
        return _raw_col_for_kind(kind, std_to_raw, std_cols, std_kinds, raw_df)
    brand_raw_col    = _resolve_key(brand_key_col,    "brand")
    sku_raw_col      = _resolve_key(sku_key_col,      "sku")
    category_raw_col = _resolve_key(category_key_col, "category")

    # ── 어떤 가상 소스가 실제 쓰이는지 먼저 스캔 → 필요한 SQLite 사전만 빌드 ─
    used_srcs = {std_to_raw.get(s) for s in std_cols}
    used_vsrcs = {s for s in used_srcs if s in VIRTUAL_SOURCES}

    # 동적 번역 가상 소스 ([번역::col]) — 사용자가 ⑤ 에서 자유 영문화한 raw 컬럼
    # 각각의 raw 컬럼에 대해 category 테이블에서 영문 매핑을 가져온다.
    free_trans_cols = [
        extract_translate_col(s) for s in used_srcs if is_translate_source(s)
    ]

    # 브랜드/제품/카테고리 영문 — raw 컬럼의 unique 값만 1회 배치 lookup
    def _build_trans_map_for_col(raw_col: str | None, entity_type: str):
        if not raw_col or raw_col not in raw_df.columns:
            return None
        keys = (
            raw_df[raw_col]
            .dropna().astype(str).str.strip()
            .replace("", pd.NA).dropna()
            .unique().tolist()
        )
        if not keys:
            return {}
        return _trans_db.get_confirmed_en_many(entity_type, keys)

    brand_en_map    = (_build_trans_map_for_col(brand_raw_col,    "brand")
                       if VSRC_BRAND_EN    in used_vsrcs else None)
    sku_en_map      = (_build_trans_map_for_col(sku_raw_col,      "product")
                       if VSRC_SKU_EN      in used_vsrcs else None)
    category_en_map = (_build_trans_map_for_col(category_raw_col, "category")
                       if VSRC_CATEGORY_EN in used_vsrcs else None)

    # 자유 번역 컬럼별 사전 (category 테이블 공용)
    free_trans_maps: dict[str, dict] = {}
    for col in free_trans_cols:
        m = _build_trans_map_for_col(col, "category")
        if m is not None:
            free_trans_maps[col] = m

    # ── 가상 소스 → comp_series 기반 dict (회사명 키) ──────────────────────
    _COMP_VSRC_MAPS: dict[str, dict] = {
        VSRC_KRX_ISIN: isin_map,
        VSRC_KRX_STK:  stk_map,
        VSRC_KRX_MKT:  mkt_map,
        VSRC_DART_CC:  cc_map,
        VSRC_DART_KR:  nkr_map,
        VSRC_DART_EN:  neng_map,
        VSRC_DART_JR:  jr_map,
        VSRC_DART_BZ:  bz_map,
    }

    for std in std_cols:
        src = std_to_raw.get(std)
        if src is None:
            out[std] = ""
            continue

        # 1) raw 컬럼 그대로
        if src in raw_df.columns:
            out[std] = raw_df[src].values
            continue

        # 동적 번역 가상 소스 [번역::col]
        if is_translate_source(src):
            col = extract_translate_col(src)
            m   = free_trans_maps.get(col) if col else None
            if col and col in raw_df.columns and m is not None:
                keys = raw_df[col].astype(str).str.strip()
                out[std] = keys.map(m).fillna(keys).values
            else:
                out[std] = ""
            continue

        if src not in VIRTUAL_SOURCES:
            out[std] = ""
            continue

        # 2) brand/sku/category 영문 — key_series.map(dict).fillna(원본)
        if src == VSRC_BRAND_EN and brand_raw_col is not None and brand_en_map is not None:
            keys = raw_df[brand_raw_col].astype(str).str.strip()
            out[std] = keys.map(brand_en_map).fillna(keys).values
            continue
        if src == VSRC_SKU_EN and sku_raw_col is not None and sku_en_map is not None:
            keys = raw_df[sku_raw_col].astype(str).str.strip()
            out[std] = keys.map(sku_en_map).fillna(keys).values
            continue
        if src == VSRC_CATEGORY_EN and category_raw_col is not None and category_en_map is not None:
            keys = raw_df[category_raw_col].astype(str).str.strip()
            out[std] = keys.map(category_en_map).fillna(keys).values
            continue

        # 3) 회사명 키 가상 소스 (KRX/DART/회사명 영문 폴백)
        if comp_series is None:
            out[std] = ""
            continue
        if src in _COMP_VSRC_MAPS:
            out[std] = comp_series.map(_COMP_VSRC_MAPS[src]).fillna("").values
            continue
        if src == VSRC_NAME_EN_FALLBACK:
            # DART 영문명 있으면 영문, 없으면 한글 raw
            out[std] = comp_series.map(neng_map).fillna(comp_series).replace("", pd.NA).fillna(comp_series).values
            continue

        out[std] = ""

    for col in extra_cols:
        if col in raw_df.columns:
            out[col] = raw_df[col].values

    return out


# ══════════════════════════════════════════════════════════════════════════════
# 직렬화 — XLSX / CSV
# ══════════════════════════════════════════════════════════════════════════════

def df_to_xlsx_bytes(df: pd.DataFrame) -> tuple[bytes, bool]:
    """
    DataFrame → xlsx bytes. (bytes, truncated:bool) 반환.

    - 빈 df 라도 최소 시트 한 개는 가지도록 안전 처리
    - 행 수가 한 시트 한도(1,048,576) 초과면 자르고 truncated=True
    """
    safe_df = df
    truncated = False

    if safe_df is None or safe_df.empty:
        safe_df = (
            df.copy() if df is not None and len(df.columns) > 0
            else pd.DataFrame({"info": ["(no data)"]})
        )
    else:
        data_limit = XLSX_MAX_ROWS - 1
        if len(safe_df) > data_limit:
            safe_df = safe_df.head(data_limit)
            truncated = True

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        safe_df.to_excel(writer, index=False, sheet_name="standard")
        ws = writer.book["standard"]
        ws.sheet_state = "visible"
    return buf.getvalue(), truncated


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    if df is None or df.empty:
        df = pd.DataFrame({"info": ["(no data)"]})
    return df.to_csv(index=False).encode("utf-8-sig")
