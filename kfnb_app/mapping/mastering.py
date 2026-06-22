"""
kfnb_app/mastering.py — 글로벌 배포용 Product/Brand Master(딕셔너리) 생성.

번역이 아니라 '마스터링'. 한글 원본은 보존하고, 공식 영문 표준명·로마자·alias·
언어무관 안정 ID·confidence/status 를 부여한다.

산출:
  - enrich_sku_master(df)      : SKU 마스터 (brand_id, sku_id, *_ko/_en/_romanized, confidence, status)
  - build_brand_master(df)     : 브랜드 마스터 (id, ko/en, aliases, status)
  - build_company_master(df)   : 회사 마스터 (ISIN, ticker, 공식 영문명)
  - build_category_master(df)  : 카테고리 영문 체계
  - build_mapping_quality(df)  : 매핑 품질(자동/수동, confidence) 요약
streamlit 비의존, 순수 pandas.
"""
from __future__ import annotations

import re

import pandas as pd

from kfnb_app import config
from kfnb_app.utils.romanization import romanize

_HANGUL_RE = re.compile(r"[가-힣]")
_HANGUL_RUN = re.compile(r"[가-힣]+")


def _no_hangul(s: str) -> str:
    """문자열에 남은 한글 덩어리만 로마자로 치환 (영문/숫자/기호는 보존)."""
    return _HANGUL_RUN.sub(lambda m: romanize(m.group()), str(s))
_PACK_RE = re.compile(r"(\d+)\s*입")


def _has_hangul(s: str) -> bool:
    return bool(_HANGUL_RE.search(str(s)))


# ──────────────────────────────────────────────────────────────────────────
# 2) variant(한글) → 영문 (규칙 기반 토큰 치환)
# ──────────────────────────────────────────────────────────────────────────
def variant_to_en(variant_ko: str) -> tuple[str, bool]:
    """variant 한글 → (영문, fully_known). 미해석 한글 잔존 시 fully_known=False."""
    s = str(variant_ko or "").strip()
    if not s:
        return "", True
    # 멀티팩 토큰은 SKU naming 에서 따로 처리하므로 제거
    s = _PACK_RE.sub("", s)
    parts: list[str] = []
    # 긴 토큰부터 치환
    for ko, en in config.SKU_TOKEN_EN.items():
        if ko in s:
            parts.append(en)
            s = s.replace(ko, " ")
    leftover = s.strip()
    fully = not _has_hangul(leftover)
    if leftover and _has_hangul(leftover):
        parts.append(romanize(leftover))   # 미해석 한글은 로마자로
    elif leftover:
        parts.append(leftover)
    # 중복 제거하며 순서 유지
    seen, ordered = set(), []
    for p in parts:
        p = p.strip()
        if p and p not in seen:
            seen.add(p); ordered.append(p)
    return " ".join(ordered), fully


# ──────────────────────────────────────────────────────────────────────────
# 3) 브랜드 해석 — 마스터 우선, 없으면 로마자 폴백
# ──────────────────────────────────────────────────────────────────────────
def resolve_brand(company_kr: str, brand_kr: str, company_slug: str = "") -> dict:
    """(회사,브랜드) → {brand_id, brand_en, aliases, curated}."""
    key = (str(company_kr), str(brand_kr))
    m = config.BRAND_MASTER.get(key)
    if m:
        return {"brand_id": m["id"], "brand_en": m["en"],
                "aliases": list(m["aliases"]) + [str(brand_kr)], "curated": True}
    # 폴백: 로마자 + 회사 slug 기반 ID
    rom = romanize(brand_kr)
    slug = (company_slug or "UNKNOWN") + "_" + re.sub(r"[^A-Za-z0-9]+", "_",
                                                      rom).upper().strip("_")
    return {"brand_id": slug, "brand_en": rom or str(brand_kr),
            "aliases": [str(brand_kr)], "curated": False}


# ──────────────────────────────────────────────────────────────────────────
# 4) SKU 마스터 — standardized EN name + 안정 ID + confidence/status
# ──────────────────────────────────────────────────────────────────────────
_PKG_CODE = {"Bag": "BAG", "Cup/Bowl": "CUP", "Can": "CAN", "Bottle": "BTL",
             "PET": "PET", "Pack": "PCK", "Unknown": "NA"}


# 용기 세부 형태 — 소컵/큰사발 등이 같은 ID 로 뭉치지 않도록 세분화
_FINE_PKG = [("큰사발", "BIGBOWL"), ("큰컵", "BIGCUP"), ("소컵", "SMALLCUP"),
             ("사발", "BOWL"), ("큰", "BIG")]


def _pkg_code(sku_name_kr: str, package_format: str) -> str:
    nm = str(sku_name_kr)
    for ko, code in _FINE_PKG:
        if ko in nm:
            return code
    return _PKG_CODE.get(package_format, "NA")


def _sku_id(brand_id: str, variant_en: str, sku_name_kr: str,
            package_format: str, pack: int) -> str:
    """언어무관 안정 SKU ID. (브랜드ID + 영문 variant slug + 세부포장 + 팩)."""
    vslug = re.sub(r"[^A-Za-z0-9]+", "_", variant_en).upper().strip("_")
    pkg = _pkg_code(sku_name_kr, package_format)
    parts = [brand_id]
    if vslug:
        parts.append(vslug)
    parts.append(pkg)
    if pack and int(pack) > 1:
        parts.append(f"{int(pack)}P")
    return "_".join(parts)


def enrich_sku_master(sku_df: pd.DataFrame) -> pd.DataFrame:
    """정규화·매핑된 SKU 테이블 → 마스터 컬럼 부착.

    필요한 입력 컬럼: company_kr, brand_kr, sku_name_kr, variant,
                     package_format, pack_count, barcode, company_slug
    """
    df = sku_df.copy()
    brand_ids, brand_ens, aliases_l, curated_l = [], [], [], []
    sku_ids, sku_ens, sku_roms = [], [], []
    confs, statuses = [], []

    for _, r in df.iterrows():
        slug = str(r.get("company_slug", "") or "")
        br = resolve_brand(r.get("company_kr"), r.get("brand_kr"), slug)
        brand_ids.append(br["brand_id"])
        brand_ens.append(br["brand_en"])
        aliases_l.append(", ".join(br["aliases"]))
        curated_l.append(br["curated"])

        var_en, var_full = variant_to_en(r.get("variant", ""))
        # 브랜드명과 중복되는 단어 제거 (예: 까르보불닭 → 'Carbonara' (Buldak 중복 제거))
        brand_words = {w.lower() for w in br["brand_en"].split()}
        var_en = " ".join(w for w in var_en.split() if w.lower() not in brand_words)
        pack = int(r.get("pack_count", 1) or 1)
        pkg = str(r.get("package_format", ""))
        size = ""
        if str(r.get("size_value", "")):
            size = f"{r.get('size_value')}{r.get('size_unit', '')}"
        # SKU 영문명 = 브랜드EN + variant + 포장 + 용량 + 팩
        name_parts = [br["brand_en"]]
        if var_en:
            name_parts.append(var_en)
        if pkg and pkg != "Unknown":
            pkg_label = "Cup" if pkg == "Cup/Bowl" else pkg
            if pkg_label.lower() not in var_en.lower():
                name_parts.append(pkg_label)
        if size:
            name_parts.append(size)
        if pack > 1:
            name_parts.append(f"{pack}-Pack")
        sku_en = _no_hangul(" ".join(p for p in name_parts if p).strip())
        sku_ens.append(sku_en)
        sku_roms.append(romanize(re.sub(r"^[^)]*\)", "", str(r.get("sku_name_kr", "")))))

        sku_ids.append(_sku_id(br["brand_id"], var_en,
                               str(r.get("sku_name_kr", "")), pkg, pack))

        # confidence/status: 브랜드 큐레이션 + variant 완전 해석 여부
        if br["curated"] and var_full:
            conf, stat = "high", "verified"
        elif br["curated"]:
            conf, stat = "medium", "auto_mapped"
        else:
            conf, stat = "low", "needs_review"
        confs.append(conf); statuses.append(stat)

    # SKU 안정 키 = 바코드(GTIN). 슬라이스/추출에 무관하게 고유·불변 (감사 D1).
    # 읽기 좋은 의미형 라벨은 sku_slug 로 별도 제공 (키로 쓰지 않음).
    df["brand_id"] = brand_ids
    df["brand_name_ko"] = df["brand_kr"]
    df["brand_name_en"] = brand_ens
    df["brand_aliases"] = aliases_l
    df["sku_id"] = df["barcode"].astype(str)
    df["sku_slug"] = sku_ids
    df["sku_name_en"] = sku_ens
    df["sku_name_romanized"] = sku_roms
    df["mapping_confidence"] = confs
    df["mapping_status"] = statuses
    # 카테고리 영문화 + 투자테마 태그
    def _cat_en(x):
        x = str(x)
        return config.CATEGORY_EN.get(x, romanize(x))
    for k in ("cat_l1", "cat_l2", "cat_l3"):
        if k in df.columns:
            df[f"{k}_en"] = df[k].map(_cat_en)
    df["investment_theme_tag"] = [
        config.INVESTMENT_THEME_TAG.get(str(c3),
            config.INVESTMENT_THEME_TAG.get(str(c2), ""))
        for c2, c3 in zip(df.get("cat_l2", ""), df.get("cat_l3", ""))]
    return df


# ──────────────────────────────────────────────────────────────────────────
# 5) 마스터 테이블들 (Dictionary)
# ──────────────────────────────────────────────────────────────────────────
def build_company_master(sku_df: pd.DataFrame) -> pd.DataFrame:
    """sku_df 의 이미-매핑된 컬럼(map_companies/DART overlay 반영)으로 회사 마스터 구성."""
    keep = ["company_kr", "company_en_official", "krx_code", "bbg_ticker",
            "bloomberg_code", "isin", "gics_sub_code", "gics_sub_name",
            "gics_sector", "listed", "map_status"]
    keep = [c for c in keep if c in sku_df.columns]
    g = sku_df[keep].drop_duplicates("company_kr").copy()
    g["company_id"] = g.apply(
        lambda r: r["isin"] if r.get("isin") else
        (config.COMPANY_MAP[r["company_kr"]].slug
         if r["company_kr"] in config.COMPANY_MAP else r["company_kr"]), axis=1)
    g["mapping_status"] = g.get("map_status", "needs_review")
    g = g.rename(columns={"company_kr": "company_name_ko",
                          "company_en_official": "company_name_en"})
    cols = ["company_id", "company_name_ko", "company_name_en", "isin", "krx_code",
            "bbg_ticker", "bloomberg_code", "gics_sub_code", "gics_sub_name",
            "gics_sector", "listed", "mapping_status"]
    for c in cols:
        if c not in g.columns:
            g[c] = ""
    return g[cols].reset_index(drop=True)


def build_brand_master(sku_master: pd.DataFrame) -> pd.DataFrame:
    g = (sku_master.groupby(
            ["brand_id", "company_kr", "brand_name_ko", "brand_name_en",
             "brand_aliases"], dropna=False)
         .agg(skus=("sku_id", "nunique"), sales_amt=("sales_amt", "sum"))
         .reset_index())
    # 브랜드 검수상태 = 큐레이션 여부
    curated_ids = {m["id"] for m in config.BRAND_MASTER.values()}
    g["mapping_status"] = g["brand_id"].map(
        lambda b: "verified" if b in curated_ids else "needs_review")
    g["mapping_confidence"] = g["brand_id"].map(
        lambda b: "high" if b in curated_ids else "low")
    co = {k: v for k, v in zip(
        sku_master["company_kr"], sku_master.get("company_en_official", sku_master["company_kr"]))}
    g["company_name_en"] = g["company_kr"].map(co)
    return g.sort_values("sales_amt", ascending=False).reset_index(drop=True)


# 사용자가 출력에 포함할지 선택하는 분석 컬럼 (key → 실제 컬럼들)
ANALYSIS_COLUMNS: dict[str, list[str]] = {
    "pack_count": ["pack_count"],
    "multipack": ["is_multipack"],
    "package": ["package_format"],
    "size": ["size_value", "size_unit"],
    "theme": ["investment_theme"],
    "spicy": ["tag_spicy"],
    "stir_fried": ["tag_stir_fried"],
    "black_bean": ["tag_black_bean"],
    "premium": ["tag_premium"],
    "imported": ["tag_imported"],
    "non_alcohol": ["tag_non_alcohol"],
    "new_product": ["tag_new"],
    "launch_date": ["first_date"],
    "asp": ["asp_won"],
}
ANALYSIS_LABELS: dict[str, str] = {
    "pack_count": "팩 수(PackCount)", "multipack": "멀티팩 여부",
    "package": "포장형태", "size": "용량(value+unit)", "theme": "투자테마",
    "spicy": "맛: Spicy", "stir_fried": "맛: 볶음면", "black_bean": "맛: 짜장",
    "premium": "프리미엄", "imported": "수입(맥주)", "non_alcohol": "무알콜",
    "new_product": "신제품 여부", "launch_date": "출시일", "asp": "ASP(원)",
}
DEFAULT_ANALYSIS = list(ANALYSIS_COLUMNS)

# 선택 가능한 식별자(증권) 컬럼 — SKU 행에 회사 식별자를 denormalize
ID_COLUMNS: dict[str, list[str]] = {
    "isin": ["isin"],
    "krx_code": ["krx_code"],
    "bbg_ticker": ["bbg_ticker"],
    "bloomberg": ["bloomberg_code"],
    "gics": ["gics_sub_code", "gics_sub_name"],
    "gics_sector": ["gics_sector"],
}
ID_LABELS: dict[str, str] = {
    "isin": "ISIN", "krx_code": "KRX 종목코드", "bbg_ticker": "Bloomberg 티커(004370 KS)",
    "bloomberg": "Bloomberg 코드(…KS Equity)", "gics": "GICS 코드+명",
    "gics_sector": "GICS 섹터",
}
DEFAULT_IDS = ["isin", "bbg_ticker"]


def build_sku_master_file(sku_master: pd.DataFrame,
                          analysis_cols: list[str] | None = None,
                          id_cols: list[str] | None = None) -> pd.DataFrame:
    """SKU 마스터 출력. analysis_cols=분석컬럼, id_cols=식별자컬럼 선택 (None=기본)."""
    df = sku_master.copy()
    if "sku_name_ko" not in df:
        df["sku_name_ko"] = df["sku_name_kr"]
    if "company_en_official" in df:          # 회사명 공시(공식) 기준
        df["company_name_en"] = df["company_en_official"]
    ident = [c for k in (DEFAULT_IDS if id_cols is None else id_cols)
             for c in ID_COLUMNS.get(k, [])]
    base = (["sku_id", "sku_slug", "barcode", "brand_id", "company_kr",
             "company_name_en"]
            + ident
            + ["brand_name_ko", "brand_name_en", "sku_name_ko", "sku_name_en",
               "sku_name_romanized", "cat_l1", "cat_l1_en", "cat_l2", "cat_l2_en",
               "cat_l3", "cat_l3_en", "investment_theme_tag",
               "mapping_confidence", "mapping_status", "sales_amt", "sales_qty"])
    keys = DEFAULT_ANALYSIS if analysis_cols is None else analysis_cols
    extra = [c for k in keys for c in ANALYSIS_COLUMNS.get(k, [])]
    cols, seen = [], set()
    for c in base + extra:
        if c in df.columns and c not in seen:
            seen.add(c)
            cols.append(c)
    return df[cols].sort_values("sales_amt", ascending=False).reset_index(drop=True)


def build_category_master(sku_master: pd.DataFrame) -> pd.DataFrame:
    cat_en = {
        "가공식사제품": "Processed Meal Products", "면류": "Noodles",
        "봉지면": "Bagged Noodles", "용기면": "Cup/Bowl Noodles",
        "주류": "Alcoholic Beverages", "맥주": "Beer", "소주": "Soju",
        "온라인주류": "Online Alcohol",
    }
    g = (sku_master.groupby(["cat_l1", "cat_l2", "cat_l3"], dropna=False)
         .agg(skus=("sku_id", "nunique")).reset_index())
    g["cat_l1_en"] = g["cat_l1"].map(lambda x: cat_en.get(str(x), romanize(x)))
    g["cat_l2_en"] = g["cat_l2"].map(lambda x: cat_en.get(str(x), romanize(x)))
    g["cat_l3_en"] = g["cat_l3"].map(lambda x: cat_en.get(str(x), romanize(x)))
    g["category_path_en"] = (g["cat_l1_en"] + " > " + g["cat_l2_en"]
                             + " > " + g["cat_l3_en"])
    return g


def build_mapping_quality(sku_master: pd.DataFrame) -> pd.DataFrame:
    total_amt = pd.to_numeric(sku_master["sales_amt"], errors="coerce").sum()
    rows = []
    for (conf, stat), grp in sku_master.groupby(["mapping_confidence", "mapping_status"]):
        amt = pd.to_numeric(grp["sales_amt"], errors="coerce").sum()
        rows.append({
            "mapping_confidence": conf, "mapping_status": stat,
            "skus": grp["sku_id"].nunique(),
            "sales_amt": amt,
            "sales_pct": round(amt / total_amt * 100, 1) if total_amt else 0.0,
        })
    return pd.DataFrame(rows).sort_values("sales_amt", ascending=False).reset_index(drop=True)


def mastering_summary(sku_master: pd.DataFrame) -> dict:
    """검증 게이트용 요약."""
    total = pd.to_numeric(sku_master["sales_amt"], errors="coerce").sum()
    verified_amt = pd.to_numeric(
        sku_master.loc[sku_master["mapping_status"] == "verified", "sales_amt"],
        errors="coerce").sum()
    need = sku_master[sku_master["mapping_status"] == "needs_review"]
    return {
        "n_skus": len(sku_master),
        "verified_amt_pct": (verified_amt / total * 100) if total else 0.0,
        "needs_review_skus": int(need["sku_id"].nunique()),
        "needs_review_brands": sorted(set(need["brand_name_ko"].astype(str)))[:20],
    }
