"""
kfnb_app/config.py — 설정 로더 (single source of truth).

모든 규칙·마스터를 configs/ 의 YAML/CSV 에서 읽어 메모리 구조로 빌드한다.
비개발자도 configs/*.yaml, configs/master/*.csv 만 고치면 파이프라인 전체에
반영된다. (기존 코드 호환을 위해 동일한 심볼명을 그대로 노출한다.)

externalized files:
  configs/owner_schema_mapping.yaml   오너별 스키마 → 표준 컬럼
  configs/tagging_rules.yaml          투자 테마 태깅 키워드
  configs/sku_translation_rules.yaml  SKU 영문 토큰 + 포장형태
  configs/category_mapping.yaml       카테고리 영문 + investment_theme_tag
  configs/quality_thresholds.yaml     검증/QC 임계값
  configs/master/company_master.csv   회사 → ISIN/티커/공식 영문명
  configs/master/brand_master.csv     브랜드 → brand_id/영문표준명/alias
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

try:                                   # PyYAML 있으면 사용
    import yaml
    def _yaml_parse(text: str):
        return yaml.safe_load(text)
except ImportError:                    # 없으면 의존성 0 폴백 파서
    from kfnb_app.utils.miniyaml import safe_load as _yaml_parse

CONFIG_DIR = Path(__file__).resolve().parent / "configs"
MASTER_DIR = CONFIG_DIR / "master"


def _load_yaml(name: str) -> dict:
    with open(CONFIG_DIR / name, encoding="utf-8") as f:
        return _yaml_parse(f.read()) or {}


def _load_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# ──────────────────────────────────────────────────────────────────────────
# 1) 오너 스키마 → 표준 컬럼
# ──────────────────────────────────────────────────────────────────────────
_schema = _load_yaml("owner_schema_mapping.yaml")
OWNER_SCHEMAS: dict[str, dict] = _schema.get("owners", {})
DEFAULT_OWNER: str = _schema.get("default_owner", "cu")
REQUIRED_CANON: list[str] = list(_schema.get("required", []))
RECOMMENDED_CANON: list[str] = list(_schema.get("recommended", []))

_default_schema = OWNER_SCHEMAS.get(DEFAULT_OWNER, {})
# canonical 컬럼 목록 (모든 오너에서 등장 순서 보존)
CANONICAL_COLS: list[str] = list(dict.fromkeys(
    c for sch in OWNER_SCHEMAS.values() for c in sch))

# 기존 코드 호환: native(대표) → canonical, synonym → native
RAW_COLUMNS: dict[str, str] = {}
COLUMN_ALIASES: dict[str, str] = {}
for canon, headers in _default_schema.items():
    if not headers:
        continue
    native = str(headers[0])
    RAW_COLUMNS[native] = canon
    for syn in headers[1:]:
        COLUMN_ALIASES[str(syn)] = native
# 필수 원천(native) — validation 호환
REQUIRED_RAW: list[str] = [
    str(_default_schema[c][0]) for c in REQUIRED_CANON
    if c in _default_schema and _default_schema[c]]


def rename_map(columns: list[str], owner: str | None = None) -> dict[str, str]:
    """원천 헤더 목록 → {원천명: canonical}. 지정 오너(없으면 default) 스키마 사용."""
    sch = OWNER_SCHEMAS.get(owner or DEFAULT_OWNER, _default_schema)
    lut: dict[str, str] = {}
    for canon, headers in sch.items():
        for h in headers or []:
            lut[str(h).strip().lstrip("﻿")] = canon
    out: dict[str, str] = {}
    for c in columns:
        key = str(c).strip().lstrip("﻿")
        if key in lut:
            out[c] = lut[key]
    return out


# ──────────────────────────────────────────────────────────────────────────
# 2) 회사 마스터 → COMPANY_MAP
# ──────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class CompanyRef:
    company_en: str
    krx_code: str
    listed: bool
    slug: str = ""
    company_en_official: str = ""
    gics_sub_code: str = ""
    gics_sub_name: str = ""
    gics_sector: str = ""
    note: str = ""

    @property
    def bbg_ticker(self) -> str:
        return f"{self.krx_code} KS" if self.krx_code else ""

    @property
    def bloomberg_code(self) -> str:
        """Bloomberg 전체 표기 (예: '004370 KS Equity')."""
        return f"{self.krx_code} KS Equity" if self.krx_code else ""

    @property
    def isin(self) -> str:
        return _krx_isin(self.krx_code) if self.krx_code else ""


def _to_bool(v) -> bool:
    return str(v).strip().lower() in ("true", "1", "y", "yes")


COMPANY_MAP: dict[str, CompanyRef] = {}
for r in _load_csv(MASTER_DIR / "company_master.csv"):
    COMPANY_MAP[r["company_kr"]] = CompanyRef(
        company_en=r.get("company_en", ""),
        krx_code=(r.get("krx_code") or "").strip(),
        listed=_to_bool(r.get("listed")),
        slug=r.get("slug", ""),
        company_en_official=r.get("company_en_official", ""),
        gics_sub_code=r.get("gics_sub_code", ""),
        gics_sub_name=r.get("gics_sub_name", ""),
        gics_sector=r.get("gics_sector", ""),
        note=r.get("note", ""))

# ──────────────────────────────────────────────────────────────────────────
# 3) 브랜드 마스터 → BRAND_MASTER {(회사,브랜드): {id,en,aliases}}
# ──────────────────────────────────────────────────────────────────────────
BRAND_MASTER: dict[tuple, dict] = {}
for r in _load_csv(MASTER_DIR / "brand_master.csv"):
    aliases = [a for a in (r.get("aliases") or "").split("|") if a]
    BRAND_MASTER[(r["company_kr"], r["brand_kr"])] = {
        "id": r["brand_id"], "en": r["brand_en"], "aliases": aliases}

# ──────────────────────────────────────────────────────────────────────────
# 4) 태깅 / SKU 토큰 / 포장 / 카테고리
# ──────────────────────────────────────────────────────────────────────────
THEME_RULES: dict[str, dict[str, list[str]]] = _load_yaml("tagging_rules.yaml")

_sku_rules = _load_yaml("sku_translation_rules.yaml")
SKU_TOKEN_EN: dict[str, str] = _sku_rules.get("sku_token_en", {})
PACKAGE_FORMAT_MAP: dict[str, str] = _sku_rules.get("package_format_map", {})
PACKAGE_NAME_TOKENS: dict[str, str] = _sku_rules.get("package_name_tokens", {})

_cat = _load_yaml("category_mapping.yaml")
CATEGORY_EN: dict[str, str] = _cat.get("category_en", {})
INVESTMENT_THEME_TAG: dict[str, str] = _cat.get("investment_theme_tag", {})
REGION_EN: dict[str, str] = _cat.get("region_en", {})

# ──────────────────────────────────────────────────────────────────────────
# 5) 임계값
# ──────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Thresholds:
    min_rows: int = 100
    asp_min_won: float = 100.0
    asp_max_won: float = 100_000.0
    barcode_len: int = 13
    map_coverage_warn: float = 0.95
    nonpos_amt_warn_pct: float = 1.0
    spike_ratio: float = 3.0
    pos_release_lag_days: int = 15


_thr = _load_yaml("quality_thresholds.yaml")
THRESHOLDS = Thresholds(**{k: v for k, v in (_thr.get("thresholds") or {}).items()
                          if k in Thresholds.__dataclass_fields__})
HALT_SEVERITIES = set(_thr.get("halt_severities", ["error", "critical"]))

# ──────────────────────────────────────────────────────────────────────────
# 데이터 출처 명세(Data Spec) — 업로드마다 달라지므로 per-run 으로 받는다.
# 결론/적합성 판단의 근거. 모르면 'unknown' 으로 두고 결론에 그대로 노출한다.
# ──────────────────────────────────────────────────────────────────────────
@dataclass
class DataSpec:
    amount_basis: str = "unknown"      # vat_incl_retail|vat_excl|discounted_net|unknown
    qty_basis: str = "unknown"         # selling_unit|each|unknown
    currency: str = "KRW"
    channel_scope: str = ""
    population: str = "unknown"        # census|sample|multi_channel|unknown
    release_cadence: str = "unknown"   # monthly|weekly|daily|unknown
    release_lag_days: int = 15
    restatement: str = "unknown"       # none|revised|unknown
    notes: str = ""

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def load_data_spec() -> DataSpec:
    d = _load_yaml("data_spec.yaml")
    fields = DataSpec.__dataclass_fields__
    return DataSpec(**{k: v for k, v in d.items() if k in fields})


DATA_SPEC_DEFAULT = load_data_spec()

# 고객 유형별 상품 모듈
CUSTOMER_PRODUCTS: dict[str, str] = {
    "quant":       "PIT 시계열 패널 + 티커 매핑 (백테스트용)",
    "fundamental": "브랜드·SKU 트래커 + ASP (종목 리서치용)",
    "vendor":      "정규화 raw feed + 데이터 사전 (재판매용)",
}


# ──────────────────────────────────────────────────────────────────────────
# ISIN check-digit (Luhn mod 10, ISO 6166) — 순수 함수
# ──────────────────────────────────────────────────────────────────────────
def _krx_isin(stock_code: str) -> str:
    """KR7 + 6자리코드 + '00' (보통주) + check-digit → 12자리 ISIN."""
    body = f"KR7{stock_code}00"
    return body + str(_isin_check_digit(body))


def _isin_check_digit(body11: str) -> int:
    digits: list[int] = []
    for ch in body11:
        if ch.isdigit():
            digits.append(int(ch))
        else:
            v = ord(ch.upper()) - 55
            digits.extend([v // 10, v % 10])
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return (10 - (total % 10)) % 10
