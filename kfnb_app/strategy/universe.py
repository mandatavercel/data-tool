"""
kfnb_app/strategy/universe.py — F&B 투자 유니버스 정기관리 (MSCI식).

데이터셋을 만들기 *이전* 단계. "어떤 회사들을 글로벌에 논리적으로 설명하며
관리할 것인가"를 정한다. 핵심 산출물:
  ① 회사 유니버스 20개 (하이브리드: 규칙기반 자동 스코어 + 애널리스트 검수)
  ② 회사별 대표 브랜드 5개 (매출 기여도 기준)
  ③ 각 선정의 '사유'(global IR 대응) + 반기(6개월) 리뷰 이력

스코어는 업로드된 회사단위 매출 + 상장여부(config/DART) + 데이터 커버리지 +
섹터 대표성으로 계산한다. 시총/유동성은 있으면(market_cap 주입) 가점, 없으면
graceful 하게 빠진다. 최종 선정은 사람이 검수·오버라이드하고 사유를 남긴다.

streamlit 비의존 — 순수 pandas. 영속화는 CSV(정기관리 저장소).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from kfnb_app import config


# ══════════════════════════════════════════════════════════════════════════════
# 이 모듈은 '데이터를 만들기 위한' 기획 도구다. 업로드된 매출 데이터가 아니라
# 한국 F&B 섹터(상장사·시총·세그먼트)에서 유니버스를 정의하고, 그 결과가
# "이 회사들 데이터를 확보/제작하자"는 타깃 스펙이 된다.
#   - 1차 입력: 유지관리되는 섹터 후보 리스트(configs/master/fnb_sector_candidates.csv)
#   - 외부 보강: 시총(pykrx) · 종목코드/공식영문명(DART)  → 모두 graceful
#   - (선택) 이미 확보한 데이터가 있으면 커버리지 점검에만 보조 사용
# ══════════════════════════════════════════════════════════════════════════════

# ── 기획(섹터·시총 기반) 스코어 가중치 — 업로드 불필요 ─────────────────────────
@dataclass(frozen=True)
class PlanWeights:
    market_cap: float = 0.40       # 시총 규모(투자가능성·글로벌 설명력)
    sector_rep: float = 0.25       # 서브섹터 내 대표성
    segment_priority: float = 0.20 # 고신호 세그먼트(라면·주류 등) 우선도
    listed: float = 0.15           # 상장(투자가능) 게이트

    def as_dict(self) -> dict:
        return {"market_cap": self.market_cap, "sector_rep": self.sector_rep,
                "segment_priority": self.segment_priority, "listed": self.listed}


# 세그먼트별 우선도 — "프로덕트레벨까지 볼 가치"가 큰 카테고리에 가중.
DEFAULT_SEGMENT_PRIORITY: dict[str, float] = {
    "ramen": 1.0, "alcohol": 0.9, "snack": 0.8, "beverage": 0.7,
    "dairy": 0.6, "processed": 0.6, "fresh": 0.5, "bakery": 0.5,
    "health_food": 0.45, "seafood": 0.4, "meat": 0.4, "flour": 0.3,
    "food_service": 0.3, "food_distribution": 0.3, "holding": 0.2,
}


# ── (선택) 데이터 커버리지 점검용 가중치 — 이미 확보한 매출이 있을 때만 ─────────
@dataclass(frozen=True)
class ScoreWeights:
    sales_scale: float = 0.30      # 우리 데이터 내 매출 규모(시장 존재감)
    investability: float = 0.30    # 상장 여부(+시총/유동성 있으면 가점)
    data_coverage: float = 0.20    # 기간 커버리지·관측 충실도
    sector_rep: float = 0.20       # 섹터(서브인더스트리) 대표성

    def as_dict(self) -> dict:
        return {"sales_scale": self.sales_scale, "investability": self.investability,
                "data_coverage": self.data_coverage, "sector_rep": self.sector_rep}


DEFAULT_TARGET_N = 20            # 메인(Professional) 유니버스 크기
DEFAULT_WATCHLIST_N = 10         # 관찰 대기군
DEFAULT_BRANDS_PER_CO = 5        # 회사별 대표 브랜드 수
REVIEW_PERIOD_MONTHS = 6         # 반기 정기 리뷰


def _minmax(s: pd.Series) -> pd.Series:
    """0~1 정규화. 전부 동일하면 0.5."""
    s = pd.to_numeric(s, errors="coerce").fillna(0.0)
    lo, hi = s.min(), s.max()
    if hi <= lo:
        return pd.Series(0.5, index=s.index)
    return (s - lo) / (hi - lo)


# ══════════════════════════════════════════════════════════════════════════════
# 0) 섹터 후보 유니버스 (유지관리 리스트) — 데이터 업로드 불필요
# ══════════════════════════════════════════════════════════════════════════════
def load_candidates(extra_map: dict | None = None) -> pd.DataFrame:
    """한국 F&B 섹터 후보 회사 리스트 로드 + 마스터/DART overlay 보강.

    seed: configs/master/fnb_sector_candidates.csv (company_kr, krx_code, listed,
          segment, sub_sector, note) — 정기적으로 유지관리.
    반환: 후보 1행/회사 (company_kr, krx_code, listed, segment, sub_sector,
          company_en_official, gics_sub_name, gics_sector, mapped).
    """
    cmap = {**config.COMPANY_MAP, **(extra_map or {})}
    rows = config._load_csv(config.MASTER_DIR / "fnb_sector_candidates.csv")
    out = []
    for r in rows:
        co = str(r.get("company_kr", "")).strip()
        if not co:
            continue
        ref = cmap.get(co)
        krx = (r.get("krx_code") or "").strip() or (ref.krx_code if ref else "")
        listed = (str(r.get("listed", "")).lower() in ("true", "1", "y", "yes")) or bool(
            ref and ref.listed) or bool(krx)
        out.append({
            "company_kr": co,
            "krx_code": krx,
            "listed": listed,
            "segment": (r.get("segment") or "").strip(),
            "sub_sector": (r.get("sub_sector") or "").strip(),
            "note_seed": (r.get("note") or "").strip(),
            "company_en_official": (ref.company_en_official if ref else ""),
            "gics_sub_name": (ref.gics_sub_name if ref else ""),
            "gics_sector": (ref.gics_sector if ref else ""),
            "mapped": ref is not None,
        })
    return pd.DataFrame(out)


def score_candidates(
    cand: pd.DataFrame,
    *,
    market_cap: dict | None = None,
    weights: PlanWeights | None = None,
    segment_priority: dict | None = None,
) -> pd.DataFrame:
    """섹터 후보 → 기획 스코어. 업로드 매출 불필요(시총·세그먼트·상장 기반).

    market_cap: {krx_code: 시총(원)} (pykrx). 없으면 상장여부로 graceful 대체.
    반환: 후보 + score 컴포넌트 + composite_score(0~100) + rank.
    """
    w = weights or PlanWeights()
    seg_pri = segment_priority or DEFAULT_SEGMENT_PRIORITY
    df = cand.copy()
    mc = market_cap or {}
    df["market_cap"] = df["krx_code"].map(lambda c: float(mc.get(str(c).zfill(6), 0)) or
                                          float(mc.get(str(c), 0)) or 0.0)
    has_mc = df["market_cap"].sum() > 0

    # 시총 점수: 있으면 log min-max, 없으면 상장 0.5/비상장 0.0 으로 graceful
    if has_mc:
        df["mc_score"] = _minmax(np.log1p(df["market_cap"]))
    else:
        df["mc_score"] = np.where(df["listed"], 0.5, 0.0)

    # 상장 게이트
    df["listed_score"] = np.where(df["listed"], 1.0, 0.0)

    # 세그먼트 우선도
    df["segment_score"] = df["segment"].map(lambda s: seg_pri.get(str(s), 0.3))

    # 서브섹터 대표성: 같은 sub_sector 내 시총(없으면 상장) 순위 → 1/rank
    df["_size"] = df["market_cap"] if has_mc else df["listed"].astype(float)
    df["sector_rep"] = 0.0
    key_col = df["sub_sector"].replace("", np.nan)
    for key, idx in df.groupby(key_col, dropna=True).groups.items():
        sub = df.loc[idx].sort_values("_size", ascending=False)
        for rank, i in enumerate(sub.index, start=1):
            df.at[i, "sector_rep"] = 1.0 / rank
    df.loc[key_col.isna(), "sector_rep"] = 0.3

    df["composite_score"] = (
        w.market_cap * df["mc_score"]
        + w.sector_rep * df["sector_rep"]
        + w.segment_priority * df["segment_score"]
        + w.listed * df["listed_score"]
    ) * 100.0
    df["composite_score"] = df["composite_score"].round(1)

    df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)
    df["rank"] = np.arange(1, len(df) + 1)
    df.drop(columns=["_size"], inplace=True)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 1) (선택) 회사 스코어링 — 이미 확보한 매출로 커버리지 점검할 때만
# ══════════════════════════════════════════════════════════════════════════════
def score_companies(
    company_df: pd.DataFrame,
    *,
    company_col: str = "company_kr",
    amount_col: str = "sales_amt",
    date_col: str | None = "date",
    market_cap: dict | None = None,
    weights: ScoreWeights | None = None,
    extra_map: dict | None = None,
) -> pd.DataFrame:
    """회사단위(또는 명세) 매출 데이터 → 회사별 스코어 테이블.

    company_df 는 행단위 매출이어도 되고 회사단위 집계여도 된다(자동 집계).
    market_cap: {회사명: 시가총액(원/USD)} — 있으면 investability 가점(graceful).
    반환: 회사별 1행, 점수 컴포넌트 + composite_score(0~100) + 메타.
    """
    w = weights or ScoreWeights()
    cmap = {**config.COMPANY_MAP, **(extra_map or {})}
    df = company_df.copy()
    df[company_col] = df[company_col].astype(str)
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)

    # ── 회사단위 집계 ──
    agg = {amount_col: "sum"}
    has_date = bool(date_col) and date_col in df.columns
    if has_date:
        d = pd.to_datetime(df[date_col], errors="coerce")
        df["_ym"] = d.dt.to_period("M").astype(str)
    g = df.groupby(company_col, dropna=True)
    out = g[amount_col].sum().rename("sales_total").reset_index()
    out["row_count"] = g.size().values
    if has_date:
        out["months_present"] = g["_ym"].nunique().values
        max_months = max(1, int(out["months_present"].max()))
    else:
        out["months_present"] = np.nan
        max_months = 1

    # ── 식별자/섹터 부착 ──
    def _ref(n):
        return cmap.get(str(n))
    out["listed"] = out[company_col].map(lambda n: bool(_ref(n) and _ref(n).listed))
    out["krx_code"] = out[company_col].map(lambda n: (_ref(n).krx_code if _ref(n) else ""))
    out["company_en_official"] = out[company_col].map(
        lambda n: (_ref(n).company_en_official if _ref(n) else ""))
    out["gics_sub_name"] = out[company_col].map(
        lambda n: (_ref(n).gics_sub_name if _ref(n) else ""))
    out["gics_sector"] = out[company_col].map(
        lambda n: (_ref(n).gics_sector if _ref(n) else ""))
    out["mapped"] = out[company_col].map(lambda n: _ref(n) is not None)

    # ── 컴포넌트 점수 (0~1) ──
    # 매출 규모: 로그 후 min-max (소수 거대기업이 전부 1.0 되는 것 완화)
    out["sales_scale"] = _minmax(np.log1p(out["sales_total"]))

    # 투자 가능성: 상장 0.6 기본 + 시총 0~0.4 가점. 비상장 0.
    if market_cap:
        mc = out[company_col].map(lambda n: float(market_cap.get(str(n), 0)) or 0.0)
        mc_norm = _minmax(np.log1p(mc))
    else:
        mc_norm = pd.Series(0.0, index=out.index)
    out["investability"] = np.where(out["listed"], 0.6 + 0.4 * mc_norm, 0.0)

    # 데이터 커버리지: 기간 충실도 + 관측 행수
    if has_date:
        cov_months = out["months_present"].fillna(0) / max_months
    else:
        cov_months = pd.Series(0.5, index=out.index)   # 날짜 없으면 중립
    out["data_coverage"] = 0.7 * cov_months + 0.3 * _minmax(np.log1p(out["row_count"]))

    # 섹터 대표성: 같은 서브인더스트리 내 매출 순위 → 1위=1.0, 2위=0.5 ...
    out["_sec_key"] = out["gics_sub_name"].replace("", np.nan)
    out["sector_rep"] = 0.0
    for key, idx in out.groupby("_sec_key", dropna=True).groups.items():
        sub = out.loc[idx].sort_values("sales_total", ascending=False)
        for rank, i in enumerate(sub.index, start=1):
            out.at[i, "sector_rep"] = 1.0 / rank
    # 섹터 미매핑(공백)은 중립 0.3
    out.loc[out["_sec_key"].isna(), "sector_rep"] = 0.3

    # ── 합성 점수 (0~100) ──
    out["composite_score"] = (
        w.sales_scale * out["sales_scale"]
        + w.investability * out["investability"]
        + w.data_coverage * out["data_coverage"]
        + w.sector_rep * out["sector_rep"]
    ) * 100.0
    out["composite_score"] = out["composite_score"].round(1)

    out = out.sort_values("composite_score", ascending=False).reset_index(drop=True)
    out["rank"] = np.arange(1, len(out) + 1)
    out.drop(columns=["_sec_key"], inplace=True)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 2) 유니버스 선정 (자동 제안 → 사람 검수)
# ══════════════════════════════════════════════════════════════════════════════
def _reason(row: pd.Series) -> str:
    """선정/제외 사유 자동 생성 (global IR 설명용 초안)."""
    bits = []
    if row.get("listed"):
        code = row.get("krx_code") or ""
        bits.append(f"상장사({code})" if code else "상장사")
    else:
        bits.append("비상장")
    sec = row.get("sub_sector") or row.get("gics_sub_name") or ""
    if sec:
        bits.append(f"{sec} 대표성 {row.get('sector_rep', 0):.2f}")
    seg = row.get("segment") or ""
    if seg:
        bits.append(f"세그먼트 {seg}")
    mcv = row.get("market_cap")
    if mcv is not None and pd.notna(mcv) and float(mcv) > 0:
        bits.append(f"시총 {float(mcv)/1e12:.2f}조")
    mp = row.get("months_present")
    if mp is not None and pd.notna(mp):
        bits.append(f"커버리지 {int(mp)}개월")
    bits.append(f"종합점수 {row.get('composite_score', 0):.1f}")
    return " · ".join(bits)


def select_universe(
    scored: pd.DataFrame,
    *,
    target_n: int = DEFAULT_TARGET_N,
    watchlist_n: int = DEFAULT_WATCHLIST_N,
    listed_only: bool = False,
) -> pd.DataFrame:
    """스코어 테이블 → 자동 선정 제안.

    status: 'selected'(메인 유니버스) | 'watchlist'(관찰) | 'excluded'.
    listed_only=True 면 비상장은 selected 후보에서 제외(watchlist 로).
    사람이 이후 status/selection_reason 을 오버라이드한다.
    """
    df = scored.copy()
    df["status"] = "excluded"
    df["analyst_override"] = False

    pool = df.copy()
    if listed_only:
        listed = pool[pool["listed"]].index
        nonlisted = pool[~pool["listed"]].index
        sel = list(listed[:target_n])
        df.loc[sel, "status"] = "selected"
        remaining = [i for i in pool.index if i not in sel]
        df.loc[remaining[:watchlist_n], "status"] = "watchlist"
    else:
        order = pool.index.tolist()
        df.loc[order[:target_n], "status"] = "selected"
        df.loc[order[target_n:target_n + watchlist_n], "status"] = "watchlist"

    df["selection_reason"] = df.apply(_reason, axis=1)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 3) 대표 브랜드 — 유지관리 브랜드 마스터에서(데이터 불필요), 또는 매출 기준
# ══════════════════════════════════════════════════════════════════════════════
BRAND_COLS = ["company_kr", "brand_kr", "brand_en", "brand_id",
              "selected", "selection_reason"]


_BRAND_CAND_CACHE: dict | None = None


def _recommended_brands() -> dict[str, list]:
    """유지관리 추천 브랜드 시드 → {회사: [(brand_kr, brand_en)]}. (캐시)"""
    global _BRAND_CAND_CACHE
    if _BRAND_CAND_CACHE is not None:
        return _BRAND_CAND_CACHE
    out: dict[str, list] = {}
    try:
        for r in config._load_csv(config.MASTER_DIR / "fnb_brand_candidates.csv"):
            co = str(r.get("company_kr", "")).strip()
            br = str(r.get("brand_kr", "")).strip()
            if co and br:
                out.setdefault(co, []).append((br, (r.get("brand_en") or "").strip()))
    except Exception:                              # noqa: BLE001
        out = {}
    _BRAND_CAND_CACHE = out
    return out


def candidate_brands(companies: list[str], top_n: int = DEFAULT_BRANDS_PER_CO) -> pd.DataFrame:
    """선정 회사들의 대표 브랜드 — 회사당 top_n(기본 5)개를 *자동 추천*해서 채운다.

    기획 단계의 '대표 브랜드 5개'는 데이터를 확보할 타깃이다. 어드민이 빈 칸을
    채우는 게 아니라, 시스템이 먼저 추천하고 어드민이 수정한다:
      1) config.BRAND_MASTER (큐레이션, 영문명·ID 보유) 우선
      2) fnb_brand_candidates.csv (회사별 대표 브랜드 추천 시드)로 채움
      3) 그래도 모자라면 빈 슬롯 패딩 (드묾)
    반환: company_kr, brand_kr, brand_en, brand_id, selected, selection_reason.
    """
    cos = [str(c) for c in (companies or [])]
    curated: dict[str, list] = {c: [] for c in cos}
    for (co, brand), ref in config.BRAND_MASTER.items():
        co = str(co)
        if co in curated:
            curated[co].append((brand, ref.get("en", ""), ref.get("id", "")))
    rec = _recommended_brands()

    rows = []
    for co in cos:
        seen = set()
        picks = []  # (brand_kr, brand_en, brand_id, source)
        for brand, en, bid in curated.get(co, []):
            if brand not in seen:
                seen.add(brand); picks.append((brand, en, bid, "curated"))
        for brand, en in rec.get(co, []):
            if len(picks) >= top_n:
                break
            if brand not in seen:
                seen.add(brand); picks.append((brand, en, "", "recommended"))
        picks = picks[:top_n]
        for brand, en, bid, src in picks:
            rows.append({"company_kr": co, "brand_kr": brand, "brand_en": en,
                         "brand_id": bid, "selected": True,
                         "selection_reason": ("브랜드 마스터 등록" if src == "curated"
                                              else "자동 추천 대표 브랜드")})
        for _ in range(max(0, top_n - len(picks))):
            rows.append({"company_kr": co, "brand_kr": "", "brand_en": "",
                         "brand_id": "", "selected": False,
                         "selection_reason": "(추천 없음 — 직접 입력)"})
    if not rows:
        return pd.DataFrame(columns=BRAND_COLS)
    return pd.DataFrame(rows)[BRAND_COLS]


def select_brands(
    brand_df: pd.DataFrame,
    *,
    companies: list[str] | None = None,
    company_col: str = "company_kr",
    brand_col: str = "brand_kr",
    amount_col: str = "sales_amt",
    top_n: int = DEFAULT_BRANDS_PER_CO,
) -> pd.DataFrame:
    """회사별 대표 브랜드 top_n (회사 내 매출 기여도 기준).

    반환: company_kr, brand_kr, brand_en, brand_sales, brand_share(회사내 %),
          rank_in_company, selection_reason.
    """
    df = brand_df.copy()
    df[company_col] = df[company_col].astype(str)
    df[brand_col] = df[brand_col].astype(str)
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)
    if companies is not None:
        df = df[df[company_col].isin([str(c) for c in companies])]

    g = (df.groupby([company_col, brand_col])[amount_col].sum()
         .rename("brand_sales").reset_index())
    co_total = g.groupby(company_col)["brand_sales"].transform("sum")
    g["brand_share"] = np.where(co_total > 0, g["brand_sales"] / co_total * 100.0, 0.0)
    g = g.sort_values([company_col, "brand_sales"], ascending=[True, False])
    g["rank_in_company"] = g.groupby(company_col).cumcount() + 1
    g = g[g["rank_in_company"] <= top_n].copy()

    def _ben(row):
        ref = config.BRAND_MASTER.get((row[company_col], row[brand_col]))
        return ref["en"] if ref else ""
    g["brand_en"] = g.apply(_ben, axis=1)
    g["selection_reason"] = g.apply(
        lambda r: f"회사 내 매출기여 {r['brand_share']:.1f}% (사내 {int(r['rank_in_company'])}위)",
        axis=1)
    return g.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# 4) 영속화 + 반기 리뷰 (정기관리 저장소)
# ══════════════════════════════════════════════════════════════════════════════
UNIVERSE_COLS = [
    "rank", "company_kr", "company_en_official", "krx_code", "listed",
    "segment", "sub_sector", "gics_sub_name", "gics_sector", "market_cap",
    "mc_score", "sector_rep", "segment_score", "listed_score",
    "sales_total", "months_present",
    "sales_scale", "investability", "data_coverage",
    "composite_score", "status", "analyst_override", "selection_reason",
]


def default_store_dir() -> Path:
    """정기관리 저장소(기본). 패키지 master 디렉터리 하위 universe/."""
    d = config.MASTER_DIR / "universe"
    return d


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def next_review_date(from_date: date | None = None,
                     months: int = REVIEW_PERIOD_MONTHS) -> date:
    """반기(기본 6개월) 후 다음 리뷰 예정일."""
    d = from_date or date.today()
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    day = min(d.day, [31, 29 if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0) else 28,
                      31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return date(y, m, day)


def save_universe(universe_df: pd.DataFrame, brands_df: pd.DataFrame | None = None,
                  store_dir: Path | None = None, note: str = "") -> dict:
    """유니버스 + 대표브랜드 저장 + 리뷰 로그 1행 append. 반환: 경로/메타."""
    store_dir = Path(store_dir or default_store_dir())
    _ensure_dir(store_dir)
    cols = [c for c in UNIVERSE_COLS if c in universe_df.columns]
    upath = store_dir / "universe_current.csv"
    universe_df[cols].to_csv(upath, index=False, encoding="utf-8-sig")
    bpath = store_dir / "brands_current.csv"
    if brands_df is not None and len(brands_df):
        brands_df.to_csv(bpath, index=False, encoding="utf-8-sig")

    today = date.today()
    n_sel = int((universe_df["status"] == "selected").sum()) if "status" in universe_df else 0
    n_watch = int((universe_df["status"] == "watchlist").sum()) if "status" in universe_df else 0
    log_row = {
        "review_date": today.isoformat(),
        "next_review": next_review_date(today).isoformat(),
        "n_selected": n_sel,
        "n_watchlist": n_watch,
        "n_brands": int(len(brands_df)) if brands_df is not None else 0,
        "note": note,
    }
    lpath = store_dir / "review_log.csv"
    log_df = pd.DataFrame([log_row])
    if lpath.exists():
        prev = pd.read_csv(lpath)
        log_df = pd.concat([prev, log_df], ignore_index=True)
    log_df.to_csv(lpath, index=False, encoding="utf-8-sig")
    return {"universe_path": str(upath), "brands_path": str(bpath),
            "log_path": str(lpath), **log_row}


def load_universe(store_dir: Path | None = None) -> dict:
    """저장된 유니버스/브랜드/리뷰로그 로드. 없으면 빈 DataFrame."""
    store_dir = Path(store_dir or default_store_dir())
    def _read(name):
        p = store_dir / name
        return pd.read_csv(p) if p.exists() else pd.DataFrame()
    return {"universe": _read("universe_current.csv"),
            "brands": _read("brands_current.csv"),
            "review_log": _read("review_log.csv")}


def review_due(store_dir: Path | None = None, today: date | None = None) -> dict:
    """반기 리뷰 도래 여부. 반환: {due, last_review, next_review, days_left}."""
    today = today or date.today()
    data = load_universe(store_dir)
    log = data["review_log"]
    if log is None or len(log) == 0:
        return {"due": True, "last_review": None, "next_review": None,
                "days_left": None, "reason": "리뷰 이력 없음 — 최초 유니버스 구성 필요"}
    last = log.iloc[-1]
    nxt = pd.to_datetime(last["next_review"]).date()
    days = (nxt - today).days
    return {"due": days <= 0, "last_review": str(last["review_date"]),
            "next_review": str(last["next_review"]), "days_left": days,
            "reason": ("반기 리뷰 도래" if days <= 0 else f"다음 리뷰까지 {days}일")}
