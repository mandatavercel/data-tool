"""
kfnb_app/insight/signal_engine.py — 글로벌 투자기관용 알파 시그널 엔진.

원천 POS(날짜·지역·회사·브랜드·카테고리·바코드·SKU·매출·수량·건수)를
"바로 백테스트 가능한" 산출물로 가공한다.

  build_alpha_panel()      → 종목단위 **와이드 PIT 패널** (available_date × ticker ×
                             시그널). 펀드가 자사 수익률과 머지해 IC/백테스트 바로 가능.
  build_category_analytics()→ 카테고리 점유율·성장·HHI (섹터 리서치)
  build_brand_analytics()  → 브랜드 모멘텀·회사내 점유율 (브랜드 트래커)
  run_engine()             → 위 전부 + (선택) 주가/공시 백테스트 리포트(alpha.py 재사용)

시그널군: 매출 모멘텀(YoY/MoM/3M) · 점유율 변화(share/share_mom/share_yoy) ·
          ASP/가격(asp_yoy/mom) · 신제품 기여 · 트렌드(매운맛/하이볼 등) 기여.
모두 Point-in-Time(각 월 t 는 t까지의 데이터만 사용, available_date=월말+릴리즈지연).
순수 pandas — 테스트 가능.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from kfnb_app import config
from kfnb_app.insight import pit

# 트렌드로 간주할 투자테마 태그(설정 category_mapping.yaml 의 investment_theme_tag)
TREND_TAGS = {"K-Highball", "K-Snack"}   # 매운맛(tag_spicy)은 별도 플래그로도 포함

PANEL_SIGNALS = [
    "sales_yoy", "sales_mom", "sales_3m",
    "share", "share_mom", "share_yoy",
    "asp_yoy", "asp_mom",
    "newproduct_share", "trend_share", "trend_share_yoy",
]


def _monthnum(ym) -> int:
    y, m = divmod(int(ym), 100)
    return y * 12 + (m - 1)


def _by_offset(df: pd.DataFrame, key: str, val: str, off: int) -> pd.Series:
    """엔티티(key)별 monthnum 기준 off개월 전 값 — 결측월에도 정확(룩업 기반)."""
    look = {(r[key], int(r["_mn"])): r[val] for _, r in df.iterrows()}
    return df.apply(lambda r: look.get((r[key], int(r["_mn"]) - off), np.nan), axis=1)


def _pct(cur: pd.Series, base: pd.Series) -> pd.Series:
    base = pd.to_numeric(base, errors="coerce")
    out = np.where((base.notna()) & (base != 0), cur / base - 1.0, np.nan)
    return pd.Series(np.round(out.astype(float), 4), index=cur.index)


def _enrich_keys(sku_master: pd.DataFrame) -> pd.DataFrame:
    """barcode → 종목 식별자/태그 룩업."""
    sm = sku_master.copy()
    sm["barcode"] = sm["barcode"].astype(str)
    sm["company_id"] = sm.apply(
        lambda r: (r.get("isin") or r.get("company_slug") or r.get("company_kr")), axis=1)
    theme = sm.get("investment_theme_tag", pd.Series("", index=sm.index)).fillna("")
    spicy = sm.get("tag_spicy", pd.Series(False, index=sm.index)).fillna(False)
    sm["trend_flag"] = spicy.astype(bool) | theme.isin(TREND_TAGS)
    keep = [c for c in ["barcode", "krx_code", "isin", "bbg_ticker",
                        "company_en_official", "company_id", "gics_sub_name",
                        "cat_l2_en", "company_kr", "brand_id", "brand_name_en",
                        "trend_flag"] if c in sm.columns]
    return sm[keep].drop_duplicates("barcode")


# ══════════════════════════════════════════════════════════════════════════════
# 1) 종목단위 와이드 PIT 알파 패널
# ══════════════════════════════════════════════════════════════════════════════
def build_alpha_panel(src, sku_master: pd.DataFrame, *, sector=None,
                      lag_days: int | None = None, listed_only: bool = True) -> pd.DataFrame:
    """available_date × ticker × 시그널(wide). 펀드 백테스트 즉시 가능."""
    keys = _enrich_keys(sku_master)
    sm = src.sku_monthly(sector).copy()
    sm["barcode"] = sm["barcode"].astype(str)
    sm = sm.merge(keys, on="barcode", how="left", suffixes=("", "_k"))
    sm = sm.rename(columns={"sales_amt": "sales", "sales_qty": "qty"})
    if listed_only:
        sm = sm[sm["krx_code"].astype(str).str.strip() != ""]
    if sm.empty:
        return pd.DataFrame(columns=["available_date", "date", "ym", "company_id",
                                     "isin", "ticker", "company_name_en"] + PANEL_SIGNALS)

    # 바코드 최초 등장월(관측 기준 신제품 판정)
    first_mn = sm.assign(_mn=sm["ym"].map(_monthnum)).groupby("barcode")["_mn"].min()
    sm["_mn"] = sm["ym"].map(_monthnum)
    sm["_first_mn"] = sm["barcode"].map(first_mn)
    sm["_is_new"] = (sm["_mn"] - sm["_first_mn"]) <= 11        # 출시 후 12개월 이내
    sm["_new_sales"] = np.where(sm["_is_new"], sm["sales"], 0.0)
    sm["_trend_sales"] = np.where(sm["trend_flag"].fillna(False), sm["sales"], 0.0)

    g = (sm.groupby(["company_id", "ym"], dropna=False)
         .agg(sales=("sales", "sum"), quantity=("qty", "sum"),
              new_sales=("_new_sales", "sum"), trend_sales=("_trend_sales", "sum"))
         .reset_index())
    # 식별자 부착
    idlook = (sm.dropna(subset=["company_id"])
              .drop_duplicates("company_id")
              .set_index("company_id")[[c for c in
               ["isin", "bbg_ticker", "company_en_official", "gics_sub_name"]
               if c in sm.columns]])
    g = g.join(idlook, on="company_id")
    g["ticker"] = g.get("bbg_ticker", "")
    g["company_name_en"] = g.get("company_en_official", "")

    # 단면 점유율(섹터 유니버스 내, t 시점만 → causal)
    tot = g.groupby("ym")["sales"].transform("sum")
    g["share"] = np.round(np.where(tot > 0, g["sales"] / tot * 100, np.nan), 4)
    g["asp"] = np.where(g["quantity"] > 0, (g["sales"] / g["quantity"]).round(1), np.nan)
    g["newproduct_share"] = np.round(
        np.where(g["sales"] > 0, g["new_sales"] / g["sales"] * 100, np.nan), 2)
    g["trend_share"] = np.round(
        np.where(g["sales"] > 0, g["trend_sales"] / g["sales"] * 100, np.nan), 2)

    g["_mn"] = g["ym"].map(_monthnum)
    g = g.sort_values(["company_id", "_mn"]).reset_index(drop=True)

    # 모멘텀(룩업 기반, 결측월 정확)
    s12 = _by_offset(g, "company_id", "sales", 12)
    s1 = _by_offset(g, "company_id", "sales", 1)
    g["sales_yoy"] = _pct(g["sales"], s12) * 100
    g["sales_mom"] = _pct(g["sales"], s1) * 100
    # 3M 모멘텀: 최근 3개월 합 vs 직전 3개월 합
    roll3 = g.groupby("company_id")["sales"].transform(
        lambda s: s.rolling(3, min_periods=3).sum())
    g["_roll3"] = roll3
    r3prev = _by_offset(g, "company_id", "_roll3", 3)
    g["sales_3m"] = _pct(g["_roll3"], r3prev) * 100
    # ASP
    a12 = _by_offset(g, "company_id", "asp", 12)
    a1 = _by_offset(g, "company_id", "asp", 1)
    g["asp_yoy"] = _pct(g["asp"], a12) * 100
    g["asp_mom"] = _pct(g["asp"], a1) * 100
    # 점유율 변화(차분)
    sh1 = _by_offset(g, "company_id", "share", 1)
    sh12 = _by_offset(g, "company_id", "share", 12)
    g["share_mom"] = (g["share"] - sh1).round(4)
    g["share_yoy"] = (g["share"] - sh12).round(4)
    ts12 = _by_offset(g, "company_id", "trend_share", 12)
    g["trend_share_yoy"] = (g["trend_share"] - ts12).round(2)

    g["date"] = g["ym"].map(lambda y: pit.month_end(int(y)).isoformat())
    g["available_date"] = g["ym"].map(
        lambda y: pit.available_date(int(y), lag_days).isoformat())

    cols = (["available_date", "date", "ym", "company_id", "isin", "ticker",
             "company_name_en", "gics_sub_name", "sales", "quantity", "asp"]
            + PANEL_SIGNALS)
    cols = [c for c in cols if c in g.columns]
    return g[cols].sort_values(["company_id", "ym"]).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# 2) 카테고리 애널리틱스
# ══════════════════════════════════════════════════════════════════════════════
def build_category_analytics(src, sku_master: pd.DataFrame, *, sector=None) -> pd.DataFrame:
    keys = _enrich_keys(sku_master)
    sm = src.sku_monthly(sector).merge(keys, on="barcode", how="left")
    sm = sm.rename(columns={"sales_amt": "sales"})
    sm["cat"] = sm.get("cat_l2_en", "").fillna("Uncategorized")
    cat = (sm.groupby(["cat", "ym"]).agg(sales=("sales", "sum"),
            n_companies=("company_id", "nunique")).reset_index())
    # HHI(회사 점유율 제곱합) + 1위 회사
    def _hhi(grp):
        s = grp.groupby("company_id")["sales"].sum()
        tot = s.sum()
        if tot <= 0:
            return pd.Series({"hhi": np.nan, "top_company": "", "top_share": np.nan})
        sh = s / tot
        top = sh.idxmax()
        nm = grp.loc[grp["company_id"] == top, "company_en_official"]
        return pd.Series({"hhi": round(float((sh ** 2).sum()), 4),
                          "top_company": (nm.iloc[0] if len(nm) else top),
                          "top_share": round(float(sh.max() * 100), 2)})
    extra = sm.groupby(["cat", "ym"])[
        ["company_id", "sales", "company_en_official"]].apply(_hhi).reset_index()
    cat = cat.merge(extra, on=["cat", "ym"], how="left")
    cat["_mn"] = cat["ym"].map(_monthnum)
    cat = cat.sort_values(["cat", "_mn"])
    s12 = _by_offset(cat.rename(columns={"cat": "company_id"}), "company_id", "sales", 12)
    cat["sales_yoy"] = (_pct(cat["sales"], s12) * 100).values
    return cat.drop(columns=["_mn"]).rename(columns={"cat": "category"}).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# 3) 브랜드 애널리틱스
# ══════════════════════════════════════════════════════════════════════════════
def build_brand_analytics(src, sku_master: pd.DataFrame, *, sector=None) -> pd.DataFrame:
    keys = _enrich_keys(sku_master)
    sm = src.sku_monthly(sector).merge(keys, on="barcode", how="left")
    sm = sm.rename(columns={"sales_amt": "sales"})
    sm["brand_id"] = sm.get("brand_id", "").fillna("")
    br = (sm.groupby(["company_id", "brand_id", "ym"])
          .agg(sales=("sales", "sum")).reset_index())
    # 회사 내 점유율
    cot = br.groupby(["company_id", "ym"])["sales"].transform("sum")
    br["brand_share_in_company"] = np.round(
        np.where(cot > 0, br["sales"] / cot * 100, np.nan), 2)
    br["_mn"] = br["ym"].map(_monthnum)
    br = br.sort_values(["company_id", "brand_id", "_mn"])
    br["_eid"] = br["company_id"].astype(str) + "|" + br["brand_id"].astype(str)
    s12 = _by_offset(br, "_eid", "sales", 12)
    s1 = _by_offset(br, "_eid", "sales", 1)
    br["sales_yoy"] = (_pct(br["sales"], s12) * 100).values
    br["sales_mom"] = (_pct(br["sales"], s1) * 100).values
    # 영문 브랜드명
    bname = sm.dropna(subset=["brand_id"]).drop_duplicates("brand_id")
    if "brand_name_en" in bname.columns:
        br = br.merge(bname[["brand_id", "brand_name_en"]], on="brand_id", how="left")
    return br.drop(columns=["_mn", "_eid"]).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# 4) 오케스트레이션 (+ 백테스트 리포트는 alpha.py 재사용)
# ══════════════════════════════════════════════════════════════════════════════
def run_engine(src, sku_master: pd.DataFrame, *, sector=None, lag_days=None) -> dict:
    """엔진 산출물 dict. 백테스트(주가/공시)는 호출부에서 alpha.* 로 연결."""
    return {
        "alpha_panel": build_alpha_panel(src, sku_master, sector=sector,
                                         lag_days=lag_days),
        "category_analytics": build_category_analytics(src, sku_master, sector=sector),
        "brand_analytics": build_brand_analytics(src, sku_master, sector=sector),
    }
