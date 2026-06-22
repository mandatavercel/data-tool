"""
kfnb_app/strategy/recommender.py — 상품 추천 엔진.

유니버스(무엇을 관리할지) 위에서 "지금 무엇을 상품화·딥다이브할지"를 제안한다.
외부 신호(트렌드·뉴스·신제품·컨센서스)를 결합해:
  ① 세그먼트별 SKU 딥다이브 우선순위 (Premium 대상)
  ② 회사별 액션 (딥다이브 / Brand Tracker 강화 / 편입 검토 / 유지·관찰)
  ③ 트렌드 패키징 제안 (예: 매운맛/불닭 트렌드)
을 사유·confidence·데이터 정직표기(data_status)와 함께 산출한다.

핵심 원칙(정직):
  - 신호가 없으면 지어내지 않는다. 가용 신호로만 점수화하고, 무엇이 비었는지 명시.
  - "컨센서스보다 깊은 인사이트": 트렌드 열기(heat)가 높은데 컨센서스 불확실성
    (분산·리비전)이 큰 곳 = 우리 alt-data 가 차별화될 알파 기회로 가점.

순수 pandas — 외부 호출/streamlit 비의존. 신호는 어댑터(ingest.trends 등) 또는
수동 입력으로 주입한다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from kfnb_app import config

# SKU 딥다이브(Premium)가 의미 있는 세그먼트 — 제품단위 경쟁이 투자신호가 되는 곳
DEEPDIVE_ELIGIBLE = {"ramen", "alcohol", "snack", "beverage", "dairy"}

HEAT_HIGH = 0.60
OPP_HIGH = 0.60
NEWPROD_REF = 4        # 최근 신제품 4건 이상 → 신제품 신호 최대
NEWS_REF = 500         # 뉴스 500건 이상 → 뉴스 신호 최대

# 매운맛/트렌드 키워드(패키징 탐지용) — 한/영
SPICY_MARKERS = ("불닭", "매운", "마라", "spicy", "buldak", "hot", "mala")


def _minmax(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.notna().sum() == 0:
        return s
    lo, hi = s.min(), s.max()
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return s.notna().astype(float) * 0.5
    return (s - lo) / (hi - lo)


def _keyword_to_company(brands_df: pd.DataFrame | None,
                        universe_df: pd.DataFrame) -> dict:
    """키워드/검색어 → 회사명 매핑 (브랜드명·영문명·회사명)."""
    m: dict[str, str] = {}
    for co in universe_df["company_kr"].astype(str):
        m[co] = co
    if brands_df is not None and len(brands_df):
        for _, r in brands_df.iterrows():
            co = str(r.get("company_kr", ""))
            for k in (r.get("brand_kr"), r.get("brand_en")):
                if isinstance(k, str) and k.strip():
                    m[k.strip()] = co
    return m


# ══════════════════════════════════════════════════════════════════════════════
# 1) 신호 결합
# ══════════════════════════════════════════════════════════════════════════════
def assemble_signals(
    universe_df: pd.DataFrame,
    *,
    brands_df: pd.DataFrame | None = None,
    trends_df: pd.DataFrame | None = None,      # keyword, trend_momentum
    news_df: pd.DataFrame | None = None,        # query, news_count
    consensus_df: pd.DataFrame | None = None,   # company_kr, consensus_revision, consensus_dispersion
    newproduct_df: pd.DataFrame | None = None,  # company_kr, new_product_count
    selected_only: bool = True,
) -> pd.DataFrame:
    """유니버스 + 외부신호 → 회사별 신호 프레임. 없는 신호는 NaN(정직)."""
    u = universe_df.copy()
    if selected_only and "status" in u.columns:
        u = u[u["status"] == "selected"]
    base_cols = [c for c in ["company_kr", "segment", "sub_sector", "listed",
                             "composite_score"] if c in u.columns]
    out = u[base_cols].drop_duplicates("company_kr").reset_index(drop=True)

    kmap = _keyword_to_company(brands_df, out)

    # 트렌드 모멘텀 → 회사별 평균
    out["trend_momentum"] = np.nan
    if trends_df is not None and len(trends_df):
        t = trends_df.copy()
        t["company_kr"] = t["keyword"].map(lambda k: kmap.get(str(k)))
        agg = t.dropna(subset=["company_kr"]).groupby("company_kr")[
            "trend_momentum"].mean()
        out["trend_momentum"] = out["company_kr"].map(agg)

    # 뉴스량 → 회사별 합
    out["news_count"] = np.nan
    if news_df is not None and len(news_df):
        n = news_df.copy()
        n["company_kr"] = n["query"].map(lambda k: kmap.get(str(k)))
        agg = n.dropna(subset=["company_kr"]).groupby("company_kr")["news_count"].sum()
        out["news_count"] = out["company_kr"].map(agg)

    # 신제품 수
    out["new_product_count"] = np.nan
    if newproduct_df is not None and len(newproduct_df):
        npd = newproduct_df.set_index("company_kr")["new_product_count"]
        out["new_product_count"] = out["company_kr"].map(npd)

    # 컨센서스 리비전·분산
    out["consensus_revision"] = np.nan
    out["consensus_dispersion"] = np.nan
    if consensus_df is not None and len(consensus_df):
        c = consensus_df.set_index("company_kr")
        if "consensus_revision" in c.columns:
            out["consensus_revision"] = out["company_kr"].map(c["consensus_revision"])
        if "consensus_dispersion" in c.columns:
            out["consensus_dispersion"] = out["company_kr"].map(c["consensus_dispersion"])
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 2) 점수화
# ══════════════════════════════════════════════════════════════════════════════
def score_signals(sig: pd.DataFrame) -> pd.DataFrame:
    """신호 프레임 → heat / consensus_opportunity / alpha_priority + data_status.

    각 신호를 *절대* 스케일로 점수화한다(피어 min-max 아님). 한 회사만 신호가
    있어도 그 강도를 그대로 반영하기 위함이며, 해석도 명확하다.
      trend_score   = clip(0.5 + 0.5·momentum, 0, 1)   # mom +20% → 0.6 초과
      newprod_score = clip(count / NEWPROD_REF, 0, 1)   # NEWPROD_REF(=4)+ → 1.0
      news_score    = clip(log1p(n) / log1p(NEWS_REF), 0, 1)
      opportunity   = mean(dispersion, |revision|)      # 둘 다 0~1 입력
    """
    df = sig.copy()
    nan = pd.Series(np.nan, index=df.index)

    def _trend(m):
        return np.clip(0.5 + 0.5 * np.clip(m, -1, 1), 0, 1)
    trend_s = df["trend_momentum"].map(_trend) if "trend_momentum" in df else nan
    prod_s = (np.clip(df["new_product_count"] / NEWPROD_REF, 0, 1)
              if "new_product_count" in df else nan)
    news_s = (np.clip(np.log1p(df["news_count"]) / np.log1p(NEWS_REF), 0, 1)
              if "news_count" in df else nan)

    df["heat"] = pd.concat([trend_s, news_s, prod_s], axis=1).mean(axis=1, skipna=True)

    disp_s = df["consensus_dispersion"].clip(0, 1) if "consensus_dispersion" in df else nan
    rev_s = df["consensus_revision"].abs().clip(0, 1) if "consensus_revision" in df else nan
    df["consensus_opportunity"] = pd.concat([disp_s, rev_s], axis=1).mean(axis=1, skipna=True)

    # alpha_priority: heat 중심 + 컨센서스 기회 가점(있으면)
    def _prio(r):
        h = r["heat"]
        if pd.isna(h):
            return np.nan
        o = r["consensus_opportunity"]
        if pd.isna(o):
            return round(h, 3)
        return round(0.65 * h + 0.35 * o, 3)
    df["alpha_priority"] = df.apply(_prio, axis=1)

    # data_status: 가용 신호 표기 (정직)
    def _status(r):
        present = []
        if pd.notna(r.get("trend_momentum")): present.append("trend")
        if pd.notna(r.get("news_count")): present.append("news")
        if pd.notna(r.get("new_product_count")): present.append("newprod")
        if pd.notna(r.get("consensus_dispersion")) or pd.notna(r.get("consensus_revision")):
            present.append("consensus")
        return ",".join(present) if present else "none"
    df["data_status"] = df.apply(_status, axis=1)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 3) 추천
# ══════════════════════════════════════════════════════════════════════════════
def _confidence(status: str) -> str:
    parts = set(p for p in status.split(",") if p)
    if {"trend"} & parts and ({"consensus"} & parts or {"news"} & parts):
        return "high"
    if parts and parts != {"none"}:
        return "medium"
    return "low"


def recommend(scored: pd.DataFrame) -> pd.DataFrame:
    """회사별 추천 액션 + 사유 + confidence."""
    rows = []
    for _, r in scored.iterrows():
        heat, opp = r.get("heat"), r.get("consensus_opportunity")
        seg = str(r.get("segment") or "")
        listed = bool(r.get("listed"))
        status = str(r.get("data_status") or "none")
        eligible = seg in DEEPDIVE_ELIGIBLE

        if pd.isna(heat):
            action = "신호 부족 — 데이터 수집 필요"
            why = "외부 신호(트렌드/뉴스/신제품/컨센서스) 미확보"
        elif heat >= HEAT_HIGH and eligible and listed:
            action = "Premium SKU 딥다이브 우선"
            why = f"트렌드 열기 {heat:.2f}·{seg} 딥다이브 적합·상장"
            if pd.notna(opp) and opp >= OPP_HIGH:
                why += f"·컨센 불확실성 {opp:.2f}(알파 기회)"
        elif heat >= HEAT_HIGH and listed:
            action = "Brand Tracker 강화 (Professional)"
            why = f"트렌드 열기 {heat:.2f}·상장(단, {seg} 는 SKU 딥다이브 비핵심)"
        elif heat >= HEAT_HIGH and not listed:
            action = "비상장 — 모니터링/대체 데이터"
            why = f"트렌드 열기 {heat:.2f} 높으나 비상장(투자 직접연결 약함)"
        elif pd.notna(opp) and opp >= OPP_HIGH:
            action = "컨센서스 갭 — 커버리지 강화"
            why = f"컨센 불확실성 {opp:.2f} 큼 → alt-data 차별화 여지"
        else:
            action = "유지 / 관찰"
            why = f"트렌드 열기 {heat:.2f} (특이신호 약함)"

        rows.append({
            "company_kr": r["company_kr"], "segment": seg, "listed": listed,
            "heat": None if pd.isna(heat) else round(float(heat), 3),
            "consensus_opportunity": None if pd.isna(opp) else round(float(opp), 3),
            "alpha_priority": r.get("alpha_priority"),
            "recommended_action": action, "rationale": why,
            "confidence": _confidence(status), "data_status": status,
        })
    out = pd.DataFrame(rows)
    return out.sort_values(
        ["alpha_priority", "heat"], ascending=False, na_position="last"
    ).reset_index(drop=True)


def segment_recommendations(scored: pd.DataFrame) -> pd.DataFrame:
    """세그먼트별 열기 집계 → SKU 딥다이브 우선 세그먼트."""
    if "segment" not in scored.columns:
        return pd.DataFrame(columns=["segment", "avg_heat", "n", "deepdive_eligible", "note"])
    g = scored.groupby("segment").agg(
        avg_heat=("heat", "mean"), n=("company_kr", "count")).reset_index()
    g["deepdive_eligible"] = g["segment"].isin(DEEPDIVE_ELIGIBLE)
    g = g.sort_values(["deepdive_eligible", "avg_heat"], ascending=False,
                      na_position="last").reset_index(drop=True)
    def _note(r):
        if pd.isna(r["avg_heat"]):
            return "신호 부족"
        if r["deepdive_eligible"] and r["avg_heat"] >= HEAT_HIGH:
            return "딥다이브 1순위 후보"
        if r["deepdive_eligible"]:
            return "딥다이브 적합(열기 보통)"
        return "회사/브랜드 레벨로 충분"
    g["note"] = g.apply(_note, axis=1)
    return g


def trend_packaging(scored: pd.DataFrame, trends_df: pd.DataFrame | None,
                    brands_df: pd.DataFrame | None) -> list[dict]:
    """급상승 트렌드 기반 테마 패키지 제안 (예: 매운맛/불닭)."""
    out = []
    if trends_df is None or not len(trends_df):
        return out
    hot = trends_df[pd.to_numeric(trends_df["trend_momentum"], errors="coerce") > 0.2]
    if not len(hot):
        return out
    spicy = hot[hot["keyword"].astype(str).str.lower().apply(
        lambda k: any(m in k for m in SPICY_MARKERS))]
    if len(spicy):
        kws = ", ".join(spicy.sort_values("trend_momentum", ascending=False)
                        ["keyword"].head(5))
        out.append({
            "theme": "매운맛/불닭 트렌드 패키지",
            "drivers": kws,
            "suggestion": ("Premium 라면 딥다이브에 매운맛 SKU 플래그 + 수출 모멘텀(L4) "
                           "결합 — 트렌드를 빠르게 반영하는 테마 상품"),
        })
    top = hot.sort_values("trend_momentum", ascending=False).head(5)
    out.append({
        "theme": "급상승 트렌드 일반",
        "drivers": ", ".join(top["keyword"].astype(str)),
        "suggestion": "상위 모멘텀 브랜드를 Brand Tracker 하이라이트로 노출",
    })
    return out


def recommendation_summary(recs: pd.DataFrame) -> dict:
    """추천 전반의 데이터 충실도/주의사항(정직)."""
    n = len(recs)
    if n == 0:
        return {"n": 0, "coverage": "추천 대상 없음",
                "caveat": "유니버스 선정 후 신호를 결합하세요."}
    has_signal = int((recs["data_status"] != "none").sum())
    conf = recs["confidence"].value_counts().to_dict()
    caveat = []
    if has_signal < n:
        caveat.append(f"{n - has_signal}개사 신호 미확보(추천 보류)")
    if "trend" not in ",".join(recs["data_status"]):
        caveat.append("트렌드 신호 없음 — 모멘텀 기반 추천 제한")
    if "consensus" not in ",".join(recs["data_status"]):
        caveat.append("컨센서스 미입력 — '컨센 대비 깊은 인사이트' 평가 제한")
    return {"n": n, "with_signal": has_signal, "confidence_mix": conf,
            "caveat": "; ".join(caveat) if caveat else "신호 충실"}
