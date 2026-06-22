"""
kfnb_app/insight/usecase.py — 전수 use-case(투자 시그널) 자동 발굴.

데이터셋 전체를 스캔해 투자기관이 바로 쓸 수 있는 시그널을 랭킹·생성한다.
한 브랜드 하이라이트가 아니라 데이터 전반의 use-case 를 뽑는 것이 목적.

유형:
  momentum      전 브랜드 TTM YoY 가속/둔화 (상장사 시그널)
  new_hit       최근 출시 SKU 의 회전 속도 — '다음 히트' 조기 포착
  share_shift   카테고리 내 회사 점유율 이동
  asp_premium   브랜드 ASP 추세 — 가격인상·믹스개선

각 시그널 레코드:
  usecase_type · entity_type · entity_kr · entity_en · ticker · isin ·
  metric · value · window · direction · confidence · thesis_ko · thesis_en
streamlit 비의존, 순수 pandas.
"""
from __future__ import annotations

import pandas as pd


# ── 유틸 ────────────────────────────────────────────────────────────────────
def _ym_index(ym: int) -> int:
    """YYYYMM → 절대 월 인덱스 (정렬·윈도우용)."""
    return (ym // 100) * 12 + (ym % 100)


def _ymd_months(ymd: int) -> int:
    return (ymd // 10000) * 12 + ((ymd // 100) % 100)


def _ttm_windows(panel: pd.DataFrame):
    """월별 패널 → (최근12개월 set, 직전12개월 set). 부족하면 (None, None)."""
    months = sorted(panel["ym"].unique())
    if len(months) < 24:
        return None, None
    return set(months[-12:]), set(months[-24:-12])


def _brand_en_map(sku_master: pd.DataFrame) -> dict:
    if sku_master is None or "brand_name_en" not in sku_master:
        return {}
    return dict(zip(sku_master["brand_kr"], sku_master["brand_name_en"]))


def _conf(ticker: str, material: bool) -> str:
    if ticker and material:
        return "high"
    if ticker:
        return "medium"
    return "low"


# ── 1) 모멘텀 스캐너 ─────────────────────────────────────────────────────────
def momentum_signals(panel: pd.DataFrame, brand_en: dict, top_n: int = 6) -> list[dict]:
    last12, prior12 = _ttm_windows(panel)
    if last12 is None:
        return []
    rows = []
    keys = ["company_kr", "brand_kr", "bbg_ticker", "isin"]
    for (co, br, tk, isin), sub in panel.groupby(keys):
        ttm = sub.loc[sub.ym.isin(last12), "sales_amt"].sum()
        pri = sub.loc[sub.ym.isin(prior12), "sales_amt"].sum()
        if pri <= 0 or ttm <= 0:
            continue
        yoy = (ttm / pri - 1) * 100
        rows.append({"company_kr": co, "brand_kr": br, "ticker": tk,
                     "isin": isin, "ttm": ttm, "yoy": yoy})
    if not rows:
        return []
    df = pd.DataFrame(rows)
    med = df["ttm"].median()
    df = df.sort_values("yoy", ascending=False)
    picks = pd.concat([df.head(top_n), df.tail(3)]).drop_duplicates()
    out = []
    for _, r in picks.iterrows():
        en = brand_en.get(r.brand_kr, r.brand_kr)
        direction = "up" if r.yoy >= 0 else "down"
        word_ko = "가속" if r.yoy >= 10 else ("둔화" if r.yoy < 0 else "완만")
        word_en = "accelerating" if r.yoy >= 10 else ("declining" if r.yoy < 0 else "soft")
        out.append({
            "usecase_type": "momentum", "entity_type": "brand",
            "entity_kr": r.brand_kr, "entity_en": en, "ticker": r.ticker,
            "isin": r["isin"], "metric": "TTM sales YoY %", "value": round(r.yoy, 1),
            "window": "trailing 12m", "direction": direction,
            "confidence": _conf(r.ticker, r.ttm >= med),
            "thesis_ko": f"{en}({r.brand_kr}) 최근 12개월 매출 YoY {r.yoy:+.1f}% — {word_ko}"
                         + (f" / {r.ticker}" if r.ticker else " (비상장)"),
            "thesis_en": f"{en} TTM sales {r.yoy:+.1f}% YoY — {word_en}"
                         + (f" ({r.ticker})" if r.ticker else ""),
        })
    return out


# ── 2) 신제품 히트 감지 ──────────────────────────────────────────────────────
def new_hit_signals(sku_master: pd.DataFrame, brand_en: dict,
                    window_months: int = 18, top_n: int = 6) -> list[dict]:
    if sku_master is None or "first_date" not in sku_master:
        return []
    df = sku_master.copy()
    df["first_date"] = pd.to_numeric(df["first_date"], errors="coerce")
    df["last_date"] = pd.to_numeric(df["last_date"], errors="coerce")
    max_m = _ymd_months(int(df["last_date"].max()))
    df["launch_m"] = df["first_date"].map(lambda x: _ymd_months(int(x)))
    df = df[(max_m - df["launch_m"]) <= window_months]
    if df.empty:
        return []
    df["months_live"] = (max_m - df["launch_m"]).clip(lower=1)
    df["velocity"] = pd.to_numeric(df["sales_amt"], errors="coerce") / df["months_live"]
    df = df.sort_values("velocity", ascending=False).head(top_n)
    out = []
    for _, r in df.iterrows():
        en = r.get("sku_name_en") or r.get("brand_name_en") or r.brand_kr
        tk = r.get("bbg_ticker", "")
        out.append({
            "usecase_type": "new_hit", "entity_type": "sku",
            "entity_kr": r.get("sku_name_kr", ""), "entity_en": en, "ticker": tk,
            "isin": r.get("isin", ""), "metric": "sales velocity (₩/mo)",
            "value": round(float(r.velocity), 0), "window": f"launched ≤{window_months}m",
            "direction": "up", "confidence": _conf(tk, True),
            "thesis_ko": f"신제품 {en} — 출시 {int(r.months_live)}개월, 월평균 매출 "
                         f"{r.velocity/1e8:.1f}억 (회전 상위)"
                         + (f" / {tk}" if tk else ""),
            "thesis_en": f"New SKU {en} — {int(r.months_live)}m since launch, "
                         f"top sell-through velocity" + (f" ({tk})" if tk else ""),
        })
    return out


# ── 3) 점유율 이동 ───────────────────────────────────────────────────────────
def share_shift_signals(panel: pd.DataFrame, company_en: dict, top_n: int = 6) -> list[dict]:
    """완전 연도(12개월) 기준 회사 점유율 first→last 변화."""
    df = panel.copy()
    df["yr"] = df["ym"] // 100
    counts = df.groupby("yr")["ym"].nunique()
    full = sorted([y for y, c in counts.items() if c >= 12])
    if len(full) < 2:
        return []
    y0, y1 = full[0], full[-1]
    share = {}
    for yr in (y0, y1):
        sub = df[df.yr == yr]
        tot = sub["sales_amt"].sum()
        if not tot:                      # 0 분모 방어
            share[yr] = pd.Series(dtype=float)
            continue
        s = sub.groupby(["company_kr", "bbg_ticker", "isin"])["sales_amt"].sum() / tot * 100
        share[yr] = s
    rows = []
    for key in share[y1].index:
        co, tk, isin = key
        s1 = share[y1].get(key, 0.0)
        s0 = share[y0].get(key, 0.0)
        rows.append({"company_kr": co, "ticker": tk, "isin": isin,
                     "s0": s0, "s1": s1, "delta": s1 - s0})
    rdf = pd.DataFrame(rows)
    rdf = rdf.reindex(rdf["delta"].abs().sort_values(ascending=False).index).head(top_n)
    res = []
    for _, r in rdf.iterrows():
        en = company_en.get(r.company_kr, r.company_kr)
        direction = "up" if r.delta >= 0 else "down"
        res.append({
            "usecase_type": "share_shift", "entity_type": "company",
            "entity_kr": r.company_kr, "entity_en": en, "ticker": r.ticker,
            "isin": r["isin"], "metric": "CU-channel share Δ (pp, mapped universe)",
            "value": round(r.delta, 1), "window": f"{y0}→{y1}", "direction": direction,
            "confidence": _conf(r.ticker, True),
            "thesis_ko": f"{en}({r.company_kr}) CU채널 점유율(매핑 유니버스 내) "
                         f"{r.s0:.1f}%→{r.s1:.1f}% ({r.delta:+.1f}pp)"
                         + (f" / {r.ticker}" if r.ticker else " (비상장)"),
            "thesis_en": f"{en} CU-channel share (mapped universe) "
                         f"{r.s0:.1f}%→{r.s1:.1f}% ({r.delta:+.1f}pp)"
                         + (f" ({r.ticker})" if r.ticker else ""),
        })
    return res


# ── 4) ASP / 프리미엄화 ──────────────────────────────────────────────────────
def asp_signals(panel: pd.DataFrame, brand_en: dict, top_n: int = 6) -> list[dict]:
    last12, prior12 = _ttm_windows(panel)
    if last12 is None:
        return []
    rows = []
    for (co, br, tk, isin), sub in panel.groupby(["company_kr", "brand_kr",
                                                  "bbg_ticker", "isin"]):
        a1 = sub.loc[sub.ym.isin(last12), "sales_amt"].sum()
        q1 = sub.loc[sub.ym.isin(last12), "sales_qty"].sum()
        a0 = sub.loc[sub.ym.isin(prior12), "sales_amt"].sum()
        q0 = sub.loc[sub.ym.isin(prior12), "sales_qty"].sum()
        if q0 <= 0 or q1 <= 0 or a1 <= 0:
            continue
        asp1, asp0 = a1 / q1, a0 / q0
        chg = (asp1 / asp0 - 1) * 100
        rows.append({"company_kr": co, "brand_kr": br, "ticker": tk, "isin": isin,
                     "asp1": asp1, "chg": chg, "ttm": a1})
    if not rows:
        return []
    df = pd.DataFrame(rows)
    med = df["ttm"].median()
    df = df.reindex(df["chg"].abs().sort_values(ascending=False).index).head(top_n)
    out = []
    for _, r in df.iterrows():
        en = brand_en.get(r.brand_kr, r.brand_kr)
        direction = "up" if r.chg >= 0 else "down"
        out.append({
            "usecase_type": "asp_premium", "entity_type": "brand",
            "entity_kr": r.brand_kr, "entity_en": en, "ticker": r.ticker,
            "isin": r["isin"], "metric": "ASP YoY %", "value": round(r.chg, 1),
            "window": "trailing 12m", "direction": direction,
            "confidence": _conf(r.ticker, r.ttm >= med),
            "thesis_ko": f"{en}({r.brand_kr}) ASP {r.asp1:,.0f}원, YoY {r.chg:+.1f}% "
                         + ("(프리미엄화/가격인상)" if r.chg >= 0 else "(가격 하락)")
                         + (f" / {r.ticker}" if r.ticker else ""),
            "thesis_en": f"{en} ASP ₩{r.asp1:,.0f}, {r.chg:+.1f}% YoY"
                         + (f" ({r.ticker})" if r.ticker else ""),
        })
    return out


# ── 통합 ────────────────────────────────────────────────────────────────────
_CONF_RANK = {"high": 3, "medium": 2, "low": 1}


def generate(monthly_panel: pd.DataFrame, annual_company: pd.DataFrame,
             sku_master: pd.DataFrame) -> pd.DataFrame:
    """전 유형 시그널 생성 → 랭킹된 use_cases DataFrame."""
    ben = _brand_en_map(sku_master)
    cen = {}
    if sku_master is not None and "company_en_official" in sku_master:
        cen = dict(zip(sku_master["company_kr"], sku_master["company_en_official"]))
    elif sku_master is not None and "company_en" in sku_master:
        cen = dict(zip(sku_master["company_kr"], sku_master["company_en"]))
    sigs = (momentum_signals(monthly_panel, ben)
            + new_hit_signals(sku_master, ben)
            + share_shift_signals(monthly_panel, cen)
            + asp_signals(monthly_panel, ben))
    if not sigs:
        return pd.DataFrame(columns=["usecase_type", "entity_kr", "ticker",
                                     "metric", "value", "confidence",
                                     "thesis_ko", "thesis_en"])
    df = pd.DataFrame(sigs)
    df["conf_rank"] = df["confidence"].map(_CONF_RANK).fillna(0)
    df["abs_val"] = df["value"].abs()
    df = df.sort_values(["conf_rank", "abs_val"], ascending=False)
    df.insert(0, "rank", range(1, len(df) + 1))
    return df.drop(columns=["conf_rank", "abs_val"]).reset_index(drop=True)


def narrative(use_cases: pd.DataFrame, sector_label: str = "K-F&B") -> str:
    """use_cases → 자동 생성 내러티브 리포트(markdown)."""
    if use_cases is None or use_cases.empty:
        return f"# {sector_label} Use-Cases\n\n(시그널 없음 — 데이터 기간/규모 확인 필요)\n"
    titles = {"momentum": "📈 모멘텀 (성장 가속/둔화)",
              "new_hit": "🆕 신제품 히트 후보",
              "share_shift": "🔀 점유율 이동",
              "asp_premium": "💰 ASP / 프리미엄화"}
    lines = [f"# {sector_label} Investment Use-Cases (자동 발굴)",
             "", "데이터셋 전수 분석으로 도출한 투자 시그널입니다. "
             "각 시그널은 confidence flag 와 함께 제공됩니다.", ""]
    for utype, title in titles.items():
        sub = use_cases[use_cases["usecase_type"] == utype].head(5)
        if sub.empty:
            continue
        lines.append(f"## {title}")
        for _, r in sub.iterrows():
            lines.append(f"- {r['thesis_ko']}  _(conf: {r['confidence']})_")
        lines.append("")
    lines.append("> 본 시그널은 국내 편의점 POS 기반이며, 수출 모멘텀은 별도 데이터로 "
                 "보완 필요. confidence=low 는 참고용.")
    return "\n".join(lines)
