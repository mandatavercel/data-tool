"""
kfnb_app/insight/alpha.py — 알파 리서치 엔진 (상관·선행성·시그널).

POS 신호를 다각도로 만들어 (1)주가 forward return 과의 lead-lag 상관/패턴,
(2)공시 분기매출 대비 선행성, (3)알파 시그널 랭킹을 도출한다.

순수 pandas — 외부 데이터(주가/공시)는 인자로 주입받는다(테스트 가능).
상관은 Spearman(rank IC) 기본. lag L≥0 = 'POS 신호가 타깃을 L기간 선행'.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

MIN_OBS = 12          # 상관 계산 최소 표본 (소표본 과적합 방지)
SIGNALS = ["sales_yoy", "asp_yoy", "share", "share_mom"]


# ── POS 월별 시그널 패널 (티커×월) ───────────────────────────────────────────
def pos_signal_panel(monthly_panel: pd.DataFrame) -> pd.DataFrame:
    """월별 패널 → 티커×월 POS 시그널 (long: krx_code, ym, signal, value)."""
    p = monthly_panel.copy()
    p = p[p.get("krx_code", "").astype(str) != ""]      # 상장사만
    if p.empty:
        return pd.DataFrame(columns=["krx_code", "ym", "signal", "value"])
    tm = (p.groupby(["krx_code", "ym"])
          .agg(sales=("sales_amt", "sum"), qty=("sales_qty", "sum"))
          .reset_index())
    # 시장 점유율 (월별 전 상장사 합 대비)
    tot = tm.groupby("ym")["sales"].transform("sum")
    tm["share"] = (tm["sales"] / tot * 100)
    tm["asp"] = tm["sales"] / tm["qty"].where(tm["qty"] > 0)
    tm = tm.sort_values(["krx_code", "ym"])
    g = tm.groupby("krx_code", group_keys=False)
    tm["sales_yoy"] = g["sales"].apply(lambda s: s.pct_change(12, fill_method=None) * 100)
    tm["asp_yoy"] = g["asp"].apply(lambda s: s.pct_change(12, fill_method=None) * 100)
    tm["share_mom"] = g["share"].apply(lambda s: s.diff())
    long = tm.melt(id_vars=["krx_code", "ym"], value_vars=SIGNALS,
                   var_name="signal", value_name="value").dropna(subset=["value"])
    return long


# ── lead-lag 상관 ────────────────────────────────────────────────────────────
def leadlag(sig: pd.Series, tgt: pd.Series, max_lag: int = 6,
            method: str = "spearman", min_obs: int | None = None) -> list[tuple]:
    """sig, tgt(동일 기간 인덱스) → [(lag, corr, n)]. lag만큼 타깃을 당겨(미래) 비교."""
    min_obs = MIN_OBS if min_obs is None else min_obs
    sig = sig.sort_index()
    tgt = tgt.sort_index()
    out = []
    for L in range(0, max_lag + 1):
        df = pd.concat([sig, tgt.shift(-L)], axis=1).dropna()
        if len(df) >= min_obs:
            a, b = df.iloc[:, 0], df.iloc[:, 1]
            if a.nunique() < 2 or b.nunique() < 2:   # 상수열 → 상관 무의미
                continue
            # Spearman = 순위 기반 Pearson (scipy 불필요)
            c = (a.rank().corr(b.rank()) if method == "spearman" else a.corr(b))
            if pd.notna(c):
                out.append((L, round(float(c), 3), len(df)))
    return out


def _best(ll: list[tuple]):
    """|corr| 최대 (lag, corr, n). 없으면 None."""
    return max(ll, key=lambda x: abs(x[1])) if ll else None


def _conf(corr: float, n: int) -> str:
    if n >= 24 and abs(corr) >= 0.4:
        return "high"
    if n >= 12 and abs(corr) >= 0.25:
        return "medium"
    return "low"


# ── 통계 유틸: p값(정규근사) + Benjamini-Hochberg FDR ────────────────────────
import math


def _pvalue(r: float, n: int) -> float:
    """상관계수 r, 표본 n → 양측 p값 (t≈정규 근사). scipy 불필요."""
    if n is None or n <= 3 or r is None:
        return 1.0
    r = max(-0.999999, min(0.999999, float(r)))   # |r|≈1 div-by-zero 방지(클립)
    t = abs(r) * math.sqrt((n - 2) / (1 - r * r))
    return max(0.0, min(1.0, 2.0 * (0.5 * math.erfc(t / math.sqrt(2)))))


def _bh_fdr(pvals: list[float]) -> list[float]:
    """Benjamini-Hochberg q값."""
    m = len(pvals)
    if m == 0:
        return []
    order = sorted(range(m), key=lambda i: pvals[i])
    q = [1.0] * m
    prev = 1.0
    for rank in range(m - 1, -1, -1):          # 큰 p부터
        i = order[rank]
        prev = min(prev, pvals[i] * m / (rank + 1))
        q[i] = min(prev, 1.0)
    return q


def _corr_at_lag(sig: pd.Series, tgt: pd.Series, lag: int):
    """고정 lag 에서 Spearman 상관 + 표본수. (out-of-sample 측정용)."""
    df = pd.concat([sig.sort_index(), tgt.sort_index().shift(-lag)], axis=1).dropna()
    if len(df) < MIN_OBS or df.iloc[:, 0].nunique() < 2 or df.iloc[:, 1].nunique() < 2:
        return None, len(df)
    return round(float(df.iloc[:, 0].rank().corr(df.iloc[:, 1].rank())), 3), len(df)


# ── (1)/(3) 주가 forward return 과의 상관/알파 ──────────────────────────────
_COLS_RET = ["krx_code", "signal", "target", "best_lead_m", "train_corr",
             "oos_corr", "full_corr", "hit_rate", "n_obs", "oos_n",
             "p_value", "q_value_fdr", "significant", "confidence"]


def research_vs_returns(monthly_panel: pd.DataFrame, prices: pd.DataFrame,
                        max_lead: int = 6, train_frac: float = 0.6,
                        fdr_alpha: float = 0.10) -> pd.DataFrame:
    """POS 시그널 × 주가 월수익률 lead-lag — walk-forward OOS + FDR 보정.

    best-lag 는 train 구간에서만 선택(데이터스누핑 차단), OOS corr 는 test 구간에서
    고정 lag 로 측정. p값은 전기간 corr 기준, q값은 Benjamini-Hochberg 보정.
    prices: [krx_code, ym, ret]. ret=월별 수익률(소수).
    """
    sigs = pos_signal_panel(monthly_panel)
    if sigs.empty or prices is None or prices.empty:
        return pd.DataFrame(columns=_COLS_RET)
    ret = prices[["krx_code", "ym", "ret"]].dropna()
    rows = []
    for (code, sgn), sub in sigs.groupby(["krx_code", "signal"]):
        s = sub.set_index("ym")["value"].sort_index()
        r = ret[ret.krx_code == code].set_index("ym")["ret"].sort_index()
        if r.empty or len(s) < MIN_OBS + 4:
            continue
        cut = s.index[int(len(s) * train_frac)]
        s_tr, r_tr = s[s.index <= cut], r[r.index <= cut]
        s_te, r_te = s[s.index > cut], r[r.index > cut]
        best = _best(leadlag(s_tr, r_tr, max_lead))   # lag 선택: train only
        if not best:
            continue
        lag = best[0]
        train_corr = best[1]
        oos_corr, oos_n = _corr_at_lag(s_te, r_te, lag)        # OOS 측정
        full_corr, n = _corr_at_lag(s, r, lag)
        if full_corr is None:
            continue
        aligned = pd.concat([s, r.shift(-lag)], axis=1).dropna()
        hit = float((np.sign(aligned.iloc[:, 0]) == np.sign(aligned.iloc[:, 1])).mean())
        rows.append({"krx_code": code, "signal": sgn, "target": "fwd_stock_return",
                     "best_lead_m": lag, "train_corr": train_corr,
                     "oos_corr": oos_corr, "full_corr": full_corr,
                     "hit_rate": round(hit, 2), "n_obs": n, "oos_n": oos_n,
                     "p_value": round(_pvalue(full_corr, n), 4)})
    if not rows:
        return pd.DataFrame(columns=_COLS_RET)
    df = pd.DataFrame(rows)
    q = _bh_fdr(df["p_value"].tolist())
    df["q_value_fdr"] = [round(x, 4) for x in q]
    df["significant"] = df["q_value_fdr"] < fdr_alpha
    # confidence: FDR 유의 + OOS 일관성까지 봐야 high
    def _c(row):
        if row["significant"] and row["oos_corr"] is not None and \
                abs(row["oos_corr"]) >= 0.2 and (row["oos_n"] or 0) >= 12:
            return "high"
        if row["significant"]:
            return "medium"
        return "low"
    df["confidence"] = df.apply(_c, axis=1)
    return df.reindex(df["full_corr"].abs().sort_values(ascending=False).index
                      ).reset_index(drop=True)[_COLS_RET]


# ── (2) 공시 분기매출 대비 선행성 ────────────────────────────────────────────
def _to_quarter(ym: int) -> int:
    y, m = ym // 100, ym % 100
    return y * 10 + ((m - 1) // 3 + 1)        # 20243 = 2024 Q3


_COLS_REV = ["krx_code", "best_lead_q", "best_lead_m", "corr", "n_obs",
             "p_value", "q_value_fdr", "significant", "confidence"]


def research_vs_revenue(monthly_panel: pd.DataFrame, revenue: pd.DataFrame,
                        max_lag_q: int = 4, min_obs_q: int = 8,
                        fdr_alpha: float = 0.10) -> pd.DataFrame:
    """POS 분기매출 성장 vs 공시 분기매출 성장 lead-lag + FDR. revenue:[krx_code,quarter,revenue].

    quarter 형식 = YYYYQ (예: 20243). 반환: 선행성 레코드(quarters/months).
    분기 표본이 적으므로 OOS 분리는 생략하고 FDR(q값)·유의성으로 보고한다.
    """
    if revenue is None or revenue.empty:
        return pd.DataFrame(columns=_COLS_REV)
    p = monthly_panel.copy()
    p = p[p.get("krx_code", "").astype(str) != ""]
    p["quarter"] = p["ym"].map(_to_quarter)
    posq = (p.groupby(["krx_code", "quarter"])["sales_amt"].sum().reset_index())
    posq["pos_yoy"] = (posq.sort_values("quarter")
                       .groupby("krx_code")["sales_amt"].pct_change(4) * 100)
    rev = revenue.copy()
    rev["rev_yoy"] = (rev.sort_values("quarter")
                      .groupby("krx_code")["revenue"].pct_change(4) * 100)
    rows = []
    for code, sub in posq.groupby("krx_code"):
        s = sub.set_index("quarter")["pos_yoy"].dropna()
        rr = rev[rev.krx_code == code].set_index("quarter")["rev_yoy"].dropna()
        if s.empty or rr.empty:
            continue
        best = _best(leadlag(s, rr, max_lag_q, min_obs=min_obs_q))
        if not best:
            continue
        lag, corr, n = best
        rows.append({"krx_code": code, "best_lead_q": lag, "best_lead_m": lag * 3,
                     "corr": corr, "n_obs": n,
                     "p_value": round(_pvalue(corr, n), 4)})
    if not rows:
        return pd.DataFrame(columns=_COLS_REV)
    df = pd.DataFrame(rows)
    q = _bh_fdr(df["p_value"].tolist())
    df["q_value_fdr"] = [round(x, 4) for x in q]
    df["significant"] = df["q_value_fdr"] < fdr_alpha
    df["confidence"] = df.apply(
        lambda r: "high" if (r["significant"] and r["n_obs"] >= 12)
        else ("medium" if r["significant"] else "low"), axis=1)
    return df[_COLS_REV]


# ── 통합 리포트 ──────────────────────────────────────────────────────────────
def alpha_report(vs_ret: pd.DataFrame, vs_rev: pd.DataFrame,
                 code_to_name: dict | None = None,
                 sector_label: str = "K-F&B") -> str:
    code_to_name = code_to_name or {}
    sig_ko = {"sales_yoy": "매출 YoY", "asp_yoy": "ASP YoY",
              "share": "점유율", "share_mom": "점유율 MoM"}
    n_sig = int(vs_ret["significant"].sum()) if (vs_ret is not None and not vs_ret.empty) else 0
    lines = [f"# {sector_label} Alpha Research (walk-forward OOS + FDR)", "",
             "POS 신호 vs 주가/공시매출의 lead-lag. best-lag 는 **train 구간에서만** 선택하고 "
             "(데이터스누핑 차단), **OOS corr** 는 test 구간에서 측정합니다. p값은 다중가설 "
             "**Benjamini-Hochberg(FDR)** 로 q값 보정하며 q<0.10 을 유의로 봅니다.", "",
             f"> 📌 주가 시그널 중 FDR 유의 = **{n_sig}건**. 유의하지 않으면 노이즈로 간주하세요. "
             "상관≠인과이며 거래비용·capacity 검증은 별도입니다.", ""]
    lines.append("## 📈 주가 (POS 신호 → 향후 주가수익률, train→OOS)")
    if vs_ret is None or vs_ret.empty:
        lines.append("- (주가 데이터 없음 — pykrx/yfinance 환경에서 실행 필요)")
    else:
        for _, r in vs_ret.head(10).iterrows():
            nm = code_to_name.get(r["krx_code"], r["krx_code"])
            oos = "n/a" if r["oos_corr"] is None else f"{r['oos_corr']:+.2f}"
            star = " ✅유의" if r["significant"] else ""
            lines.append(
                f"- {nm} · {sig_ko.get(r['signal'], r['signal'])}: "
                f"full {r['full_corr']:+.2f} / OOS {oos}, 선행 {int(r['best_lead_m'])}개월, "
                f"q={r['q_value_fdr']:.2f} (n={r['n_obs']}, {r['confidence']}){star}")
    lines.append("")
    lines.append("## ⏱ 공시매출 선행성 (POS 분기매출 → 공시 분기매출, FDR 보정)")
    if vs_rev is None or vs_rev.empty:
        lines.append("- (공시매출 없음 — DART_API_KEY 환경에서 실행 필요)")
    else:
        for _, r in vs_rev.iterrows():
            nm = code_to_name.get(r["krx_code"], r["krx_code"])
            star = " ✅유의" if r["significant"] else ""
            lines.append(
                f"- {nm}: POS가 공시매출을 약 {int(r['best_lead_m'])}개월 선행 "
                f"(corr {r['corr']:+.2f}, q={r['q_value_fdr']:.2f}, n={r['n_obs']}, "
                f"{r['confidence']}){star}")
    lines.append("")
    lines.append("> 국내 POS는 수출 비중 큰 종목의 실적을 과소반영할 수 있음. "
                 "OOS corr 의 부호가 full 과 다르면 불안정 신호로 해석하세요.")
    return "\n".join(lines)
