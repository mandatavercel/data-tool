"""Market Signal — 거래데이터를 시장 기대 변화의 선행 신호로 분석.

단순 주가 상관이 아니라:
  - 가격(OHLCV) + 거래량 동시 활용
  - 다중 빈도 (Daily/Weekly/Monthly) lag scan
  - Volume signal (거래량 급증 / 매출-거래량 상관)
  - Event study (POS 급증 → 주가 반응)
  - Rolling correlation (신호 지속성)
  - 자동 해석 (시장 기대 변화 가능성, 신호 강도, 약신호 진단)
"""
import re as _re
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import streamlit as st

from modules.common.foundation import _parse_dates
from modules.analysis.guides import render_guide
from modules.common.core.audit import (
    compute_module_audit, check_growth_sanity,
    check_sample_size_sanity, check_correlation_sanity,
)
from modules.common.core.metrics import calculate_correlation
from modules.common.core.result import enrich_result


# ── Config ────────────────────────────────────────────────────────────────────

_PRICE_MAX_WORKERS = 6

# 빈도별 lag 범위 / rolling window / event horizon
_FREQ_CONFIG = {
    # lag_max는 한국 분기 공시 cycle을 모두 포함하도록 설정:
    #   분기보고서 deadline 약 45일, 4분기 cycle ≈ 52주
    #   POS leads 가능성 + 정정공시·연간 공시까지 고려해 충분한 horizon 확보
    "Daily":   {"pd_freq": "D",   "label": "일",  "lag_max": 90,  "rolling": (30, 60),  "event_horizons": (5, 10, 20, 60)},
    "Weekly":  {"pd_freq": "W",   "label": "주",  "lag_max": 52,  "rolling": (8, 26),   "event_horizons": (1, 2, 4, 12)},
    "Monthly": {"pd_freq": "ME",  "label": "월",  "lag_max": 24,  "rolling": (6, 12),   "event_horizons": (1, 2, 3, 6)},
}
_DEFAULT_FREQ      = "Weekly"
_EVENT_TOP_PCT     = 10       # 상위 10% POS 성장 = 이벤트
_VOL_SPIKE_PCT     = 50       # 거래량 +50% = spike


# ── 종목코드 정규화 ───────────────────────────────────────────────────────────

def _to_krx_code(val: str) -> str | None:
    """ISIN/A코드/숫자 → 6자리 KRX 종목코드. 실패 시 None."""
    s = str(val).strip().replace("-", "").replace(" ", "")
    m = _re.match(r"^KR[0-9A-Z](\d{6})", s, _re.IGNORECASE)
    if m: return m.group(1)
    m = _re.match(r"^A(\d{6})$", s)
    if m: return m.group(1)
    digits = _re.sub(r"\D", "", s)
    if len(digits) == 10: return digits[1:7]
    if len(digits) == 6:  return digits
    return None


# ── 데이터 fetching ──────────────────────────────────────────────────────────

def _normalize_ohlcv(hist: pd.DataFrame, ticker_used: str) -> pd.DataFrame:
    """yfinance 결과 → 표준 OHLCV. multi-index 평탄화 + 컬럼 통일.

    yfinance 1.3.0 호환: 컬럼명이 lowercase일 수도 있고, index가 'Date'가 아니라 'datetime'일 수도 있음.
    """
    if hist is None or hist.empty:
        return pd.DataFrame()
    hist = hist.reset_index()
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = [c[0] if isinstance(c, tuple) else c for c in hist.columns]

    # case-insensitive lookup
    col_map = {c.lower(): c for c in hist.columns}

    def _col(*candidates):
        for c in candidates:
            if c.lower() in col_map:
                return col_map[c.lower()]
        return None

    date_col = _col("Date", "Datetime", "date", "datetime", "index")
    close_col = _col("Close", "close", "adj_close", "Adj Close")
    if date_col is None or close_col is None:
        return pd.DataFrame()

    open_col   = _col("Open",  "open")  or close_col
    high_col   = _col("High",  "high")  or close_col
    low_col    = _col("Low",   "low")   or close_col
    adj_col    = _col("Adj Close", "adj_close", "adjclose") or close_col
    volume_col = _col("Volume", "volume")

    df = pd.DataFrame({
        "date":      pd.to_datetime(hist[date_col]),
        "open":      pd.to_numeric(hist[open_col],  errors="coerce"),
        "high":      pd.to_numeric(hist[high_col],  errors="coerce"),
        "low":       pd.to_numeric(hist[low_col],   errors="coerce"),
        "close":     pd.to_numeric(hist[close_col], errors="coerce"),
        "adj_close": pd.to_numeric(hist[adj_col],   errors="coerce"),
        "volume":    pd.to_numeric(hist[volume_col], errors="coerce") if volume_col else 0.0,
    })
    df = df.dropna(subset=["close"])
    if df.empty:
        return pd.DataFrame()
    df["stock_return"] = df["adj_close"].pct_change() * 100
    df["vol_change"]   = df["volume"].pct_change() * 100
    df["ticker_used"]  = ticker_used
    return df


@st.cache_data(ttl=21600, show_spinner=False)
def _fetch_daily_ohlcv(ticker: str, start: str, end: str) -> pd.DataFrame:
    """yfinance OHLCV 일별 데이터. 6시간 캐시. 4경로 fallback."""
    df, _err = _fetch_daily_ohlcv_with_error(ticker, start, end)
    return df


def _fetch_daily_ohlcv_with_error(ticker: str, start: str, end: str) -> tuple[pd.DataFrame, str]:
    """디버그용 — fetch 실패 원인까지 함께 반환. (캐시 안 됨)

    yfinance 1.3.0 호환:
      - period= 기반 호출이 가장 안정적
      - end가 미래면 yfinance가 빈 응답을 던짐 → today로 cap
    """
    try:
        import yfinance as yf
    except ImportError:
        return pd.DataFrame(), "yfinance 미설치"

    # 미래 end 방지 — yfinance 1.3.0이 미래 end에서 빈 응답
    today_str = pd.Timestamp.today().strftime("%Y-%m-%d")
    end_capped = min(end, today_str)
    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end_capped)

    candidates = (
        [f"{ticker}.KS", f"{ticker}.KQ"]
        if not any(c.isalpha() for c in ticker)
        else [ticker]
    )

    errors: list[str] = []
    for t in candidates:
        # 1) period="max" + slice — yfinance 1.3.0에서 가장 안정적
        try:
            tk = yf.Ticker(t)
            hist = tk.history(period="max", auto_adjust=False)
            df = _normalize_ohlcv(hist, t)
            if not df.empty:
                # tz-aware → naive 정렬 후 사용자 범위로 자름
                df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
                sliced = df[(df["date"] >= start_ts) & (df["date"] <= end_ts)]
                if not sliced.empty:
                    return sliced.reset_index(drop=True), ""
                errors.append(f"{t} period='max': 데이터 있으나 요청 범위 밖")
            else:
                errors.append(f"{t} period='max': empty")
        except Exception as e:
            errors.append(f"{t} period='max': {type(e).__name__}: {str(e)[:60]}")

        # 2) Ticker.history(start, end) (capped)
        for auto_adj in (False, True):
            try:
                tk = yf.Ticker(t)
                hist = tk.history(start=start, end=end_capped, auto_adjust=auto_adj)
                df = _normalize_ohlcv(hist, t)
                if not df.empty:
                    return df, ""
                errors.append(f"{t} Ticker.history(start={start},end={end_capped},aa={auto_adj}): empty")
            except Exception as e:
                errors.append(f"{t} Ticker.history: {type(e).__name__}: {str(e)[:60]}")

        # 3) yf.download(start, end) (capped)
        for auto_adj in (False, True):
            try:
                hist = yf.download(
                    t, start=start, end=end_capped,
                    progress=False, auto_adjust=auto_adj, threads=False,
                )
                df = _normalize_ohlcv(hist, t)
                if not df.empty:
                    return df, ""
                errors.append(f"{t} yf.download(aa={auto_adj}): empty")
            except Exception as e:
                errors.append(f"{t} yf.download: {type(e).__name__}: {str(e)[:60]}")

    return pd.DataFrame(), " || ".join(errors[:8])


@st.cache_data(ttl=21600, show_spinner=False)
def _fetch_benchmark_daily(start: str, end: str) -> pd.DataFrame:
    """KOSPI 인덱스 daily OHLCV. universe 분석에서 'stock'으로 사용 가능.

    Returns columns: same shape as _fetch_daily_ohlcv (open/high/low/close/adj_close/
                     volume/stock_return/vol_change/ticker_used)
    """
    try:
        import yfinance as yf
    except ImportError:
        return pd.DataFrame()
    for bt in ["^KS11", "^KQ11"]:
        try:
            bh = yf.download(bt, start=start, end=end, progress=False, auto_adjust=False)
            if bh.empty:
                continue
            bh = bh.reset_index()
            if isinstance(bh.columns, pd.MultiIndex):
                bh.columns = [c[0] if isinstance(c, tuple) else c for c in bh.columns]
            df = pd.DataFrame({
                "date":      pd.to_datetime(bh["Date"]),
                "open":      bh["Open"].astype(float),
                "high":      bh["High"].astype(float),
                "low":       bh["Low"].astype(float),
                "close":     bh["Close"].astype(float),
                "adj_close": bh.get("Adj Close", bh["Close"]).astype(float),
                "volume":    bh.get("Volume", pd.Series(0, index=bh.index)).astype(float),
            })
            df["stock_return"] = df["adj_close"].pct_change() * 100
            df["vol_change"]   = df["volume"].pct_change() * 100
            df["ticker_used"]  = bt
            return df
        except Exception:
            continue
    return pd.DataFrame()


# ── 집계 helpers ─────────────────────────────────────────────────────────────

def _aggregate_sales_to_freq(daily_sales: pd.DataFrame, pd_freq: str, has_tx: bool) -> pd.DataFrame:
    """daily_sales[date, sales, tx_count?] → period 단위 집계 + growth 계산."""
    agg_spec = {"sales": "sum"}
    if has_tx:
        agg_spec["tx_count"] = "sum"
    g = (
        daily_sales.set_index("date")
        .resample(pd_freq).agg(agg_spec)
        .reset_index()
    )
    g["period"] = g["date"].dt.to_period(pd_freq if pd_freq != "ME" else "M")
    g["sales_growth"] = g["sales"].pct_change() * 100
    if has_tx:
        g["tx_growth"] = g["tx_count"].pct_change() * 100
    return g.dropna(subset=["sales"]).reset_index(drop=True)


def _aggregate_ohlcv_to_freq(daily_ohlcv: pd.DataFrame, pd_freq: str) -> pd.DataFrame:
    """daily OHLCV → period 단위 OHLCV + return + volume change.

    Daily는 그대로 반환. Weekly/Monthly는 OHLC 표준 집계.
    """
    if pd_freq == "D":
        out = daily_ohlcv.copy()
        out["period"] = out["date"].dt.to_period("D")
        return out
    g = (
        daily_ohlcv.set_index("date")
        .resample(pd_freq)
        .agg({
            "open":      "first",
            "high":      "max",
            "low":       "min",
            "close":     "last",
            "adj_close": "last",
            "volume":    "sum",
        })
        .reset_index()
    )
    g["period"]       = g["date"].dt.to_period(pd_freq if pd_freq != "ME" else "M")
    g["stock_return"] = g["adj_close"].pct_change() * 100
    g["vol_change"]   = g["volume"].pct_change() * 100
    return g.dropna(subset=["close"]).reset_index(drop=True)


# ── Signal 계산 함수들 ────────────────────────────────────────────────────────

def _compute_lag_corrs(x: pd.Series, y: pd.Series, max_lag: int) -> dict[int, float]:
    """x가 y를 lag만큼 선행한다고 가정한 Pearson r dict. lag 0~max_lag."""
    out = {}
    for lag in range(0, max_lag + 1):
        if lag == 0:
            sx, sy = x, y
        else:
            sx = x.iloc[:-lag].reset_index(drop=True)
            sy = y.iloc[lag:].reset_index(drop=True)
        r = calculate_correlation(sx, sy)
        if not math.isnan(r):
            out[lag] = round(float(r), 4)
    return out


def _compute_rolling_corr(merged: pd.DataFrame, x_col: str, y_col: str,
                          windows: tuple[int, int]) -> pd.DataFrame:
    """Rolling Pearson r 계산 — 2개 윈도우."""
    short_w, long_w = windows
    out = pd.DataFrame({"period": merged["period"]})
    out[f"roll_{short_w}"] = merged[x_col].rolling(short_w).corr(merged[y_col])
    out[f"roll_{long_w}"]  = merged[x_col].rolling(long_w).corr(merged[y_col])
    return out


def _compute_event_study(merged: pd.DataFrame, event_col: str, ret_col: str,
                         top_pct: float, horizons: tuple) -> dict:
    """이벤트(상위 top_pct%) 발생 후 horizon 단위 누적 수익률 평균.

    Returns: {n_events, horizons: {h: avg_return}, event_periods: list}
    """
    if event_col not in merged.columns or ret_col not in merged.columns:
        return {"n_events": 0, "horizons": {}, "event_periods": []}
    series = merged[event_col].dropna()
    if len(series) < 5:
        return {"n_events": 0, "horizons": {}, "event_periods": []}
    threshold = float(np.percentile(series.values, 100 - top_pct))
    event_idx = merged.index[merged[event_col] >= threshold].tolist()

    horizon_returns = {h: [] for h in horizons}
    for idx in event_idx:
        for h in horizons:
            if idx + h < len(merged):
                fwd = merged[ret_col].iloc[idx + 1 : idx + h + 1].dropna()
                if len(fwd):
                    cum = ((1 + fwd / 100).prod() - 1) * 100
                    horizon_returns[h].append(float(cum))

    return {
        "n_events":  len(event_idx),
        "threshold": round(threshold, 2),
        "horizons":  {h: (round(float(np.mean(v)), 2) if v else None)
                      for h, v in horizon_returns.items()},
        "event_periods": [str(merged["period"].iloc[i]) for i in event_idx],
    }


def _compute_volume_signal(merged: pd.DataFrame) -> dict:
    """매출 성장률 vs 거래량 변화 상관 + spike 카운트."""
    if "vol_change" not in merged.columns or "sales_growth" not in merged.columns:
        return {"corr": None, "n_spikes": 0}
    r = calculate_correlation(merged["sales_growth"], merged["vol_change"])
    n_spikes = int((merged["vol_change"] > _VOL_SPIKE_PCT).sum())
    return {
        "corr":      round(float(r), 3) if not math.isnan(r) else None,
        "n_spikes":  n_spikes,
    }


def _compute_signal_quality(
    merged: pd.DataFrame, best_lag: int, max_corr: float,
    rolling_df: pd.DataFrame, x_col: str, y_col: str,
) -> dict:
    """Hit rate / Persistence / Stability 계산."""
    # Hit rate: lag 적용 후 부호 일치율
    if best_lag > 0:
        sl = merged[x_col].iloc[:-best_lag].reset_index(drop=True)
        sk = merged[y_col].iloc[best_lag:].reset_index(drop=True)
    else:
        sl = merged[x_col].reset_index(drop=True)
        sk = merged[y_col].reset_index(drop=True)
    mask = sl.notna() & sk.notna()
    hit_rate = (
        round((np.sign(sl[mask]) == np.sign(sk[mask])).mean() * 100, 1)
        if mask.sum() > 0 else None
    )

    # Persistence: 짧은 윈도우 rolling이 0.2 초과한 기간 비율
    short_col = [c for c in rolling_df.columns if c.startswith("roll_")][0]
    rs = rolling_df[short_col].dropna()
    persistence = round(float((rs > 0.2).mean()) * 100, 1) if len(rs) > 0 else None

    # Stability: rolling 표준편차의 역수 (낮은 변동성 = 안정적)
    stability = round(float(1.0 / (1.0 + rs.std())), 3) if len(rs) > 1 and rs.std() > 0 else (1.0 if len(rs) > 0 else None)

    # Sample size
    sample_size = int(mask.sum())

    return {
        "hit_rate":    hit_rate,
        "persistence": persistence,
        "stability":   stability,
        "sample_size": sample_size,
    }


def _signals_at_freq(
    daily_sales: pd.DataFrame, daily_ohlcv: pd.DataFrame, freq_label: str,
) -> dict:
    """단일 빈도에서 모든 signal 분석 수행. Returns dict (status='ok' 또는 'insufficient')."""
    cfg     = _FREQ_CONFIG[freq_label]
    pd_freq = cfg["pd_freq"]
    has_tx  = "tx_count" in daily_sales.columns

    sales_agg = _aggregate_sales_to_freq(daily_sales, pd_freq, has_tx)
    stock_agg = _aggregate_ohlcv_to_freq(daily_ohlcv, pd_freq)

    merged = pd.merge(sales_agg, stock_agg, on="period", how="inner",
                      suffixes=("_s", "")).dropna(subset=["sales_growth", "stock_return"]).reset_index(drop=True)

    if len(merged) < 4:
        return {"status": "insufficient", "n_periods": len(merged), "freq_label": freq_label}

    # Lag correlations
    lag_corrs_sr = _compute_lag_corrs(merged["sales_growth"], merged["stock_return"], cfg["lag_max"])
    if not lag_corrs_sr:
        return {"status": "calc_failed", "freq_label": freq_label}

    best_lag = max(lag_corrs_sr, key=lambda k: abs(lag_corrs_sr[k]))
    max_corr = lag_corrs_sr[best_lag]

    # Volume lag correlations (tx_growth available?)
    lag_corrs_tv = {}
    if has_tx and "tx_growth" in merged.columns and "vol_change" in merged.columns:
        m_tv = merged.dropna(subset=["tx_growth", "vol_change"])
        if len(m_tv) >= 4:
            lag_corrs_tv = _compute_lag_corrs(
                m_tv["tx_growth"].reset_index(drop=True),
                m_tv["vol_change"].reset_index(drop=True),
                cfg["lag_max"],
            )

    # Rolling
    rolling_df = _compute_rolling_corr(merged, "sales_growth", "stock_return", cfg["rolling"])

    # Event studies
    event_sales = _compute_event_study(
        merged, "sales_growth", "stock_return",
        top_pct=_EVENT_TOP_PCT, horizons=cfg["event_horizons"],
    )
    event_volume = (
        _compute_event_study(
            merged, "vol_change", "stock_return",
            top_pct=_EVENT_TOP_PCT, horizons=cfg["event_horizons"],
        )
        if "vol_change" in merged.columns else
        {"n_events": 0, "horizons": {}, "event_periods": []}
    )

    # Volume signal
    vol_signal = _compute_volume_signal(merged)

    # Signal quality
    quality = _compute_signal_quality(
        merged, best_lag, max_corr, rolling_df, "sales_growth", "stock_return",
    )

    # Composite score (0-100): 상관 35% + Hit Rate 25% + Persistence 20% + Volume Corr 10% + Stability 10%
    corr_score = abs(max_corr) * 35
    hit_score  = max(0.0, (quality["hit_rate"] - 50) / 50 * 25) if quality["hit_rate"] is not None else 12.5
    pers_score = (quality["persistence"] / 100) * 20             if quality["persistence"] is not None else 10.0
    vol_score  = min(10.0, abs(vol_signal["corr"] or 0) * 20)
    stab_score = (quality["stability"] or 0.5) * 10
    signal_score = round(min(100.0, corr_score + hit_score + pers_score + vol_score + stab_score), 1)

    grade = ("A" if signal_score >= 70 else "B" if signal_score >= 50
             else "C" if signal_score >= 30 else "D")
    signal_strength = (
        "강함" if abs(max_corr) >= 0.5 else
        "중간" if abs(max_corr) >= 0.3 else "약함"
    )

    return {
        "status":            "ok",
        "freq_label":        freq_label,
        "best_lag":          best_lag,
        "max_corr":          max_corr,
        "lag_corrs":         lag_corrs_sr,
        "lag_corrs_volume":  lag_corrs_tv,
        "rolling_corr":      rolling_df,
        "event_sales":       event_sales,
        "event_volume":      event_volume,
        "volume_signal":     vol_signal,
        "hit_rate":          quality["hit_rate"],
        "persistence":       quality["persistence"],
        "stability":         quality["stability"],
        "sample_size":       quality["sample_size"],
        "signal_score":      signal_score,
        "grade":             grade,
        "signal_strength":   signal_strength,
        "merged":            merged,
    }


# ── Universe orchestration ───────────────────────────────────────────────────

def _compute_universe_signal(
    daily_sales_by_company: dict[str, pd.DataFrame],
    bench_daily: pd.DataFrame,
) -> dict:
    """전체 회사 매출 합계 vs KOSPI 시차 분석. 회사 signal과 같은 구조."""
    if not daily_sales_by_company or bench_daily.empty:
        return {"company": "전체 시장 (Universe)", "ticker": "^KS11",
                "status": "no_data", "signal_score": 0, "grade": "N/A",
                "_signals": {}, "_daily_ohlcv": pd.DataFrame(),
                "_daily_sales": pd.DataFrame(),
                "fail_reason": "벤치마크 데이터 없음"}

    has_tx = any("tx_count" in d.columns for d in daily_sales_by_company.values())
    parts  = []
    for d in daily_sales_by_company.values():
        cols = ["date", "sales"] + (["tx_count"] if "tx_count" in d.columns else [])
        parts.append(d[cols])
    all_d = pd.concat(parts, ignore_index=True)
    agg_spec = {"sales": "sum"}
    if has_tx:
        agg_spec["tx_count"] = "sum"
    universe_sales = (
        all_d.groupby("date", as_index=False).agg(agg_spec).sort_values("date").reset_index(drop=True)
    )

    # 모든 빈도에서 signal 계산
    signals: dict[str, dict] = {}
    for freq_label in _FREQ_CONFIG:
        try:
            signals[freq_label] = _signals_at_freq(universe_sales, bench_daily, freq_label)
        except Exception as e:
            signals[freq_label] = {"status": "calc_failed", "freq_label": freq_label,
                                   "error": str(e)[:80]}

    primary = signals.get(_DEFAULT_FREQ, {})
    if primary.get("status") != "ok":
        for fl in ("Weekly", "Monthly", "Daily"):
            if signals.get(fl, {}).get("status") == "ok":
                primary = signals[fl]
                break

    if primary.get("status") != "ok":
        return {"company": "전체 시장 (Universe)", "ticker": "^KS11",
                "status": "insufficient", "grade": "데이터 부족",
                "_signals": signals, "_daily_ohlcv": bench_daily,
                "_daily_sales": universe_sales,
                "fail_reason": "데이터 부족"}

    return {
        "company":        "전체 시장 (Universe)",
        "ticker":         "^KS11 (KOSPI)",
        "status":         "ok",
        "best_lag":       primary["best_lag"],
        "max_corr":       primary["max_corr"],
        "hit_rate":       primary["hit_rate"],
        "persistence":    primary["persistence"],
        "stability":      primary["stability"],
        "signal_score":   primary["signal_score"],
        "grade":          primary["grade"],
        "signal_strength": primary["signal_strength"],
        "_signals":       signals,
        "_daily_ohlcv":   bench_daily,
        "_daily_sales":   universe_sales,
    }


# ── 회사별 orchestration ─────────────────────────────────────────────────────

def _compute_company_signal(
    company: str, ticker_raw: str | None,
    daily_sales: pd.DataFrame, start: str, end: str,
    bench_daily: pd.DataFrame,
) -> dict:
    """단일 회사: 가격 fetch + 모든 빈도 signal 계산."""
    base = {"company": company, "ticker": ticker_raw or "N/A"}
    empty = {
        **base, "status": "no_ticker", "signal_score": 0, "grade": "N/A",
        "max_corr": None, "best_lag": None, "hit_rate": None,
        "persistence": None, "stability": None, "signal_strength": "N/A",
        "fail_reason": "종목코드 없음 — stock_code/security_code 매핑 필요",
        "_signals": {}, "_daily_ohlcv": pd.DataFrame(), "_daily_sales": daily_sales,
    }
    if not ticker_raw:
        return empty

    daily_ohlcv = _fetch_daily_ohlcv(ticker_raw, start, end)
    if daily_ohlcv.empty:
        return {
            **empty, "status": "no_data", "grade": "데이터 없음",
            "fail_reason": f"yfinance 주가 없음 ({ticker_raw}) — 상장폐지/미상장/ISIN 오류 가능",
        }
    ticker_used = daily_ohlcv["ticker_used"].iloc[0]

    # 모든 빈도에서 signal 계산
    signals: dict[str, dict] = {}
    for freq_label in _FREQ_CONFIG:
        try:
            s = _signals_at_freq(daily_sales, daily_ohlcv, freq_label)
            signals[freq_label] = s
        except Exception as e:
            signals[freq_label] = {"status": "calc_failed", "freq_label": freq_label,
                                   "error": str(e)[:80]}

    # Primary metrics는 default freq에서 추출
    primary = signals.get(_DEFAULT_FREQ, {})
    if primary.get("status") != "ok":
        # default 실패 → 다른 빈도라도 ok 있으면 사용
        for fl in ("Weekly", "Monthly", "Daily"):
            if signals.get(fl, {}).get("status") == "ok":
                primary = signals[fl]
                break

    if primary.get("status") != "ok":
        return {
            **empty, "ticker": ticker_used, "status": "insufficient",
            "grade": "데이터 부족",
            "fail_reason": f"매출·주가 병합 후 데이터 부족 (각 빈도별 최소 4기간 필요)",
            "_daily_ohlcv": daily_ohlcv,
        }

    return {
        "company":          company,
        "ticker":           ticker_used,
        "status":           "ok",
        "best_lag":         primary["best_lag"],
        "max_corr":         primary["max_corr"],
        "hit_rate":         primary["hit_rate"],
        "persistence":      primary["persistence"],
        "stability":        primary["stability"],
        "signal_score":     primary["signal_score"],
        "grade":            primary["grade"],
        "signal_strength":  primary["signal_strength"],
        "_signals":         signals,
        "_daily_ohlcv":     daily_ohlcv,
        "_daily_sales":     daily_sales,
    }

# ── Runner ────────────────────────────────────────────────────────────────────

def run_market_signal(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    sales_col   = role_map.get("sales_amount")
    date_col    = role_map.get("transaction_date")
    tx_col      = role_map.get("number_of_tx")
    # stock_code · security_code 둘 다 후보로 — _to_krx_code()가 변환 성공하는 컬럼 사용
    # (사용자가 stock_code에 13자리 법인등록번호를 잘못 매핑해도 ISIN으로 fallback)
    _stock_candidates = [
        role_map[r] for r in ("stock_code", "security_code")
        if role_map.get(r) and role_map[r] in df.columns
    ]
    stock_col = _stock_candidates[0] if _stock_candidates else None
    company_col = role_map.get("company_name")

    if not sales_col or not date_col:
        return {
            "status":  "failed",
            "message": "transaction_date / sales_amount 역할 없음",
            "data":    None,
            "metrics": {},
        }

    n_original = len(df)
    df = df.copy()
    df["_sales"] = pd.to_numeric(df[sales_col], errors="coerce")
    df["_date"]  = _parse_dates(df[date_col])
    if tx_col:
        df["_tx"] = pd.to_numeric(df[tx_col], errors="coerce")
    n_valid   = int((df["_sales"].notna() & df["_date"].notna()).sum())
    _date_min = str(df["_date"].dropna().min().date()) if n_valid > 0 else None
    _date_max = str(df["_date"].dropna().max().date()) if n_valid > 0 else None

    warnings: list[str] = []

    # 회사별 daily 매출(+거래건수) 집계
    daily_sales_by_company: dict[str, pd.DataFrame] = {}
    company_ticker_map:     dict[str, str]          = {}

    if company_col and company_col in df.columns:
        for company, grp in df.groupby(company_col):
            g = grp.dropna(subset=["_date"]).copy()
            # 날짜만 (시간 제거) — pandas 버전 무관하게 동작
            g["date"] = pd.to_datetime(g["_date"].dt.date)
            agg_spec = {"_sales": "sum"}
            if tx_col:
                agg_spec["_tx"] = "sum"
            d = (
                g.groupby("date", as_index=False)
                .agg(agg_spec)
                .rename(columns={"_sales": "sales", "_tx": "tx_count"})
                .sort_values("date")
                .reset_index(drop=True)
            )
            daily_sales_by_company[str(company)] = d

        # 후보 컬럼들을 우선순위대로 시도 — stock_code 먼저, 그래도 안 되면 security_code
        # (사용자가 stock_code에 13자리 법인등록번호를 매핑하는 경우 ISIN으로 fallback)
        if _stock_candidates:
            for company, grp in df.groupby(company_col):
                code_found = None
                for cand_col in _stock_candidates:
                    if cand_col not in grp.columns:
                        continue
                    for val in grp[cand_col].dropna().unique():
                        code = _to_krx_code(str(val))
                        if code:
                            code_found = code
                            break
                        elif "." in str(val):
                            # 이미 yfinance ticker 형식 (예: 005930.KS)
                            code_found = str(val).strip()
                            break
                    if code_found:
                        break
                if code_found:
                    company_ticker_map[str(company)] = code_found

    # 전체 monthly (report.py backwards-compat용)
    df["_ym"] = df["_date"].dt.to_period("M")
    monthly = (
        df.groupby("_ym", as_index=False)["_sales"]
        .sum()
        .rename(columns={"_ym": "period", "_sales": "sales"})
        .sort_values("period")
    )
    monthly["period_str"] = monthly["period"].astype(str)
    monthly["sales_mom"]  = monthly["sales"].pct_change() * 100

    # 분석 기간
    has_data = not df["_date"].dropna().empty
    if has_data:
        start = str(df["_date"].dropna().min().date())
        end   = str(df["_date"].dropna().max().date())
    else:
        start = end = ""

    if not _stock_candidates:
        warnings.append("stock_code/security_code 역할 없음 — 주가 연동 불가")
    elif company_col and not company_ticker_map:
        # 컬럼은 있는데 변환 가능한 종목코드가 하나도 없음 → 가장 흔한 원인 진단
        sample_vals = []
        for cand_col in _stock_candidates:
            try:
                sample_vals.extend(
                    [str(v) for v in df[cand_col].dropna().unique()[:2]]
                )
            except Exception:
                pass
        cand_names = ", ".join(_stock_candidates)
        sample_str = ", ".join(sample_vals[:4]) if sample_vals else "—"
        warnings.append(
            f"매핑된 컬럼({cand_names})에서 KRX 6자리 종목코드를 추출하지 못했습니다. "
            f"샘플값: {sample_str} — ISIN(KR+10자리) 또는 6자리 숫자 형식이어야 합니다. "
            f"법인등록번호(13자리)는 종목코드가 아닙니다."
        )
    if not tx_col:
        warnings.append("number_of_tx 역할 없음 — 거래량 동조 분석 제한")

    # Benchmark
    bench_daily = _fetch_benchmark_daily(start, end) if (has_data and stock_col) else pd.DataFrame()

    # 회사별 signal 병렬 계산
    company_signals: list[dict] = []
    if daily_sales_by_company and stock_col and has_data:
        n_total   = len(daily_sales_by_company)
        n_workers = min(_PRICE_MAX_WORKERS, max(1, n_total))
        with st.spinner(
            f"주가 OHLCV 수집 + 다중 빈도 signal 분석 중... (회사 {n_total}개 · {n_workers}개 동시)"
        ):
            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                futures = [
                    ex.submit(
                        _compute_company_signal,
                        company, company_ticker_map.get(company),
                        daily_sales_by_company[company], start, end, bench_daily,
                    )
                    for company in daily_sales_by_company
                ]
                for fut in as_completed(futures):
                    company_signals.append(fut.result())

    # Universe (전체 시장) signal — 회사 합계 vs KOSPI
    universe_signal = (
        _compute_universe_signal(daily_sales_by_company, bench_daily)
        if (has_data and not bench_daily.empty and daily_sales_by_company)
        else {}
    )

    ok_sigs   = [s for s in company_signals if s.get("status") == "ok"]
    has_stock = len(ok_sigs) > 0

    if ok_sigs:
        top         = max(ok_sigs, key=lambda x: x.get("signal_score", 0))
        ticker_used = top["ticker"]
        best_lag    = top["best_lag"]
        best_corr   = top["max_corr"]
    else:
        ticker_used = "(없음)"
        best_lag    = -1
        best_corr   = 0.0

    n_months = int(len(monthly))
    metrics = {
        "ticker":             ticker_used,
        "best_lag":           best_lag,
        "best_corr":          round(float(best_corr), 3),
        "has_stock":          has_stock,
        "n_months":           n_months,
        "n_companies_total":  len(company_signals),
        "n_companies_ok":     len(ok_sigs),
        "default_freq":       _DEFAULT_FREQ,
        "has_tx":             bool(tx_col),
    }

    status  = "warning" if warnings else "success"
    if has_stock:
        message = (
            f"{len(ok_sigs)}/{len(company_signals)}개사 분석 완료 · "
            f"최강 신호: {top['company'][:14]} (lag {best_lag}, r={best_corr:.2f})"
        )
    else:
        message = " | ".join(warnings) if warnings else f"{n_months}개월 매출 집계 완료"

    bs  = check_sample_size_sanity(n_months, min_required=12)
    if has_stock:
        bs += check_correlation_sanity(best_corr, n_months)
    bs += check_growth_sanity(monthly["sales_mom"].dropna() if "sales_mom" in monthly.columns else None)

    audit, conf = compute_module_audit(
        n_original=n_original, n_valid=n_valid,
        role_map=role_map,
        used_roles=["transaction_date", "sales_amount", "number_of_tx", "stock_code", "company_name"],
        date_min=_date_min, date_max=_date_max,
        formula="POS 성장률 × 주가 수익률 × 거래량 변화 다중 빈도(D/W/M) lag 상관 + Event Study + Rolling",
        agg_unit=_DEFAULT_FREQ.lower(),
        n_computable=n_months, n_periods=n_months,
        business_checks=bs,
    )

    return enrich_result({
        "status":              status,
        "message":             message,
        "data":                monthly,
        "metrics":             metrics,
        "_bench_df":           bench_daily,
        "_daily_sales":        daily_sales_by_company,
        "_company_signals":    company_signals,
        "_company_ticker_map": company_ticker_map,
        "_universe_signal":    universe_signal,
    }, audit, conf)


# ── Renderer ──────────────────────────────────────────────────────────────────

_GRADE_ICON  = {"A": "🟢 A", "B": "🟡 B", "C": "🟠 C", "D": "🔴 D", "N/A": "⚪ —"}
_GRADE_COLOR = {"A": "#16a34a", "B": "#84cc16", "C": "#f59e0b", "D": "#dc2626"}
_FAIL_LABEL  = {
    "no_ticker":    "❌ 종목코드 없음",
    "no_data":      "⚠️ 주가 없음",
    "insufficient": "🔸 데이터 부족",
    "calc_failed":  "🔴 계산 실패",
}


def _rebase_to_index(values: pd.Series, base: float = 100.0) -> pd.Series:
    """첫 번째 유효값을 base로 정규화 (주가/매출 비교용)."""
    first = values.dropna().iloc[0] if not values.dropna().empty else 1.0
    if first == 0:
        return values
    return values / first * base


def _t_stat(r: float, n: int) -> float:
    """Pearson r의 t-statistic. n=sample size."""
    if n <= 2 or abs(r) >= 1.0:
        return 0.0
    return float(r) * math.sqrt(n - 2) / math.sqrt(1 - r * r)


def _compute_overall_summary(ok_sigs: list[dict], freq_label: str) -> dict:
    """선택한 빈도에서 전체 회사들의 종합 시그널 통계 (퀀트 표준 지표)."""
    valid = []
    for s in ok_sigs:
        sf = s.get("_signals", {}).get(freq_label, {})
        if sf.get("status") == "ok":
            valid.append((s, sf))
    if not valid:
        return {"n_valid": 0}

    grades = [sf.get("grade", "D") for _, sf in valid]
    grade_counts = {g: grades.count(g) for g in ("A", "B", "C", "D")}
    n_strong = grade_counts["A"] + grade_counts["B"]

    best_lags = [sf["best_lag"] for _, sf in valid if sf.get("best_lag") is not None]
    raw_corrs = [sf["max_corr"] for _, sf in valid if sf.get("max_corr") is not None]
    abs_corrs = [abs(r) for r in raw_corrs]
    hit_rates = [sf["hit_rate"] for _, sf in valid if sf.get("hit_rate") is not None]

    # Volume confirmation: |volume corr| >= 0.3 인 회사 비율
    vol_conf = sum(
        1 for _, sf in valid
        if (sf.get("volume_signal") or {}).get("corr") is not None
        and abs((sf["volume_signal"]["corr"] or 0)) >= 0.3
    )

    # IC stats: 표본 회사 corr 평균/표준편차/IR (Information Ratio)
    avg_ic     = float(np.mean(abs_corrs)) if abs_corrs else 0.0
    std_ic     = float(np.std(abs_corrs))  if len(abs_corrs) > 1 else 0.0
    ir         = (avg_ic / std_ic) if std_ic > 1e-9 else 0.0
    avg_t_stat = float(np.mean([abs(_t_stat(r, sf.get("sample_size", 0)))
                                for (_, sf), r in zip(valid, raw_corrs)])) if raw_corrs else 0.0

    # Lag profile: 전체 회사의 모든 lag 평균 |r|
    cfg = _FREQ_CONFIG[freq_label]
    lag_profile = {}
    for lag in range(0, cfg["lag_max"] + 1):
        rs = [sf["lag_corrs"].get(lag) for _, sf in valid if lag in sf.get("lag_corrs", {})]
        if rs:
            lag_profile[lag] = float(np.mean([abs(r) for r in rs]))

    # Long/Short 후보: 양수/음수 r 정렬
    ranked_by_signed = sorted(valid, key=lambda x: x[1].get("max_corr") or 0, reverse=True)
    longs = [{
        "company":  s["company"], "ticker": s["ticker"],
        "r":        sf.get("max_corr"),
        "lag":      sf.get("best_lag"),
        "t":        _t_stat(sf.get("max_corr") or 0, sf.get("sample_size", 0)),
        "hit":      sf.get("hit_rate"),
        "score":    sf.get("signal_score", 0),
        "grade":    sf.get("grade", "D"),
    } for s, sf in ranked_by_signed[:5]
        if (sf.get("max_corr") or 0) > 0]
    shorts = [{
        "company":  s["company"], "ticker": s["ticker"],
        "r":        sf.get("max_corr"),
        "lag":      sf.get("best_lag"),
        "t":        _t_stat(sf.get("max_corr") or 0, sf.get("sample_size", 0)),
        "hit":      sf.get("hit_rate"),
        "score":    sf.get("signal_score", 0),
        "grade":    sf.get("grade", "D"),
    } for s, sf in reversed(ranked_by_signed[-5:])
        if (sf.get("max_corr") or 0) < 0]

    return {
        "n_valid":       len(valid),
        "grade_counts":  grade_counts,
        "n_strong":      n_strong,
        "avg_lag":       float(np.mean(best_lags))   if best_lags else None,
        "median_lag":    float(np.median(best_lags)) if best_lags else None,
        "avg_ic":        avg_ic,
        "std_ic":        std_ic,
        "ir":            ir,
        "avg_t_stat":    avg_t_stat,
        "avg_hit":       float(np.mean(hit_rates))   if hit_rates else None,
        "vol_conf_pct":  round(vol_conf / len(valid) * 100, 1),
        "lag_profile":   lag_profile,
        "longs":         longs,
        "shorts":        shorts,
        "best_lags":     best_lags,
        "valid_pairs":   valid,  # for downstream filtering
    }


def _render_data_audit(result: dict, freq_label: str, ok_sigs: list, failed: list):
    """주가 데이터 출처 + ticker 매핑 + 기간 alignment 투명 검증."""
    n_total = len(ok_sigs) + len(failed)
    n_ok    = len(ok_sigs)
    success_rate = (n_ok / n_total * 100) if n_total else 0

    # 헤더 + 요약 한 줄
    st.markdown("### 🔍 주가 데이터 매핑 검증")
    st.caption(
        f"yfinance(Yahoo Finance API)로 KOSPI/KOSDAQ 일별 OHLCV 수집 · "
        f"매핑 성공 {n_ok}/{n_total}개사 ({success_rate:.0f}%)"
    )

    with st.expander("📖 데이터를 어떻게 가져오고 매핑하는지", expanded=False):
        st.markdown(
            """
**1️⃣ 데이터 출처**
- **공급처**: Yahoo Finance API (Python `yfinance` 라이브러리)
- **수집 항목**: Open · High · Low · Close · **Adj Close** · Volume (일별)
- **사용 가격**: `Adj Close` (배당·액면분할 보정된 종가) — 수익률 계산의 표준
- **빈도**: 일별 → 분석에 따라 주별/월별 자동 집계
- **캐시**: 6시간 (같은 종목 재조회 시 즉시 반환)

**2️⃣ POS 데이터 → yfinance ticker 매핑 절차**

```
①  POS 거래 row의 stock_code (또는 security_code) 컬럼
   예: "005930" 또는 "KR7005930003" (ISIN) 또는 "A005930"

②  6자리 KRX 종목코드로 정규화 (_to_krx_code)
   - ISIN: KR로 시작하는 12자리 → 중간 6자리 추출 (KR7005930003 → 005930)
   - A코드: A005930 → 005930
   - 9~10자리 숫자: 왼쪽 6자리 (0182500001 → 018250)
   - 6자리 숫자: 그대로

③  yfinance 형식으로 변환해 호출
   - 005930 → "005930.KS"  (KOSPI 시도)
   - 빈 응답이면 "005930.KQ" (KOSDAQ 재시도)
   - 5경로 fallback: Ticker.history × auto_adjust × yf.download × period='max'

④  성공한 ticker가 결과의 "Ticker" 컬럼에 표시됨 (.KS / .KQ로 어느 시장인지 확인 가능)
```

**3️⃣ 매칭 기간 alignment**
- POS 데이터 첫 거래일 ~ 마지막 거래일을 기준으로 yfinance 호출
- yfinance는 거래일만 반환 (영업일·주말 제외)
- 분석 시 POS 분기(또는 주/월)와 yfinance 분기를 inner join — 두쪽 모두 데이터 있는 시점만 사용

**4️⃣ 실패 케이스**
- **종목코드 없음**: POS에 stock_code/security_code 매핑이 안 된 회사
- **yfinance 빈 응답**: 상장폐지 / 미상장(비상장 자회사) / KOSPI-KOSDAQ 잘못된 매핑
- **데이터 부족**: 매칭 후 4개 미만 분기 → 통계 의미 없음 (skip)
            """
        )

    # ── 회사별 매핑 검증 표 ───────────────────────────────────────────────
    co_ticker_map = result.get("_company_ticker_map", {}) or {}

    audit_rows = []
    for s in ok_sigs:
        co = s.get("company", "")
        ticker_used = s.get("ticker", "")
        sf = s.get("_signals", {}).get(freq_label, {})
        merged = sf.get("merged", pd.DataFrame()) if isinstance(sf, dict) else pd.DataFrame()
        n_periods = len(merged) if not merged.empty else 0
        # raw stock code (POS에 있던 원본 추정 — corp 매핑된 코드 사용)
        raw_code = co_ticker_map.get(co, "—")
        # 시장 식별
        market = "KOSPI" if ticker_used.endswith(".KS") else ("KOSDAQ" if ticker_used.endswith(".KQ") else "—")
        # 기간
        date_range = "—"
        daily = s.get("_daily_ohlcv", pd.DataFrame())
        if not daily.empty and "date" in daily.columns:
            try:
                d_min = pd.to_datetime(daily["date"].min()).strftime("%Y-%m-%d")
                d_max = pd.to_datetime(daily["date"].max()).strftime("%Y-%m-%d")
                date_range = f"{d_min} ~ {d_max} ({len(daily)}일)"
            except Exception:
                pass

        audit_rows.append({
            "Status":        "✅ 성공",
            "회사":           co[:24],
            "POS 종목코드":   raw_code,
            "yfinance ticker": ticker_used,
            "시장":           market,
            "주가 데이터":    date_range,
            f"매칭 {freq_label}": n_periods,
        })
    for s in failed:
        audit_rows.append({
            "Status":        _FAIL_LABEL.get(s.get("status", ""), "❓"),
            "회사":           s.get("company", "")[:24],
            "POS 종목코드":   co_ticker_map.get(s.get("company", ""), "—"),
            "yfinance ticker": s.get("ticker", "—"),
            "시장":           "—",
            "주가 데이터":    "—",
            f"매칭 {freq_label}": 0,
        })

    if audit_rows:
        st.dataframe(pd.DataFrame(audit_rows), hide_index=True, use_container_width=True)
        st.caption(
            f"📌 **POS 종목코드** = POS 데이터에서 추출한 6자리 코드 / "
            f"**yfinance ticker** = 실제 호출에 사용된 ticker (.KS=KOSPI, .KQ=KOSDAQ) / "
            f"**주가 데이터** = yfinance에서 받은 일별 가격의 시작 ~ 종료일 / "
            f"**매칭 {freq_label}** = POS와 주가가 모두 있는 분석 단위 수"
        )

    # ── 시장 분포 + 기간 alignment 시각화 ────────────────────────────────
    if ok_sigs:
        ac1, ac2 = st.columns([1, 2])

        # 시장 분포 (KOSPI/KOSDAQ)
        with ac1:
            ks = sum(1 for s in ok_sigs if s.get("ticker", "").endswith(".KS"))
            kq = sum(1 for s in ok_sigs if s.get("ticker", "").endswith(".KQ"))
            st.markdown(
                "<div style='font-size:13px;font-weight:700;color:#0f172a;margin-bottom:6px'>"
                "거래소 분포</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='background:#f8fafc;border-radius:8px;padding:12px 16px;font-size:14px;line-height:1.9'>"
                f"<div>🏛 <b>KOSPI</b>: {ks}개사</div>"
                f"<div>🏪 <b>KOSDAQ</b>: {kq}개사</div>"
                f"<div style='font-size:11px;color:#94a3b8;margin-top:6px'>"
                f"yfinance가 각 종목코드 뒤에 .KS / .KQ 어느 쪽이 응답했는지로 추정</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

        # 기간 alignment — 회사별 데이터 시작/종료
        with ac2:
            st.markdown(
                "<div style='font-size:13px;font-weight:700;color:#0f172a;margin-bottom:6px'>"
                "회사별 주가 데이터 기간 (POS와 겹치는 부분만 분석에 사용)</div>",
                unsafe_allow_html=True,
            )
            try:
                import plotly.graph_objects as go
                gantt = []
                for s in ok_sigs[:20]:  # 너무 많으면 잘림
                    daily = s.get("_daily_ohlcv", pd.DataFrame())
                    if daily.empty or "date" not in daily.columns:
                        continue
                    d_min = pd.to_datetime(daily["date"].min())
                    d_max = pd.to_datetime(daily["date"].max())
                    gantt.append({
                        "Task":   f"{s.get('company','')[:14]} ({s.get('ticker','')})",
                        "Start":  d_min,
                        "Finish": d_max,
                    })
                if gantt:
                    fig_g = go.Figure()
                    for g in gantt:
                        fig_g.add_trace(go.Scatter(
                            x=[g["Start"], g["Finish"]],
                            y=[g["Task"], g["Task"]],
                            mode="lines",
                            line=dict(color="#1e40af", width=8),
                            hovertemplate=f"<b>{g['Task']}</b><br>"
                                          f"{g['Start'].strftime('%Y-%m-%d')} ~ "
                                          f"{g['Finish'].strftime('%Y-%m-%d')}<extra></extra>",
                            showlegend=False,
                        ))
                    fig_g.update_layout(
                        height=max(220, len(gantt) * 22 + 80),
                        plot_bgcolor="#fff",
                        margin=dict(t=10, b=30, l=10, r=10),
                        xaxis=dict(title="기간", showgrid=True, gridcolor="#e2e8f0",
                                   tickformat="%Y-%m"),
                        yaxis=dict(autorange="reversed"),
                    )
                    st.plotly_chart(fig_g, key="ms_audit_gantt", use_container_width=True)
                    if len(ok_sigs) > 20:
                        st.caption(f"상위 20개사만 표시 (전체 {len(ok_sigs)}개사). 위 표에서 모든 종목 확인.")
            except Exception:
                pass


def _render(result: dict):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    render_guide("market_signal")

    if result["status"] == "failed":
        st.error(result["message"])
        return
    if result["status"] == "warning":
        st.warning(result["message"])

    m = result["metrics"]
    company_signals = result.get("_company_signals", [])

    if not m.get("has_stock"):
        co_ticker = result.get("_company_ticker_map", {}) or {}
        all_sigs  = result.get("_company_signals", []) or []
        msg = result.get("message", "")

        st.error("주가 데이터를 가져오지 못했습니다. 아래 진단을 확인하세요.")

        # ── 단계별 진단 ──────────────────────────────────────────────────
        st.markdown("### 🔬 단계별 진단")
        d1, d2, d3 = st.columns(3)
        d1.metric("① 종목코드 추출", f"{len(co_ticker)}개사",
                  help="POS 회사명별로 KRX 종목코드를 추출한 결과")
        d2.metric("② yfinance 호출", f"{len(all_sigs)}개사",
                  help="추출된 코드로 yfinance API 호출 시도")
        n_ok = sum(1 for s in all_sigs if s.get("status") == "ok")
        d3.metric("③ 주가 응답 OK", f"{n_ok}개사")

        # ── 시나리오별 진단 메시지 ───────────────────────────────────────
        if not co_ticker and "KRX 6자리 종목코드를 추출하지 못했습니다" in msg:
            st.markdown(
                f"<div style='background:#fef3c7;border-left:3px solid #d97706;"
                f"padding:10px 14px;border-radius:6px;margin:6px 0;font-size:13px'>"
                f"⚠️ <b>종목코드 형식 문제</b> — 매핑된 컬럼에서 유효 코드 추출 실패<br>"
                f"{msg.split(' — ')[0] if ' — ' in msg else msg}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                "**👉 해결 방법** (Step 2 — Schema Intelligence로 돌아가서 확인):\n"
                "1. **🆔 ISIN 컬럼**(예: `KR7005930003`)이 있으면 `security_code` 역할로 매핑\n"
                "2. **📈 종목코드 컬럼**(6자리, 예: `005930`)이 있으면 `stock_code` 역할로 매핑\n"
                "3. 법인등록번호(13자리, 예: `1101110057574`)는 종목코드가 아니므로 매핑 해제\n"
                "4. 매핑 후 Step 4(분석 설정)으로 돌아가 Market Signal 재실행"
            )
        elif co_ticker and not all_sigs:
            st.warning(
                f"종목코드 {len(co_ticker)}개 추출은 성공했으나 yfinance 호출 자체가 실행되지 않았습니다. "
                "stock_col 또는 has_data 조건을 확인하세요."
            )
        elif co_ticker and all_sigs:
            st.warning(
                f"{len(co_ticker)}개사 종목코드 추출 OK · yfinance 호출 {len(all_sigs)}회 · "
                f"성공 {n_ok}개. 회사별 실패 사유는 아래 표 참조."
            )

        # ── 회사별 추출 ticker + 실패 사유 표 ────────────────────────────
        if co_ticker or all_sigs:
            with st.expander(f"📋 회사별 추출/호출 상세 ({len(co_ticker) or len(all_sigs)}개)", expanded=True):
                rows = []
                # signals 우선 — 더 상세
                for s in all_sigs:
                    rows.append({
                        "회사":           s.get("company", "")[:24],
                        "추출 ticker":    s.get("ticker", co_ticker.get(s.get("company", ""), "—")),
                        "Status":         s.get("status", "—"),
                        "실패 사유":      s.get("fail_reason", "") or ("정상" if s.get("status") == "ok" else "—"),
                    })
                # signals에 없는 회사 (코드만 추출된 경우)
                signal_companies = {s.get("company") for s in all_sigs}
                for co, tk in co_ticker.items():
                    if co not in signal_companies:
                        rows.append({
                            "회사":         co[:24],
                            "추출 ticker":  tk,
                            "Status":       "(호출 안 됨)",
                            "실패 사유":    "yfinance 호출 미실행",
                        })
                if rows:
                    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

        # ── 가장 흔한 실패 사유 통계 ────────────────────────────────────
        if all_sigs:
            from collections import Counter
            reasons = Counter(s.get("fail_reason", "기타") for s in all_sigs if s.get("status") != "ok")
            if reasons:
                st.markdown("**실패 사유 분포**")
                for reason, cnt in reasons.most_common():
                    short = reason[:80] if reason else "(빈 사유)"
                    st.markdown(f"- `{cnt}개사`  ·  {short}")

        # ── 사용자가 즉시 할 수 있는 액션 ────────────────────────────────
        st.markdown("---")
        st.markdown(
            "**즉시 시도해볼 수 있는 것들**\n"
            "- 매핑이 바뀐 경우: Step 4로 이동 → '결과 초기화' → Market Signal 다시 체크 → 실행\n"
            "- 네트워크 환경: Yahoo Finance 접근 차단 가능성 (회사망/방화벽) → 모바일 핫스팟에서 재시도\n"
            "- ticker가 KOSPI(.KS)도 KOSDAQ(.KQ)도 아닌 경우: 비상장사이거나 상장폐지 종목"
        )
        return

    ok_sigs = [s for s in company_signals if s.get("status") == "ok"]
    failed  = [s for s in company_signals if s.get("status") != "ok"]

    if not ok_sigs:
        st.error("주가 연동에 성공한 회사가 없습니다.")
        if failed:
            with st.expander("실패 원인 보기"):
                fail_rows = [{
                    "회사":     s["company"],
                    "시도 종목": s.get("ticker", "—"),
                    "상태":     _FAIL_LABEL.get(s.get("status", ""), "—"),
                    "원인":     s.get("fail_reason", "—"),
                } for s in failed]
                st.dataframe(pd.DataFrame(fail_rows), hide_index=True, use_container_width=True)
        return

    # ── 빈도 선택 (전체 + 드릴다운에 공통 적용) ─────────────────────────────────
    freq_label = st.radio(
        "분석 빈도", list(_FREQ_CONFIG.keys()),
        index=list(_FREQ_CONFIG.keys()).index(_DEFAULT_FREQ),
        horizontal=True, key="ms_freq_sel",
        help="Daily: 단기 신호 / Weekly: 표준(추천) / Monthly: 장기 추세",
    )
    cfg  = _FREQ_CONFIG[freq_label]
    unit = cfg["label"]

    # ════════════════════════════════════════════════════════════════════════
    # 🎯 메인 차트 — Universe(전체 시장·KOSPI) 또는 회사별 drill-down
    # (가장 중요한 view를 결과 화면 최상단에 배치)
    # ════════════════════════════════════════════════════════════════════════
    universe_sig = result.get("_universe_signal", {})
    has_universe = bool(universe_sig) and universe_sig.get("status") == "ok"

    UNIVERSE_LABEL = "전체 시장 (Universe · KOSPI 대비)"
    company_options = sorted([s["company"] for s in ok_sigs])
    if has_universe:
        all_options = [UNIVERSE_LABEL] + company_options
    else:
        all_options = company_options

    sel_co = st.selectbox("Company / Universe", all_options, key="ms_drill_sel",
                          label_visibility="collapsed")

    if sel_co == UNIVERSE_LABEL:
        sig = universe_sig
    else:
        sig = next((s for s in company_signals if s["company"] == sel_co), None)
    if sig is None or sig.get("status") != "ok":
        st.warning("선택한 항목의 데이터를 찾을 수 없습니다.")
        return
    s_freq = sig["_signals"].get(freq_label)
    if not s_freq or s_freq.get("status") != "ok":
        st.warning(f"{freq_label} 빈도에서 데이터 부족 — 다른 빈도를 선택하세요.")
        return

    merged   = s_freq["merged"]
    co_corr  = s_freq.get("max_corr") or 0
    co_lag   = s_freq.get("best_lag")
    co_hit   = s_freq.get("hit_rate")
    co_n     = s_freq.get("sample_size", 0)
    co_t     = _t_stat(co_corr, co_n)
    co_score = s_freq.get("signal_score", 0)
    co_grade = s_freq.get("grade", "D")
    co_vol   = (s_freq.get("volume_signal") or {}).get("corr")
    co_pers  = s_freq.get("persistence")

    co_sig_color = "#dc2626" if abs(co_corr) < 0.3 else "#d97706" if abs(co_corr) < 0.5 else "#16a34a"
    co_strip_items = [
        ("TICKER",    sig["ticker"],                                "#fff"),
        ("IC",        f"{co_corr:+.3f}",                            co_sig_color),
        ("LAG",       f"{co_lag}{unit}" if co_lag is not None else "—", "#fff"),
        ("|t|",       f"{abs(co_t):.2f}",                           "#fff"),
        ("HIT",       f"{co_hit:.0f}%" if co_hit is not None else "—", "#fff"),
        ("PERSIST",   f"{co_pers:.0f}%" if co_pers is not None else "—", "#fff"),
        ("VOL r",     f"{co_vol:+.3f}" if co_vol is not None else "—", "#fff"),
        ("N",         f"{co_n}",                                    "#fff"),
        ("SCORE",     f"{co_score:.0f} ({co_grade})",               "#fff"),
    ]
    co_strip = "".join(
        f"<div><span style='color:#94a3b8'>{lbl}</span> "
        f"<b style='color:{col}'>{val}</b></div>"
        for lbl, val, col in co_strip_items
    )
    st.markdown(
        f"<div style='font-family:\"SF Mono\",Menlo,Consolas,monospace;font-size:13px;"
        f"background:#0f172a;color:#e2e8f0;padding:10px 16px;border-radius:4px;"
        f"display:flex;flex-wrap:wrap;gap:22px;align-items:center;line-height:1.4;"
        f"margin-bottom:6px'>{co_strip}</div>",
        unsafe_allow_html=True,
    )

    # ── 검은 박스 metric 한 줄 요약 ──────────────────────────────────────────
    _strip_summary = (
        f"<b>{sig['ticker']}</b> 주가와 매출의 시차 상관 — "
    )
    if co_corr is not None:
        _ic_word = "강한" if abs(co_corr) >= 0.5 else "중간" if abs(co_corr) >= 0.3 else "약한"
        _direction = "선행" if co_corr > 0 else "역방향" if co_corr < 0 else ""
        _strip_summary += (
            f"매출이 주가를 <b>{co_lag}{unit}</b> {_direction} (상관 <b>{co_corr:+.2f}</b>, {_ic_word} 신호) · "
        )
    if co_t is not None:
        _t_word = "유의" if abs(co_t) >= 2 else "약함"
        _strip_summary += f"통계적 {_t_word} (|t|={abs(co_t):.1f}) · "
    if co_hit is not None:
        _strip_summary += f"방향 일치 {co_hit:.0f}% · "
    _strip_summary += f"{co_n} 관측치 · 종합 점수 <b>{co_score:.0f}/100 ({co_grade}등급)</b>"

    st.markdown(
        f"<div style='font-size:12.5px;color:#475569;background:#f8fafc;"
        f"border-left:3px solid #94a3b8;padding:7px 12px;border-radius:4px;"
        f"line-height:1.55;margin-bottom:10px'>"
        f"💡 {_strip_summary}</div>",
        unsafe_allow_html=True,
    )

    with st.expander("📖 각 metric 이게 뭐예요?", expanded=False):
        st.markdown(
            "<div style='font-size:13px;line-height:1.8'>"
            "<table style='font-size:13px;width:100%'>"
            "<tr><td style='width:90px'><b>TICKER</b></td>"
            "<td>비교 대상 주가 (예: ^KS11 = KOSPI 지수, 005930.KS = 삼성전자)</td></tr>"
            "<tr><td><b>IC</b></td>"
            "<td>매출-주가 상관계수 (-1~+1). 양수 = 매출↑일 때 주가↑. "
            "|IC| ≥ 0.5 강함, 0.3+ 중간, 0.3 미만 약함</td></tr>"
            "<tr><td><b>LAG</b></td>"
            "<td>매출이 주가를 몇 주 선행하는지 (최적 시차). "
            "예: LAG 37주 = 매출 변화 후 37주 뒤 주가가 반응</td></tr>"
            "<tr><td><b>|t|</b></td>"
            "<td>t-통계량. |t| ≥ 2 면 상관이 우연이 아닐 가능성 95%+. "
            "즉 통계적으로 유의한 신호</td></tr>"
            "<tr><td><b>HIT</b></td>"
            "<td>방향 일치율 — 매출↑일 때 주가도 ↑한 비율. 50%는 동전 던지기, "
            "60%+면 의미 있음</td></tr>"
            "<tr><td><b>PERSIST</b></td>"
            "<td>신호 지속성 — 가까운 lag들에서도 같은 방향 상관이 나오는지. "
            "높을수록 우연이 아니고 안정적인 신호</td></tr>"
            "<tr><td><b>VOL r</b></td>"
            "<td>매출과 <b>거래량</b>의 상관. 가격뿐 아니라 거래량도 매출과 함께 움직이는지 검증</td></tr>"
            "<tr><td><b>N</b></td>"
            "<td>분석에 사용된 관측치 수. 많을수록 통계 신뢰도 ↑</td></tr>"
            "<tr><td><b>SCORE</b></td>"
            "<td>위 메트릭을 종합한 0~100 점수 + 등급(A~D). 80+ A: 강한 알파 후보, "
            "60+ B: 보조 신호, 40+ C: 참고만, 40 미만 D: 무신호</td></tr>"
            "</table></div>",
            unsafe_allow_html=True,
        )

    # 날짜 축에 사용할 datetime
    period_dates = [p.to_timestamp() for p in merged["period"]]
    sales_idx    = _rebase_to_index(merged["sales"])
    stock_idx    = _rebase_to_index(merged["adj_close"])
    lc           = s_freq["lag_corrs"]

    # 추천 lag = 최대 |r|. 차상위 2개도 점선 시뮬레이션용.
    sorted_by_abs = sorted(lc.items(), key=lambda x: abs(x[1]), reverse=True)
    rec_lag       = sorted_by_abs[0][0] if sorted_by_abs else 0
    rec_r         = sorted_by_abs[0][1] if sorted_by_abs else 0.0
    alt_lags      = [k for k, _ in sorted_by_abs[1:3]]  # 2위, 3위

    # ════════════════════════════════════════════════════════════════════════
    # CHART 1 — Lag-Adjusted Overlay
    #   매출이 주가를 N 선행 → 차트의 시점 t에서 매출선은 (t - N) 시점 매출.
    #   즉 "현재 주가 = N 전 매출"이 잘 맞는지 시각 비교.
    # ════════════════════════════════════════════════════════════════════════
    fig_ov = go.Figure()
    # 주가 = 기준 (빨간 점선)
    fig_ov.add_trace(go.Scatter(
        x=period_dates, y=stock_idx, name="주가 (현재 시점)",
        line=dict(color="#dc2626", width=2.5, dash="dash"),
        hovertemplate="%{x|%Y-%m-%d}<br>주가 index=%{y:.1f}<extra></extra>",
    ))
    # 동일 시점 매출 (시차 없음)
    if rec_lag != 0:
        fig_ov.add_trace(go.Scatter(
            x=period_dates, y=sales_idx, name="매출 (동일 시점, 시차 없음)",
            line=dict(color="#cbd5e1", width=1.2),
            hovertemplate="%{x|%Y-%m-%d}<br>매출(동일)=%{y:.1f}<extra></extra>",
        ))
    # 차상위 lag 점선 — "N주 전 매출"
    for alt in alt_lags:
        if alt == rec_lag or alt == 0:
            continue
        shifted = sales_idx.shift(alt)
        fig_ov.add_trace(go.Scatter(
            x=period_dates, y=shifted,
            name=f"{alt}{unit} 전 매출 (r={lc[alt]:+.2f})",
            line=dict(color="#94a3b8", width=1.2, dash="dot"),
            hovertemplate=f"%{{x|%Y-%m-%d}}<br>{alt}{unit} 전 매출=%{{y:.1f}}<extra></extra>",
        ))
    # 추천 lag — 굵은 파란 실선
    sales_rec = sales_idx.shift(rec_lag) if rec_lag > 0 else sales_idx
    rec_label = (f"⭐ {rec_lag}{unit} 전 매출 → 현재 주가 (r={rec_r:+.2f})"
                 if rec_lag > 0 else f"⭐ 매출 (동일 시점, 동행 r={rec_r:+.2f})")
    fig_ov.add_trace(go.Scatter(
        x=period_dates, y=sales_rec, name=rec_label,
        line=dict(color="#1e40af", width=2.8),
        hovertemplate=(f"%{{x|%Y-%m-%d}}<br>{rec_lag}{unit} 전 매출=%{{y:.1f}}<extra></extra>"
                       if rec_lag > 0 else
                       "%{x|%Y-%m-%d}<br>매출(동일)=%{y:.1f}<extra></extra>"),
    ))
    title_text = (
        f"매출 → 주가  ·  매출이 주가를 {rec_lag}{unit} 선행 (r={rec_r:+.2f})"
        if rec_lag > 0 else
        f"매출 ↔ 주가  ·  동행 신호 (lag 0, r={rec_r:+.2f})"
    )
    fig_ov.update_layout(
        title=dict(text=title_text, font=dict(size=14)),
        height=420, plot_bgcolor="#fff",
        margin=dict(t=60, b=40, l=10, r=10),
        yaxis=dict(title="Index = 100 (시작점 정규화)", showgrid=True, gridcolor="#e2e8f0"),
        xaxis=dict(showgrid=False,
                   tickformat="%Y-%m", nticks=12, type="date"),
        legend=dict(orientation="h", yanchor="top", y=-0.15, x=0),
        hovermode="x unified",
    )
    st.plotly_chart(fig_ov, key="ms_drill_overlay", use_container_width=True)

    # 설명 박스
    if rec_lag > 0:
        explanation = (
            f"각 시점의 주가 옆에 <b>그 시점에서 {rec_lag}{unit} 전 매출</b>을 함께 표시한 차트. "
            f"매출이 먼저 움직이고 주가가 {rec_lag}{unit} 후 반영된다는 가설을 시각적으로 검증합니다.<br>"
            f"<b>⭐ 추천 lag {rec_lag}{unit} (r={rec_r:+.3f})</b>는 절댓값 상관이 가장 높은 후행 시차. "
            f"회색 점선은 차상위 후보 — 어느 선이 빨간 주가선과 가장 비슷한지 비교하세요."
        )
    else:
        explanation = (
            "매출과 주가가 동일 시점에서 함께 움직이는 동행 신호 (lag 0). "
            "선행성보다는 현재 confirmation 용도."
        )
    st.markdown(
        f"<div style='background:#f8fafc;border-left:3px solid #1e40af;padding:12px 16px;"
        f"font-size:13px;line-height:1.7;color:#334155;margin:4px 0 24px 0;border-radius:4px'>"
        f"<b style='color:#0f172a'>차트 설명 — Lag-Aligned Overlay (매출 lead → 주가 lag)</b><br>"
        f"{explanation}<br>"
        f"<b>해석:</b> |r| ≥ 0.5 강한 선행, 0.3~0.5 중간, &lt; 0.3 약신호. "
        f"두 선이 매번 어긋난다면 매출이 주가의 선행 지표로 작동하지 않는다는 뜻."
        f"</div>",
        unsafe_allow_html=True,
    )

    # ════════════════════════════════════════════════════════════════════════
    # CHART 2 — Lag Correlation Scan (full width)
    # ════════════════════════════════════════════════════════════════════════
    lag_df = pd.DataFrame([{"lag": k, "r": v} for k, v in lc.items()]).sort_values("lag")
    if not lag_df.empty:
        n_bars = len(lag_df)
        # 막대 수가 많으면 텍스트 라벨 숨김 + 틱 간격 자동 조정
        show_text = n_bars <= 20
        dtick_x   = 1 if n_bars <= 20 else (4 if n_bars <= 40 else 8)

        bc = [
            "#1e40af" if int(l) == rec_lag
            else ("#3b82f6" if abs(v) >= 0.3 else "#cbd5e1")
            for l, v in zip(lag_df["lag"], lag_df["r"])
        ]
        fig_lag = go.Figure(go.Bar(
            x=lag_df["lag"], y=lag_df["r"], marker_color=bc,
            text=[f"{v:+.2f}" for v in lag_df["r"]] if show_text else None,
            textposition="outside" if show_text else None,
            hovertemplate=f"매출이 주가를 %{{x}}{unit} 선행<br>r=%{{y:.3f}}<extra></extra>",
        ))
        fig_lag.add_hline(y=0.30,  line_dash="dot", line_color="#64748b",
                          annotation_text="유의 임계 +0.3", annotation_position="top right",
                          annotation_font=dict(size=10, color="#64748b"))
        fig_lag.add_hline(y=-0.30, line_dash="dot", line_color="#64748b",
                          annotation_text="-0.3", annotation_position="bottom right",
                          annotation_font=dict(size=10, color="#64748b"))
        fig_lag.add_hline(y=0, line_color="#94a3b8", line_width=1)
        fig_lag.update_layout(
            title=dict(
                text=f"매출 lead × 주가 후행 — Lag별 상관계수  ·  최강 lag {rec_lag}{unit} (r={rec_r:+.3f})",
                font=dict(size=14),
            ),
            height=340, plot_bgcolor="#fff",
            margin=dict(t=50, b=40, l=10, r=10),
            xaxis=dict(title=f"매출 선행 시차 ({unit}) — 매출 → N{unit} 후 주가",
                       dtick=dtick_x),
            yaxis=dict(title="Pearson r", range=[-1, 1], showgrid=True, gridcolor="#e2e8f0"),
        )
        st.plotly_chart(fig_lag, key="ms_drill_lag", use_container_width=True)

        st.markdown(
            f"<div style='background:#f8fafc;border-left:3px solid #1e40af;padding:12px 16px;"
            f"font-size:13px;line-height:1.7;color:#334155;margin:4px 0 24px 0;border-radius:4px'>"
            f"<b style='color:#0f172a'>차트 설명 — Lag Correlation Scan</b><br>"
            f"X축의 <b>lag = 매출이 주가를 N{unit} 선행</b>한다는 의미 (lag 0 = 동행, "
            f"lag {cfg['lag_max']} = 매출이 {cfg['lag_max']}{unit} 먼저 움직이고 주가는 {cfg['lag_max']}{unit} "
            f"후에 반영). 각 lag의 막대 높이는 그 가정 하의 Pearson 상관계수.<br>"
            f"<b>진한 파란 = 최강 lag (추천)</b>, 옅은 파란 = |r| ≥ 0.3 (의미 있음), 회색 = 약신호.<br>"
            f"<b>해석:</b> 막대가 임계선(±0.3)을 넘는 lag가 있어야 시그널 사용 가능. "
            f"여러 lag에 걸쳐 비슷한 높이가 유지되면 안정적, 한 점에서만 튀면 우연일 수 있음."
            f"</div>",
            unsafe_allow_html=True,
        )

    # ════════════════════════════════════════════════════════════════════════
    # 하단 디테일 — 단일 컬럼 expander, 기본 모두 접힘
    # ════════════════════════════════════════════════════════════════════════
    if "vol_change" in merged.columns:
        with st.expander("📊 Volume Signal — 거래량 동조 분석", expanded=False):
            vsig      = s_freq.get("volume_signal", {})
            vsig_corr = vsig.get("corr")
            v_lc      = s_freq.get("lag_corrs_volume", {})
            v_lag     = max(v_lc, key=lambda k: abs(v_lc[k])) if v_lc else None
            stat_line = f"`Sales↔Vol r` **{vsig_corr:+.3f}**  ·  `Spikes` **{vsig.get('n_spikes', 0)}**" if vsig_corr is not None else "거래량 데이터 없음."
            if v_lag is not None:
                stat_line += f"  ·  `TX↔Vol best` **{v_lc[v_lag]:+.2f}** (lag {v_lag}{unit})"
            st.markdown(stat_line)

            fig_v = make_subplots(specs=[[{"secondary_y": True}]])
            fig_v.add_trace(go.Scatter(
                x=period_dates, y=merged["sales_growth"],
                name="매출 성장률(%)", line=dict(color="#1e40af", width=1.8),
            ), secondary_y=False)
            fig_v.add_trace(go.Bar(
                x=period_dates, y=merged["vol_change"],
                name="거래량 변화(%)", marker_color="rgba(220,38,38,0.45)",
            ), secondary_y=True)
            fig_v.update_layout(
                height=320, plot_bgcolor="#fff",
                margin=dict(t=10, b=40, l=10, r=10),
                xaxis=dict(showgrid=False, tickformat="%Y-%m", nticks=10, type="date"),
                legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
                hovermode="x unified",
            )
            fig_v.update_yaxes(title_text="매출 성장률(%)", secondary_y=False, gridcolor="#e2e8f0")
            fig_v.update_yaxes(title_text="거래량 변화(%)", secondary_y=True,  gridcolor="#fef3c7")
            st.plotly_chart(fig_v, key="ms_drill_volume", use_container_width=True)

            st.markdown(
                "<div style='background:#f8fafc;border-left:3px solid #94a3b8;padding:10px 14px;"
                "font-size:13px;line-height:1.6;color:#475569;margin-top:6px;border-radius:4px'>"
                "<b style='color:#0f172a'>의미</b> — 매출 변화와 거래량 변화가 같이 움직이면 "
                "시장 참여자들이 실제로 매출 신호에 반응한다는 confirmation. "
                "두 r 모두 약하면 신호가 가격에만 반영되고 참여자 흐름과는 분리되어 있을 가능성이 큽니다."
                "</div>",
                unsafe_allow_html=True,
            )

    es_sales = s_freq.get("event_sales", {})
    if es_sales.get("n_events", 0):
        with st.expander(f"🎯 Event Study — 매출 급증 후 주가 반응 (n={es_sales['n_events']})", expanded=False):
            horizons = cfg["event_horizons"]
            xs = [f"+{h}{unit}" for h in horizons]
            ys = [es_sales["horizons"].get(h) or 0 for h in horizons]
            colors = ["#16a34a" if v >= 0 else "#dc2626" for v in ys]
            fig_es = go.Figure(go.Bar(
                x=xs, y=ys, marker_color=colors,
                text=[f"{v:+.1f}%" for v in ys], textposition="outside",
            ))
            fig_es.add_hline(y=0, line_color="#94a3b8", line_width=1)
            fig_es.update_layout(
                title=dict(text=f"매출 상위 {_EVENT_TOP_PCT}% 이벤트 후 평균 누적 수익률",
                           font=dict(size=13)),
                height=320, plot_bgcolor="#fff",
                margin=dict(t=40, b=10, l=10, r=10),
                yaxis=dict(title="평균 누적 수익률(%)", gridcolor="#e2e8f0"),
            )
            st.plotly_chart(fig_es, key="ms_drill_event", use_container_width=True)

            st.markdown(
                "<div style='background:#f8fafc;border-left:3px solid #94a3b8;padding:10px 14px;"
                "font-size:13px;line-height:1.6;color:#475569;margin-top:6px;border-radius:4px'>"
                f"<b style='color:#0f172a'>의미</b> — 매출이 상위 {_EVENT_TOP_PCT}%로 급증한 시점들을 이벤트로 잡고, "
                "그 이후 +1·+2·+4 기간 동안 평균 누적 주가 수익률을 측정한 backtest 미니어처입니다. "
                "값이 양수이고 horizon에 따라 우상향이면 매출 급증이 실제 후행 수익을 동반한다는 뜻."
                "</div>",
                unsafe_allow_html=True,
            )

    rc = s_freq.get("rolling_corr", pd.DataFrame())
    if not rc.empty:
        with st.expander("🔁 Rolling IC — 신호 지속성", expanded=False):
            roll_cols  = [c for c in rc.columns if c.startswith("roll_")]
            rc_dates   = [p.to_timestamp() for p in rc["period"]]
            fig_rc = go.Figure()
            for i, col in enumerate(roll_cols):
                window_n = col.replace("roll_", "")
                fig_rc.add_trace(go.Scatter(
                    x=rc_dates, y=rc[col], mode="lines",
                    name=f"{window_n}{unit} 윈도우",
                    line=dict(width=2,
                              color=("#1e40af" if i == 0 else "#7c3aed")),
                ))
            fig_rc.add_hline(y=0.30, line_dash="dot", line_color="#64748b",
                             annotation_text="0.3", annotation_position="top right")
            fig_rc.add_hline(y=0,    line_color="#94a3b8", line_width=1)
            fig_rc.update_layout(
                height=320, plot_bgcolor="#fff",
                margin=dict(t=10, b=40, l=10, r=10),
                xaxis=dict(showgrid=False, tickformat="%Y-%m", nticks=10, type="date"),
                yaxis=dict(title="Pearson r", range=[-1, 1], gridcolor="#e2e8f0"),
                legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
            )
            st.plotly_chart(fig_rc, key="ms_drill_rolling", use_container_width=True)

            st.markdown(
                "<div style='background:#f8fafc;border-left:3px solid #94a3b8;padding:10px 14px;"
                "font-size:13px;line-height:1.6;color:#475569;margin-top:6px;border-radius:4px'>"
                "<b style='color:#0f172a'>의미</b> — 시간이 지남에 따라 IC(상관계수)가 어떻게 변하는지 "
                "롤링 윈도우로 측정. 짧은 윈도우는 단기 변동성, 긴 윈도우는 추세를 보여줍니다. "
                "0 위에서 안정적으로 유지되면 신호가 지속적, 0을 자주 가로지르면 일시적 우연일 가능성."
                "</div>",
                unsafe_allow_html=True,
            )

    # ── 전체 회사 표 (하단, expander) ────────────────────────────────────────
    with st.expander(f"All companies ({len(company_signals)})", expanded=False):
        rows = []
        for s in company_signals:
            sf = s.get("_signals", {}).get(freq_label, {})
            r_v = sf.get("max_corr")
            n_v = sf.get("sample_size", 0)
            rows.append({
                "Company":  s["company"],
                "Ticker":   s["ticker"],
                "Status":   "OK" if s.get("status") == "ok" else _FAIL_LABEL.get(s.get("status", ""), "—"),
                "r":        round(r_v, 3) if r_v is not None else None,
                "Lag":      sf.get("best_lag") if sf.get("best_lag") is not None else None,
                "|t|":      round(abs(_t_stat(r_v or 0, n_v)), 2) if r_v is not None else None,
                "Hit%":     int(sf["hit_rate"]) if sf.get("hit_rate") is not None else None,
                "Persist%": int(sf["persistence"]) if sf.get("persistence") is not None else None,
                "Score":    int(sf.get("signal_score", 0)),
                "Grade":    sf.get("grade", s.get("grade", "—")),
            })
        full_df = pd.DataFrame(rows).sort_values(
            "Score", ascending=False, na_position="last"
        ).reset_index(drop=True)
        st.dataframe(full_df, hide_index=True, use_container_width=True)

    if failed:
        with st.expander(f"Failed connections ({len(failed)})", expanded=False):
            fail_rows = [{
                "Company":  s["company"],
                "Ticker":   s.get("ticker", "—"),
                "Status":   _FAIL_LABEL.get(s.get("status", ""), "—"),
                "Reason":   s.get("fail_reason", "—"),
            } for s in failed]
            st.dataframe(pd.DataFrame(fail_rows), hide_index=True, use_container_width=True)

    st.divider()
    st.markdown(
        "### 📊 보조 분석 — 전체 회사 평균 신호 · IC 분포 · 후보 종목 표"
    )
    st.caption("위 메인 차트가 universe·회사별 상세라면, 아래는 전체 회사 평균·랭킹·heatmap.")

    # ════════════════════════════════════════════════════════════════════════
    # MARKET SIGNAL — Bloomberg / Quant 표준 view
    # ════════════════════════════════════════════════════════════════════════
    summary = _compute_overall_summary(ok_sigs, freq_label)
    if summary.get("n_valid", 0) == 0:
        st.info(f"{freq_label} 빈도에서 분석 가능한 회사가 없습니다.")
        return

    avg_ic   = summary["avg_ic"]
    std_ic   = summary["std_ic"]
    ir       = summary["ir"]
    n_valid  = summary["n_valid"]
    n_strong = summary["n_strong"]
    avg_t    = summary["avg_t_stat"]
    avg_hit  = summary.get("avg_hit") or 0
    med_lag  = summary.get("median_lag")
    vol_conf = summary.get("vol_conf_pct", 0)

    # ── 단일 헤더 strip (Bloomberg-style) ────────────────────────────────────
    sig_color   = "#dc2626" if avg_ic < 0.15 else "#d97706" if avg_ic < 0.30 else "#16a34a"
    med_lag_str = f"{med_lag:.0f}{unit}" if med_lag is not None else "—"

    strip_items = [
        ("UNIVERSE", f"{n_valid}/{m['n_companies_total']}", "#fff"),
        ("IC̄",      f"{avg_ic:.3f}",                        sig_color),
        ("σ(IC)",   f"{std_ic:.3f}",                        "#fff"),
        ("IR",      f"{ir:.2f}",                            "#fff"),
        ("|t|̄",     f"{avg_t:.2f}",                         "#fff"),
        ("HIT",     f"{avg_hit:.0f}%",                      "#fff"),
        ("VOL-CONF", f"{vol_conf:.0f}%",                    "#fff"),
        ("MED LAG",  med_lag_str,                           "#fff"),
        ("A/B",     f"{n_strong}",                          "#fff"),
    ]
    strip_inner = "".join(
        f"<div><span style='color:#94a3b8'>{lbl}</span> "
        f"<b style='color:{col}'>{val}</b></div>"
        for lbl, val, col in strip_items
    )
    st.markdown(
        f"<div style='font-family:\"SF Mono\",Menlo,Consolas,monospace;font-size:13px;"
        f"background:#0f172a;color:#e2e8f0;padding:10px 16px;border-radius:4px;"
        f"display:flex;flex-wrap:wrap;gap:22px;align-items:center;line-height:1.4;"
        f"margin-bottom:10px'>{strip_inner}</div>",
        unsafe_allow_html=True,
    )

    # ── IC by Lag (full width) ───────────────────────────────────────────────
    lp = summary.get("lag_profile", {})
    if lp:
        xs = sorted(lp.keys())
        ys = [lp[x] for x in xs]
        max_y = max(ys) if ys else 0
        fig_ic = go.Figure(go.Scatter(
            x=xs, y=ys, mode="lines+markers",
            line=dict(color="#1e40af", width=2.5),
            marker=dict(size=8, color="#1e40af"),
            fill="tozeroy", fillcolor="rgba(30,64,175,0.08)",
            hovertemplate="lag %{x}" + unit + "<br>|IC|=%{y:.3f}<extra></extra>",
        ))
        fig_ic.add_hline(y=0.30, line_dash="dot", line_color="#64748b",
                         annotation_text="유의 임계 0.30", annotation_position="top right",
                         annotation_font=dict(size=10, color="#64748b"))
        fig_ic.update_layout(
            title=dict(
                text=f"매출 lead × 주가 후행 — Lag별 평균 |IC| ({freq_label})",
                font=dict(size=14),
            ),
            height=320, plot_bgcolor="#fff",
            margin=dict(t=50, b=40, l=10, r=10),
            xaxis=dict(
                title=f"매출 선행 시차 ({unit}) — 매출 → N{unit} 후 주가",
                showgrid=True, gridcolor="#e2e8f0", dtick=1,
            ),
            yaxis=dict(title="|IC| (전 회사 평균)", range=[0, max(0.5, max_y * 1.5)],
                       showgrid=True, gridcolor="#e2e8f0"),
        )
        st.plotly_chart(fig_ic, key="ms_overall_ic_decay", use_container_width=True)

        st.markdown(
            "<div style='background:#f8fafc;border-left:3px solid #1e40af;padding:12px 16px;"
            "font-size:13px;line-height:1.7;color:#334155;margin:4px 0 24px 0;border-radius:4px'>"
            "<b style='color:#0f172a'>차트 설명 — IC by Lag (Cross-sectional Mean)</b><br>"
            "X축은 <b>매출이 주가를 N{unit} 선행</b>한다는 가정의 시차. "
            "각 lag별로 universe 전 회사 |Pearson r|를 평균낸 값.<br>"
            "<b>해석:</b> 점선(0.30) 위로 올라가는 lag가 있으면 시장 전체가 그 시차에서 반응한다는 뜻. "
            "곡선이 평탄하고 0.15 미만이면 universe 전체가 약신호 — "
            "거래 데이터가 시장 기대 변화의 선행 지표로 작동하지 않음."
            "</div>".replace("{unit}", unit),
            unsafe_allow_html=True,
        )

    # ── Top Signal Candidates 테이블 (full width) ────────────────────────────
    th1, th2 = st.columns([5, 1])
    with th1:
        st.markdown(
            "<div style='font-family:\"SF Mono\",monospace;font-size:11px;"
            "color:#64748b;letter-spacing:0.5px;margin-top:8px;margin-bottom:6px'>"
            "TOP SIGNAL CANDIDATES — LONG / SHORT</div>",
            unsafe_allow_html=True,
        )
    with th2:
        show_all_ls = st.toggle("전체 보기", value=False, key="ms_show_all_ls",
                                help="OFF: Top 5 long + Top 5 short / ON: 모든 ok 회사")

    # 전체 모드 — 모든 ok 회사를 r 부호 기준으로 분리
    if show_all_ls:
        all_valid = [
            (s, s.get("_signals", {}).get(freq_label, {}))
            for s in company_signals
            if s.get("_signals", {}).get(freq_label, {}).get("status") == "ok"
        ]
        longs_all  = sorted(
            [(s, sf) for s, sf in all_valid if (sf.get("max_corr") or 0) > 0],
            key=lambda x: x[1].get("max_corr") or 0, reverse=True,
        )
        shorts_all = sorted(
            [(s, sf) for s, sf in all_valid if (sf.get("max_corr") or 0) < 0],
            key=lambda x: x[1].get("max_corr") or 0,
        )
        long_rows  = [{
            "company": s["company"], "ticker": s["ticker"],
            "r": sf["max_corr"], "lag": sf.get("best_lag"),
            "t": _t_stat(sf["max_corr"], sf.get("sample_size", 0)),
            "hit": sf.get("hit_rate"), "score": sf.get("signal_score", 0),
        } for s, sf in longs_all]
        short_rows = [{
            "company": s["company"], "ticker": s["ticker"],
            "r": sf["max_corr"], "lag": sf.get("best_lag"),
            "t": _t_stat(sf["max_corr"], sf.get("sample_size", 0)),
            "hit": sf.get("hit_rate"), "score": sf.get("signal_score", 0),
        } for s, sf in shorts_all]
    else:
        long_rows  = summary.get("longs",  [])[:5]
        short_rows = summary.get("shorts", [])[:5]

    ls_rows = []
    for x in long_rows:
        ls_rows.append({
            "Side":         "LONG",
            "회사":          (x.get("company") or "")[:24],
            "Ticker":       x["ticker"],
            "r":            round(x["r"] or 0, 3),
            f"Lag({unit})": x["lag"] if x["lag"] is not None else "—",
            "t":            round(x["t"], 2),
            "Hit%":         int(x["hit"]) if x["hit"] is not None else "—",
            "Score":        int(x["score"]),
        })
    for x in short_rows:
        ls_rows.append({
            "Side":         "SHORT",
            "회사":          (x.get("company") or "")[:24],
            "Ticker":       x["ticker"],
            "r":            round(x["r"] or 0, 3),
            f"Lag({unit})": x["lag"] if x["lag"] is not None else "—",
            "t":            round(x["t"], 2),
            "Hit%":         int(x["hit"]) if x["hit"] is not None else "—",
            "Score":        int(x["score"]),
        })
    if ls_rows:
        n_long  = sum(1 for r in ls_rows if r["Side"] == "LONG")
        n_short = sum(1 for r in ls_rows if r["Side"] == "SHORT")
        if show_all_ls:
            table_height = min(560, 36 + 35 * len(ls_rows))
            st.dataframe(pd.DataFrame(ls_rows), hide_index=True,
                         use_container_width=True, height=table_height)
        else:
            st.dataframe(pd.DataFrame(ls_rows), hide_index=True,
                         use_container_width=True)
        st.caption(f"LONG {n_long}개 · SHORT {n_short}개")
        st.markdown(
            "<div style='background:#f8fafc;border-left:3px solid #94a3b8;padding:10px 14px;"
            "font-size:13px;line-height:1.6;color:#475569;margin:4px 0 24px 0;border-radius:4px'>"
            "<b style='color:#0f172a'>의미</b> — LONG: 매출↑ 시 주가↑ (양의 상관). "
            "SHORT: 매출↑ 시 주가↓ (음의 상관, 역방향 신호). "
            "<b>|t| ≥ 2</b>면 통계적으로 유의한 회사. r·lag·hit가 모두 좋아야 portfolio 후보로 적합."
            "</div>",
            unsafe_allow_html=True,
        )
    else:
        st.info("후보 없음.")

    # ── Heatmap (default OFF — expander 안) ────────────────────────────────
    rank_data = []
    for s in company_signals:
        sf = s.get("_signals", {}).get(freq_label, {})
        if sf.get("status") == "ok":
            rank_data.append({
                "co_short": s["company"][:14],
                "co":       s["company"],
                "ticker":   s["ticker"],
                "r":        sf.get("max_corr"),
                "lag":      sf.get("best_lag"),
                "score":    sf.get("signal_score", 0),
                "lag_corrs": sf.get("lag_corrs", {}),
            })
    rank_data.sort(key=lambda x: abs(x["r"] or 0), reverse=True)
    n_total_rank = len(rank_data)

    with st.expander(f"🗺 Lag Correlation Matrix — 회사 × Lag heatmap ({n_total_rank}개사)", expanded=False):
        if n_total_rank > 12:
            show_all = st.toggle(
                f"전체 {n_total_rank}개 회사 보기", value=False, key="ms_show_all",
            )
        else:
            show_all = True
        n_show = n_total_rank if show_all else min(12, n_total_rank)
        rank_show = rank_data[:n_show]

        if rank_show:
            lag_labels   = list(range(0, cfg["lag_max"] + 1))
            hm_companies = [f"{r['co_short']} · {r['ticker']}" for r in rank_show]
            hm_matrix    = [[r["lag_corrs"].get(lag, float("nan")) for lag in lag_labels] for r in rank_show]

            row_h = 30
            fig_hm = go.Figure(go.Heatmap(
                z=hm_matrix,
                x=[str(l) for l in lag_labels],
                y=hm_companies,
                colorscale=[[0, "#b91c1c"], [0.5, "#f8fafc"], [1, "#15803d"]],
                zmin=-0.5, zmax=0.5,
                text=[[f"{v:.2f}" if math.isfinite(v) else "" for v in row] for row in hm_matrix],
                texttemplate="%{text}",
                textfont=dict(size=10, family="SF Mono, Menlo, monospace"),
                colorbar=dict(title="r", thickness=12, len=0.7, tickfont=dict(size=10)),
                hovertemplate="<b>%{y}</b><br>lag %{x}" + unit + "<br>r=%{z:.3f}<extra></extra>",
            ))
            title_text = (f"|IC| 상위 {n_show}/{n_total_rank}"
                          if not show_all else f"전체 {n_total_rank}개사")
            fig_hm.update_layout(
                title=dict(text=title_text, font=dict(size=13)),
                height=max(260, len(hm_companies) * row_h + 80),
                margin=dict(t=40, b=40, l=10, r=10),
                xaxis=dict(title=f"Lag ({unit})", side="bottom"),
                yaxis=dict(autorange="reversed"),
                plot_bgcolor="#fff",
            )
            st.plotly_chart(fig_hm, key="ms_overall_heatmap", use_container_width=True)
            st.markdown(
                "<div style='background:#f8fafc;border-left:3px solid #94a3b8;padding:10px 14px;"
                "font-size:13px;line-height:1.6;color:#475569;margin-top:8px;border-radius:4px'>"
                "<b style='color:#0f172a'>의미</b> — 행=회사, 열=lag. 셀 색깔과 숫자는 그 회사의 "
                "해당 lag에서의 매출-주가 상관계수. "
                "특정 행에 진한 녹색 셀이 있으면 그 회사가 그 lag에서 강한 선행 신호."
                "</div>",
                unsafe_allow_html=True,
            )

    # ════════════════════════════════════════════════════════════════════════
    # 🔍 주가 데이터 매핑 검증 (Audit)
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    _render_data_audit(result, freq_label, ok_sigs, failed)

    # ════════════════════════════════════════════════════════════════════════
    # DRILL-DOWN — 단일 컬럼 풀 너비
    # ════════════════════════════════════════════════════════════════════════
    st.divider()

