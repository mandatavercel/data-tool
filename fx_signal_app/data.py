"""
FX Signal — 시장 데이터 fetcher.

yfinance만 사용 — 외부 API 키 없이 USD/KRW 환전 의사결정에 필요한 매크로 지표를 모두 수집.

수집 지표:
  USDKRW   : USD/KRW 현물 (yfinance: "KRW=X")
  DXY      : 달러 인덱스 (광의의 USD 강세) — "DX-Y.NYB" / fallback "DX=F"
  UST10Y   : 미국 10년물 국채금리 — "^TNX"  (값 = 실제 % × 10, 자동 보정)
  KOSPI    : 한국 KOSPI 지수 — "^KS11"
  BRENT    : 브렌트 원유 — "BZ=F"
  WTI      : WTI 원유 — "CL=F"
  CNY      : USD/CNY (위안화 약세 → KRW 동조 약세 압력) — "CNY=X"
  JPY      : USD/JPY — "JPY=X"
  KOSPI200 : KOSPI 200 — "^KS200"

캐싱:
  현재가/단기 시계열 — 15분 TTL
  장기 시계열       — 6시간 TTL
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Optional

import pandas as pd

# streamlit이 없는 환경(테스트 / CLI)에서도 import 가능하게 fallback.
try:
    import streamlit as st  # type: ignore
except ImportError:
    class _DummyCache:
        def cache_data(self, *args, **kwargs):
            def deco(fn):
                return fn
            # @st.cache_data(ttl=...) 또는 @st.cache_data 둘 다 지원
            if args and callable(args[0]) and not kwargs:
                return args[0]
            return deco

        def error(self, *a, **k):
            pass

    st = _DummyCache()  # type: ignore


warnings.filterwarnings("ignore", category=FutureWarning)


# ─────────────────────────────────────────────────────────────
# Ticker map — 단일 진입점에서만 ticker 사용
# ─────────────────────────────────────────────────────────────
TICKERS: dict[str, list[str]] = {
    # key       :  [primary, *fallbacks]
    "USDKRW": ["KRW=X"],
    "DXY":    ["DX-Y.NYB", "DX=F"],
    "UST10Y": ["^TNX"],
    "KOSPI":  ["^KS11"],
    "BRENT":  ["BZ=F"],
    "WTI":    ["CL=F"],
    "CNY":    ["CNY=X"],
    "JPY":    ["JPY=X"],
    "KOSPI200": ["^KS200"],
}


# 사람이 읽기 좋은 라벨
LABELS: dict[str, str] = {
    "USDKRW": "USD/KRW",
    "DXY":    "달러 인덱스 (DXY)",
    "UST10Y": "미국 10년물 국채금리",
    "KOSPI":  "KOSPI",
    "BRENT":  "브렌트 원유",
    "WTI":    "WTI 원유",
    "CNY":    "USD/CNY (위안)",
    "JPY":    "USD/JPY (엔)",
    "KOSPI200": "KOSPI 200",
}


# USD/KRW 상승에 작용하는 부호.
# +1 = 이 지표가 오르면 USD/KRW 도 오를 압력 (KRW 약세 = 환전에 우호적)
# -1 = 이 지표가 오르면 USD/KRW 가 내릴 압력 (KRW 강세 = 환전에 불리)
USDKRW_SIGN: dict[str, int] = {
    "DXY":     +1,   # 달러 강세 = 원화 약세
    "UST10Y":  +1,   # 미국 금리 ↑ = 달러 강세
    "KOSPI":   -1,   # 한국 증시 강세 = 외인 유입 = 원화 강세
    "BRENT":   +1,   # 유가 ↑ = 한국 수입 부담 ↑ = 원화 약세
    "WTI":     +1,
    "CNY":     +1,   # 위안 약세 (USD/CNY ↑) = 원화 동조 약세
    "JPY":     +1,   # 엔 약세도 원화에 부담 (지역 동조)
}


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────
# Streamlit Cloud는 Yahoo Finance가 봇으로 차단하는 경우가 잦음.
# curl-cffi 로 Chrome TLS fingerprint 위장 + yfinance에 session 명시.
def _get_curl_session():
    """curl-cffi Chrome impersonation Session. 없으면 None."""
    try:
        from curl_cffi import requests as curl_requests
        return curl_requests.Session(impersonate="chrome")
    except ImportError:
        return None


def _ensure_tz_cache():
    """yfinance tz 캐시를 쓰기 가능한 위치로 (Cloud의 read-only home 회피)."""
    try:
        import yfinance as yf
        import tempfile, os
        cache_dir = os.path.join(tempfile.gettempdir(), "yfinance-cache")
        os.makedirs(cache_dir, exist_ok=True)
        if hasattr(yf, "set_tz_cache_location"):
            yf.set_tz_cache_location(cache_dir)
    except Exception:
        pass


_TZ_CACHE_INITIALIZED = False


def _download_one(ticker: str, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    """
    yfinance에서 단일 ticker 종가 다운로드. Streamlit Cloud 차단 우회 위해:
      1) curl-cffi Chrome 위장 session 으로 yf.Ticker().history() 시도
      2) 실패 시 yf.download(session=...) 시도
      3) 그래도 실패 시 빈 DataFrame
    """
    global _TZ_CACHE_INITIALIZED
    try:
        import yfinance as yf
    except ImportError:
        st.error("yfinance가 설치되지 않았습니다. `pip install yfinance` 후 다시 시도하세요.")
        return pd.DataFrame()

    if not _TZ_CACHE_INITIALIZED:
        _ensure_tz_cache()
        _TZ_CACHE_INITIALIZED = True

    session = _get_curl_session()

    # Method 1: yf.Ticker(...).history() — session 지원 안정적
    try:
        if session is not None:
            t = yf.Ticker(ticker, session=session)
        else:
            t = yf.Ticker(ticker)
        df = t.history(period=period, interval=interval, auto_adjust=False)
        if df is not None and not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            return df
    except Exception:
        pass

    # Method 2: yf.download(session=...) fallback
    try:
        kwargs = dict(
            tickers=ticker,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if session is not None:
            kwargs["session"] = session
        df = yf.download(**kwargs)
        if df is None or df.empty:
            return pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception:
        return pd.DataFrame()


def _resolve_ticker(key: str, period: str, interval: str) -> tuple[str, pd.DataFrame]:
    """fallback 순서대로 시도. 처음으로 데이터 있는 ticker 반환."""
    for t in TICKERS.get(key, []):
        df = _download_one(t, period=period, interval=interval)
        if not df.empty:
            return t, df
    return "", pd.DataFrame()


# ─────────────────────────────────────────────────────────────
# Frankfurter (ECB) — USD/KRW 만의 백업 데이터 소스
# yfinance가 Streamlit Cloud에서 차단됐을 때 최후 보루.
# 무료, 인증 없음, ECB 공식 데이터.
# ─────────────────────────────────────────────────────────────
def _frankfurter_usdkrw(period: str = "1y") -> pd.DataFrame:
    """Frankfurter API로 USD/KRW 일별 시계열. (yfinance 백업용)"""
    try:
        import requests
        from datetime import date, timedelta

        days_map = {
            "5d": 5, "1mo": 32, "3mo": 95, "6mo": 190,
            "1y": 380, "2y": 740, "5y": 1830,
        }
        days = days_map.get(period, 380)
        end = date.today()
        start = end - timedelta(days=days)

        url = f"https://api.frankfurter.app/{start.isoformat()}..{end.isoformat()}?from=USD&to=KRW"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        rates = data.get("rates", {})
        if not rates:
            return pd.DataFrame()

        rows = sorted(rates.items())
        idx = pd.to_datetime([d for d, _ in rows])
        vals = [v.get("KRW", float("nan")) for _, v in rows]
        df = pd.DataFrame({"Close": vals}, index=idx).dropna()
        return df
    except Exception:
        return pd.DataFrame()


def _normalize_ust10y(close: pd.Series) -> pd.Series:
    """^TNX 는 실제 % × 10 으로 옴 (예: 4.5% → 45.0). %로 변환."""
    if close.empty:
        return close
    last = float(close.dropna().iloc[-1]) if not close.dropna().empty else 0.0
    # 값이 10 이상이면 *10 표기로 간주 → /10
    if last >= 10.0:
        return close / 10.0
    return close


# ─────────────────────────────────────────────────────────────
# Public API — 시계열
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=900, show_spinner=False)  # 15분 TTL
def fetch_series(key: str, period: str = "1y", interval: str = "1d") -> pd.Series:
    """
    지표 시계열의 종가(Close)만 반환. 빈 결과면 empty Series.
    key는 TICKERS의 키 (USDKRW, DXY, ...).
    """
    if key not in TICKERS:
        return pd.Series(dtype=float, name=key)

    _, df = _resolve_ticker(key, period=period, interval=interval)

    # yfinance 실패 시 USDKRW는 Frankfurter 백업으로 시도
    if df.empty and key == "USDKRW":
        df = _frankfurter_usdkrw(period=period)

    if df.empty:
        return pd.Series(dtype=float, name=key)

    # Close 컬럼 찾기 (대소문자 변형 대응)
    col = None
    for c in ("Close", "close", "Adj Close", "adj_close"):
        if c in df.columns:
            col = c
            break
    if col is None:
        return pd.Series(dtype=float, name=key)

    s = df[col].dropna().rename(key)
    if key == "UST10Y":
        s = _normalize_ust10y(s)
    return s


@st.cache_data(ttl=21600, show_spinner=False)  # 6시간 TTL
def fetch_long_series(key: str, period: str = "5y") -> pd.Series:
    """장기 시계열 (200일 이평선용 등). 6h 캐시."""
    return fetch_series(key, period=period, interval="1d")


# ─────────────────────────────────────────────────────────────
# Public API — 최근값 묶음
# ─────────────────────────────────────────────────────────────
@dataclass
class SeriesSnapshot:
    key: str
    label: str
    last: float
    prev: float                       # 전일
    ma5: float
    ma20: float
    ma60: float
    ma200: float
    pct_1d: float                     # 1일 수익률 (%)
    pct_5d: float
    pct_20d: float
    pct_60d: float
    series: pd.Series                 # 원본 series (차트용)

    @property
    def delta(self) -> float:
        return self.last - self.prev


def _snapshot_from_series(key: str, s: pd.Series) -> Optional[SeriesSnapshot]:
    """Series → SeriesSnapshot. NaN 안전."""
    s = s.dropna()
    if s.empty or len(s) < 2:
        return None
    last = float(s.iloc[-1])
    prev = float(s.iloc[-2])

    def _safe_ma(n: int) -> float:
        if len(s) < n:
            return float("nan")
        return float(s.iloc[-n:].mean())

    def _safe_pct(n: int) -> float:
        if len(s) < n + 1:
            return float("nan")
        base = float(s.iloc[-(n + 1)])
        if base == 0:
            return float("nan")
        return (last / base - 1.0) * 100.0

    return SeriesSnapshot(
        key=key,
        label=LABELS.get(key, key),
        last=last,
        prev=prev,
        ma5=_safe_ma(5),
        ma20=_safe_ma(20),
        ma60=_safe_ma(60),
        ma200=_safe_ma(200),
        pct_1d=_safe_pct(1),
        pct_5d=_safe_pct(5),
        pct_20d=_safe_pct(20),
        pct_60d=_safe_pct(60),
        series=s,
    )


@st.cache_data(ttl=900, show_spinner=False)
def fetch_snapshots(keys: list[str], period: str = "1y") -> dict[str, SeriesSnapshot]:
    """
    여러 지표의 스냅샷을 한 번에. 실패한 ticker는 결과 dict에서 빠짐.

    Returns:
        {key: SeriesSnapshot}
    """
    out: dict[str, SeriesSnapshot] = {}
    for k in keys:
        s = fetch_series(k, period=period, interval="1d")
        snap = _snapshot_from_series(k, s)
        if snap is not None:
            out[k] = snap
    return out


# ─────────────────────────────────────────────────────────────
# 헬스 체크 (선택)
# ─────────────────────────────────────────────────────────────
def health_check() -> dict[str, bool]:
    """모든 ticker가 데이터를 가져오는지 확인 (디버깅용)."""
    out = {}
    for k in TICKERS:
        s = fetch_series(k, period="5d")
        out[k] = not s.empty
    return out
