"""마켓 데이터 — 데이터 액세스 레이어.

모든 fetch는 ``@st.cache_data`` 로 감싸 KRX 호출 비용을 줄입니다.
TTL은 데이터 성격별로:
- 마스터 / 멤버십 / 상장 목록: 24h
- 일봉 OHLCV: 12h (오늘이 영업일이면 30min)
- 외국인 보유: 12h
- 지수 OHLCV: 12h
"""
from __future__ import annotations

import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Literal, Optional

import pandas as pd
import streamlit as st

# ── mandata_kr 마스터 데이터 ────────────────────────────────────────
_HERE = Path(__file__).resolve().parent              # …/marketdata_app
_DATA_TOOL = _HERE.parent                            # …/data-tool
_MANDATA_KR_PARENT = _DATA_TOOL / "korea-security-id"

if str(_MANDATA_KR_PARENT) not in sys.path:
    sys.path.insert(0, str(_MANDATA_KR_PARENT))

try:
    from mandata_kr import Identifier, SecurityRecord  # type: ignore
    _MASTER_IMPORT_ERROR: Optional[Exception] = None
except Exception as e:  # pragma: no cover
    Identifier = None  # type: ignore
    SecurityRecord = None  # type: ignore
    _MASTER_IMPORT_ERROR = e


# ── 마지막 에러 저장소 (UI 진단용) ────────────────────────────────────
# 캐시된 빈 결과 뒤의 진짜 원인을 surfacing 하기 위함.
LAST_ERRORS: dict[str, str] = {}


def _record_err(key: str, e: Exception) -> None:
    LAST_ERRORS[key] = f"{type(e).__name__}: {e}"


def clear_errors() -> None:
    LAST_ERRORS.clear()


# ── 캐시 ───────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Loading KR equity master…")
def _identifier() -> "Identifier":
    """Identifier 싱글톤 — CSV 첫 호출 시 로드."""
    if Identifier is None:
        raise RuntimeError(
            f"mandata_kr 패키지 import 실패: {_MASTER_IMPORT_ERROR}. "
            f"korea-security-id/ 폴더가 데이터툴 루트에 있는지 확인하세요."
        )
    return Identifier()


def master_status() -> dict:
    """Sidebar용 마스터 상태 조회.

    Note: ``idr.equities`` 는 raw CSV dict 리스트라 attribute 가 아니라
    ``e.get("kospi200") == "Y"`` 로 체크해야 한다.
    """
    if _MASTER_IMPORT_ERROR is not None:
        return {"ok": False, "error": str(_MASTER_IMPORT_ERROR)}
    idr = _identifier()
    return {
        "ok": True,
        "n_equities": len(idr.equities),
        "n_kospi200": sum(1 for e in idr.equities if e.get("kospi200") == "Y"),
        "n_kosdaq150": sum(1 for e in idr.equities if e.get("kosdaq150") == "Y"),
        "n_krx300": sum(1 for e in idr.equities if e.get("krx300") == "Y"),
    }


# ── pykrx wrappers ────────────────────────────────────────────────────

def _krx():
    """지연 import — pykrx는 무거우니 호출 시점에만."""
    from pykrx import stock as krx  # type: ignore
    return krx


def _yf():
    """지연 import — yfinance fallback (Korean indices & equities)."""
    import yfinance as yf  # type: ignore
    return yf


# ── Yahoo Finance helpers (pykrx가 broken일 때 fallback) ────────────────

# 한국 인덱스 → Yahoo 심볼
YF_INDEX: dict[str, str] = {
    "1001": "^KS11",      # KOSPI
    "2001": "^KQ11",      # KOSDAQ
    "1028": "^KS200",     # KOSPI 200
}

# 한국 마켓 → Yahoo suffix
YF_MARKET_SUFFIX: dict[str, str] = {
    "KOSPI": ".KS",
    "KOSDAQ": ".KQ",
    "KONEX": ".KN",
}


def _yf_symbol_for_ticker(ticker: str, market: str = "") -> str:
    """6자리 ticker + market → Yahoo 심볼. market 없으면 .KS 가정."""
    t = str(ticker).zfill(6)
    suffix = YF_MARKET_SUFFIX.get(market, ".KS")
    return f"{t}{suffix}"


def _yf_ohlcv(symbol: str, start: str, end: str,
              interval: str = "1d") -> pd.DataFrame:
    """yfinance OHLCV → 표준 컬럼명으로 정규화."""
    yf = _yf()
    # end는 inclusive로 만들기 위해 +1일
    s = _iso(start)
    e = _iso(end, add_day=1)
    try:
        raw = yf.download(symbol, start=s, end=e, interval=interval,
                          progress=False, auto_adjust=True, threads=False)
    except Exception as ex:
        _record_err(f"yf.{symbol}", ex)
        return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()
    # MultiIndex 컬럼이 올 수도 있음 (yfinance 0.2.x 신버전)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    raw = raw.reset_index()
    rename = {
        "Date": "date", "Datetime": "date",
        "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Adj Close": "close",
        "Volume": "volume",
    }
    raw = raw.rename(columns=rename)
    # value column 계산 (close × volume)
    if "value" not in raw.columns and "close" in raw.columns and "volume" in raw.columns:
        raw["value"] = (raw["close"] * raw["volume"]).round(0)
    keep = [c for c in ["date", "open", "high", "low", "close", "volume", "value"]
            if c in raw.columns]
    return raw[keep]


def diagnostics() -> dict:
    """KRX 연결·pykrx 버전·간단 fetch 테스트. UI 진단 expander 용."""
    info: dict = {
        "python_ok": False,
        "pykrx_ok": False,
        "pykrx_version": None,
        "sample_fetch_ok": False,
        "sample_error": None,
        "latest_business_day": None,
        "latest_business_day_error": None,
    }
    import sys as _sys
    info["python"] = f"{_sys.version_info.major}.{_sys.version_info.minor}.{_sys.version_info.micro}"
    info["python_ok"] = True

    try:
        import pykrx
        info["pykrx_ok"] = True
        info["pykrx_version"] = getattr(pykrx, "__version__", "?")
    except Exception as e:
        info["sample_error"] = f"import pykrx failed: {type(e).__name__}: {e}"
        return info

    # 영업일 탐색
    try:
        info["latest_business_day"] = _latest_trading_day_str()
    except Exception as e:
        info["latest_business_day_error"] = f"{type(e).__name__}: {e}"

    # 가벼운 호출 한 번 — 삼성전자 종목명
    try:
        krx = _krx()
        name = krx.get_market_ticker_name("005930")
        info["sample_fetch_ok"] = bool(name)
        info["sample_value"] = name
    except Exception as e:
        info["sample_error"] = f"{type(e).__name__}: {e}"

    # 실제 fetch 케이스를 직접 호출 (캐시 우회) — 어디서 막히는지 즉시 진단
    bday = info.get("latest_business_day") or date.today().strftime("%Y%m%d")
    start_7d = (date.today() - timedelta(days=14)).strftime("%Y%m%d")

    try:
        krx = _krx()
        df = krx.get_index_ohlcv_by_date(start_7d, bday, "1001")  # KOSPI
        info["index_kospi_ok"] = (df is not None) and (not df.empty)
        info["index_kospi_rows"] = 0 if df is None else len(df)
        info["index_kospi_error"] = None
    except Exception as e:
        info["index_kospi_ok"] = False
        info["index_kospi_error"] = f"{type(e).__name__}: {e}"

    try:
        krx = _krx()
        tickers = krx.get_market_ticker_list(date=bday, market="KOSPI")
        info["universe_kospi_ok"] = bool(tickers)
        info["universe_kospi_n"] = len(tickers or [])
        info["universe_kospi_error"] = None
    except Exception as e:
        info["universe_kospi_ok"] = False
        info["universe_kospi_error"] = f"{type(e).__name__}: {e}"

    try:
        krx = _krx()
        cap = krx.get_market_cap_by_ticker(bday, market="KOSPI")
        info["cap_kospi_ok"] = (cap is not None) and (not cap.empty)
        info["cap_kospi_rows"] = 0 if cap is None else len(cap)
        info["cap_kospi_error"] = None
    except Exception as e:
        info["cap_kospi_ok"] = False
        info["cap_kospi_error"] = f"{type(e).__name__}: {e}"

    return info


def _safe_call(fn, *args, **kwargs) -> tuple:
    """호출 → (result_or_None, error_string_or_None) 튜플로 변환.

    빈 DataFrame을 조용히 돌려주는 대신 호출자가 에러 메시지를
    UI에 보여줄 수 있게 한다.
    """
    try:
        return fn(*args, **kwargs), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def list_market_universe(market: Literal["KOSPI", "KOSDAQ", "KONEX", "ALL"] = "ALL",
                         as_of: Optional[str] = None) -> pd.DataFrame:
    """전종목 리스트 + (가능하면) 시가총액.

    pykrx 가 망가져 있어 mandata_kr 마스터 + 추가 fetch 로 구성:
    - 1차: pykrx (캡포함). 깨지면 컬럼 가변 처리 후 빈 결과로 폴백.
    - 2차: mandata_kr 마스터에서 ticker/name/market 만 채워서 반환.
    """
    # ---- 1차: pykrx 시도 (운 좋게 작동하면 시가총액까지 한 번에) ----
    try:
        krx = _krx()
        if as_of is None:
            as_of = _latest_trading_day_str()
        markets = ["KOSPI", "KOSDAQ", "KONEX"] if market == "ALL" else [market]
        parts: list[pd.DataFrame] = []
        for m in markets:
            try:
                tickers = krx.get_market_ticker_list(date=as_of, market=m)
                if not tickers:
                    raise RuntimeError(f"empty ticker list at {as_of}")
                cap_df = krx.get_market_cap_by_ticker(as_of, market=m)
                if cap_df is None or cap_df.empty:
                    raise RuntimeError("cap empty")
                cap_df = cap_df.reset_index().rename(columns={"티커": "ticker"})
                # 컬럼 가변 처리 — 있는 것만 매핑
                col_map = {
                    "시가총액": "market_cap_krw",
                    "상장주식수": "shares",
                    "종가": "last_close",
                }
                present = {k: v for k, v in col_map.items() if k in cap_df.columns}
                keep = ["ticker"] + list(present.keys())
                sliced = cap_df[keep].rename(columns=present)
                names = {t: krx.get_market_ticker_name(t) for t in tickers}
                base = pd.DataFrame({"ticker": tickers,
                                     "name": [names[t] for t in tickers],
                                     "market": m})
                parts.append(base.merge(sliced, on="ticker", how="left"))
            except Exception as e:
                _record_err(f"universe.{m}", e)
        if parts:
            out = pd.concat(parts, ignore_index=True)
            # 숫자 컬럼을 numeric으로 강제 — nlargest 등 호환
            for col in ("market_cap_krw", "shares", "last_close"):
                if col in out.columns:
                    out[col] = pd.to_numeric(out[col], errors="coerce")
            return out
    except Exception as e:
        _record_err("universe.pykrx", e)

    # ---- 2차: mandata_kr 마스터로 폴백 (캡 없음) ----
    try:
        idr = _identifier()
        wanted = None if market == "ALL" else market
        rows = [
            {
                "ticker": (e.get("local_code") or "").strip(),
                "name": (e.get("company_name_kr") or "").strip(),
                "market": (e.get("market") or "").strip(),
                "market_cap_krw": pd.NA,
                "shares": pd.NA,
                "last_close": pd.NA,
            }
            for e in idr.equities
            if e.get("local_code")
            and (wanted is None or (e.get("market") or "").upper() == wanted)
        ]
        return pd.DataFrame(rows)
    except Exception as e:
        _record_err("universe.mandata", e)
        return pd.DataFrame(columns=["ticker", "name", "market",
                                     "market_cap_krw", "shares", "last_close"])


@st.cache_data(ttl=60 * 60 * 12, show_spinner=False)
def ohlcv(ticker: str, start: str, end: str,
          freq: Literal["d", "w", "m"] = "d",
          adjusted: bool = True) -> pd.DataFrame:
    """단일 종목 OHLCV.

    Parameters
    ----------
    ticker  : "005930" 형식의 6자리 (없으면 자동 zero-pad)
    start   : "YYYYMMDD" 또는 "YYYY-MM-DD"
    end     : 같음
    freq    : 'd' 일봉, 'w' 주봉, 'm' 월봉
    adjusted: True면 수정주가 (액면분할 반영)
    """
    t = str(ticker).zfill(6)
    s = _compact(start)
    e = _compact(end)

    # 1차: pykrx 시도
    try:
        krx = _krx()
        df = krx.get_market_ohlcv_by_date(s, e, t, freq=freq, adjusted=adjusted)
        if df is not None and not df.empty:
            df = df.reset_index().rename(columns={
                "날짜": "date", "시가": "open", "고가": "high", "저가": "low",
                "종가": "close", "거래량": "volume", "거래대금": "value",
                "등락률": "pct_change",
            })
            df["ticker"] = t
            cols = ["ticker", "date", "open", "high", "low", "close",
                    "volume", "value", "pct_change"]
            return df[[c for c in cols if c in df.columns]]
    except Exception as ex:
        _record_err(f"ohlcv.{t}.pykrx", ex)

    # 2차: yfinance fallback — mandata_kr 마스터의 market 으로 .KS/.KQ 결정
    rec = lookup_security(t)
    market = (rec or {}).get("market", "KOSPI")
    yf_symbol = _yf_symbol_for_ticker(t, market)
    interval = {"d": "1d", "w": "1wk", "m": "1mo"}.get(freq, "1d")
    yf_df = _yf_ohlcv(yf_symbol, start, end, interval=interval)
    if not yf_df.empty:
        yf_df = yf_df.copy()
        yf_df["ticker"] = t
        return yf_df[["ticker", "date", "open", "high", "low", "close",
                      "volume", "value"]]

    _record_err(f"ohlcv.{t}", RuntimeError("both pykrx and yfinance returned empty"))
    return pd.DataFrame()


@st.cache_data(ttl=60 * 60 * 12, show_spinner=False)
def foreign_ownership(ticker: str, start: str, end: str) -> pd.DataFrame:
    """외국인 보유 비율 시계열."""
    krx = _krx()
    t = str(ticker).zfill(6)
    try:
        df = krx.get_exhaustion_rates_of_foreign_investment(_compact(start), _compact(end), t)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index().rename(columns={
        "날짜": "date",
        "상장주식수": "shares",
        "보유수량": "foreign_shares",
        "지분율": "foreign_pct",
        "한도수량": "limit_shares",
        "한도소진률": "limit_exhausted_pct",
    })
    df["ticker"] = t
    return df


@st.cache_data(ttl=60 * 60 * 12, show_spinner=False)
def trading_value_by_investor(ticker: str, start: str, end: str) -> pd.DataFrame:
    """투자자별 거래대금 (외국인·기관·개인)."""
    krx = _krx()
    t = str(ticker).zfill(6)
    try:
        df = krx.get_market_trading_value_by_date(_compact(start), _compact(end), t)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index().rename(columns={"날짜": "date"})
    df["ticker"] = t
    return df


@st.cache_data(ttl=60 * 60 * 12, show_spinner=False)
def index_ohlcv(index_code: str, start: str, end: str) -> pd.DataFrame:
    """지수 OHLCV. KOSPI=1001, KOSDAQ=2001, KOSPI200=1028, KRX300=5042.

    pykrx 1.2.8 ``get_index_ohlcv_by_date`` 가 ``KeyError: '지수명'`` 으로
    터지는 케이스가 많아 yfinance 를 1차로 사용 (^KS11, ^KQ11, ^KS200).
    pykrx 는 fallback.
    """
    # 1차: yfinance (안정적)
    yf_sym = YF_INDEX.get(index_code)
    if yf_sym:
        df = _yf_ohlcv(yf_sym, start, end, interval="1d")
        if not df.empty:
            return df

    # 2차: pykrx 시도
    try:
        krx = _krx()
        df = krx.get_index_ohlcv_by_date(_compact(start), _compact(end), index_code)
    except Exception as ex:
        _record_err(f"index.{index_code}.pykrx", ex)
        return pd.DataFrame()
    if df is None or df.empty:
        _record_err(f"index.{index_code}",
                    RuntimeError(f"empty {_compact(start)}–{_compact(end)} from both yf and pykrx"))
        return pd.DataFrame()
    df = df.reset_index().rename(columns={
        "날짜": "date", "시가": "open", "고가": "high", "저가": "low",
        "종가": "close", "거래량": "volume", "거래대금": "value",
    })
    return df


# ── 멤버십 ────────────────────────────────────────────────────────────

INDEX_CHOICES: dict[str, str] = {
    "KOSPI 200": "kospi200",
    "KOSDAQ 150": "kosdaq150",
    "KRX 300": "krx300",
}


def index_members(label: str) -> pd.DataFrame:
    """지수 구성종목을 DataFrame으로.

    ``Identifier.members()`` 는 SecurityRecord 리스트를 돌려주므로
    attribute 액세스가 안전하다 (raw CSV dict 가 아님).
    """
    idr = _identifier()
    try:
        records = idr.members(label)
    except Exception:
        return pd.DataFrame(
            columns=["ticker", "name_kr", "name_en", "market",
                     "sector_gics", "isin", "bloomberg", "ric"]
        )
    rows = [
        {
            "ticker": getattr(r, "local_code", "") or "",
            "name_kr": getattr(r, "name_kr", "") or "",
            "name_en": getattr(r, "name_en", "") or "",
            "market": getattr(r, "market", "") or "",
            "sector_gics": getattr(r, "sector_name_en", "") or "",
            "isin": getattr(r, "isin", "") or "",
            "bloomberg": getattr(r, "bloomberg_ticker", "") or "",
            "ric": getattr(r, "ric", "") or "",
        }
        for r in records
    ]
    return pd.DataFrame(rows)


# ── search / lookup ──────────────────────────────────────────────────

def search_securities(query: str, limit: int = 20) -> list[dict]:
    """이름/약어/코드 어떤 표기든 매칭. 사이드바 자동완성용."""
    if not query:
        return []
    idr = _identifier()
    hits = idr.search(query, limit=limit)
    return [_security_dict(r) for r in hits]


def lookup_security(query: str) -> Optional[dict]:
    """단일 종목 해상도."""
    if not query:
        return None
    idr = _identifier()
    rec = idr.lookup(query)
    return _security_dict(rec) if rec else None


def _security_dict(rec) -> dict:
    """SecurityRecord → display-friendly dict."""
    return {
        "ticker": getattr(rec, "local_code", "") or "",
        "name_kr": getattr(rec, "name_kr", "") or "",
        "name_en": getattr(rec, "name_en", "") or "",
        "market": getattr(rec, "market", "") or "",
        "share_class": getattr(rec, "share_class", "") or "",
        "isin": getattr(rec, "isin", "") or "",
        "bloomberg": getattr(rec, "bloomberg_ticker", "") or "",
        "ric": getattr(rec, "ric", "") or "",
        "sector_code_gics": getattr(rec, "sector_code_gics", "") or "",
        "sector_name_en": getattr(rec, "sector_name_en", "") or "",
        "listing_date": getattr(rec, "listing_date", "") or "",
        "kospi200": bool(getattr(rec, "kospi200", False)),
        "kosdaq150": bool(getattr(rec, "kosdaq150", False)),
        "krx300": bool(getattr(rec, "krx300", False)),
    }


# ── helpers ──────────────────────────────────────────────────────────

def _compact(d) -> str:
    """YYYY-MM-DD or YYYYMMDD or date → YYYYMMDD."""
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y%m%d")
    s = str(d).strip()
    return s.replace("-", "").replace(".", "").replace("/", "")


def _iso(d, add_day: int = 0) -> str:
    """YYYY-MM-DD or YYYYMMDD or date → YYYY-MM-DD (yfinance용)."""
    if isinstance(d, (date, datetime)):
        dd = d
    else:
        s = str(d).strip().replace("-", "").replace(".", "").replace("/", "")
        try:
            dd = datetime.strptime(s, "%Y%m%d").date()
        except ValueError:
            dd = date.today()
    if add_day:
        dd = dd + timedelta(days=add_day)
    return dd.strftime("%Y-%m-%d")


def _latest_trading_day_str() -> str:
    """가장 최근 영업일 (오늘 데이터가 아직 없을 수 있으니 어제부터 시도)."""
    krx = _krx()
    # 어제부터 시작 — 장중·장 시작 전엔 오늘 데이터가 없으므로
    candidate = date.today() - timedelta(days=1)

    # 1) pykrx 헬퍼 사용 (이름이 버전에 따라 다름)
    for fn_name in ("get_nearest_business_day_in_a_week",
                    "get_previous_business_day"):
        fn = getattr(krx, fn_name, None)
        if fn is None:
            continue
        try:
            d = fn(candidate.strftime("%Y%m%d"))
            if d:
                return d
        except Exception:
            continue

    # 2) 폴백: 최대 7일 거슬러 올라가며 주말 회피
    for back in range(7):
        d = candidate - timedelta(days=back)
        if d.weekday() < 5:  # Mon–Fri
            return d.strftime("%Y%m%d")
    return candidate.strftime("%Y%m%d")


def default_date_range(years: int = 1) -> tuple[date, date]:
    """기본 조회 구간."""
    end = date.today()
    start = end - timedelta(days=365 * years)
    return start, end


# ── 다종목 일괄 (export용) ────────────────────────────────────────────

def bulk_ohlcv(tickers: Iterable[str], start: str, end: str,
               freq: Literal["d", "w", "m"] = "d",
               adjusted: bool = True) -> pd.DataFrame:
    """여러 종목 OHLCV를 long-format으로 합쳐서 반환.

    실패한 ticker는 조용히 건너뜀 (caller가 결과의 distinct ticker로 확인).
    """
    frames: list[pd.DataFrame] = []
    for t in tickers:
        df = ohlcv(t, start, end, freq=freq, adjusted=adjusted)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
