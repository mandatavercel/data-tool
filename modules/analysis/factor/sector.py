"""Sector Master — KRX/DART에서 종목별 sector 매핑.

전략 (하이브리드, 우선순위 순):
  1) DART induty_code (이미 DART API 통합되어 있음, KSIC 5자리)
  2) pykrx WICS 업종 (KOSPI/KOSDAQ 인덱스 구성종목 역매핑)
  3) yfinance Ticker.info["sector"] (영문, fallback)

KSIC → GICS 11 sector 매핑은 수동 테이블로 처리.
결과는 6시간 캐시.
"""
from __future__ import annotations
import json
import urllib.parse
import urllib.request
import ssl
import streamlit as st
import pandas as pd

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode    = ssl.CERT_NONE


# GICS 11 sector 표준
GICS_SECTORS = [
    "Energy", "Materials", "Industrials", "Consumer Discretionary",
    "Consumer Staples", "Health Care", "Financials", "Information Technology",
    "Communication Services", "Utilities", "Real Estate",
]


# KSIC 대분류 (영문 한 자리) → GICS sector 매핑
# KSIC 9차 개정 기준 21개 대분류
_KSIC_TO_GICS = {
    "A": "Consumer Staples",          # 농업, 임업 및 어업
    "B": "Materials",                  # 광업
    "C": "Industrials",                # 제조업 (default; 아래 induty_code로 더 세분화)
    "D": "Utilities",                  # 전기, 가스, 증기 및 공기조절 공급업
    "E": "Utilities",                  # 수도, 하수 및 폐기물 처리, 원료 재생업
    "F": "Industrials",                # 건설업
    "G": "Consumer Discretionary",     # 도매 및 소매업
    "H": "Industrials",                # 운수 및 창고업
    "I": "Consumer Discretionary",     # 숙박 및 음식점업
    "J": "Communication Services",     # 정보통신업
    "K": "Financials",                 # 금융 및 보험업
    "L": "Real Estate",                # 부동산업
    "M": "Industrials",                # 전문, 과학 및 기술 서비스업
    "N": "Industrials",                # 사업시설 관리, 사업 지원 및 임대 서비스업
    "O": "Industrials",                # 공공행정 (제외 대상)
    "P": "Consumer Discretionary",     # 교육 서비스업
    "Q": "Health Care",                # 보건업 및 사회복지 서비스업
    "R": "Communication Services",     # 예술, 스포츠 및 여가관련 서비스업
    "S": "Consumer Discretionary",     # 협회 및 단체, 수리 및 기타 개인 서비스업
    "T": "Consumer Discretionary",     # 가구 내 고용활동
    "U": "Industrials",                # 국제 및 외국기관
}


# KSIC 5자리 코드 prefix → GICS 세분화 (제조업 C 안에서 구분)
_KSIC_DETAIL_TO_GICS = {
    # 식품·음료 (10, 11) → Consumer Staples
    "10": "Consumer Staples",
    "11": "Consumer Staples",
    # 의류·신발 (13, 14, 15) → Consumer Discretionary
    "13": "Consumer Discretionary",
    "14": "Consumer Discretionary",
    "15": "Consumer Discretionary",
    # 화학 (20, 21) → Materials / Health Care
    "20": "Materials",                 # 화학물질 및 화학제품
    "21": "Health Care",                # 의료용 물질 및 의약품
    # 비금속·금속 (23, 24, 25) → Materials
    "23": "Materials", "24": "Materials", "25": "Materials",
    # 전자·반도체·통신 (26) → Information Technology
    "26": "Information Technology",
    # 의료기기 (27) → Health Care
    "27": "Health Care",
    # 전기장비·기계 (28, 29) → Industrials
    "28": "Industrials", "29": "Industrials",
    # 자동차·운송 (30, 31) → Consumer Discretionary
    "30": "Consumer Discretionary", "31": "Consumer Discretionary",
    # 가구·기타 (32, 33) → Consumer Discretionary
    "32": "Consumer Discretionary", "33": "Consumer Discretionary",
}


def _ksic_to_gics(induty_code: str) -> str:
    """KSIC 코드 → GICS sector. 실패 시 'Unclassified'."""
    if not induty_code:
        return "Unclassified"
    code = str(induty_code).strip()
    # 5자리 numeric인 경우, 상위 2자리로 detail 매핑 시도
    if code.isdigit() and len(code) >= 2:
        prefix2 = code[:2]
        if prefix2 in _KSIC_DETAIL_TO_GICS:
            return _KSIC_DETAIL_TO_GICS[prefix2]
        # 1자리 알파벳 변환 (제조업 = C, 도소매 = G, ...)
        # KSIC 숫자 코드는 보통 2자리 prefix가 산업
        # 산업분류표에서 10~33 = C(제조업), 35~36 = D, 37~39 = E, 41~42 = F,
        # 45~47 = G, 49~52 = H, 55~56 = I, 58~63 = J, 64~66 = K, 68 = L,
        # 70~73 = M, 74~76 = N, 84 = O, 85 = P, 86~87 = Q, 90~91 = R,
        # 94~96 = S, 97~98 = T, 99 = U
        try:
            pref = int(prefix2)
        except ValueError:
            return "Unclassified"
        if 10 <= pref <= 33: return "Industrials"            # 기본 제조업; detail은 위에서 처리됨
        if pref in (35, 36): return "Utilities"
        if 37 <= pref <= 39: return "Utilities"
        if 41 <= pref <= 42: return "Industrials"
        if 45 <= pref <= 47: return "Consumer Discretionary"
        if 49 <= pref <= 52: return "Industrials"
        if 55 <= pref <= 56: return "Consumer Discretionary"
        if 58 <= pref <= 63: return "Communication Services"
        if 64 <= pref <= 66: return "Financials"
        if pref == 68:       return "Real Estate"
        if 70 <= pref <= 76: return "Industrials"
        if pref == 84:       return "Industrials"
        if pref == 85:       return "Consumer Discretionary"
        if 86 <= pref <= 87: return "Health Care"
        if 90 <= pref <= 91: return "Communication Services"
        if 94 <= pref <= 99: return "Consumer Discretionary"
    # 알파벳 코드 (e.g., "C", "G")
    if code[0].upper() in _KSIC_TO_GICS:
        return _KSIC_TO_GICS[code[0].upper()]
    return "Unclassified"


# ── DART industry fetcher ────────────────────────────────────────────────────

@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_dart_industry(api_key: str, corp_code: str) -> dict:
    """DART /api/company.json — induty_code 등 기업 개황. 24시간 캐시.

    requests + retry로 'Connection reset by peer'에 robust.
    """
    url = (
        "https://opendart.fss.or.kr/api/company.json?"
        + urllib.parse.urlencode({"crtfc_key": api_key, "corp_code": corp_code})
    )
    # earnings 모듈의 robust GET을 재사용
    try:
        from modules.analysis.signal.earnings import _dart_get
        raw = _dart_get(url, timeout=10, retries=2)
        data = json.loads(raw.decode())
        if data.get("status") != "000":
            return {}
        return {
            "induty_code": data.get("induty_code"),
            "corp_cls":    data.get("corp_cls"),
            "est_dt":      data.get("est_dt"),
        }
    except Exception:
        return {}


# ── pykrx fallback ───────────────────────────────────────────────────────────

@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_pykrx_listings(date: str | None = None) -> pd.DataFrame:
    """pykrx로 KOSPI + KOSDAQ 전체 종목 + 시가총액. 24시간 캐시.

    Returns: DataFrame[stock_code, name, market, market_cap]
    """
    try:
        from pykrx import stock
    except ImportError:
        return pd.DataFrame()

    if date is None:
        # 가장 최근 영업일 가져오기 (오늘 데이터가 아직 없을 수 있음)
        from datetime import date as _date, timedelta
        for delta in range(0, 7):
            d = (_date.today() - timedelta(days=delta)).strftime("%Y%m%d")
            try:
                df = stock.get_market_cap_by_ticker(d, market="KOSPI")
                if not df.empty:
                    date = d
                    break
            except Exception:
                continue

    rows = []
    for market in ["KOSPI", "KOSDAQ"]:
        try:
            df = stock.get_market_cap_by_ticker(date, market=market)
            if df.empty:
                continue
            for ticker in df.index:
                rows.append({
                    "stock_code": ticker,
                    "name":       stock.get_market_ticker_name(ticker),
                    "market":     market,
                    "market_cap": float(df.loc[ticker, "시가총액"]),
                })
        except Exception:
            continue
    return pd.DataFrame(rows)


# ── Master fetcher ──────────────────────────────────────────────────────────

@st.cache_data(ttl=21600, show_spinner=False)
def fetch_sector_master(
    stock_codes: tuple[str, ...],
    dart_api_key: str | None = None,
    corp_code_map: tuple[tuple[str, str], ...] | None = None,
) -> pd.DataFrame:
    """주어진 종목 리스트에 대해 sector master 생성.

    Parameters
    ----------
    stock_codes : 6자리 종목코드 튜플 (cache 가능)
    dart_api_key : DART API key (induty_code 조회용)
    corp_code_map : (stock_code, corp_code) 튜플 — DART 매핑 (있으면 induty_code 우선 사용)

    Returns
    -------
    DataFrame with columns:
      stock_code, name, market, market_cap, induty_code, sector_gics, source
    """
    code_to_corp = dict(corp_code_map) if corp_code_map else {}

    # pykrx로 universe 시가총액 + 시장 구분 가져옴
    listings = _fetch_pykrx_listings()
    listings_idx = listings.set_index("stock_code") if not listings.empty else None

    rows = []
    for code in stock_codes:
        # 기본 정보 from pykrx
        if listings_idx is not None and code in listings_idx.index:
            base = {
                "stock_code": code,
                "name":       listings_idx.loc[code, "name"],
                "market":     listings_idx.loc[code, "market"],
                "market_cap": listings_idx.loc[code, "market_cap"],
            }
        else:
            base = {"stock_code": code, "name": "", "market": "", "market_cap": float("nan")}

        # DART induty_code (있으면)
        induty = None
        if dart_api_key and code in code_to_corp:
            info = _fetch_dart_industry(dart_api_key, code_to_corp[code])
            induty = info.get("induty_code")

        sector = _ksic_to_gics(induty) if induty else "Unclassified"

        rows.append({
            **base,
            "induty_code": induty,
            "sector_gics": sector,
            "source":      "DART+pykrx" if induty else ("pykrx" if listings_idx is not None and code in listings_idx.index else "none"),
        })

    return pd.DataFrame(rows)
