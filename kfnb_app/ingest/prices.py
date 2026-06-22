"""
kfnb_app/ingest/prices.py — 주가(월별) 데이터 소스 (graceful).

pykrx 우선, 실패 시 yfinance 폴백. 라이브러리 미설치/네트워크 차단 시
빈 DataFrame + 사유 문자열을 반환해 파이프라인을 막지 않는다.
반환: (DataFrame[krx_code, ym, close, ret], note)
  - 외부 네트워크가 필요하므로 실제 조회는 사용자 환경에서 동작.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

COLS = ["krx_code", "ym", "close", "ret"]


def _empty(note: str):
    return pd.DataFrame(columns=COLS), note


def _finalize(code: str, close: pd.Series, dates) -> pd.DataFrame:
    s = pd.Series(close.values, index=pd.to_datetime(dates)).sort_index()
    m = s.resample("ME").last().dropna()
    ym = [d.year * 100 + d.month for d in m.index]
    df = pd.DataFrame({"krx_code": code, "ym": ym, "close": m.values})
    df["ret"] = df["close"].pct_change()
    return df


def _via_pykrx(code: str, start: str, end: str):
    try:
        from pykrx import stock
        df = stock.get_market_ohlcv(start, end, code, "m")
        if df is None or df.empty:
            return None
        return _finalize(code, df["종가"], df.index)
    except Exception:
        return None


def _via_yfinance(code: str, start: str, end: str):
    try:
        import yfinance as yf
        st = f"{start[:4]}-{start[4:6]}-{start[6:]}"
        en = f"{end[:4]}-{end[4:6]}-{end[6:]}"
        df = yf.download(f"{code}.KS", start=st, end=en, interval="1mo",
                         progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        close = df["Close"]
        if hasattr(close, "columns"):       # 멀티컬럼 방지
            close = close.iloc[:, 0]
        return _finalize(code, close, df.index)
    except Exception:
        return None


def _biz_day_candidates(on: str) -> list[str]:
    """조회 후보일: 입력일 + 직전 영업일 + 며칠 더 거슬러(장 마감 전/휴장 대비)."""
    from datetime import datetime, timedelta
    base = datetime.strptime(on, "%Y%m%d")
    cands = []
    for back in (0, 1, 2, 3, 4, 7):
        cands.append((base - timedelta(days=back)).strftime("%Y%m%d"))
    return list(dict.fromkeys(cands))


def _mc_via_pykrx(codes, on):
    """pykrx 로 시총 dict. (mc, used_date, err)."""
    try:
        from pykrx import stock
    except Exception:                              # noqa: BLE001
        return {}, None, "pykrx_missing"
    candidates = _biz_day_candidates(on)
    try:
        nearest = stock.get_nearest_business_day_in_a_week(on)
        if nearest:
            candidates = [nearest] + [c for c in candidates if c != nearest]
    except Exception:                              # noqa: BLE001
        pass
    last_err = None
    for d in candidates:
        try:
            df = stock.get_market_cap_by_ticker(d)
        except Exception as e:                     # noqa: BLE001
            last_err = e
            continue
        if df is not None and not df.empty and "시가총액" in df.columns:
            out = {}
            for code in codes:
                if code in df.index:
                    try:
                        v = float(df.loc[code, "시가총액"])
                        if v > 0:
                            out[code] = v
                    except Exception:              # noqa: BLE001
                        continue
            if out:
                return out, d, None
    return {}, None, (type(last_err).__name__ if last_err else "no_data")


def _mc_via_yfinance(codes):
    """yfinance 로 시총 dict(원). KRX 로그인 불필요 — 폴백용."""
    try:
        import yfinance as yf
    except Exception:                              # noqa: BLE001
        return {}, "yfinance_missing"
    out = {}
    for code in codes:
        for suffix in (".KS", ".KQ"):              # KOSPI / KOSDAQ
            try:
                t = yf.Ticker(f"{code}{suffix}")
                mc = (t.fast_info.get("market_cap")
                      if hasattr(t, "fast_info") else None) or t.info.get("marketCap")
                if mc and float(mc) > 0:
                    out[code] = float(mc)
                    break
            except Exception:                      # noqa: BLE001
                continue
    return out, (None if out else "no_data")


def market_caps(krx_codes, on: str | None = None) -> tuple[dict, str]:
    """KRX 6자리 코드 리스트 → {code: 시가총액(원)}. pykrx→yfinance 폴백, graceful.

    유니버스 '투자가능성(시총)' 스코어용. 둘 다 실패하면 ({}, 사유).
    영업일 보정 + 최근 며칠 재시도(장 마감 전/휴장 대비). yfinance 는 KRX 로그인
    불필요라 더 안정적인 폴백. 실제 조회는 사용자 환경에서 동작.
    """
    codes = [str(c).zfill(6) for c in dict.fromkeys(krx_codes) if str(c).strip()]
    if not codes:
        return {}, "상장 티커 없음"
    on = on or date.today().strftime("%Y%m%d")

    mc, used, err = _mc_via_pykrx(codes, on)
    if mc:
        return mc, f"{len(mc)}/{len(codes)}개 종목 시총(pykrx, 기준일 {used})"

    # pykrx 실패 → yfinance 폴백
    mc2, err2 = _mc_via_yfinance(codes)
    if mc2:
        return mc2, f"{len(mc2)}/{len(codes)}개 종목 시총(yfinance 폴백)"

    if err == "pykrx_missing" and err2 == "yfinance_missing":
        return {}, ("pykrx/yfinance 미설치 — '⬇️ pykrx 설치' 후 재시도"
                    "(없어도 상장여부 기준으로 점수 산출)")
    return {}, (f"시총 조회 불가(pykrx:{err}, yfinance:{err2}) — 평일 장마감 후 재시도 권장. "
                "시총 없이 상장여부 기준으로 점수는 정상 산출됩니다.")


def monthly_prices(krx_codes, start: str = "20200101",
                   end: str | None = None) -> tuple[pd.DataFrame, str]:
    """KRX 6자리 코드 리스트 → 월별 종가/수익률. (df, note)."""
    end = end or date.today().strftime("%Y%m%d")
    codes = [c for c in dict.fromkeys(krx_codes) if c]
    if not codes:
        return _empty("상장 티커 없음")
    frames, src = [], None
    for code in codes:
        df = _via_pykrx(code, start, end)
        if df is not None and not df.empty:
            src = src or "pykrx"
        else:
            df = _via_yfinance(code, start, end)
            if df is not None and not df.empty:
                src = src or "yfinance"
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return _empty("주가 조회 불가 (pykrx/yfinance 미설치 또는 네트워크 차단)")
    out = pd.concat(frames, ignore_index=True)
    return out, f"{out['krx_code'].nunique()}개 종목 ({src})"
