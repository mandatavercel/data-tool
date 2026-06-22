"""
kfnb_app/ingest/disclosures.py — DART 공시 분기매출 (graceful).

DART Open API 로 상장사 분기 매출액을 가져와 (krx_code, quarter, revenue) 로 반환.
누적 보고(반기/3분기/사업보고서)에서 분기 값을 차분해 산출한다.
API 키/네트워크 없으면 빈 결과 + 사유 반환 — 파이프라인 비차단.
실제 호출은 DART_API_KEY 가 있는 사용자 환경에서 동작.

quarter 형식 = YYYYQ (예: 20243 = 2024 Q3).
"""
from __future__ import annotations

import io
import zipfile
from xml.etree import ElementTree as ET

import pandas as pd

COLS = ["krx_code", "quarter", "revenue"]
_REPRT = {1: "11013", 2: "11012", 3: "11014", 4: "11011"}  # Q1/반기/3Q/사업보고서
_REVENUE_NAMES = {"매출액", "수익(매출액)", "영업수익", "매출"}


def _empty(note: str):
    return pd.DataFrame(columns=COLS), note


def _corp_code_map(api_key: str) -> dict:
    """DART corpCode.xml → {stock_code(6자리): corp_code(8자리)}."""
    import requests
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    r = requests.get(url, params={"crtfc_key": api_key}, timeout=30)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    root = ET.fromstring(z.read(z.namelist()[0]))
    out = {}
    for el in root.iter("list"):
        sc = (el.findtext("stock_code") or "").strip()
        cc = (el.findtext("corp_code") or "").strip()
        if sc and sc != " " and cc:
            out[sc] = cc
    return out


def _revenue(api_key: str, corp_code: str, year: int, reprt: str):
    """단일 보고서 누적 매출액 (없으면 None)."""
    import requests
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    r = requests.get(url, params={"crtfc_key": api_key, "corp_code": corp_code,
                                  "bsns_year": str(year), "reprt_code": reprt},
                     timeout=30)
    js = r.json()
    if js.get("status") != "000":
        return None
    for it in js.get("list", []):
        if it.get("sj_div") in ("IS", "CIS") and \
                it.get("account_nm", "").strip() in _REVENUE_NAMES:
            try:
                return float(str(it.get("thstrm_amount", "")).replace(",", ""))
            except ValueError:
                return None
    return None


def quarterly_revenue(companies, api_key: str,
                      years=range(2020, 2027)) -> tuple[pd.DataFrame, str]:
    """companies: [(krx_code, ...)] 또는 [krx_code]. → (df, note)."""
    codes = [(c[0] if isinstance(c, (list, tuple)) else c) for c in companies]
    codes = [c for c in dict.fromkeys(codes) if c]
    if not api_key:
        return _empty("DART_API_KEY 없음 — 공시매출 생략")
    if not codes:
        return _empty("상장 티커 없음")
    try:
        cmap = _corp_code_map(api_key)
    except Exception as e:
        return _empty(f"DART corpCode 조회 실패: {type(e).__name__}")

    rows = []
    for code in codes:
        cc = cmap.get(code)
        if not cc:
            continue
        for y in years:
            cum = {}
            for q, rc in _REPRT.items():
                try:
                    v = _revenue(api_key, cc, y, rc)
                except Exception:
                    v = None
                if v is not None:
                    cum[q] = v
            # 누적 → 분기 차분
            if 1 in cum:
                rows.append((code, y * 10 + 1, cum[1]))
            if 2 in cum and 1 in cum:
                rows.append((code, y * 10 + 2, cum[2] - cum[1]))
            if 3 in cum and 2 in cum:
                rows.append((code, y * 10 + 3, cum[3] - cum[2]))
            if 4 in cum and 3 in cum:
                rows.append((code, y * 10 + 4, cum[4] - cum[3]))
    if not rows:
        return _empty("공시매출 데이터 없음")
    df = pd.DataFrame(rows, columns=COLS)
    return df, f"{df['krx_code'].nunique()}개 종목 분기매출"
