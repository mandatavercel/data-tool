"""
modules/mapping/lookup.py

KRX 종목 마스터 조회 + 회사명 → ISIN 매칭 (매핑 앱 전용).

데이터 소스:
  KRX 정보데이터시스템 (http://data.krx.co.kr)
  bld=dbms/MDC/STAT/standard/MDCSTAT01901  →  전종목 기본정보
  한 번의 POST 로 ISIN(ISU_CD), 단축코드(ISU_SRT_CD), 한글약식명(ISU_ABBRV),
  한글정식명(ISU_NM), 영문명(ISU_ENG_NM), 시장(MKT_NM) 를 모두 제공.

분석 앱 코드와는 독립적으로 유지한다.
"""
from __future__ import annotations

import re
import requests
import pandas as pd
import streamlit as st


# ── KRX API ───────────────────────────────────────────────────────────────────
# HTTPS 우선 — 일부 환경(특히 회사 프록시)에서 http는 차단되거나 referer 체크가
# 더 엄격하다. 실패하면 http로 fallback.
KRX_DATA_URLS = [
    "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
    "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
]
KRX_REFERER = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd"


# ── 회사명 정규화 ─────────────────────────────────────────────────────────────
# 한국 회사명 접두/접미사 (㈜, (주), 주식회사 등) + 영문 법인 표기 제거
_SUFFIX_PATTERNS = [
    r"주식회사",
    r"㈜",
    r"\(주\)",
    r"\(유\)",
    r"유한회사",
    r"홀딩스",                       # NOTE: 일부는 정식명에 포함 — 제거하지 않을 수도 있음
    r"co\.?\s*,?\s*ltd\.?",
    r"corporation",
    r"corp\.?",
    r"company",
    r"inc\.?",
    r"ltd\.?",
    r"limited",
    r"plc\.?",
]
_PREFIX_PATTERNS = [
    r"주식회사",
    r"㈜",
    r"\(주\)",
]

_SUFFIX_RE = re.compile(
    r"\s*(?:" + "|".join(_SUFFIX_PATTERNS[:-7]) + r")\s*$",
    re.IGNORECASE,
)
# 영문 접미사는 별도 (단어 경계 신경)
_SUFFIX_EN_RE = re.compile(
    r"\b(?:co\.?\s*,?\s*ltd\.?|corporation|corp\.?|company|inc\.?|ltd\.?|limited|plc\.?)\.?\s*$",
    re.IGNORECASE,
)
_PREFIX_RE = re.compile(
    r"^\s*(?:" + "|".join(_PREFIX_PATTERNS) + r")\s*",
    re.IGNORECASE,
)


def normalize_company(name: str) -> str:
    """회사명 정규화 — 매칭 키로 사용.

    예:
      '㈜삼성전자'       → '삼성전자'
      '주식회사 네이버'  → '네이버'
      'Samsung Electronics Co., Ltd.' → 'samsungelectronics'
      'NAVER Corp.'      → 'naver'
    """
    if not isinstance(name, str):
        return ""
    s = name.strip()
    if not s:
        return ""
    # 접두사 제거
    s = _PREFIX_RE.sub("", s)
    # 접미사 제거 (한글 + 영문)
    s = _SUFFIX_RE.sub("", s)
    s = _SUFFIX_EN_RE.sub("", s)
    # 잔여 구두점/공백 제거
    s = re.sub(r"[\s\.,\-\_/]+", "", s)
    return s.lower()


# ── KRX 마스터 fetch ──────────────────────────────────────────────────────────
# KRX Open API (인증키 기반)
KRX_OPENAPI_BASE = "https://openapi.krx.co.kr/svc/apis"


@st.cache_data(ttl=24 * 60 * 60, show_spinner="KRX 종목 마스터 다운로드 중…")
def fetch_krx_master(auth_key: str | None = None) -> pd.DataFrame:
    """
    KRX 전종목 마스터. 다음 순서로 시도:
      1) KRX Open API (인증키 있으면) — ISIN 포함, 가장 안정적
      2) KRX 정보데이터시스템 direct API — ISIN 포함
      3) pykrx fallback — ISIN 없음

    반환 컬럼:
        isin, stock_code, name, name_full, name_eng, market, secgroup
    """
    errors: list[str] = []

    # ── 1차: KRX Open API (인증키 필요) ──────────────────────────────────────
    if auth_key and auth_key.strip():
        try:
            return _fetch_via_krx_openapi(auth_key.strip())
        except Exception as e:
            errors.append(f"KRX OpenAPI: {type(e).__name__}: {e}")

    # ── 2차: KRX direct (인증키 없이) ────────────────────────────────────────
    try:
        return _fetch_via_krx_direct()
    except Exception as e:
        errors.append(f"KRX direct: {type(e).__name__}: {e}")

    # ── 3차: pykrx fallback ──────────────────────────────────────────────────
    try:
        return _fetch_via_pykrx()
    except Exception as e:
        errors.append(f"pykrx: {type(e).__name__}: {e}")

    raise RuntimeError(" | ".join(errors))


def _fetch_via_krx_openapi(auth_key: str) -> pd.DataFrame:
    """
    KRX Open API — 주식 종목 기본정보.

    인증키는 header `AUTH_KEY` 로 전달. 시장별로 호출(STK=유가증권, KSQ=코스닥, KNX=코넥스).
    KRX OpenAPI 의 정확한 endpoint·파라미터는 KRX 가 변경할 수 있으므로 여러 변형 시도.
    """
    from datetime import datetime, timedelta

    # 최근 영업일 (오늘 데이터는 장 종료 전까진 없을 수 있음)
    base = datetime.now() - timedelta(days=1)
    while base.weekday() >= 5:  # 토(5), 일(6) 회피
        base -= timedelta(days=1)
    bas_dd = base.strftime("%Y%m%d")

    # 호출 변형 — (path, market_id)
    # KRX Open API 의 '주식 종목 기본정보' endpoint 후보들
    endpoint_paths = [
        "/sto/stk_isu_base_info",   # 주식 종목 기본정보 (가장 가능성 높음)
        "/sto/stk_bydd_trd",        # 주식 일별매매정보 (종목 이름 포함)
    ]
    market_ids = ["STK", "KSQ", "KNX"]  # KOSPI, KOSDAQ, KONEX
    market_label = {"STK": "KOSPI", "KSQ": "KOSDAQ", "KNX": "KONEX"}

    headers_variants = [
        {"AUTH_KEY": auth_key},        # KRX OpenAPI 표준
        {"Authorization": f"Bearer {auth_key}"},
        {"auth_key": auth_key},
    ]

    errors: list[str] = []
    rows_all: list[dict] = []
    used_endpoint: str | None = None

    for path in endpoint_paths:
        url = KRX_OPENAPI_BASE + path
        endpoint_rows: list[dict] = []
        for mkt in market_ids:
            params = {"basDd": bas_dd, "mktId": mkt}
            success = False
            for hdr in headers_variants:
                try:
                    r = requests.get(url, params=params, headers=hdr, timeout=30)
                    if r.status_code in (401, 403):
                        errors.append(f"{path} {mkt}: {r.status_code} (인증)")
                        continue
                    r.raise_for_status()
                    # 응답이 빈 본문이거나 비-JSON 일 수 있음
                    try:
                        data = r.json()
                    except Exception:
                        errors.append(f"{path} {mkt}: JSON 파싱 실패")
                        continue
                    rows = (
                        data.get("OutBlock_1")
                        or data.get("output")
                        or data.get("data")
                        or data.get("result")
                        or []
                    )
                    if rows:
                        # 시장 정보 보강
                        for row in rows:
                            row.setdefault("MKT_NM", market_label.get(mkt, mkt))
                        endpoint_rows.extend(rows)
                        success = True
                        break
                except Exception as e:
                    errors.append(f"{path} {mkt} (hdr={list(hdr.keys())[0]}): "
                                  f"{type(e).__name__}: {e}")
                    continue
            if not success and hdr != headers_variants[-1]:
                # 다른 시장으로 진행
                pass

        if endpoint_rows:
            rows_all = endpoint_rows
            used_endpoint = path
            break

    if not rows_all:
        raise RuntimeError(
            "KRX Open API 모든 endpoint 실패. 인증키와 사용 가능한 API 권한을 확인하세요. "
            "상세: " + " | ".join(errors[-5:])
        )

    df = pd.DataFrame(rows_all)
    rename = {
        "ISU_CD":             "isin",
        "ISU_SRT_CD":         "stock_code",
        "ISU_ABBRV":          "name",
        "ISU_NM":             "name_full",
        "ISU_ENG_NM":         "name_eng",
        "MKT_NM":             "market",
        "SECUGRP_NM":         "secgroup",
        "KIND_STKCERT_TP_NM": "secgroup",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    for col in ["isin", "stock_code", "name", "name_full", "name_eng", "market", "secgroup"]:
        if col not in df.columns:
            df[col] = ""
    df = df[["isin", "stock_code", "name", "name_full", "name_eng", "market", "secgroup"]]
    df = df.astype(str)
    df["isin"]       = df["isin"].str.strip()
    df["stock_code"] = df["stock_code"].str.strip()
    return df


def _fetch_via_krx_direct() -> pd.DataFrame:
    """KRX 정보데이터시스템 — 전종목 기본정보 (ISIN 포함)."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Accept-Language":  "ko-KR,ko;q=0.9,en;q=0.8",
        "Referer":          KRX_REFERER,
        "Origin":           "http://data.krx.co.kr",
        "X-Requested-With": "XMLHttpRequest",
    }
    # 여러 payload/bld 변형을 순서대로 시도
    payload_variants = [
        # KRX가 가장 자주 쓰는 형식
        {"bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
         "mktId": "ALL", "share": "1", "csvxls_isNo": "false", "locale": "ko_KR"},
        # locale 없는 버전
        {"bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
         "mktId": "ALL", "share": "1", "csvxls_isNo": "false"},
        # mktId 를 STK 으로
        {"bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
         "mktId": "STK", "share": "1", "csvxls_isNo": "false", "locale": "ko_KR"},
        # 다른 bld
        {"bld": "dbms/MDC/STAT/issue/MDCSTAT020201",
         "mktId": "ALL", "share": "1", "csvxls_isNo": "false", "locale": "ko_KR"},
    ]

    last_err: Exception | None = None
    rows = None
    for url in KRX_DATA_URLS:
        for payload in payload_variants:
            try:
                resp = requests.post(url, data=payload, headers=headers, timeout=30)
                resp.raise_for_status()
                payload_json = resp.json()
                rows = (
                    payload_json.get("OutBlock_1")
                    or payload_json.get("output")
                    or payload_json.get("block1")
                    or []
                )
                if rows:
                    break
            except Exception as e:
                last_err = e
                continue
        if rows:
            break

    if not rows:
        raise RuntimeError(
            f"KRX direct API에서 종목 데이터를 못 받음. 마지막: {last_err}"
        )

    df = pd.DataFrame(rows)
    rename = {
        "ISU_CD":             "isin",
        "ISU_SRT_CD":         "stock_code",
        "ISU_ABBRV":          "name",
        "ISU_NM":             "name_full",
        "ISU_ENG_NM":         "name_eng",
        "MKT_NM":             "market",
        "SECUGRP_NM":         "secgroup",
        "KIND_STKCERT_TP_NM": "secgroup",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    for col in ["isin", "stock_code", "name", "name_full", "name_eng", "market", "secgroup"]:
        if col not in df.columns:
            df[col] = ""
    df = df[["isin", "stock_code", "name", "name_full", "name_eng", "market", "secgroup"]]
    df = df.astype(str)
    df["isin"]       = df["isin"].str.strip()
    df["stock_code"] = df["stock_code"].str.strip()
    return df


def _fetch_via_pykrx() -> pd.DataFrame:
    """pykrx fallback — 종목코드 + 회사명 + 시장. ISIN은 빈 채.

    pykrx는 내부적으로 KRX endpoint를 호출하므로 회사 방화벽 등으로
    KRX 자체가 막혀 있으면 함께 실패한다. 그 경우 자세한 에러를 띄운다.
    """
    try:
        from pykrx import stock
    except ImportError as e:
        raise RuntimeError(f"pykrx 라이브러리가 설치되어 있지 않습니다: {e}")

    from datetime import datetime, timedelta

    # 여러 호출 방식을 시도 — pykrx 버전·KRX 응답 형태에 따라 동작 차이
    errors: list[str] = []

    def _try_call(label, fn):
        try:
            result = fn()
            if result:
                return result
            errors.append(f"{label}: 빈 결과")
        except Exception as e:
            errors.append(f"{label}: {type(e).__name__}: {e}")
        return None

    # 시도할 호출 (단순한 것부터)
    attempts = []
    for market in ["KOSPI", "KOSDAQ"]:
        attempts.append(("date 없이 " + market,
                         lambda m=market: stock.get_market_ticker_list(market=m)))
    # 어제·오늘·일주일 전 날짜로
    for delta in [0, 1, 3, 7]:
        d = (datetime.now() - timedelta(days=delta)).strftime("%Y%m%d")
        for market in ["KOSPI", "KOSDAQ"]:
            attempts.append((f"date={d} {market}",
                             lambda dd=d, mm=market: stock.get_market_ticker_list(dd, market=mm)))

    # 시장별 최초 성공한 호출 결과 사용
    tickers_by_market: dict[str, list] = {}
    for label, fn in attempts:
        # market 정보를 label에서 다시 빼오기
        if "KOSPI" in label and "KOSPI" not in tickers_by_market:
            r = _try_call(label, fn)
            if r:
                tickers_by_market["KOSPI"] = r
        elif "KOSDAQ" in label and "KOSDAQ" not in tickers_by_market:
            r = _try_call(label, fn)
            if r:
                tickers_by_market["KOSDAQ"] = r

    if not tickers_by_market:
        raise RuntimeError(
            "pykrx에서 종목 목록을 받지 못함 — KRX 사이트 자체가 차단된 듯합니다. "
            "회사 방화벽/프록시 설정을 확인하거나, 시도 로그: "
            + " | ".join(errors[-5:])
        )

    rows: list[dict] = []
    for market, tickers in tickers_by_market.items():
        for t in tickers:
            try:
                name = stock.get_market_ticker_name(t)
            except Exception:
                name = ""
            rows.append({
                "isin":       "",
                "stock_code": str(t).zfill(6),
                "name":       name,
                "name_full":  name,
                "name_eng":   "",
                "market":     market,
                "secgroup":   "보통주",
            })

    if not rows:
        raise RuntimeError("pykrx: ticker_list 는 있지만 행 생성 실패")

    return pd.DataFrame(rows)


# ── 매칭 ──────────────────────────────────────────────────────────────────────
def _build_name_index(master: pd.DataFrame) -> dict[str, dict]:
    """정규화된 회사명 → master row dict (가장 짧은 매칭이 우선되도록 약식명 먼저)."""
    idx: dict[str, dict] = {}

    # 1) 약식명 (가장 흔히 쓰는 이름)
    for _, row in master.iterrows():
        for col in ("name", "name_full", "name_eng"):
            key = normalize_company(row[col])
            if key and key not in idx:
                idx[key] = row.to_dict()
    return idx


def match_companies(
    company_names: list[str],
    master: pd.DataFrame,
) -> pd.DataFrame:
    """
    회사명 리스트 → 매칭 결과 DataFrame.

    매칭 우선순위:
      1) 정규화된 입력명 ↔ 정규화된 마스터명 완전 일치
      2) 부분 일치 (입력이 마스터의 부분문자열이거나 그 반대) — 가장 긴 매칭 채택

    반환 컬럼:
        input_name   : 입력 회사명 (원본 그대로)
        normalized   : 정규화된 입력명
        isin         : 매칭된 ISIN (실패시 빈 문자열)
        stock_code   : 매칭된 단축코드
        matched_name : 마스터의 약식명
        market       : KOSPI/KOSDAQ/KONEX
        status       : 'exact' | 'partial' | 'none'
    """
    name_index = _build_name_index(master)
    norm_keys  = list(name_index.keys())   # 부분매칭용

    results: list[dict] = []
    for raw in company_names:
        norm = normalize_company(raw)
        rec = {
            "input_name":   raw,
            "normalized":   norm,
            "isin":         "",
            "stock_code":   "",
            "matched_name": "",
            "market":       "",
            "status":       "none",
        }
        if not norm:
            results.append(rec)
            continue

        # 1) 완전 일치
        hit = name_index.get(norm)
        if hit:
            rec.update({
                "isin":         hit.get("isin", ""),
                "stock_code":   hit.get("stock_code", ""),
                "matched_name": hit.get("name", ""),
                "market":       hit.get("market", ""),
                "status":       "exact",
            })
            results.append(rec)
            continue

        # 2) 부분 일치 — 가장 긴 공통 부분 채택
        best_key:  str | None = None
        best_len:  int = 0
        for k in norm_keys:
            if k == norm:
                continue
            if norm in k or k in norm:
                # 짧은 norm이 긴 마스터에 포함될 때 매칭 신뢰도 낮으므로
                # 양쪽 길이 차가 너무 크면 (3배 이상) 스킵
                ratio = min(len(norm), len(k)) / max(len(norm), len(k))
                if ratio < 0.5:
                    continue
                if len(k) > best_len:
                    best_key = k
                    best_len = len(k)

        if best_key:
            hit = name_index[best_key]
            rec.update({
                "isin":         hit.get("isin", ""),
                "stock_code":   hit.get("stock_code", ""),
                "matched_name": hit.get("name", ""),
                "market":       hit.get("market", ""),
                "status":       "partial",
            })

        results.append(rec)

    return pd.DataFrame(results)


# ── KRX 없이 DART 단축코드만으로 ISIN 자동 산출 (ISO 6166 알고리즘) ───────────
def isin_compute_from_dart_match(
    dart_match: pd.DataFrame,
    dart_company_info: dict | None = None,
) -> pd.DataFrame:
    """
    KRX 마스터를 못 받았을 때 fallback — DART 단축코드만으로 ISIN 자동 산출.

    `compute_isin_from_stock_code` 의 ISO 6166 알고리즘으로 12자리 ISIN 생성.
    보통주 가정(`KR7 + 6자리 + 00 + check`). 우선주·신주·펀드 등은 KR7 외 분류
    코드가 다를 수 있으므로 사용자가 ④ 단계의 수동 입력으로 보정 가능.

    반환 컬럼은 `isin_from_dart_match` 와 동일:
        input_name, jurir_no, corp_code, stock_code, isin, matched_name,
        market, status, source
    """
    info_map = dart_company_info or {}
    rows: list[dict] = []
    for _, dr in dart_match.iterrows():
        inp   = dr.get("input_name", "")
        cc    = str(dr.get("corp_code", "")).strip()
        sc    = str(dr.get("stock_code", "")).strip()
        jurir = (info_map.get(cc) or {}).get("jurir_no", "") if cc else ""

        if not cc:
            rows.append({
                "input_name":   inp, "jurir_no":   "",   "corp_code": "",
                "stock_code":   "",  "isin":       "",
                "matched_name": "",  "market":     "",
                "status":       "none",
                "source":       "DART 매칭 실패",
            })
            continue

        if not sc:
            rows.append({
                "input_name":   inp, "jurir_no":   jurir, "corp_code": cc,
                "stock_code":   "",  "isin":       "",
                "matched_name": dr.get("corp_name", ""), "market": "",
                "status":       "none",
                "source":       "DART (비상장)",
            })
            continue

        sc_norm = sc.zfill(6)
        isin = compute_isin_from_stock_code(sc_norm)
        rows.append({
            "input_name":   inp,
            "jurir_no":     jurir,
            "corp_code":    cc,
            "stock_code":   sc_norm,
            "isin":         isin,
            "matched_name": dr.get("corp_name", ""),
            "market":       "",
            "status":       "exact" if isin else "none",
            "source":       "자동 산출 (ISO 6166)" if isin else "산출 실패",
        })

    return pd.DataFrame(rows)


# ── 사용자가 직접 업로드하는 매핑 파일 ─────────────────────────────────────────
def load_user_master(file_or_df) -> pd.DataFrame:
    """
    사용자 업로드 매핑 파일을 KRX 마스터와 동일한 형식으로 변환.

    허용 컬럼 (대소문자/순서 무관, 일부 누락 OK):
      - 회사명: 'name' / '회사명' / 'company' / 'company_name' / 'brand'
      - 종목코드: 'stock_code' / '종목코드' / 'ticker' / 'code'
      - ISIN: 'isin' / 'ISIN' / 'security_code'
      - 시장: 'market' / '시장'

    필수: 회사명 + (종목코드 또는 ISIN) 중 하나.
    """
    if isinstance(file_or_df, pd.DataFrame):
        df = file_or_df.copy()
    else:
        name = getattr(file_or_df, "name", str(file_or_df))
        if name.lower().endswith(".csv"):
            df = pd.read_csv(file_or_df)
        else:
            df = pd.read_excel(file_or_df)

    if df.empty:
        raise ValueError("업로드한 매핑 파일이 비어 있습니다.")

    # 컬럼명 정규화
    col_map: dict[str, str] = {}
    for c in df.columns:
        cl = str(c).strip().lower().replace("_", "")
        if cl in ("name", "회사명", "company", "companyname", "brand", "brandname"):
            col_map[c] = "name"
        elif cl in ("stockcode", "종목코드", "ticker", "code", "단축코드"):
            col_map[c] = "stock_code"
        elif cl in ("isin", "isucd", "securitycode"):
            col_map[c] = "isin"
        elif cl in ("market", "시장"):
            col_map[c] = "market"

    df = df.rename(columns=col_map)

    if "name" not in df.columns:
        raise ValueError(
            "매핑 파일에 회사명 컬럼이 없습니다. "
            "헤더에 'name' / '회사명' / 'company' 중 하나가 필요합니다."
        )
    if "stock_code" not in df.columns and "isin" not in df.columns:
        raise ValueError(
            "매핑 파일에 종목코드 또는 ISIN 컬럼이 필요합니다. "
            "헤더에 'stock_code' / '종목코드' 또는 'isin' 중 하나가 필요합니다."
        )

    # 빠진 컬럼 채움
    for col in ["isin", "stock_code", "name", "name_full", "name_eng", "market", "secgroup"]:
        if col not in df.columns:
            df[col] = ""

    df = df[["isin", "stock_code", "name", "name_full", "name_eng", "market", "secgroup"]]
    df = df.astype(str)
    df["isin"]       = df["isin"].str.strip()
    df["stock_code"] = df["stock_code"].str.strip().str.zfill(6)
    df["name"]       = df["name"].str.strip()
    # name_full 이 비었으면 name 으로 채워서 매칭 hit rate 향상
    df.loc[df["name_full"].str.strip() == "", "name_full"] = df["name"]
    return df


# ── ISIN 자동 산출 (KRX 호출 없이) ────────────────────────────────────────────
def compute_isin_from_stock_code(stock_code: str, security_type: str = "00") -> str:
    """
    6자리 단축코드 → 12자리 ISIN (ISO 6166).

    구조: `KR` (2자) + `7` (한국 발행) + 단축코드(6) + 종류 분류(2) + check digit(1) = 12자리
      예: 005930 (삼성전자 보통주) → KR + 7 + 005930 + 00 + 3 → KR7005930003

    security_type 두 자리:
      "00" → 보통주 (가장 흔함)
      "05" / "55" / 다른 값 → 우선주·신주·전환사채 등 (정확하지 않을 수 있음)

    체크디지트는 ISO 6166:
      1) 알파벳 → 숫자 (A=10, B=11, ..., Z=35) — 두 자릿수로 분해
      2) 결과 숫자열에서 끝에서부터 짝수 인덱스(0,2,4 ...) 자리를 2배
      3) 모든 자릿수 합 → (10 - sum%10) % 10
    """
    sc = str(stock_code or "").strip()
    if not sc:
        return ""
    sc = sc.zfill(6)
    if not sc.isdigit() or len(sc) != 6:
        return ""

    body = "KR7" + sc + security_type     # 11자리 (KR + 7 + 6자리 + 2자리)

    # 1) 알파벳 → 두 자릿수 숫자열로 분해
    digits: list[int] = []
    for ch in body:
        if ch.isdigit():
            digits.append(int(ch))
        elif ch.isalpha():
            v = ord(ch.upper()) - ord("A") + 10
            digits.append(v // 10)
            digits.append(v % 10)
        else:
            return ""   # 잘못된 문자

    # 2) 끝에서부터 짝수 인덱스(0,2,4...)는 2배, 자릿수 합산
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:
            doubled = d * 2
            total += (doubled // 10) + (doubled % 10)
        else:
            total += d

    # 3) check digit
    check = (10 - total % 10) % 10
    return body + str(check)   # 12자리


# ── DART 결과의 단축코드로 KRX 마스터에서 ISIN 즉시 lookup ─────────────────────
def isin_from_dart_match(
    dart_match: pd.DataFrame,
    krx_master: pd.DataFrame,
    dart_company_info: dict | None = None,
) -> pd.DataFrame:
    """
    DART 매칭 결과 → KRX ISIN.

    핵심 키 컬럼은 **법인등록번호(jurir_no)** 이며, 사용자 화면에서 매칭의 키로
    노출된다. 단, KRX 마스터에는 jurir_no 가 없으므로 실제 lookup 경로는:
        jurir_no → corp_code → stock_code (DART) → ISIN (KRX)

    Args:
        dart_match        : ③ DART 매칭 결과 (input_name, corp_code, stock_code 포함)
        krx_master        : KRX 종목 마스터 (stock_code → isin)
        dart_company_info : {corp_code: {jurir_no, bizr_no, ...}}  ③에서 일괄 조회한 결과

    Returns DataFrame:
        input_name   : 원본 입력 회사명
        jurir_no     : 법인등록번호 (13자리, DART company.json 에서)
        corp_code    : DART corp_code (8자리)
        stock_code   : KRX 단축코드 (6자리, 상장사만)
        isin         : KRX ISIN (12자리)
        matched_name : KRX 약식명 / DART 한글명
        market       : KOSPI / KOSDAQ / KONEX
        status       : 'exact' / 'none'
        source       : 'DART→KRX' / 'DART (비상장)' / 'DART (KRX 미보유)' / 'DART 매칭 실패'
    """
    info_map = dart_company_info or {}

    # KRX 마스터 인덱스 (stock_code → row dict)
    idx: dict[str, dict] = {}
    for _, r in krx_master.iterrows():
        sc = str(r.get("stock_code", "")).strip()
        if not sc:
            continue
        sc_norm = sc.zfill(6)
        if sc_norm not in idx:
            idx[sc_norm] = r.to_dict()

    rows: list[dict] = []
    for _, dr in dart_match.iterrows():
        inp = dr.get("input_name", "")
        cc  = str(dr.get("corp_code", "")).strip()
        sc  = str(dr.get("stock_code", "")).strip()
        jurir = (info_map.get(cc) or {}).get("jurir_no", "") if cc else ""

        # DART 매칭 자체가 실패한 경우 (corp_code 도 없음)
        if not cc:
            rows.append({
                "input_name":   inp,
                "jurir_no":     "",
                "corp_code":    "",
                "stock_code":   "",
                "isin":         "",
                "matched_name": "",
                "market":       "",
                "status":       "none",
                "source":       "DART 매칭 실패",
            })
            continue

        if not sc:
            # corp_code 있지만 stock_code 비어있음 — 비상장
            rows.append({
                "input_name":   inp,
                "jurir_no":     jurir,
                "corp_code":    cc,
                "stock_code":   "",
                "isin":         "",
                "matched_name": dr.get("corp_name", ""),
                "market":       "",
                "status":       "none",
                "source":       "DART (비상장)",
            })
            continue

        sc_norm = sc.zfill(6)
        row = idx.get(sc_norm)
        if row:
            rows.append({
                "input_name":   inp,
                "jurir_no":     jurir,
                "corp_code":    cc,
                "stock_code":   sc_norm,
                "isin":         row.get("isin", ""),
                "matched_name": row.get("name", "") or dr.get("corp_name", ""),
                "market":       row.get("market", ""),
                "status":       "exact",
                "source":       "DART→KRX",
            })
        else:
            # stock_code 는 있지만 KRX 에 없음 — 상장폐지 등
            rows.append({
                "input_name":   inp,
                "jurir_no":     jurir,
                "corp_code":    cc,
                "stock_code":   sc_norm,
                "isin":         "",
                "matched_name": dr.get("corp_name", ""),
                "market":       "",
                "status":       "none",
                "source":       "DART (KRX 미보유)",
            })

    return pd.DataFrame(rows)


# ── 편의 함수 ─────────────────────────────────────────────────────────────────
def lookup_summary(match_df: pd.DataFrame) -> dict:
    """매칭 결과 요약 통계 — Streamlit metric 등에 사용."""
    total = len(match_df)
    exact = int((match_df["status"] == "exact").sum())
    part  = int((match_df["status"] == "partial").sum())
    none_ = int((match_df["status"] == "none").sum())
    return {
        "total":   total,
        "exact":   exact,
        "partial": part,
        "none":    none_,
        "rate":    (exact + part) / total if total else 0.0,
    }
