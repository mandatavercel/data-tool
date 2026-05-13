"""Earnings Intelligence — quarterly POS aggregation + DART comparison by company"""
import io
import re
import ssl
import math
import zipfile
import urllib.request
import urllib.parse
import json
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode    = ssl.CERT_NONE

# DART API 호출 동시성 — 분당 1000건 제한 고려해 보수적으로 설정
_DART_MAX_WORKERS = 16   # 8 → 16 (DART rate limit 1000회/분 안에서)

from modules.common.foundation import _parse_dates
from modules.analysis.guides import render_guide
from modules.common.core.metrics import (
    calculate_correlation,
    calculate_lag_correlation,
    calculate_tracking_ratio,
    calculate_qoq,
    calculate_yoy,
)
from modules.common.core.normalizer import infer_amount_unit
from modules.common.core.audit import compute_module_audit, check_growth_sanity, check_sample_size_sanity
from modules.common.core.result import enrich_result



def run_earnings_intel(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    sales_col   = role_map.get("sales_amount")
    date_col    = role_map.get("transaction_date")
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
    df = df.dropna(subset=["_sales", "_date"])
    n_valid   = len(df)
    _date_min = str(df["_date"].min().date()) if n_valid > 0 else None
    _date_max = str(df["_date"].max().date()) if n_valid > 0 else None

    # Quarterly aggregation
    df["_quarter"] = df["_date"].dt.to_period("Q")
    group_keys = ["_quarter"]
    if company_col:
        group_keys.insert(0, company_col)

    qtr_agg = df.groupby(group_keys, as_index=False)["_sales"].sum()

    if company_col:
        qtr_agg.columns = ["company", "quarter", "pos_sales"]
        qtr_agg = qtr_agg.sort_values(["company", "quarter"])
        qtr_agg["qoq_pct"] = (
            qtr_agg.groupby("company")["pos_sales"].pct_change() * 100
        ).round(1)
        qtr_agg["yoy_pct"] = (
            qtr_agg.groupby("company")["pos_sales"]
            .pct_change(periods=4) * 100
        ).round(1)
    else:
        qtr_agg.columns = ["quarter", "pos_sales"]
        qtr_agg = qtr_agg.sort_values("quarter")
        qtr_agg["qoq_pct"] = qtr_agg["pos_sales"].pct_change() * 100
        qtr_agg["yoy_pct"] = qtr_agg["pos_sales"].pct_change(periods=4) * 100

    qtr_agg["quarter_str"] = qtr_agg["quarter"].astype(str)

    warnings: list[str] = []
    dart_by_company: dict[str, pd.DataFrame] = {}
    corp_code_map:   dict[str, str]          = {}

    dart_key = params.get("dart_api_key", "").strip()

    manual_stock: dict[str, str] = {}
    for line in params.get("manual_mapping", "").splitlines():
        if ":" in line:
            cname, _, scode = line.partition(":")
            cname = cname.strip()
            scode = scode.strip().lstrip("A").zfill(6)
            if cname and scode:
                manual_stock[cname] = scode

    # 인터랙티브 UI에서 사용자가 직접 선택한 매핑 — 최우선 (자동 매칭 덮어씀)
    user_dart_map: dict[str, str] = dict(params.get("dart_company_mapping", {}) or {})

    if dart_key:
        try:
            with st.spinner("DART 기업 목록 조회 중... (캐시되면 다음 실행부터는 즉시)"):
                name_map, stock_map = _fetch_corp_code_map(dart_key)
        except Exception as e:
            name_map = stock_map = {}
            err_msg = str(e)
            # 네트워크 차단 케이스 — 친절한 안내로 변환
            if any(k in err_msg for k in ("ConnectionError", "Max retries", "timed out",
                                            "Connection reset", "Name or service not known",
                                            "URLError", "getaddrinfo")):
                warnings.append(
                    "DART 서버 접속 차단됨 — 네트워크 환경에서 opendart.fss.or.kr 접근 불가. "
                    "POS 분석만 표시됩니다 (모바일 핫스팟·다른 네트워크에서 재시도하면 DART 비교 가능)."
                )
            else:
                warnings.append(f"DART 기업목록 조회 실패: {err_msg[:200]}")

        if name_map or stock_map:
            companies = (
                [str(c) for c in df[company_col].dropna().unique().tolist()]
                if company_col else []
            )
            if not companies:
                warnings.append("company_name 역할 없음 — 회사 매칭 불가")
            else:
                stock_col = role_map.get("stock_code") or role_map.get("security_code")
                if not stock_col:
                    stock_col = _auto_detect_code_col(df)

                company_to_stock: dict[str, str] = {}
                if stock_col and company_col and stock_col in df.columns:
                    for _, grp in df.groupby(company_col):
                        cname = str(grp[company_col].iloc[0])
                        for raw_val in grp[stock_col].dropna().astype(str):
                            sc = _extract_stock_code(raw_val)
                            if sc:
                                company_to_stock[cname] = sc
                                break

                for company in companies:
                    # 1) 사용자가 UI에서 직접 선택한 매핑 (최우선)
                    if company in user_dart_map:
                        corp_code_map[company] = user_dart_map[company]
                        continue
                    # 2) 텍스트 manual + 자동 매칭
                    sc   = manual_stock.get(company) or company_to_stock.get(company)
                    code = _match_company(company, name_map, stock_map, company_stock=sc)
                    if code:
                        corp_code_map[company] = code

                n_user   = sum(1 for c in companies if c in user_dart_map and c in corp_code_map)
                n_manual = sum(1 for c in companies if c in manual_stock and c in corp_code_map and c not in user_dart_map)
                n_auto   = len(corp_code_map) - n_user - n_manual
                n_unmapped = len(companies) - len(corp_code_map)
                st.caption(
                    f"📊 DART 매칭 현황 — 🔵 사용자 선택 {n_user}개 · "
                    f"⚙️ 수동 텍스트 {n_manual}개 · 🟢 자동 매칭 {n_auto}개 · 🔴 미매핑 {n_unmapped}개"
                )

                if not corp_code_map:
                    warnings.append(
                        "POS 회사명이 DART 기업 목록과 매칭되지 않음 — "
                        "파라미터에서 수동 종목코드를 입력하세요"
                    )
                else:
                    q_min = qtr_agg["quarter"].min()
                    q_max = qtr_agg["quarter"].max()
                    # POS 기간 정확히 (앞뒤 ±0년) — 속도 우선. 비교 분석에 추가 연도 불필요.
                    dart_year_min = q_min.year
                    dart_year_max = q_max.year

                    # 회사별 fetch를 병렬화 (캐시 히트면 즉시 반환, 미스면 동시 호출)
                    def _fetch_one(item: tuple[str, str]):
                        company, corp_code = item
                        try:
                            ddf = _fetch_dart(dart_key, corp_code, dart_year_min, dart_year_max)
                            return company, ddf, None
                        except Exception as e:
                            return company, None, f"{company} DART 조회 오류: {str(e)[:40]}"

                    n_workers = min(_DART_MAX_WORKERS, max(1, len(corp_code_map)))
                    n_total   = len(corp_code_map)
                    progress_bar = st.progress(0.0, text=f"DART 공시 수집 시작... (회사 {n_total}개 · {n_workers}개 동시)")
                    n_done = 0
                    with ThreadPoolExecutor(max_workers=n_workers) as ex:
                        futures = [ex.submit(_fetch_one, item) for item in corp_code_map.items()]
                        for fut in as_completed(futures):
                            company, ddf, err = fut.result()
                            n_done += 1
                            progress_bar.progress(
                                n_done / n_total,
                                text=f"DART 공시 수집 중... ({n_done}/{n_total}) — {company[:20]}",
                            )
                            if err:
                                warnings.append(err)
                            elif ddf is not None and not ddf.empty:
                                dart_by_company[company] = ddf
                    progress_bar.empty()

                    if not dart_by_company:
                        warnings.append("DART 공시 데이터 없음 (기간 또는 공시 확인 필요)")
    else:
        warnings.append("DART API key 없음 — POS 분기 집계만 제공")

    n_quarters = int(qtr_agg["quarter"].nunique())
    latest_qoq = None
    s = qtr_agg["qoq_pct"].dropna() if "qoq_pct" in qtr_agg.columns else pd.Series([], dtype=float)
    if not s.empty:
        latest_qoq = float(s.iloc[-1])

    # ── Tracking Quality (헤드라인) + Coverage (보조) 계산 ──────────────────
    # 편의점 같은 channel-slice POS는 raw Coverage가 의미 없음.
    # 투자자가 진짜 보고 싶은 건 Direction Match · Correlation · Stability.
    universe_cov = None
    company_cov: dict[str, float] = {}
    company_tracking: dict[str, dict] = {}   # {company: {direction_match, correlation, stability, quality, n}}
    if dart_by_company and "company" in qtr_agg.columns:
        total_pos  = 0.0
        total_dart = 0.0
        for cname, ddf in dart_by_company.items():
            co_pos = qtr_agg[qtr_agg["company"] == cname][["quarter_str", "pos_sales"]]
            if co_pos.empty or ddf.empty:
                continue
            merged = co_pos.merge(ddf[["quarter_str", "dart_sales"]],
                                  on="quarter_str", how="inner")
            merged = merged[merged["dart_sales"] > 0].sort_values("quarter_str").reset_index(drop=True)
            if merged.empty:
                continue

            # Coverage (보조)
            co_pos_sum  = float(merged["pos_sales"].sum())
            co_dart_sum = float(merged["dart_sales"].sum())
            total_pos  += co_pos_sum
            total_dart += co_dart_sum
            company_cov[cname] = round(co_pos_sum / co_dart_sum * 100, 2)

            # Tracking Quality (헤드라인) — 최소 4분기 필요
            if len(merged) < 4:
                continue
            merged["pos_qoq"]  = merged["pos_sales"].pct_change() * 100
            merged["dart_qoq"] = merged["dart_sales"].pct_change() * 100
            sub = merged.dropna(subset=["pos_qoq", "dart_qoq"])
            if len(sub) < 3:
                continue

            # 1) Direction Match — 부호 일치율
            dm = float((np.sign(sub["pos_qoq"]) == np.sign(sub["dart_qoq"])).mean() * 100)

            # 2) POS-DART Correlation (Pearson r)
            try:
                corr_val = float(calculate_correlation(sub["pos_qoq"], sub["dart_qoq"]))
            except Exception:
                corr_val = 0.0
            if math.isnan(corr_val):
                corr_val = 0.0

            # 3) Stability — tracking ratio CoV의 역수 (변동계수 낮을수록 안정)
            ratio = (merged["pos_sales"] / merged["dart_sales"]).replace([np.inf, -np.inf], np.nan).dropna()
            if len(ratio) >= 2 and ratio.mean() > 0:
                cov = float(ratio.std() / ratio.mean())
                stab = 1.0 / (1.0 + cov)
            else:
                stab = 0.0

            # Composite (0~100): direction 50% + |corr| 30% + stability 20%
            quality = (
                dm * 0.50 +
                abs(corr_val) * 100 * 0.30 +
                stab * 100 * 0.20
            )

            company_tracking[cname] = {
                "direction_match": round(dm, 1),
                "correlation":     round(corr_val, 3),
                "stability":       round(stab, 3),
                "quality":         round(float(quality), 1),
                "n_quarters":      len(sub),
            }

        if total_dart > 0:
            universe_cov = round(total_pos / total_dart * 100, 2)

    # Universe Tracking Quality (단순 평균)
    if company_tracking:
        u_dm   = float(np.mean([v["direction_match"] for v in company_tracking.values()]))
        u_corr = float(np.mean([abs(v["correlation"])  for v in company_tracking.values()]))
        u_stab = float(np.mean([v["stability"]       for v in company_tracking.values()]))
        u_qual = float(np.mean([v["quality"]         for v in company_tracking.values()]))
    else:
        u_dm = u_corr = u_stab = u_qual = None

    metrics = {
        "n_quarters":       n_quarters,
        "has_dart":         bool(dart_by_company),
        "n_dart_companies": len(dart_by_company),
        "latest_qoq":       round(float(latest_qoq), 1) if latest_qoq is not None else None,
        # Tracking Quality (헤드라인)
        "tracking_quality_avg":  round(u_qual, 1) if u_qual is not None else None,
        "direction_match_avg":   round(u_dm,   1) if u_dm   is not None else None,
        "correlation_avg":       round(u_corr, 3) if u_corr is not None else None,
        "stability_avg":         round(u_stab, 3) if u_stab is not None else None,
        "company_tracking":      company_tracking,
        # Coverage (보조)
        "universe_coverage_pct": universe_cov,
        "company_coverage":      company_cov,
    }

    status  = "warning" if warnings else "success"
    message = " | ".join(warnings) if warnings else (
        f"{n_quarters}개 분기 집계 완료 · DART {len(dart_by_company)}개사 연동"
        if dart_by_company else f"{n_quarters}개 분기 POS 집계 완료"
    )

    bs  = check_sample_size_sanity(n_quarters, min_required=6)
    bs += check_growth_sanity(qtr_agg["qoq_pct"].dropna() if "qoq_pct" in qtr_agg.columns else None)

    audit, conf = compute_module_audit(
        n_original=n_original,
        n_valid=n_valid,
        role_map=role_map,
        used_roles=["transaction_date", "sales_amount", "company_name"],
        date_min=_date_min,
        date_max=_date_max,
        formula="분기별 POS 집계 + DART YTD→분기 변환 + QoQ / YoY 성장률",
        agg_unit="분기",
        n_computable=n_quarters,
        n_periods=n_quarters,
        business_checks=bs,
    )

    result = {
        "status":           status,
        "message":          message,
        "data":             qtr_agg,
        "metrics":          metrics,
        "_dart_by_company": dart_by_company,
        "_corp_code_map":   corp_code_map,
    }
    return enrich_result(result, audit, conf)


# ── DART helpers ──────────────────────────────────────────────────────────────

def _dart_get(url: str, timeout: int = 20, retries: int = 3) -> bytes:
    """DART 호출 — requests + urllib fallback (단순 구성).

    requests가 깨지는 환경에서 urllib이 잡아주는 패턴 유지.
    connect/read retry는 명시하지 않음 — urllib3 default 동작 사용.

    시도 순서:
      1. requests (verify=True)  — modern TLS
      2. requests (verify=False) — SSL 검증 우회
      3. urllib   (verify=False) — 단순 연결, 한 번만
    """
    errors: list[str] = []

    # 1, 2) requests 두 가지 verify 모드
    try:
        import requests
        from requests.adapters import HTTPAdapter
        try:
            from urllib3.util.retry import Retry
        except ImportError:
            from requests.packages.urllib3.util.retry import Retry  # type: ignore

        session = requests.Session()
        retry_cfg = Retry(
            total=retries,
            backoff_factor=0.7,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        session.mount("https://", HTTPAdapter(max_retries=retry_cfg))

        for verify in (True, False):
            try:
                r = session.get(url, timeout=timeout, verify=verify,
                                headers={"User-Agent": "Mozilla/5.0 (compatible; alt-data-tool)"})
                r.raise_for_status()
                return r.content
            except Exception as e:
                errors.append(f"requests(verify={verify}): {type(e).__name__}: {str(e)[:80]}")
    except ImportError as e:
        errors.append(f"requests not available: {e}")

    # 3) urllib fallback — 한 번만 시도, 단순한 호출
    import urllib.request
    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 (compatible; alt-data-tool)"}
        )
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return resp.read()
    except Exception as e:
        errors.append(f"urllib(verify=False): {type(e).__name__}: {str(e)[:80]}")

    raise RuntimeError("DART 호출 실패 (모든 경로): " + " || ".join(errors))


@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_corp_code_map(api_key: str) -> tuple[dict[str, str], dict[str, str]]:
    """DART 전체 기업 목록 조회 (~3MB zip).

    24시간 캐시. 같은 api_key로 재호출 시 네트워크 호출 없이 dict 반환.
    """
    url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}"
    raw = _dart_get(url, timeout=30, retries=3)
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        xml_name = next(n for n in z.namelist() if n.endswith(".xml"))
        xml_data = z.read(xml_name)
    root     = ET.fromstring(xml_data)
    name_map:  dict[str, str] = {}
    stock_map: dict[str, str] = {}
    for item in root.iter("list"):
        name  = (item.findtext("corp_name")  or "").strip()
        code  = (item.findtext("corp_code")  or "").strip()
        stock = (item.findtext("stock_code") or "").strip()
        if name and code:
            name_map[name] = code
        if stock and code:
            stock_map[stock.zfill(6)] = code
    return name_map, stock_map


_RE_ISIN    = re.compile(r'^KR[0-9A-Z](\d{6})', re.IGNORECASE)
_RE_A_CODE  = re.compile(r'^A(\d{6})$')
_RE_SUFFIX  = re.compile(
    r"[_\s]*(ALL|CO\.?\s*LTD\.?|CORP\.?|INC\.?|LLC|PLC|LTD\.?|CO\.?)[\s.]*$",
    re.IGNORECASE,
)

def _extract_stock_code(val: str) -> str | None:
    s = str(val).strip().replace("-", "").replace(" ", "")
    m = _RE_ISIN.match(s)
    if m:
        return m.group(1)
    m = _RE_A_CODE.match(s)
    if m:
        return m.group(1)
    digits = re.sub(r"\D", "", s)
    if len(digits) == 10:
        return digits[1:7]
    if len(digits) == 6:
        return digits
    return None


def _auto_detect_code_col(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        sample = df[col].dropna().astype(str).head(30)
        hit = sum(1 for v in sample if _extract_stock_code(v) is not None)
        if hit >= len(sample) * 0.5:
            return col
    return None


def _normalize(name: str) -> str:
    prev = None
    while prev != name:
        prev = name
        name = _RE_SUFFIX.sub("", name).strip()
    return re.sub(r"\s+", " ", name).strip().lower()


def _match_company(
    company: str,
    name_map: dict[str, str],
    stock_map: dict[str, str],
    company_stock: str | None = None,
) -> str | None:
    if company_stock:
        sc = _extract_stock_code(str(company_stock))
        if sc is None:
            sc = str(company_stock).strip().zfill(6)
        if sc in stock_map:
            return stock_map[sc]

    norm_company = _normalize(company)

    for dart_name, code in name_map.items():
        if _normalize(dart_name) == norm_company:
            return code

    best_code: str | None = None
    best_len  = 0
    for dart_name, code in name_map.items():
        d = _normalize(dart_name)
        if not d:
            continue
        if d in norm_company or norm_company in d:
            matched_len = max(len(d), len(norm_company))
            if matched_len > best_len:
                best_len  = matched_len
                best_code = code

    return best_code


@st.cache_data(ttl=21600, show_spinner=False)
def _fetch_dart(api_key: str, corp_code: str, year_min: int, year_max: int) -> pd.DataFrame:
    """DART 분기 매출 조회. 누적(YTD) 값을 분기 단독값으로 변환.

    6시간 캐시. 같은 (api_key, corp_code, year_range) 호출 시 캐시 히트.

    DART API reprt_code별 thstrm_amount 성격:
      11013 (1분기보고서) → Q1 standalone
      11012 (반기보고서)  → Q1+Q2 누적 (H1)
      11014 (3분기보고서) → Q1+Q2+Q3 누적 (9M)
      11011 (사업보고서)  → 전체 연간 누적 (Annual)

    변환:
      Q1_actual = Q1 (standalone)
      Q2_actual = H1 - Q1
      Q3_actual = 9M - H1
      Q4_actual = Annual - 9M
    """
    # Collect raw YTD amounts per year
    # timeout 8s (정상 응답 1~3초), retries 1회 — 비상장사 abort 빠르게
    # 연속 실패 6회면 그 회사 abort (1.5년치 데이터 없으면 포기)
    raw: dict[int, dict[str, float]] = {}
    consecutive_failures = 0
    MAX_CONSEC_FAILURES = 6   # 12 → 6 (비상장사 빠른 컷)

    for year in range(year_min, year_max + 1):
        raw[year] = {}
        if consecutive_failures >= MAX_CONSEC_FAILURES:
            break
        for reprt_code in ["11013", "11012", "11014", "11011"]:
            if consecutive_failures >= MAX_CONSEC_FAILURES:
                break
            found = False
            for fs_div in ["OFS", "CFS"]:
                url = (
                    "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json?"
                    + urllib.parse.urlencode({
                        "crtfc_key":  api_key,
                        "corp_code":  corp_code,
                        "bsns_year":  str(year),
                        "reprt_code": reprt_code,
                        "fs_div":     fs_div,
                    })
                )
                try:
                    raw_bytes = _dart_get(url, timeout=8, retries=1)   # 15s/2 → 8s/1
                    data = json.loads(raw_bytes.decode())
                    consecutive_failures = 0  # 호출 자체는 성공 → 카운터 리셋
                    if data.get("status") != "000":
                        continue
                    for item in data.get("list", []):
                        if item.get("account_nm") in _REVENUE_ACCOUNTS:
                            if reprt_code in ("11012", "11014"):
                                amt_str = (item.get("thstrm_add_amount") or
                                           item.get("thstrm_amount", "0"))
                            else:
                                amt_str = item.get("thstrm_amount", "0")
                            raw[year][reprt_code] = float(
                                str(amt_str).replace(",", "") or 0
                            )
                            found = True
                            break
                except Exception:
                    consecutive_failures += 1
                    continue
                if found:
                    break

    rows = []
    for year in sorted(raw.keys()):
        rpts   = raw[year]
        q1_ytd  = rpts.get("11013")   # Q1 standalone
        h1_ytd  = rpts.get("11012")   # Q1+Q2 cumulative
        m9_ytd  = rpts.get("11014")   # Q1+Q2+Q3 cumulative
        ann_ytd = rpts.get("11011")   # Annual cumulative

        if q1_ytd is not None:
            rows.append({
                "quarter_str":    f"{year}Q1",
                "reprt_code":     "11013",
                "dart_sales_ytd": q1_ytd,
                "dart_sales":     q1_ytd,
                "ytd_converted":  False,
            })

        if h1_ytd is not None:
            q2 = (h1_ytd - q1_ytd) if q1_ytd is not None else None
            rows.append({
                "quarter_str":    f"{year}Q2",
                "reprt_code":     "11012",
                "dart_sales_ytd": h1_ytd,
                "dart_sales":     q2,
                "ytd_converted":  True,
            })

        if m9_ytd is not None:
            q3 = (m9_ytd - h1_ytd) if h1_ytd is not None else None
            rows.append({
                "quarter_str":    f"{year}Q3",
                "reprt_code":     "11014",
                "dart_sales_ytd": m9_ytd,
                "dart_sales":     q3,
                "ytd_converted":  True,
            })

        if ann_ytd is not None:
            q4 = (ann_ytd - m9_ytd) if m9_ytd is not None else None
            rows.append({
                "quarter_str":    f"{year}Q4",
                "reprt_code":     "11011",
                "dart_sales_ytd": ann_ytd,
                "dart_sales":     q4,
                "ytd_converted":  True,
            })

    if not rows:
        return pd.DataFrame()

    ddf = pd.DataFrame(rows)
    # Drop quarters where conversion wasn't possible (missing preceding report)
    ddf = ddf.dropna(subset=["dart_sales"])
    return ddf[["quarter_str", "dart_sales", "dart_sales_ytd", "reprt_code", "ytd_converted"]]


# ── Lead-signal helpers ───────────────────────────────────────────────────────

_LEAD_DAYS = {"11013": 45, "11012": 45, "11014": 45, "11011": 90}
_REVENUE_ACCOUNTS = {"매출액", "수익(매출액)", "수익", "영업수익", "매출"}

def _build_lead_table(pos_agg: pd.DataFrame, dart_df: pd.DataFrame) -> pd.DataFrame:
    """POS·DART 병합 + 선행 지표 계산 (분기 단독 매출 기준).

    dart_df는 POS 기간보다 확장된 전체 공시 시계열이어야 한다.
    QoQ/YoY를 전체 DART 시리즈로 먼저 계산하고 join해야
    POS 시작 분기의 성장률도 올바르게 산출된다.
    """
    if dart_df.empty or "qoq_pct" not in pos_agg.columns:
        return pd.DataFrame()

    # ── DART 전체 시리즈로 성장률 먼저 계산 (확장 기간 포함) ──────────────────
    dart_full = dart_df.sort_values("quarter_str").copy()
    dart_full["dart_qoq"] = calculate_qoq(dart_full["dart_sales"])
    dart_full["dart_yoy"] = calculate_yoy(dart_full["dart_sales"], freq="Q")

    pos_cols  = ["quarter_str", "pos_sales", "qoq_pct"]
    if "yoy_pct" in pos_agg.columns:
        pos_cols.append("yoy_pct")

    dart_cols = ["quarter_str", "dart_sales", "dart_qoq", "dart_yoy"]
    for col in ["reprt_code", "dart_sales_ytd", "ytd_converted"]:
        if col in dart_full.columns:
            dart_cols.append(col)

    merged = pd.merge(
        pos_agg[pos_cols].dropna(subset=["qoq_pct"]),
        dart_full[dart_cols],
        on="quarter_str", how="inner",
    ).sort_values("quarter_str").reset_index(drop=True)

    if len(merged) < 2:
        return pd.DataFrame()

    # Growth Gap: POS - DART (positive = POS growing faster)
    merged["qoq_gap"] = (merged["qoq_pct"] - merged["dart_qoq"]).round(1)
    if "yoy_pct" in merged.columns:
        merged["yoy_gap"] = (merged["yoy_pct"] - merged["dart_yoy"]).round(1)

    # POS Tracking Ratio: POS quarterly / DART standalone quarterly
    merged["tracking_ratio"] = calculate_tracking_ratio(
        merged["pos_sales"], merged["dart_sales"]
    ).round(1)

    # QoQ direction alignment
    pos_delta  = merged["pos_sales"].pct_change()
    dart_delta = merged["dart_sales"].pct_change()
    merged["direction_match"] = (pos_delta > 0) == (dart_delta > 0)

    # YoY direction alignment
    if "yoy_pct" in merged.columns:
        merged["yoy_direction_match"] = (merged["yoy_pct"] > 0) == (merged["dart_yoy"] > 0)

    if "reprt_code" in merged.columns:
        merged["lead_days"] = merged["reprt_code"].map(_LEAD_DAYS).fillna(45).astype(int)
    else:
        merged["lead_days"] = 45

    return merged.dropna(subset=["dart_qoq"]).reset_index(drop=True)


def _compute_lag_corrs(lead_tbl: pd.DataFrame, max_lag: int = 4) -> pd.DataFrame:
    """POS QoQ가 DART QoQ보다 몇 분기 앞서는지 lag별 Pearson r 계산.

    lag > 0 → POS가 lag분기 선행
    lag = 0 → 동행
    lag < 0 → DART가 선행 (POS가 후행)
    """
    lag_df = calculate_lag_correlation(
        lead_tbl["qoq_pct"].values,
        lead_tbl["dart_qoq"].values,
        max_lag=max_lag,
        min_lag=-2,
        name_a="POS",
        name_b="DART",
    )
    # Korean quarter-unit labels expected by the renderer
    def _label(row):
        lag = row["lag"]
        if lag > 0:
            return f"POS {lag}Q 선행"
        if lag < 0:
            return f"DART {-lag}Q 선행"
        return "동행 (lag 0)"
    lag_df["label"] = lag_df.apply(_label, axis=1)
    return lag_df


# ── Sanity Check ──────────────────────────────────────────────────────────────

def _sanity_check(lead_tbl: pd.DataFrame) -> dict:
    """
    POS vs DART 비율을 검증해 단위/범위/집계 오류 가능성을 진단한다.

    Returns
    -------
    dict with keys:
        severity      : "ok" | "caution" | "warning" | "critical"
        avg_tr        : float  평균 추적률(%)
        issues        : list[dict]  {type, label, desc}
        unit_hint     : str  단위 추정 메시지
        dart_unit     : str  DART 추정 단위
        pos_unit_note : str  POS 단위 주의사항
    """
    if lead_tbl.empty:
        return {"severity": "ok", "avg_tr": float("nan"), "issues": [],
                "unit_hint": "", "dart_unit": "원(KRW)", "pos_unit_note": ""}

    tr = lead_tbl["tracking_ratio"].replace([np.inf, -np.inf], np.nan).dropna()
    avg_tr  = float(tr.mean())  if not tr.empty else float("nan")
    max_tr  = float(tr.max())   if not tr.empty else float("nan")
    tr_std  = float(tr.std())   if len(tr) > 2   else float("nan")
    n_above = int((tr > 100).sum())
    n_neg   = int((tr < 0).sum())
    n_ext   = int(((tr > 200) | (tr < 0)).sum())

    issues: list[dict] = []

    # ── 단위 불일치 진단 (core.normalizer.infer_amount_unit 위임) ────────────
    pos_mean_val  = float(lead_tbl["pos_sales"].mean())
    dart_mean_val = float(lead_tbl["dart_sales"].mean())
    unit_info     = infer_amount_unit(pos_mean_val, dart_mean_val)
    avg_ratio_raw = unit_info["ratio"]

    dart_unit     = "원(KRW)"   # DART API 기본 단위
    pos_unit_note = ""

    if unit_info["is_mismatch"]:
        pos_unit_note = unit_info["note"]
        issues.append({"type": "unit_mismatch", "label": "단위 불일치",
                       "desc": unit_info["note"]})

    # ── 추적률 기반 진단 ────────────────────────────────────────────────────
    if not np.isnan(avg_tr):
        if avg_tr > 150:
            issues.append({"type": "channel_scope", "label": "채널 범위 초과",
                           "desc": f"평균 추적률 {avg_tr:.0f}% — POS 집계 범위가 공시 매출을 초과. "
                                   "채널 중복 집계 또는 총매출 vs 순매출 혼용 가능성"})
            issues.append({"type": "consolidated_separate", "label": "연결/별도 불일치",
                           "desc": "DART OFS(별도) 대신 CFS(연결)를 사용하거나, "
                                   "POS가 연결 기준 채널을 포함하는지 확인 필요"})
        elif avg_tr > 100:
            issues.append({"type": "channel_scope", "label": "채널 범위 경고",
                           "desc": f"평균 추적률 {avg_tr:.0f}% > 100% — "
                                   "POS 범위(총매출)와 DART 공시 범위(순매출) 차이 가능성"})

    if n_ext > 0:
        issues.append({"type": "quarterly_transform", "label": "분기 변환 이상",
                       "desc": f"{n_ext}개 분기에서 추적률이 200% 초과 또는 음수 발생 — "
                               "DART YTD→분기 단독값 변환 오류 가능성. 디버깅 탭 확인"})

    if not np.isnan(tr_std) and tr_std > 40:
        issues.append({"type": "quarterly_transform", "label": "추적률 변동 과대",
                       "desc": f"추적률 표준편차 {tr_std:.0f}pp — 분기별 변환값이 불안정. "
                               "특정 분기의 반기/3분기보고서 누계값 오사용 의심"})

    if n_neg > 0:
        issues.append({"type": "quarterly_transform", "label": "음수 분기 DART 매출",
                       "desc": f"{n_neg}개 분기에서 DART 매출이 음수 또는 0 — "
                               "YTD 누적→분기 변환 시 전기 보고서 누락 가능성"})

    # ── 중복 집계 경고 ────────────────────────────────────────────────────
    if n_above > len(tr) * 0.5 and not np.isnan(avg_tr) and avg_tr > 80:
        issues.append({"type": "duplicated_aggregation", "label": "중복 집계 의심",
                       "desc": f"전체 분기의 {n_above}/{len(tr)}에서 POS > DART — "
                               "거래 데이터의 회사/브랜드 중복 집계 여부 확인 필요"})

    # ── severity 결정 ─────────────────────────────────────────────────────
    type_set = {i["type"] for i in issues}
    if "unit_mismatch" in type_set or (not np.isnan(avg_tr) and avg_tr > 150):
        severity = "critical"
    elif "channel_scope" in type_set or "quarterly_transform" in type_set:
        severity = "warning"
    elif issues:
        severity = "caution"
    else:
        severity = "ok"

    unit_hint = (
        f"DART API 반환 단위: {dart_unit}  |  {pos_unit_note}"
        if pos_unit_note else f"DART API 반환 단위: {dart_unit}"
    )

    return {
        "severity":      severity,
        "avg_tr":        avg_tr,
        "max_tr":        max_tr,
        "tr_std":        tr_std,
        "n_above_100":   n_above,
        "n_extreme":     n_ext,
        "issues":        issues,
        "unit_hint":     unit_hint,
        "dart_unit":     dart_unit,
        "pos_unit_note": pos_unit_note,
        "avg_ratio_raw": avg_ratio_raw if not np.isnan(avg_ratio_raw) else None,
    }


# ── Renderer ──────────────────────────────────────────────────────────────────

def _render(result: dict):
    render_guide("earnings_intel")

    if result["status"] == "failed":
        st.error(result["message"])
        return
    if result["status"] == "warning":
        st.warning(result["message"])

    m = result["metrics"]
    qtr_agg_raw = result.get("data", pd.DataFrame())
    total_pos = float(qtr_agg_raw["pos_sales"].sum()) if not qtr_agg_raw.empty and "pos_sales" in qtr_agg_raw.columns else 0.0

    # ── 헤드라인: Tracking Quality 4-metric (투자자 관점) ────────────────────
    tq_avg      = m.get("tracking_quality_avg")
    dm_avg      = m.get("direction_match_avg")
    corr_avg    = m.get("correlation_avg")
    stab_avg    = m.get("stability_avg")
    co_track    = m.get("company_tracking", {}) or {}
    company_cov = m.get("company_coverage", {}) or {}   # 모든 곳에서 안전하게 접근 가능

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("총 POS 매출",       f"{total_pos:,.0f}")
    c2.metric("DART 연동 기업 수", f"{m.get('n_dart_companies', 0)}")

    if tq_avg is not None:
        tq_emoji = "🟢" if tq_avg >= 65 else "🟡" if tq_avg >= 40 else "🔴"
        c3.metric(
            "Tracking Quality",
            f"{tq_avg:.0f}/100",
            delta=tq_emoji,
            delta_color="off",
            help="투자자 관점 종합 신호 품질 (Universe 평균). "
                 "Direction Match × 0.5 + |POS-DART corr| × 0.3 + Stability × 0.2. "
                 "65+ 강한 신호 / 40~65 부분적 / <40 약함",
        )
    else:
        c3.metric("Tracking Quality", "N/A",
                  help="DART 연동 회사 없음 또는 분기 부족 (최소 4분기 필요)")

    if dm_avg is not None:
        c4.metric(
            "Direction Match",
            f"{dm_avg:.0f}%",
            delta=f"|r|={corr_avg:.2f}" if corr_avg is not None else None,
            delta_color="off",
            help="POS QoQ와 DART QoQ가 같은 방향(부호)인 분기 비율. "
                 "70%+ = POS가 실적 방향 신뢰성 있게 예측",
        )
    else:
        c4.metric("Direction Match", "N/A")

    if stab_avg is not None:
        c5.metric(
            "Stability",
            f"{stab_avg:.2f}",
            delta="비율 안정성",
            delta_color="off",
            help="POS/DART 비율의 시간적 안정성 (1/(1+CoV)). "
                 "0.7+ 매우 안정 / 0.4~0.7 보통 / <0.4 변동 큼",
        )
    else:
        c5.metric("Stability", "N/A")

    qoq = m.get("latest_qoq")
    if qoq is not None:
        st.caption(f"📊 최근 분기 POS QoQ: **{qoq:+.1f}%**")

    # ── Tracking Quality 회사별 ranking + Coverage 보조 정보 (expander) ─────
    if co_track or m.get("company_coverage"):
        with st.expander("📊 Tracking Quality · Coverage 회사별 상세", expanded=False):
            rows = []
            for cname in sorted(set(list(co_track.keys()) + list(company_cov.keys()))):
                t = co_track.get(cname, {})
                cov = company_cov.get(cname)
                cov_flag = ""
                if cov is not None:
                    if cov > 120:
                        cov_flag = "⚠ 별도 의심"
                    elif cov > 100:
                        cov_flag = "⚠ 범위 초과"
                rows.append({
                    "회사":            cname,
                    "Quality":         t.get("quality"),
                    "Direction%":      t.get("direction_match"),
                    "|r|":             round(abs(t.get("correlation", 0.0)), 3) if t else None,
                    "Stability":       t.get("stability"),
                    "N분기":           t.get("n_quarters"),
                    "Coverage%":       round(cov, 1) if cov is not None else None,
                    "Coverage 비고":   cov_flag,
                })
            df_tq = pd.DataFrame(rows).sort_values(
                "Quality", ascending=False, na_position="last"
            ).reset_index(drop=True)
            st.dataframe(df_tq, hide_index=True, width="stretch")
            st.caption(
                "**Tracking Quality** (헤드라인): Direction × 0.5 + |r| × 0.3 + Stability × 0.2. "
                "**Coverage** (보조 정보): POS / DART 매출 합계 — 편의점·카드 같은 channel-slice POS는 "
                "100% 초과되거나 회사·기간별 큰 분산을 보일 수 있음 (별도 vs 연결 / 단위 / VAT 등). "
                "투자자 의사결정엔 Quality가 더 직접적."
            )

    qtr_agg         = result.get("data", pd.DataFrame())
    dart_by_company = result.get("_dart_by_company", {})
    has_company     = "company" in qtr_agg.columns
    companies       = sorted(qtr_agg["company"].dropna().unique().tolist()) if has_company else []

    # ── 회사 선택 ──────────────────────────────────────────────────────────────
    selected = None
    if companies:
        selected = st.selectbox("회사 선택", options=["전체"] + companies, key="ei_company_sel")

        # 선택 회사의 Tracking Quality 강조 박스
        if selected and selected != "전체" and selected in co_track:
            t = co_track[selected]
            tq = t["quality"]
            tq_emoji = "🟢" if tq >= 65 else "🟡" if tq >= 40 else "🔴"
            cov_pct = company_cov.get(selected)
            cov_str = f" · Coverage {cov_pct:.0f}%" if cov_pct is not None else ""
            cov_caveat = ""
            if cov_pct is not None and cov_pct > 100:
                cov_caveat = " <span style='color:#92400e'>(⚠ 100% 초과 — 채널 슬라이스 비교 한계)</span>"

            st.markdown(
                f"<div style='background:#f8fafc;border-left:3px solid #1e40af;padding:10px 16px;"
                f"font-size:13px;color:#0f172a;margin-bottom:8px;border-radius:4px;line-height:1.6'>"
                f"<b>{tq_emoji} {selected} Tracking Quality: {tq:.0f}/100</b><br>"
                f"<span style='color:#475569;font-size:12px'>"
                f"Direction Match {t['direction_match']:.0f}% · "
                f"|r|={abs(t['correlation']):.2f} · "
                f"Stability {t['stability']:.2f} · "
                f"N={t['n_quarters']} 분기{cov_str}{cov_caveat}</span></div>",
                unsafe_allow_html=True,
            )

        # DART 매핑 상태 — 한 줄 요약 (매핑 자체는 Step 4에서 처리)
        corp_code_map = result.get("_corp_code_map", {}) or {}
        n_total       = len(companies)
        n_mapped      = sum(1 for c in companies if c in corp_code_map)
        n_with_data   = len(dart_by_company)
        if n_total:
            cov_pct = n_mapped / n_total * 100
            color   = "#16a34a" if cov_pct >= 70 else "#d97706" if cov_pct >= 30 else "#dc2626"
            st.markdown(
                f"<div style='font-size:12px;color:#475569;background:#f8fafc;"
                f"border-radius:6px;padding:6px 12px;margin:4px 0'>"
                f"🔗 <b>DART 매핑</b>: {n_mapped}/{n_total}개사 매핑 · "
                f"<span style='color:{color};font-weight:600'>{cov_pct:.0f}% 커버</span> · "
                f"공시 응답 {n_with_data}개사. "
                f"<span style='color:#94a3b8'>매핑 수정은 ← Step 4 (Analysis Setup) → Earnings 카드에서</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── 뷰 데이터 구성 ────────────────────────────────────────────────────────
    if selected and selected != "전체" and has_company:
        view_agg = qtr_agg[qtr_agg["company"] == selected].copy()
        dart_df  = dart_by_company.get(selected, pd.DataFrame())
    elif has_company:
        view_agg = (
            qtr_agg.groupby("quarter", as_index=False)["pos_sales"]
            .sum().sort_values("quarter")
        )
        view_agg["quarter_str"] = view_agg["quarter"].astype(str)
        view_agg["qoq_pct"]    = view_agg["pos_sales"].pct_change() * 100
        view_agg["yoy_pct"]    = view_agg["pos_sales"].pct_change(periods=4) * 100
        if dart_by_company:
            dart_parts = [d for d in dart_by_company.values() if not d.empty]
            if dart_parts:
                all_dart = pd.concat(
                    [d[["quarter_str", "dart_sales"] +
                       (["dart_sales_ytd"] if "dart_sales_ytd" in d.columns else []) +
                       (["reprt_code"]     if "reprt_code"     in d.columns else [])]
                     for d in dart_parts],
                    ignore_index=True,
                )
                agg_spec = {"dart_sales": ("dart_sales", "sum")}
                if "dart_sales_ytd" in all_dart.columns:
                    agg_spec["dart_sales_ytd"] = ("dart_sales_ytd", "sum")
                dart_df = (
                    all_dart.groupby("quarter_str", as_index=False)
                    .agg(**agg_spec)
                    .sort_values("quarter_str")
                )
                if "reprt_code" in all_dart.columns:
                    rc_map  = all_dart.groupby("quarter_str")["reprt_code"].first().reset_index()
                    dart_df = pd.merge(dart_df, rc_map, on="quarter_str", how="left")
            else:
                dart_df = pd.DataFrame()
        else:
            dart_df = pd.DataFrame()
    else:
        view_agg = qtr_agg.copy()
        dart_df  = pd.DataFrame()

    title_sfx = f" — {selected}" if selected and selected != "전체" else ""
    lead_tbl  = _build_lead_table(view_agg, dart_df)

    # ── 공통 요약 지표 (DART 연동 시에만) ────────────────────────────────────
    if not lead_tbl.empty:
        n          = len(lead_tbl)
        n_dir      = int(lead_tbl["direction_match"].sum())
        trend_match = n_dir / n * 100

        valid_corr  = lead_tbl[["qoq_pct", "dart_qoq"]].dropna()
        growth_corr = calculate_correlation(valid_corr["qoq_pct"], valid_corr["dart_qoq"])

        valid_levels = lead_tbl[["pos_sales", "dart_sales"]].dropna()
        level_corr   = calculate_correlation(valid_levels["pos_sales"], valid_levels["dart_sales"])

        avg_lead = int(lead_tbl["lead_days"].mean())

        tr_series  = lead_tbl["tracking_ratio"].replace([np.inf, -np.inf], np.nan).dropna()
        avg_tr     = tr_series.mean() if not tr_series.empty else float("nan")

        yoy_dir_rate = None
        if "yoy_direction_match" in lead_tbl.columns:
            yd = lead_tbl["yoy_direction_match"]
            if isinstance(yd, pd.DataFrame):
                yd = yd.iloc[:, 0]   # duplicate-column guard
            yd = yd.dropna()
            if not yd.empty:
                yoy_dir_rate = float(yd.sum()) / len(yd) * 100

        s1, s2, s3, s4, s5, s6 = st.columns(6)
        s1.metric("Trend Match Score", f"{trend_match:.0f}%",
                  f"QoQ 방향 일치 {n_dir}/{n}Q",
                  help="POS와 DART 공시의 분기 성장 방향(상승/하락)이 일치한 비율")
        s2.metric("YoY 방향 일치율",
                  f"{yoy_dir_rate:.0f}%" if yoy_dir_rate is not None else "N/A",
                  help="연간 성장 방향 일치율 (최소 5분기 필요)")
        s3.metric("Growth Corr (QoQ)",
                  f"{growth_corr:+.2f}" if not np.isnan(growth_corr) else "N/A",
                  "강함" if abs(growth_corr) >= 0.7 else ("중간" if abs(growth_corr) >= 0.4 else "약함"),
                  help="POS QoQ와 DART QoQ 성장률의 Pearson 상관계수")
        s4.metric("Level Corr (절대값)",
                  f"{level_corr:+.2f}" if not np.isnan(level_corr) else "N/A",
                  "강함" if abs(level_corr) >= 0.9 else ("중간" if abs(level_corr) >= 0.7 else "약함"),
                  help="POS 분기 매출과 DART 분기 매출 절대값의 Pearson 상관계수")
        s5.metric("공시 대비 선행일", f"~{avg_lead}일", "법정 공시기한 기준",
                  help="Q1·Q2·Q3: 분기 종료 후 45일, Q4: 90일")
        # Sanity check 실행
        sc = _sanity_check(lead_tbl)

        tr_label = f"{avg_tr:.1f}%" if not np.isnan(avg_tr) else "N/A"
        tr_delta = ""
        if sc["severity"] == "critical":
            tr_delta = "⚠️ 신뢰도 낮음"
        elif sc["severity"] == "warning":
            tr_delta = "⚠️ 범위 확인 필요"
        s6.metric("POS Tracking Ratio",
                  tr_label, tr_delta,
                  help=(
                      "POS 분기 매출 / DART 분기 단독 공시 매출 × 100\n\n"
                      "⚠️ 참고용 지표: 단위·집계범위·연결/별도 차이에 따라 "
                      "100% 초과가 발생할 수 있으며, 이 경우 Sanity Check 탭을 확인하세요."
                  ))

        # ── 선행 신호 배너 ───────────────────────────────────────────────────
        if trend_match >= 75 and abs(growth_corr) >= 0.6:
            bc, bi, bt = "#f0fdf4", "🟢", f"강한 선행 신호 — POS가 공시보다 평균 {avg_lead}일 앞서 방향성을 {trend_match:.0f}% 정확도로 추적"
        elif trend_match >= 55 or abs(growth_corr) >= 0.4:
            bc, bi, bt = "#fffbeb", "🟡", f"중간 선행 신호 — Trend Match {trend_match:.0f}%, 추가 검증 권장"
        else:
            bc, bi, bt = "#fef2f2", "🔴", f"약한 선행 신호 — POS Tracking Ratio가 낮거나 데이터 기간이 짧을 수 있음"
        st.markdown(
            f"<div style='background:{bc};border-radius:8px;padding:12px 16px;"
            f"font-size:13px;margin:8px 0 8px'>{bi} {bt}</div>",
            unsafe_allow_html=True,
        )

        # ── Sanity Check 경고 배너 ───────────────────────────────────────────
        SC_COLOR = {"critical": "#fef2f2", "warning": "#fffbeb",
                    "caution": "#fffbeb", "ok": "#f0fdf4"}
        SC_ICON  = {"critical": "🚨", "warning": "⚠️", "caution": "⚠️", "ok": "✅"}
        if sc["severity"] != "ok":
            sc_bg   = SC_COLOR.get(sc["severity"], "#fffbeb")
            sc_icon = SC_ICON.get(sc["severity"], "⚠️")
            top_issue = sc["issues"][0]["desc"] if sc["issues"] else ""
            sc_label = {"critical": "데이터 신뢰도 Critical",
                        "warning": "데이터 신뢰도 Warning",
                        "caution": "데이터 신뢰도 주의"}.get(sc["severity"], "")
            st.markdown(
                f"<div style='background:{sc_bg};border:1px solid #f59e0b;"
                f"border-radius:8px;padding:10px 16px;font-size:12px;margin:0 0 12px'>"
                f"{sc_icon} <b>Sanity Check — {sc_label}:</b> {top_issue} "
                f"<span style='color:#6b7280'>(Sanity Check 탭에서 상세 확인)</span></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<div style='background:#f0fdf4;border-radius:8px;padding:8px 16px;"
                "font-size:12px;margin:0 0 12px'>✅ <b>Sanity Check 통과</b> — "
                "POS Tracking Ratio 및 단위가 정상 범위 내</div>",
                unsafe_allow_html=True,
            )

    # ── 탭 ────────────────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 POS / DART 절대값 비교",
        "📈 Growth Alignment",
        "📡 Lead Signal",
        "📈 Coverage Stability",
        "🛡 Sanity Check",
        "🔍 디버깅",
    ])

    # ════════════════════════════════════════════════════════════════════════
    # TAB 1 — POS / DART 절대값 비교
    # ════════════════════════════════════════════════════════════════════════
    with tab1:
        # 주가 연동 상태 진단 (회사 선택 시에만 표시)
        if selected and selected != "전체":
            _render_stock_link_status(selected)

        if lead_tbl.empty:
            if not view_agg.empty and "pos_sales" in view_agg.columns:
                fig = go.Figure(go.Bar(
                    x=view_agg["quarter_str"], y=view_agg["pos_sales"],
                    name="POS 분기 매출", marker_color="#3b82f6",
                    text=[f"{v:,.0f}" for v in view_agg["pos_sales"]],
                    textposition="outside",
                ))
                fig.update_layout(
                    title=f"POS 분기별 절대 매출{title_sfx}",
                    height=380, yaxis_title="매출액",
                )
                st.plotly_chart(fig, key="earnings_1")
                st.info("DART API 연동 시 공시 매출과 절대값 비교 및 Tracking Ratio를 확인할 수 있습니다.")
            else:
                st.info("POS 데이터를 집계 중이거나 분기가 부족합니다.")
        else:
            tr_series = lead_tbl["tracking_ratio"].replace([np.inf, -np.inf], np.nan)
            avg_tr_t1 = tr_series.dropna().mean()

            a1, a2, a3, a4 = st.columns(4)
            a1.metric("총 POS 매출",        f"{lead_tbl['pos_sales'].sum():,.0f}")
            a2.metric("총 DART 매출",        f"{lead_tbl['dart_sales'].sum():,.0f}")
            a3.metric("평균 Tracking Ratio", f"{avg_tr_t1:.1f}%" if not np.isnan(avg_tr_t1) else "N/A")
            a4.metric("분석 분기 수",        f"{len(lead_tbl)}")

            level_data = lead_tbl.dropna(subset=["pos_sales", "dart_sales"])
            if not level_data.empty:
                lc  = level_corr
                gap = level_data["pos_sales"] - level_data["dart_sales"]
                gap_colors = ["#94a3b8" if v >= 0 else "#cbd5e1" for v in gap]

                fig_abs = go.Figure()
                fig_abs.add_trace(go.Bar(
                    x=level_data["quarter_str"], y=gap,
                    name="Gap (POS − DART)",
                    marker_color=gap_colors, opacity=0.55,
                ))
                fig_abs.add_trace(go.Scatter(
                    x=level_data["quarter_str"], y=level_data["pos_sales"],
                    mode="lines+markers", name="POS Revenue",
                    line=dict(color="#3b82f6", width=2.5),
                    marker=dict(size=6),
                ))
                fig_abs.add_trace(go.Scatter(
                    x=level_data["quarter_str"], y=level_data["dart_sales"],
                    mode="lines+markers", name="Reported Revenue (DART)",
                    line=dict(color="#f59e0b", width=2.5),
                    marker=dict(size=6),
                ))
                if not np.isnan(lc):
                    fig_abs.add_annotation(
                        x=0.02, y=0.96, xref="paper", yref="paper",
                        text=f"<b>Pearson r (Level): {lc:.3f}</b>",
                        showarrow=False, font=dict(size=13, color="#1e40af"),
                        bgcolor="rgba(255,255,255,0.85)",
                        bordercolor="#93c5fd", borderwidth=1,
                    )

                # ── 주가 level overlay (Market Signal 연동) ────────────────
                stock_added = _add_stock_level_overlay(
                    fig_abs, selected, level_data,
                )

                fig_abs.update_layout(
                    height=420,
                    title=(
                        f"절대값 비교 — POS vs Reported Revenue (Quarterly)"
                        f"{' vs 주가' if stock_added else ''}{title_sfx}"
                    ),
                    yaxis_title="매출",
                    legend=dict(orientation="h", y=-0.2),
                    plot_bgcolor="#f9fafb",
                    paper_bgcolor="white",
                    barmode="overlay",
                )
                if stock_added:
                    fig_abs.update_layout(
                        yaxis2=dict(
                            title="주가 (분기말 종가)", overlaying="y",
                            side="right", showgrid=False, color="#dc2626",
                        ),
                    )
                st.plotly_chart(fig_abs, key="earnings_2")
                _caption = (
                    "파란 선: POS 분기 매출 · 주황 선: DART 분기 단독 공시 매출 · "
                    "회색 막대: Gap(POS − DART). Pearson r은 두 시계열 절대값의 상관계수."
                )
                if stock_added:
                    _caption += (
                        " · **🔴 빨간 점선: 분기말 주가 (우측 축)** — 매출과 같이 움직이면 알파 신호."
                    )
                st.caption(_caption)

            disp_abs = lead_tbl[["quarter_str", "pos_sales", "dart_sales", "tracking_ratio"]].copy()
            disp_abs["tracking_ratio"] = disp_abs["tracking_ratio"].apply(
                lambda v: f"{v:.1f}%" if pd.notna(v) and not np.isinf(v) else "—"
            )
            st.dataframe(
                disp_abs.rename(columns={
                    "quarter_str":    "분기",
                    "pos_sales":      "POS 분기 매출",
                    "dart_sales":     "DART 분기 단독 매출",
                    "tracking_ratio": "POS Tracking Ratio",
                }), hide_index=True,
            )

    # ════════════════════════════════════════════════════════════════════════
    # TAB 2 — Growth Alignment
    # ════════════════════════════════════════════════════════════════════════
    with tab2:
        if lead_tbl.empty:
            if not view_agg.empty and "qoq_pct" in view_agg.columns:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=view_agg["quarter_str"], y=view_agg["qoq_pct"].round(1),
                    name="POS QoQ(%)", marker_color="#3b82f6",
                ))
                if "yoy_pct" in view_agg.columns:
                    fig.add_trace(go.Scatter(
                        x=view_agg["quarter_str"], y=view_agg["yoy_pct"].round(1),
                        mode="lines+markers", name="POS YoY(%)",
                        line=dict(color="#f59e0b", width=2),
                    ))
                fig.add_hline(y=0, line_dash="dot", line_color="gray")
                fig.update_layout(title=f"POS 분기 성장률{title_sfx}", height=380)
                st.plotly_chart(fig, key="earnings_3")
                st.info("DART API 연동 시 공시 성장률과 Growth Gap을 비교할 수 있습니다.")
            else:
                st.info("POS 데이터를 집계 중이거나 분기가 부족합니다.")
        else:
            ga1, ga2, ga3 = st.tabs(["QoQ 비교", "YoY 비교", "Trend Match"])

            with ga1:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=lead_tbl["quarter_str"], y=lead_tbl["qoq_pct"].round(1),
                    name="POS QoQ(%)", marker_color="#3b82f6", opacity=0.85,
                ))
                fig.add_trace(go.Bar(
                    x=lead_tbl["quarter_str"], y=lead_tbl["dart_qoq"].round(1),
                    name="DART 공시 QoQ(%)", marker_color="#f59e0b", opacity=0.85,
                ))
                fig.add_hline(y=0, line_dash="dot", line_color="gray")
                fig.update_layout(
                    barmode="group", height=380,
                    title=f"QoQ 성장률 비교{title_sfx}",
                    yaxis_title="QoQ (%)", legend=dict(orientation="h", y=-0.2),
                )
                st.plotly_chart(fig, key="earnings_4")

                if "qoq_gap" in lead_tbl.columns:
                    gap_data = lead_tbl.dropna(subset=["qoq_gap"])
                    if not gap_data.empty:
                        gap_colors = ["#16a34a" if v >= 0 else "#dc2626" for v in gap_data["qoq_gap"]]
                        fig_gap = go.Figure(go.Bar(
                            x=gap_data["quarter_str"], y=gap_data["qoq_gap"].round(1),
                            name="QoQ Growth Gap (POS − DART)",
                            marker_color=gap_colors,
                        ))
                        fig_gap.add_hline(y=0, line_dash="solid", line_color="#9ca3af")
                        fig_gap.update_layout(
                            height=260,
                            title="QoQ Growth Gap = POS − DART  (양수 = POS가 더 빠르게 성장)",
                            yaxis_title="Gap (pp)",
                        )
                        st.plotly_chart(fig_gap, key="earnings_5")

            with ga2:
                if "dart_yoy" in lead_tbl.columns and "yoy_pct" in lead_tbl.columns:
                    yoy_data = lead_tbl.dropna(subset=["dart_yoy", "yoy_pct"])
                    if not yoy_data.empty:
                        fig = go.Figure()
                        fig.add_trace(go.Bar(
                            x=yoy_data["quarter_str"], y=yoy_data["yoy_pct"].round(1),
                            name="POS YoY(%)", marker_color="#3b82f6", opacity=0.85,
                        ))
                        fig.add_trace(go.Bar(
                            x=yoy_data["quarter_str"], y=yoy_data["dart_yoy"].round(1),
                            name="DART 공시 YoY(%)", marker_color="#f59e0b", opacity=0.85,
                        ))
                        fig.add_hline(y=0, line_dash="dot", line_color="gray")
                        fig.update_layout(
                            barmode="group", height=380,
                            title=f"YoY 성장률 비교{title_sfx}",
                            yaxis_title="YoY (%)", legend=dict(orientation="h", y=-0.2),
                        )
                        st.plotly_chart(fig, key="earnings_6")

                        if "yoy_gap" in lead_tbl.columns:
                            yoy_gap_d = yoy_data.dropna(subset=["yoy_gap"])
                            if not yoy_gap_d.empty:
                                gap_col = ["#16a34a" if v >= 0 else "#dc2626" for v in yoy_gap_d["yoy_gap"]]
                                fig_g2 = go.Figure(go.Bar(
                                    x=yoy_gap_d["quarter_str"], y=yoy_gap_d["yoy_gap"].round(1),
                                    name="YoY Growth Gap (POS − DART)",
                                    marker_color=gap_col,
                                ))
                                fig_g2.add_hline(y=0, line_dash="solid", line_color="#9ca3af")
                                fig_g2.update_layout(
                                    height=260,
                                    title="YoY Growth Gap = POS − DART",
                                    yaxis_title="Gap (pp)",
                                )
                                st.plotly_chart(fig_g2, key="earnings_7")
                    else:
                        st.info("YoY 계산에 충분한 데이터가 없습니다 (최소 5분기 이상 필요).")
                else:
                    st.info("YoY 데이터를 계산하기 위해 더 많은 분기 데이터가 필요합니다.")

            with ga3:
                lead_copy = lead_tbl.copy()
                lead_copy["match_num"]     = lead_copy["direction_match"].astype(int)
                lead_copy["rolling_align"] = lead_copy["match_num"].rolling(3, min_periods=2).mean() * 100

                tq1, tq2 = st.columns(2)
                with tq1:
                    colors_align = ["#16a34a" if v else "#dc2626" for v in lead_copy["direction_match"]]
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=lead_copy["quarter_str"],
                        y=lead_copy["match_num"] * 100,
                        name="방향 일치",
                        marker_color=colors_align, opacity=0.8,
                    ))
                    fig.add_trace(go.Scatter(
                        x=lead_copy["quarter_str"],
                        y=lead_copy["rolling_align"],
                        mode="lines+markers",
                        name="Rolling 3Q Trend Match(%)",
                        line=dict(color="#1e40af", width=2.5),
                    ))
                    fig.add_hline(y=50, line_dash="dash", line_color="#9ca3af",
                                  annotation_text="기준선 50%")
                    fig.update_layout(
                        height=380,
                        title=f"Trend Match Score{title_sfx}",
                        yaxis=dict(title="일치율(%)", range=[0, 115]),
                        legend=dict(orientation="h", y=-0.2),
                    )
                    st.plotly_chart(fig, key="earnings_8")

                with tq2:
                    valid_corr = lead_tbl[["qoq_pct", "dart_qoq"]].dropna()
                    corr = growth_corr if not lead_tbl.empty else 0.0
                    colors_sc  = ["#16a34a" if v else "#dc2626" for v in lead_tbl["direction_match"]]
                    fig2 = go.Figure()
                    fig2.add_trace(go.Scatter(
                        x=lead_tbl["qoq_pct"], y=lead_tbl["dart_qoq"],
                        mode="markers+text",
                        text=lead_tbl["quarter_str"],
                        textposition="top center", textfont=dict(size=9),
                        marker=dict(color=colors_sc, size=10, line=dict(width=1, color="#fff")),
                        name="분기별 관측",
                    ))
                    if len(valid_corr) >= 2:
                        x_r = np.linspace(valid_corr["qoq_pct"].min(), valid_corr["qoq_pct"].max(), 50)
                        slope, intercept = np.polyfit(valid_corr["qoq_pct"], valid_corr["dart_qoq"], 1)
                        fig2.add_trace(go.Scatter(
                            x=x_r, y=slope * x_r + intercept,
                            mode="lines", name=f"추세선 (R={corr:.2f})",
                            line=dict(color="#6b7280", dash="dash", width=1.5),
                        ))
                    ax_max = max(abs(lead_tbl["qoq_pct"].max()), abs(lead_tbl["dart_qoq"].max())) * 1.1
                    fig2.add_trace(go.Scatter(
                        x=[-ax_max, ax_max], y=[-ax_max, ax_max],
                        mode="lines", name="완벽 예측선",
                        line=dict(color="#d1d5db", dash="dot", width=1),
                    ))
                    fig2.add_vline(x=0, line_dash="dot", line_color="#e5e7eb")
                    fig2.add_hline(y=0, line_dash="dot", line_color="#e5e7eb")
                    fig2.update_layout(
                        height=380,
                        title=f"POS QoQ vs DART QoQ 산점도 (R²={corr**2:.2f})",
                        xaxis_title="POS QoQ(%)", yaxis_title="DART QoQ(%)",
                        legend=dict(orientation="h", y=-0.25),
                    )
                    st.plotly_chart(fig2, key="earnings_9")

                st.markdown("**분기별 Trend Match 상세**")
                disp = lead_tbl.copy()
                disp["방향(QoQ)"] = disp["direction_match"].map({True: "✅ 일치", False: "❌ 불일치"})
                if "yoy_direction_match" in disp.columns:
                    disp["방향(YoY)"] = disp["yoy_direction_match"].map({True: "✅", False: "❌"})
                disp["POS QoQ(%)"]  = disp["qoq_pct"].round(1)
                disp["DART QoQ(%)"] = disp["dart_qoq"].round(1)
                disp["Growth Gap"]  = disp["qoq_gap"].apply(
                    lambda v: f"{v:+.1f}pp" if pd.notna(v) else "—"
                ) if "qoq_gap" in disp.columns else "—"
                disp["선행일"]      = disp["lead_days"].apply(lambda v: f"~{v}일")

                show = ["quarter_str", "POS QoQ(%)", "DART QoQ(%)", "Growth Gap", "방향(QoQ)"]
                if "yoy_direction_match" in lead_tbl.columns:
                    show.append("방향(YoY)")
                show.append("선행일")
                st.dataframe(
                    disp[show].rename(columns={"quarter_str": "분기"}), hide_index=True,
                )
                st.caption(
                    "Trend Match Score = POS와 DART 공시의 분기 성장 방향(상승/하락)이 일치한 비율. "
                    "Growth Gap = POS QoQ − DART QoQ (양수면 POS가 공시보다 빠른 성장을 선반영)."
                )

    # ════════════════════════════════════════════════════════════════════════
    # TAB 3 — Lead Signal (lag 상관분석)
    # ════════════════════════════════════════════════════════════════════════
    with tab3:
        # ── 🔗 주가 연동 상태 진단 (눈에 띄는 box) ──────────────────────────
        _render_stock_link_status(selected)

        if lead_tbl.empty:
            st.info("DART 연동 데이터가 있어야 Lead Signal 분석이 가능합니다.")
        elif len(lead_tbl) < 4:
            st.warning("Lead Signal 분석에는 최소 4분기 이상의 병합 데이터가 필요합니다.")
        else:
            lag_df = _compute_lag_corrs(lead_tbl)

            if not lag_df.empty and lag_df["r"].notna().any():
                best_row = lag_df.loc[lag_df["r"].abs().idxmax()]
                best_lag = int(best_row["lag"])
                best_r   = best_row["r"]
                same_r   = lag_df.loc[lag_df["lag"] == 0, "r"].values
                same_r   = float(same_r[0]) if len(same_r) else float("nan")

                ls1, ls2, ls3 = st.columns(3)
                ls1.metric(
                    "최적 선행 Lag",
                    f"{best_lag}Q",
                    "POS 선행" if best_lag > 0 else ("동행" if best_lag == 0 else "DART 선행"),
                    help="POS QoQ와 DART QoQ의 상관이 가장 높은 lag. 양수 = POS가 앞섬.",
                )
                ls2.metric(
                    "최적 Lag 상관계수",
                    f"{best_r:+.3f}" if not np.isnan(best_r) else "N/A",
                    "강함" if abs(best_r) >= 0.7 else ("중간" if abs(best_r) >= 0.4 else "약함"),
                )
                ls3.metric(
                    "동행(Lag 0) 상관계수",
                    f"{same_r:+.3f}" if not np.isnan(same_r) else "N/A",
                    f"선행 효과 +{best_r - same_r:.3f}" if not np.isnan(same_r) and not np.isnan(best_r) else "",
                )
                st.write("")

                # ── 1. 성장률 overlay 차트 ─────────────────────────────────
                st.markdown("#### Early Signal before the Revenue Report")
                st.caption(
                    "POS 성장률(빨강)과 DART 공시 성장률(회색)을 같은 축에 표시합니다. "
                    "POS 곡선이 공시보다 먼저 방향을 전환하면 선행 신호입니다."
                )

                # QoQ overlay
                qoq_data = lead_tbl.dropna(subset=["qoq_pct", "dart_qoq"])
                if not qoq_data.empty:
                    qoq_r_val = lag_df.loc[lag_df["lag"] == 0, "r"].values
                    qoq_r_val = float(qoq_r_val[0]) if len(qoq_r_val) and not np.isnan(qoq_r_val[0]) else None

                    fig_qoq = go.Figure()
                    fig_qoq.add_trace(go.Scatter(
                        x=qoq_data["quarter_str"],
                        y=qoq_data["qoq_pct"].round(1),
                        mode="lines+markers",
                        name="POS (QoQ %)",
                        line=dict(color="#ef4444", width=2.5),
                        marker=dict(size=6),
                    ))
                    fig_qoq.add_trace(go.Scatter(
                        x=qoq_data["quarter_str"],
                        y=qoq_data["dart_qoq"].round(1),
                        mode="lines+markers",
                        name="Reported Revenue (QoQ %)",
                        line=dict(color="#6b7280", width=2.5),
                        marker=dict(size=6),
                    ))
                    fig_qoq.add_hline(y=0, line_dash="solid", line_color="#d1d5db", line_width=1)

                    # Pearson r annotation
                    if qoq_r_val is not None:
                        fig_qoq.add_annotation(
                            x=0.02, y=0.96, xref="paper", yref="paper",
                            text=f"<b>Pearson r: {qoq_r_val:.2f}</b>",
                            showarrow=False, font=dict(size=13, color="#1e40af"),
                            bgcolor="rgba(255,255,255,0.8)",
                            bordercolor="#93c5fd", borderwidth=1,
                        )

                    # Shifted POS overlay — 모든 회사에 표시 (best_lag에 따라 방향 결정)
                    # best_lag > 0: POS가 DART 선행 → POS를 미래로 shift
                    # best_lag == 0: 동행이지만 default 1Q 선행 가정해 시각 검증
                    # best_lag < 0: DART가 선행 → POS를 과거로 shift (POS 후행 표시)
                    display_lag = best_lag if best_lag != 0 else 1
                    if abs(display_lag) > 0 and len(qoq_data) > abs(display_lag):
                        n = abs(display_lag)
                        if display_lag > 0:
                            shifted_pos  = qoq_data["qoq_pct"].values[:-n]
                            shifted_qtrs = qoq_data["quarter_str"].values[n:]
                            shift_name   = f"POS {n}Q 선행 (shifted)"
                            shift_help_hint = "(동행 가정 default)" if best_lag == 0 else ""
                        else:
                            shifted_pos  = qoq_data["qoq_pct"].values[n:]
                            shifted_qtrs = qoq_data["quarter_str"].values[:-n]
                            shift_name   = f"POS {n}Q 후행 (shifted)"
                            shift_help_hint = ""
                        full_name = (
                            f"{shift_name} {shift_help_hint}".strip()
                        )
                        fig_qoq.add_trace(go.Scatter(
                            x=shifted_qtrs,
                            y=shifted_pos.round(1),
                            mode="lines",
                            name=full_name,
                            line=dict(color="#f97316", width=1.5, dash="dot"),
                        ))

                    # ── 주가 QoQ overlay (Market Signal 결과에서 추출) ──────────
                    stock_qoq_added = _add_stock_qoq_overlay(
                        fig_qoq, selected, qoq_data,
                    )

                    fig_qoq.update_layout(
                        height=380,
                        title=dict(
                            text=f"QoQ Growth(%) — POS vs Reported Revenue"
                                 f"{' vs 주가' if stock_qoq_added else ''}"
                                 f"{title_sfx}",
                            font=dict(size=14),
                        ),
                        yaxis_title="매출 QoQ (%)",
                        xaxis_title="",
                        legend=dict(orientation="h", y=-0.18),
                        plot_bgcolor="#f9fafb",
                        paper_bgcolor="white",
                    )
                    if stock_qoq_added:
                        # 보조 축 (주가 QoQ) — 우측
                        fig_qoq.update_layout(
                            yaxis2=dict(
                                title="주가 QoQ (%)", overlaying="y", side="right",
                                showgrid=False, color="#1e40af",
                            ),
                        )
                    st.plotly_chart(fig_qoq, key="earnings_10")
                    if stock_qoq_added:
                        st.caption(
                            "💡 **파란색 점선 = 같은 분기 주가 QoQ(%)** — 우측 축. "
                            "Market Signal 결과의 일별 종가에서 분기 마지막 영업일 가격을 사용. "
                            "POS·공시·주가 세 줄이 같이 움직이면 강한 알파 신호."
                        )
                    else:
                        if st.session_state.get("results", {}).get("market_signal"):
                            st.caption(
                                "ℹ️ Market Signal 결과는 있으나 이 회사의 주가 매핑이 없어 주가 line을 표시하지 못했습니다."
                            )
                        else:
                            st.caption(
                                "ℹ️ **주가 line을 같이 보려면** Step 4로 가서 Market Signal도 함께 실행하세요. "
                                "그러면 POS·공시·주가 3가지를 한 차트에서 비교 가능."
                            )

                # YoY overlay
                if "dart_yoy" in lead_tbl.columns and "yoy_pct" in lead_tbl.columns:
                    yoy_data = lead_tbl.dropna(subset=["yoy_pct", "dart_yoy"])
                    if not yoy_data.empty and len(yoy_data) >= 3:
                        yoy_r_val = float(np.corrcoef(
                            yoy_data["yoy_pct"].values,
                            yoy_data["dart_yoy"].values,
                        )[0, 1]) if len(yoy_data) >= 3 else None

                        fig_yoy = go.Figure()
                        fig_yoy.add_trace(go.Scatter(
                            x=yoy_data["quarter_str"],
                            y=yoy_data["yoy_pct"].round(1),
                            mode="lines+markers",
                            name="POS (YoY %)",
                            line=dict(color="#ef4444", width=2.5),
                            marker=dict(size=6),
                        ))
                        fig_yoy.add_trace(go.Scatter(
                            x=yoy_data["quarter_str"],
                            y=yoy_data["dart_yoy"].round(1),
                            mode="lines+markers",
                            name="Reported Revenue (YoY %)",
                            line=dict(color="#6b7280", width=2.5),
                            marker=dict(size=6),
                        ))
                        fig_yoy.add_hline(y=0, line_dash="solid", line_color="#d1d5db", line_width=1)

                        if yoy_r_val is not None and not np.isnan(yoy_r_val):
                            fig_yoy.add_annotation(
                                x=0.02, y=0.96, xref="paper", yref="paper",
                                text=f"<b>Pearson r: {yoy_r_val:.2f}</b>",
                                showarrow=False, font=dict(size=13, color="#1e40af"),
                                bgcolor="rgba(255,255,255,0.8)",
                                bordercolor="#93c5fd", borderwidth=1,
                            )

                        fig_yoy.update_layout(
                            height=380,
                            title=dict(
                                text=f"YoY Growth(%) Correlation — POS vs Reported Revenue{title_sfx}",
                                font=dict(size=14),
                            ),
                            yaxis_title="YoY 성장률 (%)",
                            xaxis_title="",
                            legend=dict(orientation="h", y=-0.18),
                            plot_bgcolor="#f9fafb",
                            paper_bgcolor="white",
                        )
                        st.plotly_chart(fig_yoy, key="earnings_11")

                # ── 절대값 매출 overlay 차트 ──────────────────────────────
                level_data = lead_tbl.dropna(subset=["pos_sales", "dart_sales"])
                if not level_data.empty:
                    gap_abs = level_data["pos_sales"] - level_data["dart_sales"]
                    gap_colors = ["#94a3b8" if v >= 0 else "#cbd5e1" for v in gap_abs]

                    fig_lvl = go.Figure()
                    fig_lvl.add_trace(go.Bar(
                        x=level_data["quarter_str"], y=gap_abs,
                        name="Gap (POS − DART)",
                        marker_color=gap_colors, opacity=0.5,
                    ))
                    fig_lvl.add_trace(go.Scatter(
                        x=level_data["quarter_str"], y=level_data["pos_sales"],
                        mode="lines+markers", name="POS Revenue (절대값)",
                        line=dict(color="#ef4444", width=2.5),
                        marker=dict(size=6),
                    ))
                    fig_lvl.add_trace(go.Scatter(
                        x=level_data["quarter_str"], y=level_data["dart_sales"],
                        mode="lines+markers", name="Reported Revenue (절대값)",
                        line=dict(color="#6b7280", width=2.5),
                        marker=dict(size=6),
                    ))
                    if not np.isnan(level_corr):
                        fig_lvl.add_annotation(
                            x=0.02, y=0.96, xref="paper", yref="paper",
                            text=f"<b>Pearson r (Level): {level_corr:.3f}</b>",
                            showarrow=False, font=dict(size=13, color="#1e40af"),
                            bgcolor="rgba(255,255,255,0.85)",
                            bordercolor="#93c5fd", borderwidth=1,
                        )
                    fig_lvl.update_layout(
                        height=400,
                        title=dict(
                            text=f"Absolute Revenue — POS vs Reported Revenue{title_sfx}",
                            font=dict(size=14),
                        ),
                        yaxis_title="매출",
                        xaxis_title="",
                        legend=dict(orientation="h", y=-0.18),
                        plot_bgcolor="#f9fafb",
                        paper_bgcolor="white",
                        barmode="overlay",
                    )
                    st.plotly_chart(fig_lvl, key="earnings_12")
                    st.caption(
                        "빨간 선: POS 분기 매출 · 회색 선: DART 분기 단독 공시 매출 · "
                        "회색 막대: Gap(POS − DART). Pearson r은 절대값 시계열 기준."
                    )

                st.divider()

                # ── 2. Lag 상관계수 바차트 ─────────────────────────────────
                st.markdown("#### Lag별 상관계수 — 어느 분기에 가장 잘 예측하나?")
                lag_valid = lag_df.dropna(subset=["r"])
                bar_cols  = ["#1e40af" if row["lag"] == best_lag else
                             ("#3b82f6" if row["r"] >= 0 else "#ef4444")
                             for _, row in lag_valid.iterrows()]
                fig_lag = go.Figure(go.Bar(
                    x=lag_valid["label"],
                    y=lag_valid["r"],
                    marker_color=bar_cols,
                    text=lag_valid["r"].round(3),
                    textposition="outside",
                ))
                fig_lag.add_hline(y=0, line_dash="solid", line_color="#9ca3af")
                fig_lag.add_hline(y=0.6,  line_dash="dash", line_color="#16a34a",
                                  annotation_text="강한 양의 상관 (0.6)")
                fig_lag.add_hline(y=-0.6, line_dash="dash", line_color="#dc2626",
                                  annotation_text="강한 음의 상관 (−0.6)")
                fig_lag.update_layout(
                    height=340,
                    title=f"Lag별 POS QoQ → DART QoQ 상관계수{title_sfx}",
                    xaxis_title="Lag (분기)",
                    yaxis=dict(title="Pearson r", range=[-1.1, 1.1]),
                    showlegend=False,
                )
                st.plotly_chart(fig_lag, key="earnings_13")

                if best_lag > 0:
                    st.success(
                        f"✅ POS 데이터가 DART 공시보다 **{best_lag}분기 선행** (r = {best_r:.2f}). "
                        f"공시 기한(~{avg_lead}일) 포함 시 실질 정보 우위 "
                        f"약 **{best_lag * 3}개월 + {avg_lead}일**."
                    )
                elif best_lag == 0:
                    st.info(f"POS와 DART가 동행 (lag 0에서 최고 상관 r = {best_r:.2f}). 선행성은 법정 공시 기한({avg_lead}일)에서만 발생.")
                else:
                    st.warning(f"⚠️ DART가 POS보다 {-best_lag}분기 선행. POS 추적 범위 또는 데이터 품질을 점검하세요.")

                st.dataframe(
                    lag_valid[["label", "r", "n"]].rename(columns={
                        "label": "Lag 설명", "r": "Pearson r", "n": "관측 분기 수"
                    }), hide_index=True,
                )
            else:
                st.warning("분기 데이터가 부족하거나 분산이 없어 lag 상관분석을 수행할 수 없습니다.")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 4 — Coverage Stability (POS Tracking Ratio)
    # ════════════════════════════════════════════════════════════════════════
    with tab4:
        st.markdown(
            "<div style='background:#eff6ff;border-left:4px solid #3b82f6;"
            "padding:10px 14px;border-radius:4px;font-size:13px;margin-bottom:12px'>"
            "📌 <b>POS Tracking Ratio</b> = 실제 매출 중 POS 데이터로 추적 가능한 비율<br>"
            "<span style='color:#6b7280'>= POS 분기 매출 ÷ DART 분기 단독 공시 매출 × 100</span></div>",
            unsafe_allow_html=True,
        )

        if lead_tbl.empty:
            st.info("DART 연동 데이터가 있어야 POS Tracking Ratio를 계산할 수 있습니다.")
        else:
            tr = lead_tbl["tracking_ratio"].replace([np.inf, -np.inf], np.nan)
            tr_mean = tr.dropna().mean()
            tr_std  = tr.dropna().std()

            cs1, cs2, cs3, cs4 = st.columns(4)
            cs1.metric("평균 Tracking Ratio", f"{tr_mean:.1f}%" if not np.isnan(tr_mean) else "N/A")
            cs2.metric("표준편차",             f"{tr_std:.1f}pp"  if not np.isnan(tr_std)  else "N/A",
                       help="값이 낮을수록 안정적인 추적")
            cs3.metric("최솟값",               f"{tr.min():.1f}%" if not tr.dropna().empty  else "N/A")
            cs4.metric("최댓값",               f"{tr.max():.1f}%" if not tr.dropna().empty  else "N/A")

            # Line chart with ±1σ band and anomaly markers
            upper  = tr_mean + tr_std
            lower  = max(0, tr_mean - tr_std)
            anomaly_mask = (tr > upper) | (tr < lower)

            fig = go.Figure()
            # ±1σ band
            qs = lead_tbl["quarter_str"].tolist()
            fig.add_trace(go.Scatter(
                x=qs + qs[::-1],
                y=[upper] * len(qs) + [lower] * len(qs),
                fill="toself", fillcolor="rgba(59,130,246,0.08)",
                line=dict(width=0), name="±1σ 범위", showlegend=True,
            ))
            # Mean line
            fig.add_hline(y=tr_mean, line_dash="dash", line_color="#3b82f6",
                          annotation_text=f"평균 {tr_mean:.1f}%")
            # Tracking ratio line
            fig.add_trace(go.Scatter(
                x=lead_tbl["quarter_str"], y=tr,
                mode="lines+markers", name="POS Tracking Ratio(%)",
                line=dict(color="#1e40af", width=2),
                marker=dict(size=7),
            ))
            # Anomaly markers
            if anomaly_mask.any():
                anom_idx = lead_tbl[anomaly_mask.values]
                fig.add_trace(go.Scatter(
                    x=anom_idx["quarter_str"],
                    y=tr[anomaly_mask.values],
                    mode="markers", name="이상 분기 (±1σ 초과)",
                    marker=dict(color="#ef4444", size=12, symbol="circle-open", line=dict(width=2)),
                ))
            fig.update_layout(
                height=400,
                title=f"POS Tracking Ratio 추이{title_sfx}",
                yaxis=dict(title="Tracking Ratio (%)", rangemode="tozero"),
                legend=dict(orientation="h", y=-0.2),
            )
            st.plotly_chart(fig, key="earnings_14")

            # Per-quarter detail table
            disp_tr = lead_tbl[["quarter_str", "pos_sales", "dart_sales", "tracking_ratio"]].copy()
            disp_tr["상태"] = disp_tr["tracking_ratio"].apply(
                lambda v: "⚠️ 이상" if pd.notna(v) and not np.isinf(v) and (v > upper or v < lower) else "✅ 정상"
            )
            disp_tr["tracking_ratio"] = disp_tr["tracking_ratio"].apply(
                lambda v: f"{v:.1f}%" if pd.notna(v) and not np.isinf(v) else "—"
            )
            st.dataframe(
                disp_tr.rename(columns={
                    "quarter_str":    "분기",
                    "pos_sales":      "POS 분기 매출",
                    "dart_sales":     "DART 분기 단독 매출",
                    "tracking_ratio": "POS Tracking Ratio",
                }), hide_index=True,
            )
            st.caption(
                "이상 분기(⚠️) = Tracking Ratio가 평균 ±1σ를 벗어난 분기. "
                "급격한 변화는 POS 데이터 누락, 신규 채널 추가, 또는 공시 수정을 시사할 수 있습니다."
            )

    # ════════════════════════════════════════════════════════════════════════
    # TAB 5 — Sanity Check
    # ════════════════════════════════════════════════════════════════════════
    with tab5:
        if lead_tbl.empty:
            st.info("DART 연동 데이터가 있어야 Sanity Check를 수행할 수 있습니다.")
        else:
            sc_here = _sanity_check(lead_tbl)
            SC_SEV_COLOR  = {"ok": "#f0fdf4", "caution": "#fffbeb", "warning": "#fff7ed", "critical": "#fef2f2"}
            SC_SEV_BORDER = {"ok": "#16a34a", "caution": "#d97706", "warning": "#f59e0b", "critical": "#dc2626"}
            SC_SEV_LABEL  = {
                "ok": "✅ 통과 (OK)",
                "caution": "⚠️ 주의 (Caution)",
                "warning": "⚠️ 경고 (Warning)",
                "critical": "🚨 Critical — 분석 결과 신뢰도 낮음",
            }
            sev = sc_here["severity"]
            st.markdown(
                f"<div style='background:{SC_SEV_COLOR[sev]};border-left:4px solid {SC_SEV_BORDER[sev]};"
                f"padding:12px 16px;border-radius:4px;font-size:14px;margin-bottom:16px'>"
                f"<b>Sanity Check 결과: {SC_SEV_LABEL[sev]}</b><br>"
                f"<span style='font-size:12px;color:#6b7280'>{sc_here['unit_hint']}</span></div>",
                unsafe_allow_html=True,
            )

            # ── 단위 정보 + 추적률 요약 ──────────────────────────────────
            u1, u2 = st.columns(2)
            with u1:
                st.markdown("**📐 단위 정보**")
                st.markdown(f"- DART API 반환 단위: `{sc_here['dart_unit']}`")
                if sc_here["pos_unit_note"]:
                    st.markdown(f"- {sc_here['pos_unit_note']}")
                if sc_here.get("avg_ratio_raw") is not None:
                    st.markdown(f"- POS/DART 규모 비율: `{sc_here['avg_ratio_raw']:.1f}×`")
                else:
                    st.markdown("- 단위 불일치 의심 없음")
            with u2:
                st.markdown("**📊 추적률 요약**")
                _at = sc_here["avg_tr"]
                _mt = sc_here["max_tr"]
                st.markdown(f"- 평균 Tracking Ratio: `{_at:.1f}%`" if not np.isnan(_at) else "- 평균: N/A")
                st.markdown(f"- 최대 Tracking Ratio: `{_mt:.1f}%`" if not np.isnan(_mt) else "- 최대: N/A")
                st.markdown(f"- 100% 초과 분기 수: `{sc_here['n_above_100']}`")
                st.markdown(f"- 극단 이상 분기 수: `{sc_here['n_extreme']}`")

            st.divider()

            # ── Raw Audit Table ──────────────────────────────────────────
            st.markdown("**🔍 Raw Audit Table — 분기별 상세 검증**")

            def _audit_status(row):
                tr = row["tracking_ratio"]
                if pd.isna(tr) or np.isinf(tr):
                    return "⚠️ 계산 불가"
                if tr > 200 or tr < 0:
                    return "🚨 극단 이상"
                if tr > 150:
                    return "⚠️ 심각 초과"
                if tr > 100:
                    return "⚠️ 범위 초과"
                if tr < 10:
                    return "⚠️ 매우 낮음"
                return "✅ 정상"

            audit_df = lead_tbl[["quarter_str", "pos_sales", "dart_sales", "tracking_ratio"]].copy()
            audit_df["검증 상태"] = audit_df.apply(_audit_status, axis=1)
            audit_df["tracking_ratio_fmt"] = audit_df["tracking_ratio"].apply(
                lambda v: f"{v:.1f}%" if pd.notna(v) and not np.isinf(v) else "—"
            )
            st.dataframe(
                audit_df[["quarter_str", "pos_sales", "dart_sales", "tracking_ratio_fmt", "검증 상태"]].rename(columns={
                    "quarter_str":       "분기",
                    "pos_sales":         "POS 분기 매출",
                    "dart_sales":        "DART 분기 단독 매출",
                    "tracking_ratio_fmt":"POS Tracking Ratio",
                }), hide_index=True,
            )
            st.caption(
                "정상: 0~100% | 범위 초과: 100~150% (채널 범위·단위 차이 가능) | "
                "심각 초과: >150% (단위 불일치 또는 이중 집계 가능성) | 극단 이상: >200% 또는 음수"
            )

            st.divider()

            # ── Warning Classification ───────────────────────────────────
            st.markdown("**⚠️ Warning Classification — 진단된 경고 항목**")

            REMEDIATION = {
                "unit_mismatch": (
                    "데이터 전처리 시 POS 매출 단위를 원(KRW) 기준으로 통일하세요. "
                    "예) 백만원 단위 데이터 × 1,000,000"
                ),
                "channel_scope": (
                    "DART 별도(OFS) 기준 매출인지 확인하세요. "
                    "POS가 전체 채널을 집계한다면 DART 연결(CFS) 매출과 비교해야 합니다."
                ),
                "consolidated_separate": (
                    "DART API 호출 시 fs_div=OFS(별도) vs CFS(연결) 선택을 확인하세요. "
                    "POS 집계 범위와 맞는 기준을 사용하세요."
                ),
                "quarterly_transform": (
                    "DART 반기/3분기 보고서의 누적→분기 변환을 확인하세요. "
                    "디버깅 탭에서 DART YTD(API 원본) 컬럼과 변환 후 컬럼을 비교하세요."
                ),
                "duplicated_aggregation": (
                    "POS 데이터에서 동일 거래가 여러 회사/채널에 중복 집계되지 않는지 확인하세요."
                ),
            }
            ISSUE_COLOR = {
                "unit_mismatch":          "#fef2f2",
                "channel_scope":          "#fffbeb",
                "consolidated_separate":  "#fffbeb",
                "quarterly_transform":    "#fff7ed",
                "duplicated_aggregation": "#fffbeb",
            }
            ISSUE_ICON = {
                "unit_mismatch":          "🚨",
                "channel_scope":          "⚠️",
                "consolidated_separate":  "⚠️",
                "quarterly_transform":    "⚠️",
                "duplicated_aggregation": "⚠️",
            }

            if sc_here["issues"]:
                for iss in sc_here["issues"]:
                    bg  = ISSUE_COLOR.get(iss["type"], "#fffbeb")
                    ico = ISSUE_ICON.get(iss["type"], "⚠️")
                    remedy = REMEDIATION.get(iss["type"], "데이터 소스와 집계 방식을 확인하세요.")
                    st.markdown(
                        f"<div style='background:{bg};border-radius:8px;padding:12px 16px;"
                        f"margin:6px 0;font-size:13px'>"
                        f"{ico} <b>{iss['label']}</b><br>"
                        f"<span style='color:#374151'>{iss['desc']}</span><br>"
                        f"<span style='color:#6b7280;font-size:12px'>💡 {remedy}</span></div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.success("진단된 경고 항목이 없습니다. 데이터 품질이 양호합니다.")

    # ════════════════════════════════════════════════════════════════════════
    # TAB 6 — 디버깅
    # ════════════════════════════════════════════════════════════════════════
    with tab6:
        st.markdown("**DART 누적값(YTD) → 분기 단독값 변환 확인**")
        st.caption(
            "⚠️ 반기/3분기: `thstrm_add_amount`(누계) 사용 → Q2=H1-Q1, Q3=9M-H1 | "
            "1분기/사업보고서: `thstrm_amount` 사용"
        )
        if not dart_df.empty:
            debug_cols = ["quarter_str"]
            for col in ["dart_sales_ytd", "dart_sales", "ytd_converted", "reprt_code"]:
                if col in dart_df.columns:
                    debug_cols.append(col)
            disp_dart = dart_df[debug_cols].copy()
            REPRT_LABEL = {"11013": "1분기 (thstrm_amount)", "11012": "반기 (thstrm_add_amount)",
                           "11014": "3분기 (thstrm_add_amount)", "11011": "사업보고서 (thstrm_amount)"}
            if "reprt_code" in disp_dart.columns:
                disp_dart["reprt_code"] = disp_dart["reprt_code"].map(REPRT_LABEL).fillna(disp_dart["reprt_code"])
            st.dataframe(
                disp_dart.rename(columns={
                    "quarter_str":    "분기",
                    "dart_sales_ytd": "DART YTD (API 원본)",
                    "dart_sales":     "DART 분기 단독값 (변환 후)",
                    "ytd_converted":  "변환 적용",
                    "reprt_code":     "보고서 (사용 필드)",
                }), hide_index=True,
            )
        else:
            st.info("DART 연동 데이터 없음")

        if not lead_tbl.empty:
            st.markdown("**POS × DART 병합 + Tracking Ratio**")
            dbg_cols = ["quarter_str", "pos_sales", "dart_sales", "tracking_ratio",
                        "qoq_pct", "dart_qoq", "direction_match"]
            if "yoy_pct" in lead_tbl.columns:
                dbg_cols += ["yoy_pct", "dart_yoy"]
            debug2 = lead_tbl[dbg_cols].copy()
            debug2["direction_match"] = debug2["direction_match"].map({True: "✅", False: "❌"})
            debug2["tracking_ratio"]  = debug2["tracking_ratio"].apply(
                lambda v: f"{v:.1f}%" if pd.notna(v) and not np.isinf(v) else "—"
            )
            st.dataframe(
                debug2.rename(columns={
                    "quarter_str":    "분기",
                    "pos_sales":      "POS 분기 매출",
                    "dart_sales":     "DART 분기 단독",
                    "tracking_ratio": "POS Tracking Ratio",
                    "qoq_pct":        "POS QoQ(%)",
                    "dart_qoq":       "DART QoQ(%)",
                    "direction_match":"방향 일치",
                    "yoy_pct":        "POS YoY(%)",
                    "dart_yoy":       "DART YoY(%)",
                }), hide_index=True,
            )

        if not view_agg.empty:
            st.markdown("**POS 분기 집계 원본**")
            display_cols = [c for c in view_agg.columns if not str(c).startswith("_")]
            st.dataframe(view_agg[display_cols], hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# 🔗 DART 매핑 검증 + 재매핑 UI (mapping_app 패턴)
# ══════════════════════════════════════════════════════════════════════════════

def _render_dart_mapping_audit(
    pos_companies: list[str],
    corp_code_map: dict[str, str],
    dart_by_company: dict,
) -> None:
    """결과 화면 안에서 DART 매핑 상태를 검증하고 재매핑할 수 있는 UI.

    Args:
        pos_companies:   POS 데이터의 회사명 리스트 (전체)
        corp_code_map:   {POS 회사명: corp_code} — 매핑된 회사만
        dart_by_company: {POS 회사명: DART DataFrame} — 실제 응답 받은 회사
    """
    if not pos_companies:
        return

    # ── 매칭 메트릭 (mapping_app 패턴) ───────────────────────────────────────
    n_total   = len(pos_companies)
    n_mapped  = len([c for c in pos_companies if c in corp_code_map])
    n_with_data = len(dart_by_company)
    n_no_data = sum(1 for c in pos_companies if c in corp_code_map and c not in dart_by_company)
    n_unmap   = sum(1 for c in pos_companies if c not in corp_code_map)

    st.markdown("#### 🔗 DART 매핑 검증")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("POS 회사", f"{n_total}")
    m2.metric("✅ DART 매핑됨", f"{n_mapped}",
              delta=f"{n_mapped/max(n_total,1)*100:.0f}% 커버", delta_color="off")
    m3.metric("📊 공시 데이터 있음", f"{n_with_data}",
              help="매핑되었고 실제로 DART에서 공시 매출 응답을 받은 회사")
    m4.metric("❌ 미매핑", f"{n_unmap}",
              delta=f"{n_no_data}개 매핑됐지만 공시 무" if n_no_data else None,
              delta_color="off")

    # ── DART 마스터 로드 상태 확인 ────────────────────────────────────────────
    listed = st.session_state.get("_dart_listed", [])
    name_by_cc: dict[str, str] = {}
    name_by_cc_eng: dict[str, str] = {}
    stock_by_cc: dict[str, str] = {}
    if listed:
        for x in listed:
            cc = x.get("corp_code")
            if cc:
                name_by_cc[cc] = x.get("name", "")
                stock_by_cc[cc] = x.get("stock_code", "")
    # corp_code → 이름 lookup (mapping_app 결과를 활용)
    p_match = st.session_state.get("p_dart_match")
    if p_match is not None and not p_match.empty:
        for _, r in p_match.iterrows():
            cc = r.get("corp_code")
            if cc:
                name_by_cc.setdefault(cc, r.get("corp_name", ""))
                name_by_cc_eng.setdefault(cc, r.get("corp_name_eng", ""))
                stock_by_cc.setdefault(cc, r.get("stock_code", ""))

    # ── 매핑 검증 표 (mapping_app 결과 표 형태) ─────────────────────────────
    rows = []
    for co in sorted(pos_companies):
        cc = corp_code_map.get(co, "")
        has_data = co in dart_by_company
        n_quarters = 0
        if has_data:
            ddf = dart_by_company.get(co, pd.DataFrame())
            n_quarters = len(ddf) if isinstance(ddf, pd.DataFrame) else 0
        # 상태
        if has_data:
            status_label = "✅ 연동·공시"
        elif cc:
            status_label = "⚠️ 매핑만 (공시 무)"
        else:
            status_label = "❌ 미매핑"
        # 매핑 출처 — session_state의 user_mapping 비교
        user_map = st.session_state.get("_dart_user_mapping", {}) or {}
        if co in user_map and user_map[co] == cc:
            source = "🔵 수동 선택"
        elif cc:
            source = "🟢 자동 매칭"
        else:
            source = "—"
        rows.append({
            "상태":         status_label,
            "POS 회사명":   co,
            "DART 한글명":  name_by_cc.get(cc, "—") if cc else "—",
            "영문명":       name_by_cc_eng.get(cc, "") if cc else "",
            "단축코드":     stock_by_cc.get(cc, "") if cc else "",
            "corp_code":   cc or "—",
            "공시 분기":    n_quarters,
            "매핑 출처":    source,
        })
    audit_df = pd.DataFrame(rows)
    st.dataframe(audit_df, hide_index=True, use_container_width=True)
    st.caption(
        "✅ 연동·공시: 매핑됐고 DART 공시 매출 응답도 받음 · "
        "⚠️ 매핑만: DART 회사 매핑은 됐으나 해당 기간 공시 응답 없음 · "
        "❌ 미매핑: corp_code 매칭 실패"
    )

    # ── 🔁 재매핑 UI (mapping_app 패턴 그대로) ──────────────────────────────
    with st.expander(
        "🔁 회사별 DART 매핑 다시 선택 (잘못된 매핑·동명 회사 정정)",
        expanded=False,
    ):
        if not listed:
            st.warning(
                "DART 기업 목록이 아직 로드되지 않았습니다. "
                "Step 4 (분석 설정) → Earnings Intelligence → "
                "**🚀 DART 자동 매칭 시작** 버튼을 먼저 실행하세요."
            )
        else:
            st.caption(
                "선택을 바꾸려면 회사 옆 드롭다운에서 새 DART 회사를 고르세요. "
                "변경 후 아래 **재계산 권장** 버튼으로 Step 4에 돌아가 다시 실행하면 적용됩니다."
            )
            # 후보 옵션
            opt_codes  = [""] + [x["corp_code"] for x in listed]
            opt_labels = ["— 매핑 안 함 / 자동 매칭에 위임 —"] + [
                f"{x['name']}  ({x['stock_code']})" for x in listed
            ]

            user_mapping = dict(st.session_state.get("_dart_user_mapping", {}))
            changed = False

            for co in sorted(pos_companies):
                cur_cc = corp_code_map.get(co, "")
                if cur_cc in opt_codes:
                    cur_idx = opt_codes.index(cur_cc)
                else:
                    cur_idx = 0
                col_a, col_b = st.columns([1.2, 4])
                col_a.markdown(
                    f"<div style='padding:7px 0;font-size:13px'><b>{co}</b></div>",
                    unsafe_allow_html=True,
                )
                new_idx = col_b.selectbox(
                    f"_audit_pick_{co}",
                    options=range(len(opt_labels)),
                    index=cur_idx,
                    format_func=lambda i, ls=opt_labels: ls[i],
                    label_visibility="collapsed",
                    key=f"_audit_pick_{co}",
                )
                new_cc = opt_codes[new_idx]
                # 변경 감지
                if new_cc and new_cc != cur_cc:
                    user_mapping[co] = new_cc
                    changed = True
                elif not new_cc and co in user_mapping:
                    user_mapping.pop(co, None)
                    changed = True

            st.session_state["_dart_user_mapping"] = user_mapping

            if changed:
                st.warning(
                    "✏️ 매핑이 변경되었습니다. **Step 4로 돌아가 Earnings Intelligence를 다시 실행** "
                    "하면 새 매핑이 반영됩니다."
                )

    # ── 🛠 결과 화면에서 직접 매핑 완성하기 (Step 4 안 거치고) ─────────────
    _render_inline_dart_setup(pos_companies)


# ══════════════════════════════════════════════════════════════════════════════
# 🛠 결과 화면 안에서 DART 매핑 완성 + Earnings 재실행 (Step 4 안 거침)
# ══════════════════════════════════════════════════════════════════════════════

def _render_inline_dart_setup(pos_companies: list[str]) -> None:
    """결과 화면에서 DART API Key 확인/입력 → 자동 매칭 → 재매핑 → 재실행을 한 곳에서.

    Step 4로 돌아가지 않고도 매핑 완성 가능.
    """
    st.markdown("---")
    st.markdown("### 🛠 여기서 매핑 완성 + 재실행")
    st.caption(
        "Step 4로 돌아가지 않고 이 화면에서 DART 매핑을 완성하고 Earnings만 다시 실행할 수 있어요."
    )

    # ── DART API Key 입력 + 영구 저장 ─────────────────────────────────────
    try:
        from analysis_app.secrets_store import (
            load_persistent_secrets, save_persistent_secret, PERSIST_PATH,
        )
        _persisted = load_persistent_secrets()
        _has_saved = bool(_persisted.get("dart_api_key"))
    except Exception:
        _persisted = {}
        _has_saved = False

    kc1, kc2 = st.columns([5, 1])
    with kc1:
        dart_api_val = st.text_input(
            "DART API Key",
            type="password",
            key="p_dart_key",   # ← Step 4와 동일한 키 → 한쪽에서 입력하면 양쪽 공유
            help="https://opendart.fss.or.kr 에서 무료 발급. 저장하면 다음 실행 시 자동 로드.",
        )
    with kc2:
        st.markdown("<div style='padding-top:28px'></div>", unsafe_allow_html=True)
        if st.button("💾 저장", key="ei_inline_save", use_container_width=True,
                     help="이 컴퓨터에 영구 저장"):
            if dart_api_val:
                try:
                    save_persistent_secret("dart_api_key", dart_api_val)
                    st.toast("✓ 저장됨", icon="💾")
                except Exception:
                    st.toast("저장 실패", icon="❌")
            else:
                st.toast("API Key를 먼저 입력하세요", icon="⚠️")

    if not dart_api_val:
        st.info("💡 DART API Key를 입력하면 아래 자동 매칭 버튼이 활성화됩니다.")
        return

    # ── 🚀 DART 자동 매칭 (mapping_app 패턴) ───────────────────────────────
    from modules.mapping.dart_lookup import (
        fetch_dart_corp_master, match_dart_companies, dart_summary
    )

    bcol1, bcol2 = st.columns([1.4, 3])
    with bcol1:
        do_match = st.button(
            "🚀 DART 자동 매칭 실행",
            key="ei_inline_match",
            type="primary",
            disabled=not bool(dart_api_val),
            use_container_width=True,
            help=f"{len(pos_companies)}개 POS 회사명을 DART 마스터와 자동 매칭",
        )
    with bcol2:
        if st.button(
            "🔄 DART 캐시 비우고 재시도",
            key="ei_inline_clear",
            use_container_width=True,
            help="네트워크 차단·서버 reset 후 캐시 클리어 + 새로 받기",
        ):
            try:
                fetch_dart_corp_master.clear()
            except Exception:
                pass
            try:
                _fetch_corp_code_map.clear()
            except Exception:
                pass
            st.session_state.pop("p_dart_match", None)
            st.session_state.pop("_dart_listed", None)
            st.toast("DART 캐시 클리어 — '🚀 DART 자동 매칭 실행' 다시 클릭", icon="🔄")

    if do_match:
        master = None
        try:
            with st.spinner("DART 마스터 다운로드 중... (자동 재시도 포함)"):
                master = fetch_dart_corp_master(dart_api_val)
        except RuntimeError as e:
            st.error(f"❌ {e}")
        except Exception as e:
            st.error(f"❌ DART 조회 실패: {type(e).__name__}: {str(e)[:200]}")

        if master is not None and not master.empty:
            with st.spinner(f"{len(pos_companies)}개 회사 매칭 중..."):
                st.session_state["p_dart_match"] = match_dart_companies(
                    pos_companies, master,
                )
            # listed 목록도 저장 — selectbox 재매핑에서 사용
            try:
                listed_df = master[master["stock_code"].astype(str).str.strip() != ""]
                st.session_state["_dart_listed"] = [
                    {"name": r["corp_name"], "corp_code": r["corp_code"],
                     "stock_code": r["stock_code"]}
                    for _, r in listed_df.iterrows()
                ]
            except Exception:
                pass
            st.rerun()

    # ── 매칭 결과 요약 + 동명 후보 selectbox ────────────────────────────────
    match_df = st.session_state.get("p_dart_match")
    if match_df is not None and not match_df.empty:
        summary = dart_summary(match_df)
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("입력 회사",   f"{summary['total']:,}")
        m2.metric("✅ 완전 매칭", f"{summary['exact']:,}")
        m3.metric("🟡 부분 매칭", f"{summary['partial']:,}")
        m4.metric("❓ 동명 후보", f"{summary['ambiguous']:,}",
                  help="후보 2개 이상 — 아래에서 정확한 회사 선택")
        m5.metric("❌ 매칭 실패", f"{summary['none']:,}",
                  delta=f"{summary['rate']*100:.0f}% 커버", delta_color="off")

        # 동명 후보 selectbox
        amb = match_df[match_df["n_candidates"] > 1]
        if not amb.empty:
            with st.expander(
                f"❓ 동명 회사 후보 — {len(amb)}개 회사 선택 필요",
                expanded=True,
            ):
                for _, row in amb.iterrows():
                    inp = row["input_name"]
                    cands = row["candidates"]
                    if not cands:
                        continue
                    option_labels = [
                        f"{c['corp_code']}  ·  {c['corp_name']} "
                        f"{('('+c['corp_name_eng']+')') if c.get('corp_name_eng') else ''} "
                        f"{'📈상장' if c.get('stock_code') else '비상장'}"
                        for c in cands
                    ]
                    cur_cc = row.get("corp_code", "")
                    default_idx = next(
                        (i for i, c in enumerate(cands) if c.get("corp_code") == cur_cc),
                        0,
                    )
                    chosen_idx = st.selectbox(
                        f"`{inp}` — {len(cands)}개 후보",
                        options=range(len(option_labels)),
                        index=default_idx,
                        format_func=lambda i, ls=option_labels: ls[i],
                        key=f"ei_inline_pick__{inp}",
                    )
                    chosen = cands[chosen_idx]
                    mask = match_df["input_name"] == inp
                    match_df.loc[mask, "corp_code"]     = chosen.get("corp_code", "")
                    match_df.loc[mask, "corp_name"]     = chosen.get("corp_name", "")
                    match_df.loc[mask, "corp_name_eng"] = chosen.get("corp_name_eng", "")
                    match_df.loc[mask, "stock_code"]    = chosen.get("stock_code", "")
                st.session_state["p_dart_match"] = match_df

        # 사용자 매핑 (POS 회사명 → corp_code) 저장
        user_dart_map: dict[str, str] = {}
        for _, r in match_df.iterrows():
            if r.get("corp_code"):
                user_dart_map[r["input_name"]] = r["corp_code"]
        st.session_state["_dart_user_mapping"] = user_dart_map

        # ── ♻️ Earnings만 재실행 ─────────────────────────────────────────────
        st.markdown("---")
        rr1, rr2 = st.columns([1.5, 4])
        with rr1:
            rerun_clicked = st.button(
                "♻️ Earnings 재실행",
                key="ei_inline_rerun",
                type="primary",
                use_container_width=True,
                help="새 매핑으로 Earnings Intelligence만 다시 계산 (다른 모듈은 유지)",
            )
        with rr2:
            n_mapped = sum(1 for v in user_dart_map.values() if v)
            st.caption(
                f"📌 위 매핑이 확정되면 클릭 — "
                f"DART 매핑된 {n_mapped}개사 기준으로 Earnings Intelligence 재계산. "
                "다른 모듈 결과(Growth/Market Signal 등)는 그대로 유지됨."
            )

        if rerun_clicked:
            df = st.session_state.get("raw_df")
            role_map = st.session_state.get("role_map", {})
            if df is None or not role_map:
                st.error("원본 데이터 또는 role_map이 없습니다. Step 1~2를 다시 확인하세요.")
            else:
                params = {
                    "dart_api_key":         dart_api_val,
                    "dart_company_mapping": dict(user_dart_map),
                    "manual_mapping":       "",
                }
                with st.spinner("Earnings Intelligence 재실행 중..."):
                    try:
                        new_result = run_earnings_intel(df, role_map, params)
                    except Exception as exc:
                        new_result = {
                            "status": "failed",
                            "message": f"{type(exc).__name__}: {str(exc)[:200]}",
                            "data": None, "metrics": {},
                        }
                results = st.session_state.get("results", {}) or {}
                results["earnings_intel"] = new_result
                st.session_state["results"] = results
                st.toast("✓ Earnings 재실행 완료", icon="♻️")
                st.rerun()
    else:
        st.caption(
            "위 **🚀 DART 자동 매칭 실행** 버튼을 누르면 매칭 결과가 여기 표시됩니다."
        )


# ══════════════════════════════════════════════════════════════════════════════
# 🔗 Market Signal 연동 — 주가 QoQ overlay
# ══════════════════════════════════════════════════════════════════════════════

def _render_stock_link_status(company: str) -> None:
    """주가 연동 상태를 큰 box로 표시 — 사용자가 안 보이는 이유를 즉시 파악 가능하게."""
    if not company or company == "전체":
        st.info(
            "📊 **주가 연동**: '전체' 선택 시 단일 ticker로 매핑할 수 없어 주가 line이 표시되지 않습니다. "
            "위 회사 선택 드롭다운에서 특정 회사를 고르면 활성화."
        )
        return

    results = st.session_state.get("results", {}) or {}
    mkt = results.get("market_signal")

    # 케이스 1: Market Signal 자체가 실행 안 됨
    if mkt is None:
        st.warning(
            "⛔ **주가 line이 안 보이는 이유**: Market Signal이 실행되지 않았습니다.\n\n"
            "**해결 방법**:\n"
            "1. 좌하단 **← 분석 설정** 버튼 클릭 → Step 4로 이동\n"
            "2. **📉 Market Signal** 카드 좌측 체크박스 ON\n"
            "3. 우하단 **▶ 분석 실행** 클릭\n"
            "4. 자동으로 Step 5로 돌아옴 → Earnings 탭 → 이 차트에 🔴 빨간 점선 주가 line 표시"
        )
        return

    # 케이스 2: Market Signal 실행됐지만 이 회사의 매핑이 없음
    sigs = mkt.get("_company_signals", []) or []
    sig = next((s for s in sigs if s.get("company") == company), None)
    if sig is None:
        sample_co = [s.get("company") for s in sigs[:5] if s.get("company")]
        st.warning(
            f"⚠️ **주가 line 없음** — Market Signal 결과에 회사명 `'{company}'`이(가) 없습니다.\n\n"
            f"**Market Signal에 매핑된 회사 (앞 5개)**: {', '.join(sample_co) if sample_co else '없음'}\n\n"
            "**가능한 원인**:\n"
            "- POS 데이터에 회사명이 다른 이름으로 등록됨 (예: `_ALL` suffix 차이)\n"
            "- Market Signal 실행 시 stock_code 매핑 실패"
        )
        return

    # 케이스 3: 매핑은 있지만 status != ok (yfinance 실패 등)
    if sig.get("status") != "ok":
        st.warning(
            f"⚠️ **주가 line 없음** — `{company}` ticker `{sig.get('ticker','—')}`의 yfinance 응답 실패.\n\n"
            f"실패 사유: {sig.get('fail_reason', '—')}"
        )
        return

    # 케이스 4: ohlcv 데이터 없음
    daily = sig.get("_daily_ohlcv")
    if not isinstance(daily, pd.DataFrame) or daily.empty:
        st.warning(
            f"⚠️ **주가 line 없음** — `{company}` ticker `{sig.get('ticker','—')}`의 일별 주가 데이터가 비어 있습니다."
        )
        return

    # 케이스 5: 정상 — 매핑됨
    n_days = len(daily)
    date_range = ""
    if "date" in daily.columns:
        try:
            d = pd.to_datetime(daily["date"], errors="coerce").dropna()
            if not d.empty:
                date_range = f" · {d.min().date()} ~ {d.max().date()}"
        except Exception:
            pass
    st.success(
        f"✅ **주가 연동됨** — `{company}` → ticker `{sig.get('ticker','—')}` "
        f"({n_days}일 일별 종가{date_range}). 아래 QoQ 차트의 **🔵 파란 점선이 주가 QoQ(%)** 입니다 (우측 축)."
    )


def _add_stock_level_overlay(fig, company: str, level_data: pd.DataFrame) -> bool:
    """Market Signal 결과에서 해당 회사의 분기말 종가를 차트에 overlay (level).

    Args:
        fig:        plotly figure
        company:    POS 회사명
        level_data: dart level 데이터 (quarter_str 컬럼 보유)

    Returns:
        True if overlay added.
    """
    if not company or company == "전체":
        return False
    try:
        results = st.session_state.get("results", {}) or {}
        sigs = (results.get("market_signal", {}) or {}).get("_company_signals", []) or []
    except Exception:
        return False
    sig = next((s for s in sigs if s.get("company") == company
                 and s.get("status") == "ok"), None)
    if sig is None:
        return False
    daily = sig.get("_daily_ohlcv", pd.DataFrame())
    ticker = sig.get("ticker", "")
    if not isinstance(daily, pd.DataFrame) or daily.empty:
        return False
    try:
        d = daily.copy()
        if "date" not in d.columns or "adj_close" not in d.columns:
            return False
        d["date"] = pd.to_datetime(d["date"], errors="coerce")
        d = d.dropna(subset=["date", "adj_close"])
        d["quarter"] = d["date"].dt.to_period("Q")
        quarter_close = (
            d.sort_values("date")
            .groupby("quarter")["adj_close"]
            .last()
            .reset_index()
        )
        quarter_close["quarter_str"] = quarter_close["quarter"].astype(str)
        merged = level_data[["quarter_str"]].merge(
            quarter_close[["quarter_str", "adj_close"]],
            on="quarter_str", how="left",
        )
        if merged["adj_close"].notna().sum() < 2:
            return False

        import plotly.graph_objects as _go
        fig.add_trace(_go.Scatter(
            x=merged["quarter_str"],
            y=merged["adj_close"].round(0),
            mode="lines+markers",
            name=f"주가 (분기말 종가) — {ticker}",
            line=dict(color="#dc2626", width=2, dash="dot"),
            marker=dict(size=5, symbol="circle"),
            yaxis="y2",
        ))
        return True
    except Exception:
        return False


def _add_stock_qoq_overlay(fig, company: str, qoq_data: pd.DataFrame) -> bool:
    """Market Signal 결과에서 해당 회사의 주가 데이터를 가져와 분기 QoQ로 차트에 overlay.

    Args:
        fig:      plotly figure (yaxis2 overlay 추가됨)
        company:  POS 회사명 (Earnings selected 회사)
        qoq_data: dart QoQ 데이터 (quarter_str 컬럼 보유)

    Returns:
        True if overlay added, False otherwise.
    """
    if not company or company == "전체":
        return False
    try:
        results = st.session_state.get("results", {}) or {}
        mkt = results.get("market_signal", {}) or {}
        sigs = mkt.get("_company_signals", []) or []
    except Exception:
        return False

    # 회사명 매칭 — Market Signal에 동일 이름 있는지
    sig = next((s for s in sigs if s.get("company") == company
                 and s.get("status") == "ok"), None)
    if sig is None:
        return False

    daily = sig.get("_daily_ohlcv", pd.DataFrame())
    ticker = sig.get("ticker", "")
    if not isinstance(daily, pd.DataFrame) or daily.empty:
        return False

    # 일별 종가 → 분기말 종가 추출
    try:
        d = daily.copy()
        if "date" not in d.columns or "adj_close" not in d.columns:
            return False
        d["date"] = pd.to_datetime(d["date"], errors="coerce")
        d = d.dropna(subset=["date", "adj_close"])
        d["quarter"] = d["date"].dt.to_period("Q")
        # 각 분기의 마지막 거래일 종가
        quarter_close = (
            d.sort_values("date")
            .groupby("quarter")["adj_close"]
            .last()
            .reset_index()
        )
        quarter_close["quarter_str"] = quarter_close["quarter"].astype(str).str.replace("-", "")
        # 'YYYYQ#' 형식으로 변환 (예: 2024Q1)
        quarter_close["quarter_str"] = quarter_close["quarter"].astype(str)
        quarter_close["price_qoq"] = quarter_close["adj_close"].pct_change() * 100

        # qoq_data의 quarter_str 형식에 맞춰 매칭
        # earnings의 quarter_str은 보통 "2024Q1" 형식 — Period.astype(str) 결과는 "2024Q1"
        merged = qoq_data[["quarter_str"]].merge(
            quarter_close[["quarter_str", "price_qoq"]],
            on="quarter_str", how="left",
        )
        if merged["price_qoq"].notna().sum() < 2:
            return False

        import plotly.graph_objects as _go
        fig.add_trace(_go.Scatter(
            x=merged["quarter_str"],
            y=merged["price_qoq"].round(1),
            mode="lines+markers",
            name=f"주가 QoQ (%) — {ticker}",
            line=dict(color="#1e40af", width=2, dash="dash"),
            marker=dict(size=5, symbol="diamond"),
            yaxis="y2",  # 보조 축
        ))
        return True
    except Exception:
        return False
