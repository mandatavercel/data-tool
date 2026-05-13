"""
modules/mapping/dart_lookup.py

DART 공시 정보 활용 — 회사명 → 법인등록번호(jurir_no) + 영문회사명 매핑.

데이터 소스:
  - https://opendart.fss.or.kr/api/corpCode.xml  (전체 회사 마스터, ZIP)
       → corp_code, corp_name (한글), corp_eng_name, stock_code, modify_date
  - https://opendart.fss.or.kr/api/company.json  (회사 상세, corp_code 별)
       → jurir_no (법인등록번호 13자리), bizr_no (사업자등록번호), 그 외

동명 회사가 자주 있으므로 매칭 결과는 단일이 아니라 **후보 리스트**로 보관.
사용자가 어떤 후보를 채택할지 UI 에서 선택할 수 있어야 한다.

분석 앱(`modules.analysis.signal.earnings._fetch_corp_code_map`)과
의도적으로 코드를 분리한다 — 매핑 앱은 독립적으로 진화.
"""
from __future__ import annotations

import io
import re
import zipfile
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import streamlit as st


DART_BASE = "https://opendart.fss.or.kr/api"


# ── 회사명 정규화 ─────────────────────────────────────────────────────────────
# lookup.py 의 normalize_company 와 동일 의도. 양쪽이 호환되도록 import 한다.
def _normalize(name: str) -> str:
    from modules.mapping.lookup import normalize_company
    return normalize_company(name)


# ══════════════════════════════════════════════════════════════════════════════
# DART 마스터 (corpCode.xml)
# ══════════════════════════════════════════════════════════════════════════════

# ── 모듈 레벨 세션 (keep-alive) ─────────────────────────────────────────────
# Streamlit Cloud (미국 IP) → DART (한국) RTT ~300ms.
# 호출마다 새 TCP/TLS handshake 비용 ~1.2s 절약.
import threading as _threading
_DART_SESSION: requests.Session | None = None
_DART_SESSION_LOCK = _threading.Lock()


def _retry_session() -> requests.Session:
    """모듈 레벨 keep-alive 세션 — 재시도/backoff/pool 모두 포함.

    같은 host(opendart.fss.or.kr)만 호출하므로 단일 pool로 충분.
    timeout이 짧을수록 Cloud에서 무한 로딩처럼 보이지 않음.
    """
    global _DART_SESSION
    if _DART_SESSION is not None:
        return _DART_SESSION

    with _DART_SESSION_LOCK:
        if _DART_SESSION is not None:
            return _DART_SESSION

        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter

        sess = requests.Session()
        retries = Retry(
            total=2,            # 3→2 (Cloud에선 빠른 실패 우선)
            backoff_factor=1.0, # 1.5→1.0 (2s, 3s)
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=8,
            pool_maxsize=8,
        )
        sess.mount("https://", adapter)
        sess.mount("http://",  adapter)
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; alt-data-tool)",
            "Accept-Encoding": "gzip, deflate",
        })
        _DART_SESSION = sess
        return _DART_SESSION


@st.cache_data(ttl=24 * 60 * 60, show_spinner=False)
def fetch_dart_corp_master(api_key: str) -> pd.DataFrame:
    """
    DART corpCode.xml → 전체 회사 마스터 DataFrame.

    Connection reset / 5xx / 타임아웃은 최대 3회 자동 재시도 (1.5s → 3s → 6s backoff).

    반환 컬럼:
        corp_code (str, 8자리)     : DART 내부 ID (법인등록번호 아님)
        corp_name (str)            : 한글 정식명
        corp_name_eng (str)        : 영문명 (없으면 빈 문자열)
        stock_code (str, 6자리)    : KRX 단축코드 (상장사만, 비상장은 빈 문자열)
        modify_date (str, YYYYMMDD): 마스터 수정일
    """
    if not api_key or not api_key.strip():
        raise ValueError("DART API 인증키가 비어 있습니다")

    url = f"{DART_BASE}/corpCode.xml"

    # 모듈 세션 (keep-alive) — TLS handshake 한 번이면 끝
    sess = _retry_session()
    last_err: Exception | None = None
    for attempt in range(2):    # session retry(2) × outer(2) = 최대 4회
        try:
            # timeout=45 (120→45): Cloud에서 무한 로딩처럼 보이지 않게 빨리 실패
            resp = sess.get(url, params={"crtfc_key": api_key.strip()}, timeout=45)
            resp.raise_for_status()
            break
        except requests.exceptions.ConnectionError as e:
            last_err = e
            err_str = str(e)
            # 일시적인 reset/aborted는 잠시 후 재시도
            if any(k in err_str for k in ("Connection reset", "Connection aborted",
                                            "ConnectionResetError", "BrokenPipe")):
                import time as _t
                _t.sleep(1 + attempt * 2)   # 1s, 3s
                continue
            # 그 외 ConnectionError는 즉시 raise
            raise RuntimeError(
                f"DART 서버 접속 실패: {type(e).__name__}. "
                "회사망/VPN/방화벽에서 opendart.fss.or.kr이 차단됐는지 확인하세요. "
                "모바일 핫스팟 등 다른 네트워크에서 재시도 권장."
            ) from e
        except requests.exceptions.Timeout as e:
            last_err = e
            raise RuntimeError(
                "DART 서버 응답 타임아웃 (45초 초과). "
                "Streamlit Cloud는 미국 서버라 DART 응답이 느릴 수 있습니다. "
                "1분 후 다시 시도하거나, '🔄 마스터 재다운로드' 클릭."
            ) from e
        except requests.exceptions.HTTPError as e:
            last_err = e
            code = e.response.status_code if e.response is not None else "?"
            raise RuntimeError(
                f"DART 서버 HTTP 에러 ({code}). "
                "인증키가 정확한지 확인하거나 잠시 후 다시 시도하세요."
            ) from e
    else:
        # for-else: break 없이 끝남 → 모든 재시도 실패
        raise RuntimeError(
            "DART 서버가 반복적으로 연결을 끊었습니다 (Connection reset). "
            "잠시 후(30초~1분) 다시 시도하거나, 회사망 대신 모바일 핫스팟에서 재시도하세요."
        ) from last_err

    content = resp.content
    # 응답이 JSON 에러 메시지일 수도 있음 (인증키 잘못 등)
    if content[:1] in (b"{", b"<"):
        # 일부 케이스: HTML 에러 페이지로 응답
        if b"<html" in content[:200].lower():
            raise RuntimeError("DART 응답이 HTML 페이지 — 인증키 확인 또는 API 차단")
        # JSON 에러 응답
        if content[:1] == b"{":
            try:
                import json
                err = json.loads(content)
                raise RuntimeError(
                    f"DART API 에러: status={err.get('status')}, "
                    f"message={err.get('message')}"
                )
            except RuntimeError:
                raise
            except Exception:
                pass

    # ZIP 파일이어야 정상
    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile as e:
        # 응답 앞부분 표시
        head = content[:200]
        raise RuntimeError(
            f"DART 응답이 ZIP 형식 아님 — 인증키/엔드포인트 확인 필요. "
            f"앞부분: {head!r}"
        ) from e

    names = zf.namelist()
    if not names:
        raise RuntimeError("DART ZIP 응답이 비어 있음")

    with zf.open(names[0]) as f:
        xml_data = f.read()

    root = ET.fromstring(xml_data)
    rows: list[dict] = []
    for elem in root.iter("list"):
        rows.append({
            "corp_code":     (elem.findtext("corp_code") or "").strip(),
            "corp_name":     (elem.findtext("corp_name") or "").strip(),
            "corp_name_eng": (elem.findtext("corp_eng_name") or "").strip(),
            "stock_code":    (elem.findtext("stock_code") or "").strip(),
            "modify_date":   (elem.findtext("modify_date") or "").strip(),
        })
    if not rows:
        raise RuntimeError("DART corpCode.xml 에서 회사 정보를 찾지 못함")

    df = pd.DataFrame(rows)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 회사 상세 정보 (company.json) — jurir_no 등 포함
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=24 * 60 * 60, show_spinner=False)
def fetch_company_info(api_key: str, corp_code: str) -> dict:
    """단일 corp_code → 회사 상세 정보 (jurir_no=법인등록번호 포함).

    반환 키: corp_code, corp_name, corp_name_eng, jurir_no, bizr_no,
            ceo_nm, est_dt, ind_tp, hm_url, status.
    실패 시 status='error' + message.
    """
    if not api_key or not corp_code:
        return {"corp_code": corp_code, "status": "error", "message": "key/corp_code 누락"}
    url = f"{DART_BASE}/company.json"
    try:
        r = requests.get(
            url,
            params={"crtfc_key": api_key.strip(), "corp_code": str(corp_code).zfill(8)},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"corp_code": corp_code, "status": "error", "message": str(e)}

    if data.get("status") != "000":
        return {
            "corp_code": corp_code,
            "status":    "error",
            "message":   f"{data.get('status')}: {data.get('message')}",
        }

    return {
        "corp_code":     corp_code,
        "corp_name":     data.get("corp_name", ""),
        "corp_name_eng": data.get("corp_name_eng", "") or data.get("corp_name_eng_full", ""),
        "jurir_no":      data.get("jurir_no", ""),     # 법인등록번호 13자리
        "bizr_no":       data.get("bizr_no", ""),      # 사업자등록번호 10자리
        "ceo_nm":        data.get("ceo_nm", ""),
        "est_dt":        data.get("est_dt", ""),
        "ind_tp":        data.get("ind_tp", ""),
        "induty_code":   data.get("induty_code", ""),  # 표준산업분류 코드 (KSIC 6자리)
        "corp_cls":      data.get("corp_cls", ""),     # 시장구분 Y=KOSPI K=KOSDAQ N=KONEX E=기타
        "stock_code":    data.get("stock_code", "").strip() if isinstance(data.get("stock_code"), str) else "",
        "hm_url":        data.get("hm_url", ""),
        "status":        "ok",
    }


def fetch_jurir_nos_batch(
    api_key: str,
    corp_codes: list[str],
    progress_callback=None,
) -> dict[str, dict]:
    """corp_code 리스트 → {corp_code: 회사상세} 일괄 조회.

    Streamlit cache_data 가 단일 호출 단위로 작동하므로
    반복 호출이 캐시 hit 으로 빨라진다.
    """
    out: dict[str, dict] = {}
    total = len(corp_codes)
    for i, cc in enumerate(corp_codes):
        if not cc:
            continue
        info = fetch_company_info(api_key, str(cc).zfill(8))
        out[cc] = info
        if progress_callback is not None:
            progress_callback(i + 1, total, info)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 최대주주 조회 (hyslrSttus.json — 사업보고서 기준)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_largest_shareholder(
    api_key: str,
    corp_code: str,
    bsns_year: str = "2024",
    reprt_code: str = "11011",   # 11011=사업보고서, 11012=반기, 11013=1Q, 11014=3Q
) -> dict:
    """DART 최대주주현황 조회. 가장 지분율 높은 주주 1명을 반환.

    반환:
        {nm: 한글명, relate: 관계, qota_rt: 지분율(float),
         status: 'ok'|'no_data'|'error', message: ...}
    """
    if not api_key or not corp_code:
        return {"status": "error", "message": "key/corp_code 누락"}
    url = f"{DART_BASE}/hyslrSttus.json"
    try:
        r = requests.get(
            url,
            params={"crtfc_key":  api_key.strip(),
                    "corp_code":  str(corp_code).zfill(8),
                    "bsns_year":  bsns_year,
                    "reprt_code": reprt_code},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"status": "error", "message": str(e)}

    if data.get("status") != "000":
        return {
            "status":  "no_data",
            "message": f"{data.get('status')}: {data.get('message')}",
        }

    items = data.get("list", []) or []
    if not items:
        return {"status": "no_data", "message": "list 비어있음"}

    def _qota(x) -> float:
        try:
            return float(str(x.get("trmend_posesn_stock_qota_rt") or "0").replace(",", ""))
        except (ValueError, TypeError):
            return 0.0

    # 합계·소계 행은 제외 — nm='계' 또는 빈 값, relate='계'/'소계'/'합계'/'-' 등
    _BAD_NM = {"", "계", "합계", "소계", "-", "—"}
    _BAD_RELATE = {"계", "합계", "소계", ""}

    def _is_total_row(it: dict) -> bool:
        nm = str(it.get("nm") or "").strip()
        rl = str(it.get("relate") or "").strip()
        if nm in _BAD_NM:
            return True
        if rl in _BAD_RELATE and nm in _BAD_NM:
            return True
        return False

    items = [it for it in items if not _is_total_row(it)]
    if not items:
        return {"status": "no_data", "message": "유효 주주 행 없음 (합계만 존재)"}

    # '본인' 관계 우선, 없으면 지분율 최고
    self_rows = [it for it in items if str(it.get("relate", "")).strip() == "본인"]
    if self_rows:
        best = max(self_rows, key=_qota)
    else:
        best = max(items, key=_qota)

    return {
        "status":   "ok",
        "nm":       (best.get("nm") or "").strip(),
        "relate":   (best.get("relate") or "").strip(),
        "qota_rt":  _qota(best),
        "year":     bsns_year,
    }


def fetch_largest_shareholders_batch(
    api_key: str,
    corp_codes: list[str],
    bsns_years: list[str] | None = None,
    progress_callback=None,
    max_workers: int = 8,
    listed_only_codes: set[str] | None = None,
) -> dict[str, dict]:
    """corp_code 리스트 → 최대주주 dict. ThreadPool 병렬 호출.

    listed_only_codes: 이 set 에 속한 corp_code 만 호출 (비상장 skip 용). None 이면 전체.
    """
    out: dict[str, dict] = {}
    years = bsns_years or ["2024", "2023"]
    total = len(corp_codes)

    def _one(cc: str) -> tuple[str, dict]:
        cc = str(cc).zfill(8)
        # 비상장 skip
        if listed_only_codes is not None and cc not in listed_only_codes:
            return cc, {"status": "skipped_nonlisted", "year": years[0]}
        for y in years:
            r = fetch_largest_shareholder(api_key, cc, bsns_year=y)
            if r.get("status") == "ok":
                return cc, r
        return cc, {"status": "no_data", "year": years[0]}

    valid_codes = [str(c).zfill(8) for c in corp_codes if c]
    if not valid_codes:
        return out

    if max_workers <= 1 or len(valid_codes) == 1:
        for i, cc in enumerate(valid_codes):
            cc_z, r = _one(cc)
            out[cc_z] = r
            if progress_callback is not None:
                progress_callback(i + 1, total, r)
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=min(max_workers, len(valid_codes))) as ex:
            futures = {ex.submit(_one, cc): cc for cc in valid_codes}
            done = 0
            for fut in as_completed(futures):
                cc_z, r = fut.result()
                out[cc_z] = r
                done += 1
                if progress_callback is not None:
                    progress_callback(done, total, r)
    return out


# ══════════════════════════════════════════════════════════════════════════════
# 회사명 매칭 (다중 후보 보존)
# ══════════════════════════════════════════════════════════════════════════════

def _build_dart_index(master: pd.DataFrame) -> dict[str, list[dict]]:
    """정규화 회사명 → 후보 회사 리스트.
    동명 회사 처리: 같은 정규화 키에 여러 row 가능."""
    index: dict[str, list[dict]] = {}
    for _, row in master.iterrows():
        for col in ("corp_name", "corp_name_eng"):
            key = _normalize(row[col])
            if key:
                index.setdefault(key, []).append(row.to_dict())
    return index


def _rank_candidates(cands: list[dict]) -> list[dict]:
    """후보 우선순위:
    1) 상장사 (stock_code 있음) 우선
    2) modify_date 최신 우선
    3) corp_code 작은 순"""
    return sorted(
        cands,
        key=lambda r: (
            not bool(r.get("stock_code")),
            -int(r.get("modify_date", "0") or "0"),
            r.get("corp_code", ""),
        ),
    )


def match_dart_companies(
    company_names: list[str],
    master: pd.DataFrame,
) -> pd.DataFrame:
    """회사명 리스트 → DART 매칭 (다중 후보 보존).

    반환 컬럼:
        input_name (str)
        normalized (str)
        status (str)            : 'exact' | 'partial' | 'none'
        n_candidates (int)
        candidates (list[dict]) : 모든 후보 (우선순위 정렬)
        # 선택된 1개 후보 (사용자가 바꿀 수 있음 — session_state 에서)
        corp_code (str)
        corp_name (str)
        corp_name_eng (str)
        stock_code (str)
    """
    index = _build_dart_index(master)
    norm_keys = list(index.keys())

    results: list[dict] = []
    for raw in company_names:
        n = _normalize(raw)

        if not n:
            results.append({
                "input_name":    raw, "normalized": n,
                "status": "none", "n_candidates": 0, "candidates": [],
                "corp_code": "", "corp_name": "",
                "corp_name_eng": "", "stock_code": "",
            })
            continue

        # 완전 일치 + 부분 일치 후보를 모두 모은다 (동명 회사 확인 위해).
        exact_hits = list(index.get(n, []))
        partial_hits: list[dict] = []
        for key, rows in index.items():
            if key == n:
                continue
            if n in key or key in n:
                # prefix 매칭(예: '동원' → '동원F&B', '동원수산')은 ratio 검사 우회 —
                # 짧은 입력이 더 긴 회사명의 시작/끝이면 후보로 포함.
                is_prefix_or_suffix = (
                    key.startswith(n) or key.endswith(n) or
                    n.startswith(key) or n.endswith(key)
                )
                if is_prefix_or_suffix:
                    partial_hits.extend(rows)
                else:
                    # 내부 부분 매칭은 ratio 임계값 적용 (false positive 방지)
                    ratio = min(len(n), len(key)) / max(len(n), len(key))
                    if ratio >= 0.5:
                        partial_hits.extend(rows)

        # 중복 제거 — corp_code 기준
        seen: set[str] = set()
        candidates: list[dict] = []
        for r in exact_hits + partial_hits:
            cc = r.get("corp_code", "")
            if cc and cc not in seen:
                seen.add(cc)
                candidates.append(r)

        if exact_hits:
            status = "exact"
        elif partial_hits:
            status = "partial"
        else:
            status = "none"

        candidates = _rank_candidates(candidates)
        primary = candidates[0] if candidates else {}

        results.append({
            "input_name":    raw,
            "normalized":    n,
            "status":        status,
            "n_candidates":  len(candidates),
            "candidates":    candidates,
            "corp_code":     primary.get("corp_code", ""),
            "corp_name":     primary.get("corp_name", ""),
            "corp_name_eng": primary.get("corp_name_eng", ""),
            "stock_code":    primary.get("stock_code", ""),
        })

    return pd.DataFrame(results)


def dart_summary(match_df: pd.DataFrame) -> dict:
    total = len(match_df)
    exact = int((match_df["status"] == "exact").sum())
    partial = int((match_df["status"] == "partial").sum())
    none_ = int((match_df["status"] == "none").sum())
    ambiguous = int((match_df["n_candidates"] > 1).sum())
    return {
        "total":     total,
        "exact":     exact,
        "partial":   partial,
        "none":      none_,
        "ambiguous": ambiguous,    # 동명 회사 후보가 2개 이상
        "rate":      (exact + partial) / total if total else 0.0,
    }
