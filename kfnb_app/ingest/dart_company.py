"""
kfnb_app/ingest/dart_company.py — DART 기반 회사 자동 해석 (graceful).

회사 한글명 → 종목코드(stock_code) + 공식 영문 법인명(corp_name_eng) 을 자동 조회한다.
하드코딩 마스터 대신(또는 보강) '공시 기준' 값을 쓴다.

⚠️ 매핑 로직은 새로 만들지 않는다. 이미 검증된 사내 플로우
   `modules.mapping.dart_lookup` (keep-alive 세션·재시도/backoff·동명 후보
   보존·상장 우선 랭킹) 을 그대로 재사용한다. corpCode.xml 의 `corp_eng_name`
   에 공시 영문명이 들어 있어 회사당 추가 호출(company.json) 없이 1회 다운로드로 해결.

이 모듈은 그 플로우를 파이프라인 인터페이스 `resolve(names, api_key) -> (dict, note)`
로 감싸는 얇은 어댑터다. API 키/네트워크/의존성 없으면 ({}, 사유) 반환 — 비차단.
"""
from __future__ import annotations

import re

_CORP_CACHE: dict[str, list] = {}   # (하위호환 — 외부에서 참조될 수 있음)


def _norm(s: str) -> str:
    """매칭용 정규화. 사내 normalize_company 와 동일 의도(폴백 포함)."""
    try:
        from modules.mapping.lookup import normalize_company
        return normalize_company(s)
    except Exception:                              # noqa: BLE001 — 폴백
        s = re.sub(r"\(주\)|㈜|주식회사", "", str(s))
        return re.sub(r"\s+", "", s).strip().lower()


def resolve(names, api_key: str) -> tuple[dict, str]:
    """회사 한글명 리스트 → {회사명: {corp_code, krx_code, company_en_official}}.

    사내 `modules.mapping.dart_lookup` 재사용:
      fetch_dart_corp_master(api_key)  → 전체 회사 마스터(ZIP, 캐시·재시도)
      match_dart_companies(names, ...) → 동명 후보 보존·상장 우선 랭킹 매칭

    반환 (mapping, note). 키/네트워크/의존성 없으면 ({}, 사유).
    """
    names = [str(n) for n in dict.fromkeys(names) if str(n).strip()]
    if not api_key:
        return {}, "DART_API_KEY 없음 — 종목코드/영문명 자동조회 생략"
    if not names:
        return {}, "회사명 없음"

    try:
        from modules.mapping import dart_lookup
    except Exception as e:                         # noqa: BLE001
        return {}, f"DART 매핑 모듈 로드 실패: {type(e).__name__}"

    try:
        master = dart_lookup.fetch_dart_corp_master(api_key)
    except Exception as e:                         # noqa: BLE001
        # dart_lookup 이 사람이 읽을 수 있는 메시지를 RuntimeError 로 던짐
        return {}, f"DART 마스터 조회 실패: {e}"

    try:
        match = dart_lookup.match_dart_companies(names, master)
    except Exception as e:                         # noqa: BLE001
        return {}, f"DART 매칭 실패: {type(e).__name__}"

    out: dict[str, dict] = {}
    for _, row in match.iterrows():
        if row.get("status") == "none":
            continue
        krx = (row.get("stock_code") or "").strip()
        eng = (row.get("corp_name_eng") or "").strip()
        # 종목코드도 영문명도 없으면 보강 가치 없음 → skip (정적 마스터 유지)
        if not krx and not eng:
            continue
        out[str(row["input_name"])] = {
            "corp_code": (row.get("corp_code") or "").strip(),
            "krx_code": krx,
            "company_en_official": eng,
        }

    if not out:
        return {}, "DART 매칭 결과 없음(회사명 불일치)"

    try:
        summ = dart_lookup.dart_summary(match)
        note = (f"DART 자동해석 {len(out)}/{len(names)}개 "
                f"(정확 {summ['exact']} · 부분 {summ['partial']}"
                + (f" · 동명후보 {summ['ambiguous']}곳" if summ.get("ambiguous") else "")
                + ")")
    except Exception:                              # noqa: BLE001
        note = f"DART 자동해석 {len(out)}/{len(names)}개 회사"
    return out, note
