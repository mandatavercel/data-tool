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


def resolve(names, api_key: str, code_hints: dict | None = None) -> tuple[dict, str]:
    """회사 한글명 리스트 → {회사명: {corp_code, krx_code, company_en_official}}.

    ⭐ 정확도 핵심: code_hints({회사명: 종목코드})가 주어지면 **이름이 아니라 종목코드로
    공시 법인을 앵커링**해 공식 영문명을 가져온다(동명 법인 오매칭 방지 — 동원→동원F&B,
    CJ제일제당→CJ제일제당 정확). 코드 힌트 없는 회사만 이름매칭 폴백.

    사내 `modules.mapping.dart_lookup` 재사용:
      fetch_dart_corp_master(api_key)  → 전체 회사 마스터(corp_code·corp_name·
                                         corp_name_eng·stock_code; ZIP·캐시·재시도)
    반환 (mapping, note). 키/네트워크/의존성 없으면 ({}, 사유).
    """
    names = [str(n) for n in dict.fromkeys(names) if str(n).strip()]
    code_hints = {str(k): str(v).strip().zfill(6)
                  for k, v in (code_hints or {}).items() if str(v).strip()}
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
        return {}, f"DART 마스터 조회 실패: {e}"

    out: dict[str, dict] = {}
    by_code, by_name = 0, 0

    # ── 1) 종목코드 앵커 (정확) ──
    code_idx = {}
    if code_hints:
        m = master.copy()
        m["stock_code"] = m["stock_code"].astype(str).str.strip().str.zfill(6)
        for _, r in m[m["stock_code"] != "000000"].iterrows():
            code_idx.setdefault(r["stock_code"], r)
        for nm in names:
            code = code_hints.get(nm)
            if not code:
                continue
            row = code_idx.get(code)
            if row is not None:
                out[nm] = {"corp_code": str(row["corp_code"]).strip(),
                           "krx_code": code,
                           "company_en_official": str(row.get("corp_name_eng") or "").strip()}
                by_code += 1

    # ── 2) 코드 없는 회사만 이름매칭 폴백 ──
    rest = [n for n in names if n not in out]
    if rest:
        try:
            match = dart_lookup.match_dart_companies(rest, master)
            for _, row in match.iterrows():
                if row.get("status") == "none":
                    continue
                krx = (row.get("stock_code") or "").strip()
                eng = (row.get("corp_name_eng") or "").strip()
                if not krx and not eng:
                    continue
                out[str(row["input_name"])] = {
                    "corp_code": (row.get("corp_code") or "").strip(),
                    "krx_code": krx, "company_en_official": eng}
                by_name += 1
        except Exception:                          # noqa: BLE001
            pass

    if not out:
        return {}, "DART 매칭 결과 없음(회사명/코드 불일치)"

    # 법인등록번호(jurir_no) 공시 기준 보강 — corp_code 로 company.json 조회(캐시)
    jn = 0
    for nm, info in out.items():
        cc = info.get("corp_code")
        info["jurir_no"] = ""
        if cc:
            try:
                ci = dart_lookup.fetch_company_info(api_key, cc)
                info["jurir_no"] = str(ci.get("jurir_no", "") or "").strip()
                if info["jurir_no"]:
                    jn += 1
            except Exception:                      # noqa: BLE001
                pass

    note = (f"DART 자동해석 {len(out)}/{len(names)}개 "
            f"(코드앵커 {by_code} · 이름매칭 {by_name} · 법인등록번호 {jn})")
    return out, note
