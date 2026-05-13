"""
KIPRIS Plus API 어댑터 — 상표 검색.

발급 사이트: https://plus.kipris.or.kr
사용 endpoint (상표 검색):
  GET http://plus.kipris.or.kr/openapi/rest/TrademarkSearchService/trademarkSearchInfo
      ?word={검색어}&accessKey={ServiceKey}

응답: XML — items 아래 item 들. 각 item 에 한글 상표명 / 영문 상표명 / 출원번호.

⚠️ KIPRIS Plus 의 endpoint·필드명은 신청 서비스에 따라 다를 수 있다.
   응답이 비어있거나 구조가 다르면 다음과 같은 fallback 도 시도:
   - /openapi/rest/KpatTmInfoSearchService/getKpatTmInfoSearch
   - /openapi/rest/PatentDesignTrademarkService/trademarkSearchInfo
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Iterable

import requests


# ── KIPRIS Plus endpoint 후보 (사용자가 신청한 서비스에 따라 다름) ─────────────
KIPRIS_ENDPOINTS: list[str] = [
    # 통합검색·자유 검색 (KIPI 패스, 가장 흔한 형태)
    "http://plus.kipris.or.kr/kipo-api/kipi/trademarkInfoSearchService/freeSearchInfo",
    "http://plus.kipris.or.kr/kipo-api/kipi/trademarkInfoSearchService/wordSearchInfo",
    "http://plus.kipris.or.kr/kipo-api/kipi/trademarkInfoSearchService/getAdvancedSearch",
    # 옛 endpoint
    "http://plus.kipris.or.kr/openapi/rest/TrademarkSearchService/trademarkSearchInfo",
    "http://plus.kipris.or.kr/openapi/rest/PatentDesignTrademarkService/trademarkSearchInfo",
]


# 응답에서 영문/한글 상표명을 담을 수 있는 필드 후보들 (KIPRIS 응답 버전별 다양함)
_FIELD_KR  = ("productName", "productKor", "trademarkName", "applicantName")
_FIELD_ENG = (
    "productNameEng", "productEng", "trademarkNameEng",
    "titleEng", "applicantNameEng", "title",   # title 은 영문일 때 사용
)
_FIELD_APP = ("applicationNumber", "applicationNo", "appNo")


def _looks_english(s: str) -> bool:
    """문자열이 영문(ASCII 알파벳 위주)인지."""
    if not s:
        return False
    letters = [c for c in s if c.isalpha()]
    if not letters:
        return False
    ascii_letters = [c for c in letters if c.isascii()]
    return len(ascii_letters) / len(letters) >= 0.7


def _norm(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", "", s).lower()


def _extract_text(el: ET.Element, candidates: Iterable[str]) -> str:
    for c in candidates:
        v = el.findtext(c)
        if v and v.strip():
            return v.strip()
    return ""


def _match_quality(name_kr: str, found_kr: str) -> float:
    """한글명 일치도 — 정규화 후 동일 1.0, 부분 일치 0.6, 그 외 0.4."""
    a, b = _norm(name_kr), _norm(found_kr)
    if not b:
        return 0.4
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.6
    return 0.4


def lookup_brand_en(name_kr: str, auth_key: str) -> list[dict]:
    """
    KIPRIS Plus 에서 한글 상표명 → 영문 표기 후보 검색.

    Returns:
        [{candidate_en, raw_payload, match_quality}, ...]
    """
    if not name_kr or not auth_key:
        return []

    # 여러 endpoint 변형을 순서대로 시도 (KIPRIS 서비스 명에 따라)
    last_err: Exception | None = None
    trace: list[str] = []
    for url in KIPRIS_ENDPOINTS:
        try:
            resp = requests.get(
                url,
                params={
                    "word":          name_kr,
                    "searchString":  name_kr,
                    "productName":   name_kr,
                    "trademarkName": name_kr,
                    "freeSearch":    name_kr,
                    "accessKey":     auth_key,
                    "ServiceKey":    auth_key,
                    "numOfRows":     20,
                },
                timeout=20,
            )
            trace.append(f"{url} → HTTP {resp.status_code}")
            resp.raise_for_status()
        except Exception as e:
            last_err = e
            trace.append(f"{url} → {type(e).__name__}: {e}")
            continue

        # XML 파싱
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError:
            trace.append(f"{url} → XML parse failed (len={len(resp.content)})")
            continue

        items = (
            root.findall(".//item")
            or root.findall(".//items/item")
            or root.findall(".//Item")
        )
        results: list[dict] = []
        for it in items:
            # title 이 영문이면 후보, 아니면 별도 영문 필드
            eng = _extract_text(it, _FIELD_ENG)
            if not eng:
                continue
            if not _looks_english(eng):
                # 한글 title 등 — skip
                continue
            kr  = _extract_text(it, _FIELD_KR) or ""
            app = _extract_text(it, _FIELD_APP)
            mq  = _match_quality(name_kr, kr) if kr else 0.6
            results.append({
                "candidate_en":  eng,
                "raw_payload":   {"productName": kr, "applicationNumber": app, "endpoint": url},
                "match_quality": mq,
            })
        if results:
            best: dict[str, dict] = {}
            for r in results:
                key = _norm(r["candidate_en"])
                if key not in best or r["match_quality"] > best[key]["match_quality"]:
                    best[key] = r
            return list(best.values())
        else:
            trace.append(f"{url} → items={len(items)} but no English title found")

    # 모든 endpoint 가 결과 없음 — 디버그 trace 를 에러로 raise
    if last_err:
        raise RuntimeError(
            f"KIPRIS Plus 호출 실패: {type(last_err).__name__}: {last_err}\n"
            f"trace:\n  " + "\n  ".join(trace[-5:])
        )
    if trace:
        raise RuntimeError(
            "KIPRIS Plus: 모든 endpoint 에서 영문 상표 결과 0건.\n"
            "trace:\n  " + "\n  ".join(trace[-5:])
        )
    return []
