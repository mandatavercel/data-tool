"""
LLM 기반 스마트 스키마 인텔리전스 — Claude Haiku로 자동 역할 추론.

패턴 매칭(foundation.py:infer_schema)을 보완하는 second-tier 추론 엔진.

활용 흐름:
    1. foundation.infer_schema() — 빠른 패턴 매칭으로 명확한 컬럼 매핑
    2. confidence < threshold(40)인 컬럼만 LLM에 위임
    3. LLM이 role + reasoning + analysis_hint 반환
    4. UI에 결합 표시 — 사용자는 LLM 근거를 보고 검토

캐시: st.cache_data로 (col_name, dtype, sample) 기준 결과 보존.
비용: column당 ~$0.0003 (Haiku 4.5 기준), 평균 20개 컬럼 = $0.006.

API key 출처 우선순위:
    1. st.secrets["ANTHROPIC_API_KEY"]    — Streamlit Cloud
    2. env ANTHROPIC_API_KEY              — local
    3. None → LLM 호출 skip (pattern only)
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

import streamlit as st


# 모든 사용자 노출 role + 짧은 설명 (LLM 프롬프트에 주입)
_ROLE_CATALOG = {
    "transaction_date":  "거래 발생 날짜/시간 (YYYY-MM-DD, YYYYMMDD, YYYYMM 월 단위 포함)",
    "company_name":      "회사/사업자 이름 (텍스트)",
    "brand_name":        "브랜드명",
    "sku_name":          "SKU·상품·제품명",
    "category_large":    "카테고리 대분류 (음식/생활/뷰티 같은 상위 분류)",
    "category_medium":   "카테고리 중분류",
    "category_small":    "카테고리 소분류 (leaf)",
    "category_name":     "단일 카테고리 (계층 없음)",
    "sales_amount":      "거래금액·매출 (원/달러 등 통화 단위)",
    "sales_quantity":    "거래수량·판매량 (개수)",
    "sales_count":       "거래/주문/결제 건수 — 한 명이 여러 번 거래 가능 (>= active_users)",
    "active_users":      "활성 이용자수·DAU·MAU·방문자수·UU·구독자수 — 한 명이 N번 거래해도 1명 (< sales_count)",
    "unit_price":        "단가·객단가",
    "gender":            "성별 (M/F, 남/여)",
    "age_group":         "연령대 (20대, 30s 등)",
    "region":            "지역명 (서울/경기 등 광역시도)",
    "channel":           "판매채널 (온라인/오프라인/편의점)",
    "store_id":          "점포·매장 ID",
    "customer_id":       "개별 고객·회원 ID (1행=1명, 많은 unique 값)",
    "retention_flag":    "신규/재방문 플래그",
    "stock_code":        "KRX 6자리 종목코드 (예: 005930)",
    "security_code":     "ISIN 12자리 (예: KR7005930003)",
    "unknown":           "분석 사용 안 함 (사업자번호·내부ID·메모 등)",
}

_SYSTEM_PROMPT = (
    "You are a data schema expert for retail/POS/transaction/alt-data analytics. "
    "Given column metadata (name, dtype, sample values), classify it into ONE role. "
    "Always reply with a single valid JSON object only — no markdown, no commentary.\n\n"
    "JSON schema:\n"
    "{\n"
    '  "role": <one of the role keys below>,\n'
    '  "confidence": <float 0.0-1.0>,\n'
    '  "reasoning": <short Korean explanation, max 80 chars>,\n'
    '  "analysis_hint": <Korean — what analysis this column enables, max 80 chars>\n'
    "}\n\n"
    "Available roles:\n" + "\n".join(f"- {k}: {v}" for k, v in _ROLE_CATALOG.items()) + "\n\n"
    "Decision rules:\n"
    "- 'DAU/MAU/이용자수/유저수/방문자수/사용자수/unique_users/n_users/number_of_users' "
    "→ active_users (개별 유저 카운트, 한 명이 여러 번 거래해도 1명).\n"
    "- '거래건수/결제건수/주문건수/n_orders/tx_count/transaction_count' "
    "→ sales_count (결제 횟수, 한 명이 여러 번 가능).\n"
    "- 'sales_count' vs 'active_users' 헷갈리면 active_users 우선 "
    "(보통 sales_count >= active_users 관계).\n"
    "- 회사명은 '회사명'이라 적혀있지 않아도 텍스트값이 회사명 패턴이면 company_name.\n"
    "- YYYYMM(6자리), YYYYMMDD(8자리), YYYY-MM-DD 모두 transaction_date.\n"
    "- 10자리 정수는 사업자번호로 unknown.\n"
    "- 5-6자리 정수는 stock_code 가능성.\n"
    "- 신뢰도가 낮으면 confidence를 낮게 (0.3-0.5)."
)

_MODEL   = "claude-haiku-4-5"
_MAX_TOK = 256


def _get_api_key() -> str | None:
    """Streamlit Cloud secrets → env 순으로 ANTHROPIC API key 조회."""
    try:
        if "ANTHROPIC_API_KEY" in st.secrets:
            v = str(st.secrets["ANTHROPIC_API_KEY"]).strip()
            if v:
                return v
    except Exception:
        pass
    v = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    return v or None


def _parse_json_safely(text: str) -> dict | None:
    """LLM 응답에서 JSON 추출 — 마크다운 fence 처리."""
    if not text:
        return None
    # ```json ... ``` 또는 ``` ... ``` 제거
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    candidate = m.group(1) if m else text
    candidate = candidate.strip()
    # 처음 { ... 마지막 } 추출
    s = candidate.find("{")
    e = candidate.rfind("}")
    if s == -1 or e == -1:
        return None
    try:
        return json.loads(candidate[s : e + 1])
    except Exception:
        return None


@st.cache_data(ttl=24 * 60 * 60, show_spinner=False)
def _classify_one(col_name: str, dtype: str, sample_str: str,
                  n_unique: int, null_pct: float) -> dict | None:
    """단일 컬럼을 LLM에 위임 — 캐시 활성 (24시간).

    cache key: (col_name, dtype, sample_str, n_unique, null_pct).
    동일 메타는 한 번만 호출되고 이후 cache hit.

    실패 시 None 반환 (network/quota/import error 모두 None).
    """
    api_key = _get_api_key()
    if not api_key:
        return None

    try:
        import anthropic
    except ImportError:
        return None

    user_msg = (
        f"Column metadata:\n"
        f"  name: {col_name!r}\n"
        f"  dtype: {dtype}\n"
        f"  unique_count: {n_unique}\n"
        f"  null_pct: {null_pct}%\n"
        f"  sample_values: {sample_str}\n\n"
        f"Classify this column."
    )

    try:
        client = anthropic.Anthropic(api_key=api_key, timeout=15.0)
        resp = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOK,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = "".join(
            block.text for block in resp.content if hasattr(block, "text")
        )
    except Exception:
        return None

    parsed = _parse_json_safely(text)
    if not isinstance(parsed, dict):
        return None
    role = parsed.get("role")
    if role not in _ROLE_CATALOG:
        return None
    return {
        "role":          role,
        "confidence":    float(parsed.get("confidence", 0.5) or 0.5),
        "reasoning":     str(parsed.get("reasoning", "")).strip()[:160],
        "analysis_hint": str(parsed.get("analysis_hint", "")).strip()[:160],
    }


def is_available() -> bool:
    """LLM 추론 사용 가능 여부 (API key 있고 anthropic 패키지 설치)."""
    if not _get_api_key():
        return False
    try:
        import anthropic  # noqa: F401
        return True
    except ImportError:
        return False


def enhance_schema(schema: list[dict], confidence_threshold: int = 40,
                   max_llm_calls: int = 8, overall_timeout_s: float = 25.0) -> list[dict]:
    """패턴 추론 결과 list[dict] 에 LLM 결과를 병합.

    동작:
      - confidence < threshold 인 row만 LLM에 위임
      - LLM이 더 신뢰할 만한 추론(0.6+) 반환하면 inferred_role / final_role 덮어쓰기
      - llm_reasoning / llm_analysis_hint 키 추가

    안전장치:
      - max_llm_calls (기본 8): 가장 ambiguous한 컬럼 위주로 제한.
        나머지는 패턴 결과 유지 — Cloud 무료 티어 1GB / timeout 대응.
      - overall_timeout_s (기본 25s): 전체 LLM 보강에 사용할 최대 시간.
        초과 시 남은 컬럼은 enhancement 없이 패턴 결과만.

    LLM 사용 불가 환경에선 schema를 그대로 반환.
    """
    if not is_available():
        return schema

    import time

    # 1) LLM 위임할 후보를 confidence 오름차순(가장 모호한 것부터)으로 정렬
    candidates = [
        (i, row) for i, row in enumerate(schema)
        if int(row.get("confidence", 0) or 0) < confidence_threshold
    ]
    candidates.sort(key=lambda x: int(x[1].get("confidence", 0) or 0))
    # 상위 max_llm_calls 만 LLM 호출
    targets = {i for i, _ in candidates[:max_llm_calls]}

    t_start = time.time()
    out = []
    for idx, row in enumerate(schema):
        new_row = dict(row)
        # 타임아웃 또는 호출 대상 아니면 패턴 결과 그대로
        if idx in targets and (time.time() - t_start) < overall_timeout_s:
            try:
                llm = _classify_one(
                    col_name  = str(row.get("column_name", "")),
                    dtype     = str(row.get("dtype", "")),
                    sample_str= str(row.get("sample", ""))[:200],  # 메모리 cap
                    n_unique  = int(row.get("n_unique", 0) or 0),
                    null_pct  = float(row.get("null_pct", 0) or 0),
                )
            except Exception:
                llm = None
            if llm and llm.get("confidence", 0) >= 0.6:
                new_row["inferred_role"]    = llm["role"]
                new_row["final_role"]       = llm["role"]
                new_row["confidence"]       = int(llm["confidence"] * 100)
                new_row["reason"]           = f"🤖 AI: {llm['reasoning']}"
                new_row["llm_reasoning"]    = llm["reasoning"]
                new_row["llm_analysis_hint"]= llm["analysis_hint"]
            elif llm:
                new_row["llm_reasoning"]    = llm.get("reasoning", "")
                new_row["llm_analysis_hint"]= llm.get("analysis_hint", "")
        out.append(new_row)
    return out
