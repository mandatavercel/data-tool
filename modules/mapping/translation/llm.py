"""
LLM 어댑터 — Anthropic Claude 로 한글 브랜드·제품명을 영문으로 변환.

설치 필요: `pip install anthropic`

사용 모델: claude-haiku-4-5 (저렴·빠름)
응답: 시스템 프롬프트로 strict JSON 강제. `json.loads` 로 파싱.
"""
from __future__ import annotations

import json
import re
from typing import Any


# 시스템 프롬프트 — 브랜드 영문 표기 요청
_BRAND_SYSTEM = (
    "You are an expert in Korean company/brand official English transliterations. "
    "Given a Korean brand name, respond with the most commonly used official English form "
    "(e.g. 농심→Nongshim, 오뚜기→Ottogi, 삼양→Samyang, 풀무원→Pulmuone, 신라면→Shin Ramyun). "
    "Reply with a single valid JSON object only. No surrounding markdown, no commentary. "
    "Schema: {\"english_name\": str, \"confidence\": float (0.0-1.0), \"reasoning\": str}."
)

# 시스템 프롬프트 — 제품 속성 분해 + 영문 조립
_PRODUCT_SYSTEM = (
    "You are an expert in Korean food/consumer product naming. "
    "Parse the Korean product name into structured attributes and produce an official-style English name. "
    "Reply with a single valid JSON object only. No markdown, no commentary. "
    "Schema: {"
    "\"brand\": str|null, "
    "\"base_product\": str|null, "
    "\"flavor\": str|null, "
    "\"format\": str|null, "
    "\"package_size\": str|null, "
    "\"variant\": str|null, "
    "\"name_en_assembled\": str, "
    "\"confidence\": float"
    "}. "
    "Examples: '농심 신라면 큰사발면 86g' → {\"brand\":\"농심\",\"base_product\":\"신라면\","
    "\"format\":\"큰사발\",\"package_size\":\"86g\",\"name_en_assembled\":\"Nongshim Shin Ramyun Big Bowl 86g\",\"confidence\":0.95}. "
    "Use widely-used English forms (Nongshim, Ottogi, Bibigo, Shin Ramyun, Wang Gyoza, etc.)."
)

_MODEL  = "claude-haiku-4-5"
_MAX_TOK = 512


def _parse_json_safely(text: str) -> dict | None:
    """LLM 응답에서 JSON 추출. 마크다운 fence 가 섞여 있어도 처리."""
    if not text:
        return None
    text = text.strip()
    # 마크다운 코드 펜스 제거
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def _call_claude(system: str, user_msg: str, api_key: str) -> str | None:
    """Anthropic API 호출. SDK 없으면 None 반환."""
    try:
        from anthropic import Anthropic
    except ImportError:
        # SDK 미설치 — 매핑 앱에서 안내 메시지
        raise RuntimeError(
            "anthropic 패키지가 설치되어 있지 않습니다. "
            "`pip install anthropic --break-system-packages` 실행 후 다시 시도하세요."
        )

    client = Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=_MODEL,
        max_tokens=_MAX_TOK,
        system=system,
        messages=[{"role": "user", "content": user_msg}],
    )
    # content 는 list of TextBlock — 첫 번째 텍스트 추출
    try:
        return resp.content[0].text
    except (IndexError, AttributeError):
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def llm_translate_brand(name_kr: str, api_key: str | None = None) -> dict | None:
    """한글 브랜드명 → 영문 표기 (Claude haiku-4.5)."""
    if not name_kr or not api_key:
        return None

    text = _call_claude(_BRAND_SYSTEM, name_kr, api_key)
    data = _parse_json_safely(text or "")
    if not data or "english_name" not in data:
        return None
    return {
        "candidate_en": str(data["english_name"]).strip(),
        "confidence":   float(data.get("confidence", 0.7)),
        "raw_response": {
            "reasoning": data.get("reasoning", ""),
            "raw_text":  text,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# 배치 호출 — 한 번에 여러 한글명을 JSON 배열로 처리 (30~50배 단축)
# ══════════════════════════════════════════════════════════════════════════════

_BRAND_BATCH_SYSTEM = (
    "You receive a JSON array of Korean brand names. "
    "Return ONLY a JSON array with one object per input, in the SAME ORDER. "
    "No markdown, no commentary, just valid JSON. "
    "Schema: [{\"input\": str, \"english_name\": str, \"confidence\": float}]. "
    "Use widely-used official English forms (농심→Nongshim, 오뚜기→Ottogi). "
    "For loanwords like 버드와이저, return the original English (Budweiser). "
    "For unfamiliar brands give your best transliteration with lower confidence."
)

_PRODUCT_BATCH_SYSTEM = (
    "You receive a JSON array of Korean product names. "
    "Return ONLY a JSON array with one object per input, in the SAME ORDER. "
    "No markdown, no commentary, just valid JSON. "
    "Schema: [{\"input\": str, \"brand\": str|null, \"base_product\": str|null, "
    "\"flavor\": str|null, \"format\": str|null, \"package_size\": str|null, "
    "\"variant\": str|null, \"name_en_assembled\": str, \"confidence\": float}]. "
    "CRITICAL: `name_en_assembled` MUST be FULLY ENGLISH — no Hangul characters anywhere. "
    "Translate EVERY Korean word (brand, base product, flavor, format) to its widely-used "
    "official English form. If a part has no common English form, use best transliteration. "
    "Examples: "
    "'농심 신라면 큰사발면 86g' → 'Nongshim Shin Ramyun Big Bowl 86g'; "
    "'오뚜기 진라면 매운맛 5입' → 'Ottogi Jin Ramen Spicy 5-pack'; "
    "'비비고 왕만두 1kg' → 'Bibigo Wang Mandu 1kg'; "
    "'롯데 빼빼로 오리지널' → 'Lotte Pepero Original'. "
    "Use established brand English names (Nongshim, Ottogi, Bibigo, Lotte, Samyang, Orion, "
    "CJ, Paris Baguette, Pulmuone, Dongwon)."
)

_GICS_BATCH_SYSTEM = (
    "You receive a JSON array of Korean companies and classify each into the "
    "Global Industry Classification Standard (GICS). "
    "Return ONLY a JSON array (same order), one object per input. "
    "No markdown, no commentary, valid JSON only. "
    "Each object schema: {"
    "\"input_index\": int, "
    "\"gics_industry_code\": int (6-digit GICS industry e.g. 302020), "
    "\"gics_industry\": str, "
    "\"gics_sub_industry_code\": int (8-digit e.g. 30202030), "
    "\"gics_sub_industry\": str, "
    "\"confidence\": float}. "
    "Use ONLY official GICS codes/names. Reference common Korean industry mappings: "
    "Food Products(302020)→Packaged Foods & Meats(30202030); "
    "Beverages(302010)→Brewers(30201010)/Distillers & Vintners(30201020)/Soft Drinks(30201030); "
    "Tobacco(302030)→Tobacco(30203010); "
    "Personal Products(303020)→Personal Products(30302010); "
    "Household Products(303010)→Household Products(30301010); "
    "Chemicals(151010)→Commodity(15101010)/Diversified(15101020)/Fertilizers(15101030)/Industrial Gases(15101040)/Specialty(15101050); "
    "Pharmaceuticals(352010)→Pharmaceuticals(35201010); "
    "Biotechnology(352020)→Biotechnology(35202010); "
    "Semiconductors(453010)→Semiconductor Equipment(45301010)/Semiconductors(45301020); "
    "Automobiles(251020)→Automobile Manufacturers(25102010); "
    "Food & Staples Retailing(301010)→Drug Retail(30101010)/Food Distributors(30101020)/Food Retail(30101030)/Hypermarkets(30101040); "
    "Banks(401010)→Diversified Banks(40101010)/Regional Banks(40101015); "
    "Insurance(403010)→Life & Health Insurance(40301010)/Property & Casualty Insurance(40301020); "
    "IT Services(451020), Software(451030), Tech Hardware(452020), Communications Equipment(452010), "
    "Electronic Equipment(452030); "
    "Diversified Financial Services(402010)→Other Diversified Financial Services(40201010)/Multi-Sector Holdings(40201040). "
    "If a company is a holding/conglomerate, choose Multi-Sector Holdings(40201040). "
    "If truly unknown set codes to 0 and names to empty string with confidence 0."
)

_CATEGORY_BATCH_SYSTEM = (
    "You receive a JSON array of Korean product category / industry / sector labels. "
    "Translate each into a natural, concise English category label suitable for analytics "
    "and reporting. Prefer widely-used industry English terms (e.g. 라면→Instant Noodles, "
    "음료→Beverages, 주류→Alcoholic Drinks, 가공식품→Processed Food, 화장품→Cosmetics, "
    "반도체→Semiconductors). "
    "Return ONLY a JSON array with one object per input, in the SAME ORDER. "
    "No markdown, no commentary, just valid JSON. "
    "Schema: [{\"input\": str, \"english_name\": str, \"confidence\": float}]. "
    "Keep multi-word labels concise (2-4 words). Use title case."
)


def _call_claude_batch(system: str, items: list[str], api_key: str,
                       max_tokens: int = 4096) -> str | None:
    """배치 호출 — items 를 JSON array 로 보내고 응답 받음."""
    try:
        from anthropic import Anthropic
    except ImportError:
        raise RuntimeError(
            "anthropic 패키지가 설치되어 있지 않습니다. "
            "`python3 -m pip install anthropic --break-system-packages` 실행 후 재시작."
        )
    client = Anthropic(api_key=api_key)
    user_msg = json.dumps(items, ensure_ascii=False)
    resp = client.messages.create(
        model=_MODEL,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user_msg}],
    )
    try:
        return resp.content[0].text
    except (IndexError, AttributeError):
        return None


def _parse_json_array(text: str) -> list | None:
    """LLM 응답에서 JSON 배열 추출."""
    if not text:
        return None
    m = re.search(r"\[[\s\S]*\]", text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


def _run_chunks_parallel(
    system: str,
    items: list[str],
    api_key: str,
    chunk_size: int,
    max_tokens: int,
    max_workers: int,
) -> list[tuple[list[str], str | None]]:
    """청크 분할 후 ThreadPool 로 동시 호출. (chunk, response_text) 리스트 반환.

    Anthropic 동시성 한도(보통 50 RPM/Tier1) 안에서 4 워커 정도가 안전.
    """
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    if not chunks:
        return []
    if len(chunks) == 1 or max_workers <= 1:
        # 단일 청크면 병렬화 오버헤드 없이 직접 호출
        return [(chunks[0], _call_claude_batch(system, chunks[0], api_key, max_tokens))]

    from concurrent.futures import ThreadPoolExecutor

    def _one(c):
        try:
            return c, _call_claude_batch(system, c, api_key, max_tokens)
        except Exception:
            return c, None

    workers = min(max_workers, len(chunks))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(_one, chunks))


def llm_translate_brands_batch(
    names_kr: list[str],
    api_key: str,
    chunk_size: int = 50,
    max_workers: int = 4,
) -> dict[str, dict]:
    """
    한글 브랜드 리스트 → 영문 매핑 dict.

    Args:
        names_kr: 한글 브랜드명 리스트
        api_key: Anthropic API 키
        chunk_size: 한 번의 API 호출에 보낼 브랜드 수 (토큰 한도 안전)
        max_workers: 청크 동시 호출 워커 수 (Anthropic 동시성 한도 고려)

    Returns:
        {name_kr: {candidate_en, confidence, raw_response}, ...}
        호출 실패한 항목은 dict에 없음.
    """
    if not names_kr or not api_key:
        return {}

    out: dict[str, dict] = {}
    chunk_results = _run_chunks_parallel(
        _BRAND_BATCH_SYSTEM, names_kr, api_key,
        chunk_size=chunk_size, max_tokens=4096, max_workers=max_workers,
    )
    for _chunk, text in chunk_results:
        arr = _parse_json_array(text or "")
        if not arr:
            continue
        for item in arr:
            inp = (item or {}).get("input")
            eng = (item or {}).get("english_name")
            if not inp or not eng:
                continue
            out[inp] = {
                "candidate_en": str(eng).strip(),
                "confidence":   float(item.get("confidence", 0.8)),
                "raw_response": {"raw_text": (text or "")[:500]},
            }
    return out


def llm_translate_products_batch(
    names_kr: list[str],
    api_key: str,
    chunk_size: int = 50,
    max_workers: int = 4,
) -> dict[str, dict]:
    """제품 리스트 배치 호출 — 속성 분해 + 영문 조립을 JSON array 로 응답.

    Args:
        chunk_size: 50 권장 (max_tokens 8192 안에서 충분)
        max_workers: 청크 동시 호출 워커 수 (기본 4)
    """
    if not names_kr or not api_key:
        return {}

    out: dict[str, dict] = {}
    chunk_results = _run_chunks_parallel(
        _PRODUCT_BATCH_SYSTEM, names_kr, api_key,
        chunk_size=chunk_size, max_tokens=8192, max_workers=max_workers,
    )
    for _chunk, text in chunk_results:
        arr = _parse_json_array(text or "")
        if not arr:
            continue
        for item in arr:
            if not isinstance(item, dict):
                continue
            inp = item.get("input")
            ass = item.get("name_en_assembled")
            if not inp or not ass:
                continue
            out[inp] = {
                "brand":             item.get("brand"),
                "base_product":      item.get("base_product"),
                "flavor":            item.get("flavor"),
                "format":            item.get("format"),
                "package_size":      item.get("package_size"),
                "variant":           item.get("variant"),
                "name_en_assembled": str(ass).strip(),
                "confidence":        float(item.get("confidence", 0.75)),
                "raw_response":      {"raw_text": (text or "")[:500]},
            }
    return out


_PARENT_BATCH_SYSTEM = (
    "You receive a JSON array of Korean companies that are NOT publicly listed on KOSPI/KOSDAQ. "
    "For each, identify the most likely PARENT (holding) company that IS listed on the Korean "
    "stock market (KOSPI / KOSDAQ) or otherwise globally. "
    "Return ONLY a JSON array (same order), one object per input. "
    "No markdown, no commentary, valid JSON only. "
    "Each object schema: {"
    "\"input_index\": int, "
    "\"parent_kr\": str (Korean parent name, empty if unknown), "
    "\"parent_en\": str (English parent name), "
    "\"parent_stock_code\": str (6-digit Korean ticker if listed in KR, else empty), "
    "\"parent_isin\": str (12-char ISIN, KR7xxxxxxxxx if Korean listed, else empty), "
    "\"status_kind\": str ('delisted' if was once listed and removed, "
    "'subsidiary' if simple subsidiary, 'international' if parent is foreign listed), "
    "\"confidence\": float (0.0-1.0)}. "
    "Examples: "
    "동원F&B → parent_kr='동원산업', parent_en='Dongwon Industries Co Ltd', "
    "parent_stock_code='006040', parent_isin='KR7006040006', status_kind='delisted'; "
    "롯데주류 → parent_kr='롯데칠성음료', parent_stock_code='005300', status_kind='subsidiary'; "
    "한국야쿠르트(hy) → parent_kr='야쿠르트혼샤', parent_en='Yakult Honsha', "
    "parent_isin='JP3931600005', status_kind='international'. "
    "If you cannot identify a clear parent, set parent_kr to empty string and confidence=0."
)


_LARGEST_SHAREHOLDER_BATCH_SYSTEM = (
    "You receive a JSON array of Korean companies. For each, identify the LARGEST "
    "SHAREHOLDER (typically a holding company, founder, or institutional investor "
    "like National Pension Service) and provide a brief English business description. "
    "Return ONLY a JSON array (same order), one object per input. "
    "No markdown, no commentary, valid JSON only. "
    "Each object schema: {"
    "\"input_index\": int, "
    "\"largest_shareholder_company_name_en\": str (UPPERCASE official English name; "
    "use 'NATIONAL PENSION SERVICE' for NPS, 'COUPANG INC' for Coupang), "
    "\"largest_shareholder_listing_status\": str (e.g. 'NYSE (New York Stock Exchange)', "
    "'KOSPI', 'KOSDAQ', 'TSE (Tokyo Stock Exchange)', 'Not listed', 'N/A' if institutional like NPS), "
    "\"largest_shareholder_security_code\": str (ISIN if listed e.g. 'US22266T1097', "
    "'KR7xxxxxxxxx' for Korean; 'N/A' if not listed), "
    "\"mandata_brand_name_definition\": str (English description of what the brand_name "
    "represents in the data; '_ALL' suffix means all transactions under that corporation. "
    "Example: 'All transactions aggregated under Coupang corporation including COUPANG EATS.'), "
    "\"confidence\": float}. "
    "If unknown, leave fields empty and set confidence=0."
)


def llm_largest_shareholder_batch(
    items: list[dict],
    api_key: str,
    chunk_size: int = 20,
    max_workers: int = 4,
) -> list[dict]:
    """회사 → 최대주주 정보 + 영문 정의 batch.

    items: [{name_kr, name_en, mandata_brand_name, induty_code}, ...]
    Returns: 같은 순서. 빈 dict 면 실패.
    """
    if not items or not api_key:
        return [{} for _ in items]
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    out: list[dict] = [{} for _ in items]

    def _one(chunk_items: list[dict], offset: int):
        payload = [
            {"input_index": offset + i,
             "name_kr":             it.get("name_kr", ""),
             "name_en":             it.get("name_en", ""),
             "mandata_brand_name":  it.get("mandata_brand_name", ""),
             "induty_code":         it.get("induty_code", "")}
            for i, it in enumerate(chunk_items)
        ]
        try:
            text = _call_claude_batch(
                _LARGEST_SHAREHOLDER_BATCH_SYSTEM, payload, api_key, max_tokens=8192,
            )
        except Exception:
            return
        arr = _parse_json_array(text or "")
        if not arr:
            return
        for r in arr:
            if not isinstance(r, dict):
                continue
            idx = r.get("input_index")
            if not isinstance(idx, int) or not (0 <= idx < len(out)):
                continue
            out[idx] = {
                "largest_shareholder_company_name_en": str(r.get("largest_shareholder_company_name_en", "") or ""),
                "largest_shareholder_listing_status":  str(r.get("largest_shareholder_listing_status", "") or ""),
                "largest_shareholder_security_code":   str(r.get("largest_shareholder_security_code", "") or ""),
                "mandata_brand_name_definition":       str(r.get("mandata_brand_name_definition", "") or ""),
                "confidence":                           float(r.get("confidence", 0.7) or 0.0),
            }

    if len(chunks) == 1 or max_workers <= 1:
        _one(chunks[0], 0)
    else:
        from concurrent.futures import ThreadPoolExecutor
        offsets = [i * chunk_size for i in range(len(chunks))]
        with ThreadPoolExecutor(max_workers=min(max_workers, len(chunks))) as ex:
            list(ex.map(lambda args: _one(*args), zip(chunks, offsets)))
    return out


def llm_find_parents_batch(
    items: list[dict],
    api_key: str,
    chunk_size: int = 20,
    max_workers: int = 4,
) -> list[dict]:
    """비상장 회사 → 모회사 추정 batch.

    items: [{name_kr, name_en, stock_code, induty_code}, ...]
    Returns: 같은 순서. 추정 실패 시 빈 dict.
    """
    if not items or not api_key:
        return [{} for _ in items]
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    out: list[dict] = [{} for _ in items]

    def _one(chunk_items: list[dict], offset: int):
        payload = [
            {"input_index": offset + i,
             "name_kr":     it.get("name_kr", ""),
             "name_en":     it.get("name_en", ""),
             "stock_code":  it.get("stock_code", ""),
             "induty_code": it.get("induty_code", "")}
            for i, it in enumerate(chunk_items)
        ]
        try:
            text = _call_claude_batch(
                _PARENT_BATCH_SYSTEM, payload, api_key, max_tokens=8192,
            )
        except Exception:
            return
        arr = _parse_json_array(text or "")
        if not arr:
            return
        for r in arr:
            if not isinstance(r, dict):
                continue
            idx = r.get("input_index")
            if not isinstance(idx, int) or not (0 <= idx < len(out)):
                continue
            out[idx] = {
                "parent_kr":         str(r.get("parent_kr", "") or ""),
                "parent_en":         str(r.get("parent_en", "") or ""),
                "parent_stock_code": str(r.get("parent_stock_code", "") or "").zfill(6) if r.get("parent_stock_code") else "",
                "parent_isin":       str(r.get("parent_isin", "") or ""),
                "status_kind":       str(r.get("status_kind", "subsidiary") or "subsidiary"),
                "confidence":        float(r.get("confidence", 0.7) or 0.0),
            }

    if len(chunks) == 1 or max_workers <= 1:
        _one(chunks[0], 0)
    else:
        from concurrent.futures import ThreadPoolExecutor
        offsets = [i * chunk_size for i in range(len(chunks))]
        with ThreadPoolExecutor(max_workers=min(max_workers, len(chunks))) as ex:
            list(ex.map(lambda args: _one(*args), zip(chunks, offsets)))
    return out


def llm_classify_gics_batch(
    items: list[dict],
    api_key: str,
    chunk_size: int = 40,
    max_workers: int = 4,
) -> list[dict]:
    """회사 정보 리스트 → GICS 분류 결과 리스트 (같은 순서).

    Args:
        items: [{ksic_code, name_kr, name_en, stock_code}, ...]
        api_key: Anthropic API 키

    Returns:
        [{gics_industry_code, gics_industry, gics_sub_industry_code,
          gics_sub_industry, confidence}, ...] — 빈 dict 면 LLM 실패/응답누락.
    """
    if not items or not api_key:
        return [{} for _ in items]

    # 청크 분할 + 병렬 호출 — 각 청크의 items 를 JSON 으로 보냄
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    out: list[dict] = [{} for _ in items]

    def _one_chunk(chunk_items: list[dict], offset: int):
        # 각 item 에 chunk-local index 부여
        payload = [
            {"input_index": offset + i,
             "ksic_code":   it.get("ksic_code", ""),
             "name_kr":     it.get("name_kr", ""),
             "name_en":     it.get("name_en", ""),
             "stock_code":  it.get("stock_code", "")}
            for i, it in enumerate(chunk_items)
        ]
        try:
            text = _call_claude_batch(
                _GICS_BATCH_SYSTEM, payload, api_key, max_tokens=8192,
            )
        except Exception:
            return
        arr = _parse_json_array(text or "")
        if not arr:
            return
        for r in arr:
            if not isinstance(r, dict):
                continue
            idx = r.get("input_index")
            if not isinstance(idx, int) or not (0 <= idx < len(out)):
                continue
            out[idx] = {
                "gics_industry_code":     int(r.get("gics_industry_code", 0) or 0),
                "gics_industry":          str(r.get("gics_industry", "") or ""),
                "gics_sub_industry_code": int(r.get("gics_sub_industry_code", 0) or 0),
                "gics_sub_industry":      str(r.get("gics_sub_industry", "") or ""),
                "confidence":             float(r.get("confidence", 0.7) or 0.0),
            }

    if len(chunks) == 1 or max_workers <= 1:
        _one_chunk(chunks[0], 0)
    else:
        from concurrent.futures import ThreadPoolExecutor
        offsets = [i * chunk_size for i in range(len(chunks))]
        with ThreadPoolExecutor(max_workers=min(max_workers, len(chunks))) as ex:
            list(ex.map(lambda args: _one_chunk(*args), zip(chunks, offsets)))
    return out


def llm_translate_categories_batch(
    names_kr: list[str],
    api_key: str,
    chunk_size: int = 80,
    max_workers: int = 4,
) -> dict[str, dict]:
    """
    한글 카테고리 리스트 → 영문 매핑 dict.

    카테고리는 짧고 정형적이므로 chunk_size 를 크게(80) 가져가도 안전.

    Returns: {name_kr: {candidate_en, confidence, raw_response}, ...}
    """
    if not names_kr or not api_key:
        return {}

    out: dict[str, dict] = {}
    chunk_results = _run_chunks_parallel(
        _CATEGORY_BATCH_SYSTEM, names_kr, api_key,
        chunk_size=chunk_size, max_tokens=4096, max_workers=max_workers,
    )
    for _chunk, text in chunk_results:
        arr = _parse_json_array(text or "")
        if not arr:
            continue
        for item in arr:
            inp = (item or {}).get("input")
            eng = (item or {}).get("english_name")
            if not inp or not eng:
                continue
            out[inp] = {
                "candidate_en": str(eng).strip(),
                "confidence":   float(item.get("confidence", 0.85)),
                "raw_response": {"raw_text": (text or "")[:500]},
            }
    return out


def llm_parse_and_translate_product(
    name_kr: str,
    api_key: str | None = None,
) -> dict | None:
    """한글 제품명 → 속성 분해 + 영문 조립 (Claude haiku-4.5, JSON mode)."""
    if not name_kr or not api_key:
        return None

    text = _call_claude(_PRODUCT_SYSTEM, name_kr, api_key)
    data = _parse_json_safely(text or "")
    if not data or "name_en_assembled" not in data:
        return None
    return {
        "brand":             data.get("brand"),
        "base_product":      data.get("base_product"),
        "flavor":            data.get("flavor"),
        "format":            data.get("format"),
        "package_size":      data.get("package_size"),
        "variant":           data.get("variant"),
        "name_en_assembled": str(data["name_en_assembled"]).strip(),
        "confidence":        float(data.get("confidence", 0.7)),
        "raw_response":      {"raw_text": text},
    }
