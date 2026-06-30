"""
kfnb_app/mapping/brand_llm.py — LLM 기반 브랜드/카테고리 영문명 추정 (graceful).

로마자 폴백(예: Bariseutarulseu)을 공식/통용 영문명(Barista Rules)으로 바꾼다.
anthropic 라이브러리 + API 키가 있으면 사용, 없으면 ({}, 사유) 반환(비차단).
사람이 항상 검수/수정할 수 있게 *제안*만 한다.
"""
from __future__ import annotations

import json
import re

DEFAULT_MODEL = "claude-sonnet-4-6"


def _looks_romanized(en: str) -> bool:
    """공백 없는 한 단어 + 로마자스러운 패턴이면 로마자 폴백으로 간주."""
    s = str(en or "").strip()
    if not s or " " in s:
        return False
    # Pepsi/Buldak 같은 짧은 실제 단어는 제외하기 어렵지만, 길고 자모 나열형이면 의심
    return bool(re.fullmatch(r"[A-Za-z]{6,}", s))


def infer_brand_en(items, api_key: str, model: str = DEFAULT_MODEL,
                   context: str = "Korea F&B") -> tuple[dict, str]:
    """items: [(company, brand)] 또는 [(company, brand, category)] → {(company,brand): en}.

    회사·카테고리 맥락을 주고 '공식 마케팅 표기'를 우선하도록 지시한다(단순 로마자 회피).
    키/라이브러리/네트워크 없으면 ({}, 사유).
    """
    norm = []
    for it in items:
        it = list(it) + ["", ""]
        c, b, cat = str(it[0]), str(it[1]), str(it[2])
        if b.strip():
            norm.append((c, b, cat))
    # (company, brand) 중복 제거
    seen, pairs = set(), []
    for c, b, cat in norm:
        if (c, b) not in seen:
            seen.add((c, b)); pairs.append((c, b, cat))
    if not pairs:
        return {}, "추정 대상 없음"
    if not api_key:
        return {}, "LLM 키 없음 — ANTHROPIC_API_KEY 입력 시 추정 가능"
    try:
        import anthropic
    except Exception:                              # noqa: BLE001
        return {}, "anthropic 미설치 — `pip install anthropic`"
    payload = [{"company": c, "brand_ko": b, "category": cat}
               for c, b, cat in pairs[:300]]
    prompt = (
        f"You map {context} brand names (Korean) to the brand's OFFICIAL English name as "
        "the company actually markets it — on product packaging, export labels, the global "
        "website, or trademark filings. This is NOT transliteration: prefer the real brand "
        "spelling even if it differs from a literal romanization.\n"
        "Examples: 불닭->Buldak, 비비고->Bibigo, 바나나맛우유->Banana Flavored Milk, "
        "아카페라->aCafela (Binggrae coffee), 햇반->Hetbahn, 메로나->Melona, "
        "처음처럼->Chum Churum, 카누->KANU, 빵빠레->Bbangparae.\n"
        "Use the company and category as context to disambiguate. If a brand truly has no "
        "established English form, output a clean Title-Case romanization. "
        "Return ONLY a JSON array of {\"company\":..., \"brand_ko\":..., \"brand_en\":...}. "
        "No commentary.\n\n" + json.dumps(payload, ensure_ascii=False))
    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=model, max_tokens=8000,
            messages=[{"role": "user", "content": prompt}])
        text = "".join(getattr(b, "text", "") for b in msg.content)
        m = re.search(r"\[.*\]", text, re.DOTALL)
        arr = json.loads(m.group(0) if m else text)
    except Exception as e:                         # noqa: BLE001
        return {}, f"LLM 호출 실패: {type(e).__name__}"
    out = {}
    for r in arr:
        co = str(r.get("company", "")).strip()
        br = str(r.get("brand_ko", "")).strip()
        en = str(r.get("brand_en", "")).strip()
        if br and en:
            out[(co, br)] = en
    return out, f"LLM 추정 {len(out)}/{len(pairs)}개"
