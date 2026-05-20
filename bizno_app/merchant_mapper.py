"""
영문 머천트 코드 → 한글 업체명/카테고리 매핑 (Claude API)
=========================================================
- 입력: ['COUPANGEATS_MUGPOS__COMBINED', 'TOSSPAYMENTS__SINGLE', ...]
- 출력: {code: {'korean_name': '쿠팡이츠', 'category': '배달/음식', 'group': '쿠팡', 'confidence': 0.95}}
- Anthropic Claude haiku 사용 (저렴, 빠름)
- JSON 파일에 캐시 — 같은 코드 다시 부르면 API 호출 안 함
"""
from __future__ import annotations

import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

_MODEL = "claude-haiku-4-5"
_MAX_TOK = 4096
_BATCH_SIZE = 40   # 한 번에 40개씩 (너무 많으면 응답이 잘리거나 JSON 오류)
_PARALLEL = 8      # 동시 호출 배치 수 (Anthropic Tier 1: rpm 50, tpm 50k 정도 → 8 동시면 안전)

_CACHE_PATH = Path(__file__).resolve().parent / "data" / "merchant_cache.json"


SYSTEM_PROMPT = (
    "You receive a JSON array of English merchant/brand codes used in Korean payment/credit-card data. "
    "These codes typically look like 'COUPANGEATS_MUGPOS__COMBINED' or 'BAEMIN_WOOWAHAN__COMBINED' or 'DAISO__SINGLE'. "
    "The format is: BRAND[_SUBBRAND_...]__VARIANT, where VARIANT is SINGLE / COMBINED / SUMMED (you can ignore VARIANT). "
    "For each code, identify the Korean company/brand name and a category. "
    "\n\n"
    "Return ONLY a JSON array (same order, same length as input). "
    "No markdown, no commentary, no code fences. Just the raw JSON array. "
    "\n\n"
    "Schema: [{\"code\": str (echo input), \"korean_name\": str, \"category\": str, \"group\": str, \"confidence\": float (0~1)}]. "
    "\n\n"
    "Rules:\n"
    "- `korean_name`: official Korean name (e.g., '쿠팡이츠', '토스페이먼츠', 'KG이니시스', '삼성카드', '배달의민족', '신세계', '올리브영', '다이소'). "
    "  For COMBINED codes (multiple brands merged), use the parent/main brand name (e.g., COUPANGEATS_MUGPOS → '쿠팡이츠').\n"
    "- `category`: short Korean category like '이커머스', '결제/PG', '카드/금융', '배달/음식', '뷰티', '식음료', '여행/항공', "
    "  '엔터/미디어', '교육', '통신', '의류/패션', '가전', '생활/리빙', '헬스/의료', '자동차', '광고/마케팅', '부동산', '기타'. Pick the BEST single fit.\n"
    "- `group`: parent corporate group when applicable in Korean (e.g., '쿠팡', '카카오', '네이버', 'CJ', '롯데', '신세계', '현대', 'LG', "
    "  '삼성', 'SK', 'KT', '우아한형제들', '비바리퍼블리카', 'GS', '한화', '한진'). If standalone/unknown, repeat the korean_name.\n"
    "- `confidence`: 0.9+ if you're certain (famous brands), 0.6~0.8 if you can infer, <0.5 if guessing.\n"
    "- For obviously English loanword brands (NIKE, ADIDAS, GUCCI), use the Korean katakana-style transliteration (나이키, 아디다스, 구찌).\n"
    "- If completely unfamiliar, use best transliteration of the code as korean_name with confidence 0.3, category '기타', group=korean_name.\n"
)


def _strip_variant(code: str) -> str:
    """__SINGLE / __COMBINED / __SUMMED 접미사 제거 (참고용, 프롬프트에는 원본 그대로)."""
    return re.sub(r"__(SINGLE|COMBINED|SUMMED)$", "", code, flags=re.IGNORECASE)


def load_cache() -> dict[str, dict]:
    """로컬 캐시 로드."""
    if not _CACHE_PATH.exists():
        return {}
    try:
        return json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: dict[str, dict]) -> None:
    """로컬 캐시 저장."""
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _parse_json_array(text: str) -> list[dict] | None:
    """응답에서 JSON 배열 추출 (markdown fence 등 안전 처리)."""
    if not text:
        return None
    # 코드 펜스 제거
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
    # [...] 찾기 (greedy로 가장 바깥 배열)
    m = re.search(r"\[[\s\S]*\]", text)
    if not m:
        return None
    try:
        result = json.loads(m.group(0))
        return result if isinstance(result, list) else None
    except json.JSONDecodeError:
        return None


def _call_claude_batch(codes: list[str], api_key: str) -> list[dict] | None:
    """한 배치(최대 _BATCH_SIZE개) 호출."""
    try:
        from anthropic import Anthropic
    except ImportError:
        raise RuntimeError(
            "anthropic 패키지가 설치되어 있지 않습니다. "
            "터미널에서 `pip3 install anthropic --break-system-packages` 실행 후 다시 시도하세요."
        )

    client = Anthropic(api_key=api_key)
    user_msg = json.dumps(codes, ensure_ascii=False)

    resp = client.messages.create(
        model=_MODEL,
        max_tokens=_MAX_TOK,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    try:
        text = resp.content[0].text
    except (IndexError, AttributeError):
        return None
    return _parse_json_array(text)


def _process_batch(batch: list[str], api_key: str) -> dict[str, dict]:
    """한 배치를 처리하고 결과 dict를 반환. 실패 시 빈 결과로 채움."""
    out: dict[str, dict] = {}
    try:
        answer = _call_claude_batch(batch, api_key) or []
    except RuntimeError:
        raise  # SDK 미설치는 상위로
    except Exception as e:
        for c in batch:
            out[c] = {
                "korean_name": "", "category": "기타", "group": "",
                "confidence": 0.0, "cached": False, "error": str(e),
            }
        return out

    by_code = {}
    if isinstance(answer, list):
        for j, item in enumerate(answer):
            if not isinstance(item, dict):
                continue
            code_in_resp = item.get("code") or (batch[j] if j < len(batch) else None)
            if code_in_resp:
                by_code[code_in_resp] = item

    for c in batch:
        item = by_code.get(c, {})
        out[c] = {
            "korean_name": str(item.get("korean_name", "")).strip(),
            "category": str(item.get("category", "기타")).strip(),
            "group": str(item.get("group", "")).strip(),
            "confidence": float(item.get("confidence", 0.0) or 0.0),
            "cached": False,
        }
    return out


def translate_codes(
    codes: list[str],
    api_key: str,
    use_cache: bool = True,
    batch_size: int = _BATCH_SIZE,
    parallel: int = _PARALLEL,
    progress_cb: Callable[[int, int], None] | None = None,
) -> dict[str, dict]:
    """
    영문 머천트 코드 리스트를 Claude로 한글명+카테고리로 매핑.
    여러 배치를 ThreadPoolExecutor로 병렬 호출 → 500개도 ~15초.

    Returns:
        {code: {'korean_name': str, 'category': str, 'group': str, 'confidence': float, 'cached': bool}}
    """
    cache = load_cache() if use_cache else {}
    results: dict[str, dict] = {}
    pending: list[str] = []

    # 1) 캐시에 있는 건 바로 채움
    for c in codes:
        if c in cache:
            results[c] = {**cache[c], "cached": True}
        else:
            pending.append(c)

    total = len(pending)
    if total == 0:
        return results

    # 2) 배치로 쪼개기
    batches = [pending[i : i + batch_size] for i in range(0, total, batch_size)]

    # 3) 병렬 호출
    done_count = 0
    progress_lock = threading.Lock()

    def _tick(batch_len: int):
        nonlocal done_count
        with progress_lock:
            done_count += batch_len
            if progress_cb:
                progress_cb(done_count, total)

    with ThreadPoolExecutor(max_workers=min(parallel, len(batches))) as pool:
        future_to_batch = {pool.submit(_process_batch, b, api_key): b for b in batches}
        for fut in as_completed(future_to_batch):
            batch = future_to_batch[fut]
            try:
                batch_results = fut.result()
            except RuntimeError:
                raise
            except Exception as e:
                batch_results = {c: {
                    "korean_name": "", "category": "기타", "group": "",
                    "confidence": 0.0, "cached": False, "error": str(e),
                } for c in batch}

            results.update(batch_results)
            # 캐시에도 저장 (성공한 것만)
            if use_cache:
                for c, entry in batch_results.items():
                    if entry.get("korean_name"):
                        cache[c] = {k: v for k, v in entry.items() if k != "cached"}
            _tick(len(batch))

    # 4) 캐시 저장
    if use_cache:
        save_cache(cache)

    return results


def get_cache_stats() -> dict:
    """캐시 통계."""
    cache = load_cache()
    return {
        "count": len(cache),
        "path": str(_CACHE_PATH),
    }


def clear_cache() -> None:
    """캐시 비우기."""
    if _CACHE_PATH.exists():
        _CACHE_PATH.unlink()
