"""
modules/mapping/translation/pipeline.py

브랜드·제품 영문화 오케스트레이터.

브랜드 흐름:
  ① internal_db  (확정 영문명)    — confidence 1.0
  ② kipris       (상표 데이터)     — 0.9 × match_quality
  ③ llm          (Claude/OpenAI)   — 0.65 × llm_confidence
  ④ romanizer    (로마자)          — 0.4

제품 흐름:
  ① internal_db  (확정 영문명)    — 1.0
  ② 속성 분해 + brand 영문 사전 lookup + assemble — 0.7
  ③ llm          (분해 + 조립)     — 0.65 × confidence
  ④ romanizer    — 0.4

매핑 앱은 candidates 를 받아 사용자 검수 후 select_candidate() 호출 → name_en 확정.
"""
from __future__ import annotations

from . import romanizer, parser, kipris, llm
from . import has_korean
from .. import translation_db as db


# 마지막 실행의 LLM 에러 (UI 가 직후에 읽어 표시) — entity_type → error message
_LAST_LLM_ERROR: dict[str, str | None] = {"brand": None, "product": None, "category": None}


def get_last_llm_error(entity_type: str) -> str | None:
    """직전 collect_*_batch 호출의 LLM 에러 메시지. 없으면 None."""
    return _LAST_LLM_ERROR.get(entity_type)


def _set_last_llm_error(entity_type: str, err: str | None) -> None:
    _LAST_LLM_ERROR[entity_type] = err


# ── 출처별 base confidence ────────────────────────────────────────────────────
SOURCE_PRIORITY: dict[str, float] = {
    "internal_db":   1.0,
    "manual":        1.0,
    "kipris":        0.90,
    "gs1":           0.85,
    "official_site": 0.80,
    "llm":           0.65,
    "romanizer":     0.40,
}


# ══════════════════════════════════════════════════════════════════════════════
# Brand
# ══════════════════════════════════════════════════════════════════════════════

def collect_brand_candidates(
    name_kr: str,
    kipris_key: str | None = None,
    llm_key: str | None = None,
    persist: bool = True,
) -> list[dict]:
    """브랜드 영문명 후보 수집.

    Args:
        name_kr: 한글 브랜드명
        kipris_key: KIPRIS Plus API 인증키 (없으면 KIPRIS 스킵)
        llm_key: LLM API 키 (없으면 LLM 스킵)
        persist: True 면 후보를 SQLite 에 저장

    Returns:
        [{candidate_en, source, confidence, raw_payload}, ...] 내림차순
    """
    candidates: list[dict] = []

    # 1) internal DB — 확정된 영문명 있으면 그것이 최우선
    confirmed = db.get_confirmed_en("brand", name_kr)
    if confirmed:
        candidates.append({
            "candidate_en": confirmed,
            "source":       "internal_db",
            "confidence":   SOURCE_PRIORITY["internal_db"],
            "raw_payload":  None,
        })
        return candidates   # 확정 영문명이 있으면 더 시도하지 않음

    # 2) KIPRIS
    if kipris_key:
        try:
            for r in kipris.lookup_brand_en(name_kr, kipris_key):
                candidates.append({
                    "candidate_en": r["candidate_en"],
                    "source":       "kipris",
                    "confidence":   SOURCE_PRIORITY["kipris"] * r.get("match_quality", 1.0),
                    "raw_payload":  r.get("raw_payload"),
                })
        except Exception:
            pass

    # 3) LLM
    if llm_key:
        try:
            r = llm.llm_translate_brand(name_kr, llm_key)
            if r:
                candidates.append({
                    "candidate_en": r["candidate_en"],
                    "source":       "llm",
                    "confidence":   SOURCE_PRIORITY["llm"] * r.get("confidence", 0.7),
                    "raw_payload":  r.get("raw_response"),
                })
        except Exception:
            pass

    # 4) Romanizer — 최후 폴백
    rom = romanizer.romanize_brand(name_kr)
    if rom:
        candidates.append({
            "candidate_en": rom,
            "source":       "romanizer",
            "confidence":   SOURCE_PRIORITY["romanizer"],
            "raw_payload":  None,
        })

    # confidence 내림차순
    candidates.sort(key=lambda c: -c["confidence"])

    # 저장
    if persist and candidates:
        brand_id = db.upsert_brand(name_kr)
        for c in candidates:
            db.add_candidate(
                "brand", brand_id,
                c["candidate_en"], c["source"], c["confidence"], c["raw_payload"],
            )
    return candidates


# ══════════════════════════════════════════════════════════════════════════════
# Product
# ══════════════════════════════════════════════════════════════════════════════

def collect_product_candidates(
    name_kr: str,
    known_brands_kr: list[str] | None = None,
    llm_key: str | None = None,
    persist: bool = True,
) -> list[dict]:
    """제품 영문명 후보 수집.

    1) internal_db 확정 영문명
    2) 규칙 기반 속성 분해 + brand 영문 사전 lookup + assemble
    3) LLM 분해/조립
    4) romanizer

    Args:
        name_kr: 한글 제품명
        known_brands_kr: 알려진 브랜드 한글명 리스트 (prefix 매칭용)
        llm_key: LLM API 키

    Returns:
        후보 리스트 (내림차순). 각 후보에 `attributes` 키 포함 (분해 결과).
    """
    candidates: list[dict] = []

    # 1) internal_db 확정값
    confirmed = db.get_confirmed_en("product", name_kr)
    if confirmed:
        candidates.append({
            "candidate_en": confirmed,
            "source":       "internal_db",
            "confidence":   SOURCE_PRIORITY["internal_db"],
            "raw_payload":  None,
            "attributes":   None,
        })
        return candidates

    # 2) 규칙 기반 속성 분해 + 영문 조립
    attrs = parser.parse_product_name(name_kr, known_brands=known_brands_kr or [])

    # brand 영문 lookup (내부 DB 우선)
    brand_en = None
    if attrs.get("brand"):
        brand_en = db.get_confirmed_en("brand", attrs["brand"])
        if not brand_en:
            brand_en = romanizer.romanize_brand(attrs["brand"])

    assembled = parser.assemble_en(attrs, brand_en=brand_en)
    if assembled:
        # 규칙 기반은 internal_db 보다는 낮지만 romanizer 보다는 높음
        candidates.append({
            "candidate_en": assembled,
            "source":       "official_site",   # 'rule-based' 의미로 잠시 official_site 카테고리 사용 (Phase 1)
            "confidence":   0.55,              # 분해 정확도 보수적으로
            "raw_payload":  {"rule_attrs": attrs, "brand_en": brand_en},
            "attributes":   attrs,
        })

    # 3) LLM
    if llm_key:
        try:
            r = llm.llm_parse_and_translate_product(name_kr, llm_key)
            if r:
                candidates.append({
                    "candidate_en": r["name_en_assembled"],
                    "source":       "llm",
                    "confidence":   SOURCE_PRIORITY["llm"] * r.get("confidence", 0.7),
                    "raw_payload":  r.get("raw_response"),
                    "attributes":   {k: r.get(k) for k in
                                    ("brand","base_product","flavor","format","package_size","variant")},
                })
        except Exception:
            pass

    # 4) Romanizer fallback
    rom = romanizer.romanize_product(name_kr)
    if rom:
        candidates.append({
            "candidate_en": rom,
            "source":       "romanizer",
            "confidence":   SOURCE_PRIORITY["romanizer"],
            "raw_payload":  None,
            "attributes":   None,
        })

    candidates.sort(key=lambda c: -c["confidence"])

    if persist and candidates:
        product_id = db.upsert_product(name_kr, attributes=attrs)
        for c in candidates:
            db.add_candidate(
                "product", product_id,
                c["candidate_en"], c["source"], c["confidence"], c["raw_payload"],
            )
    return candidates


# ══════════════════════════════════════════════════════════════════════════════
# Lookup (확정값 조회) — 매핑 앱이 변환 시 사용
# ══════════════════════════════════════════════════════════════════════════════

def lookup_brand_en(name_kr: str) -> str | None:
    """확정된 브랜드 영문명만 반환. 없으면 None."""
    return db.get_confirmed_en("brand", name_kr)


def lookup_product_en(name_kr: str) -> str | None:
    return db.get_confirmed_en("product", name_kr)


def lookup_category_en(name_kr: str) -> str | None:
    return db.get_confirmed_en("category", name_kr)


# ══════════════════════════════════════════════════════════════════════════════
# 배치 처리 (성능 최적화)
# ══════════════════════════════════════════════════════════════════════════════

def collect_brands_batch(
    names_kr: list[str],
    kipris_key: str | None = None,
    llm_key: str | None = None,
    skip_confirmed: bool = True,
    progress_callback=None,
) -> dict[str, list[dict]]:
    """
    여러 브랜드를 효율적으로 한꺼번에 처리.

    1) confirmed skip 은 단일 IN 쿼리로 (N×connect 제거)
    2) LLM 청크 (50개) 를 ThreadPoolExecutor 로 4 워커 병렬
    3) KIPRIS 는 ThreadPoolExecutor 로 8 워커 병렬
    4) Romanizer 는 즉시 처리
    5) 결과를 단일 트랜잭션으로 SQLite 에 저장 (bulk_save_candidates)

    Returns: {name_kr: candidates_list}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from . import romanizer, kipris, llm

    results: dict[str, list[dict]] = {}

    # 1) skip confirmed — 단일 배치 쿼리
    to_process: list[str] = list(names_kr)
    if skip_confirmed and to_process:
        confirmed_map = db.get_confirmed_en_many("brand", to_process)
        for n, en in confirmed_map.items():
            results[n] = [{
                "candidate_en": en,
                "source": "internal_db",
                "confidence": SOURCE_PRIORITY["internal_db"],
                "raw_payload": None,
            }]
        to_process = [n for n in to_process if n not in confirmed_map]

    total = len(to_process)
    if total == 0:
        return results

    # 2) LLM 청크 병렬 호출 (4 워커 — 50개씩 N청크)
    llm_results: dict[str, dict] = {}
    _set_last_llm_error("brand", None)
    if llm_key:
        try:
            llm_results = llm.llm_translate_brands_batch(
                to_process, llm_key, chunk_size=50, max_workers=4,
            )
        except Exception as e:
            _set_last_llm_error("brand", f"{type(e).__name__}: {e}")
            llm_results = {}
    else:
        _set_last_llm_error("brand", "Anthropic API 키가 비어있어 LLM 호출 스킵")

    # 3) KIPRIS 병렬 호출 (8 workers)
    kipris_results: dict[str, list[dict]] = {n: [] for n in to_process}
    if kipris_key:
        def _kipris_one(n):
            try:
                return n, kipris.lookup_brand_en(n, kipris_key)
            except Exception:
                return n, []
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = [ex.submit(_kipris_one, n) for n in to_process]
            for fut in as_completed(futures):
                n, rs = fut.result()
                kipris_results[n] = rs

    # 4) 결과 조립 (in-memory)
    save_rows: list[tuple[str, dict, list[dict]]] = []
    for i, n in enumerate(to_process):
        cands: list[dict] = []
        for r in kipris_results.get(n, []):
            cands.append({
                "candidate_en": r["candidate_en"],
                "source": "kipris",
                "confidence": SOURCE_PRIORITY["kipris"] * r.get("match_quality", 1.0),
                "raw_payload": r.get("raw_payload"),
            })
        if n in llm_results:
            r = llm_results[n]
            cands.append({
                "candidate_en": r["candidate_en"],
                "source": "llm",
                "confidence": SOURCE_PRIORITY["llm"] * r.get("confidence", 0.7),
                "raw_payload": r.get("raw_response"),
            })
        rom = romanizer.romanize_brand(n)
        if rom:
            cands.append({
                "candidate_en": rom,
                "source": "romanizer",
                "confidence": SOURCE_PRIORITY["romanizer"],
                "raw_payload": None,
            })
        cands.sort(key=lambda c: -c["confidence"])
        results[n] = cands
        save_rows.append((n, {}, cands))
        if progress_callback:
            progress_callback(i + 1, total, n)

    # 5) 단일 트랜잭션으로 일괄 저장
    db.bulk_save_candidates("brand", save_rows)
    return results


def collect_categories_batch(
    names_kr: list[str],
    llm_key: str | None = None,
    skip_confirmed: bool = True,
    progress_callback=None,
) -> dict[str, list[dict]]:
    """카테고리 배치 영문화 — LLM + 로마자 폴백 (KIPRIS / 규칙기반 없음).

    카테고리는 짧고 자연어 번역에 가까워 LLM 의존도가 높다.

    Returns: {name_kr: [candidate_dict, ...]}
    """
    from . import romanizer, llm

    results: dict[str, list[dict]] = {}

    # 1) skip confirmed — 단일 IN 쿼리
    to_process = list(names_kr)
    if skip_confirmed and to_process:
        confirmed_map = db.get_confirmed_en_many("category", to_process)
        for n, en in confirmed_map.items():
            results[n] = [{
                "candidate_en": en,
                "source": "internal_db",
                "confidence": SOURCE_PRIORITY["internal_db"],
                "raw_payload": None,
            }]
        to_process = [n for n in to_process if n not in confirmed_map]

    total = len(to_process)
    if total == 0:
        return results

    # 2) LLM 청크 병렬 (80개씩, 4 워커)
    llm_results: dict[str, dict] = {}
    _set_last_llm_error("category", None)
    if llm_key:
        try:
            llm_results = llm.llm_translate_categories_batch(
                to_process, llm_key, chunk_size=80, max_workers=4,
            )
        except Exception as e:
            _set_last_llm_error("category", f"{type(e).__name__}: {e}")
            llm_results = {}
    else:
        _set_last_llm_error("category", "Anthropic API 키가 비어있어 LLM 호출 스킵")

    # 3) 후보 조립
    save_rows: list[tuple[str, dict, list[dict]]] = []
    for i, n in enumerate(to_process):
        cands: list[dict] = []
        if n in llm_results:
            r = llm_results[n]
            cands.append({
                "candidate_en": r["candidate_en"],
                "source": "llm",
                "confidence": SOURCE_PRIORITY["llm"] * r.get("confidence", 0.85),
                "raw_payload": r.get("raw_response"),
            })
        rom = romanizer.romanize_brand(n)   # 카테고리도 일반 한글 로마자 처리
        if rom:
            cands.append({
                "candidate_en": rom,
                "source": "romanizer",
                "confidence": SOURCE_PRIORITY["romanizer"],
                "raw_payload": None,
            })
        cands.sort(key=lambda c: -c["confidence"])
        results[n] = cands
        save_rows.append((n, {}, cands))
        if progress_callback:
            progress_callback(i + 1, total, n)

    # 4) 단일 트랜잭션 저장
    db.bulk_save_candidates("category", save_rows)
    return results


def collect_products_batch(
    names_kr: list[str],
    known_brands_kr: list[str] | None = None,
    llm_key: str | None = None,
    skip_confirmed: bool = True,
    progress_callback=None,
) -> dict[str, list[dict]]:
    """제품 배치 처리. LLM 청크 병렬 + 규칙 기반 파서 + romanizer.

    개선점:
      - skip_confirmed: 단일 IN 쿼리 (이전 N×connect → 1×connect)
      - LLM: 50개씩 N청크를 4 워커 ThreadPool 로 동시 호출
      - brand 영문 lookup: 1회 배치 쿼리
      - SQLite 저장: 단일 트랜잭션 (bulk_save_candidates)
    """
    from . import romanizer, parser, llm

    results: dict[str, list[dict]] = {}

    # 1) skip confirmed — 단일 배치 쿼리
    to_process = list(names_kr)
    if skip_confirmed and to_process:
        confirmed_map = db.get_confirmed_en_many("product", to_process)
        for n, en in confirmed_map.items():
            results[n] = [{
                "candidate_en": en,
                "source": "internal_db",
                "confidence": SOURCE_PRIORITY["internal_db"],
                "raw_payload": None,
                "attributes": None,
            }]
        to_process = [n for n in to_process if n not in confirmed_map]

    total = len(to_process)
    if total == 0:
        return results

    # 2) LLM 청크 병렬 (50개씩, 4 워커)
    llm_results: dict[str, dict] = {}
    _set_last_llm_error("product", None)
    if llm_key:
        try:
            llm_results = llm.llm_translate_products_batch(
                to_process, llm_key, chunk_size=50, max_workers=4,
            )
        except Exception as e:
            _set_last_llm_error("product", f"{type(e).__name__}: {e}")
            llm_results = {}
    else:
        _set_last_llm_error("product", "Anthropic API 키가 비어있어 LLM 호출 스킵")
    # LLM 결과가 0 건이면 모든 호출이 실패한 것 — 그것도 에러로 간주
    if llm_key and not llm_results:
        prev = get_last_llm_error("product") or ""
        if not prev:
            _set_last_llm_error(
                "product",
                "LLM 결과 0건 — API 응답이 모두 비었거나 파싱 실패. SDK/키/네트워크 점검.",
            )

    # 3) brand 영문 lookup 을 1회 배치 — 규칙 파서가 추출한 한글 브랜드 → 영문
    parsed_attrs: dict[str, dict] = {}
    brand_keys: set[str] = set()
    for n in to_process:
        a = parser.parse_product_name(n, known_brands=known_brands_kr or [])
        parsed_attrs[n] = a
        if a.get("brand"):
            brand_keys.add(a["brand"])
    brand_en_map = (
        db.get_confirmed_en_many("brand", list(brand_keys)) if brand_keys else {}
    )

    # 4) 결과 조립 (in-memory)
    save_rows: list[tuple[str, dict, list[dict]]] = []
    for i, n in enumerate(to_process):
        attrs = parsed_attrs[n]
        b_kr = attrs.get("brand")
        brand_en = brand_en_map.get(b_kr) if b_kr else None
        if not brand_en and b_kr:
            brand_en = romanizer.romanize_brand(b_kr)
        rule_assembled = parser.assemble_en(attrs, brand_en=brand_en)

        cands: list[dict] = []
        # 규칙 기반 후보 — 한글이 섞여 있으면(base_product 등 미번역) 제외.
        # 부분 영문화는 사용자에게 "Nongshim 신라면 Big Bowl" 같은 어색한 결과를 줌.
        if rule_assembled and not has_korean(rule_assembled):
            cands.append({
                "candidate_en": rule_assembled,
                "source": "official_site",   # rule-based
                "confidence": 0.55,
                "raw_payload": {"rule_attrs": attrs, "brand_en": brand_en},
                "attributes": attrs,
            })
        if n in llm_results:
            r = llm_results[n]
            llm_en   = r["name_en_assembled"]
            llm_conf = float(r.get("confidence", 0.7))
            # LLM 응답에 한글이 섞여 있으면(완전 영문화 실패) confidence 강등
            if has_korean(llm_en):
                llm_conf *= 0.5
            cands.append({
                "candidate_en": llm_en,
                "source": "llm",
                "confidence": SOURCE_PRIORITY["llm"] * llm_conf,
                "raw_payload": r.get("raw_response"),
                "attributes": {k: r.get(k) for k in
                               ("brand", "base_product", "flavor", "format",
                                "package_size", "variant")},
            })
        rom = romanizer.romanize_product(n)
        if rom:
            cands.append({
                "candidate_en": rom,
                "source": "romanizer",
                "confidence": SOURCE_PRIORITY["romanizer"],
                "raw_payload": None,
                "attributes": None,
            })
        cands.sort(key=lambda c: -c["confidence"])
        results[n] = cands
        save_rows.append((n, attrs, cands))
        if progress_callback:
            progress_callback(i + 1, total, n)

    # 5) 단일 트랜잭션 저장
    db.bulk_save_candidates("product", save_rows)
    return results
