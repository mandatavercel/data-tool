"""
modules.mapping.translation

브랜드·제품 영문화 파이프라인.

서브모듈:
  romanizer  — 한글 → 로마자 (RR 간단 구현)
  parser     — 제품명 속성 분해 (brand/base/flavor/format/...)
  kipris     — KIPRIS Plus API 어댑터 (상표)
  llm        — Claude/OpenAI 폴백 (영문 변환 + 속성 분해)
  pipeline   — 출처별 후보 수집 + scoring 오케스트레이터
"""
import re


def normalize_en(s: str) -> str:
    """영문명 정규화 — 공백을 언더바로 통일 + 흔한 punctuation 정리.

    회사 표준: 모든 영문명은 'Samsung_Electronics' 같이 공백 없는 underscore 형태.

    규칙 (순서 중요):
      1) 양끝 공백 strip
      2) '&' 주변 공백 제거 — "F & B" → "F&B" (브랜드명 유지)
      3) ',  . ; : ! ?' 등 일반 punctuation → 공백 (단어 분리 유도)
         "NONGSHIM CO.,LTD" → "NONGSHIM CO  LTD" → "NONGSHIM_CO_LTD"
      4) 연속 공백 → 단일 underscore
      5) 다중 underscore 압축, 양끝 underscore 제거

    예시:
      "DONGWON F & B CO.,LTD"  → "DONGWON_F&B_CO_LTD"
      "NONGSHIM CO.,LTD"        → "NONGSHIM_CO_LTD"
      "KT&G Corporation."       → "KT&G_Corporation"
      "ABC, INC."               → "ABC_INC"
      "Shin Ramyun"             → "Shin_Ramyun"
    """
    if not s:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s*&\s*", "&", s)        # F & B → F&B
    s = re.sub(r"[.,;:!?]", " ", s)       # punctuation → space
    s = re.sub(r"\s+", "_", s)            # space → underscore
    s = re.sub(r"_+", "_", s)             # 중복 underscore
    return s.strip("_")


# 한글 (Hangul) 검출 — U+AC00..U+D7A3 (음절), U+1100..U+11FF (자모), U+3130..U+318F (호환 자모)
_HANGUL_RE = re.compile(r"[가-힣ᄀ-ᇿ㄰-㆏]")


def has_korean(s: str | None) -> bool:
    """문자열에 한글이 한 글자라도 포함됐는지."""
    if not s:
        return False
    return bool(_HANGUL_RE.search(str(s)))

