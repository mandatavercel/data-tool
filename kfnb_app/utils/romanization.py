"""
kfnb_app/utils/romanization.py — 한글 → Revised Romanization(RR) 근사 변환.

저신뢰 alias/롱테일 SKU 폴백용. 정확도보다 일관성이 목적이며, 결과에는
mapping_confidence=low 가 붙는다. (RR 연음/받침 규칙은 단순화한 근사치)
"""
from __future__ import annotations

_LEAD = ["g","kk","n","d","tt","r","m","b","pp","s","ss","","j","jj","ch","k","t","p","h"]
_VOWEL = ["a","ae","ya","yae","eo","e","yeo","ye","o","wa","wae","oe","yo","u",
          "wo","we","wi","yu","eu","ui","i"]
_TAIL = ["","g","kk","gs","n","nj","nh","d","l","lg","lm","lb","ls","lt","lp",
         "lh","m","b","bs","s","ss","ng","j","ch","k","t","p","h"]


def romanize(text: str) -> str:
    """한글 → RR 로마자(근사). 비한글은 그대로 통과. 단어 첫 글자만 대문자."""
    if text is None:
        return ""
    out = []
    for ch in str(text):
        code = ord(ch)
        if 0xAC00 <= code <= 0xD7A3:
            s = code - 0xAC00
            out.append(_LEAD[s // 588] + _VOWEL[(s % 588) // 28] + _TAIL[s % 28])
        else:
            out.append(ch)
    rom = "".join(out).strip()
    return " ".join(w.capitalize() for w in rom.split())
