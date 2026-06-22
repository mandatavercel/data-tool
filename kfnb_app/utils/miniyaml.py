"""
kfnb_app/utils/miniyaml.py — 의존성 없는 최소 YAML 로더 (폴백).

PyYAML 이 설치돼 있으면 config.py 는 그것을 쓰고, 없으면 이 모듈을 쓴다.
지원 범위(우리 configs/*.yaml 가 쓰는 부분집합만):
  - 2-space 들여쓰기 중첩 매핑
  - 블록 리스트(  - item) / 인라인 리스트([a, b, "c"])
  - 스칼라: int / float / bool / null / 따옴표/비따옴표 문자열
  - 전체행·인라인 주석(#) — 따옴표/대괄호 안은 보존
복잡한 YAML(앵커, 멀티라인 등)은 지원하지 않음 — 그런 파일은 PyYAML 필요.
"""
from __future__ import annotations

import re


def safe_load(text: str):
    rows = []
    for raw in text.splitlines():
        line = _strip_comment(raw)
        if line.strip() == "":
            continue
        indent = len(line) - len(line.lstrip(" "))
        rows.append((indent, line.strip()))
    if not rows:
        return {}
    _, val = _parse_block(rows, 0, rows[0][0])
    return val


def _parse_block(rows, i, indent):
    # 리스트 블록
    if rows[i][1].startswith("- "):
        items = []
        while i < len(rows) and rows[i][0] == indent and rows[i][1].startswith("- "):
            items.append(_scalar(rows[i][1][2:].strip()))
            i += 1
        return i, items
    # 매핑 블록
    d = {}
    while i < len(rows) and rows[i][0] == indent and not rows[i][1].startswith("- "):
        key, _, rest = rows[i][1].partition(":")
        key = _scalar(key.strip())
        rest = rest.strip()
        i += 1
        if rest == "":
            if i < len(rows) and rows[i][0] > indent:
                i, sub = _parse_block(rows, i, rows[i][0])
                d[key] = sub
            else:
                d[key] = None
        else:
            d[key] = _scalar(rest)
    return i, d


def _split_list(inner: str):
    parts, buf, q = [], [], None
    for c in inner:
        if q:
            buf.append(c)
            if c == q:
                q = None
        elif c in ('"', "'"):
            q = c
            buf.append(c)
        elif c == ",":
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(c)
    if buf:
        parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip() != ""]


def _scalar(s: str):
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        return [_scalar(x) for x in _split_list(inner)] if inner else []
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        return s[1:-1]
    if re.match(r"^-?\d+$", s):
        return int(s)
    if re.match(r"^-?\d+\.\d+$", s):
        return float(s)
    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "~"):
        return None
    return s


def _strip_comment(line: str) -> str:
    out, q, depth, i = [], None, 0, 0
    while i < len(line):
        c = line[i]
        if q:
            out.append(c)
            if c == q:
                q = None
        elif c in ('"', "'"):
            q = c
            out.append(c)
        elif c == "[":
            depth += 1
            out.append(c)
        elif c == "]":
            depth = max(0, depth - 1)
            out.append(c)
        elif c == "#" and depth == 0 and (i == 0 or line[i - 1] in " \t"):
            break
        else:
            out.append(c)
        i += 1
    return "".join(out).rstrip()
