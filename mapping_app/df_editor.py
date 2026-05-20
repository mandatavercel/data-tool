"""
mapping_app/df_editor.py — 자연어 → DataFrame 편집 코드 → 안전 실행.

흐름:
  1. 사용자 자연어 명령 입력 ("company_name_en 컬럼의 CO_LTD suffix 제거")
  2. Claude 가 schema + 명령으로 pandas 코드 생성
  3. UI 에 코드 표시 → 사용자 검토 → 실행 버튼
  4. 안전한 exec (제한 globals + AST 검사) → 새 DataFrame

보안:
  - import / exec / eval / open 등 위험 토큰 차단
  - __builtins__ 제한 (str, int, len, range, abs, min, max 만)
  - exec 결과 df 만 반환
"""
from __future__ import annotations

import ast
import re
from typing import Any

import pandas as pd


_EDIT_SYSTEM = (
    "You are a pandas DataFrame editor. Given a DataFrame's schema and a Korean/English "
    "natural-language edit request, produce Python code that MUTATES the variable named `df` "
    "(a pandas DataFrame already in scope). "
    "Strict rules: "
    "(1) Output ONLY the Python code — no markdown fences, no comments, no explanation. "
    "(2) Use only `df` and pandas / Series methods (str, fillna, astype, replace, drop, etc.). "
    "(3) Do NOT use: import, exec, eval, open, os, sys, subprocess, __import__, "
    "globals, locals, getattr, setattr, __ — ANY use will be rejected. "
    "(4) Code may span multiple lines. End the script with `df` assigned to the final DataFrame. "
    "(5) Handle missing values defensively (e.g., .fillna('') before .str ops). "
    "(6) Preserve other columns; only modify what the request asks. "
    "Examples: "
    "Request: 'company_name_en 컬럼의 CO_LTD suffix 제거' → "
    "  df['company_name_en'] = df['company_name_en'].fillna('').str.replace(r'_CO_LTD$', '', regex=True). "
    "Request: 'sales_amount 가 0 인 행 제거' → "
    "  df = df[pd.to_numeric(df['sales_amount'], errors='coerce').fillna(0) != 0]. "
    "Request: 'mandata_brand_name 모두 대문자' → "
    "  df['mandata_brand_name'] = df['mandata_brand_name'].fillna('').str.upper()."
)


# ── 보안 검사 ───────────────────────────────────────────────────────────────
_FORBIDDEN_TOKENS = (
    "import", "exec(", "eval(", "open(", "os.", "sys.", "subprocess",
    "__import__", "__builtins__", "__class__", "__bases__", "__subclasses__",
    "globals(", "locals(", "getattr(", "setattr(", "delattr(",
    "compile(", "input(", "memoryview", "bytearray",
)


def _is_safe(code: str) -> tuple[bool, str]:
    """다중 검사 — 위험 토큰·AST 노드 차단. (ok, reason)."""
    s = code or ""
    # 1) 단순 토큰 차단
    for tok in _FORBIDDEN_TOKENS:
        if tok in s:
            return False, f"금지 토큰 발견: {tok!r}"
    # 2) AST 검사 — Import 노드 명시 차단
    try:
        tree = ast.parse(s)
    except SyntaxError as e:
        return False, f"SyntaxError: {e}"
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            return False, "import 구문 사용 금지"
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in ("exec", "eval", "compile", "__import__"):
                return False, f"위험 호출: {node.func.id}"
    return True, ""


def apply_edit_code(df: pd.DataFrame, code: str) -> tuple[pd.DataFrame | None, str]:
    """안전 검사 후 exec. 성공 시 (new_df, ''), 실패 시 (None, error)."""
    ok, reason = _is_safe(code)
    if not ok:
        return None, f"❌ 보안 거부: {reason}"

    safe_builtins = {
        "len": len, "range": range, "abs": abs, "min": min, "max": max,
        "str": str, "int": int, "float": float, "list": list, "dict": dict,
        "tuple": tuple, "set": set, "round": round, "sum": sum, "any": any,
        "all": all, "bool": bool, "sorted": sorted, "enumerate": enumerate,
        "zip": zip, "map": map, "filter": filter, "True": True, "False": False,
        "None": None,
    }
    g = {"__builtins__": safe_builtins, "pd": pd}
    l = {"df": df.copy()}
    try:
        exec(code, g, l)
    except Exception as e:
        return None, f"❌ 실행 오류: {type(e).__name__}: {e}"

    new_df = l.get("df")
    if not isinstance(new_df, pd.DataFrame):
        return None, "❌ 실행 후 `df` 변수가 DataFrame 이 아님"
    return new_df, ""


def llm_generate_edit_code(
    df: pd.DataFrame,
    user_request: str,
    api_key: str,
    head_rows: int = 5,
    model: str = "claude-haiku-4-5",
) -> tuple[str, str]:
    """자연어 → pandas 편집 코드 생성. (code, error)."""
    if df is None or df.empty:
        return "", "DataFrame 이 비어있음"
    if not (user_request or "").strip():
        return "", "요청이 비어있음"
    if not api_key:
        return "", "Anthropic API 키 없음"
    try:
        from anthropic import Anthropic
    except ImportError:
        return "", "anthropic SDK 미설치"

    cols      = list(df.columns)
    dtypes    = {str(c): str(df[c].dtype) for c in cols}
    head_csv  = df.head(head_rows).to_csv(index=False)
    user_msg  = (
        f"Columns: {cols}\n"
        f"Dtypes: {dtypes}\n"
        f"First {head_rows} rows:\n```\n{head_csv}\n```\n\n"
        f"Edit request: {user_request}\n\n"
        f"Output Python code only (modifies `df`)."
    )
    try:
        client = Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=model, max_tokens=2000,
            system=_EDIT_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = resp.content[0].text if resp.content else ""
    except Exception as e:
        return "", f"{type(e).__name__}: {e}"

    # 마크다운 fence 제거
    code = (text or "").strip()
    code = re.sub(r"^```(?:python)?\s*", "", code)
    code = re.sub(r"\s*```$", "", code)
    return code.strip(), ""
