"""
mapping_app/simple_layout.py — 단순 레이아웃 변경.

원천 raw 데이터 → 표준 레이아웃 컬럼 이름·순서만 그대로 변환.
DART/ISIN/영문화 같은 무거운 매핑 없이, 컬럼 매핑 + 수동 broadcast 만.

흐름:
  S1. raw + std 레이아웃 업로드
  S2. 매핑 (selectbox) + 미매핑 컬럼 수동 입력 + 다운로드
"""
from __future__ import annotations

import io

import pandas as pd


def build_simple_output_df(
    raw_df: pd.DataFrame,
    std_cols: list[str],
    std_to_raw: dict[str, str],
    manual_values: dict[str, str],
) -> pd.DataFrame:
    """std_cols 순서로 출력 DataFrame 생성.

    Args:
        raw_df: 원천 데이터
        std_cols: 표준 레이아웃 컬럼 (출력 순서·이름)
        std_to_raw: {std_col: raw_col_name 또는 ""}
        manual_values: {std_col: 수동 입력값} — 미매핑 시 모든 행에 broadcast

    Returns: DataFrame (행 수 = len(raw_df), 컬럼 = std_cols)
    """
    out = pd.DataFrame()
    n_rows = len(raw_df)
    for std in std_cols:
        raw_col = (std_to_raw.get(std) or "").strip()
        if raw_col and raw_col in raw_df.columns:
            out[std] = raw_df[raw_col].values
        else:
            v = manual_values.get(std, "")
            out[std] = [v] * n_rows
    return out


def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "data",
                     max_rows: int = 1_048_575) -> tuple[bytes, bool]:
    """xlsx bytes 직렬화. 1M 행 초과 시 자름.

    Returns: (bytes, truncated_bool)
    """
    truncated = False
    safe_df = df
    if safe_df is None or safe_df.empty:
        safe_df = pd.DataFrame({"info": ["(no data)"]})
    elif len(safe_df) > max_rows:
        safe_df = safe_df.head(max_rows)
        truncated = True
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        safe_df.to_excel(w, index=False, sheet_name=sheet_name)
        ws = w.book[sheet_name]
        ws.sheet_state = "visible"
    return buf.getvalue(), truncated


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    if df is None or df.empty:
        df = pd.DataFrame({"info": ["(no data)"]})
    return df.to_csv(index=False).encode("utf-8-sig")


# ══════════════════════════════════════════════════════════════════════════════
# LLM 으로 샘플 데이터 자동 생성
# ══════════════════════════════════════════════════════════════════════════════

_SAMPLE_SYSTEM = (
    "You generate realistic sample data rows in JSON for a Korean alt-data POS / "
    "transaction analysis schema. "
    "Given the list of column names and a user-provided context prompt, "
    "produce EXACTLY {n_rows} JSON objects, each containing ALL the column keys with "
    "plausible values consistent with the context. "
    "Korean data conventions: "
    "company names in Korean or English; dates in YYYY-MM-DD or YYYYMMDD format; "
    "monetary values as integers (KRW); brand/product names in Korean if context is "
    "Korean market; security_code/ISIN as KR7xxxxxxxxx for KOSPI/KOSDAQ. "
    "Return ONLY a JSON array — no markdown fences, no commentary, no preamble."
)


_CLONE_SYSTEM = (
    "You generate synthetic sample data in JSON, mimicking the SCHEMA and VALUE PATTERNS "
    "of a reference dataset while applying a user-provided transformation. "
    "You receive: "
    "(1) reference rows (CSV-like preview), "
    "(2) a user transformation prompt (e.g., '한국 화장품 브랜드로 바꿔줘'), "
    "(3) target row count N. "
    "Generate EXACTLY N JSON objects with these rules: "
    "- Use the EXACT same column names (same order) as the reference. "
    "- Preserve value patterns: date formats (YYYYMMDD vs YYYY-MM-DD), numeric ranges, "
    "  ID patterns (length, prefix), categorical level (e.g., '대형마트'/'편의점'). "
    "- Apply user transformation: swap identity fields (company/brand/product/category) "
    "  with values from the requested domain while keeping plausible distributions. "
    "- Keep statistical realism — sales magnitudes, frequency mix, demographics. "
    "Return ONLY a JSON array. No markdown, no commentary."
)


def llm_clone_sample_data(
    reference_df: pd.DataFrame,
    user_prompt: str,
    api_key: str,
    n_rows: int = 50,
    ref_rows: int = 10,
    model: str = "claude-haiku-4-5",
) -> tuple[pd.DataFrame, str]:
    """reference_df 의 스키마·값 패턴을 따라 user_prompt 로 변형해 N행 생성.

    Args:
        reference_df: 업로드된 raw 데이터 (앞 ref_rows 행만 LLM 에 전달)
        user_prompt: 변형 지시 (예: '한국 화장품 브랜드로 바꿔줘')
        n_rows: 생성 행 수

    Returns: (DataFrame, error_msg)
    """
    if reference_df is None or reference_df.empty:
        return pd.DataFrame(), "reference_df 가 비어 있음"
    if not api_key:
        return pd.DataFrame(), "Anthropic API 키 없음"
    try:
        from anthropic import Anthropic
    except ImportError:
        return pd.DataFrame(), "anthropic SDK 미설치"

    import json
    cols = list(reference_df.columns)
    head = reference_df.head(ref_rows).copy()
    head_csv = head.to_csv(index=False)
    # dtype 요약 (LLM 이 패턴 인식 돕도록)
    dtype_summary = {str(c): str(reference_df[c].dtype) for c in cols}

    system = _CLONE_SYSTEM
    user_msg = (
        f"Reference columns (order): {json.dumps(cols, ensure_ascii=False)}\n"
        f"Reference dtypes: {json.dumps(dtype_summary, ensure_ascii=False)}\n\n"
        f"Reference rows (first {ref_rows}):\n```\n{head_csv}\n```\n\n"
        f"User transformation: {user_prompt or '(no change — just synthesize similar data)'}\n\n"
        f"Generate {n_rows} new rows. Return JSON array only."
    )
    try:
        client = Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=model, max_tokens=16000,
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = resp.content[0].text if resp.content else ""
    except Exception as e:
        return pd.DataFrame(), f"{type(e).__name__}: {e}"

    import re
    m = re.search(r"\[[\s\S]*\]", text or "")
    if not m:
        return pd.DataFrame(), f"JSON array 파싱 실패. 응답 앞: {(text or '')[:200]}"
    try:
        arr = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        return pd.DataFrame(), f"JSON decode 실패: {e}"
    if not isinstance(arr, list):
        return pd.DataFrame(), "응답이 array 가 아님"

    rows = []
    for r in arr:
        if not isinstance(r, dict):
            continue
        rows.append({c: r.get(c, "") for c in cols})
    df = pd.DataFrame(rows, columns=cols)
    return df, ("" if not df.empty else "생성된 행 0건")


def llm_generate_sample_data(
    std_cols: list[str],
    user_prompt: str,
    api_key: str,
    n_rows: int = 50,
    model: str = "claude-haiku-4-5",
) -> tuple[pd.DataFrame, str]:
    """자연어 프롬프트 + 표준 컬럼 → LLM 으로 N 행 샘플 데이터 생성.

    Args:
        std_cols: 표준 레이아웃 컬럼 리스트
        user_prompt: 자연어 컨텍스트 (예: '한국 화장품 브랜드로 적절한 값')
        api_key: Anthropic API key
        n_rows: 생성 행 수 (기본 50)

    Returns: (DataFrame, 에러 메시지 또는 빈 문자열)
    """
    if not std_cols:
        return pd.DataFrame(), "표준 컬럼이 비어 있음"
    if not api_key:
        return pd.DataFrame(), "Anthropic API 키 없음"

    try:
        from anthropic import Anthropic
    except ImportError:
        return pd.DataFrame(), "anthropic SDK 미설치 (python3 -m pip install anthropic --break-system-packages)"

    import json
    system_prompt = _SAMPLE_SYSTEM.replace("{n_rows}", str(n_rows))
    user_msg = (
        f"Columns (in order): {json.dumps(std_cols, ensure_ascii=False)}\n\n"
        f"Context: {user_prompt or '한국 일반 회사 데이터'}\n\n"
        f"Generate {n_rows} rows."
    )

    try:
        client = Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=model,
            max_tokens=16000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        text = resp.content[0].text if resp.content else ""
    except Exception as e:
        return pd.DataFrame(), f"{type(e).__name__}: {e}"

    # JSON array 추출
    import re
    m = re.search(r"\[[\s\S]*\]", text or "")
    if not m:
        return pd.DataFrame(), f"JSON array 파싱 실패. 응답 앞부분: {(text or '')[:200]}"
    try:
        arr = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        return pd.DataFrame(), f"JSON decode 실패: {e}"
    if not isinstance(arr, list):
        return pd.DataFrame(), "응답이 array 가 아님"

    # DataFrame 구성 — std_cols 순서로
    rows = []
    for r in arr:
        if not isinstance(r, dict):
            continue
        rows.append({c: r.get(c, "") for c in std_cols})
    df = pd.DataFrame(rows, columns=std_cols)
    if df.empty:
        return df, "생성된 행 0건"
    return df, ""
