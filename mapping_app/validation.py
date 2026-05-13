"""
mapping_app/validation.py — 데이터 품질 검증 유틸.

⑦ 데이터 검증 step 에서 사용. streamlit 비의존 (순수 pandas).
date / amount kind 컬럼이 매핑되어 있으면 결측·파싱·중복을 점검.
"""
from __future__ import annotations

import re
import warnings

import pandas as pd


def parse_date_series(s: pd.Series) -> pd.Series:
    """YYYYMMDD 정수 / 'YYYY-MM-DD' / datetime 등 다양한 형식을 안전 파싱.

    ⚠️ pd.to_datetime(20240101) 는 epoch 이후 ns 로 해석돼 1970 년대 날짜가
       나오는 버그. 정수는 문자열로 변환 후 format='%Y%m%d' 명시.
    """
    if pd.api.types.is_datetime64_any_dtype(s):
        return s

    sample = s.dropna()
    if sample.empty:
        return pd.to_datetime(s, errors="coerce")

    s0 = str(sample.iloc[0]).strip().split(".")[0]

    if s0.isdigit() and len(s0) == 8 and 19000101 <= int(s0) <= 21001231:
        ss = s.apply(lambda x: str(int(x)) if pd.notna(x) else None)
        return pd.to_datetime(ss, format="%Y%m%d", errors="coerce")

    if re.match(r"^\d{4}-\d{2}-\d{2}", s0):
        return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    if re.match(r"^\d{4}/\d{2}/\d{2}", s0):
        return pd.to_datetime(s, format="%Y/%m/%d", errors="coerce")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return pd.to_datetime(s, errors="coerce")


def simple_validate(
    raw_df: pd.DataFrame,
    date_col: str | None,
    amount_col: str | None,
) -> dict:
    """date / amount raw 컬럼을 인자로 받아 결측·파싱·중복 검사.

    Returns: {total: int, checks: list[{label, severity, detail}]}
      severity ∈ {ok, info, warning, error, critical}
    """
    total = len(raw_df)
    checks: list[dict] = []

    # 1) 행 수
    if total < 10:
        checks.append({"label": "행 수", "severity": "critical",
                       "detail": f"{total}행 — 분석 불가 (최소 10행)"})
    else:
        checks.append({"label": "행 수", "severity": "ok", "detail": f"{total:,}행"})

    # 2) 날짜
    if date_col:
        s = raw_df[date_col]
        na_n = int(s.isna().sum())
        parsed = parse_date_series(s)
        bad_n = int(parsed.isna().sum() - na_n)
        if na_n:
            checks.append({"label": "날짜 결측", "severity": "warning",
                           "detail": f"{na_n:,}행 ({na_n/total*100:.1f}%)"})
        else:
            checks.append({"label": "날짜 결측", "severity": "ok", "detail": "없음"})

        if bad_n:
            checks.append({"label": "날짜 파싱 실패", "severity": "error",
                           "detail": f"{bad_n:,}행 — 형식 점검 필요"})
        else:
            checks.append({"label": "날짜 파싱", "severity": "ok", "detail": "전체 성공"})

        valid = parsed.dropna()
        if not valid.empty:
            checks.append({"label": "데이터 기간", "severity": "info",
                           "detail": f"{valid.min():%Y-%m-%d} ~ {valid.max():%Y-%m-%d}"})
    else:
        checks.append({"label": "날짜 컬럼", "severity": "warning",
                       "detail": "표준 컬럼 중 kind=date 가 매핑되지 않음"})

    # 3) 매출
    if amount_col:
        num    = pd.to_numeric(raw_df[amount_col], errors="coerce")
        na_n   = int(num.isna().sum())
        neg_n  = int((num < 0).sum())
        zero_n = int((num == 0).sum())
        if na_n:
            checks.append({"label": "매출 결측/숫자 아님", "severity": "warning",
                           "detail": f"{na_n:,}행 ({na_n/total*100:.1f}%)"})
        else:
            checks.append({"label": "매출 결측", "severity": "ok", "detail": "없음"})
        if neg_n:
            checks.append({"label": "음수 매출", "severity": "info",
                           "detail": f"{neg_n:,}행 (환불·취소 가능)"})
        if zero_n:
            checks.append({"label": "0원 매출", "severity": "info",
                           "detail": f"{zero_n:,}행"})
    else:
        checks.append({"label": "매출 컬럼", "severity": "warning",
                       "detail": "표준 컬럼 중 kind=amount 가 매핑되지 않음"})

    # 4) 중복
    dup = int(raw_df.duplicated().sum())
    if dup:
        checks.append({"label": "중복 행", "severity": "warning",
                       "detail": f"{dup:,}행 ({dup/total*100:.1f}%)"})
    else:
        checks.append({"label": "중복 행", "severity": "ok", "detail": "없음"})

    return {"total": total, "checks": checks}
