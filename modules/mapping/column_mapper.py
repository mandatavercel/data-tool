"""
modules/mapping/column_mapper.py

표준 레이아웃 컬럼 → raw 컬럼 자동 매핑 (매핑 앱 전용).

설계:
  1) 표준 컬럼 이름에서 'kind'(유형)를 추론한다 — date / amount / company /
     brand / sku / category / stock_code / count / quantity / text.
     kind는 ISIN 매핑·검증·변환 단계에서 의미 기반 처리에 사용된다.
  2) 각 (표준컬럼, raw컬럼) 쌍에 매칭 점수를 매긴다 — 컬럼명 키워드 +
     정규화 일치 + raw dtype 단서.
  3) Greedy 1:1 할당으로 표준컬럼 ← raw컬럼 매핑을 도출한다.
"""
from __future__ import annotations

import re
import pandas as pd

# ── 유형(kind) 정의 ───────────────────────────────────────────────────────────
# 표준 컬럼명의 키워드로 추론한다. 키는 kind, 값은 그 유형을 시사하는 키워드들.
KIND_KEYWORDS: dict[str, list[str]] = {
    "date": [
        "date", "day", "ymd", "yyyymmdd", "거래일", "일자", "날짜", "기준일",
        "년월일", "transaction_date", "주문일", "결제일", "tx_date",
    ],
    "amount": [
        "amount", "amt", "revenue", "sales", "value", "price", "매출", "금액",
        "거래액", "판매액", "결제액", "sale_amt", "sales_amount",
    ],
    "count": [
        "count", "cnt", "tx_cnt", "number_of_tx", "거래건수", "건수",
        "주문수", "결제건수", "n_tx",
    ],
    "quantity": [
        "qty", "quantity", "수량", "ea", "개수", "sale_qty",
    ],
    "company": [
        "company", "corp", "회사", "기업", "법인", "업체", "거래처",
        "client", "vendor", "고객사", "company_name", "corp_name",
        "grp_acnt", "account_name", "회사명", "기업명",
    ],
    "brand": [
        "brand", "브랜드", "상표", "maker", "제조", "brand_name",
        "grp_item",   # 회사별 양식에서 자주 발견
    ],
    "sku": [
        "sku", "item", "product", "상품", "제품", "품목", "goods", "아이템",
        "sku_name", "item_name", "product_name",
    ],
    "category": [
        "category", "cate", "카테고리", "분류", "구분", "genre", "type", "class",
        "category_name", "lrcl", "mdcl", "smcl",   # 대/중/소분류
        "sector", "섹터", "업종", "industry", "segment",
    ],
    "stock_code": [
        "stock", "ticker", "stk", "종목코드", "단축코드", "stock_code",
    ],
    "isin": [
        "isin", "isu_cd", "security", "sec_code",
    ],
    "corp_code": [
        # 법인등록번호 / 사업자등록번호 — DART 회사 식별자
        "법인등록번호", "법인번호", "법인id",
        "사업자등록번호", "사업자번호", "사업자",
        "jurir_no", "jurirno", "bizr_no", "bizrno",
        "corp_id", "corp_no", "corpcode",
    ],
    "name_eng": [
        # 영문 회사명
        "영문명", "영문회사명", "영문 회사명", "영문기업명",
        "name_eng", "nameeng", "corp_eng", "corpeng",
        "eng_name", "engname", "corp_name_eng", "english",
        "englishname", "english_name",
    ],
    "region": [
        "sido", "region", "지역", "시도", "주소",
    ],
    "gender": [
        "gender", "성별", "sex",
    ],
    "age": [
        "age", "연령", "나이",
    ],
}

# kind별 표시 라벨/색상 (UI 표시용)
KIND_LABEL: dict[str, tuple[str, str]] = {
    "date":       ("📅 날짜",   "#1e40af"),
    "amount":     ("💰 금액",   "#16a34a"),
    "count":      ("🔢 건수",   "#0d9488"),
    "quantity":   ("📦 수량",   "#0d9488"),
    "company":    ("🏢 회사",   "#7c3aed"),
    "brand":      ("🏷 브랜드", "#7c3aed"),
    "sku":        ("📦 상품",   "#d97706"),
    "category":   ("🗂 카테고리", "#d97706"),
    "stock_code": ("📈 종목",   "#dc2626"),
    "isin":       ("📈 ISIN",   "#dc2626"),
    "corp_code":  ("🏛 법인번호", "#dc2626"),
    "name_eng":   ("🌐 영문명",  "#0ea5e9"),
    "region":     ("📍 지역",   "#6b7280"),
    "gender":     ("👥 성별",   "#6b7280"),
    "age":        ("👥 연령",   "#6b7280"),
    "text":       ("📝 일반",   "#9ca3af"),
}


# ── 유틸 ──────────────────────────────────────────────────────────────────────
def _normalize_colname(s: str) -> str:
    """매칭용 키. 공백·언더스코어·구두점 제거 + 소문자."""
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[\s_\-\.,/()]+", "", s)
    return s.lower()


def infer_column_kind(std_col_name: str) -> str:
    """표준 컬럼명에서 유형 추론. 매칭 실패 시 'text'."""
    nl = (std_col_name or "").lower()
    nl_norm = _normalize_colname(std_col_name)

    # 우선순위 (specific → general):
    #  - isin/stock_code 는 매우 특이한 키워드라 먼저
    #  - company/brand 는 count 보다 먼저 (acnt 안의 'cnt' 오매칭 방지)
    #  - date/amount 는 흔하고 dtype 단서도 강함
    priority = [
        "isin", "stock_code", "corp_code",
        "name_eng",
        "company", "brand",
        "sku", "category",
        "date", "amount",
        "count", "quantity",
        "region", "gender", "age",
    ]
    for kind in priority:
        for kw in KIND_KEYWORDS[kind]:
            kw_lower = kw.lower()
            if kw_lower in nl or _normalize_colname(kw) in nl_norm:
                return kind
    return "text"


# ── 표준 ↔ raw 매칭 점수 ──────────────────────────────────────────────────────
def _score_pair(
    std_name: str,
    std_kind: str,
    raw_name: str,
    raw_meta: dict,
) -> tuple[int, str]:
    """
    (표준 컬럼, raw 컬럼) 매칭 점수 계산.

    Returns: (score, reason)
        score 100 → 완전 일치 (이름 정규화 후 동일)
        score 80 → kind 키워드 일치 + 부분 매칭
        score 60 → kind 키워드 일치
        score 30 → 부분 문자열 매칭만
        score 10 → dtype 단서 정도
        score  0 → 매칭 안 됨
    """
    if not std_name or not raw_name:
        return 0, ""

    std_norm = _normalize_colname(std_name)
    raw_norm = _normalize_colname(raw_name)
    raw_lower = raw_name.lower()

    # 1) 정규화 완전 일치
    if std_norm == raw_norm:
        return 100, f"이름 일치"

    # 2) kind 키워드가 raw 이름에 있는지
    kind_hit: str | None = None
    if std_kind in KIND_KEYWORDS:
        for kw in KIND_KEYWORDS[std_kind]:
            kw_n = _normalize_colname(kw)
            if kw_n and kw_n in raw_norm:
                kind_hit = kw
                break

    # 3) 부분 문자열 매칭 (정규화 후)
    substr_hit = False
    if len(std_norm) >= 2 and len(raw_norm) >= 2:
        if std_norm in raw_norm or raw_norm in std_norm:
            substr_hit = True

    # 4) dtype 단서 + 한국 회사 양식 suffix(_NM/_CD/명/이름/코드) 단서
    raw_dtype = (raw_meta or {}).get("dtype", "")
    dtype_hit = False
    if std_kind == "date":
        if "datetime" in raw_dtype:
            dtype_hit = True
        sample = str((raw_meta or {}).get("sample", ""))
        if re.match(r"^\d{8}", sample):
            dtype_hit = True
    elif std_kind in ("amount", "count", "quantity"):
        if "int" in raw_dtype or "float" in raw_dtype:
            dtype_hit = True

    # _NM/_명/_이름 (이름형) ↔ company/brand/sku/category 가산
    # _CD/_코드 (코드형) ↔ stock_code/isin 가산 / amount·count·quantity 감산
    is_name_like = bool(re.search(r"(_nm$|이름$|명$|_name$)", raw_name, re.IGNORECASE))
    is_code_like = bool(re.search(r"(_cd$|코드$|_code$)", raw_name, re.IGNORECASE))
    suffix_bonus = 0
    suffix_reason = ""
    if std_kind in ("company", "brand", "sku", "category"):
        if is_name_like:
            suffix_bonus += 15; suffix_reason = "이름형(_NM)"
        elif is_code_like:
            suffix_bonus -= 15; suffix_reason = "코드형(_CD) 페널티"
        if "object" in raw_dtype:
            dtype_hit = True
    elif std_kind in ("stock_code", "isin"):
        if is_code_like:
            suffix_bonus += 10; suffix_reason = "코드형(_CD)"
    elif std_kind in ("amount", "count", "quantity"):
        if is_code_like:
            suffix_bonus -= 10; suffix_reason = "코드형(_CD) 페널티"

    # ── 점수 산정 ─────────────────────────────────────────────────────────────
    if kind_hit and substr_hit:
        base = 80
        extra = (5 if dtype_hit else 0) + suffix_bonus
        return max(0, base + extra), (
            f"유형 '{std_kind}' + 부분일치 '{kind_hit}'"
            + (f" + {suffix_reason}" if suffix_reason else "")
        )
    if kind_hit:
        base = 60
        extra = (5 if dtype_hit else 0) + suffix_bonus
        return max(0, base + extra), (
            f"유형 '{std_kind}' 키워드 '{kind_hit}'"
            + (f" + {suffix_reason}" if suffix_reason else "")
        )
    if substr_hit:
        base = 30
        extra = (10 if dtype_hit else 0) + suffix_bonus
        return max(0, base + extra), (
            "부분 문자열 일치"
            + (" + dtype" if dtype_hit else "")
            + (f" + {suffix_reason}" if suffix_reason else "")
        )
    if dtype_hit:
        return max(0, 10 + suffix_bonus), f"dtype 단서 ({raw_dtype})"
    return 0, ""


# ── Greedy 매핑 ────────────────────────────────────────────────────────────────
def auto_map(
    std_columns: list[str],
    raw_columns: list[str],
    raw_meta: dict[str, dict] | None = None,
) -> list[dict]:
    """
    표준 컬럼 리스트 × raw 컬럼 리스트 → 표준 컬럼별 매핑 결과.

    raw_meta: { raw_col_name: { 'dtype':..., 'sample':..., 'n_unique':..., 'null_pct':... } }

    Returns: list[dict] (표준 컬럼 순서 그대로)
      std_col       : 표준 컬럼명
      kind          : 추론된 유형
      raw_col       : 매핑된 raw 컬럼 (None이면 매핑 없음)
      score         : 매칭 점수 (0-100)
      reason        : 매칭 사유
      candidates    : 점수 상위 후보 [(raw_col, score, reason), ...]
    """
    raw_meta = raw_meta or {col: {} for col in raw_columns}

    # 표준 컬럼 → kind
    std_kinds = [infer_column_kind(c) for c in std_columns]

    # 모든 (std, raw) 쌍의 점수 계산
    scored: list[tuple[int, int, int, str]] = []  # (-score, std_idx, raw_idx, reason)
    pair_info: dict[tuple[int, int], tuple[int, str]] = {}
    for i, std in enumerate(std_columns):
        for j, raw in enumerate(raw_columns):
            sc, reason = _score_pair(std, std_kinds[i], raw, raw_meta.get(raw, {}))
            if sc > 0:
                scored.append((-sc, i, j, reason))
                pair_info[(i, j)] = (sc, reason)

    # Greedy: 점수 높은 순으로 1:1 할당
    scored.sort()
    assigned_std: set[int] = set()
    assigned_raw: set[int] = set()
    assignment: dict[int, tuple[int, int, str]] = {}  # std_idx → (raw_idx, score, reason)
    for neg_sc, i, j, reason in scored:
        if i in assigned_std or j in assigned_raw:
            continue
        assignment[i] = (j, -neg_sc, reason)
        assigned_std.add(i)
        assigned_raw.add(j)

    # 결과 조립 + 후보 top3
    result: list[dict] = []
    for i, std in enumerate(std_columns):
        # 후보: 이 표준에 대한 raw별 점수, top3
        cands = []
        for j, raw in enumerate(raw_columns):
            sc, reason = pair_info.get((i, j), (0, ""))
            if sc > 0:
                cands.append((raw, sc, reason))
        cands.sort(key=lambda t: -t[1])
        cands = cands[:5]

        if i in assignment:
            j, sc, reason = assignment[i]
            result.append({
                "std_col":    std,
                "kind":       std_kinds[i],
                "raw_col":    raw_columns[j],
                "score":      sc,
                "reason":     reason,
                "candidates": cands,
            })
        else:
            result.append({
                "std_col":    std,
                "kind":       std_kinds[i],
                "raw_col":    None,
                "score":      0,
                "reason":     "매칭 없음",
                "candidates": cands,
            })
    return result


# ── 표준 레이아웃 파일 로더 ────────────────────────────────────────────────────
def _looks_like_header(values) -> bool:
    """행의 값들이 '의미 있는 헤더'인지.
    조건 — non-empty + Unnamed/nan 아닌 셀이 최소 1개 이상이고,
            그런 셀이 전체 길이의 30% 이상.
    (이전 50% 기준은 sparse 한 표준 양식에서 헤더를 놓쳤음)
    """
    vals = [v for v in values if v is not None and str(v).strip() != "" and str(v) != "nan"]
    if not vals:
        return False
    non_unnamed = [v for v in vals if not str(v).lower().startswith("unnamed")]
    if not non_unnamed:
        return False
    # 헤더가 1개여도 인정 (단, 전체 길이 대비 30% 이상)
    return len(non_unnamed) >= max(1, int(0.3 * len(values)))


def read_standard_layout(file_or_df) -> dict:
    """
    표준 레이아웃 파일에서 컬럼명 리스트 추출 — robust 버전.

    Returns:
        {
            "columns": list[str],     # 인식된 헤더 컬럼들 (Unnamed/공백 제거)
            "sheet":   str | None,    # 어떤 시트에서 읽었는지
            "header_row": int,        # 0-based 헤더 행 번호 (csv는 0 고정)
            "tried":   list[dict],    # 시도 로그 (디버깅용)
        }

    동작:
      - csv: 그대로 헤더 행 사용
      - xlsx: 모든 시트를 시도, 각 시트에서 헤더가 0~10행 중 어디에 있는지
              자동 탐색해서 가장 컬럼이 많은 후보 채택.
    """
    tried: list[dict] = []

    # DataFrame 직접 받기
    if isinstance(file_or_df, pd.DataFrame):
        cols = _clean_columns([str(c) for c in file_or_df.columns])
        return {"columns": cols, "sheet": None, "header_row": 0, "tried": tried}

    name = getattr(file_or_df, "name", str(file_or_df))

    # ── CSV ──────────────────────────────────────────────────────────────────
    if name.lower().endswith(".csv"):
        try:
            df = pd.read_csv(file_or_df, nrows=0)
            cols = _clean_columns([str(c) for c in df.columns])
            tried.append({"sheet": "(csv)", "header_row": 0, "cols": len(cols)})
            return {"columns": cols, "sheet": None, "header_row": 0, "tried": tried}
        except Exception as e:
            tried.append({"sheet": "(csv)", "error": str(e)})
            return {"columns": [], "sheet": None, "header_row": 0, "tried": tried}

    # ── XLSX/XLS ─────────────────────────────────────────────────────────────
    # 1) 모든 시트 이름 가져오기
    try:
        # ExcelFile 객체로 시트 목록 얻기 (커서 위치 고려 — UploadedFile은 BytesIO)
        if hasattr(file_or_df, "seek"):
            file_or_df.seek(0)
        xls = pd.ExcelFile(file_or_df)
        sheet_names = xls.sheet_names
    except Exception as e:
        tried.append({"error": f"ExcelFile 열기 실패: {e}"})
        return {"columns": [], "sheet": None, "header_row": 0, "tried": tried}

    best: dict | None = None
    for sheet in sheet_names:
        # 각 시트의 상위 12행을 헤더 없이 읽음
        try:
            if hasattr(file_or_df, "seek"):
                file_or_df.seek(0)
            preview = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=12)
        except Exception as e:
            tried.append({"sheet": sheet, "error": f"읽기 실패: {e}"})
            continue

        if preview.empty:
            tried.append({"sheet": sheet, "empty": True})
            continue

        # 행 0~min(10, len-1) 중 header 후보 찾기
        for hr in range(min(11, len(preview))):
            row_vals = list(preview.iloc[hr].fillna("").astype(str))
            if _looks_like_header(row_vals):
                # 그 행을 header 로 사용해 cols 추출
                cleaned = _clean_columns(row_vals)
                tried.append({"sheet": sheet, "header_row": hr, "cols": len(cleaned)})
                cand = {"columns": cleaned, "sheet": sheet, "header_row": hr}
                if best is None or len(cleaned) > len(best["columns"]):
                    best = cand
                break  # 한 시트에서 하나만 채택
        else:
            tried.append({"sheet": sheet, "note": "헤더 후보 없음"})

    if best is None:
        return {"columns": [], "sheet": None, "header_row": 0, "tried": tried}

    best["tried"] = tried
    return best


def _clean_columns(cols) -> list[str]:
    """Unnamed/빈 셀/중복을 정리한 컬럼명 리스트."""
    out: list[str] = []
    seen: set[str] = set()
    for c in cols:
        s = str(c).strip()
        if not s or s.lower().startswith("unnamed") or s == "nan":
            continue
        # 중복 방지 (같은 이름 두 번 나오면 무시)
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def raw_metadata(df: pd.DataFrame, n_samples: int = 5) -> dict[str, dict]:
    """
    raw DataFrame의 컬럼별 간단 메타 — auto_map에 입력으로 사용.

    samples: 결측 제외, 중복 제거 후 상위 n_samples 개의 실제 값 (str화).
             UI에서 사용자가 어떤 컬럼인지 파악할 수 있도록 제공.
    """
    meta: dict[str, dict] = {}
    for col in df.columns:
        s = df[col]
        # 결측 제외 + 중복 제거 후 상위 n개
        clean = s.dropna()
        try:
            uniq = clean.drop_duplicates().head(n_samples)
        except Exception:
            uniq = clean.head(n_samples)
        samples = [str(v) for v in uniq.tolist()]
        meta[col] = {
            "dtype":    str(s.dtype),
            "sample":   samples[0] if samples else "",
            "samples":  samples,
            "n_unique": int(s.nunique()),
            "null_pct": float(s.isna().mean() * 100),
        }
    return meta
