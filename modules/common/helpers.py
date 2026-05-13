import re
import pandas as pd

ROLE_OPTIONS = [
    "transaction_date", "company_name", "brand_name", "sku_name",
    "category_name", "sales_amount", "number_of_tx", "quantity",
    "stock_code", "security_code", "unknown",
]


def get_col(role_map: dict, *roles):
    """Return the first mapped column for any of the given roles."""
    for r in roles:
        if role_map.get(r):
            return role_map[r]
    return None


def normalize_stock_code(code_str: str) -> str:
    s = str(code_str).strip().replace(" ", "")
    if len(s) == 12 and s[:2].isalpha():   # ISIN: KR7005930003 → 005930
        return s[3:9]
    if len(s) == 9 and s.isdigit():
        return s[:6]
    return s.zfill(6) if s.isdigit() else s


def parse_dates(series: pd.Series) -> pd.Series:
    sample = str(series.dropna().iloc[0]).strip() if series.dropna().size > 0 else ""
    if len(sample) == 8 and sample.isdigit():
        return pd.to_datetime(series, format='%Y%m%d', errors='coerce')
    return pd.to_datetime(series, errors='coerce')


def _r_strength(r):
    a = abs(r)
    if a >= 0.7:
        return "매우 강한"
    if a >= 0.5:
        return "강한"
    if a >= 0.3:
        return "중간 정도의"
    if a >= 0.1:
        return "약한"
    return "거의 없는"


def _r_direction(r):
    return "양의" if r >= 0 else "음의"


def _lag_text(lag, unit="일"):
    if unit == "개월":
        m = {0: "당월", 1: "1개월 후", 2: "2개월 후", 3: "3개월 후", 6: "6개월 후", 12: "1년 후"}
        return m.get(lag, f"{lag}개월 후")
    m = {0: "당일", 1: "1일 후", 3: "3일 후", 7: "1주일 후", 14: "2주일 후", 30: "약 1개월 후"}
    return m.get(lag, f"{lag}일 후")


def _sig_text(p):
    if p < 0.01:
        return "매우 높은 신뢰도 (p<0.01)"
    if p < 0.05:
        return "통계적으로 유의 (p<0.05)"
    return "통계적으로 유의하지 않음 (p≥0.05)"


def col_sample(df, col, n=5):
    vals = df[col].dropna().unique()[:n]
    return " / ".join(str(v) for v in vals)


def infer_schema(df: pd.DataFrame) -> list:
    """
    Returns list of dicts: {column_name, inferred_role, confidence(0-100), reason}
    Each column gets exactly one entry.
    """
    DATE_KW  = ["date", "day", "날짜", "거래일", "일자", "기간", "time", "dt", "ymd", "거래", "transaction"]
    SALES_KW = ["amount", "sales", "revenue", "매출", "금액", "value", "amt", "money", "수익", "거래액", "price", "sum", "합계"]
    TX_KW    = ["tx", "count", "cnt", "건수", "거래수", "횟수", "number_of", "num_of", "건", "회", "times"]
    QTY_KW   = ["qty", "quantity", "수량", "ea", "개수", "pcs"]
    SKU_KW   = ["sku", "item", "product", "상품", "제품", "품목", "goods", "아이템"]
    CAT_KW   = ["category", "cate", "카테고리", "분류", "구분", "type", "class", "genre"]
    COMP_KW  = ["company", "corp", "account", "acnt", "업체", "회사", "법인", "거래처", "client", "vendor", "고객"]
    BRAND_KW = ["brand", "브랜드", "상표", "maker", "제조"]
    STOCK_KW = ["stock", "종목", "ticker", "stk"]
    SEC_KW   = ["isin", "security", "sec_code", "isu"]

    scores = {}      # col -> {role -> score}
    reasons_map = {} # col -> {role -> [reasons]}

    for col in df.columns:
        nl = col.lower()
        sample = df[col].dropna().head(100)
        dtype = str(df[col].dtype)
        s_str = sample.astype(str)
        scores[col] = {}
        reasons_map[col] = {}

        def score(role, sc, reason):
            scores[col][role] = scores[col].get(role, 0) + sc
            if role not in reasons_map[col]:
                reasons_map[col][role] = []
            reasons_map[col][role].append(reason)

        # transaction_date
        for kw in DATE_KW:
            if kw in nl:
                score("transaction_date", 50, f"컬럼명에 '{kw}'")
                break
        if "datetime" in dtype:
            score("transaction_date", 40, "datetime 타입")
        elif sample.size > 0:
            s0 = str(sample.iloc[0]).strip()
            if len(s0) == 8 and s0.isdigit() and 19000101 <= int(s0) <= 21001231:
                score("transaction_date", 40, "YYYYMMDD 형식")
            elif re.search(r'\d{4}[-/]\d{2}[-/]\d{2}', s0):
                score("transaction_date", 35, "YYYY-MM-DD 형식")
            else:
                try:
                    pd.to_datetime(sample.head(5), errors='raise')
                    score("transaction_date", 20, "날짜 파싱 가능")
                except Exception:
                    pass

        # sales_amount
        for kw in SALES_KW:
            if kw in nl:
                score("sales_amount", 50, f"컬럼명에 '{kw}'")
                break
        if any(d in dtype for d in ["int", "float"]):
            score("sales_amount", 30, "숫자형")
            num = pd.to_numeric(sample, errors="coerce")
            if num.size > 0 and (num > 0).mean() > 0.7:
                score("sales_amount", 15, f"양수 {(num > 0).mean() * 100:.0f}%")
        elif sample.size > 0:
            num = pd.to_numeric(sample, errors="coerce")
            if num.notna().mean() > 0.8:
                score("sales_amount", 15, "숫자로 변환 가능")

        # number_of_tx
        for kw in TX_KW:
            if kw in nl:
                score("number_of_tx", 55, f"컬럼명에 '{kw}'")
                break
        if any(d in dtype for d in ["int"]):
            score("number_of_tx", 20, "정수형")

        # quantity
        for kw in QTY_KW:
            if kw in nl:
                score("quantity", 60, f"컬럼명에 '{kw}'")
                break

        # sku_name
        for kw in SKU_KW:
            if kw in nl:
                score("sku_name", 60, f"컬럼명에 '{kw}'")
                break

        # category_name
        for kw in CAT_KW:
            if kw in nl:
                score("category_name", 60, f"컬럼명에 '{kw}'")
                break
        if "object" in dtype:
            n_uniq = df[col].nunique()
            if 2 <= n_uniq <= 30:
                score("category_name", 15, f"고유값 {n_uniq}개 (카테고리 범위)")

        # company_name
        for kw in COMP_KW:
            if kw in nl:
                score("company_name", 55, f"컬럼명에 '{kw}'")
                break
        if "object" in dtype:
            n_uniq = df[col].nunique()
            if 2 <= n_uniq <= 1000:
                score("company_name", 20, f"문자열, {n_uniq}개 고유값")

        # brand_name
        for kw in BRAND_KW:
            if kw in nl:
                score("brand_name", 60, f"컬럼명에 '{kw}'")
                break

        # stock_code
        for kw in STOCK_KW:
            if kw in nl:
                score("stock_code", 50, f"컬럼명에 '{kw}'")
                break
        if sample.size > 0:
            six = s_str.str.match(r'^\d{5,6}$').mean()
            if six > 0.3:
                score("stock_code", 45, f"5-6자리 숫자코드 {six * 100:.0f}%")

        # security_code (ISIN)
        for kw in SEC_KW:
            if kw in nl:
                score("security_code", 50, f"컬럼명에 '{kw}'")
                break
        if sample.size > 0:
            isin = s_str.str.match(r'^[A-Z]{2}\d{10}$').mean()
            kr = s_str.str.startswith("KR").mean()
            if isin > 0.3:
                score("security_code", 45, f"ISIN 형식 {isin * 100:.0f}%")
            elif kr > 0.3:
                score("security_code", 30, f"KR 접두사 {kr * 100:.0f}%")

    # Greedy assignment: pick best role per column, no duplicate roles
    col_bests = []
    for col in df.columns:
        sc_dict = scores[col]
        if not sc_dict:
            col_bests.append((col, "unknown", 0))
            continue
        best_role = max(sc_dict, key=sc_dict.get)
        col_bests.append((col, best_role, sc_dict[best_role]))
    col_bests.sort(key=lambda x: -x[2])

    assigned_roles = set()
    col_role = {}
    for col, role, sc in col_bests:
        if role not in assigned_roles:
            col_role[col] = (role, sc)
            assigned_roles.add(role)
        else:
            col_role[col] = ("unknown", 0)

    result = []
    for col in df.columns:
        role, sc = col_role.get(col, ("unknown", 0))
        reasons = reasons_map[col].get(role, [])
        conf = min(100, sc)
        result.append({
            "column_name": col,
            "inferred_role": role,
            "confidence": conf,
            "reason": " / ".join(reasons) if reasons else "패턴 없음",
        })

    return result
