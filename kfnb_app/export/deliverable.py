"""
kfnb_app/export/deliverable.py — 단일 엑셀 산출물 빌더 (탭 = 레이아웃).

고객 템플릿이 매번 바뀌므로, 고정 템플릿에 끼워넣지 않고 **우리가 정의한 탭들을
순서대로** 하나의 .xlsx 로 출력한다. 탭은 추가/재정렬 가능(SHEET_ORDER).

  Information : 데이터셋 메타(기간·커버리지·채널·정의·시차 등)  [채움]
  List       : 회사/브랜드 마스터(공시명·영문·법인등록번호·ISIN·티커·GICS) [채움]
  TR_BASIC   : 거래 기본(거래일×회사×브랜드×채널 매출/수량/건수) [채움]
  DEMOGRAPHIC: 성별×연령 거래액/거래자수/건수 [헤더 — 인구통계 데이터 필요]
  RETENTION  : 재구매 코호트(ru/count/amount m0..m36) [헤더 — 개인거래 데이터 필요]
  PANEL      : 패널(DAU·매출·F&B) [헤더 — 멤버스 패널 데이터 필요]

영문 전용 옵션(기본 영문). 순수 pandas/openpyxl.
"""
from __future__ import annotations

import io
from datetime import datetime

import pandas as pd

from kfnb_app.export import template_xlsx as _tx

SHEET_ORDER = ["Information", "List", "TR_BASIC", "DEMOGRAPHIC", "RETENTION", "PANEL"]

# 데이터 미보유 시트의 표준 헤더(레이아웃 정의 — 향후 채움)
_AGE = ["u10", "20s", "30s", "40s", "50s", "60plus"]
DEMOGRAPHIC_COLS = (["date", "company", "brand", "amount_na_gender_age"]
                    + [f"{g}_{a}_amount" for g in ("M", "F") for a in _AGE]
                    + [f"{g}_{a}_buyers" for g in ("M", "F") for a in _AGE]
                    + [f"{g}_{a}_count" for g in ("M", "F") for a in _AGE])
RETENTION_COLS = (["month", "company", "brand", "cohort_basis"]
                  + [f"ru_m{i}" for i in range(37)]
                  + [f"count_m{i}" for i in range(37)]
                  + [f"amount_m{i}" for i in range(37)])
PANEL_COLS = ["date", "channel", "DAU", "sales", "FNB_DAU", "FNB_sales", "data_source"]


def build_information(meta: dict) -> pd.DataFrame:
    """key/value 메타 시트."""
    order = ["dataset", "generated_at", "period", "companies", "brands", "skus",
             "channel", "population", "amount_basis", "qty_basis", "currency",
             "release_lag_days", "brand_verified_pct", "sku_verified_pct", "rows",
             "note"]
    rows = [{"field": k, "value": meta.get(k, "")} for k in order if k in meta]
    for k, v in meta.items():                      # 나머지도 포함
        if k not in order:
            rows.append({"field": k, "value": v})
    return pd.DataFrame(rows, columns=["field", "value"])


def build_list(sku_master: pd.DataFrame, *, english: bool = True) -> pd.DataFrame:
    """회사/브랜드 마스터 리스트 (공시명·영문·법인등록번호·ISIN·티커·GICS)."""
    sm = sku_master.copy()
    g = sm.drop_duplicates(["company_kr", "brand_kr"])
    coen = _tx._co_en_map(sm)

    def col(name, default=""):
        return g[name] if name in g.columns else default
    out = pd.DataFrame({
        "company_name_ko": g["company_kr"],
        "company_name_en": g["company_kr"].map(lambda c: coen.get(str(c), "")),
        "brand_name_ko": g["brand_kr"],
        "brand_name_en": col("brand_name_en"),
        "brand_id": col("brand_id"),
        "jurir_no": col("jurir_no"),
        "isin": col("isin"),
        "ticker": col("bbg_ticker"),
        "gics_sub_industry": col("gics_sub_name"),
    })
    # 공시명/브랜드명 표기 컬럼(영문/국문) + 회사_브랜드 라벨
    nm = out["company_name_en"] if english else out["company_name_ko"]
    bn = out["brand_name_en"] if english else out["brand_name_ko"]
    out.insert(0, "disclosure_name", nm.where(nm.astype(str) != "", out["company_name_ko"]))
    out.insert(1, "brand_label",
               out["disclosure_name"].astype(str) + "_" +
               bn.where(bn.astype(str) != "", out["brand_name_ko"]).astype(str))
    return out.reset_index(drop=True)


# ── TR_BASIC 의 풍부한 원본필드(매핑 편집에서 선택 가능) ──────────────────────
TR_BASE_FIELDS = ["date", "company_ko", "company_en", "brand_ko", "brand_en",
                  "brand_label", "channel", "sales_amount", "sales_qty",
                  "sales_cnt", "buyers", "isin", "ticker"]
LIST_FIELDS = ["disclosure_name", "brand_label", "company_name_ko", "company_name_en",
               "brand_name_ko", "brand_name_en", "brand_id", "jurir_no", "isin",
               "ticker", "gics_sub_industry"]

# 기본 레이아웃(출력컬럼명 ← 원본필드). 사용자가 ⑧에서 자유 편집.
DEFAULT_LAYOUTS = {
    "List": [{"name": "공시명", "from": "disclosure_name"},
             {"name": "브랜드명", "from": "brand_label"},
             {"name": "법인등록번호", "from": "jurir_no"},
             {"name": "ISIN", "from": "isin"}, {"name": "Ticker", "from": "ticker"},
             {"name": "GICS", "from": "gics_sub_industry"}],
    "TR_BASIC": [{"name": "거래일", "from": "date"}, {"name": "회사명", "from": "company_en"},
                 {"name": "브랜드명", "from": "brand_label"}, {"name": "채널", "from": "channel"},
                 {"name": "거래금액", "from": "sales_amount"},
                 {"name": "거래수량", "from": "sales_qty"},
                 {"name": "거래건수", "from": "sales_cnt"},
                 {"name": "거래자수", "from": "buyers"}],
}


def apply_layout(df: pd.DataFrame, cols) -> pd.DataFrame:
    """[{name, from}] 레이아웃대로 컬럼 선택·리네임·정렬(없는 from 은 공란)."""
    if not cols:
        return df
    out = pd.DataFrame(index=df.index)
    for c in cols:
        frm, nm = c.get("from"), c.get("name")
        out[nm] = df[frm] if frm in df.columns else ""
    return out


# ── 엑셀 템플릿 → 탭/컬럼 플랜 (그대로 반영) ─────────────────────────────────
_SYN = {
    "List": {"공시명": "disclosure_name", "회사명": "company_name_en",
             "회사": "company_name_en", "company": "company_name_en",
             "브랜드명": "brand_label", "브랜드": "brand_label", "brand": "brand_label",
             "법인등록번호": "jurir_no", "jurir_no": "jurir_no",
             "isin": "isin", "ticker": "ticker", "종목코드": "ticker",
             "gics": "gics_sub_industry"},
    "TR_BASIC": {"거래일": "date", "거래일자": "date", "date": "date",
                 "회사명": "company_en", "회사": "company_en", "company": "company_en",
                 "브랜드명": "brand_label", "브랜드": "brand_label", "brand": "brand_label",
                 "채널": "channel", "channel": "channel",
                 "거래금액": "sales_amount", "매출": "sales_amount", "sales": "sales_amount",
                 "거래수량": "sales_qty", "수량": "sales_qty", "qty": "sales_qty",
                 "거래건수": "sales_cnt", "건수": "sales_cnt",
                 "거래자수": "buyers", "buyers": "buyers"},
}


def classify_sheet(name: str):
    """시트명 → 우리 탭 kind. dictionary 시트는 None(제외)."""
    n = str(name).lower()
    if "dictionary" in n or "사전" in n:
        return None
    if "기본매출" in name or "tr_basic" in n or "basic" in n or "sales" in n:
        return "TR_BASIC"
    if "기본정보" in name or "회사정보" in name or "list" in n:
        return "List"
    if "인구" in name or "demograph" in n:
        return "DEMOGRAPHIC"
    if "재구매" in name or "retention" in n:
        return "RETENTION"
    if "패널" in name or "panel" in n:
        return "PANEL"
    if "information" in n or ("정보" in name):
        return "Information"
    return "OTHER"


def _guess_from(header: str, kind: str) -> str:
    syn = _SYN.get(kind, {})
    h = str(header).strip()
    return syn.get(h) or syn.get(h.lower()) or ""


def _extract_headers(ws, max_scan: int = 12) -> list:
    """시트에서 헤더 행(비어있지 않은 셀이 가장 많은 상단 행) → 헤더 리스트."""
    best, best_n = [], 0
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i >= max_scan:
            break
        vals = [("" if v is None else str(v).strip()) for v in row]
        nonempty = [v for v in vals if v]
        if len(nonempty) >= 2 and len(nonempty) > best_n:
            # 트레일링 공백 제거
            last = max((j for j, v in enumerate(vals) if v), default=-1)
            best, best_n = vals[:last + 1], len(nonempty)
    return [h for h in best if h]


def plan_from_template(xlsx_bytes: bytes) -> list:
    """업로드한 xlsx → [{sheet, kind, columns:[{name, from}]}] (그대로 출력용)."""
    import io
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    plan = []
    for ws in wb.worksheets:
        kind = classify_sheet(ws.title)
        if kind is None:
            continue
        headers = _extract_headers(ws)
        if not headers:
            continue
        cols = [{"name": h, "from": _guess_from(h, kind)} for h in headers]
        plan.append({"sheet": ws.title, "kind": kind, "columns": cols})
    return plan


def build_tr_base(src, sku_master: pd.DataFrame, *, channel: str = "",
                  sector=None) -> pd.DataFrame:
    """TR_BASIC 원본필드 풀(ko/en·식별자 포함). 레이아웃으로 골라 출력."""
    d = src.daily_panel(sector).copy()
    coen = _tx._co_en_map(sku_master); bren = _tx._br_en_map(sku_master)
    sm = sku_master.drop_duplicates("company_kr").set_index("company_kr")
    iso = sm["isin"].to_dict() if "isin" in sm.columns else {}
    tic = sm["bbg_ticker"].to_dict() if "bbg_ticker" in sm.columns else {}
    cen = d["company_kr"].map(lambda c: coen.get(str(c)) or str(c))
    ben = d.apply(lambda r: bren.get((str(r["company_kr"]), str(r["brand_kr"])))
                  or str(r["brand_kr"]), axis=1)
    return pd.DataFrame({
        "date": d["date"].astype(str), "company_ko": d["company_kr"],
        "company_en": cen, "brand_ko": d["brand_kr"], "brand_en": ben,
        "brand_label": cen.astype(str) + "_" + ben.astype(str),
        "channel": channel or "", "sales_amount": d["sales_amt"],
        "sales_qty": d.get("sales_qty"), "sales_cnt": d.get("sales_cnt"),
        "buyers": "", "isin": d["company_kr"].map(lambda c: iso.get(c, "")),
        "ticker": d["company_kr"].map(lambda c: tic.get(c, ""))})


def build_deliverable(src, sku_master: pd.DataFrame, *, meta: dict,
                      channel: str = "", sector=None, english: bool = True,
                      sheet_order=None, max_tr_rows: int = 1_000_000,
                      information_df=None, layouts=None, plan=None) -> tuple[bytes, dict]:
    """탭 순서대로 채운 단일 xlsx → (bytes, 시트별 리포트).

    information_df: ⑧에서 검수·수정한 Information 시트(있으면 우선).
    plan: 업로드 엑셀 템플릿에서 추출한 [{sheet, kind, columns}] — 있으면 그 시트명·
          컬럼·순서를 **그대로** 출력(엑셀 레이아웃 그대로 반영).
    """
    if plan:
        return _build_from_plan(src, sku_master, plan=plan, meta=meta, channel=channel,
                                sector=sector, english=english, max_tr_rows=max_tr_rows,
                                information_df=information_df)
    order = sheet_order or SHEET_ORDER
    layouts = layouts or {}
    report = {}
    sheets: dict[str, pd.DataFrame] = {}
    for name in order:
        if name == "Information":
            sheets[name] = (information_df if information_df is not None
                            else build_information(meta))
            report[name] = f"{len(sheets[name])} fields"
        elif name == "List":
            base = build_list(sku_master, english=english)
            lay = layouts.get("List") or DEFAULT_LAYOUTS["List"]
            sheets[name] = apply_layout(base, lay)
            report[name] = f"{len(sheets[name])} rows"
        elif name == "TR_BASIC":
            base = build_tr_base(src, sku_master, channel=channel, sector=sector)
            lay = layouts.get("TR_BASIC") or DEFAULT_LAYOUTS["TR_BASIC"]
            df = apply_layout(base, lay)
            if len(df) > max_tr_rows:
                report[name] = f"{len(df)} rows → {max_tr_rows} 로 절단(엑셀 한도)"
                df = df.head(max_tr_rows)
            else:
                report[name] = f"{len(df)} rows"
            sheets[name] = df
        elif name in ("DEMOGRAPHIC", "RETENTION", "PANEL"):
            cols = {"DEMOGRAPHIC": DEMOGRAPHIC_COLS, "RETENTION": RETENTION_COLS,
                    "PANEL": PANEL_COLS}[name]
            lay = layouts.get(name)
            if lay:                                # 사용자가 컬럼을 정의했으면 그대로(빈 데이터)
                sheets[name] = pd.DataFrame(columns=[c["name"] for c in lay])
            else:
                sheets[name] = pd.DataFrame(columns=cols)
            report[name] = "헤더만(데이터 필요)"
        else:
            sheets[name] = pd.DataFrame()
            report[name] = "빈 시트"

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        for name in order:
            sheets[name].to_excel(xw, sheet_name=name[:31], index=False)
    return buf.getvalue(), report


def _build_from_plan(src, sku_master, *, plan, meta, channel, sector, english,
                     max_tr_rows, information_df) -> tuple[bytes, dict]:
    """업로드 템플릿 플랜대로 시트명·컬럼·순서 그대로 출력."""
    report = {}
    out_sheets = []          # (sheet_name, df)
    list_base = tr_base = None
    seen = set()
    for it in plan:
        sheet = str(it["sheet"])[:31]
        # 시트명 중복 방지(양산/백테스트 등 동일 kind 다중 허용)
        base_sheet = sheet
        k = 1
        while sheet in seen:
            sheet = f"{base_sheet[:28]}_{k}"; k += 1
        seen.add(sheet)
        kind = it.get("kind"); cols = it.get("columns") or []
        if kind == "Information":
            df = information_df if information_df is not None else build_information(meta)
        elif kind == "List":
            if list_base is None:
                list_base = build_list(sku_master, english=english)
            df = apply_layout(list_base, cols)
        elif kind == "TR_BASIC":
            if tr_base is None:
                tr_base = build_tr_base(src, sku_master, channel=channel, sector=sector)
            df = apply_layout(tr_base, cols)
            if len(df) > max_tr_rows:
                df = df.head(max_tr_rows)
        else:                                       # DEMOGRAPHIC/RETENTION/PANEL/OTHER
            df = pd.DataFrame(columns=[c["name"] for c in cols])
        out_sheets.append((sheet, df))
        report[sheet] = f"{kind} · {len(df)}행 · {len(df.columns)}열"
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        for sheet, df in out_sheets:
            df.to_excel(xw, sheet_name=sheet, index=False)
    return buf.getvalue(), report


def default_meta(profile: dict, coverage: dict, data_spec, label: str = "KFNB") -> dict:
    s = (profile or {}).get("summary", {})
    sp = data_spec
    return {
        "dataset": label,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "period": s.get("period", ""),
        "companies": s.get("companies", ""),
        "brands": s.get("brands", ""),
        "skus": s.get("skus", ""),
        "rows": s.get("rows", ""),
        "channel": getattr(sp, "channel_scope", "") if sp else "",
        "population": getattr(sp, "population", "") if sp else "",
        "amount_basis": getattr(sp, "amount_basis", "") if sp else "",
        "qty_basis": getattr(sp, "qty_basis", "") if sp else "",
        "currency": getattr(sp, "currency", "KRW") if sp else "KRW",
        "release_lag_days": getattr(sp, "release_lag_days", "") if sp else "",
        "brand_verified_pct": (coverage or {}).get("brand_verified_pct", ""),
        "sku_verified_pct": (coverage or {}).get("sku_verified_pct", ""),
    }
