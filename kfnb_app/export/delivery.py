"""
kfnb_app/export/delivery.py — 글로벌 전달용 데이터 레이아웃 자동 생성기.

투자기관 전달 사양(영문 전용 · ISIN/티커/회사ID · YoY/MoM/ASP 사전계산 ·
PIT available_date)으로 3-티어 월별 팩트 + 마스터를 만든다. 레이아웃(컬럼 순서/
이름)은 configs/delivery_layout.yaml 로 제어하므로, 전달 템플릿이 바뀌면 코드 수정
없이 YAML 만 고치면 된다.

  build_facts()   → 내부 프레임 dict (company/brand/sku 월별 + 마스터)
  render_layout() → YAML 레이아웃대로 컬럼 선택·리네임·정렬
  write_delivery()→ CSV 세트 + zip + 데이터 사전
streamlit 비의존.
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from kfnb_app import config
from kfnb_app.ingest.dataio import Source
from kfnb_app.mapping import mastering


# ── 보조: ym(YYYYMM) 산술 ─────────────────────────────────────────────────────
def _prev_year(ym: int) -> int:
    return int(ym) - 100


def _prev_month(ym: int) -> int:
    y, m = divmod(int(ym), 100)
    m -= 1
    if m == 0:
        y, m = y - 1, 12
    return y * 100 + m


def _ym_to_monthend(ym: int) -> str:
    y, m = divmod(int(ym), 100)
    return pd.Period(f"{y}-{m:02d}", "M").end_time.strftime("%Y-%m-%d")


def _add_yoy_mom(df: pd.DataFrame, keys: list[str], val: str = "sales") -> pd.DataFrame:
    """엔티티(keys)별 YoY/MoM(전년동월·전월 대비 증감률) 컬럼 추가."""
    look = {(tuple(r[k] for k in keys), int(r["ym"])): r[val]
            for _, r in df.iterrows()}

    def _chg(r, prev_fn):
        base = look.get((tuple(r[k] for k in keys), prev_fn(int(r["ym"]))))
        if base in (None, 0) or pd.isna(base):
            return np.nan
        return round(r[val] / base - 1.0, 4)
    df = df.copy()
    df["sales_yoy"] = df.apply(lambda r: _chg(r, _prev_year), axis=1)
    df["sales_mom"] = df.apply(lambda r: _chg(r, _prev_month), axis=1)
    return df


# ── 1) 내부 프레임 빌드 ───────────────────────────────────────────────────────
def build_facts(src: Source, sku_master: pd.DataFrame, *,
                sector: Optional[str] = None, lag_days: Optional[int] = None,
                analysis_cols=None, id_cols=None) -> dict:
    """3-티어 월별 팩트 + 마스터(영문 전용). 반환 {name: DataFrame}."""
    lag = int(config.THRESHOLDS.pos_release_lag_days if lag_days is None else lag_days)

    # 회사/브랜드 식별자 룩업 (sku_master 에서)
    def _col(df, c):
        return df[c] if c in df.columns else ""
    sm = sku_master.copy()
    sm["barcode"] = sm["barcode"].astype(str)
    sm["company_id"] = sm.apply(
        lambda r: (r.get("isin") or r.get("company_slug") or r.get("company_kr")), axis=1)

    co_look = (sm.drop_duplicates("company_kr")
               .set_index("company_kr")[[c for c in
                ["company_id", "isin", "bbg_ticker", "company_en_official",
                 "gics_sub_name"] if c in sm.columns]])
    br_look = (sm.drop_duplicates(["company_kr", "brand_kr"])
               .set_index(["company_kr", "brand_kr"])[[c for c in
                ["brand_id", "brand_name_en"] if c in sm.columns]])
    sku_look_cols = [c for c in ["sku_id", "sku_name_en", "brand_id", "brand_name_en",
                                 "company_id", "isin", "bbg_ticker",
                                 "company_en_official", "cat_l2_en"] if c in sm.columns]
    sku_look = sm.drop_duplicates("barcode").set_index("barcode")[sku_look_cols]

    # ── SKU 월별 ──
    skum = src.sku_monthly(sector).copy()
    skum["barcode"] = skum["barcode"].astype(str)
    skum = skum.rename(columns={"sales_amt": "sales", "sales_qty": "quantity"})
    skum = skum.join(sku_look, on="barcode")
    skum["sku_id"] = skum["sku_id"].fillna(skum["barcode"])
    skum["asp"] = np.where(pd.to_numeric(skum["quantity"], errors="coerce").fillna(0) > 0,
                           (skum["sales"] / skum["quantity"]).round(1), np.nan)
    skum = _add_yoy_mom(skum, ["sku_id"])

    # ── 브랜드 월별 (SKU 롤업) ──
    brm = (skum.groupby(["ym", "company_kr", "brand_kr"], dropna=False)
           [["sales", "quantity"]].sum().reset_index())
    brm = brm.join(br_look, on=["company_kr", "brand_kr"])
    brm = brm.join(co_look, on="company_kr")
    brm = _add_yoy_mom(brm, ["company_kr", "brand_kr"])

    # ── 회사 월별 ──
    com = (skum.groupby(["ym", "company_kr"], dropna=False)
           [["sales", "quantity"]].sum().reset_index())
    com = com.join(co_look, on="company_kr")
    com = _add_yoy_mom(com, ["company_kr"])

    # 공통 파생: date / available_date / ticker 별칭
    for d in (skum, brm, com):
        d["date"] = d["ym"].map(_ym_to_monthend)
        d["available_date"] = (pd.to_datetime(d["date"]) +
                               pd.Timedelta(days=lag)).dt.strftime("%Y-%m-%d")
        if "bbg_ticker" in d.columns:
            d["ticker"] = d["bbg_ticker"]
        d["company_name_en"] = d.get("company_en_official", "")

    # 마스터 (영문 전용 빌더 재사용)
    frames = {
        "company": com, "brand": brm, "sku": skum,
        "company_master": mastering.build_company_master(sm),
        "brand_master": mastering.build_brand_master(sm),
        "sku_master": mastering.build_sku_master_file(sm, analysis_cols, id_cols),
        "category_master": mastering.build_category_master(sm),
    }
    return frames


# ── 2) 레이아웃 렌더 ─────────────────────────────────────────────────────────
# 기본 레이아웃(파이썬 = 진실원천). PyYAML 이 있으면 configs/delivery_layout.yaml 로
# 덮어쓸 수 있다(없으면 의존성 없는 폴백 파서가 list-of-dict 를 못 읽으므로 이 기본 사용).
def _c(name, frm):
    return {"name": name, "from": frm}


DEFAULT_LAYOUT = {"files": {
    "company_sales_monthly": {"source": "company", "columns": [
        _c("date", "date"), _c("available_date", "available_date"),
        _c("company_id", "company_id"), _c("isin", "isin"), _c("ticker", "ticker"),
        _c("company_name_en", "company_name_en"),
        _c("gics_sub_industry", "gics_sub_name"),
        _c("sales", "sales"), _c("quantity", "quantity"),
        _c("sales_yoy", "sales_yoy"), _c("sales_mom", "sales_mom")]},
    "brand_sales_monthly": {"source": "brand", "columns": [
        _c("date", "date"), _c("available_date", "available_date"),
        _c("company_id", "company_id"), _c("isin", "isin"), _c("ticker", "ticker"),
        _c("company_name_en", "company_name_en"),
        _c("brand_id", "brand_id"), _c("brand_name_en", "brand_name_en"),
        _c("sales", "sales"), _c("quantity", "quantity"),
        _c("sales_yoy", "sales_yoy"), _c("sales_mom", "sales_mom")]},
    "sku_sales_monthly": {"source": "sku", "columns": [
        _c("date", "date"), _c("available_date", "available_date"),
        _c("company_id", "company_id"), _c("isin", "isin"), _c("ticker", "ticker"),
        _c("company_name_en", "company_name_en"),
        _c("brand_id", "brand_id"), _c("brand_name_en", "brand_name_en"),
        _c("sku_id", "sku_id"), _c("sku_name_en", "sku_name_en"),
        _c("category", "cat_l2_en"),
        _c("sales", "sales"), _c("quantity", "quantity"), _c("asp", "asp"),
        _c("sales_yoy", "sales_yoy"), _c("sales_mom", "sales_mom")]},
    "company_master": {"source": "company_master", "columns": []},
    "brand_master": {"source": "brand_master", "columns": []},
    "sku_master": {"source": "sku_master", "columns": []},
}}


def _valid_layout(d) -> bool:
    """YAML 이 list-of-dict columns 를 제대로 파싱했는지 검증(폴백 파서 회피)."""
    try:
        for spec in (d.get("files") or {}).values():
            for c in (spec.get("columns") or []):
                if not isinstance(c, dict) or "name" not in c:
                    return False
        return bool(d.get("files"))
    except Exception:                              # noqa: BLE001
        return False


def load_layout() -> dict:
    try:
        y = config._load_yaml("delivery_layout.yaml")
        if _valid_layout(y):
            return y
    except Exception:                              # noqa: BLE001
        pass
    return DEFAULT_LAYOUT


def render_layout(frames: dict, layout: Optional[dict] = None) -> dict:
    """YAML 레이아웃대로 {파일명: DataFrame(컬럼 선택·리네임·정렬)}."""
    layout = layout or load_layout()
    out: dict[str, pd.DataFrame] = {}
    for fname, spec in (layout.get("files") or {}).items():
        src_key = spec.get("source")
        base = frames.get(src_key)
        if base is None:
            continue
        cols = spec.get("columns") or []
        if not cols:                               # 빈 = 원본 그대로(마스터)
            out[fname] = base.copy()
            continue
        ren, order = {}, []
        df = base.copy()
        for c in cols:
            frm, name = c.get("from"), c.get("name")
            if frm not in df.columns:
                df[frm] = ""                       # 템플릿 컬럼 보장(빈값)
            ren[frm] = name
            order.append(name)
        out[fname] = df[[c["from"] for c in cols]].rename(columns=ren)[order]
    return out


# ── 3) 쓰기 + zip ─────────────────────────────────────────────────────────────
def write_delivery(out_dir: str | Path, rendered: dict, *,
                   label: str = "KFNB", zip_name: Optional[str] = None,
                   signals: Optional[dict] = None) -> dict:
    """렌더된 프레임들을 CSV 로 쓰고 zip + 데이터 사전 생성. 반환 경로 정보.

    signals: {name: DataFrame} — 알파 시그널 엔진 산출물(insight/ 하위로 동봉).
    """
    out = Path(out_dir)
    (out / "data").mkdir(parents=True, exist_ok=True)
    files: dict[str, str] = {}
    dict_rows = []
    for fname, df in rendered.items():
        p = out / "data" / f"{fname}.csv"
        df.to_csv(p, index=False, encoding="utf-8-sig")
        files[f"data/{fname}.csv"] = str(p)
        for col in df.columns:
            dict_rows.append({"file": f"{fname}.csv", "column": col,
                              "rows": len(df)})
    # 알파 시그널 엔진 산출물 (백테스트 즉시용)
    if signals:
        (out / "insight").mkdir(parents=True, exist_ok=True)
        for name, df in signals.items():
            if df is None or not len(df):
                continue
            p = out / "insight" / f"{name}.csv"
            df.to_csv(p, index=False, encoding="utf-8-sig")
            files[f"insight/{name}.csv"] = str(p)
            for col in df.columns:
                dict_rows.append({"file": f"insight/{name}.csv", "column": col,
                                  "rows": len(df)})
    # 데이터 사전
    dd = out / "data_dictionary.csv"
    pd.DataFrame(dict_rows).to_csv(dd, index=False, encoding="utf-8-sig")
    files["data_dictionary.csv"] = str(dd)

    zip_path = out / (zip_name or f"{label}_DELIVERY.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for arc, path in files.items():
            if Path(path).exists():
                z.write(path, arcname=arc)
    return {"zip": str(zip_path), "files": sorted(files.keys()),
            "tables": {k: len(v) for k, v in rendered.items()}}


def build_and_write(out_dir, src, sku_master, *, sector=None, lag_days=None,
                    label="KFNB", layout=None, analysis_cols=None, id_cols=None,
                    include_signals: bool = True) -> dict:
    """원클릭: 팩트 빌드 → 레이아웃 렌더 → (옵션)알파 시그널 엔진 → CSV/zip 작성."""
    frames = build_facts(src, sku_master, sector=sector, lag_days=lag_days,
                         analysis_cols=analysis_cols, id_cols=id_cols)
    rendered = render_layout(frames, layout)
    signals = None
    if include_signals:
        try:
            from kfnb_app.insight import signal_engine
            signals = signal_engine.run_engine(src, sku_master, sector=sector,
                                               lag_days=lag_days)
        except Exception:                          # noqa: BLE001 — 비차단
            signals = None
    return write_delivery(out_dir, rendered, label=label, signals=signals)


# ── 4) 템플릿 매칭 (샘플 헤더 → 레이아웃 제안) ────────────────────────────────
def template_columns(sample_path: str | Path) -> list[str]:
    """전달 템플릿 샘플 CSV 의 헤더(컬럼명)만 읽어 반환 — 정확 매칭의 출발점."""
    from kfnb_app.ingest import dataio
    return dataio.peek_columns(str(sample_path))
