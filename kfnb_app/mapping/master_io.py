"""
kfnb_app/mapping/master_io.py — 마스터 파일 내보내기/불러오기 (큐레이션 누적).

작업(회사·브랜드·SKU·카테고리 영문 확정)을 마스터 묶음(zip)으로 내려받고, 다음에
그 zip 을 올리면 자동으로 매핑이 적용되게 한다. 한 번 확정한 것은 다시 안 해도 됨.

번들 구성(zip):
  company_master.csv   company_kr, krx_code, company_en, isin
  brand_master.csv     company_kr, brand_kr, brand_id, brand_en
  category_master.csv  category_ko, category_en
  sku_master.csv       barcode, sku_id, sku_name_en
streamlit 비의존.
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd

from kfnb_app import config


def build_bundle(sku_master: pd.DataFrame) -> dict:
    """현재 sku_master(확정 영문·식별자 반영) → 마스터 DataFrame 묶음."""
    sm = sku_master.copy()
    sm["barcode"] = sm.get("barcode", "").astype(str)
    company = (sm.drop_duplicates("company_kr")[
        [c for c in ["company_kr", "krx_code", "company_en_official", "isin", "jurir_no"]
         if c in sm.columns]].rename(columns={"company_en_official": "company_en"}))
    brand = (sm.drop_duplicates(["company_kr", "brand_kr"])[
        [c for c in ["company_kr", "brand_kr", "brand_id", "brand_name_en"]
         if c in sm.columns]].rename(columns={"brand_name_en": "brand_en"}))
    # 카테고리(대/중/소) ko→en 합치기
    cat_rows = []
    for ko, en in [("cat_l1", "cat_l1_en"), ("cat_l2", "cat_l2_en"),
                   ("cat_l3", "cat_l3_en")]:
        if ko in sm.columns and en in sm.columns:
            for _, r in sm.drop_duplicates(ko)[[ko, en]].iterrows():
                kv = str(r[ko] or "").strip()
                if kv and kv not in ("(unknown)", "Uncategorized"):
                    cat_rows.append({"category_ko": kv, "category_en": str(r[en] or "")})
    category = pd.DataFrame(cat_rows).drop_duplicates("category_ko") if cat_rows \
        else pd.DataFrame(columns=["category_ko", "category_en"])
    sku = (sm.drop_duplicates("barcode")[
        [c for c in ["barcode", "sku_id", "sku_name_en"] if c in sm.columns]])
    return {"company_master": company, "brand_master": brand,
            "category_master": category, "sku_master": sku}


def write_zip(out_path: str | Path, bundle: dict) -> str:
    out_path = str(out_path)
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as z:
        for name, df in bundle.items():
            z.writestr(f"{name}.csv", df.to_csv(index=False, encoding="utf-8-sig"))
    return out_path


def load_zip(data: bytes | str | Path) -> dict:
    """zip(바이트/경로) → {name: DataFrame}. CSV 단일도 허용(파일명으로 판정)."""
    if isinstance(data, (str, Path)):
        raw = Path(data).read_bytes()
    else:
        raw = data
    out = {}
    try:
        z = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        return out
    for n in z.namelist():
        if not n.lower().endswith(".csv"):
            continue
        key = Path(n).stem
        try:
            out[key] = pd.read_csv(io.BytesIO(z.read(n)), dtype=str)
        except Exception:                          # noqa: BLE001
            continue
    return out


def to_overrides(bundle: dict) -> dict:
    """마스터 묶음 → 적용용 override dict."""
    ov = {"company": {}, "brand": {}, "category": {}, "sku": {}}
    cm = bundle.get("company_master")
    if cm is not None:
        for _, r in cm.iterrows():
            ov["company"][str(r.get("company_kr", "")).strip()] = {
                "krx": str(r.get("krx_code", "") or "").strip(),
                "en": str(r.get("company_en", "") or "").strip(),
                "jurir_no": str(r.get("jurir_no", "") or "").strip()}
    bm = bundle.get("brand_master")
    if bm is not None:
        for _, r in bm.iterrows():
            ov["brand"][(str(r.get("company_kr", "")).strip(),
                         str(r.get("brand_kr", "")).strip())] = str(
                r.get("brand_en", "") or "").strip()
    ca = bundle.get("category_master")
    if ca is not None:
        for _, r in ca.iterrows():
            k = str(r.get("category_ko", "")).strip()
            if k:
                ov["category"][k] = str(r.get("category_en", "") or "").strip()
    sk = bundle.get("sku_master")
    if sk is not None:
        for _, r in sk.iterrows():
            bc = str(r.get("barcode", "")).strip()
            if bc:
                ov["sku"][bc] = str(r.get("sku_name_en", "") or "").strip()
    return ov


def company_overlay(ov: dict) -> dict:
    """업로드 마스터의 회사 override → {회사명: CompanyRef} (DART보다 우선)."""
    out = {}
    for co, d in (ov.get("company") or {}).items():
        krx = str(d.get("krx", "") or "").strip()
        en = str(d.get("en", "") or "").strip()
        if not co or (not krx and not en):
            continue
        base = config.COMPANY_MAP.get(co)
        out[co] = config.CompanyRef(
            company_en=(en or (base.company_en if base else co)),
            krx_code=krx or (base.krx_code if base else ""), listed=bool(krx),
            slug=(base.slug if base else None) or _slug(co),
            company_en_official=(en or (base.company_en_official if base else "")),
            gics_sub_code=(base.gics_sub_code if base else ""),
            gics_sub_name=(base.gics_sub_name if base else ""),
            gics_sector=(base.gics_sector if base else ""),
            note="업로드 마스터")
    return out


def _slug(name: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9]+", "_", str(name)).upper().strip("_") or "CO"


def apply_overrides(sku_master: pd.DataFrame, ov: dict) -> pd.DataFrame:
    """enrich 된 sku_master 에 업로드 마스터를 적용(회사 영문/코드·브랜드·카테고리·SKU)."""
    if not ov:
        return sku_master
    sm = sku_master.copy()
    # 회사 영문/코드
    for co, d in (ov.get("company") or {}).items():
        m = sm["company_kr"] == co
        if not m.any():
            continue
        if d.get("en"):
            sm.loc[m, "company_en_official"] = d["en"]
        if d.get("jurir_no"):
            if "jurir_no" not in sm.columns:
                sm["jurir_no"] = ""
            sm.loc[m, "jurir_no"] = d["jurir_no"]
        krx = str(d.get("krx", "") or "").strip()
        if krx and "krx_code" in sm.columns:
            sm.loc[m, "krx_code"] = krx
            sm.loc[m, "bbg_ticker"] = f"{krx} KS"
            sm.loc[m, "bloomberg_code"] = f"{krx} KS Equity"
            sm.loc[m, "isin"] = config._krx_isin(krx)
            sm.loc[m, "listed"] = True
    # 브랜드 영문
    if ov.get("brand") and "brand_name_en" in sm.columns:
        def _br(r):
            return (ov["brand"].get((r["company_kr"], r["brand_kr"]))
                    or r.get("brand_name_en"))
        sm["brand_name_en"] = sm.apply(_br, axis=1)
    # 카테고리 영문
    for ko, en in [("cat_l1", "cat_l1_en"), ("cat_l2", "cat_l2_en"),
                   ("cat_l3", "cat_l3_en")]:
        if ko in sm.columns and en in sm.columns and ov.get("category"):
            sm[en] = sm.apply(
                lambda r: ov["category"].get(str(r[ko]).strip(), r[en]), axis=1)
    # SKU 영문 (바코드 키)
    if ov.get("sku") and "barcode" in sm.columns and "sku_name_en" in sm.columns:
        bc = sm["barcode"].astype(str)
        sm["sku_name_en"] = [ov["sku"].get(b) or e
                             for b, e in zip(bc, sm["sku_name_en"])]
    return sm
