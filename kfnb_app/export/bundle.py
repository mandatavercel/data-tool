"""
kfnb_app/export/bundle.py — 글로벌 배포용 딜리버리 패키지 생성.

최종 상품은 단일 파일이 아니라 묶음:
    data/      daily_sales_en.csv(옵션) + company/brand/sku/category master + data_coverage
    qc/        qc_summary / unmapped_items / outlier_report / mapping_coverage
    docs/      README.md / data_dictionary.csv
    metadata/  version.json
    <label>_summary.xlsx
build_delivery_package() 가 위 전부를 만들어 zip 으로 묶는다.
"""
from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd

from kfnb_app.ingest.dataio import Source
from kfnb_app.mapping import mastering


def write_master_csvs(out_dir: str | Path, *, sku_master: pd.DataFrame,
                      src: Optional[Source] = None, sector: Optional[str] = None,
                      include_daily: bool = False,
                      analysis_cols: Optional[list[str]] = None,
                      id_cols: Optional[list[str]] = None,
                      lag_days=None) -> dict:
    """마스터 CSV(+옵션 daily) → {파일명: 경로}. analysis_cols/id_cols 로 SKU 컬럼 선택."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    files: dict[str, str] = {}

    def _w(name: str, df: pd.DataFrame):
        p = out / name
        df.to_csv(p, index=False, encoding="utf-8-sig")
        files[name] = str(p)

    _w("company_master.csv", mastering.build_company_master(sku_master))
    _w("brand_master.csv", mastering.build_brand_master(sku_master))
    _w("sku_master.csv", mastering.build_sku_master_file(sku_master, analysis_cols, id_cols))
    _w("category_master.csv", mastering.build_category_master(sku_master))
    _w("mapping_quality.csv", mastering.build_mapping_quality(sku_master))

    if include_daily and src is not None:
        keys = sku_master[["barcode", "sku_id", "sku_name_en", "brand_id",
                           "brand_name_en", "company_en_official", "isin"]].copy()
        p = out / "daily_sales_en.csv"
        src.export_daily_en(keys, str(p), sector, lag_days=lag_days)
        files["daily_sales_en.csv"] = str(p)
    return files


def write_tables(out_dir: str | Path, tables: dict) -> dict:
    """{이름: DataFrame} → CSV 들. 반환 {파일명: 경로}."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    files: dict[str, str] = {}
    for name, df in tables.items():
        p = out / f"{name}.csv"
        df.to_csv(p, index=False, encoding="utf-8-sig")
        files[f"{name}.csv"] = str(p)
    return files


def make_zip(zip_path: str | Path, arcmap: dict) -> str:
    """{arcname: filepath} → zip."""
    zip_path = str(zip_path)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for arc, path in arcmap.items():
            if path and Path(path).exists():
                z.write(path, arcname=arc)
    return zip_path


def build_delivery_package(out_dir: str | Path, *, profile: dict,
                           sku_master: pd.DataFrame, monthly_panel: pd.DataFrame,
                           annual_company: pd.DataFrame,
                           brand_trend: Optional[pd.DataFrame],
                           mapping_report: dict, brand_master: pd.DataFrame,
                           qc_result: dict, coverage_summary: dict,
                           source_name: str, sector_label: str, focus_brand: str,
                           products: list[str], sector: Optional[str] = None,
                           src: Optional[Source] = None,
                           include_daily: bool = False,
                           use_cases: Optional[pd.DataFrame] = None,
                           usecase_report: str = "",
                           alpha_returns: Optional[pd.DataFrame] = None,
                           alpha_revenue: Optional[pd.DataFrame] = None,
                           alpha_report: str = "",
                           pit_panel: Optional[pd.DataFrame] = None,
                           data_spec=None,
                           analysis_cols: Optional[list[str]] = None,
                           id_cols: Optional[list[str]] = None) -> dict:
    """투자기관용 딜리버리 패키지(zip) 전체 생성."""
    from kfnb_app.export import docs, workbook  # 지연 import (순환 방지)
    from kfnb_app import config as _cfg

    products = products or ["quant", "fundamental", "vendor"]
    if data_spec is None:
        data_spec = _cfg.DATA_SPEC_DEFAULT
    _lag = getattr(data_spec, "release_lag_days", None)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1) xlsx 요약
    xlsx = out / f"{sector_label}_summary.xlsx"
    info = workbook.build_workbook(
        xlsx, profile=profile, sku_master=sku_master, monthly_panel=monthly_panel,
        annual_company=annual_company, brand_trend=brand_trend,
        mapping_report=mapping_report, source_name=source_name,
        sector_label=sector_label, focus_brand=focus_brand, products=products,
        brand_master=brand_master, use_cases=use_cases,
        analysis_cols=analysis_cols, id_cols=id_cols)

    # 2) data/
    mfiles = write_master_csvs(out / "data", sku_master=sku_master, src=src,
                               sector=sector, include_daily=include_daily,
                               analysis_cols=analysis_cols, id_cols=id_cols,
                               lag_days=_lag)
    # data_coverage.csv
    cov_df = pd.DataFrame([coverage_summary])
    cov_p = out / "data" / "data_coverage.csv"
    cov_df.to_csv(cov_p, index=False, encoding="utf-8-sig")
    mfiles["data_coverage.csv"] = str(cov_p)

    # 3) qc/
    qcfiles = write_tables(out / "qc", qc_result.get("tables", {}))

    # 3b) insight/ — use-case 시그널 + 내러티브
    insightfiles: dict[str, str] = {}
    if use_cases is not None and not use_cases.empty:
        ins = out / "insight"
        ins.mkdir(parents=True, exist_ok=True)
        uc_p = ins / "use_cases.csv"
        use_cases.to_csv(uc_p, index=False, encoding="utf-8-sig")
        insightfiles["use_cases.csv"] = str(uc_p)
        if usecase_report:
            rp = ins / "use_cases_report.md"
            rp.write_text(usecase_report, encoding="utf-8")
            insightfiles["use_cases_report.md"] = str(rp)

    # 3c) insight/ — 알파 리서치
    if (alpha_returns is not None and not alpha_returns.empty) or \
            (alpha_revenue is not None and not alpha_revenue.empty):
        ins = out / "insight"
        ins.mkdir(parents=True, exist_ok=True)
        if alpha_returns is not None and not alpha_returns.empty:
            p = ins / "alpha_signals_price.csv"
            alpha_returns.to_csv(p, index=False, encoding="utf-8-sig")
            insightfiles["alpha_signals_price.csv"] = str(p)
        if alpha_revenue is not None and not alpha_revenue.empty:
            p = ins / "alpha_revenue_leadlag.csv"
            alpha_revenue.to_csv(p, index=False, encoding="utf-8-sig")
            insightfiles["alpha_revenue_leadlag.csv"] = str(p)
        if alpha_report:
            p = ins / "alpha_report.md"
            p.write_text(alpha_report, encoding="utf-8")
            insightfiles["alpha_report.md"] = str(p)

    # 3d) insight/ — PIT walk-forward 시그널 패널 (백테스트용)
    if pit_panel is not None and not pit_panel.empty:
        ins = out / "insight"
        ins.mkdir(parents=True, exist_ok=True)
        p = ins / "pit_signal_panel.csv"
        pit_panel.to_csv(p, index=False, encoding="utf-8-sig")
        insightfiles["pit_signal_panel.csv"] = str(p)

    # 4) docs/ + metadata/
    docfiles = docs.write_docs(out / "docs", source_name=source_name,
                               sector_label=sector_label, profile=profile,
                               coverage=coverage_summary, qc_result=qc_result,
                               products=products)
    verfiles = docs.write_version(out / "metadata", source_name=source_name,
                                  sector_label=sector_label,
                                  rows=profile.get("summary", {}).get("rows", 0),
                                  products=products)
    # 투자 적합성 진단 (솔직한 한계 포함) — DataSpec 근거
    from kfnb_app.insight import assessment as _assess
    from kfnb_app.insight import conclusion as _concl
    from kfnb_app.insight import investor_qa as _qa
    a = _assess.build_assessment(
        profile=profile, sku_master=sku_master, monthly_panel=monthly_panel,
        coverage=coverage_summary, qc_result=qc_result, source_name=source_name,
        alpha_returns=alpha_returns, pit_panel=pit_panel, spec=data_spec,
        sector_label=sector_label)
    ir = out / "docs" / "investor_readiness.md"
    ir.write_text(_assess.assessment_markdown(a, sector_label), encoding="utf-8")
    docfiles["docs/investor_readiness.md"] = str(ir)

    # 거짓 없는 결론 (백테스트/라이브 필요충분조건)
    c = _concl.build_conclusion(
        spec=data_spec, profile=profile, coverage=coverage_summary,
        sku_master=sku_master, monthly_panel=monthly_panel, pit_panel=pit_panel,
        alpha_returns=alpha_returns, sector_label=sector_label)
    cp = out / "docs" / "conclusion.md"
    cp.write_text(_concl.conclusion_markdown(c, sector_label), encoding="utf-8")
    docfiles["docs/conclusion.md"] = str(cp)

    # 투자기관 DDQ (Q&A)
    qa = _qa.build_qa(
        spec=data_spec, profile=profile, coverage=coverage_summary,
        sku_master=sku_master, monthly_panel=monthly_panel, pit_panel=pit_panel,
        alpha_returns=alpha_returns, qc_result=qc_result, sector_label=sector_label)
    qp = out / "docs" / "investor_qa.md"
    qp.write_text(_qa.qa_markdown(qa, sector_label), encoding="utf-8")
    docfiles["docs/investor_qa.md"] = str(qp)

    # 5) arcmap + zip
    arc: dict[str, str] = {xlsx.name: str(xlsx)}
    for n, p in mfiles.items():
        arc[f"data/{n}"] = p
    for n, p in qcfiles.items():
        arc[f"qc/{n}"] = p
    for n, p in insightfiles.items():
        arc[f"insight/{n}"] = p
    arc.update(docfiles)   # docs/ 프리픽스 포함
    arc.update(verfiles)   # metadata/ 프리픽스 포함
    zip_path = out / f"{sector_label}_POS_ALPHA_DATASET.zip"
    make_zip(zip_path, arc)

    return {
        "xlsx": str(xlsx),
        "sheets": info["sheets"],
        "customers": info.get("customers", products),
        "zip": str(zip_path),
        "files": sorted(arc.keys()),
    }
