"""
kfnb_app/pipeline.py — 스테이지 오케스트레이터.

각 단계를 순서대로 실행하고 검증 게이트를 통과하는지 확인한다. 게이트에서
HALT severity(error/critical)가 나오면 기본적으로 중단(strict)하되, UI 에서
사람이 검수 후 강제 진행할 수 있도록 allow_halt 플래그를 받는다.

헤드리스 전체 실행:
    result = run_pipeline("raw.csv", out_xlsx="product.xlsx", sector="면류")

UI 는 run_stage() 를 단계별로 호출해 중간 산출물/검증을 화면에 표시한다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from kfnb_app import config, panel, validation
from kfnb_app.ingest import dataio
from kfnb_app.profiling import profiler as profile
from kfnb_app.standardization import normalize, tagging
from kfnb_app.mapping import company as mapping, coverage, mastering
from kfnb_app.insight import alpha, pit, usecase
from kfnb_app.config import DataSpec, DATA_SPEC_DEFAULT
from kfnb_app.ingest import disclosures, prices as price_src, schema_mapper
from kfnb_app.qc import checks as qc
from kfnb_app.export import bundle


STAGES = ["profile", "normalize", "tagging", "mapping", "mastering",
          "panel", "usecase", "qc", "export"]
STAGE_LABELS = {
    "profile": "① 프로파일링",
    "normalize": "② 정규화",
    "tagging": "③ 투자 태깅",
    "mapping": "④ 티커 매핑",
    "mastering": "⑤ 영문 마스터링",
    "panel": "⑥ 패널 집계",
    "usecase": "⑦ Use-case 발굴",
    "alpha": "🔬 알파 리서치(주가/공시)",
    "qc": "⑧ 품질관리(QC)",
    "export": "⑨ 데이터 상품 생성",
}


@dataclass
class StageResult:
    name: str
    validation: dict                 # {checks, max_severity, halt}
    artifacts: dict = field(default_factory=dict)

    @property
    def halt(self) -> bool:
        return bool(self.validation.get("halt"))

    @property
    def severity(self) -> str:
        return self.validation.get("max_severity", "ok")


@dataclass
class PipelineState:
    """단계 간 전달되는 누적 상태."""
    src: Optional[dataio.Source] = None
    sector: Optional[str] = None          # cat_l2 필터 (예: '면류'); None=전체
    focus_brand: str = ""
    source_name: str = "(원천 미상)"
    sector_label: str = "K-F&B"
    # 산출물
    profile: dict = field(default_factory=dict)
    sku_master: Optional[pd.DataFrame] = None
    coverage: dict = field(default_factory=dict)
    map_report: dict = field(default_factory=dict)
    monthly_panel: Optional[pd.DataFrame] = None
    annual_company: Optional[pd.DataFrame] = None
    brand_trend: Optional[pd.DataFrame] = None
    pit_panel: Optional[pd.DataFrame] = None   # PIT walk-forward 시그널 패널
    products: Optional[list] = None        # 고객유형 (None=전체)
    brand_master: Optional[pd.DataFrame] = None
    master_summary: dict = field(default_factory=dict)
    use_cases: Optional[pd.DataFrame] = None
    usecase_report: str = ""
    analysis_cols: Optional[list] = None   # SKU 분석 컬럼 선택 (None=전체)
    id_cols: Optional[list] = None         # 식별자 컬럼 선택 (None=기본 ISIN+티커)
    data_spec: object = None                # 데이터 출처 명세(DataSpec)
    run_alpha: bool = False                 # 알파 리서치(주가/공시) 실행
    dart_api_key: str = ""
    company_overlay: dict = field(default_factory=dict)  # DART 자동해석 {회사명: CompanyRef}
    dart_note: str = ""                     # DART 해석 결과 설명(UI 제안)
    alpha_returns: Optional[pd.DataFrame] = None
    alpha_revenue: Optional[pd.DataFrame] = None
    alpha_report: str = ""
    qc_result: dict = field(default_factory=dict)
    include_daily: bool = False            # daily_sales_en.csv 포함 여부
    export_info: dict = field(default_factory=dict)


# ── 개별 스테이지 ──────────────────────────────────────────────────────────
def stage_profile(st: PipelineState) -> StageResult:
    st.profile = profile.build_profile(st.src)
    val = validation.validate_profile(st.profile, st.profile["canonical_cols"])
    return StageResult("profile", val, {"profile": st.profile})


def stage_normalize(st: PipelineState) -> StageResult:
    skus = st.src.distinct_skus()
    if st.sector:
        skus = skus[skus["cat_l2"] == st.sector].reset_index(drop=True)
    st.sku_master = normalize.normalize_skus(skus)
    val = validation.validate_normalize(st.sku_master)
    return StageResult("normalize", val, {"n_skus": len(st.sku_master)})


def stage_tagging(st: PipelineState) -> StageResult:
    st.sku_master = tagging.tag_skus(st.sku_master)
    st.coverage = tagging.theme_coverage(st.sku_master)
    val = validation.validate_tagging(st.coverage)
    return StageResult("tagging", val, {"coverage": st.coverage})


def stage_mapping(st: PipelineState) -> StageResult:
    # DART 자동해석: 종목코드 + 공식 영문명을 공시 기준으로 자동 보강(graceful)
    names = sorted(set(str(c) for c in st.sku_master["company_kr"].dropna()))
    try:
        from kfnb_app.ingest import dart_company
        hints = {n: config.COMPANY_MAP[n].krx_code for n in names
                 if n in config.COMPANY_MAP and config.COMPANY_MAP[n].krx_code}
        resolved, note = dart_company.resolve(names, st.dart_api_key, code_hints=hints)
        st.company_overlay = mapping.dart_overlay(resolved) if resolved else {}
        st.dart_note = note
    except Exception as e:                       # noqa: BLE001 — 비차단
        st.company_overlay, st.dart_note = {}, f"DART 자동해석 생략: {type(e).__name__}"
    ov = st.company_overlay or None
    st.sku_master = mapping.map_companies(st.sku_master, extra_map=ov)
    st.map_report = mapping.mapping_report(st.sku_master, extra_map=ov)
    st.map_report["dart_note"] = st.dart_note
    st.map_report["dart_resolved"] = sorted(st.company_overlay.keys())
    val = validation.validate_mapping(st.map_report)
    return StageResult("mapping", val, {"report": st.map_report})


def stage_mastering(st: PipelineState) -> StageResult:
    st.sku_master = mastering.enrich_sku_master(st.sku_master)
    st.brand_master = mastering.build_brand_master(st.sku_master)
    st.master_summary = mastering.mastering_summary(st.sku_master)
    val = validation.validate_mastering(st.master_summary)
    return StageResult("mastering", val, {"summary": st.master_summary})


def stage_panel(st: PipelineState) -> StageResult:
    st.monthly_panel = panel.build_monthly_panel(st.src, st.sector)
    st.annual_company = panel.build_annual_company(st.src, st.sector)
    if st.focus_brand:
        st.brand_trend = panel.build_brand_trend(st.src, st.focus_brand)
    _lag = getattr(st.data_spec, "release_lag_days", None)
    st.pit_panel = pit.build_pit_panel(st.monthly_panel, lag_days=_lag)  # PIT 패널
    t = config.THRESHOLDS
    outliers = panel.asp_outliers(st.monthly_panel, t.asp_min_won, t.asp_max_won)
    val = validation.validate_panel(st.monthly_panel, outliers)
    return StageResult("panel", val, {"n_rows": len(st.monthly_panel),
                                      "pit_rows": len(st.pit_panel)})


def stage_usecase(st: PipelineState) -> StageResult:
    st.use_cases = usecase.generate(st.monthly_panel, st.annual_company,
                                    st.sku_master)
    st.usecase_report = usecase.narrative(st.use_cases, st.sector_label)
    n = len(st.use_cases)
    by = (st.use_cases["usecase_type"].value_counts().to_dict() if n else {})
    val = {"checks": [{"label": "발굴 시그널", "severity": "ok" if n else "warning",
                       "detail": f"{n}건 · {by}"}],
           "max_severity": "ok" if n else "warning", "halt": False}
    return StageResult("usecase", val, {"n": n, "by_type": by})


def stage_alpha(st: PipelineState) -> StageResult:
    """알파 리서치 — 주가 상관/선행성 + 공시매출 lead-lag (외부 데이터)."""
    codes = sorted({str(c) for c in st.monthly_panel.get("krx_code", [])
                    if str(c)}) if st.monthly_panel is not None else []
    name_map = {ref.krx_code: ref.company_en_official
                for ref in config.COMPANY_MAP.values() if ref.krx_code}
    px, pnote = price_src.monthly_prices(codes)
    st.alpha_returns = alpha.research_vs_returns(st.monthly_panel, px)
    rev, rnote = disclosures.quarterly_revenue(codes, st.dart_api_key)
    st.alpha_revenue = alpha.research_vs_revenue(st.monthly_panel, rev)
    st.alpha_report = alpha.alpha_report(st.alpha_returns, st.alpha_revenue,
                                         name_map, st.sector_label)
    n = len(st.alpha_returns) + len(st.alpha_revenue)
    sev = "ok" if n else "warning"
    val = {"checks": [
        {"label": "주가 데이터", "severity": "info", "detail": pnote},
        {"label": "공시매출", "severity": "info", "detail": rnote},
        {"label": "알파 시그널", "severity": sev, "detail": f"{n}건"}],
        "max_severity": sev, "halt": False}
    return StageResult("alpha", val, {"n": n})


def stage_qc(st: PipelineState) -> StageResult:
    st.qc_result = qc.run_qc(st.sku_master, st.monthly_panel, st.profile)
    val = {"checks": st.qc_result["checks"],
           "max_severity": st.qc_result["max_severity"],
           "halt": st.qc_result["halt"]}
    return StageResult("qc", val, {"summary": st.qc_result.get("summary", {})})


def stage_export(st: PipelineState, out_xlsx: Union[str, Path]) -> StageResult:
    out_dir = Path(out_xlsx).parent
    sec = None if (st.sector in (None, "(전체)")) else st.sector
    cov_sum = coverage.coverage_by_sales(st.sku_master)
    pkg = bundle.build_delivery_package(
        out_dir, profile=st.profile, sku_master=st.sku_master,
        monthly_panel=st.monthly_panel, annual_company=st.annual_company,
        brand_trend=st.brand_trend, mapping_report=st.map_report,
        brand_master=st.brand_master, qc_result=st.qc_result,
        coverage_summary=cov_sum, use_cases=st.use_cases,
        usecase_report=st.usecase_report, alpha_returns=st.alpha_returns,
        alpha_revenue=st.alpha_revenue, alpha_report=st.alpha_report,
        pit_panel=st.pit_panel, data_spec=st.data_spec, source_name=st.source_name,
        sector_label=st.sector_label, focus_brand=st.focus_brand,
        products=st.products, sector=sec,
        src=(st.src if st.include_daily else None),
        include_daily=st.include_daily, analysis_cols=st.analysis_cols,
        id_cols=st.id_cols)
    info = {"path": pkg["xlsx"], "sheets": pkg["sheets"],
            "customers": pkg["customers"], "bundle_zip": pkg["zip"],
            "files": pkg["files"]}
    st.export_info = info
    val = validation.validate_export(formula_errors=0, sheets=info["sheets"])
    return StageResult("export", val, info)


# ── 헤드리스 전체 실행 ─────────────────────────────────────────────────────
def run_pipeline(src: Union[str, Path, pd.DataFrame],
                 out_xlsx: Union[str, Path],
                 *, sector: Optional[str] = "면류",
                 focus_brand: str = "불닭볶음면",
                 sector_label: str = "K-F&B",
                 source_name: Optional[str] = None,
                 products: Optional[list] = None,
                 include_daily: bool = False,
                 analysis_cols: Optional[list] = None,
                 id_cols: Optional[list] = None,
                 data_spec: object = None,
                 run_alpha: bool = False,
                 dart_api_key: str = "",
                 strict: bool = True,
                 prefer_duckdb: bool = True) -> dict:
    """전체 파이프라인 실행 → {ok, stages:[StageResult], export}.

    strict=True 면 검증 게이트(error/critical)에서 중단하고 ok=False 로 반환.
    strict=False 면 경고를 무시하고 끝까지 진행(사람이 검수 결정한 경우).
    """
    if source_name is None:
        source_name = Path(str(src)).name if not isinstance(src, pd.DataFrame) else "(DataFrame)"

    _src = dataio.open_source(src, prefer_duckdb=prefer_duckdb)
    # 스키마 사전 검증 — 임의 데이터셋이 와도 binder 에러 대신 명확히 중단
    _missing = schema_mapper.missing_required(getattr(_src, "raw_columns", []))
    if _missing:
        res = StageResult("schema", {
            "checks": [{"label": "필수 표준컬럼", "severity": "critical",
                        "detail": f"누락: {', '.join(_missing)} — owner_schema_mapping.yaml "
                                  "에 매핑 추가 필요 (POS 브랜드/SKU 구조 아님일 수 있음)"}],
            "max_severity": "critical", "halt": True}, {"missing": _missing})
        return {"ok": False, "halted_at": "schema", "stages": [res], "export": {}}

    st = PipelineState(
        src=_src,
        sector=sector, focus_brand=focus_brand,
        sector_label=sector_label, source_name=source_name,
        products=products, include_daily=include_daily,
        analysis_cols=analysis_cols, id_cols=id_cols,
        data_spec=(data_spec or DATA_SPEC_DEFAULT),
        run_alpha=run_alpha, dart_api_key=dart_api_key)

    results: list[StageResult] = []

    def _run(fn, *a):
        res = fn(st, *a)
        results.append(res)
        return res

    seq = list(STAGES)
    if run_alpha and "alpha" not in seq:
        seq.insert(seq.index("qc"), "alpha")    # usecase 다음, qc 앞
    for name in seq:
        if name == "profile":
            res = _run(stage_profile)
        elif name == "normalize":
            res = _run(stage_normalize)
        elif name == "tagging":
            res = _run(stage_tagging)
        elif name == "mapping":
            res = _run(stage_mapping)
        elif name == "mastering":
            res = _run(stage_mastering)
        elif name == "panel":
            res = _run(stage_panel)
        elif name == "usecase":
            res = _run(stage_usecase)
        elif name == "alpha":
            res = _run(stage_alpha)
        elif name == "qc":
            res = _run(stage_qc)
        elif name == "export":
            res = _run(stage_export, out_xlsx)
        if strict and res.halt:
            return {"ok": False, "halted_at": name, "stages": results,
                    "export": st.export_info}

    return {"ok": True, "halted_at": None, "stages": results,
            "export": st.export_info}
