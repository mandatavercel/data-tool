"""Tests for kfnb_app — K-F&B 데이터 상품 에이전트.

순수 함수 위주(파싱·태깅·매핑·검증) + 합성 DataFrame 으로 파이프라인 E2E.
duckdb 미설치 환경에서도 pandas 폴백으로 통과해야 한다.
"""
from pathlib import Path

import pandas as pd
import pytest

from kfnb_app import config, validation
from kfnb_app.ingest import dataio
from kfnb_app.standardization import normalize, tagging
from kfnb_app.mapping import company as mapping, mastering
from kfnb_app.pipeline import run_pipeline


# ── 합성 원천 데이터 (CU 포맷 미니) ─────────────────────────────────────────
def _raw():
    rows = [
        # date, region, co, brand, l1, l2, l3, barcode, sku, amt, qty, cnt
        (20240101, "서울", "농심", "신라면", "가공식사제품", "면류", "봉지면",
         "8801043014809", "농심)신라면", 9340, 10, 8),
        (20240101, "서울", "농심", "신라면", "가공식사제품", "면류", "봉지면",
         "8801043014830", "농심)신라면5입", 45650, 10, 9),
        (20250101, "서울", "삼양식품", "불닭볶음면", "가공식사제품", "면류", "용기면",
         "8801073210", "삼양)불닭볶음면컵", 16860, 10, 10),
        (20240101, "부산", "오뚜기", "진라면", "가공식사제품", "면류", "용기면",
         "8801045210", "오뚜기)진라면매운컵", 10250, 10, 10),
        (20240101, "서울", "듣보잡식품", "미지브랜드", "가공식사제품", "면류", "봉지면",
         "8809999999", "듣보)미지라면", 5000, 5, 5),
    ]
    cols = ["YMD_CD", "SIDO_NM", "GRP_ACNT_NM", "GRP_ITEM_NM", "LRCL_NM",
            "MDCL_NM", "SMCL_NM", "ITEM_CD", "ITEM_NM",
            "SALE_AMT", "SALE_QTY", "SALE_CNT"]
    df = pd.DataFrame([dict(zip(cols, r)) for r in rows])
    # min_rows(100) 게이트 통과용으로 복제 — 집계가 합산되므로 ASP·비중은 불변
    return pd.concat([df] * 30, ignore_index=True)


# ── normalize ───────────────────────────────────────────────────────────────
class TestNormalize:
    def test_pack_count(self):
        assert normalize.parse_pack_count("농심)신라면5입") == 5
        assert normalize.parse_pack_count("농심)신라면") == 1
        assert normalize.parse_pack_count("농심)신라면큰사발16입") == 16

    def test_strip_prefix(self):
        assert normalize.strip_company_prefix("농심)신라면5입") == "신라면5입"

    def test_package_format(self):
        assert normalize.package_format("봉지면") == "Bag"
        assert normalize.package_format("용기면") == "Cup/Bowl"
        assert normalize.package_format("", "농심)신라면큰사발") == "Big Bowl"
        assert normalize.package_format("", "농심)신라면컵") == "Cup"

    def test_normalize_adds_columns_and_asp(self):
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        out = normalize.normalize_skus(src.distinct_skus())
        assert {"pack_count", "package_format", "asp_won", "variant"} <= set(out.columns)
        # 신라면5입: 45650/10 = 4565
        row = out[out["sku_name_kr"] == "농심)신라면5입"].iloc[0]
        assert row["pack_count"] == 5
        assert row["asp_won"] == 4565


# ── tagging ─────────────────────────────────────────────────────────────────
class TestTagging:
    def test_spicy_stirfried_tags(self):
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        tagged = tagging.tag_skus(normalize.normalize_skus(src.distinct_skus()))
        bd = tagged[tagged["sku_name_kr"] == "삼양)불닭볶음면컵"].iloc[0]
        assert bd["tag_spicy"] and bd["tag_stir_fried"]
        assert "Spicy" in bd["investment_theme"]

    def test_coverage_returns_pct(self):
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        tagged = tagging.tag_skus(normalize.normalize_skus(src.distinct_skus()))
        cov = tagging.theme_coverage(tagged)
        assert "spicy" in cov and 0 <= cov["spicy"] <= 100


# ── mapping ─────────────────────────────────────────────────────────────────
class TestMapping:
    def test_known_company_maps_to_ticker(self):
        df = pd.DataFrame({"company_kr": ["농심", "삼양식품"],
                           "sales_amt": [100, 100]})
        out = mapping.map_companies(df)
        assert out.loc[0, "krx_code"] == "004370"
        assert out.loc[0, "bbg_ticker"] == "004370 KS"
        assert out.loc[0, "isin"].startswith("KR7004370")

    def test_unmapped_flagged(self):
        df = pd.DataFrame({"company_kr": ["듣보잡식품"], "sales_amt": [100]})
        rep = mapping.mapping_report(mapping.map_companies(df))
        assert "듣보잡식품" in rep["unmapped"]

    def test_isin_check_digit_valid(self):
        # 삼성전자 005930 → KR7005930003 (공인 ISIN)
        assert config._krx_isin("005930") == "KR7005930003"


# ── validation 게이트 ───────────────────────────────────────────────────────
class TestValidationGates:
    def test_unmapped_company_triggers_error_halt(self):
        df = pd.DataFrame({"company_kr": ["듣보잡식품"], "sales_amt": [100]})
        rep = mapping.mapping_report(mapping.map_companies(df))
        res = validation.validate_mapping(rep)
        assert res["max_severity"] == "error"
        assert res["halt"] is True

    def test_all_mapped_no_halt(self):
        df = pd.DataFrame({"company_kr": ["농심"], "sales_amt": [100]})
        rep = mapping.mapping_report(mapping.map_companies(df))
        res = validation.validate_mapping(rep)
        assert res["halt"] is False


# ── mastering (영문 마스터링) ────────────────────────────────────────────────
class TestMastering:
    def _enriched(self):
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        sku = mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(src.distinct_skus())))
        return mastering.enrich_sku_master(sku)

    def test_romanize_passthrough_ascii(self):
        assert mastering.romanize("ABC") == "Abc"

    def test_variant_to_en_known_token(self):
        en, full = mastering.variant_to_en("블랙")
        assert en == "Black" and full is True

    def test_variant_to_en_unknown_romanized(self):
        en, full = mastering.variant_to_en("없는맛")
        assert full is False and en  # 로마자 폴백 비어있지 않음

    def test_resolve_brand_curated(self):
        b = mastering.resolve_brand("농심", "신라면", "NONGSHIM")
        assert b["brand_id"] == "NONGSHIM_SHIN_RAMYUN"
        assert b["brand_en"] == "Shin Ramyun" and b["curated"]

    def test_resolve_brand_fallback(self):
        b = mastering.resolve_brand("듣보잡식품", "미지브랜드", "")
        assert b["curated"] is False and b["brand_id"].startswith("UNKNOWN")

    def test_sku_english_name_and_id(self):
        m = self._enriched()
        row = m[m["sku_name_kr"] == "농심)신라면5입"].iloc[0]
        assert row["sku_name_en"] == "Shin Ramyun Bag 5-Pack"
        assert row["sku_slug"] == "NONGSHIM_SHIN_RAMYUN_BAG_5P"   # 의미형 라벨
        assert row["sku_id"] == str(row["barcode"])               # 안정 키 = 바코드
        assert row["mapping_confidence"] == "high"

    def test_company_master_uses_isin_id(self):
        m = self._enriched()
        cm = mastering.build_company_master(m)
        nong = cm[cm["company_name_ko"] == "농심"].iloc[0]
        assert nong["company_id"] == "KR7004370003"
        assert nong["company_name_en"] == "NONGSHIM CO., LTD."

    def test_brand_master_has_aliases(self):
        m = self._enriched()
        bm = mastering.build_brand_master(m)
        shin = bm[bm["brand_id"] == "NONGSHIM_SHIN_RAMYUN"].iloc[0]
        assert "Shin Ramen" in shin["brand_aliases"]


# ── 신규 레이어 모듈 ─────────────────────────────────────────────────────────
class TestSchemaMapper:
    def test_detect_owner_cu(self):
        from kfnb_app.ingest import schema_mapper
        cols = ["YMD_CD", "GRP_ACNT_NM", "GRP_ITEM_NM", "ITEM_CD", "SALE_AMT"]
        assert schema_mapper.detect_owner(cols) == "cu"

    def test_owner_b_explicit_rename(self):
        from kfnb_app.ingest import schema_mapper
        cols = ["sales_dt", "manufacturer", "brand_nm", "gtin", "net_sales"]
        ren = schema_mapper.rename_map(cols, "owner_b")
        assert ren["manufacturer"] == "company_kr"
        assert ren["net_sales"] == "sales_amt"
        assert ren["gtin"] == "barcode"


class TestTextCleaning:
    def test_pack_standardize(self):
        from kfnb_app.standardization import text_cleaning as tc
        assert tc.standardize_pack("신라면 4개입") == "신라면 4-Pack"
        assert tc.standardize_pack("신라면블랙 4입") == "신라면블랙 4-Pack"

    def test_promo_token_split(self):
        from kfnb_app.standardization import text_cleaning as tc
        clean, tags = tc.strip_promo_tokens("신라면 행사 묶음")
        assert "행사" in tags and "묶음" in tags and "행사" not in clean


class TestCoverage:
    def test_sales_based_coverage(self):
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        sku = mastering.enrich_sku_master(mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(src.distinct_skus()))))
        from kfnb_app.mapping import coverage
        c = coverage.coverage_by_sales(sku)
        assert 0 <= c["sku_coverage_pct"] <= 100
        assert "high_confidence_pct" in c


class TestQC:
    def test_qc_runs_and_builds_tables(self):
        from kfnb_app.ingest.dataio import open_source
        from kfnb_app.qc import checks
        from kfnb_app import panel as panel_mod
        src = open_source(_raw(), prefer_duckdb=False)
        sku = mastering.enrich_sku_master(mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(
                src.distinct_skus()[lambda d: d.cat_l2 == "면류"]))))
        mp = panel_mod.build_monthly_panel(src, "면류")
        res = checks.run_qc(sku, mp, {})
        assert set(res["tables"]) == {"qc_summary", "unmapped_items",
                                      "outlier_report", "mapping_coverage"}
        assert res["max_severity"] in ("ok", "info", "warning", "error", "critical")


class TestNameCleaningFixes:
    def test_pack_and_size_extraction(self):
        assert normalize.parse_pack_count("기린이치방캔500ml*6") == 6
        assert normalize.parse_pack_count("처음처럼P250ml20입") == 20
        assert normalize.extract_size("카스캔500ml") == ("500", "ml")
        assert normalize.extract_size("카스P1.6L") == ("1.6", "L")

    def test_package_never_korean(self):
        # cat_l3 가 한글 카테고리여도 한글을 반환하지 않음
        assert normalize.package_format("수입맥주", "테라캔500ml") == "Can"
        assert normalize.package_format("일반소주", "참이슬병360ml") == "Bottle"

    def test_no_hangul_in_sku_name_en(self):
        raw = pd.DataFrame([{
            "company_kr": "없는회사", "brand_kr": "없는브랜드", "cat_l1": "주류",
            "cat_l2": "맥주", "cat_l3": "수입맥주", "barcode": "8809000000001",
            "sku_name_kr": "없는브랜드캔500ml수입맥주", "sales_amt": 1000,
            "sales_qty": 10, "first_date": 20240101, "last_date": 20240101}])
        m = mastering.enrich_sku_master(
            mapping.map_companies(tagging.tag_skus(normalize.normalize_skus(raw))))
        import re as _re
        assert not _re.search(r"[가-힣]", m["sku_name_en"].iloc[0])

    def test_official_company_name_in_master(self):
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        sku = mastering.enrich_sku_master(mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(src.distinct_skus()))))
        f = mastering.build_sku_master_file(sku)
        assert f["company_name_en"].str.contains("CO\\.|CORP").any()

    def test_region_en_mapping(self):
        assert config.REGION_EN["서울특별시"] == "Seoul"
        assert config.REGION_EN["부산광역시"] == "Busan"


class TestAnalysisColumnSelection:
    def test_select_subset(self):
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        sku = mastering.enrich_sku_master(mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(src.distinct_skus()))))
        only = mastering.build_sku_master_file(sku, analysis_cols=["new_product"])
        assert "tag_new" in only.columns
        assert "tag_spicy" not in only.columns and "pack_count" not in only.columns

    def test_default_includes_all(self):
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        sku = mastering.enrich_sku_master(mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(src.distinct_skus()))))
        full = mastering.build_sku_master_file(sku)
        for c in ("pack_count", "package_format", "investment_theme",
                  "tag_spicy", "asp_won", "size_value"):
            assert c in full.columns


class TestGicsBloomberg:
    def _sku(self):
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        return mastering.enrich_sku_master(mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(src.distinct_skus()))))

    def test_company_master_has_gics_bloomberg(self):
        cm = mastering.build_company_master(self._sku())
        row = cm[cm["company_name_ko"] == "농심"].iloc[0]
        assert row["gics_sub_code"] == "30202030"
        assert row["gics_sub_name"] == "Packaged Foods & Meats"
        assert row["bloomberg_code"] == "004370 KS Equity"

    def test_sku_id_cols_selectable(self):
        sku = self._sku()
        only_gics = mastering.build_sku_master_file(sku, id_cols=["gics", "bloomberg"])
        assert "gics_sub_code" in only_gics.columns
        assert "bloomberg_code" in only_gics.columns
        assert "isin" not in only_gics.columns        # 선택 안 하면 빠짐
        default = mastering.build_sku_master_file(sku)   # 기본 ISIN+티커
        assert "isin" in default.columns and "bbg_ticker" in default.columns
        assert "gics_sub_code" not in default.columns


class TestUseCase:
    def _setup(self):
        from kfnb_app import panel as panel_mod
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        sku = mastering.enrich_sku_master(mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(
                src.distinct_skus()[lambda d: d.cat_l2 == "면류"]))))
        mp = panel_mod.build_monthly_panel(src, "면류")
        ac = panel_mod.build_annual_company(src, "면류")
        return mp, ac, sku

    def test_generate_returns_ranked_signals(self):
        from kfnb_app.insight import usecase
        mp, ac, sku = self._setup()
        uc = usecase.generate(mp, ac, sku)
        # 합성 데이터는 2년치라 momentum/asp(24개월 필요)는 비어도, 구조는 유효
        assert "usecase_type" in uc.columns and "thesis_ko" in uc.columns
        if not uc.empty:
            assert uc["rank"].is_monotonic_increasing
            assert "isin" in uc.columns  # r.isin 버그 회귀 방지

    def test_narrative_is_markdown(self):
        from kfnb_app.insight import usecase
        mp, ac, sku = self._setup()
        rep = usecase.narrative(usecase.generate(mp, ac, sku), "K-Food")
        assert rep.startswith("#")


class TestAlpha:
    def _panel(self):
        import numpy as np
        months = [y * 100 + m for y in (2020, 2021, 2022) for m in range(1, 13)]
        base = np.linspace(100, 160, 36) + 8 * np.sin(np.arange(36) / 12 * 2 * np.pi)
        return pd.DataFrame({"krx_code": "004370", "ym": months,
                             "bbg_ticker": "004370 KS", "sales_amt": base * 1e8,
                             "sales_qty": np.full(36, 1e5)})

    def test_leadlag_detects_designed_lead(self):
        # i.i.d. 신호가 타깃을 정확히 3기간 선행하도록 설계 → lag 3 회수
        import numpy as np
        from kfnb_app.insight import alpha
        np.random.seed(0)
        sig = pd.Series(np.random.randn(48))
        tgt = sig.shift(3) + np.random.randn(48) * 0.05   # tgt[t]=sig[t-3]
        ll = alpha.leadlag(sig, tgt, max_lag=6)
        best = alpha._best(ll)
        assert best[0] == 3 and best[1] > 0.8

    def test_research_vs_returns_oos_fdr_columns(self):
        import numpy as np
        from kfnb_app.insight import alpha
        np.random.seed(0)
        # 48개월 + 노이즈 수익률 → OOS/FDR 컬럼 스키마 검증
        months = [y * 100 + m for y in (2020, 2021, 2022, 2023) for m in range(1, 13)]
        base = np.linspace(100, 200, 48) + 5 * np.sin(np.arange(48))
        panel = pd.DataFrame({"krx_code": "004370", "ym": months,
                              "sales_amt": base * 1e8, "sales_qty": np.full(48, 1e5)})
        prices = pd.DataFrame({"krx_code": "004370", "ym": months,
                               "ret": np.random.normal(0, 0.02, 48)})
        res = alpha.research_vs_returns(panel, prices, max_lead=6)
        need = {"krx_code", "signal", "best_lead_m", "full_corr", "oos_corr",
                "p_value", "q_value_fdr", "significant", "confidence"}
        assert need <= set(res.columns)

    def test_fdr_and_pvalue_math(self):
        from kfnb_app.insight import alpha
        # 강상관 → 작은 p, 무상관 → 큰 p
        assert alpha._pvalue(0.9, 50) < 0.01
        assert alpha._pvalue(0.02, 50) > 0.5
        q = alpha._bh_fdr([0.001, 0.4, 0.8, 0.9])
        assert q[0] < q[1] <= q[2] <= q[3] and all(0 <= x <= 1 for x in q)

    def test_graceful_when_no_prices(self):
        from kfnb_app.insight import alpha
        res = alpha.research_vs_returns(self._panel(), pd.DataFrame(), max_lead=6)
        assert res.empty

    def test_price_source_graceful_without_libs(self):
        from kfnb_app.ingest import prices
        df, note = prices.monthly_prices(["004370"])
        assert isinstance(df, pd.DataFrame) and isinstance(note, str)

    def test_disclosures_graceful_without_key(self):
        from kfnb_app.ingest import disclosures
        df, note = disclosures.quarterly_revenue(["004370"], api_key="")
        assert df.empty and "DART" in note


class TestPIT:
    def _panel(self):
        import numpy as np
        months = [y * 100 + m for y in (2020, 2021, 2022, 2023) for m in range(1, 13)]
        sales = np.linspace(100, 200, 48) + 5 * np.sin(np.arange(48))
        return pd.DataFrame({"krx_code": "004370", "ym": months,
                             "sales_amt": sales * 1e8, "sales_qty": np.full(48, 1e5)})

    def test_available_date(self):
        import datetime as dt
        from kfnb_app.insight import pit
        assert pit.available_date(202403, 15) == dt.date(2024, 4, 15)
        assert pit.available_date(202412, 15) == dt.date(2025, 1, 15)

    def test_pit_panel_is_causal(self):
        # 핵심: 미래 데이터를 잘라내도 과거 시점 값이 불변 = look-ahead 없음
        from kfnb_app.insight import pit
        panel = self._panel()
        full = pit.build_pit_panel(panel)
        t = 202301
        trunc = pit.build_pit_panel(panel[panel.ym <= t])

        def val(df, ym):
            r = df[(df.signal == "sales_yoy") & (df.ym == ym)]
            return None if r.empty else round(float(r.value.iloc[0]), 6)
        assert val(full, t) == val(trunc, t)
        assert set(full.columns) == {"krx_code", "ym", "available_date",
                                     "signal", "value"}

    def test_pit_empty_safe(self):
        from kfnb_app.insight import pit
        out = pit.build_pit_panel(pd.DataFrame())
        assert out.empty


class TestAssessment:
    def _ctx(self):
        from kfnb_app import panel as panel_mod
        from kfnb_app.mapping import coverage
        from kfnb_app.profiling import profiler
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        prof = profiler.build_profile(src)
        sku = mastering.enrich_sku_master(mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(src.distinct_skus()))))
        mp = panel_mod.build_monthly_panel(src)
        return prof, sku, mp, coverage.coverage_by_sales(sku)

    def test_build_assessment_structure(self):
        from kfnb_app.insight import assessment
        prof, sku, mp, cov = self._ctx()
        a = assessment.build_assessment(
            profile=prof, sku_master=sku, monthly_panel=mp, coverage=cov,
            qc_result={}, source_name="(CU원본)test.csv", sector_label="K-Food")
        assert a["grade"] and 1 <= a["score"] <= 3
        assert len(a["scorecard"]) >= 8
        # 솔직한 한계가 반드시 드러나야 함
        joined = " ".join(a["limitations"])
        assert "채널" in joined and "수출" in joined
        # 단일채널(CU) 인식
        assert a["kpis"]["single_channel"] is True

    def test_assessment_markdown(self):
        from kfnb_app.insight import assessment
        prof, sku, mp, cov = self._ctx()
        a = assessment.build_assessment(profile=prof, sku_master=sku,
                                        monthly_panel=mp, coverage=cov, qc_result={})
        md = assessment.assessment_markdown(a, "K-Food")
        assert md.startswith("#") and "Limitations" in md


class TestDataSpecConclusionQA:
    def _ctx(self, pop="census", rest="none"):
        from kfnb_app import config, panel as panel_mod
        from kfnb_app.mapping import coverage
        from kfnb_app.profiling import profiler
        from kfnb_app.insight import pit
        src = dataio.open_source(_raw(), prefer_duckdb=False)
        prof = profiler.build_profile(src)
        sku = mastering.enrich_sku_master(mapping.map_companies(
            tagging.tag_skus(normalize.normalize_skus(src.distinct_skus()))))
        mp = panel_mod.build_monthly_panel(src)
        spec = config.DataSpec(amount_basis="vat_incl_retail", qty_basis="selling_unit",
                               channel_scope="CU 편의점", population=pop,
                               release_cadence="monthly", release_lag_days=15,
                               restatement=rest)
        return (spec, prof, coverage.coverage_by_sales(sku), sku, mp,
                pit.build_pit_panel(mp))

    def test_conclusion_honest_unknowns(self):
        from kfnb_app.insight import conclusion
        spec, prof, cov, sku, mp, pp = self._ctx(pop="unknown", rest="unknown")
        c = conclusion.build_conclusion(spec=spec, profile=prof, coverage=cov,
                                        sku_master=sku, monthly_panel=mp, pit_panel=pp)
        # 미확인 항목이 결론에 반드시 노출
        joined = " ".join(c["unknowns"])
        assert "모집단" in joined and "정정" in joined
        # 체크리스트에 met/unmet/unknown 존재
        statuses = {x["status"] for x in c["backtest_checks"]}
        assert "met" in statuses
        md = conclusion.conclusion_markdown(c, "K-Food")
        assert "말할 수 없는 것" in md and "백테스트" in md

    def test_conclusion_restatement_unmet(self):
        from kfnb_app.insight import conclusion
        spec, prof, cov, sku, mp, pp = self._ctx(rest="revised")
        c = conclusion.build_conclusion(spec=spec, profile=prof, coverage=cov,
                                        sku_master=sku, monthly_panel=mp, pit_panel=pp)
        rcheck = [x for x in c["backtest_checks"] if "정정" in x["condition"]][0]
        assert rcheck["status"] == "unmet"

    def test_investor_qa_covers_categories(self):
        from kfnb_app.insight import investor_qa
        spec, prof, cov, sku, mp, pp = self._ctx()
        qa = investor_qa.build_qa(spec=spec, profile=prof, coverage=cov,
                                  sku_master=sku, monthly_panel=mp, pit_panel=pp)
        cats = {x["category"] for x in qa}
        for need in ("Provenance", "Coverage", "Point-in-Time", "Mapping",
                     "Backtest", "Risk"):
            assert need in cats
        assert len(qa) >= 25
        md = investor_qa.qa_markdown(qa, "K-Food")
        assert md.startswith("#")


class TestGenericGrain:
    def _company_level(self):
        # 업로드 파일 형태: 날짜+회사+매출(+건수), 브랜드/카테고리/바코드/수량 없음
        rows = []
        for d in (20240101, 20240201, 20250101, 20250201, 20260101):
            for co in ("농심", "삼양식품", "오뚜기", "없는회사"):
                rows.append({"YMD_CD": d, "GRP_ACNT_NM": co,
                             "SALE_AMT": 100000000, "SALE_CNT": 5000})
        cols = ["YMD_CD", "GRP_ACNT_NM", "SALE_AMT", "SALE_CNT"]
        return pd.DataFrame(rows * 12)[cols]

    def test_cp949_encoding(self, tmp_path):
        # 한국어 CSV(cp949) 가 UnicodeDecodeError 없이 읽혀야 함
        p = tmp_path / "k.csv"
        self._company_level().to_csv(p, index=False, encoding="cp949")
        from kfnb_app.ingest import dataio as _d
        assert _d.detect_encoding(p) in ("cp949", "euc-kr")
        assert _d.peek_columns(p) == ["YMD_CD", "GRP_ACNT_NM", "SALE_AMT", "SALE_CNT"]
        src = _d.open_source(str(p), prefer_duckdb=False)
        assert src.total_rows() > 0

    def test_delimiter_autodetect(self, tmp_path):
        from kfnb_app.ingest import dataio as _d
        base = self._company_level()
        for sep, name in [(";", "semi"), ("\t", "tab"), ("|", "pipe")]:
            p = tmp_path / f"{name}.csv"
            base.to_csv(p, index=False, sep=sep, encoding="utf-8-sig")
            assert _d.peek_columns(p) == list(base.columns)

    def test_ragged_rows_skipped(self, tmp_path):
        from kfnb_app.ingest import dataio as _d
        p = tmp_path / "ragged.csv"
        p.write_text("YMD_CD,GRP_ACNT_NM,SALE_AMT\n20240101,농심,300,X\n"
                     "20250101,농심,200\n", encoding="utf-8")
        df = _d._read_any(p)            # 필드수 불일치 행은 스킵, 크래시 없음
        assert list(df.columns) == ["YMD_CD", "GRP_ACNT_NM", "SALE_AMT"]

    def test_capabilities_company_grain(self):
        from kfnb_app.ingest import schema_mapper
        cols = ["YMD_CD", "GRP_ACNT_NM", "SALE_AMT", "SALE_CNT"]
        assert schema_mapper.missing_required(cols) == []      # 최소조건 충족
        caps = schema_mapper.capabilities(cols)
        assert caps["grain"] == "company" and not caps["has_sku"]

    def test_fill_canonical_company_level(self):
        # 누락 컬럼이 자동 보완되어 표준 컬럼이 모두 존재
        src = dataio.open_source(self._company_level(), prefer_duckdb=False)
        sku = src.distinct_skus()
        for c in ("brand_kr", "cat_l2", "barcode", "sku_name_kr"):
            assert c in sku.columns
        # 회사 단위 → 엔티티(=회사) 4개
        assert sku["company_kr"].nunique() == 4

    def test_company_level_pipeline_e2e(self, tmp_path):
        from kfnb_app.pipeline import run_pipeline
        out = tmp_path / "p.xlsx"
        r = run_pipeline(self._company_level(), out, sector=None,
                         focus_brand="", strict=False, prefer_duckdb=False)
        # 미매핑(없는회사) 때문에 mapping 게이트는 non-strict로 통과 → 완주
        assert [s.name for s in r["stages"]][-1] == "export"
        assert "data/company_master.csv" in r["export"]["files"]


class TestRomanization:
    def test_romanize(self):
        from kfnb_app.utils.romanization import romanize
        assert romanize("ABC") == "Abc"
        assert romanize("신라면")  # non-empty


# ── 파이프라인 E2E ──────────────────────────────────────────────────────────
class TestPipelineE2E:
    def test_strict_halts_on_unmapped(self, tmp_path):
        out = tmp_path / "p.xlsx"
        result = run_pipeline(_raw(), out, sector="면류",
                              focus_brand="불닭볶음면", strict=True,
                              prefer_duckdb=False)
        # 듣보잡식품(미매핑) → ④에서 중단
        assert result["ok"] is False
        assert result["halted_at"] == "mapping"

    def test_non_strict_completes_and_writes_xlsx(self, tmp_path):
        out = tmp_path / "p.xlsx"
        result = run_pipeline(_raw(), out, sector="면류",
                              focus_brand="불닭볶음면", strict=False,
                              prefer_duckdb=False)
        assert result["ok"] is True
        assert result["export"]["sheets"] >= 5
        # 딜리버리 패키지(데이터/QC/문서/메타) 생성 확인
        exp = result["export"]
        files = exp["files"]
        assert "data/company_master.csv" in files
        assert "data/brand_master.csv" in files
        assert "data/sku_master.csv" in files
        assert "qc/qc_summary.csv" in files
        assert "docs/README.md" in files
        assert "metadata/version.json" in files
        assert Path(exp["bundle_zip"]).exists()

    def test_full_stage_order_with_qc(self, tmp_path):
        out = tmp_path / "p.xlsx"
        result = run_pipeline(_raw(), out, sector="면류", strict=False,
                              prefer_duckdb=False)
        names = [s.name for s in result["stages"]]
        assert names == ["profile", "normalize", "tagging", "mapping",
                         "mastering", "panel", "usecase", "qc", "export"]

    @pytest.mark.skipif(not dataio._HAS_DUCKDB, reason="duckdb 미설치")
    def test_duckdb_and_pandas_agree(self, tmp_path):
        duck = dataio.open_source(_raw(), prefer_duckdb=True)
        pds = dataio.open_source(_raw(), prefer_duckdb=False)
        assert duck.total_rows() == pds.total_rows()
        assert duck.profile_stats()["n_sku"] == pds.profile_stats()["n_sku"]


# ── DART 회사 자동해석 (종목코드 + 공식영문명) ────────────────────────────────
class TestDartCompany:
    def test_resolve_no_key_graceful(self):
        from kfnb_app.ingest import dart_company
        out, note = dart_company.resolve(["농심", "오뚜기"], "")
        assert out == {}
        assert "DART_API_KEY" in note

    def test_resolve_empty_names(self):
        from kfnb_app.ingest import dart_company
        out, note = dart_company.resolve([], "DUMMYKEY")
        assert out == {}
        assert "회사명" in note

    def test_norm_strips_corp_markers(self):
        from kfnb_app.ingest import dart_company as dc
        assert dc._norm("(주)농심") == dc._norm("농심") == "농심"
        assert dc._norm("주식회사 오뚜기") == dc._norm("오뚜기")

    def test_dart_overlay_overrides_code_and_eng(self):
        # DART 가 종목코드·공식영문명을 채워주면 overlay 가 이를 반영
        resolved = {"테스트회사": {"corp_code": "00000000",
                                   "krx_code": "004370",
                                   "company_en_official": "Test Co., Ltd."}}
        ov = mapping.dart_overlay(resolved)
        ref = ov["테스트회사"]
        assert ref.krx_code == "004370"
        assert ref.company_en_official == "Test Co., Ltd."
        assert ref.listed is True
        assert ref.isin.startswith("KR")          # krx_code → ISIN 자동계산
        assert ref.bbg_ticker == "004370 KS"
        assert ref.note == "DART 자동해석"

    def test_dart_overlay_keeps_master_gics(self):
        # 기존 마스터에 있는 회사면 GICS/slug 유지, 종목코드·영문명만 갱신
        co = next(iter(config.COMPANY_MAP))
        base = config.COMPANY_MAP[co]
        resolved = {co: {"corp_code": "0", "krx_code": "999999",
                         "company_en_official": "Override EN"}}
        ov = mapping.dart_overlay(resolved)
        assert ov[co].gics_sub_code == base.gics_sub_code
        assert ov[co].krx_code == "999999"
        assert ov[co].company_en_official == "Override EN"

    def test_map_companies_extra_map_applies(self):
        df = pd.DataFrame({"company_kr": ["테스트회사", "테스트회사"],
                           "sales_amt": [10.0, 20.0]})
        ov = mapping.dart_overlay({"테스트회사": {
            "corp_code": "0", "krx_code": "123456",
            "company_en_official": "Test EN"}})
        out = mapping.map_companies(df, extra_map=ov)
        assert (out["krx_code"] == "123456").all()
        assert (out["company_en_official"] == "Test EN").all()
        assert (out["map_status"] == "listed").all()

    def test_mapping_report_extra_map_counts_as_listed(self):
        df = pd.DataFrame({"company_kr": ["없는회사XYZ"], "sales_amt": [100.0]})
        rep_plain = mapping.mapping_report(df)
        assert "없는회사XYZ" in rep_plain["unmapped"]
        ov = mapping.dart_overlay({"없는회사XYZ": {
            "corp_code": "0", "krx_code": "654321", "company_en_official": "X"}})
        rep = mapping.mapping_report(df, extra_map=ov)
        assert "없는회사XYZ" not in rep["unmapped"]
        assert "없는회사XYZ" in rep["listed"]

    def test_resolve_reuses_dart_lookup_flow(self, monkeypatch):
        # 사내 dart_lookup.match_dart_companies(검증된 매칭 로직)을 그대로 타는지 확인.
        # 네트워크 fetch 만 합성 마스터로 대체 → 매칭/랭킹은 실제 코드 실행.
        from kfnb_app.ingest import dart_company
        from modules.mapping import dart_lookup
        master = pd.DataFrame([
            {"corp_code": "00164779", "corp_name": "농심", "corp_name_eng": "NONGSHIM CO.,LTD.",
             "stock_code": "004370", "modify_date": "20240101"},
            {"corp_code": "00111722", "corp_name": "오뚜기", "corp_name_eng": "OTTOGI CORPORATION",
             "stock_code": "007310", "modify_date": "20240101"},
        ])
        monkeypatch.setattr(dart_lookup, "fetch_dart_corp_master", lambda key: master)
        out, note = dart_company.resolve(["(주)농심", "오뚜기"], "DUMMYKEY")
        assert out["(주)농심"]["krx_code"] == "004370"
        assert out["(주)농심"]["company_en_official"] == "NONGSHIM CO.,LTD."
        assert out["오뚜기"]["krx_code"] == "007310"
        assert "자동해석 2/2" in note

    def test_resolve_master_fetch_failure_graceful(self, monkeypatch):
        from kfnb_app.ingest import dart_company
        from modules.mapping import dart_lookup

        def _boom(key):
            raise RuntimeError("DART 서버 접속 실패")
        monkeypatch.setattr(dart_lookup, "fetch_dart_corp_master", _boom)
        out, note = dart_company.resolve(["농심"], "DUMMYKEY")
        assert out == {}
        assert "조회 실패" in note

    def test_company_master_reflects_overlay(self):
        df = pd.DataFrame({
            "company_kr": ["없는회사XYZ"],
            "sales_amt": [100.0], "brand_kr": ["B"], "sku_name_kr": ["S"],
            "sku_id": ["1"], "investment_theme": ["x"]})
        ov = mapping.dart_overlay({"없는회사XYZ": {
            "corp_code": "0", "krx_code": "654321",
            "company_en_official": "Override EN Co."}})
        mapped = mapping.map_companies(df, extra_map=ov)
        cm = mastering.build_company_master(mapped)
        row = cm[cm["company_name_ko"] == "없는회사XYZ"].iloc[0]
        assert row["company_name_en"] == "Override EN Co."
        assert row["krx_code"] == "654321"
        assert row["company_id"] == row["isin"]   # isin 있으면 company_id=isin


# ── 상품 기획 · 유니버스 관리 (strategy) ──────────────────────────────────────
class TestStrategyUniverse:
    def _comp_df(self):
        dates = pd.date_range("2023-01-01", periods=12, freq="MS")
        rows = []
        scale = {"농심": 100, "삼양식품": 80, "오뚜기": 60, "하이트진로": 50,
                 "롯데주류": 30, "무명비상장": 5}
        for co, base in scale.items():
            for d in dates:
                rows.append({"date": d, "company_kr": co, "brand_kr": co + "메인",
                             "sales_amt": base * 1_000_000})
        return pd.DataFrame(rows)

    def test_score_columns_and_range(self):
        from kfnb_app.strategy import universe as U
        sc = U.score_companies(self._comp_df())
        for c in ("composite_score", "sales_scale", "investability",
                  "data_coverage", "sector_rep", "rank", "listed"):
            assert c in sc.columns
        assert sc["composite_score"].between(0, 100).all()
        assert (sc["rank"] == range(1, len(sc) + 1)).all()

    def test_listed_outranks_unlisted(self):
        from kfnb_app.strategy import universe as U
        sc = U.score_companies(self._comp_df()).set_index("company_kr")
        # 비상장(투자불가)은 상장 대형사보다 점수 낮아야
        assert sc.loc["농심", "composite_score"] > sc.loc["무명비상장", "composite_score"]
        assert sc.loc["무명비상장", "investability"] == 0.0

    def test_select_universe_counts(self):
        from kfnb_app.strategy import universe as U
        sc = U.score_companies(self._comp_df())
        sel = U.select_universe(sc, target_n=3, watchlist_n=2)
        assert (sel["status"] == "selected").sum() == 3
        assert (sel["status"] == "watchlist").sum() == 2
        assert sel["selection_reason"].str.len().gt(0).all()

    def test_select_universe_listed_only(self):
        from kfnb_app.strategy import universe as U
        sc = U.score_companies(self._comp_df())
        sel = U.select_universe(sc, target_n=10, watchlist_n=5, listed_only=True)
        chosen = sel.loc[sel["status"] == "selected"]
        assert chosen["listed"].all()              # 선정군은 모두 상장사

    def test_select_brands_top_n_and_share(self):
        from kfnb_app.strategy import universe as U
        df = pd.DataFrame({
            "company_kr": ["농심"] * 7,
            "brand_kr": ["신라면", "짜파게티", "너구리", "안성탕면", "신라면블랙",
                         "사리곰탕", "오징어짬뽕"],
            "sales_amt": [70, 60, 50, 40, 30, 20, 10],
        })
        br = U.select_brands(df, companies=["농심"], top_n=5)
        assert len(br) == 5                        # 회사당 5개로 제한
        assert (br["rank_in_company"] == range(1, 6)).all()
        # 회사 내 비중은 전체(7개) 합 기준 → 5개 합 < 100
        assert br["brand_share"].iloc[0] > br["brand_share"].iloc[-1]

    def test_next_review_date_rollover(self):
        from kfnb_app.strategy import universe as U
        from datetime import date
        assert U.next_review_date(date(2026, 6, 16)) == date(2026, 12, 16)
        assert U.next_review_date(date(2026, 10, 31)) == date(2027, 4, 30)

    def test_save_load_review_roundtrip(self, tmp_path):
        from kfnb_app.strategy import universe as U
        sc = U.score_companies(self._comp_df())
        sel = U.select_universe(sc, target_n=3, watchlist_n=2)
        br = U.select_brands(self._comp_df(),
                             companies=sel.loc[sel.status == "selected", "company_kr"].tolist())
        store = tmp_path / "uni"
        info = U.save_universe(sel, br, store_dir=store, note="2026 H1")
        assert info["n_selected"] == 3
        loaded = U.load_universe(store_dir=store)
        assert len(loaded["universe"]) == len(sel)
        assert len(loaded["review_log"]) == 1
        rv = U.review_due(store_dir=store)
        assert rv["due"] is False                  # 방금 저장 → 6개월 뒤
        # 2회차 저장 시 리뷰로그 누적
        U.save_universe(sel, br, store_dir=store, note="2026 H2")
        assert len(U.load_universe(store_dir=store)["review_log"]) == 2

    def test_review_due_when_empty(self, tmp_path):
        from kfnb_app.strategy import universe as U
        rv = U.review_due(store_dir=tmp_path / "empty")
        assert rv["due"] is True


class TestStrategyPackages:
    def test_three_tiers(self):
        from kfnb_app.strategy import packages as P
        tiers = [p.tier for p in P.PACKAGES]
        assert tiers == ["Basic", "Professional", "Premium"]
        # Professional 은 L1+L2, Premium 은 L3 포함
        prof = next(p for p in P.PACKAGES if p.tier == "Professional")
        prem = next(p for p in P.PACKAGES if p.tier == "Premium")
        assert set(prof.layers) == {"L1", "L2"}
        assert "L3" in prem.layers

    def test_matrix_and_tables(self):
        from kfnb_app.strategy import packages as P
        m = P.package_matrix()
        assert list(m.columns) == ["항목", "Basic", "Professional", "Premium"]
        assert len(P.layer_table()) == len(P.LAYERS) == 4
        qt = P.question_table()
        assert list(qt.columns) == ["분석 질문", "필요 레이어", "최소 패키지"]
        assert len(qt) >= 5


# ── 기획: 섹터 후보 기반(업로드 불필요) ───────────────────────────────────────
class TestStrategyCandidates:
    def test_load_candidates(self):
        from kfnb_app.strategy import universe as U
        cand = U.load_candidates()
        assert len(cand) >= 25
        for c in ("company_kr", "krx_code", "listed", "segment", "sub_sector"):
            assert c in cand.columns
        # 시드의 상장 대형주는 종목코드 보유
        nong = cand[cand["company_kr"] == "농심"].iloc[0]
        assert nong["krx_code"] == "004370" and nong["listed"]

    def test_score_candidates_no_data_no_marketcap(self):
        from kfnb_app.strategy import universe as U
        sc = U.score_candidates(U.load_candidates())   # 시총 없이도 동작
        assert sc["composite_score"].between(0, 100).all()
        assert (sc["rank"] == range(1, len(sc) + 1)).all()
        # 비상장은 상장 게이트 0
        unl = sc[~sc["listed"]]
        if len(unl):
            assert (unl["listed_score"] == 0.0).all()

    def test_score_candidates_segment_priority(self):
        from kfnb_app.strategy import universe as U
        cand = pd.DataFrame([
            {"company_kr": "라면사", "krx_code": "111111", "listed": True,
             "segment": "ramen", "sub_sector": "Packaged Foods",
             "company_en_official": "", "gics_sub_name": "", "gics_sector": "",
             "mapped": False},
            {"company_kr": "지주사", "krx_code": "222222", "listed": True,
             "segment": "holding", "sub_sector": "Packaged Foods",
             "company_en_official": "", "gics_sub_name": "", "gics_sector": "",
             "mapped": False},
        ])
        sc = U.score_candidates(cand).set_index("company_kr")
        # 동일 조건이면 고신호 세그먼트(ramen) > holding
        assert sc.loc["라면사", "segment_score"] > sc.loc["지주사", "segment_score"]

    def test_score_candidates_with_marketcap(self):
        from kfnb_app.strategy import universe as U
        cand = U.load_candidates()
        codes = [c for c in cand["krx_code"].tolist() if c][:3]
        mc = {codes[0]: 5e12, codes[1]: 1e12, codes[2]: 2e11}
        sc = U.score_candidates(cand, market_cap=mc)
        assert (sc["market_cap"] > 0).any()
        # 시총 큰 종목의 mc_score 가 더 큼
        a = sc.loc[sc["krx_code"] == codes[0], "mc_score"].iloc[0]
        b = sc.loc[sc["krx_code"] == codes[2], "mc_score"].iloc[0]
        assert a >= b

    def test_candidate_brands_from_master(self):
        from kfnb_app.strategy import universe as U
        br = U.candidate_brands(["농심"])
        assert len(br) >= 1
        assert set(["company_kr", "brand_kr", "brand_en", "selected"]).issubset(br.columns)
        assert br["selected"].iloc[0]              # 상위 5개 자동 채택

    def test_save_candidate_universe_roundtrip(self, tmp_path):
        from kfnb_app.strategy import universe as U
        sc = U.score_candidates(U.load_candidates())
        sel = U.select_universe(sc, target_n=20, watchlist_n=8, listed_only=True)
        br = U.candidate_brands(sel.loc[sel.status == "selected", "company_kr"].tolist())
        info = U.save_universe(sel, br[br["selected"]], store_dir=tmp_path / "u", note="H1")
        assert info["n_selected"] == 20
        loaded = U.load_universe(store_dir=tmp_path / "u")
        assert "segment" in loaded["universe"].columns   # 후보 메타 영속화


# ── 상품 추천 엔진 (recommender) + 외부신호 어댑터(graceful) ─────────────────
class TestRecommender:
    def _universe(self):
        from kfnb_app.strategy import universe as U
        sc = U.score_candidates(U.load_candidates())
        return U.select_universe(sc, target_n=8, watchlist_n=4, listed_only=True)

    def test_assemble_and_score_with_signals(self):
        from kfnb_app.strategy import universe as U, recommender as R
        sel = self._universe()
        brands = U.candidate_brands(sel.loc[sel.status == "selected", "company_kr"].tolist())
        trends = pd.DataFrame({"keyword": ["불닭볶음면", "신라면"],
                               "trend_momentum": [0.8, 0.0]})
        cons = pd.DataFrame({"company_kr": ["삼양식품"],
                             "consensus_revision": [0.3], "consensus_dispersion": [0.7]})
        npd = pd.DataFrame({"company_kr": ["삼양식품"], "new_product_count": [4]})
        sig = R.assemble_signals(sel, brands_df=brands, trends_df=trends,
                                 consensus_df=cons, newproduct_df=npd)
        scored = R.score_signals(sig)
        assert "heat" in scored and "alpha_priority" in scored
        sam = scored[scored["company_kr"] == "삼양식품"].iloc[0]
        assert sam["heat"] > 0 and "trend" in sam["data_status"]
        assert "consensus" in sam["data_status"]

    def test_recommend_deepdive_for_hot_eligible(self):
        from kfnb_app.strategy import universe as U, recommender as R
        sel = self._universe()
        brands = U.candidate_brands(["삼양식품"])
        trends = pd.DataFrame({"keyword": ["불닭볶음면"], "trend_momentum": [0.9]})
        npd = pd.DataFrame({"company_kr": ["삼양식품"], "new_product_count": [5]})
        sig = R.assemble_signals(sel, brands_df=brands, trends_df=trends, newproduct_df=npd)
        recs = R.recommend(R.score_signals(sig))
        sam = recs[recs["company_kr"] == "삼양식품"].iloc[0]
        assert "딥다이브" in sam["recommended_action"]
        # 신호 없는 회사는 보류/관찰로 정직하게
        nosig = recs[recs["data_status"] == "none"]
        if len(nosig):
            assert nosig["recommended_action"].str.contains("신호 부족|관찰").all()

    def test_recommend_no_signals_graceful(self):
        from kfnb_app.strategy import universe as U, recommender as R
        sel = self._universe()
        sig = R.assemble_signals(sel)            # 신호 0
        recs = R.recommend(R.score_signals(sig))
        assert (recs["data_status"] == "none").all()
        assert (recs["confidence"] == "low").all()
        summ = R.recommendation_summary(recs)
        assert summ["with_signal"] == 0

    def test_segment_and_packaging(self):
        from kfnb_app.strategy import universe as U, recommender as R
        sel = self._universe()
        brands = U.candidate_brands(["삼양식품"])
        trends = pd.DataFrame({"keyword": ["불닭볶음면"], "trend_momentum": [0.9]})
        scored = R.score_signals(R.assemble_signals(sel, brands_df=brands, trends_df=trends))
        seg = R.segment_recommendations(scored)
        assert "deepdive_eligible" in seg.columns
        pkgs = R.trend_packaging(scored, trends, brands)
        assert any("매운맛" in p["theme"] for p in pkgs)


class TestTrendsAdapter:
    def test_google_trends_graceful(self):
        from kfnb_app.ingest import trends
        df, note = trends.google_trends(["불닭볶음면"])
        # 샌드박스: pytrends 미설치 또는 네트워크 차단 → 빈 결과 + 사유
        assert list(df.columns) == trends.TREND_COLS
        assert isinstance(note, str) and note

    def test_google_trends_empty_input(self):
        from kfnb_app.ingest import trends
        df, note = trends.google_trends([])
        assert df.empty and "키워드" in note

    def test_news_volume_no_key(self):
        from kfnb_app.ingest import trends
        df, note = trends.news_volume(["농심"])
        assert df.empty and "키" in note


class TestBrandAutoRecommend:
    def test_flagship_gets_five_recommended(self):
        from kfnb_app.strategy import universe as U
        for co in ["농심", "삼양식품", "롯데칠성", "오리온"]:
            br = U.candidate_brands([co])
            filled = br[br["brand_kr"].astype(str).str.strip() != ""]
            assert len(filled) == 5, f"{co}: {len(filled)} brands"
            assert filled["selected"].all()
            # 자동 추천이므로 빈 입력이 아니라 실제 브랜드명이 채워짐
            assert all(filled["brand_kr"].str.len() > 0)

    def test_reason_marks_source(self):
        from kfnb_app.strategy import universe as U
        br = U.candidate_brands(["농심"])
        reasons = set(br["selection_reason"])
        assert {"브랜드 마스터 등록", "자동 추천 대표 브랜드"} & reasons
