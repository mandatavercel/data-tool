"""Step 6 — Signal Dashboard + Excel Export + Final Report."""
from __future__ import annotations

from datetime import date
import streamlit as st

from modules.common.dashboard import render_signal_dashboard
from modules.analysis.report import (
    check_conditions, build_report, render_final_report,
    export_html as export_report_html,
)
from analysis_app.navigation import go_to
from analysis_app.export     import build_export_excel


SHEET_LABEL = {
    "schema_profile":    "🔍 Schema",
    "validation_report": "✅ Validation",
    "capability_map":    "🗺 Capability",
    "growth_analysis":   "📈 Growth",
    "demand_analysis":   "🔥 Demand",
    "anomaly_detection": "🚨 Anomaly",
    "brand_analysis":    "🏷 Brand",
    "sku_analysis":      "📦 SKU",
    "category_analysis": "🗂 Category",
    "market_signal":     "📉 Market",
    "earnings_intel":    "📊 Earnings",
    "alpha_validation":  "🎯 Alpha",
    "signal_dashboard":  "📡 Dashboard",
}


def render() -> None:
    render_signal_dashboard()

    # ── Excel Export ─────────────────────────────────────────────────────────
    st.divider()
    st.markdown("#### 📥 전체 결과 Export")

    export_result = build_export_excel()
    if export_result is None:
        st.info("내보낼 결과가 없습니다. Step 5에서 분석을 실행하세요.")
    else:
        excel_bytes, sheet_names = export_result

        badge_html = " ".join(
            f"<span style='background:#dbeafe;color:#1e40af;border-radius:5px;"
            f"padding:3px 10px;font-size:12px;font-weight:600'>"
            f"{SHEET_LABEL.get(s, s)}</span>"
            for s in sheet_names
        )
        st.markdown(
            f"<div style='margin-bottom:10px'>포함 시트: {badge_html}</div>",
            unsafe_allow_html=True,
        )

        st.download_button(
            label=f"📥 Excel 다운로드 ({len(sheet_names)}개 시트)",
            data=excel_bytes,
            file_name="alt_data_intelligence_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )

    # ── Final Report ─────────────────────────────────────────────────────────
    st.divider()
    st.subheader("📋 Final Report")

    results       = st.session_state.get("results", {})
    role_map      = st.session_state.get("role_map", {})
    raw_df        = st.session_state.get("raw_df")
    quality_score = st.session_state.get("quality_score")

    can_gen, reason = check_conditions(results, role_map)
    if not can_gen:
        st.info(f"⚠️ Final Report 생성 조건 미충족: {reason}")
        st.button("📋 Generate Final Report", disabled=True)
    else:
        gen_clicked = st.button("📋 Generate Final Report", type="primary",
                                 key="final_report_gen")
        # report를 session_state에 캐시 (한/영 양쪽) — 다운로드 클릭 시 재생성 방지
        if gen_clicked:
            with st.spinner("리포트 생성 중 (한국어 + English)..."):
                st.session_state["_final_report"] = build_report(
                    results, role_map, raw_df, quality_score, lang="ko",
                )
                st.session_state["_final_report_en"] = build_report(
                    results, role_map, raw_df, quality_score, lang="en",
                )

        report    = st.session_state.get("_final_report")
        report_en = st.session_state.get("_final_report_en")
        if report is not None:
            render_final_report(report)

            # ── 📥 다운로드 4종 (PPT/PDF × 한/영) + HTML ─────────────────────
            st.divider()
            st.markdown("#### 📥 리포트 다운로드")
            st.caption("PPT/PDF · 한국어/영어 — 클릭하면 즉시 생성·다운로드됩니다.")
            from analysis_app.report_export import export_pptx, export_pdf

            d1, d2, d3, d4, d5 = st.columns(5)
            today = date.today()

            # PPT 한글
            with d1:
                try:
                    pptx_ko = export_pptx(report, lang="ko")
                    st.download_button(
                        "📊 PPT (한글)", pptx_ko,
                        file_name=f"final_report_ko_{today}.pptx",
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.button("📊 PPT (한글)", disabled=True,
                              help=f"생성 실패: {str(e)[:80]}",
                              use_container_width=True)

            # PPT 영문 — 영문 report dict 사용
            with d2:
                try:
                    pptx_en = export_pptx(report_en or report, lang="en")
                    st.download_button(
                        "📊 PPT (English)", pptx_en,
                        file_name=f"final_report_en_{today}.pptx",
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.button("📊 PPT (English)", disabled=True,
                              help=f"생성 실패: {str(e)[:80]}",
                              use_container_width=True)

            # PDF 한글
            with d3:
                try:
                    pdf_ko = export_pdf(report, lang="ko")
                    st.download_button(
                        "📄 PDF (한글)", pdf_ko,
                        file_name=f"final_report_ko_{today}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.button("📄 PDF (한글)", disabled=True,
                              help=f"생성 실패: {str(e)[:80]}",
                              use_container_width=True)

            # PDF 영문 — 영문 report dict 사용
            with d4:
                try:
                    pdf_en = export_pdf(report_en or report, lang="en")
                    st.download_button(
                        "📄 PDF (English)", pdf_en,
                        file_name=f"final_report_en_{today}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.button("📄 PDF (English)", disabled=True,
                              help=f"생성 실패: {str(e)[:80]}",
                              use_container_width=True)

            # HTML (기존, 한국어만)
            with d5:
                html_bytes = export_report_html(report)
                st.download_button(
                    "🌐 HTML", html_bytes,
                    file_name=f"final_report_{today}.html",
                    mime="text/html",
                    use_container_width=True,
                )

    # ── 🛒 Data Catalog Export ─────────────────────────────────────────
    st.divider()
    st.markdown("#### 🛒 Data Catalog Export (외부 고객용)")
    st.caption(
        "분석 결과를 카탈로그 형식(parquet)으로 export해서 "
        "**Data Catalog 앱**(고객 마켓플레이스)에 공급합니다. "
        "고객은 카탈로그에서 필터·검색·장바구니로 원하는 회사를 선택할 수 있어요."
    )
    _render_catalog_export()

    st.divider()
    c_prev, c_new = st.columns(2)
    with c_prev:
        if st.button("← Results"):
            go_to(5)
    with c_new:
        if st.button("🔄 전체 초기화"):
            for k in ["results", "selected_analysis"]:
                st.session_state.pop(k, None)
            go_to(4)


def _render_catalog_export() -> None:
    """분석 결과 → catalog DataFrame → parquet/xlsx 다운로드 + repo 저장."""
    import pandas as pd
    from datetime import datetime
    from pathlib import Path

    results = st.session_state.get("results") or {}
    role_map = st.session_state.get("role_map") or {}
    raw_df   = st.session_state.get("raw_df")

    # 회사 목록 추출
    co_col = role_map.get("company_name")
    if raw_df is None or not co_col or co_col not in raw_df.columns:
        st.info("회사명 매핑이 필요합니다 (Step 2). 분석 후 다시 시도하세요.")
        return

    companies = sorted(str(c) for c in raw_df[co_col].dropna().unique())

    # Market Signal 결과에서 회사별 시그널 추출
    sig_map: dict[str, dict] = {}
    market = results.get("market_signal") or {}
    for s in (market.get("_company_signals") or []):
        co = str(s.get("company", ""))
        if co:
            sig_map[co] = {
                "ticker":       str(s.get("ticker", "") or ""),
                "signal_score": float(s.get("signal_score", 0) or 0),
                "has_stock":    s.get("status") == "ok",
            }

    # Earnings Intel에서 DART 연동 여부
    earnings = results.get("earnings_intel") or {}
    dart_companies = set()
    for c in (earnings.get("_dart_by_company") or {}):
        dart_companies.add(str(c))

    # 섹터 추출 (factor_research에 있으면)
    factor = results.get("factor_research") or {}
    sector_map: dict[str, str] = {}
    try:
        panel = factor.get("data")
        if panel is not None and "company" in panel and "sector" in panel:
            for co, sec in zip(panel["company"], panel["sector"]):
                if co and sec:
                    sector_map[str(co)] = str(sec)
    except Exception:
        pass

    # 카탈로그 row 구성
    rows = []
    for co in companies:
        sig = sig_map.get(co, {})
        rows.append({
            "company":         co,
            "ticker":          sig.get("ticker", ""),
            "sector":          sector_map.get(co, "기타"),
            "signal_score":    sig.get("signal_score", 0.0),
            "mom_growth":      0.0,    # TODO: growth_analysis에서 추출
            "coverage_months": 0,      # TODO: 데이터 기간에서 계산
            "has_dart":        co in dart_companies,
            "has_stock":       sig.get("has_stock", False),
            "exported_at":     datetime.now().isoformat(timespec="seconds"),
        })
    catalog_df = pd.DataFrame(rows)

    # 통계 표시
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("회사 수",     f"{len(catalog_df):,}")
    s2.metric("시그널 매칭", f"{(catalog_df['signal_score'] > 0).sum():,}")
    s3.metric("DART 연동",   f"{catalog_df['has_dart'].sum():,}")
    s4.metric("주가 데이터", f"{catalog_df['has_stock'].sum():,}")

    # 다운로드 + 저장
    today = date.today()
    cx1, cx2 = st.columns(2)
    with cx1:
        # parquet (catalog 앱이 사용)
        try:
            import io
            buf = io.BytesIO()
            catalog_df.to_parquet(buf, index=False)
            buf.seek(0)
            st.download_button(
                "📦 카탈로그 다운로드 (.parquet)",
                data=buf.getvalue(),
                file_name=f"catalog_{today}.parquet",
                mime="application/octet-stream",
                use_container_width=True,
                help="Data Catalog 앱(고객용)에 업로드하거나 catalog/ 폴더에 저장하세요.",
            )
        except Exception as e:
            st.button("📦 .parquet (실패)", disabled=True,
                      help=f"pyarrow 미설치: {e}",
                      use_container_width=True)

    with cx2:
        # xlsx (사람이 직접 확인용)
        try:
            import io
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                catalog_df.to_excel(w, index=False, sheet_name="catalog")
            buf.seek(0)
            st.download_button(
                "📋 카탈로그 미리보기 (.xlsx)",
                data=buf.getvalue(),
                file_name=f"catalog_{today}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception:
            pass

    # repo의 catalog/ 폴더에 저장 옵션 (로컬 실행시만)
    import os as _os
    on_cloud = _os.path.isdir("/mount/src")
    if not on_cloud:
        if st.button("💾 catalog/ 폴더에 저장 (로컬)", use_container_width=True,
                     help="Data Catalog 앱이 자동 로드하도록 repo에 저장"):
            try:
                target_dir = Path(__file__).parent.parent.parent / "catalog"
                target_dir.mkdir(parents=True, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                target = target_dir / f"catalog_{ts}.parquet"
                catalog_df.to_parquet(target, index=False)
                st.success(f"✅ 저장 완료: `{target}`")
            except Exception as e:
                st.error(f"저장 실패: {type(e).__name__}: {e}")
