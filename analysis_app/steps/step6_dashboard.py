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
