import streamlit as st


# 전체 분석 모듈 카탈로그
CATALOG = {
    "growth": {
        "name":  "📈 Growth Analytics",
        "desc":  "YoY / QoQ / MoM 성장률, 모멘텀 분석",
        "layer": "Intelligence",
        "requires": ["transaction_date", "sales_amount"],
        "optional": ["company_name", "brand_name"],
        "star":  False,
        "ready": True,
    },
    "demand": {
        "name":  "🔥 Demand Intelligence",
        "desc":  "거래건수 vs 객단가 분해, Demand Signal 탐지",
        "layer": "Intelligence",
        "requires": ["transaction_date", "sales_amount"],
        "optional": ["company_name", "brand_name", "number_of_tx"],
        "star":  True,
        "ready": True,
    },
    "anomaly": {
        "name":  "🚨 Anomaly Detection",
        "desc":  "소비 급등·급락 시그널 탐지 (Z-score / IQR)",
        "layer": "Intelligence",
        "requires": ["transaction_date", "sales_amount"],
        "optional": ["company_name", "brand_name"],
        "star":  True,
        "ready": True,
    },
    "brand": {
        "name":  "🏷 Brand Intelligence",
        "desc":  "브랜드 경쟁력, Momentum 분석",
        "layer": "Intelligence",
        "requires": ["brand_name", "sales_amount"],
        "optional": ["transaction_date"],
        "star":  False,
        "ready": False,
    },
    "sku": {
        "name":  "📦 SKU Intelligence",
        "desc":  "SKU 기여도, 신흥 SKU 탐지, Lifecycle",
        "layer": "Intelligence",
        "requires": ["sku_name", "sales_amount"],
        "optional": ["transaction_date", "company_name"],
        "star":  True,
        "ready": False,
    },
    "category": {
        "name":  "🗂 Category Intelligence",
        "desc":  "카테고리 성장, 시장 점유율 이동",
        "layer": "Intelligence",
        "requires": ["category_name", "sales_amount"],
        "optional": ["transaction_date"],
        "star":  False,
        "ready": False,
    },
    "market_signal": {
        "name":  "📉 Market Signal",
        "desc":  "매출 성장률 vs 주가 수익률 시차 상관분석",
        "layer": "Signal",
        "requires": ["transaction_date", "sales_amount", "stock_code"],
        "optional": ["company_name", "brand_name"],
        "star":  True,
        "ready": False,
    },
    "earnings_intel": {
        "name":  "📊 Earnings Intelligence",
        "desc":  "POS 데이터가 공시매출보다 먼저 움직이는지 검증 (DART API)",
        "layer": "Signal",
        "requires": ["transaction_date", "sales_amount", "stock_code"],
        "optional": [],
        "star":  True,
        "ready": False,
    },
}


def _check_availability(role_map: dict, requires: list) -> tuple[bool, str]:
    missing = [r for r in requires
               if r not in role_map and
               not (r == "stock_code" and "security_code" in role_map)]
    if missing:
        return False, "필요 역할 없음: " + ", ".join(missing)
    return True, ""


def render(go_to):
    if "role_map" not in st.session_state:
        st.warning("먼저 STEP 1을 완료하세요.")
        go_to(1)
        st.stop()

    role_map = st.session_state["role_map"]
    st.subheader("② 분석 선택")

    # ── Capability Map ─────────────────────────────────────────────────────────
    st.markdown("#### Capability Map")

    intel_items  = [(k, v) for k, v in CATALOG.items() if v["layer"] == "Intelligence"]
    signal_items = [(k, v) for k, v in CATALOG.items() if v["layer"] == "Signal"]

    def _card(key, info, role_map):
        avail, reason = _check_availability(role_map, info["requires"])
        ready  = info.get("ready", False)
        star   = "⭐ " if info["star"] else ""
        status = "✅ 가능" if (avail and ready) else ("🔜 준비중" if avail and not ready else "❌ 불가")
        bg     = "#e8f5e9" if (avail and ready) else ("#fff8e1" if avail and not ready else "#fafafa")
        note   = "" if (avail and ready) else (
            "<br><small style='color:#f59e0b'>모듈 준비 중</small>" if avail and not ready
            else f"<br><small style='color:#9ca3af'>{reason}</small>"
        )
        st.markdown(
            f"""<div style="border:1px solid #e0e0e0;border-radius:8px;padding:14px;
            background:{bg};min-height:110px;margin-bottom:4px;">
            <b>{star}{info['name']}</b><br>
            <small>{info['desc']}</small><br><br>
            <b>{status}</b>{note}
            </div>""",
            unsafe_allow_html=True,
        )

    st.caption("Intelligence Hub")
    cols_i = st.columns(len(intel_items))
    for idx, (key, info) in enumerate(intel_items):
        with cols_i[idx]:
            _card(key, info, role_map)

    st.caption("Signal Layer")
    cols_s = st.columns(len(signal_items))
    for idx, (key, info) in enumerate(signal_items):
        with cols_s[idx]:
            _card(key, info, role_map)

    # ── 분석 선택 ──────────────────────────────────────────────────────────────
    st.markdown("#### 분석 선택 및 파라미터 설정")

    runnable = [
        (k, v) for k, v in CATALOG.items()
        if _check_availability(role_map, v["requires"])[0] and v.get("ready", False)
    ]

    if not runnable:
        st.error("실행 가능한 분석이 없습니다. Foundation으로 돌아가 역할을 재설정하세요.")
        if st.button("← Foundation으로", key="sel_back"):
            go_to(1)
        st.stop()

    labels = [v["name"] for _, v in runnable]
    keys   = [k for k, _ in runnable]

    selected_label = st.radio("실행할 분석", labels, key="sel_radio")
    selected_key   = keys[labels.index(selected_label)]
    selected_info  = CATALOG[selected_key]

    params = {}
    with st.container(border=True):
        st.markdown(f"**{selected_info['name']} 파라미터**")

        if selected_key == "growth":
            agg_unit = st.radio("집계 단위", ["월", "분기", "주", "일"], horizontal=True, key="g_agg")
            metrics  = st.multiselect("성장률 종류", ["MoM", "QoQ", "YoY"], default=["MoM", "YoY"], key="g_metrics")
            params   = {"agg_unit": agg_unit, "metrics": metrics or ["MoM"]}

        elif selected_key == "demand":
            agg_unit = st.radio("집계 단위", ["월", "주", "일"], horizontal=True, key="d_agg")
            metrics  = st.multiselect(
                "분석 지표", ["매출액", "거래건수", "건당평균단가"],
                default=["매출액", "거래건수", "건당평균단가"], key="d_metrics"
            )
            params = {"agg_unit": agg_unit, "metrics": metrics or ["매출액"]}

        elif selected_key == "anomaly":
            agg_unit   = st.radio("집계 단위", ["일", "주", "월"], horizontal=True, key="an_agg")
            method     = st.radio("탐지 방법", ["Z-score", "IQR", "둘 다"], horizontal=True, key="an_method")
            threshold  = st.slider("Z-score 임계값", 1.5, 4.0, 2.5, 0.1, key="an_thresh")
            params = {"agg_unit": agg_unit, "method": method, "threshold": threshold}

        elif selected_key == "market_signal":
            lag_unit = st.radio("Lag 단위", ["일", "개월"], horizontal=True, key="ms_lag_unit")
            lags = (
                st.multiselect("Lag 기간 (일)",  [0, 1, 3, 7, 14, 30], default=[0, 1, 7, 30], key="ms_lags_d")
                if lag_unit == "일"
                else st.multiselect("Lag 기간 (개월)", [0, 1, 2, 3, 6, 12], default=[0, 1, 3, 6], key="ms_lags_m")
            )
            rw = st.slider("Rolling Window", 7, 60, 30, key="ms_rw")
            params = {"lag_unit": lag_unit, "lags": lags or [0], "rolling_window": rw}

        elif selected_key == "earnings_intel":
            dart_key = st.text_input("DART API Key", type="password", key="ei_key")
            reprt_map = {"1분기": "11011", "반기": "11012", "3분기": "11013", "사업보고서": "11014"}
            reprt_sel = st.multiselect("보고서 종류", list(reprt_map.keys()),
                                       default=["사업보고서", "반기"], key="ei_reprt")
            lead_w = st.slider("분기 초반 N주 선행 분석", 1, 12, 4, key="ei_lead")
            params = {
                "dart_api_key": dart_key,
                "reprt_codes": [reprt_map[r] for r in reprt_sel],
                "lead_weeks": lead_w,
            }

    if st.button("▶ 분석 실행", type="primary", key="sel_run"):
        st.session_state["selected_analysis"] = selected_key
        st.session_state["analysis_params"]   = params
        go_to(3)

    if st.button("← Foundation", key="sel_prev"):
        go_to(1)
