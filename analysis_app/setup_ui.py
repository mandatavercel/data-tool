"""
Step 4 (Analysis Setup) 모듈별 파라미터 입력 블록.

각 분석 모듈(growth, demand, anomaly, earnings_intel, factor_research)이
실행 전 필요한 사용자 입력을 받는 UI. 모듈 선택 시 인라인으로 펼쳐짐.

외부 사용:
    from analysis_app.setup_ui import render_param_block
    params = render_param_block("earnings_intel")  # → dict
"""
from __future__ import annotations

import streamlit as st

from analysis_app.secrets_store import (
    PERSIST_PATH,
    load_persistent_secrets,
    save_persistent_secret,
)


def render_param_block(key: str) -> dict:
    """주어진 모듈 key에 맞는 파라미터 입력 UI를 렌더링하고 dict 반환."""
    p: dict = {}

    # ── 📈 Growth Analytics ───────────────────────────────────────────────
    if key == "growth":
        ga1, ga2 = st.columns([1, 1])
        agg = ga1.radio("집계 단위", ["월", "분기", "주", "일"],
                        horizontal=True, key="p_g_agg")
        mt  = ga2.multiselect("성장률", ["MoM", "QoQ", "YoY"],
                              default=st.session_state.get("p_g_metrics", ["MoM", "YoY"]),
                              key="p_g_metrics")
        p = {"agg_unit": agg, "metrics": mt or ["MoM"]}

    # ── 🔥 Demand Intelligence ────────────────────────────────────────────
    elif key == "demand":
        agg = st.radio("집계 단위", ["월", "주", "일"], horizontal=True, key="p_d_agg")
        p = {"agg_unit": agg}

    # ── 🚨 Anomaly Detection ──────────────────────────────────────────────
    elif key == "anomaly":
        a1, a2 = st.columns([1, 1])
        agg    = a1.radio("집계 단위", ["일", "주", "월"], horizontal=True, key="p_an_agg")
        method = a2.radio("탐지 방법", ["Z-score", "IQR", "둘 다"], horizontal=True, key="p_an_method")
        t1, t2 = st.columns([1, 1])
        thr = t1.slider("Z-score 임계값", 1.5, 4.0, 2.5, 0.1, key="p_an_thresh")
        win = t2.slider("Rolling 윈도우", 3, 24, 6, 1, key="p_an_window")
        p = {"agg_unit": agg, "method": method, "threshold": thr, "window": win}

    # ── 📊 Earnings Intelligence ──────────────────────────────────────────
    elif key == "earnings_intel":
        p = _render_earnings_params()

    # ── 📉 Market Signal ──────────────────────────────────────────────────
    elif key == "market_signal":
        _render_market_signal_info()
        # Market Signal은 별도 사용자 파라미터 없음 — 정보 표시만
        p = {}

    # ── 🧪 Factor Research ────────────────────────────────────────────────
    elif key == "factor_research":
        fr1, fr2 = st.columns([1.4, 1])
        dart_api_fr = fr1.text_input(
            "DART API Key (선택, sector 분류용)", type="password",
            value=st.session_state.get("p_dart_key", ""),
            key="p_fr_dart_key",
            help="DART induty_code로 GICS sector 분류. 미입력 시 pykrx만.",
        )
        avail_lag = fr2.slider("Available lag (영업일)", 1, 30, 5, key="p_fr_lag",
                                help="매출월 종료 후 가용까지 영업일 (PIT 가드)")
        p = {"dart_api_key": dart_api_fr, "available_lag_days": avail_lag}

    return p


# ══════════════════════════════════════════════════════════════════════════════
# Market Signal — 데이터 출처 안내 (사용자 파라미터 없음)
# ══════════════════════════════════════════════════════════════════════════════

def _render_market_signal_info() -> None:
    """Market Signal이 주가 데이터를 어떻게 수집하는지 4단계 흐름 표시.

    클라우드(미국 IP)에서 Yahoo Finance가 자주 차단되는 환경 대응 — pykrx 우선 전략.
    """
    st.markdown(
        """
<div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;
            padding:12px 14px;font-size:12.5px;line-height:1.65;color:#334155'>
  <div style='font-weight:700;color:#0f172a;margin-bottom:6px;font-size:13px'>
    📡 주가 데이터 수집 흐름 — 자동 4단계 폴백
  </div>
  <div style='display:flex;gap:8px;flex-wrap:wrap;margin-bottom:6px'>
    <span style='background:#dbeafe;color:#1e40af;padding:2px 8px;
                 border-radius:10px;font-size:11.5px;font-weight:600'>
      ① POS 회사명 → 6자리 종목코드 추출
    </span>
    <span style='background:#dcfce7;color:#166534;padding:2px 8px;
                 border-radius:10px;font-size:11.5px;font-weight:600'>
      ② pykrx (KRX 한국 서버) 우선 호출
    </span>
    <span style='background:#fef3c7;color:#92400e;padding:2px 8px;
                 border-radius:10px;font-size:11.5px;font-weight:600'>
      ③ 실패시 yfinance (.KS/.KQ) 백업
    </span>
    <span style='background:#e0e7ff;color:#3730a3;padding:2px 8px;
                 border-radius:10px;font-size:11.5px;font-weight:600'>
      ④ OHLCV → 매출 vs 주가 시차 상관
    </span>
  </div>
  <div style='font-size:11.5px;color:#64748b;margin-top:2px'>
    💡 <b>왜 pykrx 우선?</b> Streamlit Cloud는 미국 서버라 Yahoo Finance에 자주 차단됨.
    pykrx는 KRX 한국 서버를 직접 호출해 IP 제한 없이 안정적으로 작동.
    수정종가는 미제공 → 종가로 대체 (단기 시차 상관 분석에 영향 미미).
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Earnings Intelligence 전용 — DART API Key + 영구 저장 + 회사 매핑 UI
# ══════════════════════════════════════════════════════════════════════════════

def _render_earnings_params() -> dict:
    """Earnings Intel 파라미터 — DART API Key, 영구 저장, 인터랙티브 회사 매핑."""
    manual_map_txt = ""    # 기본값

    # ── DART API Key 입력 + 영구 저장/삭제 ────────────────────────────────
    persisted = load_persistent_secrets()
    has_saved = bool(persisted.get("dart_api_key"))
    kc1, kc2, kc3 = st.columns([5, 1, 1])
    with kc1:
        dart_api_val = st.text_input(
            "DART API Key (선택, 한번 저장하면 다음부터 자동 로드)",
            type="password", key="p_dart_key",
            help="https://opendart.fss.or.kr 에서 무료 발급 — "
                 "💾 저장 버튼 클릭 시 ~/.mandata_analysis/config.json에 영구 저장됩니다.",
        )
    with kc2:
        st.markdown("<div style='padding-top:28px'></div>", unsafe_allow_html=True)
        if st.button(
            "💾 저장", key="p_dart_save", use_container_width=True,
            help="이 컴퓨터에 API Key를 영구 저장 (다음 실행부터 자동 로드)",
        ):
            if dart_api_val:
                if save_persistent_secret("dart_api_key", dart_api_val):
                    st.toast("✓ DART API Key 영구 저장됨", icon="💾")
                else:
                    st.toast("저장 실패 — 파일 권한 확인", icon="❌")
            else:
                st.toast("API Key를 먼저 입력하세요", icon="⚠️")
    with kc3:
        st.markdown("<div style='padding-top:28px'></div>", unsafe_allow_html=True)
        if st.button(
            "🗑 삭제", key="p_dart_clear", use_container_width=True,
            disabled=not has_saved,
            help="저장된 API Key 삭제 (다음 실행 시 빈 상태)" if has_saved
                 else "저장된 키가 없습니다",
        ):
            save_persistent_secret("dart_api_key", "")
            st.session_state.pop("p_dart_key", None)
            st.toast("저장된 DART API Key 삭제됨", icon="🗑")
            st.rerun()
    if has_saved:
        st.caption(f"💾 저장된 키 자동 로드됨  ·  `{PERSIST_PATH}` (chmod 600)")

    # ── POS 회사 목록 추출 ─────────────────────────────────────────────────
    pos_companies: list[str] = []
    raw_df  = st.session_state.get("raw_df")
    r_map   = st.session_state.get("role_map", {}) or {}
    name_c  = r_map.get("company_name")
    if raw_df is not None and name_c and name_c in raw_df.columns:
        try:
            pos_companies = sorted(str(x) for x in raw_df[name_c].dropna().unique().tolist())
        except Exception:
            pos_companies = []

    # ── 🚀 DART 자동 매칭 UI ───────────────────────────────────────────────
    if not pos_companies:
        st.warning("회사 매핑 UI를 사용하려면 company_name 역할 매핑이 필요합니다 (Step 2).")
    else:
        _render_dart_match_ui(dart_api_val, pos_companies)

    # ── ⚙️ 텍스트 매핑 (고급 fallback) ────────────────────────────────────
    with st.expander("⚙️ 텍스트로 종목코드 직접 입력 (매칭 실패 회사용)", expanded=False):
        manual_map_txt = st.text_area(
            "회사명:종목코드 (한 줄에 하나)",
            placeholder="농심: 004370\n오뚜기: 007310",
            key="p_dart_manual", height=80,
            help="자동 매칭이 실패한 회사를 수동으로 입력",
        )

    return {
        "dart_api_key":         dart_api_val,
        "manual_mapping":       manual_map_txt,
        "dart_company_mapping": dict(st.session_state.get("_dart_user_mapping", {})),
    }


def _render_dart_match_ui(dart_api_val: str, pos_companies: list[str]) -> None:
    """DART 자동 매칭 + 동명 후보 선택 + 결과 요약 (mapping_app 패턴)."""
    from modules.mapping.dart_lookup import (
        fetch_dart_corp_master, match_dart_companies, dart_summary
    )

    # 액션 버튼
    bcol1, bcol2, bstat = st.columns([1.4, 1.4, 3])
    with bcol1:
        do_match = st.button(
            "🚀 DART 자동 매칭 시작",
            key="p_dart_btn_match", type="primary",
            disabled=not bool(dart_api_val),
            use_container_width=True,
            help=(f"{len(pos_companies)}개 POS 회사명을 DART 마스터와 자동 매칭"
                  if dart_api_val else "DART API Key 입력 후 활성화"),
        )
    with bcol2:
        do_refresh = st.button(
            "🔄 마스터 재다운로드",
            key="p_dart_btn_refresh",
            disabled=not bool(dart_api_val),
            use_container_width=True,
            help="DART corpCode 캐시 무효화",
        )
    with bstat:
        if not dart_api_val:
            st.info(
                "💡 API Key 입력 후 **🚀 DART 자동 매칭 시작** 클릭 — "
                "동명 후보만 수동 선택, 나머지는 자동 매칭됩니다."
            )

    if do_refresh and dart_api_val:
        try:
            fetch_dart_corp_master.clear()
        except Exception:
            pass
        st.session_state.pop("p_dart_match", None)
        st.rerun()

    if do_match and dart_api_val:
        master = None
        try:
            with st.spinner("DART 마스터 다운로드 중... (자동 재시도 포함 · 최대 1~2분)"):
                master = fetch_dart_corp_master(dart_api_val)
        except RuntimeError as e:
            # _retry_session이 친절 메시지를 RuntimeError로 raise함
            st.error(f"❌ {e}")
            st.markdown(
                "**해결 방법**:\n"
                "1. **🔄 다시 시도** — 일시적 차단일 수 있어요. 30초~1분 후 같은 버튼 재클릭\n"
                "2. **네트워크 변경** — 회사망에서 차단된 경우 모바일 핫스팟에서 재시도\n"
                "3. **인증키 확인** — `https://opendart.fss.or.kr` 로그인 → 마이페이지 → 인증키 활성 상태 확인\n"
                "4. **위 ⚙️ 텍스트 매핑**으로 우회 — `회사명: 종목코드` 형식으로 직접 입력"
            )
        except Exception as e:
            st.error(f"❌ DART 마스터 조회 실패: {type(e).__name__}: {str(e)[:200]}")
        if master is not None and not master.empty:
            with st.spinner(f"{len(pos_companies)}개 회사 매칭 중..."):
                st.session_state["p_dart_match"]  = match_dart_companies(pos_companies, master)
                # 전체 상장사 listed 저장 — selectbox 그리드 옵션 풀
                try:
                    listed_df = master[master["stock_code"].astype(str).str.strip() != ""].copy()
                    listed_df = listed_df.sort_values("corp_name")
                    st.session_state["_dart_listed"] = [
                        {"name": r["corp_name"], "corp_code": r["corp_code"],
                         "stock_code": r["stock_code"], "name_eng": r.get("corp_name_eng", "")}
                        for _, r in listed_df.iterrows()
                    ]
                except Exception:
                    st.session_state["_dart_listed"] = []
            st.rerun()

    # 매칭 결과
    match_df = st.session_state.get("p_dart_match")
    listed   = st.session_state.get("_dart_listed", [])
    if match_df is None or match_df.empty:
        return

    summary = dart_summary(match_df)
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("입력 회사", f"{summary['total']:,}")
    m2.metric("✅ 완전 매칭", f"{summary['exact']:,}")
    m3.metric("🟡 부분 매칭", f"{summary['partial']:,}")
    m4.metric("❓ 동명 후보",  f"{summary['ambiguous']:,}")
    m5.metric("❌ 매칭 실패", f"{summary['none']:,}",
              delta=f"{summary['rate']*100:.0f}% 커버", delta_color="off")

    st.write("")

    # ── 🔁 회사별 매핑 selectbox 그리드 (모든 회사) ──────────────────────────
    st.markdown("**🔁 회사별 DART 매핑** — 자동 매칭이 default. 잘못된 매칭은 드롭다운에서 직접 정정.")
    st.caption(
        f"DART 상장사 {len(listed)}개를 검색 가능. 드롭다운 펼친 후 회사명·종목코드로 type-ahead 검색하세요. "
        "💡 비슷한 이름 회사가 있을 때 드롭다운에 추천 후보군이 자동으로 위쪽에 표시됩니다."
    )

    # 전체 옵션 (검색 가능)
    opt_codes_all  = [""] + [x["corp_code"] for x in listed]
    opt_labels_all = ["─ 매핑 안 함 / 자동 매칭에 위임 ─"] + [
        f"{x['name']} ({x['stock_code']})" for x in listed
    ]
    label_by_code  = {x["corp_code"]: f"{x['name']} ({x['stock_code']})" for x in listed}

    def _options_for_row(row) -> tuple[list[str], list[str], int]:
        """행 별 옵션 구성: 미매핑 + match_df 후보 (상단) + 전체 (하단). 자동 default index 반환."""
        cands     = row.get("candidates") or []
        cur_cc    = row.get("corp_code", "")
        # 1) 추천 후보 (match_df candidates)
        cand_codes  = [c.get("corp_code", "") for c in cands if c.get("corp_code")]
        cand_labels = [
            f"{c['corp_name']} ({c.get('stock_code','—')})"
            for c in cands if c.get("corp_code")
        ]
        # 2) 전체에서 추천 제외한 나머지
        rest_codes  = [cc for cc in opt_codes_all[1:] if cc not in cand_codes]
        rest_labels = [label_by_code[cc] for cc in rest_codes]
        # 3) 최종 옵션: "─ 미매핑" + 추천 후보들 + 구분선 + 전체 나머지
        if cand_codes:
            codes  = [""] + cand_codes + [""] + rest_codes
            labels = (
                ["─ 매핑 안 함"]
                + [f"⭐ {l}" for l in cand_labels]
                + [f"─── 전체 {len(rest_codes)}개사 ───"]
                + rest_labels
            )
        else:
            codes  = [""] + rest_codes
            labels = ["─ 매핑 안 함"] + rest_labels
        # default index
        if cur_cc in codes:
            idx = codes.index(cur_cc)
        else:
            idx = 0
        return codes, labels, idx

    # 그리드 렌더링 — 회사명(라벨) + selectbox 두 컬럼
    for _, row in match_df.iterrows():
        inp = row["input_name"]
        codes, labels, default_idx = _options_for_row(row)

        c1, c2 = st.columns([1.2, 4])
        c1.markdown(
            f"<div style='padding:7px 0;font-size:13px'><b>{inp}</b></div>",
            unsafe_allow_html=True,
        )
        sel_idx = c2.selectbox(
            f"_dart_grid_{inp}",
            options=range(len(labels)),
            index=default_idx,
            format_func=lambda i, ls=labels: ls[i],
            label_visibility="collapsed",
            key=f"p_dart_grid__{inp}",
        )
        new_cc = codes[sel_idx]

        # 구분선("─── 전체 ───") 옵션 선택 방지: 빈 코드면 미매핑 처리
        if not new_cc:
            mask = match_df["input_name"] == inp
            match_df.loc[mask, "corp_code"]     = ""
            match_df.loc[mask, "corp_name"]     = ""
            match_df.loc[mask, "corp_name_eng"] = ""
            match_df.loc[mask, "stock_code"]    = ""
        else:
            # listed에서 정보 찾아서 모든 컬럼 업데이트
            chosen = next((x for x in listed if x["corp_code"] == new_cc), None)
            if chosen:
                mask = match_df["input_name"] == inp
                match_df.loc[mask, "corp_code"]     = chosen["corp_code"]
                match_df.loc[mask, "corp_name"]     = chosen["name"]
                match_df.loc[mask, "corp_name_eng"] = chosen.get("name_eng", "")
                match_df.loc[mask, "stock_code"]    = chosen.get("stock_code", "")

    st.session_state["p_dart_match"] = match_df

    # 사용자 매핑 (POS 회사명 → corp_code) 저장
    user_dart_map: dict[str, str] = {}
    for _, r in match_df.iterrows():
        if r.get("corp_code"):
            user_dart_map[r["input_name"]] = r["corp_code"]
    st.session_state["_dart_user_mapping"] = user_dart_map

    st.caption(
        f"📊 현재 {len(user_dart_map)}/{len(match_df)} 매핑 완료. "
        "변경은 즉시 저장 — Earnings/Factor 다음 실행 시 자동 적용됩니다."
    )
