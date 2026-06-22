"""
K-F&B 데이터 상품 에이전트 — Streamlit UI (스텝형 / human-in-the-loop)

📂 위치: kfnb_app/app.py
📋 역할:
    원천 F&B POS 데이터를 단계별로 검증·수정·승인하며 투자등급 데이터 상품으로
    변환한다. 각 단계에서 멈춰 산출물을 검토하고, ①섹터선택 ③태그수정
    ④티커매핑승인 ⑥고객유형선택을 직접 한 뒤 '확인하고 다음'으로 진행한다.
🚀 실행: `🍜 KFnB 상품 실행.command` 더블클릭 (포트 8508)
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from kfnb_app import config, panel, validation
from kfnb_app.ingest import dataio
from kfnb_app.profiling import profiler as profile_mod
from kfnb_app.standardization import normalize, tagging
from kfnb_app.mapping import company as mapping, coverage, mastering
from kfnb_app.qc import checks as qc
from kfnb_app.export import bundle
from kfnb_app.strategy import universe as uni, packages as pkg

st.set_page_config(page_title="🍜 K-F&B 데이터 상품 — Mandata",
                   page_icon="🍜", layout="wide")

STEPS = ["데이터 입력", "① 프로파일·섹터", "② 정규화", "③ 투자 태깅",
         "④ 티커 매핑", "⑤ 영문 마스터링", "⑥ 패널 집계", "⑦ Use-case 발굴",
         "⑧ 상품 생성", "⑨ 대시보드·투자적합성"]

_SEV = {"ok": ("✅", "#16a34a"), "info": ("ℹ️", "#2563eb"),
        "warning": ("⚠️", "#d97706"), "error": ("❌", "#dc2626"),
        "critical": ("🛑", "#991b1b")}

SS = st.session_state


# ── 공용 헬퍼 ────────────────────────────────────────────────────────────────
def go_to(step: int):
    SS["step"] = step
    st.rerun()


def _src():
    """src_path 로부터 Source 를 연다 (advance 시에만 호출)."""
    return dataio.open_source(SS["src_path"])


def _remap(df):
    """DART 자동해석(종목코드·공식영문명)을 적용해 회사 매핑. 키 없으면 정적 마스터."""
    names = sorted(set(str(c) for c in df["company_kr"].dropna()))
    overlay, note = {}, ""
    try:
        from kfnb_app.ingest import dart_company
        resolved, note = dart_company.resolve(names, SS.get("dart_api_key", ""))
        overlay = mapping.dart_overlay(resolved) if resolved else {}
    except Exception as e:  # noqa: BLE001 — 비차단
        note = f"DART 자동해석 생략: {type(e).__name__}"
    SS["company_overlay"] = overlay
    SS["dart_note"] = note
    ov = overlay or None
    df = mapping.map_companies(df, extra_map=ov)
    rep = mapping.mapping_report(df, extra_map=ov)
    rep["dart_note"] = note
    rep["dart_resolved"] = sorted(overlay.keys())
    return df, rep


# ════════════════════════════════════════════════════════════════════════════
# 상품 기획·유니버스 관리 모드 (데이터셋 제작 *이전* 단계)
# ════════════════════════════════════════════════════════════════════════════
def _planning_overlay(names):
    """DART 키가 있으면 종목코드·공식영문명 보강 overlay 생성 (graceful)."""
    key = SS.get("dart_api_key", "")
    if not key:
        return None, "DART 키 없음 — 시드 종목코드/정적 마스터 사용"
    try:
        from kfnb_app.ingest import dart_company
        resolved, note = dart_company.resolve(names, key)
        return (mapping.dart_overlay(resolved) or None), note
    except Exception as e:                         # noqa: BLE001
        return None, f"DART 보강 생략: {type(e).__name__}"


def render_planning():
    st.subheader("📋 상품 기획 · 투자 유니버스 관리")
    st.caption(f"**{pkg.SUITE_NAME}** — {pkg.SUITE_TAGLINE}.  "
               "데이터를 만들기 *이전에* '어떤 회사/브랜드의 데이터를 확보·제작할지'를 "
               "한국 F&B 섹터(상장·시총·세그먼트) 기준으로 정하고 반기마다 관리합니다.")

    # 반기 리뷰 도래 배너
    rv = uni.review_due()
    if rv["due"]:
        st.warning(f"🔔 {rv['reason']} — 유니버스를 (재)구성하고 저장하세요. "
                   + (f"마지막 리뷰: {rv['last_review']}" if rv.get("last_review") else ""))
    else:
        st.info(f"✅ 유니버스 최신 — 마지막 {rv['last_review']} · 다음 리뷰 {rv['next_review']} "
                f"({rv['days_left']}일 남음)")

    tab_u, tab_r, tab_p = st.tabs(["🎯 유니버스 관리 (20사 + 브랜드 5)",
                                   "🧭 상품 추천 (트렌드·컨센서스)",
                                   "📦 상품 패키지 구조"])

    # ── 유니버스 관리 (데이터 업로드 불필요) ──────────────────────────────────
    with tab_u:
        st.markdown("##### 1) 섹터 후보 → 자동 스코어링 (시총·세그먼트·상장 기준)")
        st.caption("후보 = 유지관리되는 한국 F&B 섹터 회사 리스트. "
                   "시총은 pykrx, 종목코드·공식영문명은 DART로 자동 보강(graceful).")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            target_n = st.number_input("유니버스 크기", 5, 40, uni.DEFAULT_TARGET_N, key="plan_n")
        with c2:
            watch_n = st.number_input("관찰군(watchlist)", 0, 30, uni.DEFAULT_WATCHLIST_N,
                                      key="plan_w")
        with c3:
            listed_only = st.checkbox("상장사만 선정", value=True, key="plan_listed",
                                      help="투자가능 종목 중심(비상장은 watchlist).")
        with c4:
            use_mktcap = st.checkbox("시총 자동조회(pykrx)", value=False, key="plan_mc",
                                     help="체크 시 실행 환경에서 KRX 시총을 조회해 점수에 반영. "
                                          "실패해도 상장여부 기준으로 점수는 산출됩니다.")
        if use_mktcap:
            try:
                import pykrx  # noqa: F401
            except Exception:
                st.warning("pykrx 미설치 — 시총 조회가 비활성화됩니다.")
                if st.button("⬇️ pykrx · yfinance 설치"):
                    from kfnb_app.utils.pkg import pip_install
                    with st.spinner("pykrx · yfinance 설치 중…"):
                        ok, log = pip_install(["pykrx", "yfinance"])
                    (st.success if ok else st.error)(
                        "설치 완료 — 다시 시도하세요." if ok else f"설치 실패:\n\n{log[-800:]}")

        if st.button("🔎 스코어링 실행", type="primary"):
            try:
                cand = uni.load_candidates()
                names = sorted(set(cand["company_kr"].astype(str)))
                ov, ov_note = _planning_overlay(names)
                if ov:                              # DART 종목코드/영문명 반영
                    cand = uni.load_candidates(extra_map=ov)
                mc, mc_note = {}, "시총 미조회(상장여부 기준)"
                if use_mktcap:
                    from kfnb_app.ingest import prices as price_src
                    codes = [c for c in cand["krx_code"].tolist() if c]
                    mc, mc_note = price_src.market_caps(codes)
                    if not mc:                      # 조회 실패 — 눈에 띄게 안내
                        st.warning(f"시총 조회 실패: {mc_note}")
                scored = uni.score_candidates(cand, market_cap=mc)
                sel = uni.select_universe(scored, target_n=int(target_n),
                                          watchlist_n=int(watch_n), listed_only=listed_only)
                SS["plan_scored"] = sel
                SS["plan_notes"] = f"{ov_note} · {mc_note}"
                st.success(f"{len(sel)}개 후보 스코어링 완료 · {SS['plan_notes']}")
            except Exception as e:                 # noqa: BLE001
                st.error(f"스코어링 실패: {e}")

        if "plan_scored" in SS:
            sel = SS["plan_scored"]
            if SS.get("plan_notes"):
                st.caption("보강: " + SS["plan_notes"])
            st.markdown("##### 2) 유니버스 검수 — 상태·사유를 직접 수정")
            st.caption("status: selected(메인 20사) / watchlist(관찰) / excluded. "
                       "자동 제안을 애널리스트가 오버라이드하고 사유를 남깁니다(global IR 대응).")
            cols = [c for c in ["rank", "company_kr", "company_en_official", "krx_code",
                                "listed", "segment", "sub_sector", "market_cap",
                                "composite_score", "status", "selection_reason"]
                    if c in sel.columns]
            view = sel[cols].copy()
            edited = st.data_editor(
                view, hide_index=True, width="stretch", height=460, key="plan_editor",
                disabled=[c for c in cols if c not in ("status", "selection_reason")],
                column_config={
                    "market_cap": st.column_config.NumberColumn("시총(원)", format="%.0f"),
                    "status": st.column_config.SelectboxColumn(
                        "상태", options=["selected", "watchlist", "excluded"]),
                    "selection_reason": st.column_config.TextColumn("선정 사유"),
                })
            sel = sel.copy()
            sel["status"] = edited["status"].values
            sel["selection_reason"] = edited["selection_reason"].values
            sel["analyst_override"] = (
                edited["status"].values != SS["plan_scored"]["status"].values)
            SS["plan_scored"] = sel

            selected_cos = sel.loc[sel["status"] == "selected", "company_kr"].tolist()
            st.markdown(f"##### 3) 대표 브랜드 5개 — 선정 {len(selected_cos)}개사")
            st.caption("브랜드 마스터(유지관리)에서 회사별 대표 브랜드 후보를 표시. "
                       "selected 체크로 5개를 확정하세요(데이터 불필요).")
            brands = uni.candidate_brands(selected_cos)
            st.caption("회사별 대표 브랜드 5개를 **자동 추천**해 채웠습니다. "
                       "어드민이 직접 수정/추가/삭제할 수 있고, '대표 채택'된 행만 저장됩니다.")
            if len(brands):
                bed = st.data_editor(
                    brands[["company_kr", "brand_kr", "brand_en", "selected",
                            "selection_reason"]],
                    hide_index=True, width="stretch", height=380, key="plan_brand_editor",
                    num_rows="dynamic",
                    column_config={
                        "company_kr": st.column_config.TextColumn("회사"),
                        "brand_kr": st.column_config.TextColumn("브랜드(한글)"),
                        "brand_en": st.column_config.TextColumn("브랜드(영문)"),
                        "selected": st.column_config.CheckboxColumn("대표 채택"),
                        "selection_reason": st.column_config.TextColumn("사유")})
                bed = bed[bed["brand_kr"].astype(str).str.strip() != ""]
                SS["plan_brands"] = bed[bed["selected"]].copy()
                # 회사별 채택 수 안내(5개 권장)
                cnt = SS["plan_brands"].groupby("company_kr").size()
                short = [c for c in selected_cos if int(cnt.get(c, 0)) < 5]
                if short:
                    st.caption("⚠️ 대표 브랜드 5개 미만인 회사: " + ", ".join(short[:10])
                               + (" …" if len(short) > 10 else ""))
            else:
                SS["plan_brands"] = brands

            st.markdown("##### 4) 저장 (반기 정기관리 저장소)")
            note = st.text_input("리뷰 메모 (예: 2026 H1 정기 리뷰)", key="plan_note")
            cc1, cc2 = st.columns(2)
            with cc1:
                if st.button("💾 유니버스 저장 + 리뷰 기록", type="primary"):
                    info = uni.save_universe(sel, SS.get("plan_brands"), note=note)
                    SS["plan_saved"] = info
                    st.success(f"저장 완료 · 선정 {info['n_selected']} · 관찰 "
                               f"{info['n_watchlist']} · 브랜드 {info['n_brands']} · "
                               f"다음 리뷰 {info['next_review']}")
            with cc2:
                if selected_cos and st.button("➡️ 이 유니버스로 데이터셋 제작 시작"):
                    SS["plan_universe_companies"] = selected_cos
                    SS["mode"] = "build"
                    SS["step"] = 0
                    st.rerun()

        # 선택: 이미 확보한 데이터가 있으면 커버리지 점검 (보조)
        with st.expander("📥 (선택) 이미 확보한 데이터로 커버리지 점검"):
            st.caption("유니버스 선정 회사 중 실제 데이터로 잡히는 비율을 확인합니다. "
                       "선정 자체는 데이터 없이 이뤄지며, 이건 갭 점검용입니다.")
            up = st.file_uploader("회사/브랜드 매출 파일 (CSV/XLSX)",
                                  type=["csv", "xlsx", "xls"], key="plan_cov_upl")
            if up is not None and "plan_scored" in SS:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(up.name).suffix)
                tmp.write(up.getbuffer()); tmp.flush()
                try:
                    mp = dataio.open_source(tmp.name).monthly_panel()
                    have = set(mp["company_kr"].astype(str))
                    sel = SS["plan_scored"]
                    want = sel.loc[sel["status"] == "selected", "company_kr"].tolist()
                    covered = [c for c in want if c in have]
                    missing = [c for c in want if c not in have]
                    st.metric("데이터 확보 커버리지",
                              f"{len(covered)}/{len(want)}개사")
                    if missing:
                        st.warning("데이터 미확보(원천사 요청 대상): " + ", ".join(missing))
                except Exception as e:             # noqa: BLE001
                    st.error(f"커버리지 점검 실패: {e}")

    # ── 상품 패키지 구조 ──────────────────────────────────────────────────────
    with tab_p:
        st.markdown("##### 레이어(빌딩블록)")
        st.dataframe(pkg.layer_table(), hide_index=True, width="stretch")
        st.markdown("##### 판매 패키지 비교 — **Professional 이 대표 상품**")
        st.dataframe(pkg.package_matrix(), hide_index=True, width="stretch")
        st.markdown("##### 분석 질문 → 필요한 상품")
        st.dataframe(pkg.question_table(), hide_index=True, width="stretch")
        with st.expander("브랜드 선정 기준 (IR 대응)"):
            for rank, desc in pkg.BRAND_SELECTION_CRITERIA:
                st.markdown(f"- **{rank}** — {desc}")
        with st.expander("패키지별 영문 피치 (global IR)"):
            for p in pkg.PACKAGES:
                st.markdown(f"**{p.tier} · {p.name}** — {p.positioning}\n\n"
                            f"> {p.pitch_en}\n\n타깃: {p.target}")

    # ── 상품 추천 (트렌드·컨센서스) ───────────────────────────────────────────
    with tab_r:
        from kfnb_app.strategy import recommender as rec
        from kfnb_app.ingest import trends as trends_src

        sel = SS.get("plan_scored")
        if sel is None or "status" not in getattr(sel, "columns", []):
            st.info("먼저 '🎯 유니버스 관리'에서 스코어링을 실행하세요. "
                    "추천은 선정 유니버스 위에서 외부 신호를 결합합니다.")
        else:
            selected_cos = sel.loc[sel["status"] == "selected", "company_kr"].tolist()
            brands = SS.get("plan_brands")
            if brands is None or not len(brands):
                brands = uni.candidate_brands(selected_cos)
            st.markdown("##### 1) 외부 신호 수집 — 트렌드 (graceful)")
            st.caption("선정 회사의 대표 브랜드를 키워드로 구글 트렌드 모멘텀을 조회합니다. "
                       "pytrends 는 구글 트렌드를 파이썬으로 가져오는 라이브러리입니다 "
                       "(비공식·무료, 키 불필요). 미설치면 아래 버튼으로 설치하세요.")
            try:
                import pytrends  # noqa: F401
                _has_pytrends = True
            except Exception:
                _has_pytrends = False
            if not _has_pytrends:
                st.warning("pytrends 미설치 — 트렌드 자동조회가 비활성화됩니다. "
                           "설치 없이도 수동 신호(신제품·컨센서스)로 추천은 가능합니다.")
                if st.button("⬇️ pytrends 설치"):
                    from kfnb_app.utils.pkg import pip_install
                    with st.spinner("pytrends 설치 중…"):
                        ok, log = pip_install(["pytrends"])
                    (st.success if ok else st.error)(
                        "설치 완료 — 다시 시도하세요." if ok else f"설치 실패:\n\n{log[-800:]}")
            if st.button("🔥 트렌드 자동조회 (pytrends)", disabled=not _has_pytrends):
                kws = brands["brand_kr"].astype(str).tolist()[:25] if len(brands) else selected_cos
                tdf, tnote = trends_src.google_trends(kws)
                SS["rec_trends"] = tdf
                (st.success if len(tdf) else st.warning)(tnote)

            st.markdown("##### 2) (선택) 수동 신호 입력 — 신제품 수 · 컨센서스")
            st.caption("'컨센서스보다 깊은 인사이트'를 평가하려면 컨센 리비전/분산을 입력하세요"
                       "(없으면 트렌드 위주로 추천).")
            base = pd.DataFrame({"company_kr": selected_cos,
                                 "new_product_count": [0] * len(selected_cos),
                                 "consensus_revision": [0.0] * len(selected_cos),
                                 "consensus_dispersion": [0.0] * len(selected_cos)})
            manual = st.data_editor(
                base, hide_index=True, width="stretch", height=260, key="rec_manual",
                disabled=["company_kr"],
                column_config={
                    "new_product_count": st.column_config.NumberColumn("최근 신제품 수"),
                    "consensus_revision": st.column_config.NumberColumn(
                        "컨센 리비전(-1~1)", min_value=-1.0, max_value=1.0),
                    "consensus_dispersion": st.column_config.NumberColumn(
                        "컨센 분산(0~1)", min_value=0.0, max_value=1.0)})

            if st.button("🧭 추천 생성", type="primary"):
                trends_df = SS.get("rec_trends")
                npd = manual[manual["new_product_count"] > 0][
                    ["company_kr", "new_product_count"]]
                cons = manual[(manual["consensus_revision"] != 0) |
                              (manual["consensus_dispersion"] != 0)][
                    ["company_kr", "consensus_revision", "consensus_dispersion"]]
                sig = rec.assemble_signals(
                    sel, brands_df=brands,
                    trends_df=trends_df if (trends_df is not None and len(trends_df)) else None,
                    consensus_df=cons if len(cons) else None,
                    newproduct_df=npd if len(npd) else None)
                scored = rec.score_signals(sig)
                SS["rec_result"] = rec.recommend(scored)
                SS["rec_segment"] = rec.segment_recommendations(scored)
                SS["rec_pkg"] = rec.trend_packaging(scored, trends_df, brands)
                SS["rec_summary"] = rec.recommendation_summary(SS["rec_result"])

            if "rec_result" in SS:
                summ = SS["rec_summary"]
                st.markdown("##### 3) 추천 결과")
                st.caption(f"데이터 충실도: {summ.get('with_signal', 0)}/{summ['n']}개사 신호 보유 · "
                           f"⚠️ {summ.get('caveat', '')}")
                st.dataframe(
                    SS["rec_result"][["company_kr", "segment", "heat",
                                      "consensus_opportunity", "recommended_action",
                                      "confidence", "rationale"]],
                    hide_index=True, width="stretch", height=380)
                st.markdown("**세그먼트 딥다이브 우선순위**")
                st.dataframe(SS["rec_segment"], hide_index=True, width="stretch")
                if SS.get("rec_pkg"):
                    st.markdown("**트렌드 패키징 제안**")
                    for p in SS["rec_pkg"]:
                        st.markdown(f"- **{p['theme']}** — 동인: {p['drivers']}\n\n  "
                                    f"  {p['suggestion']}")


def _badge(sev: str) -> str:
    icon, color = _SEV.get(sev, ("·", "#6b7280"))
    return (f"<span style='background:{color};color:#fff;border-radius:6px;"
            f"padding:2px 8px;font-size:12px;font-weight:600;'>{icon} {sev.upper()}</span>")


def render_checks(val: dict):
    st.markdown(f"검증 결과 &nbsp; {_badge(val['max_severity'])}",
                unsafe_allow_html=True)
    rows = [{"항목": c["label"], "상태": _SEV.get(c["severity"], ("·",))[0],
             "내용": c["detail"]} for c in val["checks"]]
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    return val


def render_stepper():
    cur = SS.get("step", 0)
    chips = []
    for i, name in enumerate(STEPS):
        if i < cur:
            chips.append(f"✅ {name}")
        elif i == cur:
            chips.append(f"**🔵 {name}**")
        else:
            chips.append(f"⚪ {name}")
    st.markdown("  →  ".join(chips))
    st.divider()


def nav(prev: int | None, next_cb=None, next_label="확인하고 다음 →",
        next_disabled=False, next_type="primary"):
    """이전/다음 버튼. next_cb 가 None 이면 다음 버튼 없음.

    단계별 고유 key 부여 — 라벨이 같아도 위젯이 충돌하지 않도록.
    """
    cur = SS.get("step", 0)
    st.divider()
    l, _, r = st.columns([1, 4, 1])
    with l:
        if prev is not None and st.button("← 이전", width="stretch",
                                          key=f"nav_prev_{cur}"):
            go_to(prev)
    with r:
        if next_cb is not None:
            if st.button(next_label, type=next_type, width="stretch",
                         disabled=next_disabled, key=f"nav_next_{cur}"):
                next_cb()


# ════════════════════════════════════════════════════════════════════════════
st.title("🍜 K-F&B 데이터 상품 에이전트")
st.caption("단계마다 검증 → 검토·수정 → 승인하며 최종 투자등급 데이터(xlsx)를 만듭니다.")

with st.sidebar:
    st.header("⚙️ 기본 설정")
    SS.setdefault("label", "K-Food")
    SS.setdefault("focus_brand", "불닭볶음면")
    SS["label"] = st.text_input("상품 라벨", value=SS["label"])
    SS["focus_brand"] = st.text_input(
        "(선택) 하이라이트 브랜드", value=SS["focus_brand"],
        help="⑥패널에 이 브랜드 추세를 따로 표시. 전수 use-case 발굴은 ⑦단계에서 자동 수행.")
    st.divider()
    # DART Open API 키 — 종목코드·공식영문명 자동해석(④단계). st.secrets > 입력
    _dart_default = ""
    try:
        _dart_default = st.secrets.get("DART_API_KEY", "")  # type: ignore[attr-defined]
    except Exception:
        _dart_default = os.environ.get("DART_API_KEY", "")
    SS.setdefault("dart_api_key", _dart_default)
    SS["dart_api_key"] = st.text_input(
        "DART API 키 (선택)", value=SS["dart_api_key"], type="password",
        help="입력 시 ④단계에서 종목코드·공식 영문 법인명을 공시 기준으로 자동 보강합니다. "
             "https://opendart.fss.or.kr 에서 무료 발급.")
    st.divider()
    st.caption(f"엔진: {'duckdb' if dataio._HAS_DUCKDB else 'pandas'} · "
               f"v{__import__('kfnb_app').__version__}")
    if st.button("🔄 처음부터 다시"):
        for k in list(SS.keys()):
            del SS[k]
        st.rerun()

# ── 최상단 모드 선택: 상품 기획 ↔ 데이터셋 제작 ───────────────────────────────
SS.setdefault("mode", "plan")
_MODE_LABELS = {"plan": "📋 상품 기획 · 유니버스 관리", "build": "🏭 데이터셋 제작"}
_mode = st.radio("작업 모드", list(_MODE_LABELS.keys()),
                 format_func=lambda k: _MODE_LABELS[k],
                 horizontal=True, key="mode", label_visibility="collapsed")

if _mode == "plan":
    render_planning()
    st.stop()

# ── 데이터셋 제작 모드 (스텝형) ───────────────────────────────────────────────
if SS.get("plan_universe_companies"):
    st.success("🎯 기획 단계에서 선정한 유니버스 "
               f"{len(SS['plan_universe_companies'])}개사로 데이터셋을 제작합니다: "
               + ", ".join(SS["plan_universe_companies"][:8])
               + (" …" if len(SS["plan_universe_companies"]) > 8 else ""))

SS.setdefault("step", 0)
render_stepper()
step = SS["step"]


# ── STEP 0: 데이터 입력 ──────────────────────────────────────────────────────
if step == 0:
    st.subheader("데이터 입력")
    mode = st.radio("입력 방식", ["파일 업로드", "서버 경로 (대용량 권장)"],
                    horizontal=True, label_visibility="collapsed")
    path = None
    if mode == "파일 업로드":
        up = st.file_uploader("원천 POS 파일 (CSV / XLSX)", type=["csv", "xlsx", "xls"])
        if up is not None:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(up.name).suffix)
            tmp.write(up.getbuffer()); tmp.flush()
            path = tmp.name; SS["src_name"] = up.name
            st.success(f"✅ {up.name} ({up.size/1e6:.1f} MB)")
    else:
        p = st.text_input("서버상의 CSV 경로")
        if p and Path(p).exists():
            path = p; SS["src_name"] = Path(p).name
            st.success(f"✅ {p}")
        elif p:
            st.error("경로를 찾을 수 없습니다.")

    # ── 데이터 출처 명세(Data Spec) — 데이터셋마다 다름. 결론/적합성 근거 ──
    from kfnb_app.config import DataSpec, DATA_SPEC_DEFAULT
    with st.expander("📑 데이터 출처 명세 (Data Spec) — 결론·백테스트 적합성의 근거", expanded=False):
        st.caption("이 명세가 결론·QA·available_date를 좌우합니다. 모르면 'unknown'으로 두세요(거짓 없이 표기됨).")
        d = DATA_SPEC_DEFAULT
        c1, c2, c3 = st.columns(3)
        with c1:
            channel = st.text_input("채널 범위", value="CU 편의점", key="ds_chan")
            population = st.selectbox("모집단", ["unknown", "census", "sample",
                "multi_channel"], index=0, key="ds_pop")
        with c2:
            amount_basis = st.selectbox("매출 정의", ["vat_incl_retail", "vat_excl",
                "discounted_net", "unknown"], index=0, key="ds_amt")
            qty_basis = st.selectbox("수량 정의", ["selling_unit", "each", "unknown"],
                index=0, key="ds_qty")
        with c3:
            cadence = st.selectbox("입수주기", ["unknown", "daily", "weekly", "monthly"],
                index=0, key="ds_cad")
            lag_days = st.number_input("입수지연(일)", 0, 90, int(d.release_lag_days),
                key="ds_lag")
            restatement = st.selectbox("정정 여부", ["unknown", "none", "revised"],
                index=0, key="ds_rest")
        SS["data_spec"] = DataSpec(
            amount_basis=amount_basis, qty_basis=qty_basis, currency="KRW",
            channel_scope=channel, population=population, release_cadence=cadence,
            release_lag_days=int(lag_days), restatement=restatement)

    def _load():
        from kfnb_app.ingest import schema_mapper
        # 헤더만 읽어 최소조건(날짜+회사+매출) 검증. 나머지는 자동 보완.
        cols = dataio.peek_columns(path)
        owner = schema_mapper.detect_owner(cols)
        missing = schema_mapper.missing_required(cols, owner)
        caps = schema_mapper.capabilities(cols, owner)
        if missing:
            ren = schema_mapper.rename_map(cols, owner)
            st.session_state["_schema_error"] = {
                "missing": missing, "mapped": ren, "cols": cols, "owner": owner}
            st.rerun()
        SS["src_path"] = path
        SS["capabilities"] = caps
        SS.pop("_schema_error", None)
        with st.spinner("프로파일링 중… (전체 스캔, 대용량은 수십 초)"):
            src = _src()
            SS["profile"] = profile_mod.build_profile(src)
            SS["categories"] = src.category_options()
        go_to(1)

    # 입력 가능 여부 미리보기 (비차단)
    if path:
        from kfnb_app.ingest import schema_mapper as _sm
        _cols = dataio.peek_columns(path)
        _miss = _sm.missing_required(_cols)
        _caps = _sm.capabilities(_cols)
        if _miss:
            st.error(f"❌ 최소조건 미충족 — 다음 표준 컬럼이 필요합니다(최소): "
                     f"**{', '.join(_miss)}**. (날짜·회사·매출만 있으면 처리됩니다)")
        else:
            grain_ko = {"sku": "SKU 단위", "brand": "브랜드 단위",
                        "company": "회사 단위"}.get(_caps["grain"], _caps["grain"])
            st.success(f"✅ 처리 가능 — **{grain_ko}** 데이터로 인식. "
                       f"누락 컬럼은 자동 보완됩니다.")
            if _caps["missing_recommended"]:
                st.caption("ℹ️ 미제공(자동 보완·해당 분석 축소): "
                           + ", ".join(_caps["missing_recommended"])
                           + f"  · 수량 없음 → ASP 생략" * (0 if _caps["has_qty"] else 1))

    err = SS.get("_schema_error")
    if err:
        st.error("❌ 최소조건(날짜·회사·매출) 표준 컬럼을 찾지 못했습니다. "
                 f"**누락**: {', '.join(err['missing'])}")
        st.caption(f"감지된 오너 프로파일: `{err['owner']}` · 매핑된 컬럼 "
                   f"{len(err['mapped'])}/{len(err['cols'])}개")
        with st.expander("이 파일의 원천 컬럼 보기 / 매핑 추가 방법", expanded=True):
            st.write("**원천 컬럼:**", ", ".join(f"`{c}`" for c in err["cols"]))
            st.write("**매핑된 표준 컬럼:**",
                     ", ".join(f"`{r}`→`{c}`" for r, c in err["mapped"].items()) or "(없음)")
            st.markdown(
                "`kfnb_app/configs/owner_schema_mapping.yaml` 의 오너 블록에 원천 컬럼명을 "
                "표준 컬럼(date·company_kr·sales_amt 등) 후보로 추가하면 인식됩니다. "
                "예: `company_kr: [제조사, 가맹점명, merchant, vendor, ...]`")

    nav(prev=None, next_cb=(_load if path else None),
        next_label="🚀 적재 + 프로파일링", next_disabled=path is None)


# ── STEP 1: 프로파일 검토 + 섹터 선택 ───────────────────────────────────────
elif step == 1:
    st.subheader("① 프로파일링 — 검토 후 섹터 선택")
    prof = SS["profile"]
    s, q = prof["summary"], prof["quality"]
    c = st.columns(5)
    c[0].metric("기간", s["period"].split(" ~ ")[0] + " ~")
    c[1].metric("행 수", f"{s['rows']:,}")
    c[2].metric("회사", s["companies"])
    c[3].metric("브랜드", s["brands"])
    c[4].metric("SKU", s["skus"])

    val = validation.validate_profile(prof, prof["canonical_cols"])
    render_checks(val)
    if val["halt"]:
        st.error("🛑 프로파일 검증 실패 — 원천 데이터를 확인하세요.")

    st.markdown("#### 상품화할 섹터(cat_l2) 선택")
    cats = SS["categories"].copy()
    cats["sales_amt"] = cats["sales_amt"].map(lambda v: f"{v/1e8:,.0f}억")
    st.dataframe(cats.rename(columns={"cat_l2": "카테고리", "skus": "SKU수",
                                      "sales_amt": "매출"}),
                 hide_index=True, width="stretch")
    opts = SS["categories"]["cat_l2"].tolist() + ["(전체)"]
    SS.setdefault("sector", opts[0])
    SS["sector"] = st.selectbox("이 섹터를 상품화합니다", opts,
                                index=opts.index(SS["sector"]) if SS["sector"] in opts else 0)

    def _approve():
        sec = None if SS["sector"] == "(전체)" else SS["sector"]
        with st.spinner("SKU 정규화 중…"):
            src = _src()
            skus = src.distinct_skus()
            if sec:
                skus = skus[skus["cat_l2"] == sec].reset_index(drop=True)
            SS["sku_master"] = normalize.normalize_skus(skus)
        go_to(2)

    nav(prev=0, next_cb=_approve, next_disabled=val["halt"])


# ── STEP 2: 정규화 검토 ──────────────────────────────────────────────────────
elif step == 2:
    st.subheader("② 정규화 — SKU 파싱 검토")
    sku = SS["sku_master"]
    st.caption(f"섹터: **{SS['sector']}** · {len(sku)} SKU · "
               "회사명 prefix 제거, 멀티팩/포장/variant 파싱, ASP 계산")
    show = sku[["company_kr", "brand_kr", "sku_name_kr", "product_family",
                "variant", "package_format", "pack_count", "asp_won",
                "sales_amt"]]
    st.dataframe(show, hide_index=True, width="stretch", height=420)
    render_checks(validation.validate_normalize(sku))

    def _approve():
        SS["sku_master"] = tagging.tag_skus(SS["sku_master"])
        SS["coverage"] = tagging.theme_coverage(SS["sku_master"])
        go_to(3)

    nav(prev=1, next_cb=_approve)


# ── STEP 3: 투자 태깅 + 수정 ─────────────────────────────────────────────────
elif step == 3:
    st.subheader("③ 투자 태깅 — 검토 후 수정")
    sku = SS["sku_master"]
    cov = SS["coverage"]
    st.caption("자동 태깅 결과입니다. **investment_theme 열을 직접 수정**할 수 있어요. "
               f"테마 매출비중: " +
               ", ".join(f"{k} {v}%" for k, v in
                         sorted(cov.items(), key=lambda x: -x[1])[:5]))

    edit_cols = ["company_kr", "brand_kr", "sku_name_kr", "investment_theme", "sales_amt"]
    edited = st.data_editor(
        sku[edit_cols], hide_index=True, width="stretch", height=420,
        disabled=["company_kr", "brand_kr", "sku_name_kr", "sales_amt"],
        column_config={"investment_theme": st.column_config.TextColumn(
            "Investment Theme (수정 가능)")},
        key="tag_editor")
    render_checks(validation.validate_tagging(cov))

    def _approve():
        SS["sku_master"]["investment_theme"] = edited["investment_theme"].values
        SS["sku_master"], SS["map_report"] = _remap(SS["sku_master"])
        go_to(4)

    nav(prev=2, next_cb=_approve, next_label="태그 확정하고 다음 →")


# ── STEP 4: 티커 매핑 승인 + 수정 ───────────────────────────────────────────
elif step == 4:
    st.subheader("④ 티커 매핑 — 검수 후 승인")
    sku = SS["sku_master"]
    rep = SS["map_report"]
    SS.setdefault("map_overrides", {})

    # DART 자동해석 제안 — 종목코드·공식 영문명을 공시 기준으로 보강
    _dart_resolved = rep.get("dart_resolved") or []
    _dart_note = rep.get("dart_note") or ""
    if _dart_resolved:
        st.success(f"🤖 DART 자동해석: 공시 기준으로 **{len(_dart_resolved)}개 회사**의 "
                   f"종목코드·공식 영문 법인명을 자동 보강했습니다 — "
                   f"{', '.join(_dart_resolved)}")
    elif SS.get("dart_api_key"):
        st.info(f"DART 자동해석 결과 없음 — {_dart_note}")
    else:
        st.caption("💡 사이드바에 DART API 키를 넣으면 종목코드·공식 영문명을 공시 기준으로 "
                   "자동 보강합니다(수동 입력 불필요).")

    # 회사 단위 요약 (공식 영문명 포함)
    grp = ["company_kr", "company_en_official", "krx_code", "bbg_ticker",
           "isin", "map_status"]
    grp = [c for c in grp if c in sku.columns]
    comp = (sku.groupby(grp, dropna=False)["sales_amt"]
            .sum().reset_index().sort_values("sales_amt", ascending=False))
    st.dataframe(comp.rename(columns={"company_kr": "회사",
                                      "company_en_official": "공식 영문명",
                                      "map_status": "상태"}),
                 hide_index=True, width="stretch")

    unmapped = rep["unmapped"]
    if unmapped:
        st.warning(f"🛑 매핑 사전에 없는 회사 {len(unmapped)}곳 — 검수 필요: "
                   f"{', '.join(unmapped)}")
        st.markdown("##### 미매핑 회사 처리")
        for co in unmapped:
            cc1, cc2 = st.columns([2, 1])
            with cc1:
                code = st.text_input(f"`{co}` 종목코드(6자리)",
                                     key=f"ov_code_{co}", max_chars=6,
                                     placeholder="예: 004370")
            with cc2:
                priv = st.checkbox("비상장 처리", key=f"ov_priv_{co}")
            SS["map_overrides"][co] = {"krx": code.strip(), "private": priv}
    else:
        st.success("✅ 모든 회사가 매핑 사전에 존재합니다.")

    render_checks(validation.validate_mapping(rep))

    # 게이트: 미매핑이 모두 (코드입력 OR 비상장) 으로 해소돼야 진행 가능
    def _resolved(co):
        ov = SS["map_overrides"].get(co, {})
        return bool(ov.get("krx")) or bool(ov.get("private"))
    blocked = [c for c in unmapped if not _resolved(c)]

    def _apply_and_next():
        df = SS["sku_master"]
        for co, ov in SS["map_overrides"].items():
            m = df["company_kr"] == co
            if ov.get("private"):
                df.loc[m, ["krx_code", "bbg_ticker", "isin"]] = ""
                df.loc[m, "listed"] = False
                df.loc[m, "map_status"] = "private (manual)"
            elif ov.get("krx"):
                code = ov["krx"]
                df.loc[m, "krx_code"] = code
                df.loc[m, "bbg_ticker"] = f"{code} KS"
                df.loc[m, "isin"] = config._krx_isin(code)
                df.loc[m, "listed"] = True
                df.loc[m, "map_status"] = "listed (manual)"
        SS["sku_master"] = df
        SS["map_report"] = mapping.mapping_report(
            df, extra_map=(SS.get("company_overlay") or None))
        # 영문 마스터링 (SKU 단위 — 빠름)
        SS["sku_master"] = mastering.enrich_sku_master(SS["sku_master"])
        SS["brand_master"] = mastering.build_brand_master(SS["sku_master"])
        SS["master_summary"] = mastering.mastering_summary(SS["sku_master"])
        go_to(5)

    if blocked:
        st.info(f"⏳ 다음 회사를 종목코드 입력 또는 비상장 처리해야 진행됩니다: "
                f"{', '.join(blocked)}")
    nav(prev=3, next_cb=_apply_and_next, next_label="매핑 승인하고 다음 →",
        next_disabled=bool(blocked))


# ── STEP 5: 영문 마스터링 검수 + 수정 ───────────────────────────────────────
elif step == 5:
    st.subheader("⑤ 영문 마스터링 — 표준 영문명 검수")
    bm = SS["brand_master"]
    summ = SS["master_summary"]
    st.caption("브랜드 영문명은 '번역'이 아니라 공식 글로벌 표준 표기입니다. "
               f"검수완료(verified) 매출비중 **{summ['verified_amt_pct']:.1f}%**. "
               "needs_review 브랜드의 영문명을 직접 확정할 수 있어요.")

    st.markdown("##### 브랜드 마스터 (영문명 수정 가능)")
    bedit = st.data_editor(
        bm[["brand_id", "brand_name_ko", "brand_name_en", "brand_aliases",
            "mapping_status"]],
        hide_index=True, width="stretch", height=300,
        disabled=["brand_id", "brand_name_ko", "brand_aliases", "mapping_status"],
        column_config={"brand_name_en": st.column_config.TextColumn("Brand(EN) 수정")},
        key="brand_editor")

    st.markdown("##### SKU 영문 표준명 (샘플)")
    st.dataframe(SS["sku_master"][["sku_id", "sku_name_kr", "sku_name_en",
                 "mapping_confidence", "mapping_status"]].head(50),
                 hide_index=True, width="stretch", height=280)
    render_checks(validation.validate_mastering(summ))

    def _approve_master():
        # 브랜드 영문명 수정 반영 → sku_master 의 brand_name_en 도 갱신
        new_en = dict(zip(bedit["brand_id"], bedit["brand_name_en"]))
        SS["brand_master"]["brand_name_en"] = SS["brand_master"]["brand_id"].map(
            lambda b: new_en.get(b, ""))
        SS["sku_master"]["brand_name_en"] = SS["sku_master"]["brand_id"].map(
            lambda b: new_en.get(b, SS["sku_master"].get("brand_name_en")))
        sec = None if SS["sector"] == "(전체)" else SS["sector"]
        with st.spinner("패널 집계 중… (전체 스캔)"):
            src = _src()
            SS["monthly_panel"] = panel.build_monthly_panel(src, sec)
            SS["annual_company"] = panel.build_annual_company(src, sec)
            SS["brand_trend"] = (panel.build_brand_trend(src, SS["focus_brand"])
                                 if SS["focus_brand"] else None)
            from kfnb_app.insight import pit
            _lag = getattr(SS.get("data_spec"), "release_lag_days", None)
            SS["pit_panel"] = pit.build_pit_panel(SS["monthly_panel"], lag_days=_lag)
        go_to(6)

    nav(prev=4, next_cb=_approve_master, next_label="영문명 확정하고 다음 →")


# ── STEP 6: 패널 집계 검토 ───────────────────────────────────────────────────
elif step == 6:
    st.subheader("⑥ 패널 집계 — 검토")
    mp = SS["monthly_panel"]
    t = config.THRESHOLDS
    outliers = panel.asp_outliers(mp, t.asp_min_won, t.asp_max_won)
    st.caption(f"월별 패널 {len(mp):,}행 · ASP 이상치 {len(outliers)}행")
    st.dataframe(mp[["ym", "company_kr", "bbg_ticker", "brand_kr",
                     "sales_amt", "asp_won"]].head(200),
                 hide_index=True, width="stretch", height=360)
    if SS.get("brand_trend") is not None and not SS["brand_trend"].empty:
        st.markdown(f"**{SS['focus_brand']} 연도별 모멘텀**")
        st.dataframe(SS["brand_trend"], hide_index=True, width="stretch")
    render_checks(validation.validate_panel(mp, outliers))

    def _to_usecase():
        from kfnb_app.insight import usecase
        with st.spinner("전수 use-case 발굴 중…"):
            SS["use_cases"] = usecase.generate(SS["monthly_panel"],
                                               SS["annual_company"], SS["sku_master"])
            SS["usecase_report"] = usecase.narrative(SS["use_cases"], SS["label"])
        go_to(7)

    nav(prev=5, next_cb=_to_usecase)


# ── STEP 7: Use-case 발굴 (전수 시그널) ─────────────────────────────────────
elif step == 7:
    st.subheader("⑦ Use-case 발굴 — 데이터셋 전수 시그널")
    uc = SS.get("use_cases")
    if uc is None or uc.empty:
        st.warning("발굴된 시그널이 없습니다 (데이터 기간/규모 확인).")
    else:
        by = uc["usecase_type"].value_counts().to_dict()
        st.caption(f"총 {len(uc)}개 시그널 — {by}. confidence 순 정렬.")
        labels_uc = {"momentum": "📈 모멘텀", "new_hit": "🆕 신제품 히트",
                     "share_shift": "🔀 점유율 이동", "asp_premium": "💰 ASP/프리미엄화"}
        pick = st.multiselect("유형 필터", list(labels_uc), default=list(labels_uc),
                              format_func=lambda k: labels_uc.get(k, k))
        view = uc[uc["usecase_type"].isin(pick)] if pick else uc
        st.dataframe(view[["rank", "usecase_type", "entity_kr", "ticker",
                           "value", "confidence", "thesis_ko"]],
                     hide_index=True, width="stretch", height=380)
        with st.expander("📄 자동 생성 내러티브 리포트"):
            st.markdown(SS.get("usecase_report", ""))
    nav(prev=6, next_cb=lambda: go_to(8))


# ── STEP 8: 고객유형 선택 + 딜리버리 패키지 생성 ────────────────────────────
elif step == 8:
    st.subheader("⑧ 데이터 상품 생성 — 고객유형 + 딜리버리 패키지")
    st.markdown("어떤 고객용 상품으로 내보낼지 선택하세요 (복수 가능):")
    labels = {"quant": "퀀트/헤지펀드 — PIT 패널 + 티커",
              "fundamental": "펀더멘탈/PM — SKU 트래커 + 모멘텀",
              "vendor": "데이터 벤더 — 정규화 feed + 사전"}
    chosen = []
    cc = st.columns(3)
    for i, (k, lab) in enumerate(labels.items()):
        with cc[i]:
            if st.checkbox(lab, value=True, key=f"prod_{k}"):
                chosen.append(k)
    incl_daily = st.checkbox("일별 거래데이터(daily_sales_en.csv, 대용량) 포함",
                             value=False)

    st.markdown("##### SKU 마스터에 포함할 컬럼 (선택)")
    from kfnb_app.mapping.mastering import (ANALYSIS_LABELS, DEFAULT_ANALYSIS,
                                            ID_LABELS, ID_COLUMNS, DEFAULT_IDS)
    cA, cB = st.columns(2)
    with cA:
        analysis_cols = st.multiselect(
            "분석 컬럼", DEFAULT_ANALYSIS, default=DEFAULT_ANALYSIS,
            format_func=lambda k: ANALYSIS_LABELS.get(k, k),
            help="신제품·맛·팩수·포장·용량·ASP 등")
    with cB:
        id_cols = st.multiselect(
            "식별자 컬럼 (ISIN/티커/GICS/Bloomberg)", list(ID_COLUMNS),
            default=DEFAULT_IDS, format_func=lambda k: ID_LABELS.get(k, k),
            help="ISIN·KRX·Bloomberg·GICS 코드를 SKU 행에 포함")
    run_alpha = st.checkbox("🔬 알파 리서치 포함 (주가 상관·공시 선행성, 외부데이터 필요)",
                            value=False)
    dart_key = ""
    if run_alpha:
        # 주가 소스(pykrx/yfinance) 가용성 점검 + 설치 플로우
        import importlib.util as _ilu
        has_pykrx = _ilu.find_spec("pykrx") is not None
        has_yf = _ilu.find_spec("yfinance") is not None
        if has_pykrx or has_yf:
            srcs = ", ".join(s for s, ok in [("pykrx", has_pykrx),
                             ("yfinance", has_yf)] if ok)
            st.success(f"✅ 주가 소스 사용 가능: {srcs}")
        else:
            st.warning("⚠️ 주가 모듈(pykrx)이 설치돼 있지 않아 주가 상관 분석이 비활성화됩니다.")
            if st.button("📥 주가 모듈 설치 (pykrx + yfinance)", key="install_pykrx"):
                from kfnb_app.utils.pkg import pip_install
                with st.spinner("pip install pykrx yfinance … (1~3분)"):
                    ok, log = pip_install(["pykrx", "yfinance"])
                if ok:
                    st.success("✅ 설치 완료 — 페이지를 새로고침하면 활성화됩니다.")
                else:
                    st.error("설치 실패 — 터미널: `python3 -m pip install --break-system-packages pykrx yfinance`")
                    st.code(log[-800:])
            st.caption("또는 터미널에서 한 번만: `pip3 install pykrx yfinance`")
        try:
            dart_default = st.secrets.get("DART_API_KEY", "")
        except Exception:
            dart_default = ""
        dart_key = st.text_input("DART_API_KEY (공시매출 선행성용, 선택)",
                                 value=dart_default, type="password")
    st.caption("최종 산출: company/brand/sku/category master + use_cases + QC + 문서 → zip")

    def _generate():
        pkg_dir = Path(tempfile.gettempdir()) / "kfnb_pkg"
        sec = None if SS["sector"] == "(전체)" else SS["sector"]
        qc_res = qc.run_qc(SS["sku_master"], SS["monthly_panel"], SS["profile"])
        cov_sum = coverage.coverage_by_sales(SS["sku_master"])
        ar = arr = None
        arep = ""
        if run_alpha:
            from kfnb_app.insight import alpha
            from kfnb_app.ingest import disclosures, prices as price_src
            codes = sorted({str(c) for c in SS["monthly_panel"].get("krx_code", [])
                            if str(c)})
            nmap = {r.krx_code: r.company_en_official
                    for r in config.COMPANY_MAP.values() if r.krx_code}
            with st.spinner("주가/공시 데이터로 알파 리서치 중…"):
                px, _ = price_src.monthly_prices(codes)
                ar = alpha.research_vs_returns(SS["monthly_panel"], px)
                rev, _ = disclosures.quarterly_revenue(codes, dart_key)
                arr = alpha.research_vs_revenue(SS["monthly_panel"], rev)
                arep = alpha.alpha_report(ar, arr, nmap, SS["label"])
            SS["alpha_report"] = arep
        pkg = bundle.build_delivery_package(
            pkg_dir, profile=SS["profile"], sku_master=SS["sku_master"],
            monthly_panel=SS["monthly_panel"], annual_company=SS["annual_company"],
            brand_trend=SS.get("brand_trend"), mapping_report=SS["map_report"],
            brand_master=SS["brand_master"], qc_result=qc_res,
            coverage_summary=cov_sum, source_name=SS.get("src_name", "?"),
            sector_label=SS["label"], focus_brand=SS["focus_brand"],
            products=chosen, sector=sec,
            src=(_src() if incl_daily else None), include_daily=incl_daily,
            use_cases=SS.get("use_cases"),
            usecase_report=SS.get("usecase_report", ""),
            alpha_returns=ar, alpha_revenue=arr, alpha_report=arep,
            pit_panel=SS.get("pit_panel"), data_spec=SS.get("data_spec"),
            analysis_cols=analysis_cols, id_cols=id_cols)
        SS["export_info"] = {"sheets": pkg["sheets"], "customers": pkg["customers"],
                             "bundle_zip": pkg["zip"], "files": pkg["files"]}
        SS["qc_result"] = qc_res
        SS["out_path"] = pkg["xlsx"]

    st.button("🧩 데이터 상품 + 마스터 번들 생성", type="primary", width="stretch",
              disabled=not chosen, on_click=_generate)

    info = SS.get("export_info")
    if info and Path(SS.get("out_path", "")).exists():
        qc_res = SS.get("qc_result", {})
        if qc_res:
            render_checks({"checks": qc_res["checks"],
                           "max_severity": qc_res["max_severity"],
                           "halt": qc_res["halt"]})
        st.success(f"🎉 완료 — xlsx {info['sheets']}시트 + 패키지 "
                   f"{len(info.get('files', []))}항목 "
                   f"(고객유형: {', '.join(info['customers'])})")
        st.caption("패키지 구성: " + ", ".join(info.get("files", [])))
        if SS.get("alpha_report"):
            with st.expander("🔬 알파 리서치 리포트 (주가 상관·공시 선행성)"):
                st.markdown(SS["alpha_report"])
        zp = info.get("bundle_zip")
        if zp and Path(zp).exists():
            with open(zp, "rb") as f:
                st.download_button("⬇️ 딜리버리 패키지 전체 (zip) 다운로드", f.read(),
                                   file_name=Path(zp).name,
                                   mime="application/zip", type="primary",
                                   width="stretch")
        with open(SS["out_path"], "rb") as f:
            st.download_button("⬇️ xlsx 요약만 다운로드", f.read(),
                               file_name=f"{SS['label']}_POS_Product.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               width="stretch")
    nav(prev=7, next_cb=lambda: go_to(9), next_label="📊 대시보드 보기 →")


# ── STEP 9: 대시보드 & 투자 적합성 ──────────────────────────────────────────
elif step == 9:
    from kfnb_app import dashboard
    dashboard.render(SS)
    nav(prev=8, next_cb=None)
