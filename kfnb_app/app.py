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

STEPS = ["① 데이터 입력", "② 적재·프로파일", "③ 회사명 매핑", "④ 브랜드명 매핑",
         "⑤ SKU명 매핑", "⑥ 카테고리 매핑", "⑦ 글로벌 식별자", "⑧ 레이아웃 변환①",
         "⑨ 레이아웃 변환②(사전·마스터)", "⑩ 다운로드"]

_SEV = {"ok": ("✅", "#16a34a"), "info": ("ℹ️", "#2563eb"),
        "warning": ("⚠️", "#d97706"), "error": ("❌", "#dc2626"),
        "critical": ("🛑", "#991b1b")}

SS = st.session_state

# 이전 실행이 남긴 임시파일 정리(디스크 회수) — 세션당 1회
if not SS.get("_temp_cleaned"):
    try:
        dataio.cleanup_temp()
    except Exception:
        pass
    SS["_temp_cleaned"] = True


# ── 공용 헬퍼 ────────────────────────────────────────────────────────────────
def go_to(step: int):
    SS["step"] = step
    st.rerun()


def _src(progress=None):
    """src_path 로부터 Source 를 연다. **세션 캐시** — 한 번만 적재하고 모든 단계가
    재사용한다(대용량 파일을 단계마다 재적재해 디스크/시간 폭발하던 문제 해결)."""
    key = (SS.get("src_path"), tuple(sorted((SS.get("col_map") or {}).items())))
    if SS.get("_source") is not None and SS.get("_source_key") == key:
        return SS["_source"]
    src = dataio.open_source(SS["src_path"],
                             extra_rename=SS.get("col_map") or None,
                             progress=progress)
    SS["_source"] = src
    SS["_source_key"] = key
    return src


def _remap(df):
    """DART 자동해석(종목코드·공식영문명)을 적용해 회사 매핑. 키 없으면 정적 마스터."""
    names = sorted(set(str(c) for c in df["company_kr"].dropna()))
    overlay, note, resolved = {}, "", {}
    try:
        from kfnb_app.ingest import dart_company
        hints = {n: config.COMPANY_MAP[n].krx_code for n in names
                 if n in config.COMPANY_MAP and config.COMPANY_MAP[n].krx_code}
        resolved, note = dart_company.resolve(names, SS.get("dart_api_key", ""),
                                              code_hints=hints)
        overlay = mapping.dart_overlay(resolved) if resolved else {}
    except Exception as e:  # noqa: BLE001 — 비차단
        note = f"DART 자동해석 생략: {type(e).__name__}"
    # 업로드한 마스터가 있으면 회사 코드/영문명을 최우선 적용(DART보다 우선)
    um = SS.get("user_master")
    if um and um.get("company"):
        try:
            from kfnb_app.mapping import master_io
            overlay = {**overlay, **master_io.company_overlay(um)}
        except Exception:                          # noqa: BLE001
            pass
    SS["company_overlay"] = overlay
    SS["dart_note"] = note
    SS["dart_resolved_raw"] = resolved or {}       # 회사명 → {corp_code, krx_code, company_en_official, jurir_no}
    # 법인등록번호 맵 (업로드 마스터 > DART 공시)
    jmap = {co: str((d or {}).get("jurir_no", "") or "")
            for co, d in (resolved or {}).items()}
    for co, d in ((um or {}).get("company") or {}).items():
        if d.get("jurir_no"):
            jmap[co] = str(d["jurir_no"])
    SS["jurir_map"] = jmap
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
        hints = {n: config.COMPANY_MAP[n].krx_code for n in names
                 if n in config.COMPANY_MAP and config.COMPANY_MAP[n].krx_code}
        resolved, note = dart_company.resolve(names, key, code_hints=hints)
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


def render_analyze():
    """🔬 분석 모드 — 코어(제작) 이후의 알파·대시보드·투자적합성·번들.

    제작 모드에서 만든 sku_master 위에서 동작. 주가/공시는 사용자 환경에서 graceful.
    """
    st.subheader("🔬 분석 — 알파 · Use-case · 대시보드")
    sku = SS.get("sku_master")
    if sku is None or not len(sku):
        st.info("먼저 '🏭 데이터셋 제작'에서 ③ 회사명 매핑까지 진행해 sku_master 를 "
                "만든 뒤 분석 모드로 오세요. 분석은 제작 결과 위에서 수행됩니다.")
        return
    sec = None if SS.get("sector", "(전체)") == "(전체)" else SS["sector"]
    # 패널/시그널 준비 (분석 진입 시 1회)
    if st.button("🔁 분석 데이터 준비/갱신 (패널·PIT·시그널)") or "monthly_panel" not in SS:
        with st.spinner("패널·PIT·알파 시그널 준비 중…"):
            src = _src()
            SS["monthly_panel"] = panel.build_monthly_panel(src, sec)
            SS["annual_company"] = panel.build_annual_company(src, sec)
            from kfnb_app.insight import pit, signal_engine
            _lag = getattr(SS.get("data_spec"), "release_lag_days", None)
            SS["pit_panel"] = pit.build_pit_panel(SS["monthly_panel"], lag_days=_lag)
            SS["alpha_panel_df"] = signal_engine.build_alpha_panel(
                src, sku, sector=sec, lag_days=_lag)
    tabs = st.tabs(["📊 대시보드·적합성", "🧭 알파 패널", "🔬 백테스트(주가/공시)"])
    with tabs[0]:
        try:
            from kfnb_app import dashboard
            dashboard.render(SS)
        except Exception as e:                     # noqa: BLE001
            st.warning(f"대시보드 렌더 일부 생략: {e}")
    with tabs[1]:
        ap = SS.get("alpha_panel_df")
        if ap is not None and len(ap):
            st.caption(f"종목단위 PIT 알파 패널 — {ap['ticker'].nunique()}종목 · {len(ap)}행")
            st.dataframe(ap.head(300), hide_index=True, width="stretch", height=420)
        else:
            st.info("위 '준비/갱신'을 눌러 알파 패널을 생성하세요.")
    with tabs[2]:
        st.caption("주가 상관·공시매출 선행성(IC·분위수). pykrx/yfinance·DART 키 필요(없으면 graceful).")
        if st.button("주가/공시로 백테스트 리포트 생성"):
            from kfnb_app.insight import alpha
            from kfnb_app.ingest import disclosures, prices as price_src
            mp = SS["monthly_panel"]
            codes = sorted({str(c) for c in mp.get("krx_code", []) if str(c)})
            nmap = {r.krx_code: r.company_en_official
                    for r in config.COMPANY_MAP.values() if r.krx_code}
            with st.spinner("주가/공시 조회·분석 중…"):
                px, pnote = price_src.monthly_prices(codes)
                ar = alpha.research_vs_returns(mp, px)
                rev, _ = disclosures.quarterly_revenue(codes, SS.get("dart_api_key", ""))
                arr = alpha.research_vs_revenue(mp, rev)
                SS["alpha_report"] = alpha.alpha_report(ar, arr, nmap, SS.get("label", "K-FnB"))
            st.caption(f"주가: {pnote}")
        if SS.get("alpha_report"):
            st.markdown(SS["alpha_report"])


def _sheet_layout_editor(sheet: str, available: list, default_layout: list, key: str):
    """탭별 컬럼 매핑 편집기 — 출력컬럼명 ↔ 원본필드(데이터) 선택. SS['wb_layouts'] 저장."""
    SS.setdefault("wb_layouts", {})
    SS.setdefault("wb_done", {})
    cur = SS["wb_layouts"].get(sheet) or default_layout
    df = pd.DataFrame([{"출력컬럼명": c["name"], "원본필드": c["from"]} for c in cur])
    st.caption("출력컬럼명=내보낼 헤더 · 원본필드=끌어올 데이터. 행 추가/삭제·순서 변경 가능. "
               f"사용 가능 원본필드: {', '.join(available)}")
    ed = st.data_editor(
        df, hide_index=True, width="stretch", height=320, num_rows="dynamic", key=key,
        column_config={
            "출력컬럼명": st.column_config.TextColumn("출력컬럼명(헤더)"),
            "원본필드": st.column_config.SelectboxColumn("원본필드(데이터)", options=available)})
    if st.button(f"✅ {sheet} 컬럼 적용", key=key + "_apply", type="primary"):
        SS["wb_layouts"][sheet] = [
            {"name": str(r["출력컬럼명"]).strip(), "from": str(r["원본필드"]).strip()}
            for _, r in ed.iterrows()
            if str(r["출력컬럼명"]).strip() and str(r["원본필드"]).strip()]
        SS["wb_done"][sheet] = True
        st.success(f"{sheet} 컬럼 적용 — {len(SS['wb_layouts'][sheet])}개")
        st.rerun()
    return SS["wb_layouts"].get(sheet) or default_layout


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
    # DART Open API 키 — 종목코드·공식영문명 자동해석(③단계).
    # 우선순위: st.secrets / 환경변수 > 로컬 저장파일(~/.kfnb/dart.key) > 입력
    from kfnb_app.utils import secrets_store as _ks
    _dart_default = ""
    try:
        _dart_default = st.secrets.get("DART_API_KEY", "")  # type: ignore[attr-defined]
    except Exception:
        _dart_default = os.environ.get("DART_API_KEY", "")
    if not _dart_default:
        _dart_default = _ks.load_key("dart")       # 저장해둔 키 자동 로드
    SS.setdefault("dart_api_key", _dart_default)
    SS["dart_api_key"] = st.text_input(
        "DART API 키", value=SS["dart_api_key"], type="password",
        help="③ 회사명 매핑에서 종목코드·공식영문명을 공시 기준으로 자동 보강. "
             "https://opendart.fss.or.kr 무료 발급. 아래 '저장'을 누르면 다음부터 자동 입력됩니다.")
    _saved = bool(_ks.load_key("dart"))
    cdk1, cdk2 = st.columns([3, 2])
    with cdk1:
        if st.button("💾 키 저장(기억)", width="stretch",
                     disabled=not SS["dart_api_key"]):
            ok = _ks.save_key(SS["dart_api_key"], "dart")
            st.success("저장됨 — 다음 실행부터 자동 입력" if ok else "저장 실패")
    with cdk2:
        if _saved and st.button("키 삭제", width="stretch"):
            _ks.clear_key("dart"); SS["dart_api_key"] = ""; st.rerun()
    st.caption(("🔑 저장된 키 자동 로드됨" if _saved else "🔓 미저장 — 저장 시 ~/.kfnb 에 보관")
               + " · DART " + ("연결" if SS["dart_api_key"] else "미연결"))
    # LLM(Anthropic) 키 — 브랜드/카테고리 영문명 추정(④/⑥단계, 선택)
    _llm_default = ""
    try:
        _llm_default = st.secrets.get("ANTHROPIC_API_KEY", "")  # type: ignore[attr-defined]
    except Exception:
        _llm_default = os.environ.get("ANTHROPIC_API_KEY", "")
    if not _llm_default:
        _llm_default = _ks.load_key("anthropic")
    SS.setdefault("llm_api_key", _llm_default)
    SS["llm_api_key"] = st.text_input(
        "LLM(Anthropic) 키", value=SS["llm_api_key"], type="password",
        help="④ 브랜드명 매핑에서 로마자 영문명을 공식 영문명으로 자동 추정(선택).")
    if st.button("💾 LLM 키 저장", width="stretch", disabled=not SS["llm_api_key"]):
        st.success("저장됨" if _ks.save_key(SS["llm_api_key"], "anthropic") else "저장 실패")
    st.divider()
    st.caption(f"엔진: {'duckdb' if dataio._HAS_DUCKDB else 'pandas'} · "
               f"v{__import__('kfnb_app').__version__}")
    if st.button("🔄 처음부터 다시"):
        for k in list(SS.keys()):
            del SS[k]
        st.rerun()

# ── 최상단 모드 선택: 상품 기획 ↔ 데이터셋 제작 ───────────────────────────────
SS.setdefault("mode", "plan")
_MODE_LABELS = {"plan": "📋 상품 기획 · 유니버스 관리", "build": "🏭 데이터셋 제작",
                "analyze": "🔬 분석 (알파·대시보드)"}
_mode = st.radio("작업 모드", list(_MODE_LABELS.keys()),
                 format_func=lambda k: _MODE_LABELS[k],
                 horizontal=True, key="mode", label_visibility="collapsed")

if _mode == "plan":
    render_planning()
    st.stop()

if _mode == "analyze":
    render_analyze()
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
    mode = st.radio("입력 방식",
                    ["파일 업로드", "내 컴퓨터 파일 경로 (대용량 권장)"],
                    horizontal=True, label_visibility="collapsed")
    path = None
    if mode == "파일 업로드":
        up = st.file_uploader("원천 POS 파일 (CSV / XLSX) — 최대 50GB",
                              type=["csv", "xlsx", "xls"])
        st.caption("💡 10GB 이상 초대용량 파일은 브라우저 업로드가 메모리에 적재되어 "
                   "느리거나 실패할 수 있습니다. 이 경우 위의 **'내 컴퓨터 파일 경로'** 로 "
                   "파일 경로를 직접 지정하세요(복사 없이 바로 읽어 가장 안정적).")
        if up is not None:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(up.name).suffix)
            tmp.write(up.getbuffer()); tmp.flush()
            path = tmp.name; SS["src_name"] = up.name
            st.success(f"✅ {up.name} ({up.size/1e9:.2f} GB)")
    else:
        p = st.text_input(
            "내 컴퓨터에 있는 CSV 파일 또는 폴더의 전체 경로",
            placeholder="예: /Users/yonghan/Desktop/data/포스데이터.csv  또는  …/BGF데이터셋폴더",
            help="이 앱은 내 맥에서 로컬로 돌아가므로 '서버'='내 컴퓨터'입니다. "
                 "단일 CSV(확장자 없어도 OK)·여러 CSV가 든 폴더 모두 가능. "
                 "복사·적재 없이 디스크에서 바로 스트리밍으로 읽어 대용량에 안정적입니다.")
        st.caption("📍 경로 얻는 법(macOS): Finder에서 파일/폴더 우클릭 → ⌥Option 누른 채 "
                   "**'(이름) 경로 복사'** 클릭 → 여기 붙여넣기. "
                   "회사별 CSV가 여러 개면 그 폴더 경로를 넣으면 자동 결합합니다.")
        if p and Path(p.strip().strip('"').strip("'")).exists():
            p = p.strip().strip('"').strip("'")
            path = p; SS["src_name"] = Path(p).name
            st.success(f"✅ {p}")
        elif p:
            st.error("경로를 찾을 수 없습니다.")

    # ── 기존 마스터 업로드 — 이전 작업 누적분 자동 매핑 ──
    with st.expander("📥 기존 마스터 업로드 (이전 작업 자동 적용)", expanded=False):
        st.caption("이전에 다운로드한 마스터 zip(회사·브랜드·카테고리·SKU 영문)을 올리면, "
                   "③~⑥ 매핑에서 자동 적용됩니다. 한 번 확정한 건 다시 안 해도 됩니다.")
        mz = st.file_uploader("마스터 zip", type=["zip"], key="master_upl")
        if mz is not None:
            from kfnb_app.mapping import master_io
            bundle = master_io.load_zip(mz.getvalue())
            if bundle:
                SS["user_master"] = master_io.to_overrides(bundle)
                um = SS["user_master"]
                st.success(f"마스터 로드됨 — 회사 {len(um['company'])} · 브랜드 "
                           f"{len(um['brand'])} · 카테고리 {len(um['category'])} · "
                           f"SKU {len(um['sku'])}건 자동 적용 대기")
            else:
                st.error("마스터 zip 을 읽지 못했습니다(형식 확인).")
        if SS.get("user_master"):
            um = SS["user_master"]
            st.caption(f"🔁 적용 대기 마스터: 회사 {len(um['company'])}·브랜드 {len(um['brand'])}"
                       f"·카테고리 {len(um['category'])}·SKU {len(um['sku'])}")
            if st.button("마스터 해제"):
                SS.pop("user_master", None); st.rerun()

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
        cmap = SS.get("col_map") or None
        owner = schema_mapper.detect_owner(cols)
        missing = schema_mapper.missing_required(cols, owner, cmap)
        caps = schema_mapper.capabilities(cols, owner, cmap)
        if missing:
            st.session_state["_schema_error"] = {"missing": missing, "cols": cols}
            st.rerun()
        SS["src_path"] = path
        SS["capabilities"] = caps
        SS.pop("_schema_error", None)
        pb = st.progress(0.0, "데이터 적재 준비 중…")
        def _cb(frac, msg=""):
            try:
                pb.progress(min(max(float(frac), 0.0), 1.0), msg or "적재 중…")
            except Exception:
                pass
        try:
            src = _src(progress=_cb)
            pb.progress(0.9, "프로파일링(전체 1회 스캔) 중…")
            SS["profile"] = profile_mod.build_profile(src)
            SS["categories"] = src.category_options()
            pb.progress(1.0, "완료")
        except Exception as e:                     # noqa: BLE001
            pb.empty()
            st.error(f"적재 실패: {e}")
            return
        go_to(1)

    # 입력 가능 여부 미리보기 + 수동 컬럼 매핑 (비차단)
    if path:
        from kfnb_app.ingest import schema_mapper as _sm
        _cols = dataio.peek_columns(path)
        _cmap = SS.get("col_map") or None
        _auto = _sm.rename_map(_cols)            # 자동 인식 결과(수동 제외)
        _miss = _sm.missing_required(_cols, extra_rename=_cmap)
        _caps = _sm.capabilities(_cols, extra_rename=_cmap)

        # 수동 컬럼 매핑 — 자동 인식이 놓친 컬럼을 직접 지정 (범용 대응)
        with st.expander("🔧 컬럼 매핑 확인/수정 — 자동 인식이 틀리면 여기서 직접 지정",
                         expanded=bool(_miss)):
            st.caption("이 파일에서 감지된 원천 컬럼을 표준 컬럼에 연결합니다. "
                       "자동 인식된 건 그대로 두고, 비어있는 필수 항목만 골라주세요.")
            _opts = ["(자동)"] + list(_cols)
            targets = [("date", "날짜 *"), ("company_kr", "회사 *"),
                       ("sales_amt", "매출액 *"), ("brand_kr", "브랜드"),
                       ("sku_name_kr", "상품명(SKU)"), ("barcode", "바코드/상품코드"),
                       ("cat_l2", "카테고리(중분류)"), ("sales_qty", "판매수량")]
            cur = dict(SS.get("col_map") or {})
            # 역방향(현재 canon→raw) 초기값
            canon_to_raw = {v: k for k, v in cur.items()}
            new_map = {}
            cc = st.columns(4)
            for i, (canon, label) in enumerate(targets):
                auto_raw = next((r for r, c in _auto.items() if c == canon), None)
                default = canon_to_raw.get(canon) or (auto_raw if auto_raw else "(자동)")
                idx = _opts.index(default) if default in _opts else 0
                with cc[i % 4]:
                    pick = st.selectbox(label, _opts, index=idx, key=f"map_{canon}")
                if pick != "(자동)":
                    new_map[pick] = canon
            if st.button("매핑 적용"):
                SS["col_map"] = new_map
                st.rerun()
            if SS.get("col_map"):
                st.caption("현재 수동 매핑: " +
                           ", ".join(f"`{r}`→`{c}`" for r, c in SS["col_map"].items()))

        if _miss:
            st.error(f"❌ 최소조건 미충족 — 다음 표준 컬럼이 필요합니다(최소): "
                     f"**{', '.join(_miss)}**. 위 '컬럼 매핑'에서 해당 원천 컬럼을 골라주세요. "
                     f"(감지된 원천 컬럼: {', '.join(_cols[:20])}"
                     + (" …" if len(_cols) > 20 else "") + ")")
        else:
            grain_ko = {"sku": "SKU 단위", "brand": "브랜드 단위",
                        "company": "회사 단위"}.get(_caps["grain"], _caps["grain"])
            st.success(f"✅ 처리 가능 — **{grain_ko}** 데이터로 인식. "
                       f"누락 컬럼은 자동 보완됩니다.")
            if _caps["missing_recommended"]:
                st.caption("ℹ️ 미제공(자동 보완·해당 분석 축소): "
                           + ", ".join(_caps["missing_recommended"])
                           + f"  · 수량 없음 → ASP 생략" * (0 if _caps["has_qty"] else 1))

    nav(prev=None, next_cb=(_load if path else None),
        next_label="🚀 적재 + 프로파일링", next_disabled=path is None)


# ── STEP 1: 프로파일 검토 + 섹터 선택 ───────────────────────────────────────
elif step == 1:
    st.subheader("② 적재·프로파일 — 검토 후 (선택) 섹터 범위")
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
        with st.spinner("SKU 정규화·태깅 중…"):
            src = _src()
            skus = src.distinct_skus()
            if sec:
                skus = skus[skus["cat_l2"] == sec].reset_index(drop=True)
            SS["sku_master"] = tagging.tag_skus(normalize.normalize_skus(skus))
            SS["coverage"] = tagging.theme_coverage(SS["sku_master"])
            SS.pop("_company_mapped", None)        # ③에서 다시 매핑
        go_to(2)

    nav(prev=0, next_cb=_approve, next_disabled=val["halt"], next_label="적재 확정 → 회사명 매핑")


# ── STEP 2: 회사명 매핑 (종목코드·공식영문명, 코드앵커 DART) ─────────────────
elif step == 2:
    st.subheader("③ 회사명 매핑 — 종목코드·공식 영문명")
    # 진입 시 1회: 회사 매핑(코드앵커 DART) 수행
    if not SS.get("_company_mapped"):
        with st.spinner("회사명 매핑(종목코드·공식영문명) 중…"):
            SS["sku_master"], SS["map_report"] = _remap(SS["sku_master"])
            SS["_company_mapped"] = True
    sku = SS["sku_master"]; rep = SS["map_report"]
    _dn = rep.get("dart_resolved") or []
    if _dn:
        st.success(f"🤖 DART 코드앵커 자동해석 {len(_dn)}개사 — {rep.get('dart_note','')}")
    elif SS.get("dart_api_key"):
        st.info(f"DART: {rep.get('dart_note','')}")
    else:
        st.caption("💡 사이드바 DART 키를 넣으면 종목코드·공식영문명을 공시 기준으로 보강합니다.")
    st.caption("**검증**: '적용값'(왼쪽) ↔ **DART 공시 원본**(DART영문명·DART코드·corp_code) "
               "을 나란히 비교하세요. 출처=코드앵커/이름매칭/큐레이션, 일치=적용코드와 DART코드 동일. "
               "동명회사 오매칭이면 krx_code·공식영문명·상태를 직접 고치면 ISIN·블룸버그가 재계산됩니다.")
    raw = SS.get("dart_resolved_raw", {}) or {}
    base = (sku.groupby(["company_kr", "company_en_official", "krx_code", "map_status"],
                        dropna=False)["sales_amt"].sum().reset_index()
            .sort_values("sales_amt", ascending=False))
    base["status_sel"] = base["map_status"].apply(
        lambda s: "private" if str(s).startswith("private")
        else ("listed" if str(s).startswith("listed") else "unmapped"))
    # DART 원본 비교 컬럼
    def _dval(co, k):
        return str((raw.get(str(co)) or {}).get(k, "") or "")
    base["dart_en"] = base["company_kr"].map(lambda c: _dval(c, "company_en_official"))
    base["dart_krx"] = base["company_kr"].map(lambda c: _dval(c, "krx_code"))
    base["dart_corp"] = base["company_kr"].map(lambda c: _dval(c, "corp_code"))
    base["jurir_no"] = base["company_kr"].map(lambda c: _dval(c, "jurir_no"))
    def _origin(co):
        co = str(co)
        if co in config.COMPANY_MAP and config.COMPANY_MAP[co].krx_code:
            return "큐레이션"
        if co in raw:
            return "DART"
        return "수동/미매핑"
    base["출처"] = base["company_kr"].map(_origin)
    base["일치"] = base.apply(
        lambda r: ("—" if not r["dart_krx"] else
                   ("✅" if str(r["krx_code"]).zfill(6) == str(r["dart_krx"]).zfill(6)
                    else "⚠️차이")), axis=1)
    view = base[["company_kr", "company_en_official", "krx_code", "status_sel",
                 "jurir_no", "dart_en", "dart_krx", "dart_corp", "출처", "일치",
                 "sales_amt"]].rename(
        columns={"company_kr": "회사", "company_en_official": "공식영문명(적용)",
                 "krx_code": "krx_code(적용)", "status_sel": "상태",
                 "jurir_no": "법인등록번호", "dart_en": "DART영문명", "dart_krx": "DART코드",
                 "dart_corp": "corp_code", "sales_amt": "매출"})
    edited = st.data_editor(
        view, hide_index=True, width="stretch", height=440, key="map_editor",
        disabled=["회사", "매출", "DART영문명", "DART코드", "corp_code", "출처", "일치"],
        column_config={
            "공식영문명(적용)": st.column_config.TextColumn("공식영문명(적용)"),
            "krx_code(적용)": st.column_config.TextColumn("krx_code(적용,6자리)", max_chars=6),
            "상태": st.column_config.SelectboxColumn(
                "상태", options=["listed", "private", "unmapped"]),
            "법인등록번호": st.column_config.TextColumn("법인등록번호(공시)"),
            "DART영문명": st.column_config.TextColumn("DART영문명(공시)"),
            "DART코드": st.column_config.TextColumn("DART코드"),
            "corp_code": st.column_config.TextColumn("corp_code"),
            "출처": st.column_config.TextColumn("출처"),
            "일치": st.column_config.TextColumn("적용=DART?"),
            "매출": st.column_config.NumberColumn("매출", format="%.0f")})
    edited = edited.rename(columns={"공식영문명(적용)": "공식영문명",
                                    "krx_code(적용)": "krx_code"})
    _mismatch = [r["회사"] for _, r in edited.iterrows()
                 if base.loc[base["company_kr"] == r["회사"], "일치"].iloc[0] == "⚠️차이"]
    if _mismatch:
        st.warning("⚠️ 적용값과 DART 공시코드가 다른 회사(검증 필요): " + ", ".join(_mismatch[:15]))

    def _bad(r):
        code = str(r["krx_code"] or "").strip()
        if r["상태"] == "listed":
            return not (code.isdigit() and len(code) == 6)
        return r["상태"] == "unmapped"
    blocked = [r["회사"] for _, r in edited.iterrows() if _bad(r)]
    render_checks(validation.validate_mapping(rep))

    def _approve():
        df = SS["sku_master"]
        if "jurir_no" not in df.columns:
            df["jurir_no"] = ""
        jur_by_co = {}
        for _, r in edited.iterrows():
            m = df["company_kr"] == r["회사"]
            en = str(r["공식영문명"] or "").strip(); code = str(r["krx_code"] or "").strip()
            jur = str(r.get("법인등록번호", "") or "").strip()
            if en:
                df.loc[m, "company_en_official"] = en
            if jur:
                df.loc[m, "jurir_no"] = jur
                jur_by_co[r["회사"]] = jur
            if r["상태"] == "private":
                df.loc[m, ["krx_code", "bbg_ticker", "isin", "bloomberg_code"]] = ""
                df.loc[m, "listed"] = False; df.loc[m, "map_status"] = "private (manual)"
            elif r["상태"] == "listed" and code:
                df.loc[m, "krx_code"] = code
                df.loc[m, "bbg_ticker"] = f"{code} KS"
                df.loc[m, "bloomberg_code"] = f"{code} KS Equity"
                df.loc[m, "isin"] = config._krx_isin(code)
                df.loc[m, "listed"] = True; df.loc[m, "map_status"] = "listed (manual)"
        SS["sku_master"] = df
        SS["map_report"] = mapping.mapping_report(
            df, extra_map=(SS.get("company_overlay") or None))
        # 영문 마스터링(브랜드/SKU 영문 부착) — 이후 ④⑤에서 검수
        SS["sku_master"] = mastering.enrich_sku_master(SS["sku_master"])
        # 법인등록번호 컬럼 보존(enrich 후 재부착)
        if jur_by_co:
            cur = SS["sku_master"].get("jurir_no")
            SS["sku_master"]["jurir_no"] = SS["sku_master"]["company_kr"].map(
                lambda c: jur_by_co.get(str(c), "")) if cur is None else \
                SS["sku_master"].apply(
                    lambda r: jur_by_co.get(str(r["company_kr"]), r.get("jurir_no", "")), axis=1)
        # 업로드한 마스터(이전 작업 누적) 자동 적용 — 회사/브랜드/카테고리/SKU 영문
        if SS.get("user_master"):
            from kfnb_app.mapping import master_io
            SS["sku_master"] = master_io.apply_overrides(SS["sku_master"], SS["user_master"])
        SS["brand_master"] = mastering.build_brand_master(SS["sku_master"])
        SS["master_summary"] = mastering.mastering_summary(SS["sku_master"])
        go_to(3)

    if blocked:
        st.info(f"⏳ 상태=listed 면 krx_code 6자리, 아니면 private 로: {', '.join(blocked[:15])}")
    nav(prev=1, next_cb=_approve, next_label="회사명 확정 → 브랜드 매핑",
        next_disabled=bool(blocked))


# ── STEP 3: 브랜드명 매핑 (영문, 없으면 수동 SKIP) ──────────────────────────
elif step == 3:
    st.subheader("④ 브랜드명 매핑 — 영문 (없으면 SKIP)")
    from kfnb_app.mapping import review as _rev
    has_brand = SS["sku_master"]["brand_kr"].astype(str).str.strip().replace(
        {"": None, "(unknown)": None}).notna().any()
    if not has_brand:
        st.info("이 데이터에는 식별 가능한 브랜드가 없습니다(회사단위). 브랜드 매핑을 SKIP 하세요.")
    else:
        cov = _rev.coverage_summary(SS["sku_master"])
        st.caption(f"브랜드 검증 매출비중 **{cov['brand_verified_pct']}%**. "
                   "미검증 브랜드를 매출순으로 띄웁니다 — LLM 추정 또는 직접 확정하세요.")
        q = _rev.brand_review_queue(SS["sku_master"], top=60)
        # LLM 추정 제안을 brand_en 에 미리 채움
        llm = SS.get("llm_brand_en", {})
        if len(q) and llm:
            q = q.copy()
            q["brand_en"] = q.apply(
                lambda r: llm.get((r["company_kr"], r["brand_kr"]), r["brand_en"]), axis=1)
        cL, cR = st.columns([3, 2])
        with cL:
            if st.button("🤖 LLM으로 영문명 추정 (로마자→공식 영문)",
                         disabled=not SS.get("llm_api_key") or not len(q)):
                from kfnb_app.mapping import brand_llm
                # 브랜드별 카테고리(중분류 영문) 맥락 첨부 — 동명/모호성 해소
                smk = SS["sku_master"]
                catcol = "cat_l2_en" if "cat_l2_en" in smk.columns else (
                    "cat_l2" if "cat_l2" in smk.columns else None)
                cmap = {}
                if catcol:
                    cmap = (smk.dropna(subset=["brand_kr"])
                            .groupby(["company_kr", "brand_kr"])[catcol]
                            .agg(lambda s: s.mode().iloc[0] if len(s.mode()) else "")
                            .to_dict())
                items = [(r["company_kr"], r["brand_kr"],
                          cmap.get((r["company_kr"], r["brand_kr"]), ""))
                         for _, r in q.iterrows()]
                with st.spinner("LLM 추정 중…"):
                    res, lnote = brand_llm.infer_brand_en(items, SS.get("llm_api_key", ""))
                SS["llm_brand_en"] = {**SS.get("llm_brand_en", {}), **res}
                (st.success if res else st.warning)(lnote)
                if res:
                    st.rerun()
        with cR:
            if not SS.get("llm_api_key"):
                st.caption("LLM 추정하려면 사이드바에 Anthropic 키를 넣으세요.")
        if len(q):
            qed = st.data_editor(
                q[["company_kr", "brand_kr", "brand_en", "sales_pct", "cum_pct"]],
                hide_index=True, width="stretch", height=380, key="brand_rev",
                disabled=["company_kr", "brand_kr", "sales_pct", "cum_pct"],
                column_config={"brand_en": st.column_config.TextColumn("공식 영문명(확정)"),
                               "sales_pct": st.column_config.NumberColumn("매출%", format="%.2f"),
                               "cum_pct": st.column_config.NumberColumn("누적%", format="%.1f")})
            if st.button("✅ 확정 영문명 반영", type="primary"):
                ov = {(r["company_kr"], r["brand_kr"]): r["brand_en"]
                      for i, r in qed.iterrows()
                      if str(r["brand_en"]).strip() and r["brand_en"] != q.iloc[i]["brand_en"]}
                # LLM 제안을 그대로 받아들인 경우도 반영(편집 안 했어도)
                for i, r in qed.iterrows():
                    key = (r["company_kr"], r["brand_kr"])
                    if str(r["brand_en"]).strip():
                        ov[key] = r["brand_en"]
                if ov:
                    SS["sku_master"] = _rev.apply_brand_overrides(SS["sku_master"], ov)
                    SS["brand_master"] = mastering.build_brand_master(SS["sku_master"])
                    SS["brand_curation_csv"] = _rev.overrides_to_master_csv(SS["sku_master"], ov)
                    st.success(f"{len(ov)}개 반영 — 누적 커버리지 상승.")
                    st.rerun()
        else:
            st.success("✅ 미검증 브랜드 없음 — 모두 검증되었습니다.")
        if SS.get("brand_curation_csv") is not None and len(SS["brand_curation_csv"]):
            st.download_button("⬇️ 브랜드 큐레이션 CSV (brand_master.csv 추가용)",
                               SS["brand_curation_csv"].to_csv(index=False).encode("utf-8-sig"),
                               file_name="brand_curation_add.csv", mime="text/csv")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("⏭️ 브랜드 매핑 SKIP", width="stretch"):
            go_to(4)
    with c2:
        if st.button("브랜드 확정 → SKU 매핑 →", type="primary", width="stretch"):
            go_to(4)
    if st.button("← 이전"):
        go_to(2)


# ── STEP 4: SKU명 매핑 (영문, 없으면 수동 SKIP) ─────────────────────────────
elif step == 4:
    st.subheader("⑤ SKU명 매핑 — 영문 (없으면 SKIP)")
    from kfnb_app.mapping import review as _rev2
    sku = SS["sku_master"]
    has_sku = ("sku_name_kr" in sku.columns and
               sku["sku_name_kr"].astype(str).str.strip().replace(
                   {"": None, "(unknown)": None}).notna().any())
    if not has_sku:
        st.info("이 데이터에는 식별 가능한 SKU(상품명)가 없습니다. SKU 매핑을 SKIP 하세요.")
    else:
        cov = _rev2.coverage_summary(sku)
        st.caption(f"SKU 검증 매출비중 **{cov['sku_verified_pct']}%**. SKU 영문명은 "
                   "브랜드영문+맛/포장/용량으로 조립됩니다(바코드=안정 키). "
                   "미검증 상위 SKU를 매출순으로 확인하세요.")
        st.markdown("##### 영문 SKU 표준명 (검증완료 샘플)")
        st.dataframe(sku[["sku_id", "sku_name_kr", "sku_name_en",
                          "mapping_confidence"]].head(50),
                     hide_index=True, width="stretch", height=240)
        q = _rev2.sku_review_queue(sku, top=30)
        if len(q):
            st.markdown("##### 미검증 SKU — 매출순 (상위만 확정하면 커버리지↑)")
            st.dataframe(q, hide_index=True, width="stretch", height=260)
        else:
            st.success("✅ 미검증 SKU 없음.")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("⏭️ SKU 매핑 SKIP", width="stretch"):
            go_to(5)
    with c2:
        if st.button("SKU 확정 → 카테고리 매핑 →", type="primary", width="stretch"):
            go_to(5)
    if st.button("← 이전", key="sku_prev"):
        go_to(3)


# ── STEP 5: 카테고리 매핑 (영문) ────────────────────────────────────────────
elif step == 5:
    st.subheader("⑥ 카테고리 매핑 — 영문 (대/중/소)")
    sku = SS["sku_master"]
    st.caption("대/중/소 분류의 영문 표준명입니다. 로마자로 잡힌(미사전) 항목을 직접 보정하면 "
               "config 사전에 추가할 CSV로도 받을 수 있습니다.")
    rows = []
    for lv, ko, en in [("L1", "cat_l1", "cat_l1_en"), ("L2", "cat_l2", "cat_l2_en"),
                       ("L3", "cat_l3", "cat_l3_en")]:
        if ko not in sku.columns:
            continue
        amt = "sales_amt" if "sales_amt" in sku.columns else None
        gcols = [ko] + ([en] if en in sku.columns else [])
        if amt:
            g = sku.groupby(gcols, dropna=False)[amt].sum().reset_index()
        else:
            g = sku[gcols].drop_duplicates().assign(**{"sales_amt": 0.0})
            amt = "sales_amt"
        for _, r in g.iterrows():
            kov = str(r[ko] or "").strip()
            if not kov or kov in ("(unknown)", "Uncategorized"):
                continue
            env = str(r[en] or "") if en in sku.columns else kov
            rows.append({"level": lv, "category_ko": kov, "category_en": env,
                         "in_dict": kov in config.CATEGORY_EN,
                         "sales": float(r[amt] or 0)})
    cat_cols = ["level", "category_ko", "category_en", "in_dict", "sales"]
    catdf = pd.DataFrame(rows, columns=cat_cols)
    if len(catdf):
        catdf = catdf.sort_values(["level", "sales"], ascending=[True, False])
    n_missing = int((~catdf["in_dict"]).sum()) if len(catdf) else 0
    st.caption(f"카테고리 {len(catdf)}종 · 미사전(직접 보정 권장) {n_missing}종")
    if len(catdf):
        ced = st.data_editor(
            catdf[["level", "category_ko", "category_en", "in_dict"]],
            hide_index=True, width="stretch", height=400, key="cat_editor",
            disabled=["level", "category_ko", "in_dict"],
            column_config={"category_en": st.column_config.TextColumn("영문(수정 가능)"),
                           "in_dict": st.column_config.CheckboxColumn("사전등록", disabled=True)})
        if st.button("✅ 카테고리 영문 반영"):
            ko2en = {r["category_ko"]: r["category_en"] for _, r in ced.iterrows()
                     if str(r["category_en"]).strip()}
            for lv, ko, en in [("L1", "cat_l1", "cat_l1_en"), ("L2", "cat_l2", "cat_l2_en"),
                               ("L3", "cat_l3", "cat_l3_en")]:
                if ko in sku.columns:
                    sku[en] = sku[ko].map(lambda k: ko2en.get(str(k).strip(), None)) \
                        .where(lambda s: s.notna(), sku.get(en))
            SS["sku_master"] = sku
            add = pd.DataFrame(
                [{"category_ko": k, "category_en": v} for k, v in ko2en.items()
                 if k not in config.CATEGORY_EN])
            SS["cat_curation_csv"] = add
            st.success("카테고리 영문 반영 완료." +
                       (f" 미사전 {len(add)}종은 추가용 CSV로 받을 수 있어요." if len(add) else ""))
            st.rerun()
        if SS.get("cat_curation_csv") is not None and len(SS["cat_curation_csv"]):
            st.download_button("⬇️ 카테고리 사전 추가용 CSV",
                               SS["cat_curation_csv"].to_csv(index=False).encode("utf-8-sig"),
                               file_name="category_en_add.csv", mime="text/csv")
    else:
        st.info("카테고리 컬럼이 없습니다 — 다음 단계로 진행하세요.")
    nav(prev=4, next_cb=lambda: go_to(6), next_label="카테고리 확정 → 식별자")


# ── STEP 6: 글로벌 식별자 컬럼 선택 (GICS·Bloomberg·ISIN) ────────────────────
elif step == 6:
    st.subheader("⑦ 글로벌 식별자 — GICS·Bloomberg·ISIN 선택")
    st.caption("글로벌 투자자용 식별자/분석 컬럼을 고릅니다. 최종 레이아웃·마스터에 반영됩니다.")
    from kfnb_app.mapping.mastering import (ANALYSIS_LABELS, DEFAULT_ANALYSIS,
                                            ID_LABELS, ID_COLUMNS, DEFAULT_IDS)
    cA, cB = st.columns(2)
    with cA:
        SS["analysis_cols"] = st.multiselect(
            "분석 컬럼 (신제품·맛·팩수·포장·ASP 등)", DEFAULT_ANALYSIS,
            default=SS.get("analysis_cols", DEFAULT_ANALYSIS),
            format_func=lambda k: ANALYSIS_LABELS.get(k, k))
    with cB:
        SS["id_cols"] = st.multiselect(
            "식별자 컬럼 (ISIN·KRX·Bloomberg·GICS)", list(ID_COLUMNS),
            default=SS.get("id_cols", DEFAULT_IDS),
            format_func=lambda k: ID_LABELS.get(k, k))
    # 회사 식별자 미리보기
    sku = SS["sku_master"]
    cols = [c for c in ["company_kr", "company_en_official", "krx_code", "bbg_ticker",
                        "bloomberg_code", "isin", "gics_sub_code", "gics_sub_name"]
            if c in sku.columns]
    prev = sku[cols].drop_duplicates("company_kr") if "company_kr" in cols else sku[cols]
    st.markdown("##### 회사 식별자 미리보기")
    st.dataframe(prev, hide_index=True, width="stretch", height=300)
    nav(prev=5, next_cb=lambda: go_to(7), next_label="식별자 확정 → 레이아웃 변환")


# ── STEP 7: 레이아웃 변환① — 3-티어 월별 팩트 ───────────────────────────────
elif step == 7:
    st.subheader("⑧ 산출물 구성 — 탭을 하나씩 완성")
    st.caption("최종 산출물은 **탭 6개짜리 단일 엑셀**입니다. 위에서부터 하나씩 완성하세요. "
               "Information·List·TR_BASIC 은 지금 데이터로 채워지고, DEMOGRAPHIC·RETENTION·"
               "PANEL 은 레이아웃(헤더)만 확정해두면 데이터가 들어올 때 채워집니다.")
    from kfnb_app.export import deliverable as _dv
    from kfnb_app.mapping import review as _rv2
    SS.setdefault("wb_done", {})
    _done = SS["wb_done"]
    st.markdown("**진행:** " + "  ".join(
        (("✅ " if _done.get(t) else "⬜ ") + t) for t in _dv.SHEET_ORDER))
    sku = SS["sku_master"]

    # ── 엑셀 레이아웃 업로드 → 시트/컬럼 그대로 반영 ──
    with st.expander("📤 엑셀로 레이아웃 주기 — 템플릿 xlsx 업로드 시 시트·컬럼 그대로 반영",
                     expanded=False):
        st.caption("고객 템플릿 xlsx 를 올리면 각 시트(헤더)를 읽어 그대로 출력 시트로 만들고, "
                   "헤더를 우리 원본필드에 자동 매핑합니다(매핑은 검수 가능). 시트명·순서·컬럼명 그대로.")
        lz = st.file_uploader("레이아웃 템플릿 xlsx", type=["xlsx"], key="layout_xlsx_up")
        if lz is not None and st.button("📥 이 엑셀 레이아웃 반영"):
            plan = _dv.plan_from_template(lz.getvalue())
            if plan:
                SS["wb_plan"] = plan
                st.success(f"{len(plan)}개 시트 반영 — " +
                           ", ".join(f"{p['sheet']}({p['kind']})" for p in plan[:8])
                           + (" …" if len(plan) > 8 else ""))
            else:
                st.error("시트/헤더를 읽지 못했습니다(형식 확인).")
        if SS.get("wb_plan"):
            st.caption(f"🔁 적용된 엑셀 레이아웃: {len(SS['wb_plan'])}개 시트. "
                       "아래 탭에서 자동매핑된 컬럼을 검수·수정하세요.")
            # 플랜의 매핑을 탭 레이아웃으로 주입(검수용)
            for p in SS["wb_plan"]:
                if p["kind"] in ("List", "TR_BASIC", "DEMOGRAPHIC", "RETENTION", "PANEL"):
                    SS.setdefault("wb_layouts", {})[p["kind"]] = p["columns"]
            if st.button("엑셀 레이아웃 해제"):
                SS.pop("wb_plan", None); st.rerun()

    _wtabs = st.tabs([f"{i+1}. {t}" for i, t in enumerate(_dv.SHEET_ORDER)])

    with _wtabs[0]:   # Information
        st.markdown("**Information** — 데이터셋 메타. 값 검수·수정 후 확정하세요.")
        cov = {}
        try:
            cov = _rv2.coverage_summary(sku)
        except Exception:
            pass
        meta = _dv.default_meta(SS.get("profile", {}), cov, SS.get("data_spec"),
                                SS.get("label", "KFNB"))
        base = SS.get("wb_information")
        base = base if base is not None else _dv.build_information(meta)
        ed = st.data_editor(base, hide_index=True, width="stretch", height=420,
                            num_rows="dynamic", key="wb_info_ed",
                            column_config={"field": st.column_config.TextColumn("항목"),
                                           "value": st.column_config.TextColumn("값(수정 가능)")})
        if st.button("✅ Information 확정", type="primary"):
            SS["wb_information"] = ed.reset_index(drop=True)
            SS["wb_done"]["Information"] = True
            st.success("Information 확정 — ⑩에서 이 내용으로 엑셀이 만들어집니다.")
            st.rerun()

    with _wtabs[1]:   # List
        st.markdown("**List** — 회사/브랜드 마스터. 컬럼을 추가/수정하고 각 컬럼이 끌어올 원본을 고르세요.")
        lay = _sheet_layout_editor("List", _dv.LIST_FIELDS, _dv.DEFAULT_LAYOUTS["List"],
                                   "lay_list")
        base = _dv.build_list(sku, english=True)
        st.markdown("미리보기 (적용된 컬럼대로):")
        st.dataframe(_dv.apply_layout(base, lay).head(50), hide_index=True,
                     width="stretch", height=320)

    with _wtabs[2]:   # TR_BASIC
        ch = getattr(SS.get("data_spec"), "channel_scope", "") or ""
        st.markdown("**TR_BASIC** — 거래 기본(거래일×회사×브랜드×채널). 컬럼·원본을 직접 구성.")
        st.caption(f"채널 = '{ch or '(미지정 — ① Data Spec에서 설정)'}'. 다운로드 시 일별 전체.")
        lay = _sheet_layout_editor("TR_BASIC", _dv.TR_BASE_FIELDS,
                                   _dv.DEFAULT_LAYOUTS["TR_BASIC"], "lay_tr")
        if st.button("표본 미리보기", key="tr_prev"):
            sec = None if SS["sector"] == "(전체)" else SS["sector"]
            with st.spinner("표본 생성 중…"):
                bse = _dv.build_tr_base(_src(), sku, channel=ch, sector=sec)
                SS["tr_preview"] = _dv.apply_layout(bse, lay).head(20)
        if SS.get("tr_preview") is not None:
            st.dataframe(SS["tr_preview"], hide_index=True, width="stretch")

    for _i, (_nm, _attr, _need) in enumerate([
            ("DEMOGRAPHIC", "DEMOGRAPHIC_COLS", "성별×연령 인구통계 데이터"),
            ("RETENTION", "RETENTION_COLS", "개인 거래(코호트) 데이터"),
            ("PANEL", "PANEL_COLS", "멤버스 패널(DAU) 데이터")], start=3):
        with _wtabs[_i]:
            cols = getattr(_dv, _attr)
            st.markdown(f"**{_nm}** — 컬럼(헤더)을 정의해 둡니다. 데이터(**{_need}**)가 들어오면 채워집니다.")
            _def = [{"name": c, "from": c} for c in cols]
            _sheet_layout_editor(_nm, cols, _def, f"lay_{_nm}")

    st.divider()
    st.caption("※ 퀀트용 '월별 패널(company/brand/sku_sales_monthly)'은 별개 산출물입니다. "
               "필요하면 상단 **🔬 분석** 모드에서 생성하세요. 여기 ⑧은 위 6개 탭(단일 엑셀)만 다룹니다.")
    nav(prev=6, next_cb=lambda: go_to(8), next_label="탭 구성 확정 → 미리보기")


# ── STEP 9: 산출물 미리보기 — 최종 엑셀 탭 확인 ─────────────────────────────
elif step == 8:
    st.subheader("⑨ 산출물 미리보기 — 최종 엑셀 탭 확인")
    st.caption("⑧에서 구성한 탭들이 최종 엑셀에 어떻게 들어가는지 미리 봅니다. 확인 후 ⑩에서 다운로드.")
    from kfnb_app.export import deliverable as _dv
    sku = SS["sku_master"]
    layouts = SS.get("wb_layouts") or {}
    done = SS.get("wb_done", {})
    st.markdown("**탭 상태:** " + "  ".join(
        (("✅ " if done.get(t) else "⬜ ") + t) for t in _dv.SHEET_ORDER))

    # Information
    info = SS.get("wb_information")
    if info is None:
        from kfnb_app.mapping import review as _rv3
        cov = {}
        try:
            cov = _rv3.coverage_summary(sku)
        except Exception:
            pass
        info = _dv.build_information(_dv.default_meta(SS.get("profile", {}), cov,
                                    SS.get("data_spec"), SS.get("label", "KFNB")))
    st.markdown("##### 1. Information")
    st.dataframe(info, hide_index=True, width="stretch", height=240)

    # List
    st.markdown("##### 2. List")
    lst = _dv.apply_layout(_dv.build_list(sku, english=True),
                           layouts.get("List") or _dv.DEFAULT_LAYOUTS["List"])
    st.dataframe(lst.head(30), hide_index=True, width="stretch")

    # TR_BASIC (표본)
    st.markdown("##### 3. TR_BASIC (표본 20행)")
    if st.button("TR_BASIC 표본 보기"):
        ch = getattr(SS.get("data_spec"), "channel_scope", "") or ""
        sec = None if SS["sector"] == "(전체)" else SS["sector"]
        bse = _dv.build_tr_base(_src(), sku, channel=ch, sector=sec)
        SS["tr_preview2"] = _dv.apply_layout(
            bse, layouts.get("TR_BASIC") or _dv.DEFAULT_LAYOUTS["TR_BASIC"]).head(20)
    if SS.get("tr_preview2") is not None:
        st.dataframe(SS["tr_preview2"], hide_index=True, width="stretch")

    # 나머지 탭(헤더만)
    for nm, attr in [("4. DEMOGRAPHIC", "DEMOGRAPHIC_COLS"),
                     ("5. RETENTION", "RETENTION_COLS"), ("6. PANEL", "PANEL_COLS")]:
        key = nm.split(". ")[1]
        cols = [c["name"] for c in layouts[key]] if layouts.get(key) else getattr(_dv, attr)
        st.markdown(f"##### {nm} — 헤더만(데이터 들어오면 채움)")
        st.caption("컬럼: " + ", ".join(cols[:14]) + (" …" if len(cols) > 14 else ""))
    nav(prev=7, next_cb=lambda: go_to(9), next_label="확인 → 다운로드")


# ── STEP 9: 다운로드 (CSV 세트 + zip) ───────────────────────────────────────
elif step == 9:
    st.subheader("⑩ 다운로드 — 글로벌 전달 패키지 (CSV 세트 + zip)")
    st.caption("당사 글로벌 레이아웃: company/brand/sku 월별 팩트 + 마스터 + Data Dictionary "
               "+ insight/alpha_panel(종목 PIT 알파) → zip. 영문 전용·ISIN/티커·PIT.")
    up_tpl = st.file_uploader("(선택) 전달 템플릿 샘플 CSV — 목표 컬럼 확인용",
                              type=["csv"], key="tpl_upl")
    if up_tpl is not None:
        _t = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        _t.write(up_tpl.getbuffer()); _t.flush()
        try:
            from kfnb_app.export import delivery as _dlv
            st.info("템플릿 컬럼: " + ", ".join(f"`{c}`" for c in
                    _dlv.template_columns(_t.name)))
            st.caption("이 컬럼명에 맞추려면 delivery_layout.yaml 의 name 값을 동일하게 "
                       "바꾸면 됩니다(요청 주시면 맞춰 드립니다).")
        except Exception as e:                     # noqa: BLE001
            st.warning(f"템플릿 읽기 실패: {e}")

    def _gen_delivery():
        try:
            from kfnb_app.export import delivery as _dlv
            sec = None if SS["sector"] == "(전체)" else SS["sector"]
            lag = getattr(SS.get("data_spec"), "release_lag_days", None)
            out_dir = Path(tempfile.gettempdir()) / "kfnb_delivery"
            SS["delivery_info"] = _dlv.build_and_write(
                out_dir, _src(), SS["sku_master"], sector=sec, lag_days=lag,
                label=SS["label"], analysis_cols=SS.get("analysis_cols"),
                id_cols=SS.get("id_cols"), layout=SS.get("delivery_layout"))
        except OSError as e:
            import errno
            SS["delivery_info"] = {"error": (
                "💾 디스크 부족 — 공간 확보 후 재시도" if getattr(e, "errno", None)
                == errno.ENOSPC else f"실패: {e}")}
        except Exception as e:                     # noqa: BLE001
            SS["delivery_info"] = {"error": f"실패: {e}"}

    st.button("🌐 전달 패키지 생성 (CSV 세트 + zip)", type="primary",
              on_click=_gen_delivery, width="stretch")
    dinfo = SS.get("delivery_info")
    if dinfo:
        if dinfo.get("error"):
            st.error(dinfo["error"])
        else:
            st.success("생성 완료 — " +
                       ", ".join(f"{k}({v}행)" for k, v in dinfo["tables"].items()))
            dz = dinfo.get("zip")
            if dz and Path(dz).exists():
                with open(dz, "rb") as f:
                    st.download_button("⬇️ 전달 패키지 (zip) 다운로드", f.read(),
                                       file_name=Path(dz).name, mime="application/zip",
                                       type="primary", width="stretch")
    # ── 마스터 파일 다운로드 (현재 확정분 — 다음에 올리면 자동 매핑) ──
    st.divider()
    st.markdown("##### 💾 마스터 파일 (큐레이션 누적)")
    st.caption("이번 작업까지 확정된 회사·브랜드·카테고리·SKU 영문 매핑을 마스터 zip 으로 "
               "내려받으세요. 다음 데이터셋 작업 시 ① 단계에서 이 zip 을 올리면 자동 적용됩니다.")
    if st.button("🧱 마스터 zip 생성", width="stretch"):
        from kfnb_app.mapping import master_io
        bundle = master_io.build_bundle(SS["sku_master"])
        mpath = Path(tempfile.gettempdir()) / f"{SS['label']}_MASTER.zip"
        master_io.write_zip(mpath, bundle)
        SS["master_zip"] = str(mpath)
        SS["master_counts"] = {k: len(v) for k, v in bundle.items()}
    if SS.get("master_zip") and Path(SS["master_zip"]).exists():
        st.caption("구성: " + ", ".join(f"{k}({v})" for k, v in
                   SS.get("master_counts", {}).items()))
        with open(SS["master_zip"], "rb") as f:
            st.download_button("⬇️ 마스터 파일 (zip) 다운로드", f.read(),
                               file_name=Path(SS["master_zip"]).name,
                               mime="application/zip", width="stretch")
    # ── 단일 엑셀 산출물 (탭별 레이아웃, 매번 바뀌는 템플릿 대응) ──
    st.divider()
    st.markdown("##### 📘 단일 엑셀 산출물 (탭: Information·List·TR_BASIC·…)")
    st.caption("고정 템플릿 없이 우리가 정의한 탭을 순서대로 하나의 .xlsx 로 출력합니다. "
               "Information·List·TR_BASIC 은 자동으로 채워지고, DEMOGRAPHIC·RETENTION·PANEL "
               "은 레이아웃(헤더)만 잡아둡니다(데이터 들어오면 채움). 탭은 추후 추가 가능.")
    dlv_eng = st.checkbox("영문으로 출력", value=True, key="dlv_eng")
    if st.button("📘 단일 엑셀 생성", type="primary", key="gen_workbook"):
        try:
            from kfnb_app.export import deliverable as _dv
            from kfnb_app.mapping import coverage as _cov
            sec = None if SS["sector"] == "(전체)" else SS["sector"]
            ch = getattr(SS.get("data_spec"), "channel_scope", "") or ""
            cov = _cov.coverage_by_sales(SS["sku_master"]) if hasattr(
                _cov, "coverage_by_sales") else {}
            try:
                from kfnb_app.mapping import review as _rv
                cov = {**cov, **_rv.coverage_summary(SS["sku_master"])}
            except Exception:
                pass
            meta = _dv.default_meta(SS.get("profile", {}), cov, SS.get("data_spec"),
                                    SS.get("label", "KFNB"))
            with st.spinner("탭별 레이아웃 생성 중…"):
                xb, rep = _dv.build_deliverable(
                    _src(), SS["sku_master"], meta=meta, channel=ch, sector=sec,
                    english=dlv_eng, information_df=SS.get("wb_information"),
                    layouts=SS.get("wb_layouts"), plan=SS.get("wb_plan"))
            SS["workbook_xlsx"] = xb; SS["workbook_rep"] = rep
            st.success("생성 완료 — " + " · ".join(f"{k}:{v}" for k, v in rep.items()))
        except Exception as e:                     # noqa: BLE001
            st.error(f"생성 실패: {e}")
    if SS.get("workbook_xlsx"):
        st.download_button("⬇️ 단일 엑셀 산출물 (xlsx) 다운로드", SS["workbook_xlsx"],
                           file_name=f"{SS['label']}_deliverable.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           type="primary", width="stretch")

    # ── 고객 전달 템플릿(.xlsx) 그대로 채우기 ──
    st.divider()
    st.markdown("##### 📑 (선택) 고정 고객 템플릿(.xlsx) 채우기")
    st.caption("고객 템플릿(예: Custom_F,B&T.xlsx)을 올리면 시트 구조·사전을 보존한 채 "
               "**기본정보(공시명·브랜드명·법인등록번호) + 기본매출(거래일×회사×브랜드)** 을 "
               "채워 완성본을 돌려드립니다. 인구통계·재구매·패널은 추가 데이터가 있어야 채워집니다.")
    tplf = st.file_uploader("전달 템플릿 xlsx 업로드", type=["xlsx"], key="fill_tpl")
    eng_fill = st.checkbox("영문으로 채우기 (회사·브랜드 영문명)", value=True, key="tpl_eng",
                           help="공시명/회사명=공식 영문명, 브랜드명=영문회사_영문브랜드")
    if tplf is not None and st.button("📑 템플릿 채우기 실행", type="primary"):
        try:
            from kfnb_app.export import template_xlsx as _tx
            sec = None if SS["sector"] == "(전체)" else SS["sector"]
            ch = getattr(SS.get("data_spec"), "channel_scope", "") or ""
            with st.spinner("기본정보·기본매출 생성 후 템플릿에 채우는 중…"):
                bi = _tx.build_basic_info(SS["sku_master"], english=eng_fill)
                bs = _tx.build_basic_sales(_src(), SS["sku_master"], channel=ch,
                                           sector=sec, english=eng_fill)
                if len(bs) > 1_000_000:
                    st.warning(f"기본매출 {len(bs):,}행 — 엑셀 한도(약 104만행) 초과분은 "
                               "잘립니다. 전체는 CSV 전달 패키지를 사용하세요.")
                    bs = bs.head(1_000_000)
                filled, rep = _tx.fill_template(tplf.getvalue(), basic_info=bi,
                                                basic_sales_prod=bs, basic_sales_bt=bs)
            SS["filled_tpl"] = filled
            SS["filled_rep"] = rep
            st.success("완성 — " + ", ".join(f"{k}:{v}" for k, v in rep.items()
                                              if "행" in str(v)))
        except Exception as e:                     # noqa: BLE001
            st.error(f"템플릿 채우기 실패: {e}")
    if SS.get("filled_tpl"):
        st.caption("시트별: " + " · ".join(f"{k}={v}" for k, v in
                   (SS.get("filled_rep") or {}).items()))
        st.download_button("⬇️ 채워진 템플릿 (xlsx) 다운로드", SS["filled_tpl"],
                           file_name="Custom_FBT_filled.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           type="primary", width="stretch")

    st.caption("📈 알파·대시보드·백테스트 등 분석은 상단 **🔬 분석** 모드에서 이어서 하세요.")
    nav(prev=8, next_cb=None)
