"""
분석 API 진단 패널 — pykrx / yfinance / DART 즉석 테스트.

Step 4 상단에 expander로 렌더링. 사용자가 원할 때 3개 데이터 소스를 개별 호출해서:
    - 실제 응답 수신 여부
    - 응답 시간
    - 실패 시 정확한 예외 타입·메시지

앱 시작 시 자동 실행 안 함 (사용자 요청 시만). 캐시 초기화 버튼도 함께.
"""
from __future__ import annotations

import os
import time
import streamlit as st
import pandas as pd


def render_diagnostic_panel() -> None:
    """Step 4 상단에 렌더링할 즉석 진단 패널."""
    with st.expander(
        "🩺 네트워크·API 진단 — pykrx / yfinance / DART 연결 상태 즉석 확인",
        expanded=False,
    ):
        # 환경 표시
        on_cloud = os.path.isdir("/mount/src")
        env_icon = "☁️ Streamlit Cloud (US)" if on_cloud else "💻 로컬 실행"
        st.caption(
            f"환경: **{env_icon}** — 로컬은 한국이라 KR API(pykrx/DART)가 훨씬 안정적."
        )

        # secrets 확인
        dart_key_set = _check_secret("DART_API_KEY")
        anth_key_set = _check_secret("ANTHROPIC_API_KEY")
        s1, s2 = st.columns(2)
        s1.metric("DART API Key",      "✅ 설정됨" if dart_key_set else "❌ 없음")
        s2.metric("Anthropic API Key", "✅ 설정됨" if anth_key_set else "⚪ 선택")

        st.divider()

        # 3개 진단 버튼 나란히
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("🔬 pykrx 테스트 (Samsung 005930)",
                         use_container_width=True, key="diag_pykrx"):
                _run_pykrx_probe()
        with c2:
            if st.button("🔬 yfinance 테스트 (^KS11 KOSPI)",
                         use_container_width=True, key="diag_yf"):
                _run_yfinance_probe()
        with c3:
            if st.button("🔬 DART 테스트 (corp master)",
                         use_container_width=True, key="diag_dart"):
                _run_dart_probe()

        st.divider()
        st.markdown("**⚡ 캐시 관리**")
        cc1, cc2 = st.columns(2)
        with cc1:
            if st.button("🗑 주가/시그널 캐시 초기화",
                         use_container_width=True, key="diag_clear_price"):
                _clear_price_caches()
        with cc2:
            if st.button("🗑 DART 마스터 캐시 초기화",
                         use_container_width=True, key="diag_clear_dart"):
                _clear_dart_caches()


# ── secrets 검사 ─────────────────────────────────────────────────────────
def _check_secret(key: str) -> bool:
    try:
        if key in st.secrets:
            v = str(st.secrets[key]).strip()
            if v:
                return True
    except Exception:
        pass
    return bool(os.environ.get(key, "").strip())


# ── pykrx probe ──────────────────────────────────────────────────────────
def _run_pykrx_probe() -> None:
    st.markdown("**🔬 pykrx 진단 결과**")
    # 1) import 자체
    try:
        import pykrx
        ver = getattr(pykrx, "__version__", "?")
        st.write(f"✅ `import pykrx` OK · 버전 `{ver}`")
    except Exception as e:
        st.error(f"❌ import 실패: **{type(e).__name__}**: {e}")
        return

    try:
        from pykrx import stock as krx
        st.write("✅ `from pykrx import stock` OK")
    except Exception as e:
        st.error(f"❌ stock 서브모듈 import 실패: **{type(e).__name__}**: {e}")
        return

    # 2) 실제 KRX 호출 — Samsung 005930, 최근 14일
    end_d   = pd.Timestamp.today().strftime("%Y%m%d")
    start_d = (pd.Timestamp.today() - pd.Timedelta(days=14)).strftime("%Y%m%d")
    try:
        t0 = time.time()
        with st.spinner(f"KRX 호출 중... ({start_d} ~ {end_d}, ticker=005930)"):
            raw = krx.get_market_ohlcv_by_date(start_d, end_d, "005930")
        dt = time.time() - t0
        if raw is None or raw.empty:
            st.error(
                f"❌ 응답 성공했지만 빈 DataFrame ({dt:.1f}초). "
                "KRX 서버가 데이터를 안 줌 — 주말·공휴일이거나 IP 차단."
            )
        else:
            st.success(
                f"✅ pykrx 정상 — {len(raw)}행 수신 ({dt:.1f}초) · "
                f"최근 종가 **{raw['종가'].iloc[-1]:,.0f}원**"
            )
            st.dataframe(raw.tail(3), use_container_width=True)
    except Exception as e:
        st.error(
            f"❌ API 호출 실패: **{type(e).__name__}**: {str(e)[:300]}\n\n"
            "KRX 서버 접속 자체가 막힘. 클라우드 IP 차단 또는 pykrx 엔드포인트 stale."
        )


# ── yfinance probe ───────────────────────────────────────────────────────
def _run_yfinance_probe() -> None:
    st.markdown("**🔬 yfinance 진단 결과**")
    try:
        import yfinance as yf
        ver = getattr(yf, "__version__", "?")
        st.write(f"✅ `import yfinance` OK · 버전 `{ver}`")
    except Exception as e:
        st.error(f"❌ import 실패: **{type(e).__name__}**: {e}")
        return

    try:
        t0 = time.time()
        with st.spinner("yfinance 호출 중... (^KS11 KOSPI index, 최근 14일)"):
            hist = yf.download("^KS11",
                               start=(pd.Timestamp.today() - pd.Timedelta(days=14)).strftime("%Y-%m-%d"),
                               end=pd.Timestamp.today().strftime("%Y-%m-%d"),
                               progress=False, auto_adjust=False, threads=False)
        dt = time.time() - t0
        if hist is None or hist.empty:
            st.error(
                f"❌ 빈 응답 ({dt:.1f}초). "
                "Yahoo가 Cloud IP를 차단한 상태 → pykrx 폴백 필수. "
                "curl_cffi 우회도 안 통함(yfinance 내부 session 차단)."
            )
        else:
            st.success(
                f"✅ yfinance 정상 — {len(hist)}행 수신 ({dt:.1f}초)"
            )
            st.dataframe(hist.tail(3), use_container_width=True)
    except Exception as e:
        st.error(f"❌ 호출 실패: **{type(e).__name__}**: {str(e)[:300]}")


# ── DART probe ───────────────────────────────────────────────────────────
def _run_dart_probe() -> None:
    st.markdown("**🔬 DART API 진단 결과**")
    # API key 가져오기 (st.secrets 우선)
    api_key = ""
    try:
        if "DART_API_KEY" in st.secrets:
            api_key = str(st.secrets["DART_API_KEY"]).strip()
    except Exception:
        pass
    if not api_key:
        api_key = os.environ.get("DART_API_KEY", "").strip()
    # 로컬 저장소 fallback
    if not api_key:
        try:
            from analysis_app.secrets_store import load_persistent_secrets
            api_key = load_persistent_secrets().get("dart_api_key", "").strip()
        except Exception:
            pass

    if not api_key:
        st.error(
            "❌ DART_API_KEY 없음.\n\n"
            "**해결**: (1) Streamlit Cloud Settings → Secrets에 "
            "`DART_API_KEY = \"...\"` 추가, "
            "(2) 또는 Step 4 Earnings Intel 파라미터에서 직접 입력 후 💾 저장."
        )
        return
    st.write(f"✅ API Key 확인됨 (`{api_key[:6]}...{api_key[-4:]}`)")

    # 실제 호출
    try:
        from modules.mapping.dart_lookup import fetch_dart_corp_master
        # 캐시 초기화 후 새로 시도
        try:
            fetch_dart_corp_master.clear()
        except Exception:
            pass

        t0 = time.time()
        with st.spinner("DART 마스터 다운로드 중... (약 3MB, 15~45초)"):
            master = fetch_dart_corp_master(api_key)
        dt = time.time() - t0

        if master is None or master.empty:
            st.error(f"❌ 빈 응답 ({dt:.1f}초). API key 재확인.")
        else:
            listed = master[master["stock_code"].astype(str).str.strip() != ""]
            st.success(
                f"✅ DART 정상 — 전체 {len(master):,}개사 · "
                f"상장사 {len(listed):,}개사 · **{dt:.1f}초**"
            )
            st.dataframe(listed.head(5)[["corp_code", "corp_name", "stock_code"]],
                         use_container_width=True)
    except Exception as e:
        st.error(
            f"❌ DART 호출 실패: **{type(e).__name__}**: {str(e)[:400]}"
        )


# ── 캐시 관리 ────────────────────────────────────────────────────────────
def _clear_price_caches() -> None:
    cleared = []
    try:
        from modules.analysis.signal.market import (
            _fetch_daily_ohlcv_cached, _fetch_benchmark_daily,
        )
        _fetch_daily_ohlcv_cached.clear()
        cleared.append("주가 OHLCV")
        _fetch_benchmark_daily.clear()
        cleared.append("벤치마크")
    except Exception as e:
        st.warning(f"일부 캐시 초기화 실패: {e}")

    # session_state의 results도 옵셔널 정리
    if "results" in st.session_state:
        st.session_state["results"].pop("market_signal", None)
        cleared.append("Market Signal 결과")

    st.success(f"✅ 초기화됨: {', '.join(cleared) if cleared else '없음'}")


def _clear_dart_caches() -> None:
    cleared = []
    try:
        from modules.mapping.dart_lookup import fetch_dart_corp_master
        fetch_dart_corp_master.clear()
        cleared.append("DART corp_master (마켓 mapping 앱)")
    except Exception:
        pass
    try:
        from modules.analysis.signal.earnings import _fetch_corp_code_map
        _fetch_corp_code_map.clear()
        cleared.append("DART corp_code_map (Earnings Intel)")
    except Exception:
        pass

    # session state의 매칭 결과도 정리
    for k in ["p_dart_match", "_dart_listed", "_dart_user_mapping"]:
        if k in st.session_state:
            st.session_state.pop(k, None)
            cleared.append(f"session_state.{k}")
    if "results" in st.session_state:
        st.session_state["results"].pop("earnings_intel", None)
        cleared.append("Earnings Intel 결과")

    st.success(f"✅ 초기화됨: {', '.join(cleared) if cleared else '없음'}")
