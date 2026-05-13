"""
Market Signal — stub (준비 중)
매출 성장률 vs 주가 수익률 시차 상관분석
"""
import streamlit as st


def render(go_to):
    st.subheader("③ Market Signal — 매출 vs 주가 시차 상관분석")
    st.info("🔜 이 모듈은 현재 개발 중입니다.")
    st.markdown("""
**계획된 분석:**
- 기간별 매출 성장률 계산
- yfinance를 통한 주가 데이터 수집 (.KS / .KQ)
- Lag 0 / 1 / 3 / 7 / 14 / 30일 시차 상관계수 계산
- 선행 시그널 탐지 (Lead Time, Hit Rate)
    """)
    if st.button("← 분석 선택", key="ms_prev"):
        go_to(2)
