"""
Earnings Intelligence — stub (준비 중)
거래/매출 데이터가 공시매출보다 먼저 움직이는지 검증 (DART API)
"""
import streamlit as st


def render(go_to):
    st.subheader("③ Earnings Intelligence — 매출 선행성 검증")
    st.info("🔜 이 모듈은 현재 개발 중입니다.")
    st.markdown("""
**계획된 분석:**
- DART API로 분기별 공시매출 수집 (`fnlttSinglAcnt`)
- POS 집계 매출과 분기 단위 정렬 및 비교
- 선행 N주 POS 집계 vs 공시매출 상관계수
- 예측 정확도 (Hit Rate) 계산
    """)
    if st.button("← 분석 선택", key="ei_prev"):
        go_to(2)
