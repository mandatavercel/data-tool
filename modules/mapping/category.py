"""
Category Intelligence — stub (준비 중)
"""
import streamlit as st


def render(go_to):
    st.subheader("③ Category Intelligence — 카테고리 성장 분석")
    st.info("🔜 이 모듈은 현재 개발 중입니다.")
    if st.button("← 분석 선택", key="cat_prev"):
        go_to(2)
