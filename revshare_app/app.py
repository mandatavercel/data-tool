"""
수익배분율 산정 도구 (Revenue Share Calculator)
================================================
- 데이터 파트너 수익배분율을 점수 기반으로 자동 산정
- 단일 HTML 앱(index.html)을 Streamlit으로 임베드
- index.html은 단독으로도 동작 (외부 파트너 공유 가능)

로컬 실행:
    streamlit run app.py --server.port 8505
또는 통합 런처(streamlit_app.py)의 ▶실행 버튼.
"""
from __future__ import annotations

from pathlib import Path
import streamlit as st
import streamlit.components.v1 as components


APP_DIR = Path(__file__).resolve().parent
HTML_PATH = APP_DIR / "index.html"


st.set_page_config(
    page_title="수익배분율 산정 도구",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Streamlit chrome을 최소화하고 임베드 도구에 공간을 최대한 할당
st.markdown(
    """
    <style>
      /* 메인 컨테이너 패딩 최소화 */
      .block-container { padding: 0.5rem 1rem 1rem; max-width: 1280px; }
      /* footer 숨김 */
      footer { visibility: hidden; }
      #MainMenu { visibility: hidden; }
      header[data-testid="stHeader"] { background: transparent; }
      /* iframe 컨테이너 보더 제거 */
      iframe { border: none !important; }
    </style>
    """,
    unsafe_allow_html=True,
)


def main() -> None:
    if not HTML_PATH.exists():
        st.error(f"❌ HTML 파일을 찾을 수 없습니다: {HTML_PATH}")
        st.info(
            "이 도구는 `index.html`을 Streamlit으로 임베드해서 실행합니다. "
            "파일이 같은 폴더에 있는지 확인하세요."
        )
        return

    html = HTML_PATH.read_text(encoding="utf-8")

    # localStorage(이력 저장)가 정상 동작하도록 iframe + 충분한 높이로 임베드
    # scrolling=True 로 두면 내부 스크롤이 활성화되어 모든 컨텐츠 접근 가능
    components.html(html, height=1500, scrolling=True)

    # 도움말 / 외부 공유 안내 (Streamlit 측)
    with st.expander("ℹ️ 이 도구에 대해 / 외부 파트너에게 공유하기"):
        st.markdown(
            f"""
            **이 도구의 특징**
            - 단일 HTML 파일 (`index.html`)로 동작 — 외부 인터넷 연결 없이 로컬 PC에서 실행 가능
            - 모든 산정 결과는 브라우저 `localStorage` 에 저장되며 외부로 전송되지 않음
            - 화면의 **📋 산정 결과서** 버튼으로 계약서·제안서 즉시 복사 가능

            **외부 파트너에게 공유하기**
            - 파트너가 직접 산정 결과를 확인할 수 있도록 `index.html` 단일 파일을 전달하면 됩니다.
              파일 위치: `{HTML_PATH}`
            - 파트너는 별도 설치 없이 브라우저(Chrome/Safari/Edge)로 더블클릭만 하면 실행됩니다.

            **정책 변경 시**
            - `index.html` 상단의 `POLICY` 객체에서 기본 배분율, 가중치, Tier 가산을 수정하면
              자동 계산이 즉시 반영됩니다.
            """
        )


if __name__ == "__main__" or True:
    # Streamlit 실행 시 항상 main 실행
    main()
