#!/bin/bash
# ════════════════════════════════════════════════════════════════════════
# Mandata Alt-Data Intelligence Platform — 데이터 분석 프로그램
# analysis_app.py 실행 바로가기 (macOS)
# 사용법: Finder에서 더블클릭
# ════════════════════════════════════════════════════════════════════════

# 스크립트 위치한 폴더로 이동 (Desktop의 alias 통해서도 동작)
cd "$(dirname "$0")"

clear
cat <<'EOF'
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║    📊  Mandata Alt-Data Intelligence Platform                      ║
║        데이터 분석 프로그램 (analysis_app.py)                       ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
EOF
echo ""
echo "📂 실행 폴더: $(pwd)"
echo ""

# ─── Python3 확인 ──────────────────────────────────────────────────
if ! command -v python3 &> /dev/null; then
    echo "❌ python3가 설치되어 있지 않습니다."
    echo "👉 https://www.python.org/downloads/ 에서 Python 3.10+ 설치 후 다시 실행하세요."
    echo ""
    read -n 1 -s -r -p "아무 키나 누르면 창이 닫힙니다..."
    exit 1
fi

PY_VER=$(python3 --version 2>&1)
echo "🐍 $PY_VER 감지"
echo ""

# ─── Streamlit 설치 확인 ───────────────────────────────────────────
if ! python3 -c "import streamlit" &> /dev/null 2>&1; then
    echo "📦 필수 패키지가 설치되어 있지 않습니다. 최초 1회 설치를 진행합니다..."
    echo "   (약 1~3분 소요, 인터넷 연결 필요)"
    echo ""
    # requirements.txt가 있으면 사용, 없으면 최소 패키지 설치
    if [ -f "requirements.txt" ]; then
        echo "📋 requirements.txt 기반 설치 중..."
        python3 -m pip install -r requirements.txt --break-system-packages 2>/dev/null \
            || python3 -m pip install -r requirements.txt
    else
        echo "📋 핵심 패키지 설치 중..."
        python3 -m pip install --break-system-packages \
            streamlit pandas numpy yfinance pykrx plotly scipy openpyxl requests 2>/dev/null \
            || python3 -m pip install \
                streamlit pandas numpy yfinance pykrx plotly scipy openpyxl requests
    fi
    echo ""
    echo "✅ 패키지 설치 완료"
    echo ""
fi

# ─── Report export 패키지 (PPT/PDF) — 별도 체크 (개별 추가 가능) ────
if ! python3 -c "import pptx, reportlab" &> /dev/null 2>&1; then
    echo "📦 리포트 export 패키지 설치 중 (python-pptx, reportlab)..."
    python3 -m pip install --break-system-packages \
        python-pptx reportlab 2>/dev/null \
        || python3 -m pip install python-pptx reportlab
    echo "✅ Report export 패키지 준비됨"
    echo ""
fi

# ─── Streamlit 실행 ────────────────────────────────────────────────
echo "🚀 분석 프로그램 시작 중..."
echo "   • 브라우저가 자동으로 열립니다 (http://localhost:8501)"
echo "   • 종료하려면 이 창에서 Ctrl+C 를 누르세요"
echo "   • 종료 후에는 이 창을 닫아도 됩니다"
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo ""

# analysis_app/ 으로 이동해서 실행 → analysis_app/.streamlit/config.toml 적용됨
# (entry point 안에서 sys.path를 프로젝트 루트로 잡고 있어 modules.* 임포트는 정상)
cd analysis_app
python3 -m streamlit run analysis_app.py --server.headless false --server.port 8501 --server.maxUploadSize 5120

# ─── 종료 후 창 유지 ───────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "📴 프로그램이 종료되었습니다."
read -n 1 -s -r -p "아무 키나 누르면 창이 닫힙니다..."
