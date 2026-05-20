#!/bin/bash
# ════════════════════════════════════════════════════════════════════════
# Mandata Alt-Data Intelligence Platform — 사업자조회 프로그램
# bizno_app/app.py 실행 바로가기 (macOS)
# 사용법: Finder에서 더블클릭
# ════════════════════════════════════════════════════════════════════════

cd "$(dirname "$0")"

clear
cat <<'EOF'
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║    🏢  Mandata Alt-Data Intelligence Platform                      ║
║        사업자조회 프로그램 (bizno_app/app.py)                       ║
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

# ─── Python 3.14+ 경고 ─────────────────────────────────────────────
PY_MAJOR=$(python3 -c "import sys; print(sys.version_info[0])")
PY_MINOR=$(python3 -c "import sys; print(sys.version_info[1])")
if [ "$PY_MAJOR" = "3" ] && [ "$PY_MINOR" -ge "14" ]; then
    echo "⚠️  Python $PY_MAJOR.$PY_MINOR 감지 — pandas 최신 버전을 사용합니다 (3.14 호환)."
    echo ""
fi

# ─── 필수 패키지 확인 (requirements.txt 안 씀 — pinned 버전이 3.14 빌드 실패 유발) ──
if ! python3 -c "import streamlit, bs4, openpyxl, requests, pandas, anthropic" &> /dev/null 2>&1; then
    echo "📦 사업자조회 앱에 필요한 패키지를 설치합니다..."
    echo "   (streamlit, pandas, requests, openpyxl, beautifulsoup4, lxml, anthropic)"
    echo "   (약 1~3분 소요, 인터넷 연결 필요)"
    echo ""
    python3 -m pip install --break-system-packages --upgrade \
        streamlit pandas requests openpyxl beautifulsoup4 lxml anthropic 2>/dev/null \
        || python3 -m pip install --upgrade \
            streamlit pandas requests openpyxl beautifulsoup4 lxml anthropic
    echo ""
    echo "✅ 패키지 설치 완료"
    echo ""
fi

# ─── Streamlit 실행 (포트 8503 — 다른 앱과 충돌 회피) ──────────────
echo "🚀 사업자조회 프로그램 시작 중..."
echo "   • 브라우저가 자동으로 열립니다 (http://localhost:8503)"
echo "   • 다른 앱들이 동시에 실행 중이어도 OK (포트 분리)"
echo "   • 종료하려면 이 창에서 Ctrl+C 를 누르세요"
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo ""

cd bizno_app
python3 -m streamlit run app.py --server.headless false --server.port 8503 --server.maxUploadSize 1024

# ─── 종료 후 창 유지 ───────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "📴 프로그램이 종료되었습니다."
read -n 1 -s -r -p "아무 키나 누르면 창이 닫힙니다..."
