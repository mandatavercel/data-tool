#!/bin/bash
# ════════════════════════════════════════════════════════════════════════
# Mandata Alt-Data Intelligence Platform — 계약서 생성기
# contract_app/app.py 실행 바로가기 (macOS)
# 사용법: Finder에서 더블클릭
# ════════════════════════════════════════════════════════════════════════

cd "$(dirname "$0")"

clear
cat <<'EOF'
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║    📜  Mandata Alt-Data Intelligence Platform                       ║
║        계약서 생성기 (contract_app)                                  ║
║                                                                    ║
║    질문 답변 → 표준 양식 .docx 초안 자동 생성                          ║
║    국내 DSA v1.0 / 향후 글로벌·NDA·DDQ 확장 가능                       ║
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

# ─── 필수 패키지 (streamlit, python-docx) ──────────────────────────
NEEDS_INSTALL=0
python3 -c "import streamlit" &> /dev/null 2>&1 || NEEDS_INSTALL=1
python3 -c "import docx" &> /dev/null 2>&1 || NEEDS_INSTALL=1

if [ "$NEEDS_INSTALL" -eq 1 ]; then
    echo "📦 필요한 패키지를 설치합니다 (streamlit, python-docx)..."
    echo "   (약 30초~1분 소요, 인터넷 연결 필요)"
    echo ""
    python3 -m pip install --break-system-packages --upgrade streamlit python-docx 2>/dev/null \
        || python3 -m pip install --upgrade streamlit python-docx
    echo ""
    echo "✅ 패키지 설치 완료"
    echo ""
fi

# ─── Streamlit 실행 (포트 8506 — 다른 앱과 충돌 회피) ──────────────
echo "🚀 계약서 생성기 시작 중..."
echo "   • 브라우저가 자동으로 열립니다 (http://localhost:8506)"
echo "   • 새 양식 추가는 contract_app/templates/<key>/ 폴더 3개 파일만 추가"
echo "   • 종료하려면 이 창에서 Ctrl+C 를 누르세요"
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo ""

python3 -m streamlit run contract_app/app.py \
    --server.headless false \
    --server.port 8506 \
    --browser.gatherUsageStats false

# ─── 종료 후 창 유지 ───────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "📴 프로그램이 종료되었습니다."
read -n 1 -s -r -p "아무 키나 누르면 창이 닫힙니다..."
