#!/bin/bash
# ════════════════════════════════════════════════════════════════════════
# Mandata Alt-Data Intelligence Platform — 수익배분율 산정 도구
# revshare_app/app.py 실행 바로가기 (macOS)
# 사용법: Finder에서 더블클릭
# ════════════════════════════════════════════════════════════════════════

cd "$(dirname "$0")"

clear
cat <<'EOF'
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║    💼  Mandata Alt-Data Intelligence Platform                      ║
║        데이터 파트너 수익배분율 산정 도구 (revshare_app)            ║
║                                                                    ║
║    원천사 유형 + 7개 평가 항목 점수 → Tier별 배분율 자동 산정       ║
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

# ─── 필수 패키지 (streamlit만 필요 — 산정 로직은 HTML/JS 안에 있음) ───
if ! python3 -c "import streamlit" &> /dev/null 2>&1; then
    echo "📦 streamlit을 설치합니다..."
    echo "   (약 30초~1분 소요, 인터넷 연결 필요)"
    echo ""
    python3 -m pip install --break-system-packages --upgrade streamlit 2>/dev/null \
        || python3 -m pip install --upgrade streamlit
    echo ""
    echo "✅ 패키지 설치 완료"
    echo ""
fi

# ─── Streamlit 실행 (포트 8505 — 다른 앱과 충돌 회피) ──────────────
echo "🚀 수익배분 산정 도구 시작 중..."
echo "   • 브라우저가 자동으로 열립니다 (http://localhost:8505)"
echo "   • 산정 이력은 브라우저 localStorage에 저장됩니다 (외부 전송 없음)"
echo "   • 외부 파트너에게 공유하려면 'revshare_app/index.html' 파일을 전달하세요"
echo "   • 종료하려면 이 창에서 Ctrl+C 를 누르세요"
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo ""

cd revshare_app
python3 -m streamlit run app.py --server.headless false --server.port 8505 --browser.gatherUsageStats false

# ─── 종료 후 창 유지 ───────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "📴 프로그램이 종료되었습니다."
read -n 1 -s -r -p "아무 키나 누르면 창이 닫힙니다..."
