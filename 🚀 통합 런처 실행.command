#!/bin/bash
# ════════════════════════════════════════════════════════════════════════
# Mandata Alt-Data Intelligence Platform — 통합 런처
# streamlit_app.py 실행 바로가기 (macOS)
# 한 화면에서 4개 앱(분석/매핑/카탈로그/사업자조회)을 보고 실행
# 사용법: Finder에서 더블클릭
# ════════════════════════════════════════════════════════════════════════

cd "$(dirname "$0")"

clear
cat <<'EOF'
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║    🚀  Mandata Data-Tool 통합 런처                                 ║
║                                                                    ║
║    한 화면에서 모든 앱을 보고 실행하세요:                          ║
║      📊 데이터분석   🗂 데이터매핑                                 ║
║      🛒 데이터카탈로그   🏢 사업자조회                              ║
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
    echo "⚠️  Python $PY_MAJOR.$PY_MINOR 감지 — 일부 앱(분석/매핑)은 pandas 2.2.3을 요구해서"
    echo "    Python 3.12 환경을 권장합니다. (사업자조회·런처는 3.14에서도 동작)"
    echo "    👉 https://www.python.org/downloads/release/python-3128/ 에서 3.12 설치"
    echo ""
fi

# ─── 통합 런처가 모든 앱을 한 프로세스에서 띄우므로 모든 의존성 필요 ──
# Python 3.14 호환을 위해 pinned 버전 안 쓰고 unpinned latest로 설치
# (각 패키지의 latest는 3.14 wheel 제공). 이미 깔린 건 건드리지 않음.
#
# 패키지 → import 이름 매핑 (pip 이름이 import 이름과 다른 케이스)
declare -A PKG_IMPORT_MAP=(
    ["beautifulsoup4"]="bs4"
    ["python-pptx"]="pptx"
    ["python-docx"]="docx"
    ["pyyaml"]="yaml"
    ["scikit-learn"]="sklearn"
)

# 필요한 모든 패키지 (런처 + 5개 앱이 함께 쓰는 의존성)
ALL_PKGS=(
    # 공통 / 런처
    streamlit pandas numpy requests openpyxl
    # bizno_app
    beautifulsoup4 lxml anthropic
    # analysis_app
    plotly pykrx yfinance scipy curl-cffi python-pptx reportlab
    # mapping_app / catalog_app
    rapidfuzz
    # contract_app
    python-docx
)

NEED_INSTALL=""
for pkg in "${ALL_PKGS[@]}"; do
    import_name="${PKG_IMPORT_MAP[$pkg]:-$pkg}"
    if ! python3 -c "import $import_name" &> /dev/null 2>&1; then
        NEED_INSTALL="$NEED_INSTALL $pkg"
    fi
done

if [ -n "$NEED_INSTALL" ]; then
    echo "📦 빠진 패키지 설치 중:$NEED_INSTALL"
    echo "   (최초 1회는 5~10분, 그 후엔 건너뜀)"
    python3 -m pip install --break-system-packages $NEED_INSTALL 2>/dev/null \
        || python3 -m pip install $NEED_INSTALL
    echo "✅ 설치 완료"
    echo ""
fi

# ─── Streamlit 실행 (포트 8500 — 런처 전용) ─────────────────────────
echo "🚀 통합 런처 시작 중..."
echo "   • 브라우저가 자동으로 열립니다 (http://localhost:8500)"
echo "   • 런처에서 각 앱을 ▶실행 / 🌐열기 / ⏹중단 할 수 있어요"
echo "   • 런처를 닫아도 띄워둔 앱은 계속 살아있어요"
echo "   • 종료하려면 이 창에서 Ctrl+C 를 누르세요"
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo ""

python3 -m streamlit run streamlit_app.py --server.headless false --server.port 8500 --browser.gatherUsageStats false

# ─── 종료 후 창 유지 ───────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "📴 런처가 종료되었습니다."
echo "   (런처에서 띄운 앱들은 별도로 살아있을 수 있어요. 활성 모니터에서 streamlit 검색)"
read -n 1 -s -r -p "아무 키나 누르면 창이 닫힙니다..."
