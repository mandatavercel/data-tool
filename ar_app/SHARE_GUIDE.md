# AR Management 클라우드 배포 가이드 (PC 꺼져도 항상 접속)

PC와 무관하게 24시간 접속 가능하게 **Streamlit Community Cloud**에 배포합니다.
클라우드는 디스크가 임시(재시작 시 초기화)라서, 데이터는 **무료 Postgres(Neon)** 에 저장해 영구 보존합니다. (구글 미사용)
동시 편집은 기존 **1인 편집 락**이 그대로 막아줍니다.

순서: **① Neon Postgres 만들기 → ② GitHub에 푸시 → ③ Streamlit Cloud 배포 → ④ secrets 입력 → ⑤ 동료 접속**

---

## ① Neon Postgres 만들기 (무료, 5분)
1. https://neon.tech 가입(GitHub 계정으로 가능) → **Create project**.
2. 프로젝트 생성 후 **Connection string**(`postgresql://USER:PASSWORD@HOST/DBNAME?sslmode=require`)을 복사해 둡니다.
   - 대시보드의 "Connection Details"에서 **Pooled connection** 문자열을 쓰면 안정적입니다.
3. 테이블은 앱이 처음 켜질 때 자동 생성하고, **기존 데이터(현재 JSON)를 한 번 자동 이관(seed)** 합니다. 별도 작업 불필요.

> 대안: Supabase(https://supabase.com)도 동일하게 Postgres 연결 문자열을 제공합니다. 둘 중 편한 걸 쓰세요.

## ② GitHub 푸시
```bash
cd ~/Desktop/data-tool
rm -f .git/index.lock
git add -A
git commit -m "AR: 클라우드 배포용 Postgres 영구저장 백엔드 + 1인 편집 락"
git push origin main
```

## ③ Streamlit Community Cloud 배포
1. https://share.streamlit.io 로그인(GitHub) → **New app**.
2. Repository `mandatavercel/data-tool`, Branch `main`, **Main file path** `streamlit_app.py` → **Deploy**.

## ④ secrets 입력 (영구 저장 연결)
배포된 앱 → **Settings → Secrets** 에 아래 한 줄을 붙여넣고 저장(앱 자동 재시작):
```toml
DATABASE_URL = "postgresql://USER:PASSWORD@HOST/DBNAME?sslmode=require"
```
- 저장 후 앱이 재시작되면서 DB에 테이블을 만들고 현재 데이터를 이관합니다.
- 이후 모든 수금·배분 체크가 DB에 영구 저장되어, **앱이 재시작되거나 PC가 꺼져도 데이터가 유지**됩니다.

## ⑤ 동료 접속 (권한)
- 접근은 `acl.json`으로 관리됩니다. 현재 `default_access: *@mandata.kr` → **mandata.kr 이메일 동료는 추가 설정 없이 바로 접근**.
- 배포된 앱 주소를 동료에게 공유하면 됩니다. (AR 직접 링크는 주소 끝에 `/ar`)
- 외부 이메일을 초대하려면 `acl.json`의 `default_access`/`admins`에 추가 후 다시 커밋·푸시.

---

## 동시 사용 / 안전장치
- **1인 편집 락**: 한 번에 한 명만 편집. 다른 사람은 "🔒 사용 중" 대기, 약 2.5분 미사용 시 자동 해제, 하단 "🔓 사용 종료"로 즉시 양보. 관리자는 강제 인수 가능.
- **DB 영구 저장**: 데이터는 Neon Postgres에 안전 보존. (DATABASE_URL 미설정이면 자동으로 기존 JSON 파일 방식으로 폴백 — 로컬 개발용)
- 화면은 약 3초 캐시 후 갱신됩니다.

## 로컬에서만 쓸 때
DATABASE_URL을 설정하지 않으면 예전처럼 `ar_app/data/*.json` 파일에 저장됩니다(그 PC 한정). 클라우드 배포 때만 위 ①·④가 필요합니다.
