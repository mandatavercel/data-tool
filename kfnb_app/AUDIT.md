# K-F&B POS Alt-Data 파이프라인 — 감사 보고서 (Audit)

독립 코드 리뷰 + 정량 관점 점검. 목적은 **약점을 드러내는 것**이며, 투자분석가가
실사(due diligence)에서 던질 질문 기준으로 평가했다. 심각도: **Critical / High / Medium / Low**.

> 핵심 결론: 이 도구는 **가설 생성(hypothesis generation)·조기신호 탐색**으로는 유효하나,
> 현재 산출되는 `usecase`/`alpha` 결과는 **"주가 예측력"·"market share"처럼 실제보다 과신하는
> 표현**을 쓰고 있고, **백테스트 적합성(point-in-time)을 충족하지 못한다.** `assessment.py`가
> 스스로 "조기신호용/제한적"이라고 솔직히 낮춰 평가하는 것과, 다른 모듈의 자신감 있는 표현
> 사이의 **내부 모순**이 가장 큰 문제다.

---

## A. 백테스트 적합성 (가장 치명적)

**A1. [Critical] Point-in-time(PIT)·데이터 가용시점(release lag) 모델링 부재.**
`ingest/dataio.py`의 집계는 달력월(`date/100`) 기준일 뿐, "그 달 데이터가 언제 입수 가능했는가"를
나타내는 `available_date`/`as_of` 컬럼이 없다. POS는 월 마감 + 벤더 ETL 지연(수일~수주) 후에야
쓸 수 있는데, 모든 시그널이 *월초에 그 달 데이터를 안다*고 암묵 가정한다. 상품은 "PIT 패널
(백테스트용)"으로 표기되지만 실제로는 PIT가 아니다. → **`available_date = 월말 + lag` 추가, 모든
시그널을 그 시점 기준으로 산출, lag 분포 문서화.**

**A2. [Critical] 모든 시그널이 전체기간 1회 스냅샷 = look-ahead.**
`usecase._ttm_windows`는 *전체* 데이터의 최근 12개월/직전 12개월을 쓰고, `share_shift`는 첫 완전연도→
마지막 완전연도를 쓴다. 즉 "특정 과거 시점에 이 시그널이 뭐였나"를 재구성할 수 없는 **종점 단일값**이다.
백테스터에게 한 개 숫자만 준다. → **as-of 월별로 walk-forward 시계열 시그널 패널을 emit.**

**A3. [High] 생존편향·재작성(restatement) 편향 — 마스터가 현재 스냅샷.**
`COMPANY_MAP`/`BRAND_MASTER`/GICS/상장여부/ISIN을 현재값으로 전체 과거에 소급 적용. DART 분기매출도
*최신 정정치*를 가져와 원발표치를 덮어쓴다(`disclosures.py`). → **마스터에 effective date 버전 부여,
최초보고 vs 정정 매출 분리 저장.**

**A4. [High] 부분기간 오염(2026 Q1/당월).** 불완전한 최근 월/분기를 잘라내지 않아 `pct_change(12)`,
`pct_change(4)`가 *부분* 당기 vs *완전* 전년동기를 비교 → 가장 actionable한 최신 관측치에 구조적
하향 편향. → **trailing window 불완전 기간 제거/플래그.**

**A5. [Medium·일부 수정] 날짜/주입 안전성.** 날짜가 깔끔한 `YYYYMMDD` 정수라고 가정(NaN 시 `astype(int)`
예외 가능). cat_l2 필터를 문자열 보간하던 쿼리는 **이번에 파라미터 바인딩으로 수정**(SQL 주입·따옴표
이슈 제거). 날짜 도메인 검증은 미적용.

---

## B. 알파 엔진 통계 엄밀성 (`insight/alpha.py`)

**B1. [Critical] best-lag 선택 = 데이터 스누핑.** `leadlag`가 lag 0~6을 스캔하고 `_best`가 |corr|
최대를 고른다. 7개 시행의 최대값을 "선행 N개월"로 보고 → 순수 노이즈에서도 |Spearman|이 쉽게 0.5를
넘는다. → **단일 lag 사전등록 또는 전체 lag 프로파일 + 다중검정 보정 제시.**

**B2. [Critical] 다중가설 보정 없음.** (티커 × 4시그널 × 7lag) ≈ 수십~수백 상관을 |corr|로 정렬해
상위 8개를 출력. Bonferroni/BH(FDR) 없음, 귀무분포 없음. 상위는 선택 인공물. → **FDR 보정 p값,
상위가 아닌 전체 그리드 보고.**

**B3. [Critical] 표본수가 통계적으로 무의미.** `MIN_OBS=8`. 공시 lead-lag는 *분기* 단위 → 종목당
YoY 점이 ~8~12개. n=8 Spearman의 95% CI는 대략 ±0.7. `_conf`는 n≥24·|r|≥0.4를 "high"로 부르지만
24개월도 2년뿐. → **MIN_OBS 대폭 상향(월간 ≥30), CI 표기, 소표본 high 억제.**

**B4. [High] 중첩윈도 자기상관이 상관을 부풀림.** TTM/YoY는 중첩윈도라 본질적으로 자기상관이 크고,
타깃도 forward-shift 중첩 → Spearman 유의성 과대(유효표본·Newey-West 보정 부재). → **비중첩 윈도
또는 유효표본 축소; 레벨이 아닌 수익률로 검정.**

**B5. [High] hit_rate가 스누핑된 lag에서·부호 퇴화.** `share`(항상 양수)·자주 양수인 `sales_yoy`는
sign(signal)이 거의 상수 → hit_rate가 "양(+) 수익률 월 비율"(시장방향)로 붕괴. → **시그널 디민 후
초과수익 부호로, out-of-sample lag에서.**

**B6. [Medium] 분기 인덱스 비연속 shift 오정렬.** 주석은 "연속 정수"라지만 `YYYYQ`(20243) 인덱스에
`shift(-L)`를 적용 → 결측 분기가 위치 기준으로 밀려 선행 시차가 잘못 매겨질 수 있음. → **완전한
연속 분기축으로 reindex 후 shift.**

**B7. [Medium] 종목 3~8개로 횡단면 추론 불가.** 상장 5사로는 어떤 랭킹도 노이즈. → **하드 한계로 명시,
랭킹을 actionable로 제시하지 말 것.**

---

## C. 시그널 타당성 (`insight/usecase.py`)

**C1. [High] "Market share"는 시장점유율이 아님 — 매핑된 소수 유니버스 내 점유율.**
`share`는 (CU 내) 상장 5사 합 또는 panel 내 합 대비 비중. Nielsen식 카테고리 점유율로 오인 위험.
폐쇄집합이라 한 곳의 상승이 다른 곳의 하락으로 강제됨. → **`cu_listed_share`/`cu_channel_share`로
개명, market share라 부르지 말 것.**

**C2. [High] new_hit velocity가 최신 출시 편향 + 파이프필 혼동.** `velocity = 누적매출/개월수`(개월수≥1)
→ 지난달 출시 + 대량 초도물량 SKU가 과대평가. 최소 tenure·코호트 정규화 없음, confidence는 항상 ≥medium.
→ **최소 생존월·코호트 대비 정규화·초도물량 제외.**

**C3. [Medium] 모멘텀 픽이 임의 head+tail.** 작은 분모에서 +수백% YoY 가능(`pri<=0`만 필터). →
**윈저라이즈·최소 기준매출·변동성 스케일.**

**C4. [Medium] ASP 프리미엄 = 가격 vs 믹스 구분 불가.** 멀티팩/대용량 전환만으로 ASP 상승하는데
"프리미엄화/가격인상"으로 단정. → **고정 믹스 ASP 또는 "단위당 매출(믹스영향)"로 라벨.**

---

## D. 매핑·커버리지 (`mapping/*`, config)

**D1. [High] sku_id "안정성"이 슬라이스 의존 → 사실상 불안정.** 충돌 시에만 `barcode[-5:]`를 붙여,
같은 물리 SKU가 추출/섹터필터에 따라 다른 id를 가질 수 있음. barcode 끝 5자리도 유일 보장 안 됨.
→ **항상 전체 바코드(자연키)로 결정적 생성.**

**D2. [High] 로마자 품질이 낮은데 산출물로 출하.** 단순화 RR(연음/받침 미반영)이 `sku_name_romanized`·
비큐레이션 `brand_en`·미등록 카테고리 영문·**브랜드 fallback ID seed**로 사용됨 → 글로벌 투자자에게
나가는 영문명에 로마자가 섞임. → **로마자 필드는 'machine-transliterated'로 명시, ID seed로 쓰지 말 것.**

**D3. [High] ASP=amount/qty가 `sales_qty`=판매단위 가정에 의존(미검증).** 만약 qty가 inner-unit/케이스/
멀티팩=1이면 ASP가 조용히 틀림. 100~100,000원 sanity밴드가 넓어 대부분 통과. → **벤더에 qty 정의 확인,
실판매가와 대조.**

**D4. [Medium] "매출기준 커버리지"가 과대.** `sku_name_en`이 비어있지 않으면 covered로 카운트하는데,
로마자 fallback은 항상 비어있지 않음 → `sku_coverage_pct`≈100%가 구조적. → **커버리지를 'verified/
curated 매출%'로 재정의.** (마케팅의 "매출 X% 매핑" 주장과 직결되는 문제)

**D5. [Medium] GICS/ISIN 하드코딩·외부검증 없음.** ISIN은 Luhn으로 *계산*(소싱 아님)이라 krx_code가
틀리면 체크섬은 맞지만 잘못된 ISIN이 조용히 흐름. → **권위 소스에서 ISIN 소싱·주기 대조.**

---

## E. 소프트웨어/로직 취약점

**E1. [High·일부 수정] duckdb vs pandas 산출 불일치 위험.** (a) cat_l2 문자열 보간 → **이번에 파라미터화
수정.** (b) duck `read_csv_auto`의 타입추론 vs pandas의 문자열 강제 → **바코드 leading-zero 손실이
엔진별로 달라** QC·조인 결과가 갈릴 수 있음(미수정). (c) profile의 nonpos 정의 차이. → **dtype 통일,
duck-vs-pandas 패리티 테스트 추가.**

**E2. [High] `export_daily_en` 바코드 조인 무성 오매핑.** duck가 바코드를 int로 추론(앞 0 손실)하면
문자열 마스터와 조인 실패 → 실제 행에 회사/ISIN이 null로 출하(에러 없음). → **모든 조인 전 바코드를
단일 정규화 문자열로 canonical화.**

**E3. [Medium] 0 division/NaN 처리 비일관.** ASP는 가드되나 `pct_change` 0분모는 inf 미필터,
`share_shift` 분모 미가드. → **safe-divide 헬퍼 일원화.**

**E4. [Medium] 통화/VAT 가정 미문서화.** `sales_amt`가 VAT 포함 소매가인지, 반품/프로모션 제외인지
불명. 회사 순매출(ex-VAT)과 비교 시 레벨·심지어 YoY 방향이 다를 수 있음. → **가격 기준 문서화/ex-VAT 환산.**

**E5. [Medium] 단일채널 감지가 파일명 substring.** `assessment`의 `"CU" in source_name` → 파일명만
바뀌면 헤드라인 한계가 스코어카드에서 조용히 사라짐. → **채널을 데이터/설정에서 도출.**

---

## F. 백사이드 분석가가 반드시 물을 것

1. **"out-of-sample IC 보여줘."** 전부 in-sample·best-lag·n≈8~24·5종목 → walk-forward/train-test/거래비용·
   capacity 분석 없음. 알파 리포트는 기껏 가설생성인데 "예측력"으로 표현.
2. **"거래일에 내가 가졌을 바로 그 숫자인가?"** as_of 없음, 현재 마스터 소급, 정정매출 덮어쓰기,
   부분기간 YoY → PIT 실사 탈락.
3. **"패널의 카테고리 커버리지는?"** CU는 편의점 1체인 — 라면/주류 소매의 소수 채널. "점유율"은
   CU 내 5사 점유율.
4. **"수요 vs 프로모션, 동일점 vs 신규점 어떻게 분리?"** promo/정상가/점포수 없음(assessment가 인정).
5. **"삼양은 수출 ~80% — 국내 POS가 핵심 동인을 놓친다."** assessment는 인정하나 모멘텀/알파는 여전히
   삼양을 국내 대표처럼 랭킹.
6. **"커버리지 100%? 비어있지 않은 로마자 문자열 아냐?"** 커버리지 지표가 게임 가능(D4).

---

## 우선 수정 순위 (권고)

| 순위 | 항목 | 효과 |
|---|---|---|
| 1 | A1·A2 PIT/walk-forward 시그널 패널 + available_date | "백테스트용" 주장 충족의 전제 |
| 2 | B1·B2·B3 다중검정·표본·best-lag 보정 | 알파 엔진에 통계적 신뢰성 부여 |
| 3 | C1·D4 "market share"·"coverage" 표현 정정 | 헤드라인 지표의 오인 제거 |
| 4 | E1·E2 바코드 dtype 통일 + 패리티 테스트 | 산출물 식별자 무성 오류 차단 |
| 5 | D1 sku_id 항상 바코드 기반 | ID 안정성(시계열) 확보 |

**이미 이번 라운드에 반영:** cat 영문화, daily 카테고리 영문, cat_l2 쿼리 파라미터화(주입/따옴표),
assessment의 솔직한 한계 명시.

> 한 줄: **"정직한 alt-data 탐색 도구로는 잘 만들어졌으나, '백테스트 가능한 알파 데이터'로
> 판매하려면 PIT·다중검정·표현 정정·바코드 정합 4가지를 반드시 먼저 고쳐야 한다."**
