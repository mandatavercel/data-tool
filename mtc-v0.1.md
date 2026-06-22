# MTC v0.1 — Mandata Taxonomy Code

**KR-equity classification system that sits on top of GICS but drills deeper where Korea actually moves.**

Every node maps to:
1. Closest **GICS** sub-industry (for global investor handoff)
2. Anchor **KR tickers**
3. Mandata **primitives** that observe it (card · POS · customs · footprint · text · telecom)
4. Investor-attribute **tags** (BRANDED · EXPORT-KFOOD · CHINA-DTC · CYCLICAL …)

Stats: **18 sectors · 60+ subsectors · 27+ categories · 23 tags**

---

## Why MTC (not just GICS)

Pure GICS is the right top tier — and the wrong middle tier for Korea.

**1. GICS-compat top tier.** Top codes map cleanly to GICS sectors / industry groups. PMs at Citadel or BlackRock can hand off MTC tags without translation — Bloomberg / Refinitiv ↔ MTC stays lossless at L1.

**2. KR-native drill-down.** GICS puts Hotel Shilla under "Hotels & Resorts" but the company is ~80% duty-free cosmetics. MTC carries `HL-DTF` (Duty-Free) as its own node so the read across to Amorepacific actually reflects how the cash flows.

**3. Alt-data-aware tags.** `EXPORT-KBEAUTY · CHINA-DTC · CYCLICAL · IP-DRIVEN · COMMODITY-LINKED` — orthogonal to the hierarchy, queryable. Lets a quant team say "long CHINA-DTC, short CYCLICAL+COMMODITY-LINKED" in one filter.

### What v0.1 still doesn't handle (roadmap to v0.2)

- **Revenue-mix weights.** CJ Cheiljedang carries Bio (B2B amino acids) + Consumer F&B + FoodService under one ticker. Today a ticker can sit under multiple nodes; v0.2 will carry a weighted vector per ticker so the signal blends correctly.
- **Geographic exposure.** KR cosmetics is dominated by China DTC; semis is 60–80% export. Geo deserves a first-class axis, not just the `EXPORT-*` / `CHINA-DTC` tags it lives in today.
- **B2C / B2B / B2G split.** Already a tag, but should become required metadata per node so card signals don't get diluted by B2B-heavy parents.
- **MTC × Channel cross-product.** Same instant-ramen SKU sold via CVS vs duty-free vs e-com vs export shows wildly different elasticity. v0.2 surfaces an MTC × Channel coverage cube.

---

## Code format

Two- to four-segment hierarchical code.

```
FB - PKG - NDL - INR
│    │     │     │
│    │     │     └── Subcategory  (Instant Ramen)
│    │     └────── Category     (Noodles)
│    └──────────── Subsector    (Packaged Food)
└───────────────── Sector       (Food & Beverage)
```

- **GICS map:** Consumer Staples · Packaged Foods & Meats
- **Anchors:** 004370 Nongshim · 001800 Ottogi
- **Tags:** BRANDED · EXPORT-KFOOD · VALUE

---

## Full mapping map

### FB — Food & Beverage  · 식음료
- **GICS:** Consumer Staples · Food, Beverage & Tobacco
- **Sources:** card · POS · customs · footprint · text
- **Intro:** Packaged + fresh + beverages + alcohol. Excludes Restaurants (lives under HL).

#### FB-PKG · Packaged Food  *(GICS: Packaged Foods & Meats)*
| Code | Name | Anchor tickers | Tags |
|---|---|---|---|
| FB-PKG-NDL | Noodles · ramen | 004370 Nongshim · 001800 Ottogi · 049770 Dongwon F&B | BRANDED · EXPORT-KFOOD · VALUE |
| FB-PKG-SNK | Snacks · confectionery | 271560 Orion · 097950 CJ Cheiljedang | BRANDED · EXPORT-KFOOD |
| FB-PKG-SCE | Sauces · seasonings | 001800 Ottogi · 002270 Lotte F&B | BRANDED · OFF-PREMISE |
| FB-PKG-FRZ | Frozen · HMR | 097950 CJ Cheiljedang · 280360 Lotte Wellfood | CONVENIENCE · COLD-CHAIN · BRANDED |
| FB-PKG-MEA | Processed meat | 136490 Sunjin · 017810 Pulmuone | COMMODITY-LINKED · COLD-CHAIN |

#### FB-BEV · Beverages (non-alcoholic)  *(GICS: Soft Drinks & Non-alcoholic Beverages)*
| Code | Name | Anchor | Tags |
|---|---|---|---|
| FB-BEV-SFT | Soft drinks · sparkling | 051900 LG H&H · 035250 KT&G (HK Inno.N) | BRANDED · VALUE |
| FB-BEV-COF | Coffee · RTD coffee | 097950 CJ Cheiljedang · 017810 Pulmuone | BRANDED · CONVENIENCE · PREMIUM |
| FB-BEV-WAT | Bottled water | 051900 LG H&H | BRANDED · VALUE |
| FB-BEV-FNC | Functional · wellness | 271940 Hyundai Bioland · 271560 Orion | HEALTH-WELLNESS · PREMIUM |

#### FB-ALC · Alcoholic beverages  *(GICS: Brewers · Distillers & Vintners)*
| Code | Name | Anchor | Tags |
|---|---|---|---|
| FB-ALC-BER | Beer | 000080 Hite Jinro | REGULATED · ON-PREMISE |
| FB-ALC-SOJ | Soju · local spirit | 000080 Hite Jinro · 005300 Lotte Chilsung | REGULATED · ON-PREMISE · EXPORT-KFOOD |
| FB-ALC-WSK | Whisky · premium | 005300 Lotte Chilsung | PREMIUM · REGULATED |
| FB-ALC-MAK | Makgeolli · traditional | 007570 Ilyang Pharm | REGULATED · EXPORT-KFOOD |

#### FB-FRS · Fresh & Perishable  *(GICS: Agricultural Products & Services)*
| Code | Name | Anchor | Tags |
|---|---|---|---|
| FB-FRS-DRY | Dairy | 004990 Lotte Holdings · 002270 Lotte F&B | COMMODITY-LINKED · COLD-CHAIN · FRESH-PERISHABLE |
| FB-FRS-MTS | Meat · seafood | 136490 Sunjin · 017810 Pulmuone | COMMODITY-LINKED · FRESH-PERISHABLE |
| FB-FRS-FRT | Fruit · vegetables | 004410 Sajo Industries | FRESH-PERISHABLE · COMMODITY-LINKED |

#### FB-FRM · Fermented · Traditional  *(GICS: Packaged Foods & Meats — KR-local)*
| Code | Name | Anchor | Tags |
|---|---|---|---|
| FB-FRM-KIM | Kimchi | 001680 Daesang · 011170 Lotte Chemical | EXPORT-KFOOD · OFF-PREMISE |
| FB-FRM-JNG | Jang · sauces | 001680 Daesang · 001800 Ottogi | EXPORT-KFOOD · BRANDED |

#### FB-BIO · Bio · ingredients (B2B)  *(GICS: Agricultural Products & Services — B2B segment)*
- Anchor: 097950 CJ Cheiljedang (Bio · lysine · methionine)

#### FB-BEV-SFT update
- Soft drinks · sparkling: 051900 LG H&H (Coca-Cola Korea bottling) · 005300 Lotte Chilsung
- (Earlier draft mis-attributed 035250 — that ticker is Kangwon Land, not KT&G.)

---

### TB — Tobacco  · 담배·KT&G
- **GICS:** Consumer Staples · Food, Beverage & Tobacco
- **Sources:** customs · text
- **Intro:** Dominated by KT&G (state-origin monopoly). Carries a parallel health/ginseng line (KGC · 정관장) and growing next-gen heated-tobacco exports.

#### TB-CIG · Cigarettes  *(GICS: Tobacco)*
| Code | Name | Anchor | Tags |
|---|---|---|---|
| TB-CIG-TRD | Traditional combustible | 033780 KT&G | REGULATED · DEFENSIVE · B2C |
| TB-CIG-NGN | Next-gen · heat-not-burn (lil) | 033780 KT&G | REGULATED · PREMIUM · B2C |

#### TB-EXP · Tobacco exports
- Anchor: 033780 KT&G (Middle East · SEA volumes)
- GICS: Tobacco (export segment)

#### TB-GIN · Ginseng · health (KGC)
- Anchor: 033780 KT&G (Korea Ginseng Corp · 정관장 brand)
- GICS: Personal Care Products (KR-local)

> Note: KT&G is unusual — single ticker spans tobacco (cigarettes + HNB), health (ginseng / KGC), and modest pharma exposure. Sits naturally under TB but a v0.2 revenue-mix vector should split it: TB-CIG ~55% · TB-GIN ~30% · others ~15% (approximate, FY-end varies).

---

### CB — Cosmetics & Beauty  · 화장품
- **GICS:** Consumer Staples · Household & Personal Products
- **Sources:** card · POS · footprint · customs · text
- **Intro:** K-Beauty universe. Heavy China DTC + duty-free exposure; channel mix drives margin.

#### CB-SKN · Skincare  *(GICS: Personal Care Products)*
| Code | Name | Anchor | Tags |
|---|---|---|---|
| CB-SKN-LUX | Luxury · prestige | 090430 Amorepacific (Sulwhasoo) · 051900 LG H&H (Whoo) | PREMIUM · CHINA-DTC · INBOUND-TOURISM · EXPORT-KBEAUTY |
| CB-SKN-MAS | Masstige | 090430 Amorepacific (Laneige) · 192820 Cosmax | BRANDED · EXPORT-KBEAUTY |
| CB-SKN-DRM | Derma · dermatologist | 192820 Cosmax · 214450 Pharmaresearch | HEALTH-WELLNESS · PREMIUM |

#### CB-MKP · Makeup · Color
| Code | Name | Anchor | Tags |
|---|---|---|---|
| CB-MKP-COL | Color cosmetics | 090430 Amorepacific (Etude) · 081660 Fila Holdings | BRANDED · EXPORT-KBEAUTY |
| CB-MKP-LIP | Lips | 192820 Cosmax · 214390 Cosmecca Korea | BRANDED · EXPORT-KBEAUTY |

#### CB-OEM · ODM · OEM
- Anchors: 192820 Cosmax · 214450 Pharmaresearch · 214390 Cosmecca

#### CB-MEN · Men's grooming
- Anchors: 090435 Amorepacific Pref · 051915 LG H&H Pref

---

### HP — Household & Personal Goods  · 생활용품
- **GICS:** Consumer Staples · Household Products
- **Sources:** card · POS

| Subsector | GICS | Anchors |
|---|---|---|
| HP-DET · Detergents · cleaners | Household Products | 051900 LG H&H |
| HP-PAP · Tissue · paper hygiene | Household Products | 004990 Kleannara |
| HP-ORL · Oral · personal care | Personal Care Products | 051900 LG H&H |

---

### AP — Apparel & Luxury  · 패션
- **GICS:** Consumer Discretionary · Textiles, Apparel & Luxury Goods
- **Sources:** card · POS · footprint

| Subsector | GICS | Anchors |
|---|---|---|
| AP-PRE · Premium · contemporary | Apparel, Accessories & Luxury Goods | 007700 F&F · 105630 Hansae |
| AP-CAS · Casual · streetwear | Apparel, Accessories & Luxury Goods | 007700 F&F · 081660 Fila Holdings |
| AP-ATH · Athleisure · sportswear | Apparel, Accessories & Luxury Goods | 081660 Fila Holdings |
| AP-OEM · Apparel OEM · ODM | Apparel, Accessories & Luxury Goods | 105630 Hansae |

---

### HL — Hotels · Restaurants · Leisure  · 여행·외식·레저
- **GICS:** Consumer Discretionary · Hotels, Restaurants & Leisure
- **Sources:** card · footprint · telecom

| Subsector | GICS | Anchors |
|---|---|---|
| HL-DTF · Duty-free | Hotels, Resorts & Cruise Lines (KR-channel-deep) | 008770 Hotel Shilla · 023590 Lotte Tour |
| HL-HTL · Hotels · resorts | Hotels, Resorts & Cruise Lines | 008770 Hotel Shilla |
| HL-CSN · Casinos · IR | Casinos & Gaming | 034230 Paradise · 035250 Kangwon Land |
| HL-RES · Restaurants · QSR | Restaurants | CJ Foodville · KC |
| HL-CAF · Cafés · F&B chains | Restaurants | 247540 Ediya Coffee |

---

### RT — Retail · Distribution  · 유통
- **GICS:** Consumer Staples · Consumer Staples Distribution & Retail
- **Sources:** card · POS · footprint

| Subsector | GICS | Anchors |
|---|---|---|
| RT-DEP · Department stores | Multiline Retail | 069960 Hyundai Dept · 004170 Shinsegae · 023530 Lotte Shopping |
| RT-CVS · Convenience stores | Food Retail | 282330 BGF Retail · 007070 GS Retail |
| RT-MRT · Hypermarket · super | Food Retail | 139480 Emart · 023530 Lotte Shopping |
| RT-HBR · H&B specialty | Specialty Stores | 161890 ABLE C&C |
| RT-FUR · Home furnishing | Homefurnishing Retail | 009290 Hanssem |

---

### IT — Internet · Platform  · 인터넷·플랫폼
- **GICS:** Communication Services · Interactive Media & Services
- **Sources:** card · text · footprint

| Subsector | GICS | Anchors |
|---|---|---|
| IT-ECM · E-commerce platforms | Internet & Direct Marketing Retail | 035420 NAVER (Smart Store) · CPNG Coupang |
| IT-SCH · Search · ad networks | Interactive Media & Services | 035420 NAVER · 035720 Kakao |
| IT-FIN · Fintech · payments | Data Processing & Outsourced Services | 377300 KakaoPay |
| IT-DEL · Delivery · quick-commerce | Internet & Direct Marketing Retail | Coupang Eats · Kakao Talk-route |

---

### MC — Media · K-Content  · 미디어·콘텐츠
- **GICS:** Communication Services · Entertainment
- **Sources:** card · text

| Subsector | GICS | Anchors |
|---|---|---|
| MC-MUS · K-Pop labels | Movies & Entertainment | 352820 HYBE · 041510 SM · 035900 JYP · 122870 YG |
| MC-DRA · K-Drama · TV | Movies & Entertainment | 079160 CJ ENM · 067160 AfreecaTV |
| MC-FLM · Film · cinema | Movies & Entertainment | 079160 CJ ENM · 079160 CJ CGV |
| MC-PUB · Publishing · webtoon | Publishing | 035420 NAVER (Webtoon) · 035720 Kakao (Picoma) |
| MC-GAM · Gaming | Interactive Home Entertainment | 036570 NCsoft · 251270 Netmarble · 112040 Krafton · 293490 Kakao Games |

---

### TC — Telecom Services  · 통신
- **GICS:** Communication Services · Telecommunication Services
- **Sources:** telecom · footprint

| Subsector | GICS | Anchors |
|---|---|---|
| TC-MOB · Mobile carriers | Wireless Telecom Services | 030200 KT · 017670 SK Telecom · 032640 LG U+ |
| TC-FIX · Fixed line · IPTV | Integrated Telecom Services | 030200 KT |

---

### SM — Semiconductor  · 반도체
- **GICS:** Information Technology · Semiconductors & Semi Equipment
- **Sources:** customs · text
- **Intro:** KR's biggest export sector. Memory + HBM is the cycle; logic foundry + equipment + materials are the support cast.

#### SM-MEM · Memory  *(GICS: Semiconductors)*
| Code | Name | Anchor | Tags |
|---|---|---|---|
| SM-MEM-DRM | DRAM | 005930 Samsung · 000660 SK Hynix | CYCLICAL · EXPORT-MEMORY · B2B |
| SM-MEM-NND | NAND | 005930 Samsung · 000660 SK Hynix | CYCLICAL · EXPORT-MEMORY · B2B |
| SM-MEM-HBM | HBM | 000660 SK Hynix · 005930 Samsung | CYCLICAL · EXPORT-MEMORY · B2B |

#### Other SM subsectors
| Subsector | GICS | Anchors |
|---|---|---|
| SM-LGC · Logic · foundry | Semiconductors | 005930 Samsung (Foundry) · 000990 DB HiTek |
| SM-EQP · Semi equipment | Semiconductor Equipment | 042700 Hanmi Semi · 294870 Hana Materials · 240810 Wonik IPS |
| SM-MAT · Semi materials | Semiconductor Equipment (materials) | 036490 SK Materials · 278280 Cheil Industrial Tech |
| SM-PKG · Packaging · OSAT | Semiconductors | 005935 Samsung Pref |

---

### BT — Battery · EV materials  · 이차전지·소재
- **GICS:** Industrials / IT · Electrical Components
- **Sources:** customs

| Subsector | GICS | Anchors |
|---|---|---|
| BT-CEL · Cell makers | Industrial Conglomerates | 373220 LG Energy Solution · 006400 Samsung SDI · 096770 SK Innovation |
| BT-CTH · Cathodes | Specialty Chemicals | 003670 Posco Future M · 247540 EcoPro BM · 086520 Ecopro |
| BT-ANO · Anodes · graphite | Specialty Chemicals | 003670 Posco Future M |
| BT-SEP · Separators | Specialty Chemicals | 298050 SK ie Technology |
| BT-EQP · Battery equipment | Industrial Machinery | 278280 Cheil Eng. · 108860 Cosmos Y |

---

### AM — Auto & Mobility  · 자동차·모빌리티
- **GICS:** Consumer Discretionary · Automobiles & Components
- **Sources:** customs · text

| Subsector | GICS | Anchors |
|---|---|---|
| AM-OEM · Auto OEM | Automobile Manufacturers | 005380 Hyundai Motor · 000270 Kia |
| AM-SUP · Auto parts | Auto Parts & Equipment | 012330 Hyundai Mobis · 204320 HL Mando |
| AM-TIR · Tires | Tires & Rubber | 161390 Hankook Tire & Tech |
| AM-EVC · EV components | Auto Parts & Equipment | 012450 Hanwha Aerospace · 267260 Hyundai Electric |

---

### HC — Health Care  · 헬스케어
- **GICS:** Health Care · Pharmaceuticals · Biotech · Devices
- **Sources:** text
- **Intro:** KR bio biased toward biosimilars + CDMO; classical pharma is small relative to global.

| Subsector | GICS | Anchors |
|---|---|---|
| HC-CDM · Bio · CDMO | Biotechnology | 207940 Samsung Biologics · 302440 SK Bioscience |
| HC-BSM · Biosimilars | Biotechnology | 068270 Celltrion |
| HC-PHM · Pharma (small mol) | Pharmaceuticals | 000100 Yuhan · 271940 Hyundai Bioland |
| HC-MED · Medical devices | Health Care Equipment | 091990 Celltrion Healthcare · 017900 AI Holdings |

---

### IN — Industrials  · 산업재
- **GICS:** Industrials · Capital Goods · Transportation
- **Sources:** customs
- **Intro:** Shipbuilding orderbook + construction backlog drive most of the cyclical signal.

| Subsector | GICS | Anchors |
|---|---|---|
| IN-SHP · Shipbuilding · marine | Construction & Engineering | 009540 HD Korea Shipbuilding · 010140 Samsung Heavy · 042660 Hanwha Ocean |
| IN-CON · Construction · E&C | Construction & Engineering | 028050 Samsung E&A · 000720 Hyundai E&C · 047040 Daewoo E&C |
| IN-MCH · Industrial machinery | Industrial Machinery | 267250 Hyundai Heavy · 079550 LIG Nex1 |
| IN-DEF · Defence · aerospace | Aerospace & Defense | 012450 Hanwha Aerospace · 047810 Korea Aerospace · 272210 Hanwha Systems |
| IN-LOG · Logistics · transport | Air Freight & Logistics | 011200 HMM · 044820 Cosmo Chemical |

---

### MT — Materials  · 소재
- **GICS:** Materials · Chemicals · Metals & Mining
- **Sources:** customs

| Subsector | GICS | Anchors |
|---|---|---|
| MT-CHM · Chemicals · petrochem | Commodity Chemicals | 051910 LG Chem · 011170 Lotte Chemical · 298020 HD Hyundai Chemical |
| MT-STL · Steel · metals | Steel | 005490 POSCO Holdings · 004020 Hyundai Steel |
| MT-PAP · Paper · packaging | Paper Packaging | 004540 Daehan Steel |

---

### FN — Financials  · 금융
- **GICS:** Financials · Banks · Insurance · Securities
- **Sources:** (none yet — coverage roadmap item)

| Subsector | GICS | Anchors |
|---|---|---|
| FN-BNK · Banks · financial hldgs | Banks | 105560 KB Financial · 055550 Shinhan · 086790 Hana · 316140 Woori |
| FN-INS · Insurance | Insurance | 032830 Samsung Life · 001450 Hyundai Marine & Fire |
| FN-SEC · Securities · brokerage | Capital Markets | 005940 NH Investment · 008560 Mirae Asset · 030610 Kyobo |

---

### EU — Energy · Utilities  · 에너지·유틸리티
- **GICS:** Energy · Utilities
- **Sources:** customs

| Subsector | GICS | Anchors |
|---|---|---|
| EU-REF · Refining · oil & gas | Oil, Gas & Consumable Fuels | 010950 S-Oil · 096770 SK Innovation · 078930 GS Holdings |
| EU-PWR · Power · utilities | Electric Utilities | 015760 KEPCO · 036460 KOGAS |
| EU-RNW · Renewables · solar/wind | Independent Power Producers | 009830 Hanwha Solutions |

---

## Tag glossary (23 tags, orthogonal axis)

| Tag | Meaning |
|---|---|
| BRANDED | Brand premium / pricing power |
| COMMODITY-LINKED | Raw-material price sensitive |
| PREMIUM | Premium / luxury mix |
| VALUE | Value / discount mix |
| HEALTH-WELLNESS | Health / functional / wellness |
| CONVENIENCE | Ready-to-eat / convenience |
| FRESH-PERISHABLE | Fresh / perishable / spoilage |
| COLD-CHAIN | Requires refrigeration / freezing |
| REGULATED | Alcohol / tobacco / infant / claims |
| ON-PREMISE | Restaurant / bar / café consumption |
| OFF-PREMISE | Retail / e-com / take-home |
| EXPORT-KFOOD | Korean food export theme |
| EXPORT-KBEAUTY | K-Beauty export / China DTC theme |
| EXPORT-MEMORY | Memory / semis export theme |
| PRIVATE-LABEL | PB / store brand |
| B2B | Sells to other businesses |
| B2C | Sells to consumers |
| B2G | Sells to government / institutions |
| CYCLICAL | Macro-cyclical earnings |
| DEFENSIVE | Macro-defensive earnings |
| CHINA-DTC | China DTC + duty-free exposure |
| INBOUND-TOURISM | KR inbound tourism flow |
| IP-DRIVEN | IP / catalog monetisation |

---

## Questions for GPT review

When asking for a critique, useful angles:

1. **Coverage gaps.** Are there KR sectors / sub-sectors we're missing that institutional investors care about? (Shipbuilding modular yards? Defence drone? Bio-CMO sub-segments? KR-listed China holdcos?)
2. **GICS mapping correctness.** Have we mis-mapped anything to GICS at the sector level?
3. **Tag system.** Are 23 tags too many / too few / orthogonal enough? Any redundant or missing dimensions (geo, B2C/B2B, time-of-day, customer-cohort)?
4. **Code-format pragmatics.** Is the 2–4 segment `SECTOR-SUBSECTOR-CATEGORY-SUB` format too rigid? Should we leave room for a 5th level (sub-sub)?
5. **Roadmap priorities.** Of the four v0.2 items (revenue-mix weights · geo axis · B2C/B2B required · MTC × Channel cube), which would have the biggest impact for a quant team? For a discretionary HF?
6. **Korean-specific nodes.** Are there KR-native categories we should add that don't map cleanly to global taxonomies (e.g. 김치 · 라면 · 소주 · 면세 · K-Content IP-licensing)?
