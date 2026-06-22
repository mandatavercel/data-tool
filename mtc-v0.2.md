# MTC v0.2 — Mandata Taxonomy Code

**MTC is Mandata's proprietary classification system for Korean equities and alternative data, built to remain compatible with GICS while drilling deeper into Korea-specific revenue drivers, consumption channels, and investable themes. Each MTC node links to relevant Korean tickers, observable Mandata data primitives, and investor-facing attributes, enabling global investors to analyse Korean markets at a more decision-useful level than standard industry classification alone.**

23 sectors · ~70 subsectors · ~50 categories · ~70 L4 leaves (FB + CB + TB + MC-GAM + SM-MEM drilled to leaf) · 23 tags grouped into 8 groups.

---

## Code format

**Standardised four-level hierarchy:**

```
Sector - Subsector - Category - Subcategory
```

L1–L3 are valid aggregation codes (portfolio rollups, reporting, dashboarding). **L4 is the canonical leaf** — what an instrument, transaction, or product should resolve to wherever possible.

**Example:**

```
FB - PKG - NDL - INR
│    │     │     │
│    │     │     └── L4 Subcategory  Instant Ramen   ← leaf
│    │     └──────── L3 Category     Noodles / Ramen
│    └────────────── L2 Subsector    Packaged Food
└─────────────────── L1 Sector       Food & Beverage
```

| Code | Level | Meaning |
|---|---|---|
| `FB` | L1 | Food & Beverage sector aggregate |
| `FB-PKG` | L2 | Packaged Food subsector aggregate |
| `FB-PKG-NDL` | L3 | Noodles / Ramen category aggregate |
| `FB-PKG-NDL-INR` | **L4** | **Instant Ramen leaf-level code (default)** |

Anchors for `FB-PKG-NDL-INR`: 004370 Nongshim (Shin) · 001800 Ottogi (Jin) · 248170 Samyang Foods (Buldak).
GICS map: Consumer Staples · Packaged Foods & Meats. Tags: BRANDED · EXPORT-KFOOD · VALUE.

---

## Why MTC (not just GICS)

**Pure GICS is the right top tier — and the wrong middle tier for Korea.**

1. **GICS-compat top tier.** L1 sector codes map cleanly to GICS sectors / industry groups. Bloomberg / Refinitiv ↔ MTC stays lossless at L1.
2. **KR-native drill-down.** GICS puts Hotel Shilla under "Hotels & Resorts" but ~80% of revenue is duty-free cosmetics. MTC carries `HL-DTF` as its own node so the read-across to Amorepacific actually reflects how the cash flows.
3. **Alt-data-aware leaves.** L4 is picked so each leaf resolves to one or more Mandata-observable primitives (card · POS · customs · footprint · text · telecom).

### v0.3 roadmap (in priority order)

1. **Revenue-mix weights** — Many KR conglomerates (CJ Cheiljedang, LG H&H, KT&G) span multiple MTC nodes. v0.3 attaches a weighted vector per ticker so signals blend correctly.
2. **Channel Axis (separate dimension)** — Today RT and HL-DTF mix product taxonomy with sales channel. v0.3 promotes Channel to a first-class dimension (CVS · E-com · Duty-free · Foodservice · Export) and removes the channel-ish nodes from MTC.
3. **Geography Axis** — KR / CN / US / JP / SEA / global becomes a required axis.
4. **B2C / B2B / B2G required metadata** — currently a tag; v0.3 makes it required per leaf.
5. **Full GICS 4-column split** — v0.2 carries a single GICS string per subsector; v0.3 splits into Sector / Industry Group / Industry / Sub-Industry columns.

---

## Full mapping map (23 sectors)

> Sectors with full L4 drill-down: FB, CB, TB, MC (gaming), SM (memory). Others carry L2/L3 anchors only — L4 lands as we add coverage.

### FB — Food & Beverage  · 식음료
- **GICS:** Consumer Staples · Food, Beverage & Tobacco
- **Sources:** card · POS · customs · footprint · text
- Packaged + fresh + beverages + alcohol + bio ingredients. Excludes Restaurants (HL) and Tobacco (TB).

#### FB-PKG · Packaged Food
- GICS: Packaged Foods & Meats

| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| NDL | INR | Instant Ramen (bag) | 004370 Nongshim · 001800 Ottogi · 248170 Samyang Foods | BRANDED · EXPORT-KFOOD · VALUE |
| NDL | CUP | Cup Ramen | 004370 Nongshim · 001800 Ottogi · 248170 Samyang Foods | BRANDED · CONVENIENCE · EXPORT-KFOOD |
| RIC | IRB | Instant Rice | 097950 CJ Cheiljedang (Hetbahn) · 001800 Ottogi | CONVENIENCE · BRANDED |
| HMR | RTE | Ready-to-Eat | 097950 CJ Cheiljedang (Bibigo) · 280360 Lotte Wellfood · 017810 Pulmuone | CONVENIENCE · BRANDED · COLD-CHAIN |
| HMR | RTC | Ready-to-Cook | 097950 CJ Cheiljedang · 017810 Pulmuone | CONVENIENCE · BRANDED |
| SNK | CHP | Chips | 271560 Orion · 004370 Nongshim · 280360 Lotte Wellfood | BRANDED · EXPORT-KFOOD |
| SNK | BIS | Biscuits / cookies | 271560 Orion · 280360 Lotte Wellfood | BRANDED |
| SNK | CDY | Confectionery (candy/choc) | 271560 Orion · 280360 Lotte Wellfood | BRANDED |
| SCE | JNG | Jang-based sauces (gochujang · ssamjang) | 097950 CJ Cheiljedang (Haechandle) · 001680 Daesang (Chungjungone) · 001800 Ottogi | EXPORT-KFOOD · BRANDED |
| SCE | SOY | Soy sauce · vinegar | 001680 Daesang · 097950 CJ Cheiljedang | BRANDED |
| SCE | OIL | Cooking oils | 001800 Ottogi · 002270 Lotte F&B | COMMODITY-LINKED · OFF-PREMISE |
| FRZ | DMP | Frozen dumplings (mandu) | 097950 CJ Cheiljedang (Bibigo) · 280360 Lotte Wellfood | CONVENIENCE · COLD-CHAIN · EXPORT-KFOOD |
| FRZ | PZA | Frozen pizza | 097950 CJ Cheiljedang · 001800 Ottogi | CONVENIENCE · COLD-CHAIN |
| MEA | — | Processed Meat *(L3 agg)* | 136490 Sunjin · 017810 Pulmuone | — |

#### FB-BEV · Beverages (non-alcoholic)
- GICS: Soft Drinks & Non-alcoholic Beverages

| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| SFT | — | Soft drinks · sparkling *(L3 agg)* | 051900 LG H&H (Coca-Cola KR bottling) · 005300 Lotte Chilsung | — |
| COF | RTD | RTD coffee (canned · bottled) | 005300 Lotte Chilsung (Kanu RTD via JV) · 097950 CJ Cheiljedang | BRANDED · CONVENIENCE · PREMIUM |
| COF | INS | Instant coffee / sticks | 006040 Dongsuh (Maxim) | BRANDED · CONVENIENCE |
| TEA | BOT | Bottled tea (green · barley · 17차) | 005300 Lotte Chilsung · 051900 LG H&H · 097950 CJ Cheiljedang | BRANDED · HEALTH-WELLNESS |
| ENG | ENG | Energy drinks (Hot6 · Bacchus) | 005300 Lotte Chilsung (Hot6) · 000100 Yuhan (Bacchus) | BRANDED · VALUE |
| WAT | — | Bottled water *(L3 agg)* | 051900 LG H&H · 005300 Lotte Chilsung | — |
| FNC | — | Functional · wellness *(L3 agg)* | 271940 Hyundai Bioland · 001680 Daesang Wellife | — |

#### FB-ALC · Alcoholic Beverages
- GICS: Brewers · Distillers & Vintners

| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| SOJ | SOJ | Soju | 000080 HiteJinro (Chamisul) · 005300 Lotte Chilsung (Saero · Chum-churum) | REGULATED · ON-PREMISE · EXPORT-KFOOD |
| BER | — | Beer *(L3 agg)* | 000080 HiteJinro · 005300 Lotte Chilsung | — |
| RTD | HBL | Highball · canned cocktails | 000080 HiteJinro · 005300 Lotte Chilsung | REGULATED · PREMIUM · CONVENIENCE |
| WSK | — | Whisky · premium *(L3 agg)* | 005300 Lotte Chilsung (Scotch import) | — |
| MAK | — | Makgeolli *(L3 agg)* | 043710 Seoul Jangsu (Kooksoondang group) · 071050 Kooksoondang | — |

#### FB-FRS · Fresh & Perishable
- GICS: Agricultural Products & Services

| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| DRY | MLK | Milk | 267980 Maeil Dairies · 003920 Namyang Dairy | FRESH-PERISHABLE · COLD-CHAIN · COMMODITY-LINKED |
| DRY | YOG | Yoghurt · kefir | 267980 Maeil Dairies · 004990 Lotte Holdings | FRESH-PERISHABLE · COLD-CHAIN · HEALTH-WELLNESS |
| MTS | MTF | Meat (fresh) | 136490 Sunjin · 017810 Pulmuone | COMMODITY-LINKED · FRESH-PERISHABLE · COLD-CHAIN |
| MTS | SEA | Seafood (fresh) | 004410 Sajo Industries · 049770 Dongwon F&B | COMMODITY-LINKED · FRESH-PERISHABLE · EXPORT-KFOOD |
| FRT | — | Fruit · Vegetables *(L3 agg)* | 004410 Sajo · 017810 Pulmuone | — |

#### FB-FRM · Fermented · Traditional
- GICS: Packaged Foods & Meats (KR-local)

| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| KIM | PKG | Packaged kimchi (retail) | 097950 CJ Cheiljedang (Bibigo) · 001680 Daesang (Jongga) · 017810 Pulmuone | EXPORT-KFOOD · OFF-PREMISE · COLD-CHAIN |
| KIM | FSV | Foodservice kimchi (B2B) | 001680 Daesang · 097950 CJ Cheiljedang | B2B · OFF-PREMISE |
| SEA | GIM | Gim · roasted seaweed | 049770 Dongwon F&B · 097950 CJ Cheiljedang | EXPORT-KFOOD · BRANDED · HEALTH-WELLNESS |
| JNG | — | Jang · traditional sauces *(L3 agg)* | 001680 Daesang · 097950 CJ · 001800 Ottogi | — |

#### FB-BIO · Bio · Ingredients (B2B)
- GICS: Agricultural Products & Services (B2B segment)

| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| AMN | LYS | Lysine | 097950 CJ Cheiljedang (Bio) | B2B · COMMODITY-LINKED · CYCLICAL |
| AMN | MET | Methionine | 097950 CJ Cheiljedang (Bio) | B2B · COMMODITY-LINKED · CYCLICAL |
| ADD | SWT | Sweeteners (allulose · stevia) | 271940 Hyundai Bioland · 097950 CJ Cheiljedang | B2B · HEALTH-WELLNESS |

---

### TB — Tobacco  · 담배·KT&G
- **GICS:** Consumer Staples · Food, Beverage & Tobacco
- **Sources:** customs · text
- KT&G-dominated. Single-ticker, multi-segment (cigarettes + KGC ginseng) — needs revenue-mix vector in v0.3.

| L2 | L3 | L4 | Leaf | Anchor | Tags |
|---|---|---|---|---|---|
| CIG | TRD | DOM | Domestic combustible | 033780 KT&G | REGULATED · DEFENSIVE · B2C |
| CIG | NGN | LIL | Heat sticks (lil) | 033780 KT&G | REGULATED · PREMIUM · B2C |
| EXP | — | — | Tobacco exports *(L2 agg)* | 033780 KT&G (Middle East · SEA) | — |
| GIN | RHP | EXT | Red ginseng extract (정관장) | 033780 KT&G (KGC) | HEALTH-WELLNESS · PREMIUM · EXPORT-KFOOD |
| GIN | RHP | DRK | Red ginseng drinks | 033780 KT&G (KGC) | HEALTH-WELLNESS · CONVENIENCE |

---

### CB — Cosmetics & Beauty  · 화장품
- **GICS:** Consumer Staples · Household & Personal Products
- **Sources:** card · POS · footprint · customs · text
- K-Beauty universe. Heavy China DTC + duty-free. CB-MED covers medical-aesthetic crossover.

#### CB-SKN · Skincare
| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| LUX | CRM | Luxury cream | 090430 Amorepacific (Sulwhasoo) · 051900 LG H&H (Whoo) | PREMIUM · CHINA-DTC · EXPORT-KBEAUTY · INBOUND-TOURISM |
| LUX | SRM | Luxury serum / essence | 090430 Amorepacific (Sulwhasoo) · 051900 LG H&H (Whoo) | PREMIUM · CHINA-DTC · EXPORT-KBEAUTY |
| LUX | MSK | Luxury mask / cushion | 090430 Amorepacific (Sulwhasoo · Hera) · 051900 LG H&H (Whoo) | PREMIUM · CHINA-DTC · EXPORT-KBEAUTY |
| MAS | CRM | Masstige cream | 090430 Amorepacific (Laneige · Innisfree) · 051900 LG H&H (Belief) | BRANDED · EXPORT-KBEAUTY |
| MAS | SLP | Masstige sleeping mask | 090430 Amorepacific (Laneige) | BRANDED · EXPORT-KBEAUTY |
| DRM | CRM | Derma cream | 214450 Pharmaresearch (Rejuran) · 192820 Cosmax (Dr. Jart mfg.) | HEALTH-WELLNESS · PREMIUM |
| DRM | SUN | Derma sunscreen | 090430 Amorepacific (Etude · Aestura) · 214450 Pharmaresearch | HEALTH-WELLNESS · EXPORT-KBEAUTY |

#### CB-MKP · Makeup · Color
| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| COL | FND | Foundation · base | 090430 Amorepacific (Hera · Etude) · 192820 Cosmax | BRANDED · EXPORT-KBEAUTY |
| COL | LIP | Lip products | 192820 Cosmax · 214390 Cosmecca · 090430 Amorepacific | BRANDED · EXPORT-KBEAUTY |
| COL | EYE | Eye / brow | 192820 Cosmax · 214390 Cosmecca | BRANDED · EXPORT-KBEAUTY |

#### CB-OEM · ODM · OEM (manufacturer)
| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| SKN | ODM | Skincare ODM | 192820 Cosmax · 214450 Pharmaresearch · 214390 Cosmecca | B2B · EXPORT-KBEAUTY |
| MKP | ODM | Makeup ODM | 192820 Cosmax · 214390 Cosmecca | B2B · EXPORT-KBEAUTY |

#### CB-MED · Beauty Medical · Aesthetics
| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| TOX | INJ | Toxin injectables | 145020 Hugel · 086900 Medytox · 950160 Daewoong Pharm (Jeuveau) | REGULATED · EXPORT-KBEAUTY · PREMIUM |
| FIL | HA  | HA fillers | 145020 Hugel · 086900 Medytox · 214450 Pharmaresearch | REGULATED · EXPORT-KBEAUTY · PREMIUM |

#### CB-MEN · Men's grooming  *(L2 agg)*
- Anchors: 090430 Amorepacific (men's lines) · 051900 LG H&H (men's lines)

---

### HP — Household & Personal Goods  · 생활용품
- **GICS:** Consumer Staples · Household Products  ·  **Sources:** card · POS

| Subsector | GICS | Anchors |
|---|---|---|
| HP-DET · Detergents · Cleaners | Household Products | 051900 LG H&H |
| HP-PAP · Tissue · Paper hygiene | Household Products | (KR pure-play listings thin — embedded in LG H&H) |
| HP-ORL · Oral · Personal care | Personal Care Products | 051900 LG H&H |

---

### AP — Apparel & Luxury  · 패션
- **GICS:** Consumer Discretionary · Textiles, Apparel & Luxury Goods  ·  **Sources:** card · POS · footprint

| Subsector | Anchors |
|---|---|
| AP-PRE · Premium · Contemporary | 007700 F&F (MLB · Discovery) · 081660 Fila Holdings |
| AP-CAS · Casual · Streetwear | 007700 F&F · 081660 Fila Holdings |
| AP-ATH · Athleisure · Sportswear | 081660 Fila Holdings |
| AP-OEM · Apparel OEM · ODM | 105630 Hansae · 111110 Hosung |

---

### HL — Hotels · Restaurants · Leisure  · 여행·외식·레저
- **GICS:** Consumer Discretionary · Hotels, Restaurants & Leisure  ·  **Sources:** card · footprint · telecom
- **v0.2 NOTE:** HL-DTF will move to the Channel Axis in v0.3.

| Subsector | Anchors |
|---|---|
| HL-DTF · Duty-free | 008770 Hotel Shilla · 023590 Lotte Tour · 069960 Hyundai Dept (DF) |
| HL-HTL · Hotels · Resorts | 008770 Hotel Shilla · 069640 Hansol Hotel |
| HL-CSN · Casinos · IR | 034230 Paradise · 035250 Kangwon Land |
| HL-RES · Restaurants · QSR | CJ Foodville (private) · BHC (private) |
| HL-CAF · Cafés · F&B chains | Starbucks Korea (private) · 247540 Ediya Coffee (delisted) |

---

### TR — Travel & OTA  · 여행·OTA  *(NEW in v0.2)*
- **GICS:** Consumer Discretionary · Hotels, Restaurants & Leisure  ·  **Sources:** card · footprint · text
- Outbound + inbound travel agencies + OTAs. Recovery-cycle, overlaps with HL-DTF on inbound.

| Subsector | Anchors |
|---|---|
| TR-AGY · Travel agencies | 039130 Hana Tour · 080160 Mode Tour · 032350 Lotte Tour |
| TR-OTA · Online Travel (OTA) | Yanolja (private) · Interpark (private) · 035720 Kakao (Travel) |
| TR-AIR · Airlines | 003490 Korean Air · 020560 Jin Air · 089590 Jeju Air |

---

### RT — Retail · Distribution  · 유통
- **GICS:** Consumer Staples · Distribution & Retail  ·  **Sources:** card · POS · footprint
- **v0.2 NOTE:** this whole sector becomes the Channel Axis in v0.3.

| Subsector | Anchors |
|---|---|
| RT-DEP · Department stores | 069960 Hyundai Dept · 004170 Shinsegae · 023530 Lotte Shopping |
| RT-CVS · Convenience stores | 282330 BGF Retail · 007070 GS Retail |
| RT-MRT · Hypermarket · Super | 139480 Emart · 023530 Lotte Shopping (Lotte Mart) |
| RT-HBR · H&B specialty | CJ Olive Young (private) · 161890 ABLE C&C |
| RT-FUR · Home furnishing | 009290 Hanssem |

---

### IT — Internet · Platform  · 인터넷·플랫폼
- **GICS:** Communication Services · Interactive Media & Services  ·  **Sources:** card · text · footprint

| Subsector | Anchors |
|---|---|
| IT-ECM · E-commerce platforms | 035420 NAVER (Smart Store) · CPNG Coupang (US-listed) |
| IT-SCH · Search · ad networks | 035420 NAVER · 035720 Kakao |
| IT-FIN · Fintech · payments | 377300 KakaoPay · 035720 Kakao (KakaoBank holding) |
| IT-DEL · Delivery · Quick-commerce | Coupang Eats · Baemin (private) |

---

### MC — Media · K-Content  · 미디어·콘텐츠
- **GICS:** Communication Services · Entertainment  ·  **Sources:** card · text

| Subsector | Anchors |
|---|---|
| MC-MUS · K-Pop labels | 352820 HYBE · 041510 SM · 035900 JYP · 122870 YG |
| MC-DRA · K-Drama · TV | 079160 CJ ENM · 067160 AfreecaTV |
| MC-FLM · Film · cinema | 079160 CJ ENM · 079160 CJ CGV |
| MC-PUB · Publishing · webtoon | 035420 NAVER (Webtoon) · 035720 Kakao (Picoma) |

#### MC-GAM · Gaming (L4 drill-down)
| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| MOB | MMO | Mobile MMO / RPG | 036570 NCsoft · 251270 Netmarble · 263750 Pearl Abyss | IP-DRIVEN · B2C |
| MOB | CAS | Mobile casual / puzzle | 293490 Kakao Games · 251270 Netmarble | B2C |
| PC | MMO | PC MMO / RPG | 036570 NCsoft (Lineage) · 263750 Pearl Abyss (Black Desert) | IP-DRIVEN · B2C |
| PC | BR | PC battle royale | 112040 Krafton (PUBG) | IP-DRIVEN · B2C |
| LIV | — | Live-service · console *(L3 agg)* | 112040 Krafton · 194480 NEXON | — |

---

### TC — Telecom Services  · 통신
- **GICS:** Communication Services · Telecommunication Services  ·  **Sources:** telecom · footprint

| Subsector | Anchors |
|---|---|
| TC-MOB · Mobile carriers | 030200 KT · 017670 SK Telecom · 032640 LG U+ |
| TC-FIX · Fixed line · IPTV | 030200 KT |

---

### SM — Semiconductor  · 반도체
- **GICS:** Information Technology · Semiconductors & Semi Equipment  ·  **Sources:** customs · text

#### SM-MEM · Memory (L4 drill-down)
| L3 | L4 | Leaf | Anchors | Tags |
|---|---|---|---|---|
| DRM | LEG | Legacy DRAM (DDR4) | 005930 Samsung · 000660 SK Hynix | CYCLICAL · EXPORT-MEMORY · B2B |
| DRM | ADV | Advanced DRAM (DDR5) | 005930 Samsung · 000660 SK Hynix | CYCLICAL · EXPORT-MEMORY · B2B · PREMIUM |
| NND | CST | Consumer NAND (mobile · SSD) | 005930 Samsung · 000660 SK Hynix | CYCLICAL · EXPORT-MEMORY · B2B |
| NND | ENT | Enterprise NAND (datacentre) | 005930 Samsung · 000660 SK Hynix | CYCLICAL · EXPORT-MEMORY · B2B · PREMIUM |
| HBM | 3E | HBM3 / HBM3E | 000660 SK Hynix · 005930 Samsung | CYCLICAL · EXPORT-MEMORY · B2B · PREMIUM |
| HBM | 4 | HBM4 (roadmap) | 000660 SK Hynix · 005930 Samsung | CYCLICAL · EXPORT-MEMORY · B2B · PREMIUM |

#### Other SM subsectors
| Subsector | Anchors |
|---|---|
| SM-LGC · Logic · foundry | 005930 Samsung (Foundry) · 000990 DB HiTek |
| SM-EQP · Semi equipment | 042700 Hanmi Semi · 240810 Wonik IPS · 108320 LX Semicon |
| SM-MAT · Semi materials | 036490 SK Materials · 278280 Cheil Industrial Tech |
| SM-PKG · Packaging · OSAT | 005935 Samsung Pref · 067160 Tessera |

---

### BT — Battery · EV materials  · 이차전지·소재
- **GICS:** Industrials / IT · Electrical Components  ·  **Sources:** customs

| Subsector | Anchors |
|---|---|
| BT-CEL · Cell makers | 373220 LG Energy Solution · 006400 Samsung SDI · 096770 SK Innovation |
| BT-CTH · Cathodes | 003670 Posco Future M · 247540 EcoPro BM · 086520 Ecopro |
| BT-ANO · Anodes · Graphite | 003670 Posco Future M |
| BT-SEP · Separators | 298050 SK ie Technology |
| BT-EQP · Battery equipment | 278280 Cheil Eng. · 108860 Cosmos Y |

---

### AM — Auto & Mobility  · 자동차·모빌리티
- **GICS:** Consumer Discretionary · Automobiles & Components  ·  **Sources:** customs · text

| Subsector | Anchors |
|---|---|
| AM-OEM · Auto OEM | 005380 Hyundai Motor · 000270 Kia |
| AM-SUP · Auto parts | 012330 Hyundai Mobis · 204320 HL Mando |
| AM-TIR · Tires | 161390 Hankook Tire & Tech |
| AM-EVC · EV components | 012450 Hanwha Aerospace · 267260 Hyundai Electric |

---

### HC — Health Care  · 헬스케어
- **GICS:** Health Care · Pharma · Biotech · Devices  ·  **Sources:** text

| Subsector | Anchors |
|---|---|
| HC-CDM · Bio · CDMO | 207940 Samsung Biologics · 302440 SK Bioscience |
| HC-BSM · Biosimilars | 068270 Celltrion |
| HC-PHM · Pharma (small mol) | 000100 Yuhan · 271940 Hyundai Bioland |
| HC-MED · Medical devices | 091990 Celltrion Healthcare · 266470 Inbody |

---

### IN — Industrials  · 산업재
- **GICS:** Industrials · Capital Goods · Transportation  ·  **Sources:** customs

| Subsector | Anchors |
|---|---|
| IN-SHP · Shipbuilding · marine | 009540 HD Korea Shipbuilding · 010140 Samsung Heavy · 042660 Hanwha Ocean |
| IN-CON · Construction · E&C | 028050 Samsung E&A · 000720 Hyundai E&C · 047040 Daewoo E&C |
| IN-MCH · Industrial machinery | 267250 HD Hyundai · 079550 LIG Nex1 |
| IN-DEF · Defence · aerospace | 012450 Hanwha Aerospace · 047810 Korea Aerospace · 272210 Hanwha Systems |
| IN-LOG · Logistics · transport | 011200 HMM |

---

### MT — Materials  · 소재
- **GICS:** Materials · Chemicals · Metals & Mining  ·  **Sources:** customs

| Subsector | Anchors |
|---|---|
| MT-CHM · Chemicals · petrochem | 051910 LG Chem · 011170 Lotte Chemical · 298020 HD Hyundai Chemical |
| MT-STL · Steel · metals | 005490 POSCO Holdings · 004020 Hyundai Steel |
| MT-PAP · Paper · packaging | (pure-play KR listings thin) |

---

### FN — Financials  · 금융
- **GICS:** Financials · Banks · Insurance · Securities

| Subsector | Anchors |
|---|---|
| FN-BNK · Banks · Financial holdings | 105560 KB Financial · 055550 Shinhan · 086790 Hana · 316140 Woori |
| FN-INS · Insurance | 032830 Samsung Life · 001450 Hyundai Marine & Fire |
| FN-SEC · Securities · Brokerage | 005940 NH Investment · 008560 Mirae Asset · 030610 Kyobo |

---

### EU — Energy · Utilities  · 에너지·유틸리티
- **GICS:** Energy · Utilities  ·  **Sources:** customs

| Subsector | Anchors |
|---|---|
| EU-REF · Refining · Oil & gas | 010950 S-Oil · 096770 SK Innovation · 078930 GS Holdings |
| EU-PWR · Power · Utilities | 015760 KEPCO · 036460 KOGAS |
| EU-RNW · Renewables · Solar/wind | 009830 Hanwha Solutions |

---

### ED — Education  · 교육  *(NEW in v0.2)*
- **GICS:** Consumer Discretionary · Diversified Consumer Services (Education)  ·  **Sources:** card · text

| Subsector | Anchors |
|---|---|
| ED-HAG · Hagwon · academic prep | 215200 Megastudy Education · 100120 Visang Education · 068930 Digital Daesung |
| ED-EDT · EdTech · online learning | 215200 Megastudy Education · 094800 Mathpresso |
| ED-PUB · Educational publishing | 100220 Visang Education · 036120 SCI Evaluation |
| ED-LNG · Language · adult learning | 096240 Chungdahm Learning |

---

### PT — Pets  · 반려동물  *(NEW in v0.2)*
- **GICS:** Consumer Staples · Household Products (Pet segment)  ·  **Sources:** card · POS
- KR pure-play listings thin; most pet exposure is embedded in F&B or specialty retailers.

| Subsector | Anchors |
|---|---|
| PT-FOD · Pet food | 049770 Dongwon F&B (Tema) · 017810 Pulmuone · 271940 Hyundai Bioland (pet div) |
| PT-CAR · Pet care · hygiene | 051900 LG H&H (pet line) |
| PT-VET · Veterinary · pet-tech | 263720 Daewoong Pharm (animal) · Pet-friends (private) |

---

### BK — Baby & Kids  · 유아·아동  *(NEW in v0.2)*
- **GICS:** Consumer Staples · Personal Products (Infant)  ·  **Sources:** card · POS

| Subsector | Anchors |
|---|---|
| BK-INF · Infant formula | 267980 Maeil Dairies (Absolute) · 003920 Namyang Dairy (Imperial XO) |
| BK-FSH · Kids fashion | Agabang & Co. (private) |
| BK-TOY · Toys · learning | Sonokong (private) |

---

### RN — Rental & Subscription  · 렌탈·구독  *(NEW in v0.2)*
- **GICS:** Consumer Discretionary · Diversified Consumer Services  ·  **Sources:** card

| Subsector | Anchors |
|---|---|
| RN-APL · Appliance rental | 021240 Coway · 079160 SK Magic · 017960 Cuckoo Holdings |
| RN-MAT · Mattress / wellness rental | 079430 Hyundai Livart · 021240 Coway (mattress) |
| RN-SUB · Subscription services | 021240 Coway (recurring) · 017960 Cuckoo Holdings |

---

## Tag glossary (23 tags · 8 groups)

| Group | Tags |
|---|---|
| **Business model** | B2B · B2C · B2G |
| **Demand cyclicality** | CYCLICAL · DEFENSIVE |
| **Pricing / mix** | BRANDED · PREMIUM · VALUE · PRIVATE-LABEL |
| **Supply chain** | COMMODITY-LINKED · COLD-CHAIN · FRESH-PERISHABLE |
| **Consumer theme** | HEALTH-WELLNESS · CONVENIENCE |
| **Channel / occasion** | ON-PREMISE · OFF-PREMISE · INBOUND-TOURISM |
| **Geo / export** | EXPORT-KFOOD · EXPORT-KBEAUTY · EXPORT-MEMORY · CHINA-DTC |
| **Regulation / IP** | REGULATED · IP-DRIVEN |

Full label per tag:

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

## v0.2 → v0.3 priorities (in order)

1. **Revenue-mix weights per ticker** — CJ Cheiljedang, LG H&H, KT&G span multiple L4s; need weighted vectors so signals blend correctly.
2. **Channel Axis (separate dimension)** — promote RT and HL-DTF to a first-class channel dimension; remove channel-ish nodes from MTC.
3. **Geography Axis** — KR / CN / US / JP / SEA / global as required axis.
4. **B2C / B2B / B2G required metadata** — currently a tag; v0.3 makes it required per leaf.
5. **Full GICS 4-column split** — Sector / Industry Group / Industry / Sub-Industry as discrete columns.

---

## Questions for GPT review

1. **L4 leaf coverage.** For the sectors that still stop at L2/L3 (HP, AP, HL, RT, IT, TC, AM, HC, IN, MT, FN, EU, ED, PT, BK, RN, TR) — which 2–3 should get L4 drill-down next? Where's the highest signal-to-effort ratio for institutional investors?
2. **Anchor ticker accuracy.** Spot-check the FB, CB, TB anchors. Any obvious mis-attributions? Any missing must-include names (e.g. is 248170 Samyang Foods correctly slotted under FB-PKG-NDL-*?).
3. **Tag system.** Are the 8 groups orthogonal enough? Any tags that should split or merge? Should EXPORT- prefix be unified into a single tag + a country code?
4. **New sectors (ED, PT, BK, TR, RN).** Coverage relevant? Anything obviously missing (e.g. Beauty Medical / Aesthetics — currently lives under CB-MED; should it be its own sector?)
5. **Channel Axis design (v0.3).** When we pull RT + HL-DTF out into a separate axis, should it sit alongside MTC (parallel) or under MTC (sub-axis per leaf)? What's the cleanest mental model for a quant team?
6. **Revenue-mix weights (v0.3).** Should weights be exposed in the API per (ticker, L4) pair, or per (ticker, ANY-level) so consumers can roll up themselves? Industry precedent?
