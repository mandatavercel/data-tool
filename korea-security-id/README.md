# Mandata Korea Security ID

*Investment mandated to data.*

A multi-modal Korean security identifier with rich enrichment — wrap any kind
of Korean security identifier (Korean name, English alias, local code, ISIN,
Bloomberg ticker, Refinitiv RIC, DART corp code, partial code, vendor short
name) and get back a complete record with **sector, listing date, index
membership, preferred-share counterpart, DART filing URL** and more.

Three ways to use it:

1. **Web app** — open `web/index.html` in any browser (no install)
2. **CLI** — `python -m mandata_kr lookup "삼성전자"` (no install)
3. **Python library** — `from mandata_kr import lookup`

All three share the same engine and the same bundled dataset (217 records:
182 KOSPI/KOSDAQ equities + 21 preferred shares + 14 indices/commodities/
rates/FX).

---

## What's new vs. v1 (krx-underlying-isin-mapper)

| Capability | v1 | v2 (this) |
|---|---|---|
| Accept Korean company name (`삼성전자`) | ✗ | ✓ |
| Accept Bloomberg ticker (`005930 KS Equity`) | ✗ | ✓ |
| Accept Refinitiv RIC (`005930.KS`) | ✗ | ✓ |
| Accept DART corp code (8-digit) | ✗ | ✓ |
| Accept partial local code (`5930`) | ✗ | ✓ |
| Resolve preferred shares (`005935 → 삼성전자우`) | ✗ | ✓ |
| Reverse: common ↔ preferred pair | ✗ | ✓ |
| Return sector + industry | ✗ | ✓ |
| Return listing date | ✗ | ✓ |
| Return KOSPI 200 / KOSDAQ 150 / KRX 300 flags | ✗ | ✓ |
| Generate DART filing URL | ✗ | ✓ |
| List index members (KOSPI200/KOSDAQ150/KRX300) | ✗ | ✓ |
| Validate / fix ISIN check digit | – | ✓ |
| CLI tool | ✗ | ✓ |
| Python package importable | ✗ | ✓ |

---

## 1. Web app

Just open `web/index.html`. Four tabs:

* **Lookup** — type any identifier; result with full enrichment
* **Search** — Korean/English substring across 200+ securities
* **Index members** — full KOSPI 200 / KOSDAQ 150 / KRX 300 with sector filter
* **ISIN validate** — paste any ISIN; check-digit verified, corrupted ones auto-corrected

No install. No data leaves your browser.

## 2. CLI

```bash
cd korea-security-id/
python -m mandata_kr lookup "삼성전자"
python -m mandata_kr lookup "samsungelec"
python -m mandata_kr lookup "005935"               # preferred share
python -m mandata_kr lookup "005930 KS Equity"     # Bloomberg ticker
python -m mandata_kr lookup "00126380"             # DART corp code
python -m mandata_kr search "한미"                  # substring across master
python -m mandata_kr members KOSPI200
python -m mandata_kr validate KR7005930003
python -m mandata_kr bulk my_list.csv -o resolved.csv --column query
```

Add `--json` to any subcommand for machine-readable output.

**Mac shortcut**: double-click `run_mac.command` for an interactive REPL.
**Windows**: double-click `run_windows.bat`.

## 3. Python library

```python
from mandata_kr import lookup, search, members, validate_isin, fix_isin

rec = lookup("삼성전자")
print(rec.isin)              # KR7005930003
print(rec.bloomberg_ticker)  # "005930 KS Equity"
print(rec.kospi200)          # True
print(rec.dart_url)          # https://dart.fss.or.kr/...
print(rec.related)           # [{'relation': 'preferred_of', 'local_code': '005935', ...}]

for r in members("KOSPI200"):
    print(r.local_code, r.name_kr, r.sector_name_en)

validate_isin("KR7005930003")  # True
fix_isin("KR7005930009")        # "KR7005930003"  (corrects the check digit)
```

No `pip install` required — drop the `korea-security-id/` folder anywhere
and `python -m mandata_kr` works.

For pip-installable distribution: `pip install -e .` from this folder
(requires you to add a minimal `pyproject.toml`; not yet bundled).

---

## Folder layout

```
korea-security-id/
├── README.md
├── run_mac.command                  Double-click on macOS for interactive REPL
├── run_windows.bat                  Same on Windows
├── mandata_kr/                      Python package
│   ├── __init__.py                  Public API
│   ├── __main__.py                  Enables `python -m mandata_kr ...`
│   ├── identifier.py                Core engine (10 match tiers)
│   ├── cli.py                       Subcommand CLI
│   └── data/
│       ├── equity_master.csv        182 securities w/ sector, listing, DART, index flags
│       ├── preferred_pairs.csv      21 보통주↔우선주 pairs w/ both ISINs
│       └── non_equity_underlyings.csv  14 indices / commodities / rates / FX
└── web/
    └── index.html                   Self-contained web app (100KB, fully offline)
```

---

## Matching tiers (in order)

The engine tries these strategies in order; the first one to hit wins.

| Tier | Method | Example input | Confidence |
|---|---|---|---|
| 1 | Bloomberg ticker | `005930 KS Equity` | 1.00 |
| 2 | Refinitiv RIC | `005930.KS` | 1.00 |
| 3 | Exact ISIN | `KR7005930003` | 1.00 |
| 3b | ISIN with corrupted check digit | `KR7005930009` | 0.85 |
| 4 | DART corp code | `00126380` | 1.00 |
| 5 | Exact local code | `005930` | 1.00 |
| 5b | Partial local code (unique suffix) | `5930` | 0.90 |
| 6 | Korean name exact | `삼성전자` | 0.97 |
| 7 | Normalized name exact | `samsungelec`, `Samsung Electronics` | 0.95 |
| 8 | Token-set alignment | `HYOSUNG HEAVY`, `Hanwha Vision` | 0.85–0.95 |
| 9 | Fuzzy (Levenshtein ≥0.80) | `Hyndai Motor` (typo) | ratio |

## Data caveats

* The bundled `equity_master.csv` is a curated MVP set covering KOSPI 200 +
  major KOSDAQ underlyings (182 rows). It is **not** the full ~2,500 KRX
  universe — for production use, sync from KRX live via the
  `krx_underlying_isin_mapper.py --mode full` script in the sister tool.
* DART corp codes are filled in for ~40 marquee names (Samsung, SK hynix,
  POSCO, etc.). Others are blank pending a one-time DART API sync.
* Preferred-share ISINs (`KR7005931001` for 삼성전자우 etc.) follow KRX
  encoding rules that are *not* simply `local_code + "00"`. The 21 pairs in
  `preferred_pairs.csv` are hand-curated and verified.
* KOSPI 200 / KOSDAQ 150 / KRX 300 flags are best-effort and should be
  refreshed semi-annually after KRX rebalances.

## Roadmap (Phase 2 of v2)

* Old / renamed-company history (`아프리카TV → SOOP`)
* Group / parent search (`Samsung Group → all listings`)
* ETF / ETN / ELW first-class support
* Full DART code coverage (one-time DART API sync)
* REST API server (`GET /lookup?q=...`)
* Excel add-in (`=MANDATA_ISIN("삼성전자")`)
* Chrome extension (highlight Korean name → show ISIN)
