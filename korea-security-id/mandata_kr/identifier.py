"""Core identifier engine — multi-modal input → rich SecurityRecord output."""

from __future__ import annotations

import csv
import difflib
import json
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterable, Optional

DATA_DIR = Path(__file__).parent / "data"
SYNC_META = DATA_DIR / "sync_meta.json"


def sync_status() -> dict:
    """Return {last_synced_utc, source, row_count, ...} or empty dict if never synced."""
    if SYNC_META.exists():
        try:
            return json.loads(SYNC_META.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

# --------------------------------------------------------------------------- #
# Normalization (mirrors the krx-underlying-isin-mapper logic, kept in sync)
# --------------------------------------------------------------------------- #

_CAMEL_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

ABBREV_EXPANSIONS = {
    "elec": "electronics", "elecmech": "electromechanics", "mech": "mechanics",
    "mtr": "motor", "hldgs": "holdings", "hldg": "holdings",
    "fingrp": "financialgroup", "fin": "financial", "grp": "group",
    "tel": "telecom", "telecom": "telecom",
    "innov": "innovation", "bio": "biologics", "biosci": "bioscience",
    "sec": "securities", "aero": "aerospace", "sys": "systems",
    "intl": "international", "corp": "corporation", "inc": "incorporated",
    "co": "company", "ltd": "limited", "ent": "enterprise",
    "ind": "industries", "constr": "construction",
}
SUFFIX_TOKENS = {
    "co", "corp", "corporation", "inc", "incorporated", "ltd", "limited",
    "company", "plc", "kg", "ag", "sa", "nv", "se",
}


def _tokenize(raw: str) -> list[str]:
    s = str(raw or "").strip()
    s = re.sub(r"[,&/\-_.()]+", " ", s)
    s = _CAMEL_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    tokens = [t for t in s.split(" ") if t]
    while tokens and tokens[-1] in SUFFIX_TOKENS:
        tokens.pop()
    return tokens


def _alphanum(s: str) -> str:
    return re.sub(r"[^a-z0-9가-힣]+", "", s.lower())


def normalize_variants(raw: str) -> list[str]:
    tokens = _tokenize(raw)
    if not tokens:
        return []
    expanded = [ABBREV_EXPANSIONS.get(t, t) for t in tokens]
    variants = {
        _alphanum("".join(expanded)),
        _alphanum("".join(tokens)),
    }
    return [v for v in variants if v]


def token_set(raw: str) -> set[str]:
    tokens = _tokenize(raw)
    if not tokens:
        return set()
    out = set(tokens) | {ABBREV_EXPANSIONS.get(t, t) for t in tokens}
    return {t for t in out if t}


# --------------------------------------------------------------------------- #
# ISIN check digit (ISO 6166)
# --------------------------------------------------------------------------- #

def isin_check_digit(body: str) -> str:
    """Compute the ISIN check digit for `body` (11 alphanumerics, no check)."""
    digits: list[int] = []
    for ch in body:
        if ch.isdigit():
            digits.append(int(ch))
        else:
            v = 10 + (ord(ch.upper()) - ord('A'))
            digits.append(v // 10)
            digits.append(v % 10)
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return str((10 - (total % 10)) % 10)


def validate_isin(isin: str) -> bool:
    """True iff the 12-character string passes ISO 6166 check-digit math."""
    if not isin or len(isin) != 12:
        return False
    body, check = isin[:-1], isin[-1]
    if not check.isdigit():
        return False
    try:
        return isin_check_digit(body) == check
    except Exception:
        return False


def fix_isin(isin: str) -> Optional[str]:
    """Return the same ISIN with the correct check digit (or None if invalid prefix)."""
    if not isin or len(isin) != 12:
        return None
    body = isin[:-1]
    try:
        return body + isin_check_digit(body)
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #

@dataclass
class SecurityRecord:
    """A canonical Korean security identification record."""
    local_code: str = ""
    isin: str = ""
    name_kr: str = ""
    name_en: str = ""
    krx_short_en: str = ""
    market: str = ""                # KOSPI / KOSDAQ / KONEX / INDEX / etc.
    share_class: str = "COMMON"     # COMMON | PREFERRED_1 | PREFERRED_2B | INDEX | etc.
    sector_code_gics: str = ""
    sector_name_en: str = ""
    industry_name_kr: str = ""
    listing_date: str = ""
    dart_corp_code: str = ""
    kospi200: bool = False
    kosdaq150: bool = False
    krx300: bool = False
    related: list[dict] = field(default_factory=list)
    aliases: list[dict] = field(default_factory=list)  # ALL the ways this security can be searched
    bloomberg_ticker: str = ""
    ric: str = ""
    dart_url: str = ""
    # Matching telemetry
    matched_input: str = ""
    match_method: str = ""
    match_method_human: str = ""   # human-readable explanation for the UI
    confidence: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


# Human-readable explanation of each internal match method
MATCH_LABELS = {
    "bloomberg_ticker":       "Matched as Bloomberg ticker",
    "refinitiv_ric":          "Matched as Refinitiv RIC",
    "isin":                   "Matched as ISIN",
    "isin_fixed_checkdigit":  "Matched as ISIN (auto-corrected check digit)",
    "dart_corp_code":         "Matched as DART corporate code",
    "local_code":             "Matched as KRX 6-digit short code",
    "partial_local_code":     "Matched as partial KRX code (unique)",
    "korean_name_exact":      "Matched on Korean company name",
    "normalized_exact":       "Matched on normalized name / vendor alias / derivative underlying",
    "token_align":            "Matched on token alignment (multi-word company name)",
    "token_align_single":     "Matched on token alignment (single word — lower confidence)",
    "fuzzy":                  "Matched approximately (fuzzy, accepts typos)",
}


# --------------------------------------------------------------------------- #
# Identifier
# --------------------------------------------------------------------------- #

class Identifier:
    """Accepts almost any Korean security identifier and returns a SecurityRecord."""

    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = Path(data_dir) if data_dir else DATA_DIR
        self._load()

    # ----- data loading ---------------------------------------------------

    def _load(self):
        # Equity master (common stocks + indices etc.)
        self.equities: list[dict] = []
        master = self.data_dir / "equity_master.csv"
        if master.exists():
            with master.open(encoding="utf-8") as f:
                self.equities = list(csv.DictReader(f))
        # Non-equity reference (indices, commodities, rates, FX)
        self.non_equity: list[dict] = []
        ne = self.data_dir / "non_equity_underlyings.csv"
        if ne.exists():
            with ne.open(encoding="utf-8") as f:
                self.non_equity = list(csv.DictReader(f))
        # Preferred pairs (common ↔ pref)
        self.pref_pairs: list[dict] = []
        pp = self.data_dir / "preferred_pairs.csv"
        if pp.exists():
            with pp.open(encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if row.get("pref_local_code") and row.get("pref_isin"):
                        self.pref_pairs.append(row)

        # Build indices ---------------------------------------------------
        self._by_local: dict[str, dict] = {}
        self._by_isin: dict[str, dict] = {}
        self._by_dart: dict[str, dict] = {}
        self._by_kr: dict[str, dict] = {}        # normalized Korean name → row
        self._by_name: dict[str, dict] = {}      # normalized any-name → row
        self._all_keys: list[str] = []
        self._token_sets: list[tuple[dict, set]] = []

        for e in self.equities:
            self._index_equity(e)

        # Add preferred-share rows derived from pref_pairs as virtual equities
        for p in self.pref_pairs:
            virtual = {
                "local_code": p["pref_local_code"],
                "isin": p["pref_isin"],
                "company_name_en": p.get("pref_name_en") or (p.get("common_name_kr","") + " Pref"),
                "company_name_kr": p["pref_name_kr"],
                "krx_short_en": "",
                "market": "PREFERRED",
                "share_class": _pref_class(p.get("pref_class", "1우")),
                "sector_code_gics": "", "sector_name_en": "", "industry_name_kr": "",
                "listing_date": "", "dart_corp_code": "",
                "kospi200": "", "kosdaq150": "", "krx300": "",
                "_common_local": p["common_local_code"],
            }
            self._index_equity(virtual)

        # Non-equity (indices etc.) as virtual records
        for ne_row in self.non_equity:
            virtual = {
                "local_code": "",
                "isin": ne_row["reference_code"],
                "company_name_en": ne_row["name_en"],
                "company_name_kr": ne_row["name_kr"],
                "krx_short_en": ne_row["krx_short_en"],
                "market": ne_row["underlying_type"],
                "share_class": ne_row["underlying_type"].upper(),
                "sector_code_gics": "", "sector_name_en": "", "industry_name_kr": "",
                "listing_date": "", "dart_corp_code": "",
                "kospi200": "", "kosdaq150": "", "krx300": "",
            }
            self._index_equity(virtual)

    def _index_equity(self, e: dict):
        local = (e.get("local_code") or "").zfill(6) if e.get("local_code") else ""
        e["local_code"] = local
        if local:
            self._by_local[local] = e
        if e.get("isin"):
            self._by_isin[e["isin"]] = e
        if e.get("dart_corp_code"):
            self._by_dart[e["dart_corp_code"]] = e

        # Korean name index
        if e.get("company_name_kr"):
            kr = _alphanum(e["company_name_kr"])
            if kr:
                self._by_kr.setdefault(kr, e)

        # Multi-variant name index
        for src in (e.get("company_name_en"), e.get("company_name_kr"),
                    e.get("krx_short_en")):
            if not src:
                continue
            for k in normalize_variants(src):
                self._by_name.setdefault(k, e)
                if k not in self._all_keys:
                    self._all_keys.append(k)

        # Token set
        toks = set()
        for src in (e.get("company_name_en"), e.get("company_name_kr"),
                    e.get("krx_short_en")):
            if src:
                toks |= token_set(src)
        self._token_sets.append((e, toks))

    # ----- matching -------------------------------------------------------

    def lookup(self, query: str) -> Optional[SecurityRecord]:
        """Resolve any of the supported identifier types to a SecurityRecord."""
        if not query:
            return None
        raw = str(query).strip()

        # ---- direct identifier matches ----------------------------------
        # 1) Bloomberg ticker pattern: "005930 KS Equity"
        m = re.match(r"^\s*(\d{6})\s+(KS|KQ|KX)\s+(Equity|Index)?\s*$",
                     raw, re.IGNORECASE)
        if m:
            code = m.group(1).zfill(6)
            if code in self._by_local:
                return self._make(self._by_local[code], raw, "bloomberg_ticker", 1.0)

        # 2) Refinitiv RIC pattern: "005930.KS"
        m = re.match(r"^\s*(\d{6})\.(KS|KQ|KX|KN)\s*$", raw, re.IGNORECASE)
        if m:
            code = m.group(1).zfill(6)
            if code in self._by_local:
                return self._make(self._by_local[code], raw, "refinitiv_ric", 1.0)

        # 3) ISIN
        if len(raw) == 12 and raw.upper().startswith("KR"):
            up = raw.upper()
            if up in self._by_isin:
                return self._make(self._by_isin[up], raw, "isin", 1.0)
            # Try fixing the check digit; if base body matches anything, suggest it
            fixed = fix_isin(up)
            if fixed and fixed in self._by_isin:
                rec = self._make(self._by_isin[fixed], raw, "isin_fixed_checkdigit", 0.85)
                return rec

        # 4) DART corp code (8 digits, NOT 6)
        if raw.isdigit() and len(raw) == 8:
            if raw in self._by_dart:
                return self._make(self._by_dart[raw], raw, "dart_corp_code", 1.0)

        # 5) Local code (1-6 digit numeric)
        stripped = raw.replace(" ", "")
        if stripped.isdigit() and 1 <= len(stripped) <= 6:
            code = stripped.zfill(6)
            if code in self._by_local:
                return self._make(self._by_local[code], raw, "local_code", 1.0)
            # Partial code — find unique suffix match
            partials = [c for c in self._by_local if c.endswith(stripped)]
            if len(partials) == 1:
                return self._make(self._by_local[partials[0]], raw,
                                  "partial_local_code", 0.9)

        # ---- name-based matches ------------------------------------------
        # 6) Exact Korean name
        kr_key = _alphanum(raw)
        if kr_key and kr_key in self._by_kr:
            return self._make(self._by_kr[kr_key], raw, "korean_name_exact", 0.97)

        # 7) Normalized exact (any variant)
        for v in normalize_variants(raw):
            if v in self._by_name:
                return self._make(self._by_name[v], raw, "normalized_exact", 0.95)

        # 8) Token-set alignment
        in_tokens = token_set(raw)
        if in_tokens:
            best = None
            for e, toks in self._token_sets:
                if not toks:
                    continue
                if in_tokens.issubset(toks):
                    score = len(in_tokens & toks) / max(len(toks), 1)
                    if best is None or score > best[0]:
                        best = (score, e)
            if best is not None:
                score, e = best
                if len(in_tokens) >= 2:
                    return self._make(e, raw, "token_align",
                                      round(0.85 + 0.10 * score, 3))
                else:
                    return self._make(e, raw, "token_align_single",
                                      round(0.78 + 0.10 * score, 3))

        # 9) Fuzzy
        best_fuzzy = None
        for v in normalize_variants(raw):
            close = difflib.get_close_matches(v, self._all_keys, n=1, cutoff=0.80)
            if close:
                ratio = difflib.SequenceMatcher(None, v, close[0]).ratio()
                if best_fuzzy is None or ratio > best_fuzzy[0]:
                    best_fuzzy = (ratio, self._by_name[close[0]])
        if best_fuzzy is not None:
            ratio, e = best_fuzzy
            return self._make(e, raw, "fuzzy", round(ratio, 3))

        return None

    def search(self, query: str, *, limit: int = 10) -> list[SecurityRecord]:
        """Substring / prefix search across name & code fields. Returns up to `limit` hits."""
        if not query:
            return []
        q = _alphanum(query)
        hits = []
        seen = set()
        for e in self.equities:
            keys = [
                _alphanum(e.get("company_name_kr") or ""),
                _alphanum(e.get("company_name_en") or ""),
                _alphanum(e.get("krx_short_en") or ""),
                (e.get("local_code") or "").lower(),
            ]
            if any(q in k for k in keys if k):
                if e["local_code"] in seen:
                    continue
                seen.add(e["local_code"])
                hits.append(self._make(e, query, "search", 0.0))
                if len(hits) >= limit:
                    break
        return hits

    def members(self, index_name: str) -> list[SecurityRecord]:
        """Return all securities in an index (KOSPI200 / KOSDAQ150 / KRX300)."""
        col = {"KOSPI200": "kospi200", "KOSDAQ150": "kosdaq150",
               "KRX300": "krx300"}.get(index_name.upper().replace(" ", ""))
        if not col:
            return []
        return [self._make(e, index_name, "index_member", 1.0)
                for e in self.equities if e.get(col) == "Y"]

    def related(self, sec: SecurityRecord) -> list[dict]:
        """Find related instruments (common↔pref pair)."""
        out: list[dict] = []
        # Common → pref(s)
        for p in self.pref_pairs:
            if p.get("common_local_code") == sec.local_code and p.get("pref_local_code"):
                out.append({
                    "relation": "preferred_of",
                    "local_code": p["pref_local_code"],
                    "isin": p["pref_isin"],
                    "name_kr": p["pref_name_kr"],
                    "pref_class": p.get("pref_class", ""),
                })
            if p.get("pref_local_code") == sec.local_code:
                out.append({
                    "relation": "common_of",
                    "local_code": p["common_local_code"],
                    "isin": p["common_isin"],
                    "name_kr": p["common_name_kr"],
                    "pref_class": "COMMON",
                })
        return out

    # ----- internal -------------------------------------------------------

    def _make(self, e: dict, raw: str, method: str, confidence: float) -> SecurityRecord:
        local = e.get("local_code") or ""
        rec = SecurityRecord(
            local_code = local,
            isin = e.get("isin") or "",
            name_kr = e.get("company_name_kr") or "",
            name_en = e.get("company_name_en") or "",
            krx_short_en = e.get("krx_short_en") or "",
            market = e.get("market") or "",
            share_class = e.get("share_class") or "COMMON",
            sector_code_gics = e.get("sector_code_gics") or "",
            sector_name_en = e.get("sector_name_en") or "",
            industry_name_kr = e.get("industry_name_kr") or "",
            listing_date = e.get("listing_date") or "",
            dart_corp_code = e.get("dart_corp_code") or "",
            kospi200 = (e.get("kospi200") == "Y"),
            kosdaq150 = (e.get("kosdaq150") == "Y"),
            krx300 = (e.get("krx300") == "Y"),
            matched_input = raw,
            match_method = method,
            match_method_human = MATCH_LABELS.get(method, method),
            confidence = confidence,
        )
        # Derived tickers (Bloomberg / RIC)
        if local and len(local) == 6 and local.isdigit():
            if rec.market == "KOSPI":
                rec.bloomberg_ticker = f"{local} KS Equity"
                rec.ric = f"{local}.KS"
            elif rec.market == "KOSDAQ":
                rec.bloomberg_ticker = f"{local} KQ Equity"
                rec.ric = f"{local}.KQ"
            elif rec.market == "KONEX":
                rec.bloomberg_ticker = f"{local} KX Equity"
                rec.ric = f"{local}.KN"
            elif rec.market == "PREFERRED":
                rec.bloomberg_ticker = f"{local} KS Equity"
                rec.ric = f"{local}.KS"
        if rec.dart_corp_code:
            rec.dart_url = (
                "https://dart.fss.or.kr/dsab007/main.do?autoSearch=true&"
                f"corpCode={rec.dart_corp_code}"
            )
        rec.related = self.related(rec)
        rec.aliases = self._aliases(rec, e)
        return rec

    def _aliases(self, rec: "SecurityRecord", e: dict) -> list[dict]:
        """All the input strings that should resolve to this security.

        Returned as a list of {kind, value, note} dicts — the UI shows this
        so the user understands "I can search this security as any of these"
        and explicitly sees the derivative-underlying short name (which is
        what trips most people up).
        """
        out: list[dict] = []
        if rec.name_kr:
            out.append({"kind": "Korean name",        "value": rec.name_kr,
                        "note": ""})
        if rec.name_en:
            out.append({"kind": "English name",       "value": rec.name_en,
                        "note": ""})
        if rec.local_code:
            out.append({"kind": "KRX local code",     "value": rec.local_code,
                        "note": "6-digit KRX short code"})
        if rec.isin:
            out.append({"kind": "ISIN",               "value": rec.isin,
                        "note": "ISO 6166"})
        if rec.bloomberg_ticker:
            out.append({"kind": "Bloomberg ticker",   "value": rec.bloomberg_ticker,
                        "note": ""})
        if rec.ric:
            out.append({"kind": "Refinitiv RIC",      "value": rec.ric,
                        "note": ""})
        if rec.dart_corp_code:
            out.append({"kind": "DART corp code",     "value": rec.dart_corp_code,
                        "note": "Korean regulatory disclosure system (DART) corporate code"})
        if rec.krx_short_en:
            out.append({"kind": "Derivative underlying name",
                        "value": rec.krx_short_en,
                        "note": "How this equity is referenced as an underlying in the KRX derivative market"})
            # Also expose lowercased vendor-style alias if different
            lower = rec.krx_short_en.lower()
            if lower != rec.krx_short_en:
                out.append({"kind": "Vendor alias (lowercased)",
                            "value": lower,
                            "note": "Common lowercased form used by vendor / market-data feeds"})
        return out


def _pref_class(token: str) -> str:
    t = token.strip().upper()
    if t in ("1우", "1U", "PREFERRED_1"): return "PREFERRED_1"
    if t in ("2우B", "2UB"):             return "PREFERRED_2B"
    if t in ("3우B", "3UB"):             return "PREFERRED_3B"
    return f"PREFERRED_{t}"


# --------------------------------------------------------------------------- #
# Module-level singleton + helpers
# --------------------------------------------------------------------------- #

_default: Optional[Identifier] = None

def _get() -> Identifier:
    global _default
    if _default is None:
        _default = Identifier()
    return _default

def lookup(query: str) -> Optional[SecurityRecord]:
    """Module-level convenience: `mandata_kr.lookup("삼성전자")`."""
    return _get().lookup(query)

def search(query: str, *, limit: int = 10) -> list[SecurityRecord]:
    return _get().search(query, limit=limit)

def members(index_name: str) -> list[SecurityRecord]:
    return _get().members(index_name)

def related(sec: SecurityRecord) -> list[dict]:
    return _get().related(sec)
