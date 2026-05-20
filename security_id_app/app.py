"""
종목 식별 — Korea Security ID (Streamlit sub-app)
==================================================
한국 주식 식별자(이름/약어/로컬코드/ISIN/Bloomberg/RIC/DART 코드) 매칭 +
파생 underlying 변환. 매칭 엔진은 sibling 폴더의 `korea-security-id/mandata_kr/`.

진입 방식:
  - 통합 런처 (8500): pages/security_id.py 가 auth.run_legacy_app("security_id_app", "app.py") 호출
  - 단독 실행:        streamlit run security_id_app/app.py --server.port 8510
"""
from __future__ import annotations

import io
import json
import sys
from dataclasses import asdict
from pathlib import Path

import streamlit as st


# ── 1. make `mandata_kr` importable ──────────────────────────────────
#    sibling 폴더 `korea-security-id/` 에서 mandata_kr 패키지를 import.
_HERE = Path(__file__).resolve().parent                    # …/security_id_app
_DATA_TOOL = _HERE.parent                                  # …/data-tool
_MANDATA_KR_PARENT = _DATA_TOOL / "korea-security-id"

if str(_MANDATA_KR_PARENT) not in sys.path:
    sys.path.insert(0, str(_MANDATA_KR_PARENT))

try:
    from mandata_kr import (
        Identifier,
        validate_isin,
        fix_isin,
        sync_status,
        __version__ as MANDATA_VERSION,
    )
    _IMPORT_ERROR = None
except ImportError as e:  # pragma: no cover — surfaced in UI
    Identifier = None  # type: ignore
    MANDATA_VERSION = "?"
    _IMPORT_ERROR = e


# ── 2. cached Identifier singleton — CSVs load on first call ─────────
@st.cache_resource(show_spinner="Loading equity master & alias index…")
def _idr():
    return Identifier()


# ── 3. small UI helpers ──────────────────────────────────────────────
def _flag_chip(label: str, on: bool) -> str:
    color = "#0EA5E9" if on else "#E5E7EB"
    text = "#FFFFFF" if on else "#9CA3AF"
    return (
        f'<span style="background:{color};color:{text};padding:2px 8px;'
        f'border-radius:999px;font-size:11px;margin-right:6px;">{label}</span>'
    )


def _record_panel(rec) -> None:
    col_head_l, col_head_r = st.columns([3, 2])
    with col_head_l:
        st.markdown(
            f"### {rec.name_kr or '(no Korean name)'}  "
            f"<span style='color:#6B7280;font-weight:400;font-size:18px;'>"
            f"{rec.name_en or ''}</span>",
            unsafe_allow_html=True,
        )
    with col_head_r:
        chips = (
            _flag_chip("KOSPI 200", bool(rec.kospi200))
            + _flag_chip("KOSDAQ 150", bool(rec.kosdaq150))
            + _flag_chip("KRX 300", bool(rec.krx300))
        )
        st.markdown(chips, unsafe_allow_html=True)

    left, right = st.columns(2)
    with left:
        st.markdown(
            f"**ISIN** &nbsp; `{rec.isin}`  \n"
            f"**Local code** &nbsp; `{rec.local_code}`  \n"
            f"**Bloomberg** &nbsp; `{rec.bloomberg_ticker or '—'}`  \n"
            f"**RIC** &nbsp; `{rec.ric or '—'}`",
            unsafe_allow_html=True,
        )
    with right:
        sector = (
            f"{rec.sector_code_gics} · {rec.sector_name_en}"
            if rec.sector_code_gics else "—"
        )
        st.markdown(
            f"**Market** &nbsp; {rec.market or '—'}  \n"
            f"**Share class** &nbsp; {rec.share_class or '—'}  \n"
            f"**Sector (GICS)** &nbsp; {sector}  \n"
            f"**Listing date** &nbsp; {rec.listing_date or '—'}",
            unsafe_allow_html=True,
        )

    if rec.dart_corp_code:
        st.caption(
            f"DART corp code: `{rec.dart_corp_code}`"
            + (f" · [DART filings]({rec.dart_url})" if rec.dart_url else "")
        )

    st.caption(
        f"Match: **{rec.match_method_human or rec.match_method}** "
        f"(confidence {rec.confidence:.2f})"
    )

    if rec.aliases:
        with st.expander(f"Also searchable as ({len(rec.aliases)})"):
            for a in rec.aliases:
                note = f" — _{a['note']}_" if a.get("note") else ""
                st.markdown(f"- **{a['kind']}** &nbsp; `{a['value']}`{note}")

    if rec.related:
        with st.expander(f"Related instruments ({len(rec.related)})"):
            for r in rec.related:
                pref = f" ({r.get('pref_class')})" if r.get("pref_class") else ""
                st.markdown(
                    f"- **{r['relation']}** &nbsp; `{r['local_code']}` "
                    f"&nbsp; {r.get('name_kr', '')}{pref}"
                )

    with st.expander("Raw JSON record"):
        st.code(json.dumps(asdict(rec), ensure_ascii=False, indent=2), language="json")


# ── 4. main render ───────────────────────────────────────────────────
def render() -> None:
    """Paint the entire 종목 식별 page."""
    # set_page_config is monkey-patched to no-op by streamlit_app.py
    # when invoked via the launcher; safe to call for standalone use.
    try:
        st.set_page_config(page_title="Mandata · 종목 식별", page_icon="🔎", layout="wide")
    except Exception:
        pass

    if _IMPORT_ERROR is not None:
        st.error("`mandata_kr` package not importable")
        st.caption(
            f"Tried to import from: `{_MANDATA_KR_PARENT}/mandata_kr/`\n\n"
            f"Error: `{_IMPORT_ERROR}`\n\n"
            "Check that `~/Desktop/data-tool/korea-security-id/mandata_kr/` exists."
        )
        return

    idr = _idr()

    # header + dataset status
    meta = sync_status()
    total = len(idr.equities) + len(idr.non_equity) + len(idr.pref_pairs)

    h_left, h_right = st.columns([4, 1])
    with h_left:
        st.title("🔎 종목 식별 · Korea Security ID")
        st.caption(
            "한국어/영문/약어/로컬코드/ISIN/Bloomberg/RIC/DART 코드 — "
            "어떤 표기로 들어와도 단일 종목 레코드로 매칭. "
            "파생 underlying name까지 처리."
        )
    with h_right:
        st.metric("Records", f"{total:,}")
        if meta:
            st.caption(f"Last KRX sync: {meta.get('last_synced_utc', '?')[:10]}")
        else:
            st.caption(":orange[Demo set] — run KRX sync for full universe")

    if not meta:
        st.warning(
            "번들된 hand-curated 데모셋 사용 중 (KRX 전체 ~2,500 종목 중 일부). "
            "풀 커버리지로 키우려면 터미널에서: "
            "`cd ~/Desktop/data-tool/korea-security-id && python3 -m mandata_kr.sync`",
            icon="⚠️",
        )

    tab_lookup, tab_search, tab_members, tab_validate, tab_bulk = st.tabs(
        ["Lookup", "Search by name", "Index members", "Validate ISIN", "Bulk CSV"]
    )

    # ── tab 1 — single lookup ───────────────────────────────────────
    with tab_lookup:
        with st.form("lookup_form", clear_on_submit=False):
            q = st.text_input(
                "Identifier",
                value=st.session_state.get("sid_last_q", ""),
                placeholder="삼성전자  /  005930  /  KR7005930003  /  "
                            "005930 KS Equity  /  samsungelec  /  00126380",
                help="Korean name · English name · KRX abbreviation · "
                     "local code · ISIN · Bloomberg ticker · RIC · DART corp code",
            )
            submitted = st.form_submit_button("Look up", type="primary")
        if submitted and q.strip():
            st.session_state["sid_last_q"] = q.strip()
            rec = idr.lookup(q.strip())
            if rec is None:
                st.warning(f"No match for `{q.strip()}`.")
            else:
                _record_panel(rec)

    # ── tab 2 — substring search ────────────────────────────────────
    with tab_search:
        c1, c2 = st.columns([4, 1])
        with c1:
            sq = st.text_input(
                "Substring (Korean or English)",
                placeholder="한미 · hyundai · 바이오",
                key="sid_search_q",
            )
        with c2:
            limit = st.number_input(
                "Max hits", min_value=1, max_value=50, value=10, step=1,
                key="sid_limit",
            )
        if sq.strip():
            hits = idr.search(sq.strip(), limit=int(limit))
            if not hits:
                st.info("No matches.")
            else:
                st.write(f"**{len(hits)}** match(es)")
                rows = []
                for r in hits:
                    rows.append({
                        "Local code":  r.local_code,
                        "ISIN":        r.isin,
                        "한글명":      r.name_kr,
                        "English":     r.name_en,
                        "Market":      r.market,
                        "Share class": r.share_class,
                        "KOSPI 200":   "✓" if r.kospi200 else "",
                        "KOSDAQ 150":  "✓" if r.kosdaq150 else "",
                    })
                st.dataframe(rows, use_container_width=True, hide_index=True)

    # ── tab 3 — index members ───────────────────────────────────────
    with tab_members:
        idx = st.selectbox("Index", ["KOSPI200", "KOSDAQ150", "KRX300"], key="sid_idx")
        if idx:
            rows = idr.members(idx)
            if not rows:
                st.info(f"No members loaded for {idx}.")
            else:
                st.write(f"**{idx}** — {len(rows)} member(s)")
                st.dataframe(
                    [{
                        "Local code": r.local_code,
                        "ISIN":       r.isin,
                        "한글명":     r.name_kr,
                        "English":    r.name_en,
                        "Market":     r.market,
                    } for r in rows],
                    use_container_width=True, hide_index=True,
                )

    # ── tab 4 — validate / fix ISIN ─────────────────────────────────
    with tab_validate:
        isin_in = st.text_input(
            "ISIN to validate", placeholder="KR7005930003", key="sid_isin_in"
        )
        if isin_in.strip():
            isin = isin_in.strip().upper()
            ok = validate_isin(isin)
            if ok:
                st.success(f"✓  `{isin}`  is a valid ISIN (check digit passes)")
            else:
                st.error(f"✗  `{isin}`  fails ISIN check digit")
                fixed = fix_isin(isin)
                if fixed:
                    st.info(f"Suggested correction: `{fixed}`")

    # ── tab 5 — bulk CSV ────────────────────────────────────────────
    with tab_bulk:
        st.caption(
            "Upload a CSV with one query per row. Pick the column that "
            "holds the identifier; the rest is preserved in the output."
        )
        up = st.file_uploader("CSV file", type=["csv"], key="sid_bulk_upload")
        if up is not None:
            import csv
            text = up.getvalue().decode("utf-8-sig", errors="replace")
            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)
            if not rows:
                st.warning("CSV has no rows.")
            else:
                cols = list(rows[0].keys())
                qcol = st.selectbox("Query column", cols, index=0, key="sid_qcol")
                if st.button("Resolve all", type="primary", key="sid_resolve_btn"):
                    out_rows = []
                    matched = 0
                    prog = st.progress(0.0, text="Matching…")
                    for i, r in enumerate(rows):
                        res = idr.lookup(r.get(qcol, ""))
                        merged = dict(r)
                        if res:
                            matched += 1
                            merged.update({
                                "matched_isin":        res.isin,
                                "matched_local_code":  res.local_code,
                                "matched_name_kr":     res.name_kr,
                                "matched_name_en":     res.name_en,
                                "matched_market":      res.market,
                                "matched_share_class": res.share_class,
                                "matched_method":      res.match_method,
                                "matched_confidence":  f"{res.confidence:.3f}",
                            })
                        else:
                            for k in ("matched_isin", "matched_local_code",
                                      "matched_name_kr", "matched_name_en",
                                      "matched_market", "matched_share_class",
                                      "matched_method", "matched_confidence"):
                                merged[k] = ""
                        out_rows.append(merged)
                        prog.progress((i + 1) / len(rows))
                    prog.empty()
                    st.success(f"{matched}/{len(rows)} rows resolved.")
                    st.dataframe(out_rows, use_container_width=True, hide_index=True)

                    buf = io.StringIO()
                    w = csv.DictWriter(buf, fieldnames=list(out_rows[0].keys()))
                    w.writeheader()
                    w.writerows(out_rows)
                    st.download_button(
                        "Download resolved CSV",
                        data=buf.getvalue().encode("utf-8-sig"),
                        file_name=Path(up.name).stem + "_mandata_resolved.csv",
                        mime="text/csv",
                    )

    st.divider()
    st.caption(
        f"mandata_kr v{MANDATA_VERSION} · "
        f"{len(idr.equities)} equities · "
        f"{len(idr.pref_pairs)} preferred pairs · "
        f"{len(idr.non_equity)} non-equity underlyings"
    )


# ── 5. auto-run when loaded by runpy / streamlit ────────────────────
# `auth.run_legacy_app(...)` runs this file with runpy → __name__ == "__main__".
# `streamlit run security_id_app/app.py` also sets __name__ == "__main__".
# Either way, render the page on import.
render()
