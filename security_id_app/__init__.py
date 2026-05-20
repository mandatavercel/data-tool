"""security_id_app — Streamlit sub-app for Mandata Korea Security ID.

The page UI lives in `app.py`. It is loaded by the unified launcher via
`auth.run_legacy_app("security_id_app", "app.py")` from
`pages/security_id.py`, or run standalone with:

    streamlit run security_id_app/app.py --server.port 8510

The matching engine itself is the sibling Python package
`korea-security-id/mandata_kr/`.
"""

__version__ = "0.1.0"
