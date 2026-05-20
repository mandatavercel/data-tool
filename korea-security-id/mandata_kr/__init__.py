"""mandata_kr — Korean security identifier & enrichment.

Public API:

    >>> from mandata_kr import lookup, search, members, validate_isin
    >>> lookup("samsungelec")
    SecurityRecord(local_code='005930', isin='KR7005930003', name_kr='삼성전자', ...)
    >>> lookup("삼성전자")
    SecurityRecord(...)
    >>> lookup("005935")
    SecurityRecord(local_code='005935', isin='KR7005931001', name_kr='삼성전자우', share_class='PREFERRED_1', ...)
    >>> lookup("005930 KS Equity")           # Bloomberg ticker
    >>> lookup("005930.KS")                  # Refinitiv RIC
    >>> lookup("KR7042700005")               # ISIN
    >>> lookup("00126380")                   # DART corp code
    >>> validate_isin("KR7005930003")
    True
    >>> members("KOSPI200")[:5]              # index constituents
    [...]

The package is designed to work without any extra install: drop the
folder anywhere and run `python -m mandata_kr.cli ...` from the parent
directory.
"""

from .identifier import (
    SecurityRecord,
    Identifier,
    lookup,
    search,
    members,
    related,
    validate_isin,
    fix_isin,
    sync_status,
)

__version__ = "0.3.0"
__all__ = [
    "SecurityRecord", "Identifier",
    "lookup", "search", "members", "related",
    "validate_isin", "fix_isin",
    "sync_status",
    "__version__",
]
