"""Allow `python -m mandata_kr ...` as a shortcut for `python -m mandata_kr.cli ...`."""
from .cli import main
import sys
raise SystemExit(main(sys.argv[1:]))
