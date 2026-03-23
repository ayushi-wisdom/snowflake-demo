"""
Project config: data-as-of date for demo (no data generated after this day).
Set DATA_AS_OF_DATE=YYYY-MM-DD in .env to cap all generated data at that date (e.g. 2026-03-10).
If unset, scripts use date.today() so normal daily runs work unchanged.
"""
import os
from datetime import date
from pathlib import Path

_env_file = Path(__file__).resolve().parent / ".env"


def get_data_as_of_date() -> date:
    """
    Return the "current day" for data generation. No data is generated after this date.
    - If DATA_AS_OF_DATE is set in .env (YYYY-MM-DD), use that.
    - Otherwise use date.today().
    """
    try:
        if _env_file.exists():
            for line in _env_file.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    if k.strip() == "DATA_AS_OF_DATE" and v.strip():
                        return date.fromisoformat(v.strip())
    except Exception:
        pass
    return date.today()
