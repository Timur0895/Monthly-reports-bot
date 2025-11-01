import re
from datetime import datetime
from dateutil import tz

def extract_spreadsheet_id_from_url(url: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if not m:
        raise ValueError(f"Bad Google Sheet URL: {url}")
    return m.group(1)

def normalize(s: str) -> str:
    return (s or "").strip().lower()

def parse_period_ddmm_dash_ddmm(s: str, year_hint: int | None = None) -> tuple[str, str]:
    """
    '06.07–06.08' -> ('YYYY-07-06', 'YYYY-08-06')
    Если период пересекает год — при необходимости расширим логику.
    """
    s = s.replace(" ", "")
    parts = re.split(r"[–\-—]+", s)
    if len(parts) != 2:
        raise ValueError("Ожидался формат периода DD.MM–DD.MM")

    d1, m1 = parts[0].split(".")
    d2, m2 = parts[1].split(".")
    today = datetime.now(tz=tz.gettz("Asia/Almaty"))
    year = year_hint or today.year
    since = f"{year}-{int(m1):02d}-{int(d1):02d}"
    until = f"{year}-{int(m2):02d}-{int(d2):02d}"
    # При необходимости: если until < since, прибавляем год
    if until < since:
        until = f"{year+1}-{int(m2):02d}-{int(d2):02d}"
    return since, until
