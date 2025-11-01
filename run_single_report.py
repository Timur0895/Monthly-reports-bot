# run_single_report.py
from __future__ import annotations
import sys
import re
import datetime as dt
from calendar import monthrange

import gspread
from sheets.gs_client import get_gs_client
from catalog.master_index import find_client_by_name
from report_service import generate_report

# ──────────────────────────────────────────────────────────────────────────────
# ВВЕДИ ЗДЕСЬ КЛИЕНТА И ПЕРИОД ДЛЯ ПРОГОНА
AD_NAME = "Bakery Aigul"     # ← имя как в колонке B (ad_name)
PERIOD  = "01.10–20.10"      # варианты: "01.10–20.10", "2025-10-01..2025-10-20", "октябрь 2025", "последние 30 дней"
# ──────────────────────────────────────────────────────────────────────────────

# Мини-парсер периода
RU_MONTHS = {"январ":1,"феврал":2,"март":3,"апрел":4,"ма":5,"июн":6,"июл":7,"август":8,"сентябр":9,"октябр":10,"ноябр":11,"декабр":12}
def parse_period(text: str) -> tuple[str,str]:
    s = (text or "").strip().lower()

    m = re.match(r"последние\s+(\d{1,3})\s+дн", s)
    if m:
        n = int(m.group(1))
        end = dt.date.today()
        start = end - dt.timedelta(days=n-1)
        return start.isoformat(), end.isoformat()

    m = re.match(r"(\d{1,2})[.\-/](\d{1,2})\s*[–\-]\s*(\d{1,2})[.\-/](\d{1,2})", s)
    if m:
        y = dt.date.today().year
        d1, mo1, d2, mo2 = map(int, m.groups())
        a = dt.date(y, mo1, d1); b = dt.date(y, mo2, d2)
        if a>b: a,b = b,a
        return a.isoformat(), b.isoformat()

    m = re.match(r"(\d{4}-\d{2}-\d{2})\s*\.\.\s*(\d{4}-\d{2}-\d{2})", s)
    if m:
        a = dt.date.fromisoformat(m.group(1)); b = dt.date.fromisoformat(m.group(2))
        if a>b: a,b = b,a
        return a.isoformat(), b.isoformat()

    for key, idx in RU_MONTHS.items():
        if key in s:
            y = re.findall(r"(20\d{2})", s)
            y = int(y[0]) if y else dt.date.today().year
            last = monthrange(y, idx)[1]
            a = dt.date(y, idx, 1); b = dt.date(y, idx, last)
            return a.isoformat(), b.isoformat()

    raise ValueError("Не удалось распарсить период")

def main():
    # 1) подключимся к Google и найдём клиента
    gc: gspread.Client = get_gs_client()
    client = find_client_by_name(gc, AD_NAME)
    if not client:
        print(f"❌ Клиент '{AD_NAME}' не найден в листе Monthly")
        sys.exit(1)

    ad_account_id = (client.get("ad_account_id") or "").strip()
    spreadsheet_id = (client.get("spreadsheet_id") or "").strip()
    if not ad_account_id or not spreadsheet_id:
        print(f"❌ У клиента не заполнены ad_account_id или spreadsheet_id.\n{client}")
        sys.exit(1)

    # 2) распарсим период
    since, until = parse_period(PERIOD)

    # 3) генерим отчёт
    print(f"⏳ Формирую отчёт: {AD_NAME} • {since}..{until}")
    url = generate_report(
        ad_name=AD_NAME,
        ad_account_id=ad_account_id,
        spreadsheet_id=spreadsheet_id,
        since=since, until=until
    )
    print("✅ Отчёт готов:", f"{AD_NAME} • {since}..{until}")
    print(url)

if __name__ == "__main__":
    main()
