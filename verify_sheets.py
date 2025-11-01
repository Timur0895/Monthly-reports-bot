# verify_sheets.py
from __future__ import annotations
import os, sys, time
import gspread
from typing import List, Dict, Any
from sheets.gs_client import get_gs_client
from catalog.master_index import load_clients
from config import MONTHLY_SHEET_ID, MONTHLY_SHEET_NAME

WRITE_CHECK = os.getenv("WRITE_CHECK", "0") == "1"  # 0 = только чтение, 1 = тест записи

def main():
    gc = get_gs_client()

    # 1) Проверяем доступ к листу Monthly
    try:
        doc = gc.open_by_key(MONTHLY_SHEET_ID).worksheet(MONTHLY_SHEET_NAME)
        print(f"✅ Monthly доступен: {MONTHLY_SHEET_ID} / лист '{MONTHLY_SHEET_NAME}'")
    except Exception as e:
        print(f"❌ Нет доступа к Monthly ({MONTHLY_SHEET_ID}): {e}")
        sys.exit(1)

    # 2) Читаем всех клиентов
    try:
        clients: List[Dict[str, Any]] = load_clients(gc)
    except Exception as e:
        print(f"❌ Не удалось загрузить клиентов из Monthly: {e}")
        sys.exit(1)

    ok, warn, bad = [], [], []

    # 3) Пробуем открыть каждую таблицу клиента
    for c in clients:
        name = c.get("ad_name") or "?"
        ssid = (c.get("spreadsheet_id") or "").strip()

        if not ssid:
            warn.append((name, "пустой spreadsheet_id"))
            print(f"⚠️  {name}: пустой spreadsheet_id")
            continue

        try:
            sh = gc.open_by_key(ssid)
            sheet = sh.sheet1  # просто дергаем первый лист
            title = sheet.title

            if WRITE_CHECK:
                # Аккуратный write-check: пишем/очищаем в дальнюю ячейку
                cell = "ZZ1000"
                old = sheet.acell(cell).value
                sheet.update_acell(cell, "__ping__")
                sheet.update_acell(cell, old or "")
                print(f"✅✍️  {name}: открыт, запись ок (лист: {title})")
            else:
                print(f"✅ {name}: открыт (лист: {title})")

            ok.append(name)

        except Exception as e:
            bad.append((name, str(e)))
            print(f"❌ {name}: нет доступа/ошибка → {e}")

        # чуть притормозим, чтобы не спамить API
        time.sleep(0.1)

    # 4) Сводка
    print("\n— РЕЗЮМЕ —")
    print(f"ОК: {len(ok)} | WARN: {len(warn)} | ERR: {len(bad)}")
    if warn:
        print("⚠️  WARN:")
        for n, msg in warn:
            print(f"  - {n}: {msg}")
    if bad:
        print("❌ ERR:")
        for n, err in bad:
            print(f"  - {n}: {err}")

if __name__ == "__main__":
    main()
