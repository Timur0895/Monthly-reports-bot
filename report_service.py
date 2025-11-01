# report_service.py
from __future__ import annotations

from typing import Dict, Any, List
from fb.insights import (
    fetch_campaign_insights,
    fetch_campaign_statuses,
    strict_result_value,
    build_overall_effectiveness_from_fb,
)
from sheets.writer import write_monthly_report


def _sum_spend(rows: List[Dict[str, Any]]) -> float:
    s = 0.0
    for r in rows or []:
        try:
            s += float(r.get("spend") or 0)
        except Exception:
            pass
    return s


def generate_report(
    ad_name: str,
    ad_account_id: str,
    spreadsheet_id: str,
    since: str,
    until: str,
) -> str:
    """
    Генерирует месячный отчёт и возвращает URL таблицы.
    Даты: YYYY-MM-DD.
    """

    print(f"⏳ Формирую отчёт: {ad_name} • {since}..{until}")
    print(f"   ↳ ad_account_id={ad_account_id} | spreadsheet_id={spreadsheet_id}")

    # 1) Инсайты по кампаниям
    rows = fetch_campaign_insights(
        ad_account_id=ad_account_id, since=since, until=until
    )
    spend_total = _sum_spend(rows)
    print(f"🔎 FB insights: campaigns={len(rows)} | spend_total={spend_total:.2f}")

    # 2) Статусы кампаний (для сортировки/отображения)
    status_map = fetch_campaign_statuses(ad_account_id=ad_account_id)
    print(f"🔎 FB statuses: loaded={len(status_map)}")

    # обогащаем строки статусом
    for r in rows:
        cid = r.get("campaign_id") or r.get("id") or ""
        r["effective_status"] = status_map.get(cid, "")

    # 3) «Общая эффективность» тем же правилом, что и таблица кампаний
    overall = build_overall_effectiveness_from_fb(
        rows=rows,
        date_from=since,
        date_to=until,
        chooser=strict_result_value,
    )
    print(
        f"🧮 Overall: has_data={overall.get('has_data')} "
        f"| goals={list((overall.get('goals') or {}).keys())} "
        f"| spend={overall.get('spend', 0)} | period='{overall.get('period')}'"
    )

    # 4) Пишем в Google Sheet
    payload = {"rows": rows, "overall": overall}
    try:
        print(
            f"📝 Пишу в Google Sheet: {spreadsheet_id} "
            f"(лист по умолчанию в файле клиента) | rows={len(rows)}"
        )
        write_monthly_report(
            spreadsheet_id=spreadsheet_id,
            ad_name=ad_name,
            data=payload,
            since=since,
            until=until,
        )
        print("✅ Запись в Google Sheets завершена")
    except Exception as e:
        # даём максимально информативную ошибку
        print(f"❌ Ошибка записи в Google Sheets: {type(e).__name__}: {e}")
        raise

    # 5) Ссылка на файл
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
    print(f"✅ Отчёт готов: {ad_name} • {since}..{until}\n{url}")
    return url
