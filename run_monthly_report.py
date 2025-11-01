# -*- coding: utf-8 -*-
import os
import re
from dotenv import load_dotenv

from sheets.gs_client import get_gs_client
from catalog.master_index import load_clients, find_client_by_name

from sheets.writer import (
    write_overview_dynamic,
    write_campaign_table,
    insert_gap_after_campaigns,
)

from fb.insights import (
    fetch_campaign_insights,
    fetch_campaign_statuses,
    strict_result_value,                 # ← используем жёсткий выбор
    goal_by_objective,
    build_overall_effectiveness_from_fb,
)

from fb.budgets import fetch_adsets_daily_budgets, choose_display_daily_budget
from fb.previews import fetch_any_ad_id_of_campaign, get_best_creative_link_for_ad
from utils import parse_period_ddmm_dash_ddmm
from config import FB_ACCESS_TOKEN

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
#                          ХЕЛПЕРЫ ДЛЯ GOOGLE SHEETS
# ──────────────────────────────────────────────────────────────────────────────



def _copy_master_template_to_period(
    gc,
    target_spreadsheet,
    master_spreadsheet_id: str,
    template_sheet_name: str,
    period_title: str,
):
    """Копирует шаблонный лист в файл клиента и переименовывает по периоду."""
    master = gc.open_by_key(master_spreadsheet_id)
    tpl_ws = master.worksheet(template_sheet_name)
    created = tpl_ws.copy_to(target_spreadsheet.id)
    new_sheet_id = created["sheetId"]

    get_by_id = getattr(target_spreadsheet, "get_worksheet_by_id", None)
    ws = get_by_id(new_sheet_id) if callable(get_by_id) else None
    if not ws:
        for w in target_spreadsheet.worksheets():
            if getattr(w, "id", None) == new_sheet_id:
                ws = w
                break
    if not ws:
        raise RuntimeError(f"Не удалось найти скопированный лист по id={new_sheet_id}")

    existing = {w.title for w in target_spreadsheet.worksheets()}
    new_title = period_title
    suffix = 2
    while new_title in existing:
        new_title = f"{period_title}-{suffix}"
        suffix += 1
    ws.update_title(new_title)
    return ws


# ──────────────────────────────────────────────────────────────────────────────
#                  МЭППИНГ ЦЕЛЕЙ / ВЫБОР МЕТРИКИ (жёсткий)
# ──────────────────────────────────────────────────────────────────────────────

def choose_result_label_value(row):
    """
    Выбор цели и результата по жёстким правилам:
    Переписки → messaging_conversation_started_7d
    Лиды → lead
    Клики → link_click (или clicks)
    Продажи → purchase
    """
    label, value = strict_result_value(row)
    return label, value


# ──────────────────────────────────────────────────────────────────────────────
#                      ФОРМИРОВАНИЕ СТРОК ДЛЯ КАМПАНИЙ
# ──────────────────────────────────────────────────────────────────────────────

def build_campaign_rows(insights, statuses_map):
    """Формирует список строк для блока «Рекламные кампании»."""
    tmp = []
    for row in insights:
        cid = row.get("campaign_id")
        name = row.get("campaign_name") or ""
        spend = float(row.get("spend", 0) or 0)

        # цель и результат по жёстким правилам
        goal_label, result_val = choose_result_label_value(row)
        price = f"{(spend / result_val):.2f}" if result_val and result_val > 0 else ""

        # бюджеты и предпросмотр
        adset_budgets_minor = fetch_adsets_daily_budgets(cid)
        daily_budget_display = choose_display_daily_budget(adset_budgets_minor)
        ad_id = fetch_any_ad_id_of_campaign(cid)
        preview = get_best_creative_link_for_ad(ad_id) if ad_id else ""

        eff_status = (statuses_map.get(cid, "") or "").upper()
        status_display = "Активна" if "ACTIVE" in eff_status else "Неактивна"

        reach = int(float(row.get("reach", 0) or 0))

        tmp.append([
            name, goal_label, status_display, result_val, price, reach,
            daily_budget_display, spend, preview,
        ])

    # сортируем: активные вверх, неактивные вниз
    rows = sorted(tmp, key=lambda r: (0 if r[2] == "Активна" else 1, r[0].lower()))
    return rows


# ──────────────────────────────────────────────────────────────────────────────
#                                   MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main(client_query: str, period_text: str):
    """Основной процесс создания месячного отчёта."""
    if not FB_ACCESS_TOKEN:
        raise RuntimeError("FB_ACCESS_TOKEN missing in .env")

    # 1. Google + master_index
    gc = get_gs_client()
    # clients = load_clients(gc)
    client = find_client_by_name(gc, client_query)
    if not client:
        raise RuntimeError(f"Клиент не найден: {client_query}")

    ad_account_id  = (client.get("ad_account_id")  or "").strip()
    spreadsheet_id = (client.get("spreadsheet_id") or "").strip()

    if not ad_account_id or not spreadsheet_id:
        raise RuntimeError("spreadsheet_id or ad_account_id missing in master_index")

    # 2. Период ('YYYY-MM-DD')
    since, until = parse_period_ddmm_dash_ddmm(period_text)

    # 3. Открываем Google Sheet клиента
    doc = gc.open_by_key(spreadsheet_id)

    # 4. Копируем шаблон
    master_tpl_id = client.get("report_template_spreadsheet_id") or os.getenv("TEMPLATE_SPREADSHEET_ID")
    template_name = client.get("report_template_sheet") or os.getenv("TEMPLATE_SHEET_NAME", "Шаблон")
    if not master_tpl_id:
        raise RuntimeError("No TEMPLATE_SPREADSHEET_ID (env or master_index)")

    ws = _copy_master_template_to_period(
        gc=gc,
        target_spreadsheet=doc,
        master_spreadsheet_id=master_tpl_id,
        template_sheet_name=template_name,
        period_title=period_text,
    )

    # 5. Данные Facebook
    insights = fetch_campaign_insights(ad_account_id, since, until)
    statuses = fetch_campaign_statuses(ad_account_id)

    # 6. Общая эффективность
    overall = build_overall_effectiveness_from_fb(
        insights, since, until, chooser=choose_result_label_value
    )
    write_overview_dynamic(ws, overall["period"], overall)

    # 7. Кампании
    rows = build_campaign_rows(insights, statuses)
    last_row = write_campaign_table(ws, rows)
    insert_gap_after_campaigns(ws, last_row, gap=2)

    # 8. Ссылка на лист
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}#gid={ws.id}"


if __name__ == "__main__":
    client_q = os.getenv("TEST_CLIENT_QUERY", "gravo 2")
    period = os.getenv("TEST_PERIOD", "01.10–20.10")
    url = main(client_q, period)
    print(f"✅ Отчёт готов: {client_q} • {period}\n{url}")
