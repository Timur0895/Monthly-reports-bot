# -*- coding: utf-8 -*-  # sheets/writer.py
from __future__ import annotations
from typing import List, Dict, Any, Tuple
import re
import gspread

from config import TEMPLATE_SHEET_NAME  # ← имя листа-шаблона из .env

# ── Якоря под твой шаблон ─────────────────────────────────────────────────────
OVERVIEW_START_CELL  = "A45"   # якорь блока «Общая эффективность»: тут стоит "Период"
CAMPAIGNS_START_CELL = "A50"   # якорь блока «Рекламные кампании»: тут стоит "Кампания"

# Шапка таблицы кампаний
CAMPAIGNS_HEADERS = [
    "Кампания",
    "Цель",
    "Статус",
    "Результат",
    "Цена (за действие)",
    "Охваты",
    "Бюджет",
    "Расходы",
    "Ссылка на объявление",
]

# Колонки, которые центрируем / форматируем как валюту
_CAMPAIGNS_CENTER_COLS   = {"Результат", "Цена (за действие)", "Охваты", "Бюджет", "Расходы"}
_CAMPAIGNS_CURRENCY_COLS = {"Цена (за действие)", "Бюджет", "Расходы"}

# ── ВСПОМОГАТЕЛЬНОЕ ───────────────────────────────────────────────────────────
_A1_RE = re.compile(r"^([A-Za-z]+)(\d+)$")

def _a1_to_rowcol(a1: str) -> Tuple[int, int]:
    m = _A1_RE.match(a1)
    if not m:
        raise ValueError(f"Bad A1: {a1}")
    col_letters, row_str = m.groups()
    row = int(row_str)
    col = 0
    for ch in col_letters.upper():
        col = col * 26 + (ord(ch) - ord("A") + 1)
    return row, col

def _col_to_letters(col: int) -> str:
    letters = []
    n = col
    while n:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))

def _rowcol_to_a1(row: int, col: int) -> str:
    return f"{_col_to_letters(col)}{row}"

def _range_a1(r1: int, c1: int, r2: int, c2: int) -> str:
    return f"{_rowcol_to_a1(r1, c1)}:{_rowcol_to_a1(r2, c2)}"

def _format_center(ws: gspread.Worksheet, a1_range: str):
    try:
        ws.format(a1_range, {"horizontalAlignment": "CENTER"})
    except Exception:
        pass

def _format_header(ws: gspread.Worksheet, a1_range: str):
    try:
        ws.format(a1_range, {
            "textFormat": {"bold": True},
            "horizontalAlignment": "CENTER",
            "backgroundColor": {"red": 0.90, "green": 0.95, "blue": 0.98}
        })
    except Exception:
        pass

def _format_currency_usd(ws: gspread.Worksheet, a1_range: str):
    try:
        ws.format(a1_range, {"numberFormat": {"type": "CURRENCY", "pattern": "\"$\"#,##0.00"}})
    except Exception:
        pass

def _set_basic_filter(ws: gspread.Worksheet, a1_range: str):
    try:
        ws.set_basic_filter(a1_range)
    except Exception:
        pass

def _freeze_rows(ws: gspread.Worksheet, rows: int):
    try:
        ws.freeze(rows=rows)
    except Exception:
        pass

# ── ОТКРЫТИЕ/ШАБЛОН (если нужно где-то ещё) ──────────────────────────────────
def open_target_sheet(gc: gspread.Client, monthly_report_url: str) -> gspread.Spreadsheet:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", monthly_report_url)
    if not m:
        raise ValueError(f"Bad Google Sheet URL: {monthly_report_url}")
    return gc.open_by_key(m.group(1))

# ── «ОБЩАЯ ЭФФЕКТИВНОСТЬ» ─────────────────────────────────────────────────────
def write_overview_dynamic(ws: gspread.Worksheet, period_text: str, overall: Dict[str, Any]):
    """
    Пишет блок «Общая эффективность» динамически:
      - Заголовок: Период | <цели с >0> | Расходы
      - Значения:  <period> | <суммы>    | <spend>
    overall: {"period": "...", "goals": {"Переписки": 583, ...}, "spend": 123.45}
    """
    start_row, start_col = _a1_to_rowcol(OVERVIEW_START_CELL)

    goals = []
    goals_dict = (overall.get("goals") or {})
    for g in sorted(goals_dict.keys()):
        if goals_dict[g] and float(goals_dict[g]) > 0:
            goals.append(g)

    headers = ["Период"] + goals + ["Расходы"]
    values  = [period_text] + [goals_dict.get(g, "—") or "—" for g in goals] + [overall.get("spend", 0) or 0]

    end_col = start_col + len(headers) - 1

    # очистим область под шапку+строку значений
    ws.batch_clear([_range_a1(start_row, start_col, start_row + 1, end_col)])

    # запись
    ws.update(_range_a1(start_row, start_col, start_row, end_col), [headers])
    ws.update(_range_a1(start_row + 1, start_col, start_row + 1, end_col), [values])

    # форматирование
    _format_header(ws, _range_a1(start_row, start_col, start_row, end_col))
    if end_col > start_col:
        _format_center(ws, _range_a1(start_row + 1, start_col + 1, start_row + 1, end_col))
    _format_currency_usd(ws, _range_a1(start_row + 1, end_col, start_row + 1, end_col))

# ── ТАБЛИЦА КАМПАНИЙ ──────────────────────────────────────────────────────────
def write_campaign_table(ws: gspread.Worksheet, rows: List[List[Any]]) -> int:
    """
    Пишем шапку с A53 и строки с A54 (см. CAMPAIGNS_START_CELL).
    Возвращает last_row — номер последней строки с данными.
    """
    header_row, start_col = _a1_to_rowcol(CAMPAIGNS_START_CELL)
    data_start = header_row + 1
    end_col = start_col + len(CAMPAIGNS_HEADERS) - 1
    last_row = data_start + max(len(rows), 1) - 1

    # чистим диапазон под таблицу
    ws.batch_clear([_range_a1(header_row, start_col, max(last_row, header_row + 1), end_col)])

    # шапка
    ws.update(_range_a1(header_row, start_col, header_row, end_col), [CAMPAIGNS_HEADERS])
    _format_header(ws, _range_a1(header_row, start_col, header_row, end_col))

    # данные
    if rows:
        ws.update(_range_a1(data_start, start_col, last_row, end_col), rows)

    # оформление таблицы
    _apply_campaigns_format(ws, header_row, start_col, last_row, end_col)

    # 🔓 гарантированно снимаем закрепление (и строк, и столбцов)
    try:
        ws.freeze(rows=0, cols=0)
    except Exception:
        pass

    return last_row

def insert_gap_after_campaigns(ws: gspread.Worksheet, last_row_of_table: int, gap: int = 2):
    """Вставляет gap строк сразу после таблицы кампаний, смещая вниз весь шаблон."""
    insert_at = last_row_of_table + 1
    # 1️⃣ Вставляем пустые строки
    for _ in range(gap):
        ws.insert_row([], insert_at)   # вставляем ПЕРЕД index

    # 2️⃣ Добавляем блок итогового резюме
    summary_values = [
        ["✅ Итоговое резюме для клиента", ""],
        ["Краткий абзац 2–3 предложения:", ""],
    ]
    ws.update(f"A{insert_at + gap}:B{insert_at + gap + 1}", summary_values)

    # 3️⃣ Немного оформления (жирный заголовок)
    try:
        ws.format(f"A{insert_at + gap}", {
            "textFormat": {"bold": True, "fontSize": 11},
            "horizontalAlignment": "LEFT"
        })
    except Exception:
        pass

    # 4️⃣ Возвращаем позицию после вставленного блока
    return insert_at + gap + len(summary_values)

def _apply_campaigns_format(ws: gspread.Worksheet, header_row: int, start_col: int, last_row: int, end_col: int):
    """Оформление: заморозка, фильтр, центровка чисел, валютные форматы."""
    _freeze_rows(ws, header_row)
    _set_basic_filter(ws, _range_a1(header_row, start_col, max(last_row, header_row + 1), end_col))

    header_to_index = {name: i for i, name in enumerate(CAMPAIGNS_HEADERS)}  # 0-based

    for col_name in _CAMPAIGNS_CENTER_COLS:
        if col_name in header_to_index:
            c = start_col + header_to_index[col_name]
            _format_center(ws, _range_a1(header_row + 1, c, max(last_row, header_row + 1), c))

    for col_name in _CAMPAIGNS_CURRENCY_COLS:
        if col_name in header_to_index:
            c = start_col + header_to_index[col_name]
            _format_currency_usd(ws, _range_a1(header_row + 1, c, max(last_row, header_row + 1), c))

# ── ДОБАВЛЕНО: выбор листа по периоду + дублирование шаблона ────────────────
from sheets.gs_client import get_gs_client
from fb.insights import strict_result_value

def _safe_float(x, default=0.0) -> float:
    try:
        return float(x or 0)
    except Exception:
        return default

def _build_campaign_rows(raw_rows: List[Dict[str, Any]]) -> List[List[Any]]:
    """Собирает строки для таблицы кампаний в порядке CAMPAIGNS_HEADERS."""
    out: List[List[Any]] = []
    for r in raw_rows or []:
        name   = r.get("campaign_name") or r.get("name") or ""
        status = r.get("effective_status") or r.get("status") or ""
        goal, result_val = strict_result_value(r)

        spend = _safe_float(r.get("spend"), 0.0)
        price = spend / result_val if result_val and result_val > 0 else None
        reach = _safe_float(r.get("reach"), 0.0)

        budget = None        # заполним позже, если подключим fb/budgets.py
        preview_link = ""    # можно подставить через fb/previews.py

        out.append([name, goal, status, result_val, price, reach, budget, spend, preview_link])
    return out

def _period_title(since: str, until: str) -> str:
    """
    Имя листа: YYYY-MM (DD–DD), напр. '2025-10 (01–20)'.
    Если весь месяц — просто '2025-10'. Если разные месяцы — '2025-09_2025-10'.
    """
    try:
        y1, m1, d1 = since.split("-")
        y2, m2, d2 = until.split("-")
        base = f"{y1}-{m1}"
        if (y1, m1) == (y2, m2):
            return base if d1 == "01" else f"{base} ({d1}–{d2})"
        return f"{y1}-{m1}_{y2}-{m2}"
    except Exception:
        return f"{since}..{until}"

def _ensure_period_worksheet(doc: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    """
    Возвращает лист с именем title. Если нет — делает копию шаблона TEMPLATE_SHEET_NAME
    и переименовывает. Если шаблона нет — создаёт пустой лист.
    """
    for ws in doc.worksheets():
        if ws.title == title:
            return ws

    # пробуем найти шаблон
    try:
        tpl = doc.worksheet(TEMPLATE_SHEET_NAME)
        new_ws = doc.duplicate_sheet(source_sheet_id=tpl.id, new_sheet_name=title)
        return new_ws
    except Exception:
        # нет шаблона — создаём пустой
        return doc.add_worksheet(title=title, rows=300, cols=40)

# ── ТОЧКА ВХОДА ───────────────────────────────────────────────────────────────
def write_monthly_report(
    spreadsheet_id: str,
    ad_name: str,
    data: Dict[str, Any],
    since: str,
    until: str
) -> None:
    """
    Главная точка записи:
      1) Создаёт/находит лист периода (из шаблона, если он есть)
      2) Пишет блок «Общая эффективность»
      3) Пишет таблицу кампаний
      4) Добавляет 2 пустые строки после таблицы и снимает закрепления
    Ожидает data = {"overall": {...}, "rows": [...]}
    """
    gc = get_gs_client()
    doc = gc.open_by_key(spreadsheet_id)

    # 👉 работаем с листом периода, а не с sheet1
    title = _period_title(since, until)
    ws: gspread.Worksheet = _ensure_period_worksheet(doc, title)

    # 1) Общая эффективность
    overall: Dict[str, Any] = (data or {}).get("overall") or {}
    period_text = overall.get("period") or f"{since}–{until}"
    write_overview_dynamic(ws, period_text, overall)

    # 2) Таблица кампаний
    table_rows = _build_campaign_rows((data or {}).get("rows") or [])
    last_row = write_campaign_table(ws, table_rows)

    # 3) Разрыв после таблицы
    target_row = insert_gap_after_campaigns(ws, last_row, gap=2)

    # 4) Добавим финальный блок "Итоговое резюме для клиента"
    summary_values = [
        ["✅ Итоговое резюме для клиента", ""],
        ["Краткий абзац 2–3 предложения:", ""],
    ]
    # вставляем эти строки под таблицей
    # Пишем в A..B две строки подряд
    ws.update(f"A{target_row}:B{target_row+1}", summary_values)

    # Немного оформления заголовка
    try:
        ws.format(f"A{target_row}", {"textFormat": {"bold": True, "fontSize": 11}})
    except Exception:
        pass

    # 4) На всякий случай — снять закрепление ещё раз
    try:
        ws.freeze(rows=0, cols=0)
    except Exception:
        pass
