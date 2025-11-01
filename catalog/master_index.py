# catalog/master_index.py
from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple
import gspread
from utils import normalize

# ── Конфиг: поддержим новые константы и обратную совместимость ────────────────
try:
    # Предпочитаем явные константы для Monthly
    from config import MONTHLY_SHEET_ID as SHEET_ID, MONTHLY_SHEET_NAME as TAB_NAME
except Exception:
    # Если их ещё нет – используем старые, чтобы не падало
    from config import MASTER_INDEX_SHEET_ID as SHEET_ID, MASTER_INDEX_SHEET_NAME as TAB_NAME

HEADERS = ["ad_account_id", "ad_name", "spreadsheet_id"]
COL_A, COL_B, COL_C = 1, 2, 3  # A, B, C

# ── Вспомогательно ────────────────────────────────────────────────────────────
def _ws(gc: gspread.Client):
    """Вернёт объект Worksheet для листа Monthly."""
    return gc.open_by_key(SHEET_ID).worksheet(TAB_NAME)

def _row_to_dict(row: List[str]) -> Dict[str, Any]:
    """Собираем dict из сырых значений ряда (A..C)."""
    a = row[0].strip() if len(row) > 0 else ""
    b = row[1].strip() if len(row) > 1 else ""
    c = row[2].strip() if len(row) > 2 else ""
    return {"ad_account_id": a, "ad_name": b, "spreadsheet_id": c}

def _find_row_index_by_ad_name(ws, ad_name: str) -> Optional[int]:
    """Найдёт индекс строки (1-based) по значению в колонке B (ad_name)."""
    target = normalize(ad_name)
    col_b = ws.col_values(COL_B)  # весь столбец B, включая заголовок
    # Пропускаем заголовок (строка 1)
    for idx, val in enumerate(col_b[1:], start=2):
        if normalize(val) == target:
            return idx
    return None

# ── Публичные функции ─────────────────────────────────────────────────────────
def load_clients(gc: gspread.Client) -> List[Dict[str, Any]]:
    """
    Загрузить всех клиентов из листа Monthly (A2:C).
    Возвращает список словарей с ключами: ad_account_id, ad_name, spreadsheet_id.
    """
    ws = _ws(gc)
    # get_all_records() ориентируется на заголовок в 1-й строке
    # и вернёт список словарей; но чтобы быть устойчивыми к порядку,
    # считываем сырые значения и сами маппим.
    values = ws.get_all_values()
    if not values:
        return []
    # Ожидаем, что заголовок в первой строке
    data_rows = values[1:]  # со второй строки и ниже
    clients: List[Dict[str, Any]] = []
    for r in data_rows:
        d = _row_to_dict(r)
        if d["ad_name"]:
            clients.append(d)
    return clients

def find_client_by_name(gc: gspread.Client, ad_name: str) -> Optional[Dict[str, Any]]:
    """
    Найти клиента по имени (колонка B: ad_name), регистр/пробелы не важны.
    """
    ws = _ws(gc)
    row_idx = _find_row_index_by_ad_name(ws, ad_name)
    if row_idx is None:
        return None
    row = ws.row_values(row_idx)
    return _row_to_dict(row)

def find_client_row(gc: gspread.Client, ad_name: str) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    """
    Вернёт (row_index, client_dict). Удобно, когда сразу нужна строка для апдейта.
    """
    ws = _ws(gc)
    row_idx = _find_row_index_by_ad_name(ws, ad_name)
    if row_idx is None:
        return None, None
    row = ws.row_values(row_idx)
    return row_idx, _row_to_dict(row)

def write_spreadsheet_id(gc: gspread.Client, ad_name: str, spreadsheet_id: str) -> bool:
    """
    Записать spreadsheet_id (колонка C) для клиента с именем ad_name.
    Возвращает True, если запись выполнена.
    """
    ws = _ws(gc)
    row_idx = _find_row_index_by_ad_name(ws, ad_name)
    if row_idx is None:
        return False
    ws.update_cell(row_idx, COL_C, spreadsheet_id)
    return True
