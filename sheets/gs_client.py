# sheets/gs_client.py
from __future__ import annotations
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import gspread
from config import GOOGLE_SERVICE_ACCOUNT_JSON
import os

# Доступы к Google API
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# ───────────────────────────────────────────────────────────────
# КЛИЕНТЫ
# ───────────────────────────────────────────────────────────────

def get_gs_client() -> gspread.Client:
    """Авторизация gspread (для работы с таблицами)."""
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_SERVICE_ACCOUNT_JSON, SCOPES
    )
    return gspread.authorize(creds)

def get_drive_service():
    """Создать сервис Google Drive API (для копирования файлов)."""
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_SERVICE_ACCOUNT_JSON, SCOPES
    )
    return build("drive", "v3", credentials=creds)

# ───────────────────────────────────────────────────────────────
# УТИЛИТЫ ДЛЯ ЧТЕНИЯ/ЗАПИСИ
# ───────────────────────────────────────────────────────────────

def get_values(spreadsheet_id: str, range_: str):
    """Прочитать диапазон из Google Sheets (list of lists)."""
    gc = get_gs_client()
    ws = gc.open_by_key(spreadsheet_id)
    sheet = ws.worksheet(range_.split("!")[0]) if "!" in range_ else ws.sheet1
    return sheet.get(range_.split("!")[1] if "!" in range_ else range_)

def update_value(spreadsheet_id: str, cell: str, value: str):
    """Записать значение в конкретную ячейку (A1-формат)."""
    gc = get_gs_client()
    ws = gc.open_by_key(spreadsheet_id)
    sheet = ws.worksheet(cell.split("!")[0]) if "!" in cell else ws.sheet1
    sheet.update_acell(cell.split("!")[1] if "!" in cell else cell, value)

def find_row_index(spreadsheet_id: str, tab: str, col_letter: str, needle: str):
    """Найти номер строки (int) по значению needle в заданной колонке."""
    gc = get_gs_client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(tab)
    values = ws.col_values(_col_to_index(col_letter))
    for i, v in enumerate(values, start=1):
        if v.strip().lower() == needle.strip().lower():
            return i
    return None

def _col_to_index(col_letter: str) -> int:
    """A→1, B→2 и т.п."""
    col_letter = col_letter.upper()
    result = 0
    for c in col_letter:
        result = result * 26 + (ord(c) - ord("A")) + 1
    return result

# ───────────────────────────────────────────────────────────────
# КОПИРОВАНИЕ ШАБЛОНА В ПАПКУ
# ───────────────────────────────────────────────────────────────

def create_spreadsheet_copy(
    src_id: str,
    dst_title: str,
    dst_folder_id: str | None = None
) -> str:
    """
    Создать копию Google Spreadsheet (src_id) с новым именем dst_title.
    Если указан dst_folder_id — поместить копию в эту папку.
    Возвращает ID созданного файла.
    """
    drive = get_drive_service()
    body = {"name": dst_title}
    if dst_folder_id:
        body["parents"] = [dst_folder_id]

    new_file = (
        drive.files()
        .copy(fileId=src_id, body=body, fields="id")
        .execute()
    )
    return new_file["id"]

# ───────────────────────────────────────────────────────────────
# ПРИМЕРЫ ВЫЗОВА
# ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import TEMPLATE_SPREADSHEET_ID, GDRIVE_FOLDER_ID
    new_id = create_spreadsheet_copy(
        src_id=TEMPLATE_SPREADSHEET_ID,
        dst_title="Monthly_report_Test",
        dst_folder_id=GDRIVE_FOLDER_ID,
    )
    print("✅ Новый файл создан:", new_id)
