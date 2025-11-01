# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# === Google Sheets / Drive ===================================================
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# Главная таблица клиентов (Monthly)
# Если в .env не задано, fallback на старые константы
MONTHLY_SHEET_ID = os.getenv("MONTHLY_SHEET_ID") or os.getenv("MASTER_INDEX_SHEET_ID")
MONTHLY_SHEET_NAME = os.getenv("MONTHLY_SHEET_NAME", "Monthly")

# Папка-хранилище для клиентских отчётов (Drive)
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")  # ID папки, куда будут создаваться файлы
TEMPLATE_SPREADSHEET_ID = os.getenv("TEMPLATE_SPREADSHEET_ID")  # шаблон отчёта
TEMPLATE_SHEET_NAME = os.getenv("TEMPLATE_SHEET_NAME", "Report_Template")

# === Facebook Ads ============================================================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v19.0")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")

# === Telegram ================================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_TOPIC_ID = os.getenv("TELEGRAM_TOPIC_ID")

# === Timezone ================================================================
TZ = os.getenv("TZ", "Asia/Almaty")
