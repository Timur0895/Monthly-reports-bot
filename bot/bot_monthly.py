# -*- coding: utf-8 -*-  # bot/bot_monthly.py
from __future__ import annotations

import os
import re
import time
from typing import Dict, Any, List, Tuple

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
from telebot.apihelper import ApiTelegramException  # ← ловим точный тип
from telebot.types import Message

# ВАЖНО: Запускай как модуль из корня:
#   python -m bot.bot_monthly
# Тогда импорты ниже работают без sys.path-хаков.
import gspread
from sheets.gs_client import get_gs_client
from catalog.master_index import load_clients, find_client_by_name

# 👇 Главный оркестратор отчёта (создание листа из шаблона, бюджеты, превью)
from run_monthly_report import main as run_monthly  # main(ad_name: str, period_text: str) -> url

# ──────────────────────────────────────────────────────────────────────────────
# ENV
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID  = int(os.getenv("TELEGRAM_CHAT_ID", "0") or "0")
TELEGRAM_TOPIC_ID = int(os.getenv("TELEGRAM_TOPIC_ID", "0") or "0")
TZ                = os.getenv("TZ", "Asia/Almaty")

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")

# По умолчанию оставляем Markdown, но критичные отправки делаем через _send_safe
BOT = telebot.TeleBot(TELEGRAM_TOKEN, parse_mode="Markdown")
GC: gspread.Client = get_gs_client()  # один клиент на процесс

# Кэш клиентов
_CLIENTS_CACHE: List[Dict[str, Any]] = []
_CACHE_TS: float = 0.0
_CACHE_TTL: float = 60.0  # сек

# ──────────────────────────────────────────────────────────────────────────────
# УТИЛИТЫ ОТПРАВКИ С FALLBACK

def log_err(e: Exception):
    try:
        with open("error.log", "a", encoding="utf-8") as f:
            f.write(f"[monthly-bot] {type(e).__name__}: {e}\n")
    except Exception:
        pass

def _strip_md(text: str) -> str:
    """Грубое снятие Markdown-символов для безопасного plain-text."""
    if text is None:
        return ""
    return (
        text.replace("*", "")
            .replace("_", "")
            .replace("`", "")
            .replace("[", "")
            .replace("]", "")
            .replace("(", "")
            .replace(")", "")
    )

def _send_in_forum_raw(text: str, **kwargs):
    """Базовая отправка в нужную форум-тему/чат (без логики fallback)."""
    if TELEGRAM_TOPIC_ID:
        return BOT.send_message(
            TELEGRAM_CHAT_ID, text, message_thread_id=TELEGRAM_TOPIC_ID, **kwargs
        )
    return BOT.send_message(TELEGRAM_CHAT_ID, text, **kwargs)

def _send_safe(
    text: str,
    reply_markup=None,
    disable_web_page_preview: bool | None = None,
    fallback_text: str | None = None,
    try_markdown: bool = True,
):
    """
    Универсальная безопасная отправка:
    1) Пробуем Markdown (если try_markdown=True).
    2) Если 'can't parse entities' — повторяем с parse_mode=None и fallback_text.
    """
    # 1. Попытка в Markdown
    if try_markdown:
        try:
            return _send_in_forum_raw(
                text,
                reply_markup=reply_markup,
                disable_web_page_preview=disable_web_page_preview,
                parse_mode="Markdown",
            )
        except ApiTelegramException as e:
            # Ловим частый кейс поломки Markdown
            s = str(e).lower()
            if "can't parse entities" in s or "bad request: can't parse entities" in s:
                log_err(e)
            else:
                # Любая другая телега-ошибка — тоже логируем и пробуем plain
                log_err(e)
            # падать не даём — идём в fallback
        except Exception as e:
            log_err(e)
            # пробуем plain

    # 2. Fallback: plain-text без Markdown
    safe_text = fallback_text if fallback_text is not None else _strip_md(text)
    try:
        return _send_in_forum_raw(
            safe_text,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview,
            parse_mode=None,
        )
    except Exception as e:
        # Последняя линия обороны — лог, чтобы не терять контекст
        log_err(e)
        # Ничего не возвращаем, чтобы не ронять поток
        return None

def _send_plain(text: str, **kwargs):
    """Отправка строго без Markdown/HTML, с временным сбросом parse_mode у бота."""
    old_mode = getattr(BOT, "parse_mode", None)
    try:
        BOT.parse_mode = None
        return _send_in_forum_raw(text, parse_mode=None, **kwargs)
    finally:
        BOT.parse_mode = old_mode

def _send_error(text: str):
    """Отправка ошибок БЕЗ Markdown — чтобы не падать на спецсимволах."""
    return _send_safe(_strip_md(text), try_markdown=False)

def _bold_safe(text: str) -> str:
    """Безопасное выделение жирным в Markdown (экранируем самые частые символы)."""
    if text is None:
        text = ""
    safe = text.replace("_", "\\_").replace("*", "\\*").replace("`", "\\`")
    return f"*{safe}*"

def _send_make_report_button(text: str = "Готово. Запустить новый отчёт?"):
    """Показывает повторную кнопку, чтобы сразу начать следующий отчёт."""
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🧾 Сделать отчёт", callback_data="make_report"))
    return _send_safe(text, reply_markup=kb)

def _get_clients() -> List[Dict[str, Any]]:
    global _CLIENTS_CACHE, _CACHE_TS
    now = time.time()
    if not _CLIENTS_CACHE or (now - _CACHE_TS) > _CACHE_TTL:
        _CLIENTS_CACHE = load_clients(GC)
        _CACHE_TS = now
    return _CLIENTS_CACHE

def _period_parse_for_examples() -> str:
    return "В таком формате: 01.10-31.10 (без года)"

def _period_parse(text: str) -> Tuple[str, str]:
    """Те же правила, что и в run_monthly_report/main (сокращённая версия)."""
    import datetime as dt
    from calendar import monthrange

    s = (text or "").strip().lower()

    m = re.match(r"последние\s+(\d{1,3})\s+дн", s)
    if m:
        n = int(m.group(1))
        end = dt.date.today()
        start = end - dt.timedelta(days=n - 1)
        return start.isoformat(), end.isoformat()

    m = re.match(r"(\d{1,2})[.\-/](\d{1,2})\s*[–\-]\s*(\d{1,2})[.\-/](\d{1,2})", s)
    if m:
        y = dt.date.today().year
        d1, mo1, d2, mo2 = map(int, m.groups())
        a = dt.date(y, mo1, d1)
        b = dt.date(y, mo2, d2)
        if a > b:
            a, b = b, a
        return a.isoformat(), b.isoformat()

    m = re.match(r"(\d{4}-\d{2}-\d{2})\s*\.\.\s*(\d{4}-\d{2}-\d{2})", s)
    if m:
        a = dt.date.fromisoformat(m.group(1))
        b = dt.date.fromisoformat(m.group(2))
        if a > b:
            a, b = b, a
        return a.isoformat(), b.isoformat()

    RU_MONTHS = {
        "январ": 1, "феврал": 2, "март": 3, "апрел": 4, "ма": 5, "июн": 6,
        "июл": 7, "август": 8, "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12
    }
    for key, idx in RU_MONTHS.items():
        if key in s:
            ys = re.findall(r"(20\d{2})", s)
            y = int(ys[0]) if ys else dt.date.today().year
            last_day = monthrange(y, idx)[1]
            a = dt.date(y, idx, 1)
            b = dt.date(y, idx, last_day)
            return a.isoformat(), b.isoformat()

    raise ValueError("Не понял формат периода")

def _clients_kb(page: int = 0, per_page: int = 20) -> InlineKeyboardMarkup:
    items = _get_clients()
    kb = InlineKeyboardMarkup(row_width=2)
    start = page * per_page
    end = min(len(items), start + per_page)

    for c in items[start:end]:
        name = c.get("ad_name") or ""
        if not name:
            continue
        kb.add(InlineKeyboardButton(name, callback_data=f"client:{name}"))

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⟵", callback_data=f"page:{page-1}"))
    if end < len(items):
        nav.append(InlineKeyboardButton("⟶", callback_data=f"page:{page+1}"))
    if nav:
        kb.row(*nav)

    kb.row(
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh"),
        InlineKeyboardButton("✖️ Отмена", callback_data="cancel"),
    )
    return kb

# ──────────────────────────────────────────────────────────────────────────────
@BOT.message_handler(commands=["start", "help"])
def cmd_start(msg):
    if msg.chat.id != TELEGRAM_CHAT_ID:
        BOT.reply_to(msg, "Этот бот работает только в нашем форуме.")
        return
    _send_make_report_button(
        "👋 *Привет!*\n Нажми «Сделать отчёт», выбери клиента и укажи период.\n"
    )

@BOT.message_handler(commands=["debug"])
def debug_info(msg: Message):
    chat_id = msg.chat.id
    thread_id = getattr(msg, "message_thread_id", None)
    text = (
        "🧩 DEBUG\n"
        f"chat_id = {chat_id}\n"
        f"thread_id = {thread_id}\n"
        f"title = {getattr(msg.chat, 'title', '')}"
    )
    # Отправляем без Markdown, чтобы ничего не сломать
    old_mode = getattr(BOT, "parse_mode", None)
    try:
        BOT.parse_mode = None
        BOT.send_message(chat_id, text, message_thread_id=thread_id, disable_web_page_preview=True)
    finally:
        BOT.parse_mode = old_mode

@BOT.callback_query_handler(func=lambda c: c.data == "make_report")
def on_make_report(call):
    BOT.answer_callback_query(call.id)
    _send_safe("Выбери клиента 👇", reply_markup=_clients_kb(page=0))

@BOT.callback_query_handler(func=lambda c: c.data == "refresh")
def on_refresh(call):
    global _CLIENTS_CACHE, _CACHE_TS
    _CLIENTS_CACHE = []
    _CACHE_TS = 0.0
    BOT.answer_callback_query(call.id, "Обновлено")
    _send_safe("Выбери клиента 👇", reply_markup=_clients_kb(page=0))

@BOT.callback_query_handler(func=lambda c: c.data == "cancel")
def on_cancel(call):
    BOT.answer_callback_query(call.id, "Отменено")
    _send_safe("Отменил процесс формирования отчёта.")
    _send_make_report_button("Запустить новый отчёт?")

@BOT.callback_query_handler(func=lambda c: c.data.startswith("page:"))
def on_page(call):
    try:
        page = int(call.data.split(":")[1])
    except Exception:
        page = 0
    BOT.answer_callback_query(call.id)
    _send_safe("Выбери клиента 👇", reply_markup=_clients_kb(page=page))

@BOT.callback_query_handler(func=lambda c: c.data.startswith("client:"))
def on_client(call):
    ad_name = call.data.split(":", 1)[1]
    BOT.answer_callback_query(call.id)

    fr = ForceReply(selective=True, input_field_placeholder="например 01.10–20.10")
    sent = _send_safe(
        f"{_bold_safe(ad_name)}\n*Укажи период*\n" + _period_parse_for_examples(),
        reply_markup=fr
    )
    BOT.register_next_step_handler(sent, on_period_reply, ad_name)

def on_period_reply(msg, ad_name: str):
    if msg.chat.id != TELEGRAM_CHAT_ID:
        return

    try:
        _period_parse(msg.text)  # валидация формата
    except Exception as e:
        fr = ForceReply(selective=True, input_field_placeholder="например 01.10–20.10")
        sent = _send_safe(f"Не понял период: {e}\n" + _period_parse_for_examples(), reply_markup=fr)
        BOT.register_next_step_handler(sent, on_period_reply, ad_name)
        return

    period_text = (msg.text or "").strip()

    # Стартовое уведомление — можно с Markdown
    _send_safe(f"⏳ Формирую отчёт: {_bold_safe(ad_name)} • {period_text}")

    try:
        # 💥 Главный вызов
        url = run_monthly(ad_name, period_text)
        if not url:
            url = "(URL не получен)"
        elif not isinstance(url, str):
            url = str(url)

        # ✅ Успешное сообщение — строго plain (без Markdown/HTML)
        success_plain = (
            "✅ Отчёт готов\n"
            f"Клиент: {ad_name}\n"
            f"Период: {period_text}\n"
            f"{url}"
        )

        print("[DEBUG] Попытка отправить сообщение в форум (plain)")
        print(f"[DEBUG] CHAT_ID={TELEGRAM_CHAT_ID}, TOPIC_ID={TELEGRAM_TOPIC_ID}")
        print(f"[DEBUG] TEXT:\n{success_plain}")

        _send_plain(success_plain, disable_web_page_preview=True)

        # Кнопка — отдельно
        _send_make_report_button()

    except Exception as e:
        import traceback
        print("[ERROR] Ошибка при отправке сообщения:")
        traceback.print_exc()
        log_err(e)

        # Фолбек — тоже строго plain
        _send_plain(
            f"⚠️ Отчёт сформирован, но возникла ошибка при отправке сообщения.\nСсылка: {url}",
            disable_web_page_preview=True,
        )
        _send_make_report_button("Хочешь попробовать другой отчёт?")



# ──────────────────────────────────────────────────────────────────────────────
@BOT.message_handler(commands=["ping"])
def ping(msg):
    if msg.chat.id != TELEGRAM_CHAT_ID:
        return
    _send_safe("pong ✅")

def main():
    print("▶ bot_monthly: polling started")
    BOT.infinity_polling(timeout=60, long_polling_timeout=50)

if __name__ == "__main__":
    main()
