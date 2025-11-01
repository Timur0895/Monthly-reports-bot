# -*- coding: utf-8 -*-
from typing import Dict, Any, List, Callable, Tuple
from collections import defaultdict
from datetime import datetime, date
import datetime as dt
import json

from .fb_client import get

# =====================================================================
#                         ВСПОМОГАТЕЛЬНОЕ
# =====================================================================

def _sanitize_account_id(ad_account_id: str) -> str:
    """Возвращает ID в формате act_<id>."""
    s = (ad_account_id or "").strip()
    return s if s.startswith("act_") else f"act_{s}"

def _to_date(s: str, default: dt.date | None = None) -> dt.date:
    """'YYYY-MM-DD' -> date. Если строка пустая/кривая — вернём default или today."""
    try:
        return dt.datetime.strptime(str(s), "%Y-%m-%d").date()
    except Exception:
        return default or dt.date.today()

def _sanitize_time_range(since: str, until: str) -> Dict[str, str]:
    """
    Делает time_range валидным для Graph API:
      - гарантирует наличие since & until (если until пустой — берём since)
      - если since > until — меняем местами
      - если until в будущем — обрезаем до сегодня
      - формат строго YYYY-MM-DD
    """
    today = dt.date.today()
    s = _to_date(since, default=today)
    u = _to_date(until, default=s)   # если until нет — используем since

    if u > today:
        u = today
    if s > u:
        s, u = u, s

    return {"since": s.strftime("%Y-%m-%d"), "until": u.strftime("%Y-%m-%d")}

# =====================================================================
#                         ЗАПРОСЫ К FACEBOOK API
# =====================================================================

def fetch_campaign_insights(ad_account_id: str, since: str, until: str) -> List[Dict[str, Any]]:
    """
    Возвращает сырые инсайты по кампаниям за период [since..until], формат дат 'YYYY-MM-DD'.
    ВАЖНО: time_range сериализуем в JSON-строку — так избегаем 400 ('time_range must be non-empty').
    """
    account = _sanitize_account_id(ad_account_id)
    time_range = _sanitize_time_range(since, until)

    params = {
        "level": "campaign",
        "time_range": json.dumps(time_range, separators=(",", ":")),
        "fields": ",".join([
            "campaign_id",
            "campaign_name",
            "objective",
            "spend",
            "impressions",
            "reach",
            "clicks",
            "actions",
        ]),
        "limit": 5000,
    }
    data = get(f"/{account}/insights", params)
    return data.get("data", [])

def fetch_campaign_statuses(ad_account_id: str) -> Dict[str, str]:
    """Карта id кампании -> effective_status."""
    account = _sanitize_account_id(ad_account_id)
    fields = "id,name,status,effective_status"
    data = get(f"/{account}/campaigns", {"fields": fields, "limit": 5000})
    return {c["id"]: c.get("effective_status", "") for c in data.get("data", [])}

# =====================================================================
#                         ПАРСИНГ ДЕЙСТВИЙ / МЕТРИК
# =====================================================================

def extract_action(actions: List[Dict[str, Any]], action_type: str) -> float:
    """Ищет action по типу и возвращает value как float."""
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == action_type:
            try:
                return float(a.get("value", 0) or 0)
            except Exception:
                return 0.0
    return 0.0

# Варианты ключей для переписок (могут отличаться)
MESSAGING_KEYS = [
    "onsite_conversion.messaging_conversation_started_7d",
    "messaging_conversation_started",
    "onsite_conversion.meta_messaging_conversation_started_7d",
]

def extract_any_messaging(actions: List[Dict[str, Any]]) -> float:
    for k in MESSAGING_KEYS:
        v = extract_action(actions, k)
        if v and v > 0:
            return v
    return 0.0

def extract_link_clicks(row: Dict[str, Any]) -> float:
    """Предпочтительно actions['link_click'], иначе fallback на поле 'clicks'."""
    v = extract_action(row.get("actions", []) or [], "link_click")
    if v and v > 0:
        return v
    try:
        return float(row.get("clicks", 0) or 0)
    except Exception:
        return 0.0

# Варианты ключей покупок
PURCHASE_KEYS = [
    "purchase",
    "onsite_conversion.purchase",
    "offsite_conversion.fb_pixel_purchase",
    "offsite_conversion.purchase",
]

def extract_any_purchase(actions: List[Dict[str, Any]]) -> float:
    for k in PURCHASE_KEYS:
        v = extract_action(actions, k)
        if v and v > 0:
            return v
    return 0.0

# =====================================================================
#                     ЖЁСТКИЙ МЭППИНГ ЦЕЛЕЙ → МЕТРИК
# =====================================================================

# Нормализованные наборы objective (upper) под основные цели
OBJ_MSG   = {"OUTCOME_MESSAGING", "MESSAGES", "MESSAGE", "CLICK_TO_MESSAGE", "OUTCOME_ENGAGEMENT"}
OBJ_LEAD  = {"OUTCOME_LEADS", "LEAD", "LEADS", "LEAD_GEN", "LEAD_GENERATION"}
OBJ_CLICK = {"OUTCOME_TRAFFIC", "TRAFFIC", "LINK_CLICKS", "CLICKS", "ENGAGEMENT"}
OBJ_SALE  = {"OUTCOME_SALES", "OUTCOME_CONVERSIONS", "SALES", "SALE", "PURCHASE", "CONVERSIONS"}

# Жёсткое соответствие «цель → action_type»
ACTION_FOR_GOAL = {
    "Переписки": "onsite_conversion.messaging_conversation_started_7d",
    "Лиды":       "lead",
    "Клики":      "link_click",  # fallback: поле 'clicks'
    # "Продажи" → через extract_any_purchase(...)
}

def normalize_objective(obj: str) -> str:
    return (obj or "").strip().upper()

def goal_by_objective(obj: str) -> str:
    o = normalize_objective(obj)
    if any(tok in o for tok in OBJ_MSG):   return "Переписки"
    if any(tok in o for tok in OBJ_LEAD):  return "Лиды"
    if any(tok in o for tok in OBJ_CLICK): return "Клики"
    if any(tok in o for tok in OBJ_SALE):  return "Продажи"
    return "Клики"  # безопасный дефолт

def strict_result_value(row: Dict[str, Any]) -> Tuple[str, float]:
    """
    Жестко выбирает метрику 'Результат' исходя из цели (по objective):
      - Переписки → actions['onsite_conversion.messaging_conversation_started_7d']
      - Лиды      → actions['lead']
      - Клики     → actions['link_click'] (fallback: поле 'clicks')
      - Продажи   → purchase-экшены
    Возвращает (label, value).
    """
    actions = row.get("actions", []) or []
    label = goal_by_objective(row.get("objective", ""))

    if label == "Переписки":
        return label, extract_action(actions, ACTION_FOR_GOAL["Переписки"])

    if label == "Лиды":
        return label, extract_action(actions, ACTION_FOR_GOAL["Лиды"])

    if label == "Клики":
        v = extract_action(actions, ACTION_FOR_GOAL["Клики"])
        if not v or v == 0:
            try:
                v = float(row.get("clicks", 0) or 0)
            except Exception:
                v = 0.0
        return label, v

    if label == "Продажи":
        return label, extract_any_purchase(actions)

    return "Клики", extract_action(actions, "link_click")

# =====================================================================
#                ОБЩАЯ ЭФФЕКТИВНОСТЬ (ДИНАМИЧЕСКИЕ ЦЕЛИ)
# =====================================================================

def _fmt_ddmm(d) -> str:
    if isinstance(d, date):
        return d.strftime("%d.%m")
    # ожидаем 'YYYY-MM-DD' строку
    return datetime.strptime(str(d), "%Y-%m-%d").strftime("%d.%m")

def build_overall_effectiveness_from_fb(
    rows: List[Dict[str, Any]],
    date_from,
    date_to,
    chooser: Callable[[Dict[str, Any]], tuple] = None,
) -> Dict[str, Any]:
    """
    Собирает блок «Общая эффективность».
    Если передан chooser(row) -> (label, value), используем его (тот же выбор, что в таблице кампаний).
    Иначе — дефолтный мэппинг ниже.
    """
    totals_by_goal: Dict[str, float] = defaultdict(float)
    total_spend = 0.0

    for r in rows:
        try:
            total_spend += float(r.get("spend") or 0)
        except Exception:
            pass

        if chooser:
            label, value = chooser(r)
        else:
            label, value = strict_result_value(r)

        if value and value > 0:
            totals_by_goal[label] += float(value)

    period_str = f"{_fmt_ddmm(date_from)}–{_fmt_ddmm(date_to)}"

    return {
        "period": period_str,
        "goals": dict(totals_by_goal),
        "spend": total_spend,
        "has_data": bool(totals_by_goal) or total_spend > 0,
    }

# =====================================================================
#                                __all__
# =====================================================================

__all__ = [
    # API
    "fetch_campaign_insights",
    "fetch_campaign_statuses",
    # parsers
    "extract_action",
    "extract_any_messaging",
    "extract_link_clicks",
    "extract_any_purchase",
    # strict mapping
    "strict_result_value",
    "goal_by_objective",
    # overall
    "build_overall_effectiveness_from_fb",
]
