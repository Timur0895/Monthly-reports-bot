from typing import Dict, Any, List
from .fb_client import get

def fetch_adsets_daily_budgets(campaign_id: str) -> List[int]:
    # вернём список daily_budget (в minor units)
    fields = "id,name,daily_budget,status"
    data = get(f"{campaign_id}/adsets", {"fields": fields, "limit": 5000})
    budgets = []
    for adset in data.get("data", []):
        val = adset.get("daily_budget")
        if val is not None:
            try:
                budgets.append(int(val))
            except:
                pass
    return budgets

def choose_display_daily_budget(budgets_minor_units: List[int]) -> str:
    # Правило из ТЗ: берём бюджет из ad set, если есть; если нет — "—".
    if not budgets_minor_units:
        return "—"
    # Возьмём первый попавшийся ad set бюджет (можно суммировать при желании)
    v = budgets_minor_units[0]
    return f"{v/100:.2f}"
