# fb/fb_client.py
import requests
import json
from typing import Dict, Any
from config import FB_API_VERSION, FB_ACCESS_TOKEN

BASE_URL = f"https://graph.facebook.com/{FB_API_VERSION}"

def get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    GET к Graph API. Добавляет access_token.
    Нормализует time_range (dict -> JSON string), если он передан.
    В случае ошибки печатает понятное тело ответа.
    """
    url = f"{BASE_URL}/{path.lstrip('/')}"
    p = dict(params or {})
    p["access_token"] = FB_ACCESS_TOKEN

    # 🔧 НОРМАЛИЗУЕМ time_range здесь, чтобы не зависеть от вызывающего кода
    if "time_range" in p and isinstance(p["time_range"], dict):
        p["time_range"] = json.dumps(p["time_range"], separators=(",", ":"))

    # Также поддержим вариант, если кто-то передал раздельно time_range[since]/time_range[until]
    if ("time_range[since]" in p or "time_range[until]" in p) and "time_range" not in p:
        tr = {}
        if "time_range[since]" in p: tr["since"] = p.pop("time_range[since]")
        if "time_range[until]" in p: tr["until"] = p.pop("time_range[until]")
        if tr:
            p["time_range"] = json.dumps(tr, separators=(",", ":"))

    r = requests.get(url, params=p, timeout=60)

    if r.status_code >= 400:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise requests.HTTPError(
            f"{r.status_code} {r.reason} for URL: {url}\n"
            f"Params={p}\n"
            f"Response={detail}"
        )

    try:
        return r.json()
    except Exception:
        return {"raw": r.text}
