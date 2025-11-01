import re
from typing import Optional
from .fb_client import get

def fetch_any_ad_id_of_campaign(campaign_id: str) -> Optional[str]:
    """Берём любой ad внутри кампании (для MVP этого достаточно)."""
    adsets = get(f"{campaign_id}/adsets", {"fields": "id", "limit": 50}).get("data", [])
    for adset in adsets:
        ads = get(f"{adset['id']}/ads", {"fields": "id", "limit": 50}).get("data", [])
        if ads:
            return ads[0]["id"]
    return None

def get_best_creative_link_for_ad(ad_id: str) -> Optional[str]:
    """
    Пытаемся вернуть устойчивую публичную ссылку на креатив:
      1) instagram_permalink_url (если IG)
      2) object_story_id/effective_object_story_id -> permalink_url (FB)
      3) thumbnail_url (как последняя «видимая» альтернатива)
      4) fallback: Ads Library на ad_id
    """
    # 1) поля креатива
    try:
        ad = get(f"{ad_id}", {
            "fields": "creative{instagram_permalink_url,object_story_id,effective_object_story_id,thumbnail_url}"
        })
        cr = (ad or {}).get("creative", {}) or {}

        ig_link = cr.get("instagram_permalink_url")
        if ig_link:
            return ig_link

        for key in ("object_story_id", "effective_object_story_id"):
            sid = cr.get(key)
            if sid:
                try:
                    post = get(f"{sid}", {"fields": "permalink_url"})
                    url = (post or {}).get("permalink_url")
                    if url:
                        return url
                except Exception:
                    pass

        thumb = cr.get("thumbnail_url")
        if thumb:
            return thumb
    except Exception:
        pass

    # 2) как совсем последний вариант — html превью
    try:
        resp = get(f"{ad_id}/previews", {"ad_format": "DESKTOP_FEED_STANDARD"})
        items = resp.get("data", [])
        if items:
            html = items[0].get("body") or items[0].get("html") or items[0].get("html_rendered") or ""
            m = re.search(r'https://[^\s"<>]+', html)
            if m:
                return m.group(0)
    except Exception:
        pass

    # 3) фолбэк — Ads Library по ad_id (не всегда откроется, но линк стабильный)
    return f"https://www.facebook.com/ads/library/?id={ad_id}"
