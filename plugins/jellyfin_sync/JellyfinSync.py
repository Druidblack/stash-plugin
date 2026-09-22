import json
import re
import sys
from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qs

import requests
import stashapi.log as log
from stashapi.stashapp import StashInterface


ITEM_ID_RE = re.compile(r"^[0-9a-fA-F]{32}$")


def _iter_scene_urls(scene: Dict[str, Any]) -> Iterable[str]:
    """Yield URL strings from a Stash scene in a version-tolerant way."""
    for value in scene.get("urls") or []:
        if isinstance(value, str):
            url = value.strip()
        elif isinstance(value, dict):
            url = str(value.get("url") or "").strip()
        else:
            url = ""
        if url:
            yield url


def _extract_jellyfin_item_id_from_url(url: str) -> Optional[str]:
    """Extract a Jellyfin item id from common Web/API URL forms.

    Supported examples:
      http://host:8096/web/index.html#/details?id=<ItemId>&serverId=...
      http://host:8096/web/index.html#!/details?id=<ItemId>&serverId=...
      http://host:8096/web/#/details?id=<ItemId>
      http://host:8096/Items/<ItemId>
      jellyfin/items/<ItemId>
    """
    if not url:
        return None

    # Optional legacy/internal marker.
    match = re.search(r"\bjellyfin/items/([0-9a-fA-F]{32})\b", url, flags=re.IGNORECASE)
    if match:
        return match.group(1)

    # Jellyfin REST URL.
    match = re.search(r"/Items/([0-9a-fA-F]{32})(?:\b|/|\?)", url, flags=re.IGNORECASE)
    if match:
        return match.group(1)

    # Jellyfin Web hash routes. Everything after '#' is a client-side route,
    # so urllib.urlparse().query would not contain the item id.
    if "#" in url:
        fragment = url.split("#", 1)[1]
        if "details" in fragment.lower() and "?" in fragment:
            query = fragment.split("?", 1)[1]
            values = parse_qs(query)
            item_id = (values.get("id") or values.get("Id") or [None])[0]
            if isinstance(item_id, str) and ITEM_ID_RE.fullmatch(item_id):
                return item_id

    return None


def _find_jellyfin_item_id(scene: Dict[str, Any]) -> Optional[str]:
    """Return the first Jellyfin item id stored in scene.urls."""
    for url in _iter_scene_urls(scene):
        item_id = _extract_jellyfin_item_id_from_url(url)
        if item_id:
            log.info(f"Found Jellyfin itemId in Stash URL: {item_id}")
            return item_id
    return None


def _build_headers(api_key: str) -> Dict[str, str]:
    return {
        "X-Emby-Token": api_key,
        "X-MediaBrowser-Token": api_key,
        "Authorization": f'MediaBrowser Token="{api_key}"',
        "Accept": "application/json",
    }


def _refresh_jellyfin_item(
    base_url: str,
    api_key: str,
    item_id: str,
    verify_tls: bool,
) -> bool:
    """Queue a full metadata + image refresh for one Jellyfin item."""
    url = f"{base_url.rstrip('/')}/Items/{item_id}/Refresh"
    params = {
        "metadataRefreshMode": "FullRefresh",
        "imageRefreshMode": "FullRefresh",
        "replaceAllMetadata": "true",
        "replaceAllImages": "true",
        "recursive": "true",
    }

    try:
        response = requests.post(
            url,
            headers=_build_headers(api_key),
            params=params,
            timeout=40,
            verify=verify_tls,
        )
    except requests.RequestException as exc:
        log.error(f"Jellyfin refresh request failed: {exc}")
        return False

    if response.ok:
        log.info(
            f"Jellyfin metadata refresh queued for item {item_id}: "
            f"HTTP {response.status_code}"
        )
        return True

    if response.status_code == 404:
        log.error(
            f"Jellyfin item {item_id} was not found (HTTP 404). "
            "The URL stored in Stash may be outdated or belong to another Jellyfin server."
        )
    elif response.status_code in (401, 403):
        log.error(
            f"Jellyfin rejected the API key or its permissions: HTTP {response.status_code}."
        )
    else:
        log.error(
            f"Jellyfin refresh failed for item {item_id}: "
            f"HTTP {response.status_code}: {response.text}"
        )
    return False


def main() -> int:
    json_input = json.load(sys.stdin)
    server_connection = json_input["server_connection"]
    stash = StashInterface(server_connection)

    config = stash.get_configuration()
    settings: Dict[str, Any] = {
        "jellyfinBaseUrl": "http://localhost:8096",
        "jellyfinApiKey": "",
        "verifyTls": False,
        "skipUnorganized": False,
    }
    settings.update((config.get("plugins") or {}).get("JellyfinSync") or {})

    hook_context = (json_input.get("args") or {}).get("hookContext") or {}
    if hook_context.get("type") != "Scene.Update.Post":
        log.info(f"Unsupported hook type {hook_context.get('type')}; nothing to do.")
        return 0

    scene_id = hook_context.get("id")
    if not scene_id:
        log.info("No hookContext.id; nothing to do.")
        return 0

    scene = stash.find_scene(scene_id)
    if not scene:
        log.error(f"Scene {scene_id} not found.")
        return 1

    if bool(settings.get("skipUnorganized")) and not scene.get("organized"):
        log.info("Scene is not organized; skipping.")
        return 0

    base_url = str(settings.get("jellyfinBaseUrl") or "").strip().rstrip("/")
    api_key = str(settings.get("jellyfinApiKey") or "").strip()
    verify_tls = bool(settings.get("verifyTls"))

    if not base_url or not api_key:
        log.error("Missing Jellyfin base URL or API key in plugin settings.")
        return 1

    item_id = _find_jellyfin_item_id(scene)
    if not item_id:
        log.info(
            "Scene has no supported Jellyfin URL. "
            "Skipping without title/filename search."
        )
        return 0

    return 0 if _refresh_jellyfin_item(
        base_url=base_url,
        api_key=api_key,
        item_id=item_id,
        verify_tls=verify_tls,
    ) else 1


if __name__ == "__main__":
    sys.exit(main())
