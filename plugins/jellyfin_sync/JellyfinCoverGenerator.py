#!/usr/bin/env python3
"""
Stash -> Jellyfin Cover Generator

Generates a portrait Jellyfin Primary poster from:
  - Stash scene screenshot / current cover image (Scene.paths.screenshot)
  - Stash studio logo (Scene.studio.image_path)

Features:
  - saliency-only crop using the v0.2.3 saliency-cluster-centered algorithm
  - face/eye detection intentionally disabled for stable crop behavior
  - proportional bottom-right studio logo overlay
  - Jellyfin item matching by existing Stash URLs, path, filename/title search
  - Jellyfin Primary image upload using the same base64-body approach as the
    existing Jellyfin performer image sync plugin.
"""

from __future__ import annotations

import base64
import io
import json
import math
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from PIL import Image, ImageEnhance, ImageFilter, ImageDraw, ImageFont

PLUGIN_VERSION = "0.2.3-saliency-quiet-libfilter-fix2"
REQUIRED_KEYS = {"jellyfin_url", "jellyfin_api_key"}
HEX_ID_RE = re.compile(r"[0-9a-fA-F]{32}")

try:
    RESAMPLE_LANCZOS = Image.Resampling.LANCZOS  # Pillow >= 9.1
except Exception:  # pragma: no cover
    RESAMPLE_LANCZOS = Image.LANCZOS


# -------------------------
# Stash logging helpers
# -------------------------

RUN_LOGS: List[str] = []

_STASH_LOG_CODES = {
    "TRACE": "t",
    "DEBUG": "d",
    "INFO": "i",
    "WARNING": "w",
    "WARN": "w",
    "ERROR": "e",
    "CRITICAL": "e",
}

def _env_on(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def log(level: str, message: str) -> None:
    """Log to Stash with encoded levels while keeping stdout valid JSON.

    Raw Stash plugins must reserve stdout for the task result. Plugin log
    messages go to stderr. To prevent Stash from treating normal INFO/WARNING
    lines as ERROR, stderr messages are encoded using Stash's SOH + level + STX
    prefix format. DEBUG lines are kept in the JSON result by default and can be
    emitted to Stash by setting JCG_LOG_DEBUG=1.
    """
    lvl = (level or "info").upper()
    if lvl == "WARN":
        lvl = "WARNING"
    line = f"[{lvl}] {message}"
    RUN_LOGS.append(line)

    emit_debug = _env_on("JCG_LOG_DEBUG", False)
    if lvl == "DEBUG" and not emit_debug:
        return

    code = _STASH_LOG_CODES.get(lvl, "i")
    try:
        print(f"\x01{code}\x02{line}", file=sys.stderr, flush=True)
    except Exception:
        # Last-resort fallback. If this happens, errLog will decide the level.
        print(line, file=sys.stderr, flush=True)


def add_run_logs(result: Any) -> Any:
    if isinstance(result, dict):
        result.setdefault("messages", list(RUN_LOGS))
        return result
    return {"message": result, "messages": list(RUN_LOGS)}


def read_input() -> Dict[str, Any]:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    return json.loads(raw)


# -------------------------
# Generic settings helpers
# -------------------------

def normalize_id(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def snake_key(k: str) -> str:
    k = (k or "").strip()
    if not k:
        return ""
    k = k.replace("-", "_")
    k = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", k)
    return k.lower()


def flatten_settings(obj: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            kk = snake_key(str(k))
            if not kk:
                continue
            if isinstance(v, dict):
                if "value" in v or "Value" in v:
                    out[kk] = v.get("value", v.get("Value"))
                elif "string" in v or "String" in v:
                    out[kk] = v.get("string", v.get("String"))
                else:
                    out[kk] = v
            else:
                out[kk] = v
        return out

    if isinstance(obj, list):
        for it in obj:
            if not isinstance(it, dict):
                continue
            k = it.get("key") or it.get("Key") or it.get("name") or it.get("Name")
            v = it.get("value") if "value" in it else it.get("Value")
            kk = snake_key(str(k or ""))
            if kk:
                out[kk] = v
    return out


def extract_settings_from_payload(inp: Dict[str, Any]) -> Dict[str, Any]:
    args = inp.get("args") or {}
    merged: Dict[str, Any] = {}

    for key in ("settings", "pluginSettings", "plugin_settings", "config", "pluginConfig", "plugin_config"):
        v = args.get(key)
        if isinstance(v, (dict, list)):
            merged.update(flatten_settings(v))

    flat = flatten_settings(args) if isinstance(args, dict) else {}
    known = {
        "jellyfin_url", "jellyfin_api_key", "jellyfin_user_id", "update_on_scene_update",
        "skip_unorganized", "path_rewrite_from", "path_rewrite_to", "poster_width",
        "poster_height", "logo_width_percent", "logo_max_height_percent",
        "logo_padding_percent", "image_quality", "save_debug_copy", "dry_run",
        "scene_id", "timeout_seconds", "use_studio_name_when_logo_missing",
        "logo_trim_transparent_padding", "logo_visibility_boost", "logo_background_opacity",
        "upload_generated_to_jellyfin", "generated_folder", "generated_upload_limit",
        "jellyfin_upload_delay_seconds", "include_library_paths", "exclude_library_paths",
    }
    for k, v in flat.items():
        if k in known:
            merged[k] = v
    return merged


def _s(v: Any, default: str = "") -> str:
    if v is None:
        return default
    return str(v)


def _bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _num(v: Any, default: int, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    try:
        n = int(float(str(v).strip()))
    except Exception:
        n = int(default)
    if min_value is not None:
        n = max(min_value, n)
    if max_value is not None:
        n = min(max_value, n)
    return n


def apply_defaults(settings: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(settings or {})
    out.setdefault("jellyfin_url", "")
    out.setdefault("jellyfin_api_key", "")
    out.setdefault("jellyfin_user_id", "")
    out.setdefault("update_on_scene_update", False)
    out.setdefault("skip_unorganized", False)
    out.setdefault("path_rewrite_from", "")
    out.setdefault("path_rewrite_to", "")
    out.setdefault("poster_width", 800)
    out.setdefault("poster_height", 1200)
    out.setdefault("logo_width_percent", 50)
    # Kept only for backward compatibility with old saved plugin settings.
    # v0.1.2 sizes the logo by width and preserves logo aspect ratio.
    out.setdefault("logo_max_height_percent", 100)
    out.setdefault("logo_padding_percent", 3)
    out.setdefault("image_quality", 92)
    out.setdefault("save_debug_copy", False)
    out.setdefault("dry_run", False)
    out.setdefault("timeout_seconds", 30)
    out.setdefault("use_studio_name_when_logo_missing", False)
    out.setdefault("logo_trim_transparent_padding", False)
    out.setdefault("logo_visibility_boost", False)
    out.setdefault("logo_background_opacity", 0)
    # Manual-only bulk mode: upload already generated local posters to Jellyfin.
    out.setdefault("upload_generated_to_jellyfin", False)
    out.setdefault("generated_folder", "")
    out.setdefault("generated_upload_limit", 0)
    out.setdefault("jellyfin_upload_delay_seconds", 2)
    out.setdefault("include_library_paths", "")
    out.setdefault("exclude_library_paths", "")
    return out


# -------------------------
# Stash connection + GraphQL
# -------------------------

def stash_base_from_server_connection(sc: Dict[str, Any]) -> str:
    scheme = sc.get("Scheme") or sc.get("scheme") or "http"
    host = sc.get("Host") or sc.get("host") or "localhost"
    port = sc.get("Port") or sc.get("port") or 9999
    if host in ("0.0.0.0", "::"):
        host = "localhost"
    return f"{scheme}://{host}:{port}"


def stash_plugin_dir_from_server_connection(sc: Dict[str, Any]) -> str:
    return str(sc.get("PluginDir") or sc.get("pluginDir") or sc.get("plugin_dir") or "")


def stash_plugin_id_from_plugin_dir(plugin_dir: str) -> str:
    if not plugin_dir:
        return ""
    return os.path.basename(plugin_dir.rstrip("/\\"))


def stash_cookie_from_server_connection(sc: Dict[str, Any]) -> str:
    c = sc.get("SessionCookie") or sc.get("sessionCookie") or sc.get("session_cookie") or ""
    if not c:
        c = sc.get("cookie") or sc.get("Cookies") or sc.get("cookies") or ""

    def one(x: Any) -> str:
        if not x:
            return ""
        if isinstance(x, str):
            return x.strip()
        if isinstance(x, (bytes, bytearray)):
            try:
                return bytes(x).decode("utf-8").strip()
            except Exception:
                return ""
        if isinstance(x, dict):
            name = str(x.get("Name") or x.get("name") or "").strip()
            val = str(x.get("Value") or x.get("value") or "").strip()
            if name and val:
                return f"{name}={val}"
            raw = x.get("Raw") or x.get("raw") or ""
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
        return ""

    if isinstance(c, list):
        return "; ".join(p for p in (one(i) for i in c) if p)
    return one(c)


def gql_post(stash_base: str, cookie: str, query: str, variables: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    headers: Dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
    if cookie:
        headers["Cookie"] = cookie

    r = requests.post(
        f"{stash_base}/graphql",
        headers=headers,
        json={"query": query, "variables": variables or {}},
        timeout=timeout,
    )
    text = (r.text or "").strip()
    try:
        data = r.json() if text else {}
    except Exception:
        data = {"raw": text}

    if r.status_code >= 400:
        raise RuntimeError(f"Stash GraphQL HTTP {r.status_code}: {text[:1200]}")
    if isinstance(data, dict) and data.get("errors"):
        raise RuntimeError(f"Stash GraphQL errors: {data['errors']}")
    return data


def fetch_plugin_settings_from_stash(stash_base: str, cookie: str, timeout: int, plugin_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    debug: Dict[str, Any] = {"plugin_id": plugin_id, "cookie_present": bool(cookie), "stash_base": stash_base}

    try:
        data = gql_post(stash_base, cookie, "query C { configuration { plugins } }", {}, timeout)
        conf = (data.get("data") or {}).get("configuration") or {}
        plugins = conf.get("plugins")
        if isinstance(plugins, dict):
            debug["plugins_shape"] = "map"
            if plugin_id and plugin_id in plugins:
                return flatten_settings(plugins[plugin_id]), debug
            pid = normalize_id(plugin_id)
            for k, v in plugins.items():
                if normalize_id(str(k)) == pid and pid:
                    return flatten_settings(v), debug
            # fallback: find a config containing our required keys
            for k, v in plugins.items():
                s = flatten_settings(v)
                if s and all(x in s for x in REQUIRED_KEYS):
                    debug["matched_key"] = k
                    return s, debug
        else:
            debug["plugins_shape"] = type(plugins).__name__
    except Exception as e:
        debug["plugins_scalar_query_error"] = str(e)

    try:
        data = gql_post(stash_base, cookie, "query C { configuration { plugins { name settings } } }", {}, timeout)
        conf = (data.get("data") or {}).get("configuration") or {}
        plugins = conf.get("plugins")
        if isinstance(plugins, list):
            pid = normalize_id(plugin_id)
            for p in plugins:
                if not isinstance(p, dict):
                    continue
                if normalize_id(str(p.get("name") or "")) == pid and pid:
                    return flatten_settings(p.get("settings")), debug
            for p in plugins:
                if not isinstance(p, dict):
                    continue
                s = flatten_settings(p.get("settings"))
                if s and all(x in s for x in REQUIRED_KEYS):
                    debug["matched_name"] = p.get("name")
                    return s, debug
    except Exception as e:
        debug["plugins_list_query_error"] = str(e)

    return {}, debug


def get_scene(stash_base: str, cookie: str, scene_id: str, timeout: int) -> Dict[str, Any]:
    q = """
    query FindSceneForJellyfinCover($id: ID!) {
      findScene(id: $id) {
        id
        title
        date
        organized
        urls
        paths { screenshot }
        files { path basename }
        studio { id name image_path parent_studio { id name image_path parent_studio { id name image_path } } }
      }
    }
    """
    data = gql_post(stash_base, cookie, q, {"id": str(scene_id)}, timeout)
    scene = ((data.get("data") or {}).get("findScene") or {})
    if not scene:
        raise RuntimeError(f"Scene {scene_id} not found in Stash")
    return scene


def scene_primary_path(scene: Dict[str, Any]) -> str:
    files = scene.get("files") or []
    if files and isinstance(files[0], dict):
        return str(files[0].get("path") or "")
    return ""


def _basename_no_ext(path: str) -> str:
    if not path:
        return ""
    return os.path.splitext(os.path.basename(path))[0]


def _rewrite_prefix(path: str, prefix_from: str, prefix_to: str) -> str:
    if not path:
        return ""
    pf = (prefix_from or "").strip()
    pt = (prefix_to or "").strip()
    if not pf:
        return path
    if path.startswith(pf):
        return pt + path[len(pf):]
    return path


def _parse_path_prefixes(raw: Any) -> List[str]:
    text = _s(raw).strip()
    if not text:
        return []
    parts = re.split(r"[\n,;]+", text)
    out: List[str] = []
    for part in parts:
        val = part.strip()
        if not val:
            continue
        val = val.replace('\\', '/').rstrip('/')
        out.append(val if val else '/')
    return out


def _normalize_path_for_match(path: str) -> str:
    val = _s(path).strip().replace('\\', '/')
    val = re.sub(r'/+', '/', val)
    if len(val) > 1 and val.endswith('/'):
        val = val.rstrip('/')
    return val


def _matches_prefix(path: str, prefix: str) -> bool:
    p = _normalize_path_for_match(path)
    pref = _normalize_path_for_match(prefix)
    if not pref:
        return False
    return p == pref or p.startswith(pref + '/')


def library_filter_decision(path: str, settings: Dict[str, Any]) -> Tuple[bool, str]:
    norm_path = _normalize_path_for_match(path)
    include_prefixes = _parse_path_prefixes(settings.get('include_library_paths'))
    exclude_prefixes = _parse_path_prefixes(settings.get('exclude_library_paths'))

    if not norm_path:
        if include_prefixes or exclude_prefixes:
            return False, 'scene has no primary file path for library filter check'
        return True, ''

    for pref in exclude_prefixes:
        if _matches_prefix(norm_path, pref):
            return False, f'path excluded by rule {pref}'

    if include_prefixes:
        for pref in include_prefixes:
            if _matches_prefix(norm_path, pref):
                return True, ''
        return False, 'path is outside allowed library list'

    return True, ''


def fetch_stash_image(stash_base: str, cookie: str, image_path: str, timeout: int) -> Tuple[bytes, str]:
    if not image_path:
        raise RuntimeError("Empty Stash image path")
    url = image_path
    if url.startswith("/"):
        url = stash_base + url

    headers: Dict[str, str] = {"Accept": "image/*,*/*"}
    if cookie:
        headers["Cookie"] = cookie

    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.content or b""
    ct = (r.headers.get("Content-Type") or "").split(";")[0].lower().strip()
    head = data[:256].lstrip().lower()
    if ct.startswith("text/html") or head.startswith(b"<!doctype") or head.startswith(b"<html"):
        raise RuntimeError("Stash returned HTML instead of image. Check plugin session/cookie/auth.")
    if data.startswith(b"\xff\xd8\xff"):
        return data, "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return data, "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return data, "image/webp"
    if is_svg_payload(data, ct):
        return data, "image/svg+xml"
    if ct in ("image/jpeg", "image/jpg", "image/png", "image/webp", "image/svg+xml"):
        return data, "image/jpeg" if ct == "image/jpg" else ct
    raise RuntimeError(f"Unrecognized image payload from Stash (Content-Type={ct})")


# -------------------------
# Jellyfin helpers
# -------------------------

def jellyfin_headers(api_key: str) -> Dict[str, str]:
    mb = (
        'MediaBrowser '
        'Client="Jellyfin%20Web", '
        'Device="Stash", '
        'DeviceId="stash-jellyfin-cover-generator", '
        f'Version="{PLUGIN_VERSION}", '
        f'Token="{api_key}"'
    )
    return {
        "X-Emby-Token": api_key,
        "X-Emby-Authorization": mb,
        "Authorization": mb,
        "Accept": "application/json",
        "User-Agent": f"StashJellyfinCoverGenerator/{PLUGIN_VERSION}",
    }


def jf_request(method: str, jf_url: str, api_key: str, path: str, timeout: int, **kwargs: Any) -> requests.Response:
    url = f"{jf_url.rstrip('/')}{path}"
    headers = {**jellyfin_headers(api_key), **(kwargs.pop("headers", {}) or {})}
    return requests.request(method, url, headers=headers, timeout=timeout, **kwargs)


def jf_get(jf_url: str, api_key: str, path: str, timeout: int, params: Optional[Dict[str, Any]] = None) -> Any:
    r = jf_request("GET", jf_url, api_key, path, timeout, params=params or {})
    r.raise_for_status()
    return r.json()


def jf_pick_user_id(jf_url: str, api_key: str, timeout: int) -> str:
    try:
        users = jf_get(jf_url, api_key, "/Users", timeout)
        if not isinstance(users, list):
            return ""
        for u in users:
            if isinstance(u, dict) and (u.get("Policy") or {}).get("IsAdministrator"):
                return str(u.get("Id") or "")
        if users and isinstance(users[0], dict):
            return str(users[0].get("Id") or "")
    except Exception as e:
        log("warning", f"Cannot auto-pick Jellyfin user id: {e}")
    return ""


def extract_jellyfin_item_id_from_urls(urls: List[str]) -> str:
    for u in urls or []:
        s = str(u or "").strip()
        if not s:
            continue
        # Internal marker used by existing/similar plugins.
        m = re.search(r"jellyfin/items/([0-9a-fA-F]{32})", s)
        if m:
            return m.group(1)

        # Jellyfin web UI usually stores item id in query string or hash fragment.
        try:
            p = urlparse(s)
            for part in (p.query, p.fragment):
                if not part:
                    continue
                qpart = str(part).replace("#!", "").strip()
                # Jellyfin Web URLs commonly look like:
                #   /web/index.html#!/details?id=<ItemId>&serverId=<ServerId>
                # urlparse() puts "!/details?id=..." into fragment, so keep only
                # the actual query string after the first question mark.
                if "?" in qpart:
                    qpart = qpart.split("?", 1)[1]
                qpart = qpart.lstrip("?")
                qs = parse_qs(qpart)
                for key in ("id", "itemId", "itemid", "itemID"):
                    for v in qs.get(key, []):
                        vv = str(v or "")
                        if HEX_ID_RE.fullmatch(vv):
                            return vv
        except Exception:
            pass

        # Generic fallback: exactly one 32-char hex id in the URL.
        found = HEX_ID_RE.findall(s)
        if len(found) == 1:
            return found[0]
    return ""


def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("…", "...")
    s = re.sub(r"\.{3,}", "...", s)
    s = s.translate(str.maketrans({
        "“": '"', "”": '"', "„": '"', "‟": '"',
        "‘": "'", "’": "'", "‚": "'", "‛": "'",
        "—": "-", "–": "-", "\u00A0": " ",
    }))
    return re.sub(r"\s+", " ", s)


def strip_quality_suffix(name: str) -> str:
    s = os.path.splitext(name or "")[0].strip()
    s = re.sub(r"\s*-\s*\[[^\]]+\]\s*$", "", s)
    s = re.sub(r"\s*\[[^\]]+\]\s*$", "", s)
    return re.sub(r"\s+", " ", s).strip()


def title_variants(title: str, path: str) -> List[str]:
    out: List[str] = []

    def add(x: str) -> None:
        x = (x or "").strip()
        if x and x not in out:
            out.append(x)

    add(title or "")
    fn = _basename_no_ext(path)
    add(fn)
    add(strip_quality_suffix(fn))
    add(strip_quality_suffix(title or ""))
    return out


def choose_best_jf_item(items: List[Dict[str, Any]], jellyfin_path: str, scene_title: str) -> Optional[Dict[str, Any]]:
    if not items:
        return None
    jp = norm_text(jellyfin_path)
    target_bn = norm_text(_basename_no_ext(jellyfin_path))
    title = norm_text(scene_title)

    exact_path: List[Dict[str, Any]] = []
    exact_name: List[Dict[str, Any]] = []
    loose: List[Dict[str, Any]] = []

    for it in items:
        path = norm_text(str(it.get("Path") or it.get("path") or ""))
        name = norm_text(str(it.get("Name") or it.get("name") or ""))
        bn = norm_text(_basename_no_ext(path))
        if jp and path and jp == path:
            exact_path.append(it)
        elif target_bn and (name == target_bn or bn == target_bn):
            exact_name.append(it)
        elif title and name == title:
            exact_name.append(it)
        elif target_bn and name and (target_bn in name or name in target_bn):
            loose.append(it)
        elif title and name and (title in name or name in title):
            loose.append(it)

    for bucket in (exact_path, exact_name, loose):
        if bucket:
            return bucket[0]
    return items[0] if len(items) == 1 else None


def jf_search_items(jf_url: str, api_key: str, user_id: str, search_term: str, timeout: int, limit: int = 25) -> List[Dict[str, Any]]:
    if not search_term:
        return []
    params: Dict[str, Any] = {
        "Recursive": "true",
        "IncludeItemTypes": "Video",
        "SearchTerm": search_term,
        "Fields": "Path,PremiereDate",
        "Limit": str(limit),
    }
    paths = []
    if user_id:
        paths.append(f"/Users/{user_id}/Items")
    paths.append("/Items")

    for path in paths:
        try:
            data = jf_get(jf_url, api_key, path, timeout, params=params)
            items = data.get("Items") if isinstance(data, dict) else []
            if items:
                return items
        except Exception as e:
            log("debug", f"Jellyfin search via {path} failed for '{search_term}': {e}")
    return []


def resolve_jellyfin_item_id(
    jf_url: str,
    api_key: str,
    user_id: str,
    scene: Dict[str, Any],
    jellyfin_path: str,
    timeout: int,
) -> str:
    from_urls = extract_jellyfin_item_id_from_urls(scene.get("urls") or [])
    if from_urls:
        log("debug", f"Resolved Jellyfin item id from scene.urls: {from_urls}")
        return from_urls

    title = str(scene.get("title") or "")
    for term in title_variants(title, jellyfin_path):
        items = jf_search_items(jf_url, api_key, user_id, term, timeout)
        if not items:
            continue
        best = choose_best_jf_item(items, jellyfin_path, title)
        if best and best.get("Id"):
            item_id = str(best.get("Id"))
            log("debug", f"Resolved Jellyfin item id by search '{term}': {item_id}")
            return item_id
        log("warning", f"Jellyfin search for '{term}' returned {len(items)} candidates, but none could be safely selected.")

    raise RuntimeError("Could not resolve matching Jellyfin item. Add a Jellyfin URL to scene.urls or check path/title matching settings.")


def jf_upload_primary_image(jf_url: str, api_key: str, item_id: str, content: bytes, content_type: str, timeout: int) -> None:
    # Jellyfin's SetItemImage endpoint expects the body as base64 text, while the
    # Content-Type remains image/jpeg or image/png.
    payload = base64.b64encode(content)
    url_path = f"/Items/{item_id}/Images/Primary"
    base_headers = {"Content-Type": content_type, "Accept": "application/json"}

    attempts: List[Tuple[str, Dict[str, str], Dict[str, Any]]] = [
        ("headers", base_headers, {}),
        ("headers+api_key", base_headers, {"api_key": api_key}),
        ("api_key_only", {"Content-Type": content_type, "Accept": "application/json"}, {"api_key": api_key}),
    ]
    last_resp: Optional[requests.Response] = None
    last_exc: Optional[Exception] = None
    for label, hdrs, params in attempts:
        try:
            if label == "api_key_only":
                r = requests.post(f"{jf_url.rstrip('/')}{url_path}", headers=hdrs, params=params, data=payload, timeout=timeout)
            else:
                r = jf_request("POST", jf_url, api_key, url_path, timeout, headers=hdrs, params=params or None, data=payload)
            last_resp = r
            r.raise_for_status()
            return
        except Exception as e:
            last_exc = e
            if last_resp is not None and 400 <= last_resp.status_code < 500:
                break

    body = ""
    status = ""
    if last_resp is not None:
        status = f"HTTP {last_resp.status_code}"
        body = (last_resp.text or "").strip()[:2000]
    raise RuntimeError(f"Jellyfin Primary image upload failed ({status}): {last_exc}. Response body: {body}")


# -------------------------
# Image processing
# -------------------------

def load_image(data: bytes) -> Image.Image:
    im = Image.open(io.BytesIO(data))
    im.load()
    return im.convert("RGB")


def is_svg_payload(data: bytes, content_type: str = "") -> bool:
    """Detect SVG from Content-Type or payload head.

    Stash can store studio logos as SVG and return them from /studio/<id>/image.
    Pillow cannot decode SVG directly, so we detect it before normal image load.
    """
    ct = (content_type or "").split(";")[0].lower().strip()
    if ct in {"image/svg+xml", "image/svg"}:
        return True
    head = (data or b"")[:4096].lstrip().lower()
    if head.startswith(b"<?xml") and b"<svg" in head:
        return True
    if head.startswith(b"<svg") or b"<svg" in head[:1024]:
        return True
    return False


def _svg_to_png_bytes(data: bytes, output_width: int = 1600, output_height: Optional[int] = None) -> bytes:
    """Render SVG bytes to PNG bytes using CairoSVG.

    output_width gives enough resolution for a 400px overlay after resizing while
    keeping memory usage reasonable. If CairoSVG or system cairo dependencies are
    unavailable, the caller gets a clear RuntimeError and the plugin can fall
    back/log the reason.
    """
    try:
        import cairosvg  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "SVG logo detected, but CairoSVG is not installed in the Python environment used by Stash. "
            "Install plugin requirements again: python3 -m pip install -r requirements.txt"
        ) from e

    kwargs: Dict[str, Any] = {"bytestring": data, "output_width": max(64, int(output_width))}
    if output_height is not None:
        kwargs["output_height"] = max(64, int(output_height))
    try:
        return cairosvg.svg2png(**kwargs)
    except Exception as e:
        raise RuntimeError(f"SVG logo conversion failed: {e}") from e


def load_logo_with_info(data: bytes) -> Tuple[Image.Image, Dict[str, Any]]:
    info: Dict[str, Any] = {"input_bytes": len(data or b"")}
    if is_svg_payload(data):
        info["format"] = "svg"
        png = _svg_to_png_bytes(data, output_width=1600)
        info["svg_converted_to_png_bytes"] = len(png)
        im = Image.open(io.BytesIO(png))
        im.load()
        out = im.convert("RGBA")
        info["decoded_size"] = list(out.size)
        info["converted"] = "svg_to_png"
        return out, info

    im = Image.open(io.BytesIO(data))
    im.load()
    out = im.convert("RGBA")
    info["format"] = (getattr(im, "format", None) or "raster").lower()
    info["decoded_size"] = list(out.size)
    return out, info


def load_logo(data: bytes) -> Image.Image:
    im, _ = load_logo_with_info(data)
    return im


def iter_studio_logo_candidates(studio: Dict[str, Any]) -> List[Dict[str, str]]:
    """Return current studio and up to two parent studios as logo candidates."""
    candidates: List[Dict[str, str]] = []
    current: Any = studio if isinstance(studio, dict) else {}
    seen: set = set()
    depth = 0
    while isinstance(current, dict) and depth < 3:
        sid = str(current.get("id") or "")
        name = str(current.get("name") or "")
        image_path = str(current.get("image_path") or "").strip()
        key = sid or name or str(depth)
        if key not in seen:
            candidates.append({
                "source": "studio_image" if depth == 0 else f"parent_studio_image_{depth}",
                "studio_id": sid,
                "studio_name": name,
                "image_path": image_path,
            })
            seen.add(key)
        current = current.get("parent_studio")
        depth += 1
    return candidates


def fetch_jellyfin_item_image(
    jf_url: str,
    api_key: str,
    item_id: str,
    image_type: str,
    timeout: int,
) -> Tuple[bytes, str]:
    """Fetch a Jellyfin item image, for example Logo. Raises on 404/empty."""
    r = jf_request(
        "GET",
        jf_url,
        api_key,
        f"/Items/{item_id}/Images/{image_type}",
        timeout,
        headers={"Accept": "image/*,*/*"},
    )
    r.raise_for_status()
    data = r.content or b""
    if not data:
        raise RuntimeError(f"Jellyfin image {image_type} response is empty")
    return data, r.headers.get("Content-Type", "application/octet-stream")


def _box_iou(a: Tuple[int, int, int, int], b: Tuple[int, int, int, int]) -> float:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    ax2, ay2 = ax + aw, ay + ah
    bx2, by2 = bx + bw, by + bh
    ix1, iy1 = max(ax, bx), max(ay, by)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    iw, ih = max(0, ix2 - ix1), max(0, iy2 - iy1)
    inter = float(iw * ih)
    union = float(max(1, aw * ah + bw * bh - inter))
    return inter / union


def _dedupe_faces(faces: List[Tuple[int, int, int, int]], iou_threshold: float = 0.28) -> List[Tuple[int, int, int, int]]:
    # Keep larger boxes first, drop near-duplicates found by several cascades.
    ordered = sorted(faces, key=lambda f: f[2] * f[3], reverse=True)
    kept: List[Tuple[int, int, int, int]] = []
    for face in ordered:
        if all(_box_iou(face, old) < iou_threshold for old in kept):
            kept.append(face)
    return kept


def detect_faces_opencv(im: Image.Image) -> List[Tuple[int, int, int, int]]:
    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore
    except Exception as e:
        log("debug", f"OpenCV is not available; face-aware crop is disabled for this run: {e}")
        return []

    try:
        required_attrs = ("CascadeClassifier", "cvtColor", "COLOR_RGB2GRAY", "equalizeHist")
        missing = [name for name in required_attrs if not hasattr(cv2, name)]
        if missing:
            cv2_file = getattr(cv2, "__file__", "unknown")
            log(
                "debug",
                "Imported module 'cv2' is not a full OpenCV build "
                f"(missing: {', '.join(missing)}; file: {cv2_file}). "
                "Install/reinstall opencv-python-headless in the same Python environment used by Stash. "
                "Using smart-crop fallback for this run.",
            )
            return []

        data_obj = getattr(cv2, "data", None)
        haar_dir = getattr(data_obj, "haarcascades", "") if data_obj is not None else ""

        cascade_specs: List[Tuple[str, str, bool]] = []
        env_cascade = _s(os.environ.get("JCG_HAAR_CASCADE"))
        if env_cascade:
            cascade_specs.append(("custom", env_cascade, False))
        if haar_dir:
            for label, filename, mirrored in (
                ("frontal_default", "haarcascade_frontalface_default.xml", False),
                ("frontal_alt2", "haarcascade_frontalface_alt2.xml", False),
                ("frontal_alt", "haarcascade_frontalface_alt.xml", False),
                ("profile", "haarcascade_profileface.xml", False),
                ("profile_mirror", "haarcascade_profileface.xml", True),
            ):
                cascade_specs.append((label, os.path.join(haar_dir, filename), mirrored))

        existing_specs = [(label, path, mirrored) for label, path, mirrored in cascade_specs if path and os.path.exists(path)]
        if not existing_specs:
            log("debug", "No usable OpenCV Haar cascade file was found. Using smart-crop fallback for this run.")
            return []

        rgb = im.convert("RGB")
        arr = np.array(rgb)
        gray0 = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
        gray_eq = cv2.equalizeHist(gray0)
        gray_inputs: List[Tuple[str, Any]] = [("eq", gray_eq), ("plain", gray0)]
        if hasattr(cv2, "createCLAHE"):
            try:
                clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
                gray_inputs.insert(0, ("clahe", clahe.apply(gray0)))
            except Exception:
                pass

        flags = getattr(cv2, "CASCADE_SCALE_IMAGE", 0)
        img_w, img_h = im.size
        min_side_values = [
            max(18, int(min(img_w, img_h) * 0.028)),
            max(22, int(min(img_w, img_h) * 0.040)),
            max(28, int(min(img_w, img_h) * 0.060)),
        ]
        param_sets = [
            (1.04, 3),
            (1.06, 3),
            (1.08, 3),
            (1.10, 4),
        ]

        # Search several ROIs. Faces on scene covers are often small and off-center;
        # running cascades on overlapping regions effectively enlarges them.
        roi_specs: List[Tuple[str, Tuple[int, int, int, int]]] = [
            ("full", (0, 0, img_w, img_h)),
            ("upper", (0, 0, img_w, max(1, int(img_h * 0.82)))),
            ("center", (max(0, int(img_w * 0.14)), 0, max(1, int(img_w * 0.72)), img_h)),
            ("left", (0, 0, max(1, int(img_w * 0.72)), img_h)),
            ("right", (max(0, int(img_w * 0.28)), 0, max(1, int(img_w * 0.72)), img_h)),
            ("upper_left", (0, 0, max(1, int(img_w * 0.68)), max(1, int(img_h * 0.82)))),
            ("upper_right", (max(0, int(img_w * 0.32)), 0, max(1, int(img_w * 0.68)), max(1, int(img_h * 0.82)))),
        ]
        scale_values = [1.0, 1.35, 1.7]

        found: List[Tuple[int, int, int, int]] = []
        for label, cascade_path, mirrored in existing_specs:
            cascade = cv2.CascadeClassifier(cascade_path)
            if cascade.empty():
                continue
            for gray_label, gray in gray_inputs:
                for roi_label, (rx, ry, rw, rh) in roi_specs:
                    sub = gray[ry:ry + rh, rx:rx + rw]
                    if sub is None or getattr(sub, 'size', 0) == 0:
                        continue
                    for scale in scale_values:
                        if scale != 1.0:
                            detect_gray = cv2.resize(sub, (max(1, int(round(rw * scale))), max(1, int(round(rh * scale)))), interpolation=cv2.INTER_LINEAR)
                        else:
                            detect_gray = sub
                        if mirrored and hasattr(cv2, "flip"):
                            detect_gray = cv2.flip(detect_gray, 1)
                        det_h, det_w = detect_gray.shape[:2]
                        for min_side in min_side_values:
                            scaled_min = max(14, int(round(min_side * scale)))
                            if scaled_min >= min(det_w, det_h):
                                continue
                            for scale_factor, min_neighbors in param_sets:
                                faces = cascade.detectMultiScale(
                                    detect_gray,
                                    scaleFactor=scale_factor,
                                    minNeighbors=min_neighbors,
                                    minSize=(scaled_min, scaled_min),
                                    flags=flags,
                                )
                                for (x, y, fw, fh) in faces:
                                    x, y, fw, fh = int(x), int(y), int(fw), int(fh)
                                    if mirrored:
                                        x = det_w - x - fw
                                    # Map back from scaled ROI to original image coordinates.
                                    if scale != 1.0:
                                        x = int(round(x / scale))
                                        y = int(round(y / scale))
                                        fw = int(round(fw / scale))
                                        fh = int(round(fh / scale))
                                    x += rx
                                    y += ry
                                    if fw <= 0 or fh <= 0:
                                        continue
                                    if x < 0 or y < 0 or x + fw > img_w or y + fh > img_h:
                                        # Clamp mildly rather than dropping; detectors near ROI edges can overshoot a little.
                                        x = max(0, min(x, img_w - 1))
                                        y = max(0, min(y, img_h - 1))
                                        fw = min(fw, img_w - x)
                                        fh = min(fh, img_h - y)
                                    # Drop very unlikely huge false positives.
                                    if fw * fh > img_w * img_h * 0.45:
                                        continue
                                    # Ignore boxes that are too low in the frame or absurdly thin/wide.
                                    aspect = fw / max(1.0, fh)
                                    cy = (y + fh / 2.0) / max(1, img_h)
                                    if cy > 0.88 or aspect < 0.45 or aspect > 1.65:
                                        continue
                                    found.append((x, y, fw, fh))

        out = _dedupe_faces(found)
        out.sort(key=lambda f: f[2] * f[3], reverse=True)
        if out:
            log("debug", f"OpenCV face detection found {len(out)} face candidate(s) after dedupe; raw={len(found)}; mode=multi-roi-upscale")
        else:
            log("debug", "OpenCV face detection found no faces after multi-ROI cascades; using saliency fallback.")
        return out
    except Exception as e:
        log("debug", f"OpenCV face detection failed; using smart-crop fallback: {e}")
        return []


def detect_eye_focal_opencv(im: Image.Image) -> Optional[Tuple[float, float, str]]:
    """Weak fallback when face cascades miss a visible face.

    Haar face detection often misses side-lit, partially visible, or tilted faces.
    Eye cascades are not reliable enough to be the primary detector, but when a
    plausible pair of eyes is found they are a better crop anchor than a generic
    saliency cluster.  We keep this conservative and return None when evidence is
    weak so the normal saliency fallback can still run.
    """
    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore
    except Exception:
        return None

    try:
        data_obj = getattr(cv2, "data", None)
        haar_dir = getattr(data_obj, "haarcascades", "") if data_obj is not None else ""
        if not haar_dir:
            return None

        specs = []
        for label, filename in (
            ("eye", "haarcascade_eye.xml"),
            ("eye_tree", "haarcascade_eye_tree_eyeglasses.xml"),
        ):
            path = os.path.join(haar_dir, filename)
            if os.path.exists(path):
                specs.append((label, path))
        if not specs:
            return None

        rgb = im.convert("RGB")
        arr = np.array(rgb)
        gray0 = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
        gray_inputs: List[Tuple[str, Any]] = [("eq", cv2.equalizeHist(gray0)), ("plain", gray0)]
        if hasattr(cv2, "createCLAHE"):
            try:
                clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
                gray_inputs.insert(0, ("clahe", clahe.apply(gray0)))
            except Exception:
                pass

        w, h = im.size
        min_side_values = [
            max(8, int(min(w, h) * 0.012)),
            max(10, int(min(w, h) * 0.018)),
            max(14, int(min(w, h) * 0.025)),
        ]
        param_sets = [(1.05, 3), (1.08, 4), (1.12, 5)]
        flags = getattr(cv2, "CASCADE_SCALE_IMAGE", 0)
        eyes: List[Tuple[int, int, int, int]] = []
        for label, path in specs:
            cascade = cv2.CascadeClassifier(path)
            if cascade.empty():
                continue
            for gray_label, gray in gray_inputs:
                for min_side in min_side_values:
                    for scale_factor, min_neighbors in param_sets:
                        found = cascade.detectMultiScale(
                            gray,
                            scaleFactor=scale_factor,
                            minNeighbors=min_neighbors,
                            minSize=(min_side, min_side),
                            flags=flags,
                        )
                        for (x, y, ew, eh) in found:
                            x, y, ew, eh = int(x), int(y), int(ew), int(eh)
                            # Eyes should not cover too much of the whole image and
                            # should normally be in the upper/middle part of a poster source.
                            if ew * eh > w * h * 0.035:
                                continue
                            if (y + eh / 2.0) / max(1, h) > 0.78:
                                continue
                            eyes.append((x, y, ew, eh))

        eyes = _dedupe_faces(eyes, iou_threshold=0.20)
        if len(eyes) < 2:
            return None

        pairs: List[Tuple[float, Tuple[int, int, int, int], Tuple[int, int, int, int], Dict[str, Any]]] = []
        for i in range(len(eyes)):
            for j in range(i + 1, len(eyes)):
                a = eyes[i]
                b = eyes[j]
                ax, ay, aw, ah = a
                bx, by, bw, bh = b
                acx, acy = ax + aw / 2.0, ay + ah / 2.0
                bcx, bcy = bx + bw / 2.0, by + bh / 2.0
                if acx > bcx:
                    a, b = b, a
                    ax, ay, aw, ah = a
                    bx, by, bw, bh = b
                    acx, acy = ax + aw / 2.0, ay + ah / 2.0
                    bcx, bcy = bx + bw / 2.0, by + bh / 2.0

                avg_eye = max(1.0, (aw + ah + bw + bh) / 4.0)
                dx = bcx - acx
                dy = abs(bcy - acy)
                # Plausible eye pair geometry.  This intentionally rejects many
                # false positives and only accepts strong pair evidence.
                if dx < avg_eye * 0.75 or dx > avg_eye * 6.5:
                    continue
                if dy > avg_eye * 1.15:
                    continue
                size_similarity = min(aw * ah, bw * bh) / max(1.0, max(aw * ah, bw * bh))
                if size_similarity < 0.28:
                    continue

                pair_cx = (acx + bcx) / 2.0
                pair_cy = (acy + bcy) / 2.0
                centrality = 1.0 - min(1.0, abs((pair_cx / max(1, w)) - 0.5) / 0.5)
                score = (aw * ah + bw * bh) * (0.75 + 0.25 * size_similarity) * (0.85 + 0.15 * centrality)
                info = {
                    "eyes_detected": len(eyes),
                    "left_eye": [int(ax), int(ay), int(aw), int(ah)],
                    "right_eye": [int(bx), int(by), int(bw), int(bh)],
                    "pair_center": [round(pair_cx / max(1, w), 4), round(pair_cy / max(1, h), 4)],
                    "size_similarity": round(size_similarity, 4),
                    "score": round(score, 2),
                }
                pairs.append((score, a, b, info))

        if not pairs:
            return None
        pairs.sort(key=lambda item: item[0], reverse=True)
        _, a, b, info = pairs[0]
        ax, ay, aw, ah = a
        bx, by, bw, bh = b
        pair_cx = (ax + aw / 2.0 + bx + bw / 2.0) / 2.0
        pair_cy = (ay + ah / 2.0 + by + bh / 2.0) / 2.0
        avg_eye_h = (ah + bh) / 2.0
        fx = max(0.0, min(1.0, pair_cx / max(1, w)))
        # Anchor below the eyes so the full face is kept, but still keep eyes high.
        fy = max(0.0, min(1.0, (pair_cy + avg_eye_h * 1.55) / max(1, h)))
        info["selection"] = "eye-pair-fallback"
        info["top_pairs"] = [entry[3] for entry in pairs[:3]]
        return fx, fy, f"eye-pair-selected; eye_info={json.dumps(info, ensure_ascii=False)}"
    except Exception as e:
        log("debug", f"Eye-pair fallback failed; continuing to saliency fallback: {e}")
        return None


def fallback_saliency_point(im: Image.Image) -> Tuple[float, float, str]:
    """Return approximate focal point when no face is detected.

    This is deliberately not a plain center crop.  If OpenCV misses the faces,
    a normal center-biased saliency crop can cut two side-by-side faces.  We
    instead build an edge/contrast map in the upper/middle portion of the frame,
    find the strongest broad cluster, and anchor the poster crop there.
    """
    w, h = im.size
    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore

        small_w = 360
        small_h = max(1, int(h * small_w / max(1, w)))
        small_rgb = im.resize((small_w, small_h), RESAMPLE_LANCZOS).convert("RGB")
        arr_rgb = np.array(small_rgb)
        gray = cv2.cvtColor(arr_rgb, cv2.COLOR_RGB2GRAY)

        # Combine edges and local contrast.  Faces, hair, eyes and bodies tend to
        # produce dense detail clusters; flat backgrounds produce weaker scores.
        edges = np.abs(cv2.Laplacian(gray, cv2.CV_64F))
        blur = cv2.GaussianBlur(gray, (0, 0), 3)
        contrast = np.abs(gray.astype("float32") - blur.astype("float32"))
        score = edges.astype("float32") * 0.70 + contrast.astype("float32") * 0.30

        yy, xx = np.mgrid[0:small_h, 0:small_w]
        yn = yy / max(1, small_h - 1)
        # Prefer the upper/middle part where faces normally are, but do not add a
        # strong horizontal center bias.  This lets the crop choose one side when
        # two people/faces sit left and right of center.
        upper_mid = np.exp(-(((yn - 0.34) ** 2) / (2 * 0.24 ** 2)))
        lower_penalty = np.clip((yn - 0.72) / 0.28, 0.0, 1.0)
        score = score * (0.35 + upper_mid) * (1.0 - 0.55 * lower_penalty)
        score = cv2.GaussianBlur(score, (0, 0), 9)

        if float(np.max(score)) <= 0.0:
            return (0.5, 0.42, "saliency-empty-center")

        # Pick a broad horizontal cluster, not just a single edge pixel.
        x_profile = score.sum(axis=0).astype("float32")
        x_profile = cv2.GaussianBlur(x_profile.reshape(1, -1), (0, 0), 7).reshape(-1)
        sx = int(np.argmax(x_profile))

        # Now pick vertical focus inside a band around that horizontal cluster.
        band_half = max(8, int(small_w * 0.11))
        x1 = max(0, sx - band_half)
        x2 = min(small_w, sx + band_half + 1)
        y_profile = score[:, x1:x2].sum(axis=1).astype("float32")
        y_profile = cv2.GaussianBlur(y_profile.reshape(-1, 1), (0, 0), 5).reshape(-1)
        sy = int(np.argmax(y_profile))

        raw_fx = max(0.0, min(1.0, sx / max(1, small_w - 1)))
        raw_fy = max(0.0, min(1.0, sy / max(1, small_h - 1)))

        # Saliency is only a weak proxy for a face.  In very wide source images a
        # wrong saliency peak can push the portrait crop far to one side, making a
        # real face sit on the poster edge.  Blend the horizontal saliency anchor
        # back toward the center and clamp it to a conservative range.  Real face
        # detections and eye-pair detections above are not affected by this.
        center_blend = 0.50
        fx = 0.5 + (raw_fx - 0.5) * center_blend
        fx = max(0.36, min(0.64, fx))
        fy = raw_fy
        # If the saliency peak is too low, pull it back to a safer face-like zone.
        fy = min(fy, 0.58)
        return (fx, fy, f"saliency-cluster-centered; cluster=({sx},{sy}); band=({x1},{x2}); raw_focal=({raw_fx:.3f},{raw_fy:.3f}); center_blend={center_blend}")
    except Exception as e:
        log("debug", f"Saliency fallback failed; using center/upper-middle focal point: {e}")
        return (0.5, 0.42, "saliency-failed-center")


def choose_primary_face(faces: List[Tuple[int, int, int, int]], image_size: Tuple[int, int]) -> Tuple[Tuple[int, int, int, int], Dict[str, Any]]:
    """Pick one face to use as the crop anchor.

    Older versions averaged several faces, which often pulled the focal point back
    to the image center when two or more faces were detected.  For poster covers
    it is usually better to follow one dominant face.  The score strongly favors
    face size (a practical foreground proxy), then uses centrality and a small
    upper/middle preference as tie breakers.
    """
    w, h = image_size
    if not faces:
        raise ValueError("choose_primary_face called with no faces")

    img_area = float(max(1, w * h))
    scored: List[Tuple[float, Tuple[int, int, int, int], Dict[str, Any]]] = []
    for idx, (x, y, fw, fh) in enumerate(faces):
        area = float(max(1, fw * fh))
        cx = (x + fw / 2.0) / max(1, w)
        cy = (y + fh / 2.0) / max(1, h)
        area_ratio = area / img_area
        # Area dominates.  A larger face is normally closer to the camera / more
        # important.  Centrality and vertical placement only break close ties.
        centrality = 1.0 - min(1.0, (((cx - 0.5) / 0.5) ** 2 + ((cy - 0.42) / 0.58) ** 2) ** 0.5)
        score = area * (1.0 + 0.18 * centrality)
        info = {
            "index": idx,
            "box": [int(x), int(y), int(fw), int(fh)],
            "center": [round(cx, 4), round(cy, 4)],
            "area_ratio": round(area_ratio, 6),
            "centrality": round(centrality, 4),
            "score": round(score, 2),
        }
        scored.append((score, (x, y, fw, fh), info))

    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best_face, best_info = scored[0]
    best_info["detected_faces"] = len(faces)
    best_info["selection"] = "largest-face-with-centrality-tiebreak"
    # Keep the top few candidates in the log/debug string for troubleshooting.
    best_info["top_candidates"] = [entry[2] for entry in scored[:5]]
    return best_face, best_info


def face_focal_point(im: Image.Image) -> Tuple[float, float, str]:
    """Saliency-only crop anchor.

    This build intentionally disables face and eye detection.  It always uses the
    v0.2.3 `saliency-cluster-centered` algorithm so generated covers are stable
    and do not switch between face/eye/saliency behavior from scene to scene.
    """
    fx, fy, method = fallback_saliency_point(im)
    if method.startswith("saliency-cluster-centered"):
        method += "; face_detection=disabled"
    else:
        method += "; saliency_only=true; face_detection=disabled"
    return fx, fy, method


def smart_crop_resize(im: Image.Image, out_w: int, out_h: int) -> Tuple[Image.Image, str]:
    w, h = im.size
    target_aspect = out_w / out_h
    src_aspect = w / h
    fx, fy, method = face_focal_point(im)

    # For portrait posters, faces look better slightly above the vertical center.
    desired_x = 0.50
    desired_y = 0.38 if out_h >= out_w else 0.50

    if src_aspect > target_aspect:
        crop_h = h
        crop_w = int(round(crop_h * target_aspect))
        focal_x = fx * w
        left = int(round(focal_x - crop_w * desired_x))
        left = max(0, min(left, w - crop_w))
        top = 0
    else:
        crop_w = w
        crop_h = int(round(crop_w / target_aspect))
        focal_y = fy * h
        top = int(round(focal_y - crop_h * desired_y))
        top = max(0, min(top, h - crop_h))
        left = 0

    cropped = im.crop((left, top, left + crop_w, top + crop_h))
    resized = cropped.resize((out_w, out_h), RESAMPLE_LANCZOS)
    return resized, f"{method}; crop=({left},{top},{crop_w},{crop_h}); focal=({fx:.3f},{fy:.3f})"


def _alpha_bbox(im: Image.Image, threshold: int = 8) -> Optional[Tuple[int, int, int, int]]:
    alpha = im.convert("RGBA").split()[-1]
    mask = alpha.point(lambda px: 255 if px > threshold else 0)
    return mask.getbbox()


def trim_logo_transparent_padding(logo: Image.Image) -> Tuple[Optional[Image.Image], Dict[str, Any]]:
    """Trim transparent borders so the visible logo really gets the requested width.

    A common studio-image problem is a PNG logo inside a large transparent canvas.
    If we resize the whole canvas to 50% of the poster width, the real visible logo
    can become tiny and look like it is missing. Trimming fixes that while keeping
    the logo proportions.
    """
    im = logo.convert("RGBA")
    w, h = im.size
    bbox = _alpha_bbox(im)
    info: Dict[str, Any] = {"input_size": [w, h], "alpha_bbox": list(bbox) if bbox else None}
    if not bbox:
        info["reason"] = "logo_has_no_visible_alpha_pixels"
        return None, info

    left, top, right, bottom = bbox
    visible_w = max(1, right - left)
    visible_h = max(1, bottom - top)

    # Crop only if there is meaningful transparent padding. Keep a small margin
    # so logos do not look cut off.
    margin = max(2, int(round(max(visible_w, visible_h) * 0.04)))
    crop = (
        max(0, left - margin),
        max(0, top - margin),
        min(w, right + margin),
        min(h, bottom + margin),
    )
    should_crop = crop != (0, 0, w, h) and (visible_w < w * 0.92 or visible_h < h * 0.92)
    if should_crop:
        im = im.crop(crop)
        info["trimmed"] = True
        info["trim_crop"] = list(crop)
        info["trimmed_size"] = list(im.size)
    else:
        info["trimmed"] = False
        info["trimmed_size"] = [w, h]
    return im, info


def _font_candidates() -> List[str]:
    return [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
    ]


def _load_font(size: int) -> ImageFont.ImageFont:
    for path in _font_candidates():
        try:
            if os.path.exists(path):
                return ImageFont.truetype(path, size=size)
        except Exception:
            pass
    try:
        return ImageFont.truetype("DejaVuSans-Bold.ttf", size=size)
    except Exception:
        return ImageFont.load_default()


def _text_bbox(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> Tuple[int, int, int, int]:
    try:
        return draw.textbbox((0, 0), text, font=font)
    except Exception:
        w, h = draw.textsize(text, font=font)  # type: ignore[attr-defined]
        return (0, 0, int(w), int(h))


def _wrap_studio_name(draw: ImageDraw.ImageDraw, name: str, font: ImageFont.ImageFont, max_width: int) -> List[str]:
    words = [w for w in re.split(r"\s+", (name or "").strip()) if w]
    if not words:
        return []
    lines: List[str] = []
    current = ""
    for word in words:
        test = word if not current else f"{current} {word}"
        bbox = _text_bbox(draw, test, font)
        if bbox[2] - bbox[0] <= max_width or not current:
            current = test
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines[:2]


def make_text_logo(studio_name: str) -> Optional[Image.Image]:
    name = (studio_name or "").strip()
    if not name:
        return None

    canvas_w = 1200
    max_text_w = 1080
    target_h = 360
    tmp = Image.new("RGBA", (canvas_w, target_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(tmp)

    chosen_font: Optional[ImageFont.ImageFont] = None
    chosen_lines: List[str] = []
    for size in range(230, 52, -8):
        font = _load_font(size)
        lines = _wrap_studio_name(draw, name.upper(), font, max_text_w)
        if not lines:
            continue
        line_boxes = [_text_bbox(draw, line, font) for line in lines]
        text_w = max((b[2] - b[0]) for b in line_boxes)
        line_h = max((b[3] - b[1]) for b in line_boxes)
        text_h = len(lines) * line_h + max(0, len(lines) - 1) * int(size * 0.15)
        if text_w <= max_text_w and text_h <= 280:
            chosen_font = font
            chosen_lines = lines
            break

    if chosen_font is None:
        chosen_font = _load_font(72)
        chosen_lines = _wrap_studio_name(draw, name.upper(), chosen_font, max_text_w) or [name.upper()]

    # Render with a black stroke/shadow and white fill. The add_logo() visibility
    # boost will add an extra glow/shadow around it when pasted on the poster.
    line_boxes = [_text_bbox(draw, line, chosen_font) for line in chosen_lines]
    line_h = max((b[3] - b[1]) for b in line_boxes) if line_boxes else 80
    gap = max(8, int(line_h * 0.18))
    total_h = len(chosen_lines) * line_h + max(0, len(chosen_lines) - 1) * gap
    y = max(10, (target_h - total_h) // 2)

    im = Image.new("RGBA", (canvas_w, target_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(im)
    for line, bbox in zip(chosen_lines, line_boxes):
        tw = bbox[2] - bbox[0]
        x = (canvas_w - tw) // 2
        try:
            draw.text((x, y), line, font=chosen_font, fill=(255, 255, 255, 255), stroke_width=7, stroke_fill=(0, 0, 0, 210))
        except TypeError:
            draw.text((x + 3, y + 3), line, font=chosen_font, fill=(0, 0, 0, 210))
            draw.text((x, y), line, font=chosen_font, fill=(255, 255, 255, 255))
        y += line_h + gap

    trimmed, _ = trim_logo_transparent_padding(im)
    return trimmed or im


def add_logo(
    poster: Image.Image,
    logo: Optional[Image.Image],
    logo_width_percent: int,
    logo_max_height_percent: int,
    logo_padding_percent: int,
    trim_transparent_padding: bool = True,
    visibility_boost: bool = True,
    background_opacity: int = 0,
    logo_source: str = "studio_image",
) -> Tuple[Image.Image, Dict[str, Any]]:
    base = poster.convert("RGBA")
    info: Dict[str, Any] = {"applied": False, "source": logo_source}
    if logo is None:
        info["reason"] = "no_logo_image"
        return base.convert("RGB"), info

    out_w, out_h = base.size
    logo = logo.convert("RGBA")
    info["input_size"] = list(logo.size)

    if trim_transparent_padding:
        trimmed, trim_info = trim_logo_transparent_padding(logo)
        info.update({f"trim_{k}": v for k, v in trim_info.items()})
        if trimmed is None:
            info["reason"] = trim_info.get("reason") or "logo_has_no_visible_pixels"
            log("warning", f"Studio logo was loaded but has no visible pixels; source={logo_source}. Continuing without logo.")
            return base.convert("RGB"), info
        logo = trimmed

    lw, lh = logo.size
    if lw <= 0 or lh <= 0:
        info["reason"] = "invalid_logo_size"
        return base.convert("RGB"), info

    target_w = max(1, int(round(out_w * max(1, logo_width_percent) / 100.0)))

    # v0.1.2+: logo sizing is width-driven. Height is always proportional
    # to the original logo aspect ratio. The max-height argument is kept
    # only as a safety guard for unusual very tall logos.
    scale = target_w / float(lw)
    target_h = max(1, int(round(lh * scale)))

    pad = max(0, int(out_w * max(0, logo_padding_percent) / 100.0))
    usable_h = max(1, out_h - pad * 2)
    if target_h > usable_h:
        log("warning", f"Studio logo is unusually tall after proportional scaling ({target_w}x{target_h}); reducing to fit poster height.")
        scale = usable_h / float(lh)
        target_h = usable_h
        target_w = max(1, int(round(lw * scale)))

    logo_resized = logo.resize((target_w, target_h), RESAMPLE_LANCZOS)
    alpha = logo_resized.split()[-1]
    bbox = alpha.point(lambda px: 255 if px > 8 else 0).getbbox()
    if not bbox:
        info["reason"] = "resized_logo_has_no_visible_pixels"
        return base.convert("RGB"), info

    shadow_pad = max(4, int(out_w * 0.008))
    layer = Image.new("RGBA", (target_w + shadow_pad * 2, target_h + shadow_pad * 2), (0, 0, 0, 0))

    bg_opacity = max(0, min(180, int(background_opacity or 0)))
    if bg_opacity > 0:
        # Optional contrast plate. Disabled by default, but useful for difficult
        # black/white logos. Use a rounded rectangle when Pillow supports it.
        plate = Image.new("RGBA", layer.size, (0, 0, 0, 0))
        pd = ImageDraw.Draw(plate)
        radius = max(8, int(out_w * 0.018))
        rect = (0, 0, layer.size[0] - 1, layer.size[1] - 1)
        try:
            pd.rounded_rectangle(rect, radius=radius, fill=(0, 0, 0, bg_opacity))
        except Exception:
            pd.rectangle(rect, fill=(0, 0, 0, bg_opacity))
        layer.alpha_composite(plate, (0, 0))

    if visibility_boost:
        blur = max(2, shadow_pad)
        white_glow = ImageEnhance.Brightness(alpha.filter(ImageFilter.GaussianBlur(blur))).enhance(0.55)
        black_shadow = ImageEnhance.Brightness(alpha.filter(ImageFilter.GaussianBlur(blur))).enhance(0.78)
        # White glow first helps dark/black transparent logos on dark posters.
        layer.paste((255, 255, 255, 255), (shadow_pad, shadow_pad), white_glow)
        # Black shadow helps white/light transparent logos on light posters.
        layer.paste((0, 0, 0, 255), (shadow_pad, shadow_pad), black_shadow)

    layer.paste(logo_resized, (shadow_pad, shadow_pad), logo_resized)

    x = max(0, out_w - layer.size[0] - pad)
    y = max(0, out_h - layer.size[1] - pad)
    base.alpha_composite(layer, (x, y))

    info.update({
        "applied": True,
        "trimmed_size": list(logo.size),
        "target_size": [target_w, target_h],
        "layer_size": list(layer.size),
        "position": [x, y],
        "padding_px": pad,
        "visible_bbox_after_resize": list(bbox),
    })
    return base.convert("RGB"), info


def build_poster(
    cover_bytes: bytes,
    logo_bytes: Optional[bytes],
    out_w: int,
    out_h: int,
    logo_width_percent: int,
    logo_max_height_percent: int,
    logo_padding_percent: int,
    quality: int,
    studio_name: str = "",
    logo_source_label: str = "studio_image",
    use_studio_name_when_logo_missing: bool = False,
    logo_trim_transparent_padding: bool = False,
    logo_visibility_boost: bool = False,
    logo_background_opacity: int = 0,
) -> Tuple[bytes, str, str, Dict[str, Any]]:
    source = load_image(cover_bytes)
    poster, crop_info = smart_crop_resize(source, out_w, out_h)

    logo_img: Optional[Image.Image] = None
    logo_source = "none"
    logo_info: Dict[str, Any] = {}

    if logo_bytes:
        try:
            logo_img, decoded_info = load_logo_with_info(logo_bytes)
            logo_source = logo_source_label or "studio_image"
            logo_info.update({f"decode_{k}": v for k, v in decoded_info.items()})
            logo_info["decoded_logo_size"] = list(logo_img.size)
            if decoded_info.get("format") == "svg":
                log("debug", f"Converted SVG logo to raster image for overlay; source={logo_source}, size={logo_img.size[0]}x{logo_img.size[1]}")
        except Exception as e:
            log("warning", f"Studio logo could not be decoded/converted: {e}")
            logo_info["decode_error"] = str(e)
            logo_img = None

    if logo_img is None and use_studio_name_when_logo_missing and (studio_name or "").strip():
        logo_img = make_text_logo(studio_name)
        if logo_img is not None:
            logo_source = "studio_name_fallback"
            log("warning", f"Using studio name text fallback instead of image logo: '{studio_name}'")

    poster, overlay_info = add_logo(
        poster,
        logo_img,
        logo_width_percent,
        logo_max_height_percent,
        logo_padding_percent,
        trim_transparent_padding=logo_trim_transparent_padding,
        visibility_boost=logo_visibility_boost,
        background_opacity=logo_background_opacity,
        logo_source=logo_source,
    )
    logo_info.update(overlay_info)

    buf = io.BytesIO()
    q = max(60, min(98, int(quality)))
    poster.save(buf, format="JPEG", quality=q, optimize=True, progressive=False)
    return buf.getvalue(), "image/jpeg", crop_info, logo_info


def save_debug_copy(plugin_dir: str, scene_id: str, poster_bytes: bytes) -> str:
    out_dir = os.path.join(plugin_dir or ".", "generated")
    os.makedirs(out_dir, exist_ok=True)
    # Save to a stable filename so regenerating a poster overwrites the previous local copy.
    path = os.path.join(out_dir, f"scene_{scene_id}.jpg")
    with open(path, "wb") as f:
        f.write(poster_bytes)
    return path


def generated_folder_path(plugin_dir: str, settings: Dict[str, Any]) -> str:
    custom = _s(settings.get("generated_folder")).strip()
    if custom:
        if os.path.isabs(custom):
            return custom
        return os.path.abspath(os.path.join(plugin_dir or ".", custom))
    return os.path.join(plugin_dir or ".", "generated")


GENERATED_SCENE_FILE_RE = re.compile(r"^scene[_-](\d+)(?:[_-]\d+)?\.(?:jpe?g|png|webp)$", re.IGNORECASE)


def scene_id_from_generated_filename(name: str) -> str:
    m = GENERATED_SCENE_FILE_RE.match(os.path.basename(name or ""))
    return m.group(1) if m else ""


def detect_local_image_mime(data: bytes, filename: str = "") -> str:
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    ext = os.path.splitext(filename or "")[1].lower()
    if ext in (".jpg", ".jpeg"):
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    if ext == ".webp":
        return "image/webp"
    raise RuntimeError("Unsupported local generated image format")


def collect_generated_cover_files(folder: str, limit: int = 0) -> List[Tuple[str, str]]:
    if not os.path.isdir(folder):
        raise RuntimeError(f"Generated folder does not exist: {folder}")

    # Map scene id -> newest matching image. This also handles old names like
    # scene_76005_1783506892.jpg from pre-v0.1.7 versions.
    by_scene: Dict[str, str] = {}
    for name in os.listdir(folder):
        sid = scene_id_from_generated_filename(name)
        if not sid:
            continue
        path = os.path.join(folder, name)
        if not os.path.isfile(path):
            continue
        old = by_scene.get(sid)
        if not old or os.path.getmtime(path) >= os.path.getmtime(old):
            by_scene[sid] = path

    items = sorted(by_scene.items(), key=lambda x: int(x[0]))
    if limit and limit > 0:
        items = items[:limit]
    return items


def upload_generated_covers_to_jellyfin(
    stash_base: str,
    cookie: str,
    plugin_dir: str,
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    timeout = _num(settings.get("timeout_seconds"), 30, 5, 300)
    jf_url = _s(settings.get("jellyfin_url")).rstrip("/")
    jf_key = _s(settings.get("jellyfin_api_key"))
    jf_user = _s(settings.get("jellyfin_user_id"))
    if not jf_url or not jf_key:
        raise RuntimeError("Missing jellyfin_url or jellyfin_api_key in plugin settings")

    folder = generated_folder_path(plugin_dir, settings)
    limit = _num(settings.get("generated_upload_limit"), 0, 0, 1000000)
    files = collect_generated_cover_files(folder, limit=limit)
    log("info", f"Manual generated-folder upload: found {len(files)} cover file(s)")

    if not jf_user:
        jf_user = jf_pick_user_id(jf_url, jf_key, timeout)
        if jf_user:
            log("debug", f"Manual generated-folder upload: auto-picked Jellyfin user id: {jf_user}")

    dry = _bool(settings.get("dry_run"), False)
    uploaded = 0
    skipped = 0
    failed = 0
    results: List[Dict[str, Any]] = []

    for scene_id, path in files:
        item_result: Dict[str, Any] = {"scene_id": scene_id, "file": path}
        try:
            scene = get_scene(stash_base, cookie, scene_id, timeout)
            if _bool(settings.get("skip_unorganized"), False) and not bool(scene.get("organized")):
                skipped += 1
                item_result["skipped"] = "unorganized"
                log("info", f"Scene {scene_id}: skipped generated cover upload because scene is not organized")
                results.append(item_result)
                continue

            stash_path = scene_primary_path(scene)
            allowed, reason = library_filter_decision(stash_path, settings)
            if not allowed:
                skipped += 1
                item_result["skipped"] = "library_filter"
                log("info", f"Scene {scene_id}: skipped generated cover upload ({reason})")
                results.append(item_result)
                continue

            jellyfin_path = _rewrite_prefix(
                stash_path,
                _s(settings.get("path_rewrite_from")),
                _s(settings.get("path_rewrite_to")),
            )
            item_id = resolve_jellyfin_item_id(jf_url, jf_key, jf_user, scene, jellyfin_path, timeout)

            with open(path, "rb") as f:
                img_bytes = f.read()
            if not img_bytes:
                raise RuntimeError("local generated image is empty")
            img_ct = detect_local_image_mime(img_bytes, path)

            if dry:
                log("info", f"[DRY RUN] Scene {scene_id}: would upload {os.path.basename(path)} to Jellyfin")
                item_result.update({"item_id": item_id, "dry_run": True, "bytes": len(img_bytes), "content_type": img_ct})
            else:
                upload_delay = _num(settings.get("jellyfin_upload_delay_seconds"), 2, 0, 300)
                if upload_delay > 0:
                    log("info", f"Scene {scene_id}: waiting {upload_delay}s before uploading generated cover to Jellyfin")
                    time.sleep(upload_delay)
                jf_upload_primary_image(jf_url, jf_key, item_id, img_bytes, img_ct, timeout)
                uploaded += 1
                log("info", f"Scene {scene_id}: Jellyfin cover updated from generated/{os.path.basename(path)}")
                item_result.update({"item_id": item_id, "uploaded": True, "bytes": len(img_bytes), "content_type": img_ct, "upload_delay_seconds": upload_delay})
            results.append(item_result)
        except Exception as e:
            failed += 1
            item_result["error"] = str(e)
            log("error", f"Scene {scene_id}: failed to upload generated cover {os.path.basename(path)}: {e}")
            results.append(item_result)

    log("info", f"Manual generated-folder upload finished: files={len(files)}, uploaded={uploaded}, skipped={skipped}, failed={failed}, dry_run={dry}")
    return {
        "mode": "upload_generated_to_jellyfin",
        "generated_folder": folder,
        "files": len(files),
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "dry_run": dry,
        "results": results,
    }


# -------------------------
# Processing
# -------------------------

def resolve_scene_id(inp: Dict[str, Any], settings: Dict[str, Any]) -> Tuple[str, bool, str]:
    args = inp.get("args") or {}
    hc = args.get("hookContext") or {}
    hook_type = str(hc.get("type") or "")
    hook_scene_id = str(hc.get("id") or "").strip()

    if hook_scene_id:
        return hook_scene_id, True, hook_type

    manual_scene_id = str(settings.get("scene_id") or args.get("scene_id") or "").strip()
    if manual_scene_id:
        return manual_scene_id, False, "manual"

    return "", False, "manual"


def process_scene(
    stash_base: str,
    cookie: str,
    plugin_dir: str,
    settings: Dict[str, Any],
    scene_id: str,
) -> Dict[str, Any]:
    timeout = _num(settings.get("timeout_seconds"), 30, 5, 300)
    jf_url = _s(settings.get("jellyfin_url")).rstrip("/")
    jf_key = _s(settings.get("jellyfin_api_key"))
    jf_user = _s(settings.get("jellyfin_user_id"))
    if not jf_url or not jf_key:
        raise RuntimeError("Missing jellyfin_url or jellyfin_api_key in plugin settings")

    log("info", f"Scene {scene_id}: generating Jellyfin cover")

    scene = get_scene(stash_base, cookie, scene_id, timeout)
    if _bool(settings.get("skip_unorganized"), False) and not bool(scene.get("organized")):
        log("info", f"Scene {scene_id} is not organized, skipped.")
        return {"scene_id": scene_id, "skipped": "unorganized"}

    cover_path = ((scene.get("paths") or {}).get("screenshot") or "").strip()
    if not cover_path:
        raise RuntimeError(f"Scene {scene_id} has no paths.screenshot/cover image")

    studio = scene.get("studio") or {}
    studio_name = str(studio.get("name") or "") if isinstance(studio, dict) else ""

    cover_bytes, cover_ct = fetch_stash_image(stash_base, cookie, cover_path, timeout)
    log("debug", f"Scene {scene_id}: fetched source cover ({cover_ct}, {len(cover_bytes)} bytes)")

    stash_path = scene_primary_path(scene)
    allowed, reason = library_filter_decision(stash_path, settings)
    if not allowed:
        log("info", f"Scene {scene_id}: skipped ({reason})")
        return {"scene_id": scene_id, "skipped": "library_filter", "reason": reason}

    jellyfin_path = _rewrite_prefix(
        stash_path,
        _s(settings.get("path_rewrite_from")),
        _s(settings.get("path_rewrite_to")),
    )

    if not jf_user:
        jf_user = jf_pick_user_id(jf_url, jf_key, timeout)
        if jf_user:
            log("debug", f"Scene {scene_id}: auto-picked Jellyfin user id: {jf_user}")

    item_id = resolve_jellyfin_item_id(jf_url, jf_key, jf_user, scene, jellyfin_path, timeout)

    logo_bytes: Optional[bytes] = None
    logo_source_label = "none"
    logo_ct = ""
    logo_candidates = iter_studio_logo_candidates(studio if isinstance(studio, dict) else {})
    candidate_summary = [
        f"{c.get('source')}:{c.get('studio_name') or c.get('studio_id') or '-'}:path={'yes' if c.get('image_path') else 'no'}"
        for c in logo_candidates
    ]
    log("debug", f"Scene {scene_id}: logo candidates from Stash: {', '.join(candidate_summary) if candidate_summary else 'none'}")

    for cand in logo_candidates:
        logo_path = cand.get("image_path", "")
        cand_name = cand.get("studio_name", "")
        cand_source = cand.get("source", "studio_image")
        if not logo_path:
            continue
        try:
            logo_bytes, logo_ct = fetch_stash_image(stash_base, cookie, logo_path, timeout)
            logo_source_label = cand_source
            if cand_source == "studio_image":
                log("debug", f"Scene {scene_id}: using Stash studio logo '{cand_name}' ({logo_ct}, {len(logo_bytes)} bytes)")
            else:
                log("debug", f"Scene {scene_id}: current studio has no usable logo; using parent studio logo '{cand_name}' ({logo_ct}, {len(logo_bytes)} bytes)")
            break
        except Exception as e:
            log("debug", f"Scene {scene_id}: could not fetch {cand_source} for studio '{cand_name}' from {logo_path!r}: {e}")

    if logo_bytes is None:
        try:
            logo_bytes, logo_ct = fetch_jellyfin_item_image(jf_url, jf_key, item_id, "Logo", timeout)
            logo_source_label = "jellyfin_item_logo"
            log("debug", f"Scene {scene_id}: no usable Stash studio logo; using Jellyfin item Logo image ({logo_ct}, {len(logo_bytes)} bytes)")
        except Exception as e:
            log("debug", f"Scene {scene_id}: no usable logo found in Stash and Jellyfin item Logo is unavailable: {e}")

    out_w = _num(settings.get("poster_width"), 800, 200, 4000)
    out_h = _num(settings.get("poster_height"), 1200, 200, 6000)
    quality = _num(settings.get("image_quality"), 92, 60, 98)
    logo_wp = _num(settings.get("logo_width_percent"), 50, 1, 100)
    logo_hp = _num(settings.get("logo_max_height_percent"), 100, 1, 100)
    logo_pad = _num(settings.get("logo_padding_percent"), 3, 0, 20)
    logo_trim = _bool(settings.get("logo_trim_transparent_padding"), False)
    logo_boost = _bool(settings.get("logo_visibility_boost"), False)
    logo_text_fallback = _bool(settings.get("use_studio_name_when_logo_missing"), False)
    logo_bg_opacity = _num(settings.get("logo_background_opacity"), 0, 0, 180)
    log(
        "debug",
        f"Scene {scene_id}: logo options width={logo_wp}%, trim={logo_trim}, "
        f"visibility_boost={logo_boost}, text_fallback={logo_text_fallback}, "
        f"background_opacity={logo_bg_opacity}, source={logo_source_label}"
    )

    poster_bytes, poster_ct, crop_info, logo_info = build_poster(
        cover_bytes,
        logo_bytes,
        out_w,
        out_h,
        logo_wp,
        logo_hp,
        logo_pad,
        quality,
        studio_name=studio_name,
        logo_source_label=logo_source_label,
        use_studio_name_when_logo_missing=logo_text_fallback,
        logo_trim_transparent_padding=logo_trim,
        logo_visibility_boost=logo_boost,
        logo_background_opacity=logo_bg_opacity,
    )
    log("debug", f"Generated poster {out_w}x{out_h}, {len(poster_bytes)} bytes, crop: {crop_info}; logo: {json.dumps(logo_info, ensure_ascii=False)}")

    debug_path = ""
    if _bool(settings.get("save_debug_copy"), False):
        debug_path = save_debug_copy(plugin_dir, scene_id, poster_bytes)
        rel_debug_path = os.path.relpath(debug_path, plugin_dir or ".") if debug_path else ""
        log("info", f"Scene {scene_id}: generated poster and saved local copy: {rel_debug_path}")
    else:
        log("info", f"Scene {scene_id}: generated poster")

    if _bool(settings.get("dry_run"), False):
        log("info", f"Scene {scene_id}: dry run complete; Jellyfin upload skipped")
        return {
            "scene_id": scene_id,
            "item_id": item_id,
            "dry_run": True,
            "debug_path": debug_path,
            "crop_info": crop_info,
            "logo_info": logo_info,
        }

    upload_delay = _num(settings.get("jellyfin_upload_delay_seconds"), 2, 0, 300)
    if upload_delay > 0:
        log("info", f"Scene {scene_id}: waiting {upload_delay}s before uploading cover to Jellyfin")
        time.sleep(upload_delay)

    jf_upload_primary_image(jf_url, jf_key, item_id, poster_bytes, poster_ct, timeout)
    log("info", f"Scene {scene_id}: Jellyfin cover updated successfully")
    return {
        "scene_id": scene_id,
        "item_id": item_id,
        "uploaded": True,
        "debug_path": debug_path,
        "crop_info": crop_info,
        "logo_info": logo_info,
        "upload_delay_seconds": upload_delay,
    }


def main() -> None:
    try:
        inp = read_input()
        args = inp.get("args") or {}
        sc = inp.get("server_connection") or inp.get("serverConnection") or {}
        stash_base = stash_base_from_server_connection(sc)
        cookie = stash_cookie_from_server_connection(sc)
        plugin_dir = stash_plugin_dir_from_server_connection(sc)
        plugin_id = stash_plugin_id_from_plugin_dir(plugin_dir) or "stash_jellyfin_cover_generator"

        settings_payload = extract_settings_from_payload(inp)
        settings_conf: Dict[str, Any] = {}
        debug: Dict[str, Any] = {}
        try:
            settings_conf, debug = fetch_plugin_settings_from_stash(
                stash_base,
                cookie,
                _num(settings_payload.get("timeout_seconds"), 30, 5, 300),
                plugin_id,
            )
        except Exception as e:
            debug = {"settings_fetch_error": str(e), "plugin_id": plugin_id, "cookie_present": bool(cookie)}

        settings = apply_defaults({**settings_conf, **settings_payload})
        scene_id, is_hook, source_type = resolve_scene_id(inp, settings)

        if is_hook and not _bool(settings.get("update_on_scene_update"), False):
            log("info", f"Hook {source_type} received, but update_on_scene_update is disabled. Skipping.")
            print(json.dumps({"output": add_run_logs("Skipped: update_on_scene_update is disabled")}, ensure_ascii=False))
            return

        # Manual-only bulk mode. It scans pluginDir/generated, extracts Stash scene ids
        # from filenames like scene_76005.jpg / scene_76005_1783506892.jpg, resolves
        # the matching Jellyfin item, and replaces its Primary image with that file.
        if (not is_hook) and _bool(settings.get("upload_generated_to_jellyfin"), False):
            result = upload_generated_covers_to_jellyfin(stash_base, cookie, plugin_dir, settings)
            print(json.dumps({"output": add_run_logs(result)}, ensure_ascii=False))
            return

        if not scene_id:
            raise RuntimeError("No scene_id provided. For manual generation, edit task defaultArgs.scene_id. For bulk upload, run the 'Upload generated covers to Jellyfin' task or enable upload_generated_to_jellyfin.")

        result = process_scene(stash_base, cookie, plugin_dir, settings, scene_id)
        print(json.dumps({"output": add_run_logs(result)}, ensure_ascii=False))
    except Exception as e:
        log("error", str(e))
        print(json.dumps({"error": str(e), "messages": list(RUN_LOGS)}, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
