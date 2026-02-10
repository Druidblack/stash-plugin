#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stash → Jellyfin Performer Image+Overview Sync v0.3.6.9

Fix: reliably reads plugin settings saved in UI.

Additional fix in v0.3.4:
- Jellyfin image upload now sends a full `Authorization: MediaBrowser ...` header
  (in addition to X-Emby-Token) and retries with an api_key query param.
  This improves compatibility with Jellyfin installs that return HTTP 500 when
  using only X-Emby-Token.

Why previous versions failed:
- Stash external plugins (raw) do NOT include plugin settings in hook args in many versions/builds.
- Therefore we must read settings from Stash configuration via GraphQL.
- GraphQL schema differs between versions: configuration.plugins may be a JSON/map, not a list.
- This version derives plugin_id from server_connection.pluginDir and supports both shapes.
"""

import base64
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAP_PATH = os.path.join(SCRIPT_DIR, "stash_to_jellyfin_map.json")

REQUIRED_KEYS = ("jellyfin_url", "jellyfin_api_key")


# -------------------------
# Helpers
# -------------------------

def jprint(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False))


def _bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _num(v: Any, default: int) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _s(v: Any) -> str:
    return "" if v is None else str(v)


def normalize_name(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[\s\u00A0]+", " ", s)
    s = re.sub(r"[\W_]+$", "", s)
    return s



def jf_person_web_url(jf_url: str, person_id: str) -> str:
    """Build a Jellyfin web URL to the Person details page (best-effort)."""
    base = (jf_url or "").strip().rstrip("/")
    # If user accidentally provided /web as base, strip it
    if base.lower().endswith("/web"):
        base = base[:-4].rstrip("/")
    return f"{base}/web/index.html#!/details?id={person_id}"


def overview_add_jellyfin_url(overview: str, jf_url: str, person_id: str) -> str:
    """Append Jellyfin profile link to the 🌐 URLs block in Overview (with blank lines)."""
    url = jf_person_web_url(jf_url, person_id)
    if not url:
        return overview or ""

    if overview and "🌐 URLs:" in overview:
        head, tail = overview.split("🌐 URLs:", 1)
        head = head.rstrip()
        tail = tail.lstrip("\n")
        if tail.strip():
            tail = tail.rstrip() + "\n\n" + url
        else:
            tail = url
        return (head + "\n\n🌐 URLs:\n" + tail).strip()

    if overview:
        return (overview.rstrip() + "\n\n🌐 URLs:\n" + url).strip()

    return ("🌐 URLs:\n" + url).strip()

def normalize_id(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def snake_key(k: str) -> str:
    """Convert jellyfinUrl/jellyfin-url -> jellyfin_url"""
    k = (k or "").strip()
    if not k:
        return ""
    k = k.replace("-", "_")
    # camelCase -> snake_case
    k = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", k)
    return k.lower()


def flatten_settings(obj: Any) -> Dict[str, Any]:
    """
    Normalize possible shapes of settings objects:
    - {"jellyfin_url":"..."}
    - {"jellyfinUrl":"..."}
    - {"jellyfin_url":{"value":"..."}}
    - [{"key":"jellyfin_url","value":"..."}]
    """
    out: Dict[str, Any] = {}

    if isinstance(obj, dict):
        for k, v in obj.items():
            kk = snake_key(str(k))
            if isinstance(v, dict):
                # common: {"value": "..."} or {"Value": "..."}
                if "value" in v or "Value" in v:
                    out[kk] = v.get("value", v.get("Value"))
                elif "string" in v or "String" in v:
                    out[kk] = v.get("string", v.get("String"))
                else:
                    # keep raw dict
                    out[kk] = v
            else:
                out[kk] = v
        return out

    if isinstance(obj, list):
        for it in obj:
            if isinstance(it, dict):
                k = it.get("key") or it.get("Key") or it.get("name") or it.get("Name")
                v = it.get("value") or it.get("Value")
                kk = snake_key(str(k or ""))
                if kk:
                    out[kk] = v
        return out

    return {}


def load_map() -> Dict[str, str]:
    if not os.path.exists(MAP_PATH):
        return {}
    try:
        with open(MAP_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_map(m: Dict[str, str]) -> None:
    tmp = MAP_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(m, f, ensure_ascii=False, indent=2)
    os.replace(tmp, MAP_PATH)


def read_input() -> Dict[str, Any]:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    return json.loads(raw)


def stash_base_from_server_connection(sc: Dict[str, Any]) -> str:
    scheme = sc.get("Scheme") or sc.get("scheme") or "http"
    host = sc.get("Host") or sc.get("host") or "localhost"
    port = sc.get("Port") or sc.get("port") or 9999
    if host == "0.0.0.0":
        host = "localhost"
    return f"{scheme}://{host}:{port}"


def stash_plugin_dir_from_server_connection(sc: Dict[str, Any]) -> str:
    return str(sc.get("PluginDir") or sc.get("pluginDir") or sc.get("plugin_dir") or "")


def stash_plugin_id_from_plugin_dir(plugin_dir: str) -> str:
    # .../plugins/<id>/ -> <id>
    if not plugin_dir:
        return ""
    p = plugin_dir.rstrip("/\\")
    return os.path.basename(p)


def stash_cookie_from_server_connection(sc: Dict[str, Any]) -> str:
    """
    Normalize Stash session cookie from raw input.
    Stash sends server_connection.sessionCookie as an http.Cookie-like object.
    """
    c = sc.get("SessionCookie") or sc.get("sessionCookie") or sc.get("session_cookie") or ""
    if not c:
        c = sc.get("cookie") or sc.get("Cookies") or sc.get("cookies") or ""

    def _one(x: Any) -> str:
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
            val  = str(x.get("Value") or x.get("value") or "").strip()
            if name and val:
                return f"{name}={val}"
            raw = x.get("Raw") or x.get("raw") or ""
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
            return ""
        return ""

    if isinstance(c, list):
        parts = [_one(i) for i in c]
        parts = [p for p in parts if p]
        return "; ".join(parts)

    return _one(c)


def extract_settings_from_payload(inp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Some Stash builds may include settings inside args. Support multiple shapes.
    """
    args = inp.get("args") or {}
    merged: Dict[str, Any] = {}

    for key in ("settings", "pluginSettings", "plugin_settings", "config", "pluginConfig", "plugin_config"):
        v = args.get(key)
        if isinstance(v, (dict, list)):
            merged.update(flatten_settings(v))

    # Sometimes settings are flattened into args directly (rare). Pick known keys.
    flat = flatten_settings(args) if isinstance(args, dict) else {}
    for k, v in flat.items():
        if k in REQUIRED_KEYS or k in ("jellyfin_user_id", "update_image", "reencode_images", "image_upload_format", "ffmpeg_path", "timeout_seconds", "dry_run"):
            merged[k] = v

    return merged


# -------------------------
# Stash GraphQL
# -------------------------

def gql_post(stash_base: str, cookie: str, query: str, variables: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    url = f"{stash_base}/graphql"
    headers: Dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
    if cookie:
        headers["Cookie"] = cookie

    r = requests.post(url, headers=headers, json={"query": query, "variables": variables}, timeout=timeout)

    text = (r.text or "").strip()
    try:
        data = r.json() if text else {}
    except Exception:
        data = {"raw": text}

    if isinstance(data, dict) and data.get("errors"):
        raise RuntimeError(f"Stash GraphQL errors: {data['errors']}")

    if r.status_code >= 400:
        raise RuntimeError(f"Stash GraphQL HTTP {r.status_code}: {text[:1200]}")

    return data


def fetch_plugin_settings_from_stash(stash_base: str, cookie: str, timeout: int, plugin_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Returns (settings, debugInfo)
    Supports:
      - configuration { plugins }  (plugins is JSON/map)
      - configuration { plugins { name settings } } (plugins list)
    """
    debug: Dict[str, Any] = {"plugin_id": plugin_id, "stash_base": stash_base, "cookie_present": bool(cookie)}

    # 1) Preferred: plugins as JSON scalar/map
    try:
        q = "query C { configuration { plugins } }"
        data = gql_post(stash_base, cookie, q, {}, timeout)
        conf = (data.get("data") or {}).get("configuration") or {}
        plugins = conf.get("plugins")

        if isinstance(plugins, dict):
            debug["plugins_shape"] = "map"
            debug["plugins_keys_sample"] = list(plugins.keys())[:30]

            # keys can be plugin ids; try direct match
            if plugin_id and plugin_id in plugins:
                s = flatten_settings(plugins.get(plugin_id))
                return s, debug

            # sometimes key is yaml name normalized; try heuristic
            nid = normalize_id(plugin_id)
            for k, v in plugins.items():
                if normalize_id(str(k)) == nid and nid:
                    s = flatten_settings(v)
                    return s, debug

            # fallback: find any settings object containing required keys
            for k, v in plugins.items():
                s = flatten_settings(v)
                if s and all(rk in s for rk in REQUIRED_KEYS):
                    debug["matched_key"] = k
                    return s, debug

            # not found -> continue to list-based query
        elif isinstance(plugins, list):
            # some builds may already return list here
            debug["plugins_shape"] = "list"
            # fall through to list parsing below
            raise RuntimeError("plugins returned list in scalar query; retry list mode")
        else:
            debug["plugins_shape"] = type(plugins).__name__
    except Exception as e:
        debug["plugins_scalar_query_error"] = str(e)

    # 2) Alternative: plugins list objects (name/settings)
    try:
        q = "query C { configuration { plugins { name settings } } }"
        data = gql_post(stash_base, cookie, q, {}, timeout)
        conf = (data.get("data") or {}).get("configuration") or {}
        plugins = conf.get("plugins")
        if isinstance(plugins, list):
            debug["plugins_shape"] = "list"
            debug["plugins_names_sample"] = [p.get("name") for p in plugins[:30] if isinstance(p, dict)]
            # match by plugin_id vs name
            pid_norm = normalize_id(plugin_id)
            for p in plugins:
                if not isinstance(p, dict):
                    continue
                name = str(p.get("name") or "")
                if normalize_id(name) == pid_norm and pid_norm:
                    s = flatten_settings(p.get("settings"))
                    return s, debug
            for p in plugins:
                if not isinstance(p, dict):
                    continue
                s = flatten_settings(p.get("settings"))
                if s and all(rk in s for rk in REQUIRED_KEYS):
                    debug["matched_name"] = p.get("name")
                    return s, debug
    except Exception as e:
        debug["plugins_list_query_error"] = str(e)

    return {}, debug


# -------------------------
# Stash performer schema helpers (for compatibility across Stash versions)
# -------------------------

_PERFORMER_FIELDS_CACHE: Optional[Dict[str, Tuple[str, str]]] = None  # field -> (base_kind, base_name)


def _unwrap_gql_type(t: Any) -> Tuple[str, str]:
    """
    Return (kind, name) of the deepest named type by unwrapping NON_NULL/LIST.
    Example: NON_NULL -> LIST -> NON_NULL -> OBJECT(URL) => ("OBJECT","URL")
    """
    cur = t or {}
    while isinstance(cur, dict) and cur.get("kind") in ("NON_NULL", "LIST") and cur.get("ofType"):
        cur = cur.get("ofType")
    kind = (cur or {}).get("kind") or ""
    name = (cur or {}).get("name") or ""
    return str(kind), str(name)


def introspect_performer_fields(stash_base: str, cookie: str, timeout: int) -> Dict[str, Tuple[str, str]]:
    global _PERFORMER_FIELDS_CACHE
    if _PERFORMER_FIELDS_CACHE is not None:
        return _PERFORMER_FIELDS_CACHE

    q = """
    query IntrospectPerformer {
      __type(name: "Performer") {
        fields {
          name
          type {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
        }
      }
    }
    """
    data = gql_post(stash_base, cookie, q, {}, timeout)
    t = ((data.get("data") or {}).get("__type") or {})
    fields = t.get("fields") or []
    out: Dict[str, Tuple[str, str]] = {}
    for f in fields:
        if not isinstance(f, dict):
            continue
        name = f.get("name")
        if not name:
            continue
        kind, base = _unwrap_gql_type((f.get("type") or {}))
        out[str(name)] = (kind, base)
    _PERFORMER_FIELDS_CACHE = out
    return out


def get_performer(stash_base: str, cookie: str, performer_id: int, timeout: int) -> Dict[str, Any]:
    """
    Fetch performer data from Stash.
    We build the selection set dynamically using GraphQL introspection to avoid
    "Cannot query field ..." errors across different Stash versions.
    """
    base_fields = ["id", "name", "image_path"]

    # Canonical field -> candidate field names (snake_case and camelCase variants)
    desired_map: Dict[str, List[str]] = {
        "details": ["details"],
        "aliases": ["aliases", "alias_list", "aliasList"],
        "birthdate": ["birthdate", "birth_date", "birthDate", "date_of_birth", "dateOfBirth", "dob"],
        "deathdate": ["deathdate", "death_date", "deathDate", "date_of_death", "dateOfDeath", "dod"],
        # Prefer country (object) but allow text birthplace-like fields as fallback
        "country": ["country", "birth_country", "birthCountry", "birthplace", "birth_place", "birthPlace"],
        "ethnicity": ["ethnicity"],
        "hair_color": ["hair_color", "hairColor"],
        "eye_color": ["eye_color", "eyeColor"],
        "height": ["height", "height_cm", "heightCm"],
        "weight": ["weight", "weight_kg", "weightKg"],
        "penis_length": ["penis_length", "penisLength"],
        "circumcised": ["circumcised"],
        "measurements": ["measurements"],
        "fake_tits": ["fake_tits", "fakeTits"],
        "tattoos": ["tattoos"],
        "piercings": ["piercings"],
        "career_length": ["career_length", "careerLength"],
        "career_start_year": ["career_start_year", "careerStartYear"],
        "career_end_year": ["career_end_year", "careerEndYear"],
        "urls": ["urls"],
        "url": ["url"],
        "twitter": ["twitter"],
        "instagram": ["instagram"],
        "website": ["website"],
    }

    selection: List[str] = list(base_fields)

    try:
        available = introspect_performer_fields(stash_base, cookie, timeout)
    except Exception:
        available = {}

    chosen: Dict[str, str] = {}
    for canon, candidates in desired_map.items():
        for cand in candidates:
            if cand in available:
                chosen[canon] = cand
                break

    # Build selection set using the actual field names.
    for canon, actual in chosen.items():
        if canon == "urls":
            kind, base = available.get(actual, ("", ""))
            # urls can be [URL] (object) or [String] (scalar) depending on version.
            if kind == "OBJECT" or base in ("URL", "Url", "PerformerURL", "URLFragment"):
                selection.append(f"{actual} {{ url type site {{ name }} }}")
            else:
                selection.append(actual)
        elif canon == "country":
            kind, base = available.get(actual, ("", ""))
            # country is typically an OBJECT (Country). If so, request its name.
            if kind == "OBJECT" or base in ("Country", "PerformerCountry"):
                selection.append(f"{actual} {{ name }}")
            else:
                selection.append(actual)
        else:
            selection.append(actual)

    sel = "\n        ".join(selection)


    q = f"""


    query FindPerformer($id: ID!) {{


      findPerformer(id: $id) {{


        {sel}


      }}


    }}


    """


    data = gql_post(stash_base, cookie, q, {"id": str(performer_id)}, timeout)
    p = (data.get("data") or {}).get("findPerformer")
    if not p:
        raise RuntimeError("findPerformer returned null")
    return p

# -------------------------
# Image handling
# -------------------------


def _extract_stash_urls_raw(performer: Dict[str, Any]) -> Any:
    return performer.get("urls") or performer.get("Urls") or performer.get("URLS") or performer.get("url") or performer.get("URL")


def stash_add_jellyfin_profile_url_to_performer(
    stash_base: str,
    cookie: str,
    timeout: int,
    performer_id: int,
    performer: Dict[str, Any],
    jf_profile_url: str,
    url_type: str = "Jellyfin",
) -> bool:
    """Add Jellyfin profile URL into Stash performer's Links/URLs (best-effort).

    Supports Stash versions that have:
      - performer.urls (list of URL objects or strings), OR
      - performer.url (single string; may contain multiple URLs separated by newlines).
    Returns True if an update mutation was sent, False if no changes were needed.
    """
    jf_profile_url = (jf_profile_url or "").strip()
    if not jf_profile_url:
        return False

    raw = _extract_stash_urls_raw(performer)

    # 1) Prefer urls list if available in performer payload
    if isinstance(raw, list):
        existing: List[Dict[str, str]] = []
        seen = set()

        def _norm(u: str) -> str:
            return (u or "").strip()

        for u in raw:
            if isinstance(u, dict):
                uu = _norm(u.get("url") or u.get("URL") or u.get("link") or u.get("value"))
                tt = _norm(u.get("type") or u.get("Type") or "")
                if uu:
                    key = uu.lower()
                    if key not in seen:
                        existing.append({"url": uu, "type": tt})
                        seen.add(key)
            else:
                uu = _norm(str(u))
                if uu:
                    key = uu.lower()
                    if key not in seen:
                        existing.append({"url": uu, "type": ""})
                        seen.add(key)

        if jf_profile_url.lower() in seen:
            return False

        existing.append({"url": jf_profile_url, "type": url_type})

        # Try updating urls list
        q = """mutation performerUpdate($input: PerformerUpdateInput!) { performerUpdate(input: $input) { id } }"""
        try:
            gql_post(stash_base, cookie, q, {"input": {"id": str(performer_id), "urls": existing}}, timeout)
            return True
        except Exception:
            # Fall back to url string below
            pass

        # Fall through to url string update if urls update failed
        raw = performer.get("url") or performer.get("URL") or ""

    # 2) Update single url string (common in older Stash versions)
    if isinstance(raw, str) or raw is None:
        s = (raw or "").strip()
        # Dedup check (simple containment)
        if jf_profile_url in s:
            return False
        if s:
            combined = s.rstrip() + "\n" + jf_profile_url
        else:
            combined = jf_profile_url

        q = """mutation performerUpdate($input: PerformerUpdateInput!) { performerUpdate(input: $input) { id } }"""
        gql_post(stash_base, cookie, q, {"input": {"id": str(performer_id), "url": combined}}, timeout)
        return True

    # Unknown shape -> do nothing
    return False

def detect_image_mime(data: bytes, header_ct: str) -> str:
    ct = (header_ct or "").split(";")[0].strip().lower()

    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "image/gif"

    if ct == "image/jpg":
        return "image/jpeg"
    if ct in ("image/jpeg", "image/png", "image/webp", "image/gif"):
        return ct

    return ""


def looks_like_html(data: bytes) -> bool:
    head = data[:256].lstrip().lower()
    return head.startswith(b"<!doctype") or head.startswith(b"<html") or b"<head" in head or b"<body" in head


def reencode_image(data: bytes, out_fmt: str, ffmpeg_path: str) -> Optional[Tuple[bytes, str]]:
    fmt = (out_fmt or "jpeg").strip().lower()
    if fmt not in ("jpeg", "jpg", "png"):
        fmt = "jpeg"
    if fmt == "jpg":
        fmt = "jpeg"
    out_mime = "image/png" if fmt == "png" else "image/jpeg"

    # Pillow first (optional)
    try:
        from PIL import Image  # type: ignore
        import io
        with Image.open(io.BytesIO(data)) as im:
            out = io.BytesIO()
            if fmt == "png":
                im.save(out, format="PNG")
                return out.getvalue(), "image/png"
            if im.mode not in ("RGB", "L"):
                im = im.convert("RGB")
            im.save(out, format="JPEG", quality=92, optimize=True, progressive=False)
            return out.getvalue(), "image/jpeg"
    except Exception:
        pass

    # ffmpeg fallback
    try:
        import subprocess, tempfile, os as _os
        in_suffix = ".img"
        out_suffix = ".png" if fmt == "png" else ".jpg"
        with tempfile.NamedTemporaryFile(delete=False, suffix=in_suffix) as fin:
            fin.write(data)
            fin.flush()
            in_path = fin.name
        with tempfile.NamedTemporaryFile(delete=False, suffix=out_suffix) as fout:
            out_path = fout.name

        cmd = [ffmpeg_path or "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", in_path, "-frames:v", "1", out_path]
        subprocess.run(cmd, check=True)

        with open(out_path, "rb") as f:
            out_bytes = f.read()

        try:
            _os.unlink(in_path)
            _os.unlink(out_path)
        except Exception:
            pass

        return out_bytes, out_mime
    except Exception:
        return None


def fetch_stash_image(stash_base: str, cookie: str, image_path: str, timeout: int) -> Tuple[bytes, str]:
    url = image_path
    if url.startswith("/"):
        url = stash_base + url

    headers: Dict[str, str] = {"Accept": "image/*,*/*"}
    if cookie:
        headers["Cookie"] = cookie

    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()

    data = r.content or b""
    header_ct = (r.headers.get("Content-Type") or "")

    if header_ct.lower().startswith("text/html") or looks_like_html(data):
        raise RuntimeError("Stash returned HTML instead of an image (likely auth/session problem).")

    ct = detect_image_mime(data, header_ct)
    if not ct:
        raise RuntimeError(f"Unrecognized image payload from Stash (Content-Type={header_ct}).")

    return data, ct


# -------------------------
# Jellyfin
# -------------------------

PLUGIN_VERSION = "0.3.6.18"
def jellyfin_headers(api_key: str) -> Dict[str, str]:
    """Return Jellyfin auth headers.

    Jellyfin's API auth can be picky about the Authorization header format.
    Some installs may accept only X-Emby-Token, while others require a full
    `Authorization: MediaBrowser ...` value.

    We send both variants for maximum compatibility.
    """

    # Use Jellyfin Web as the client string to mimic the official web client.
    # This is known to work on setups where bare API keys in Authorization fail.
    mb = (
        'MediaBrowser '
        'Client="Jellyfin%20Web", '
        'Device="Stash", '
        'DeviceId="stash-jellyfin-performer-image-sync", '
        f'Version="{PLUGIN_VERSION}", '
        f'Token="{api_key}"'
    )

    return {
        "X-Emby-Token": api_key,
        "X-Emby-Authorization": mb,
        "Authorization": mb,
        "Accept": "application/json",
        "User-Agent": f"StashJellyfinPerformerImageSync/{PLUGIN_VERSION}",
    }


def jf_get(jf_url: str, api_key: str, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 20) -> Any:
    r = requests.get(f"{jf_url}{path}", headers=jellyfin_headers(api_key), params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

def jf_post_json(jf_url: str, api_key: str, path: str, payload: Dict[str, Any], timeout: int = 20) -> Any:
    """POST JSON to Jellyfin with robust auth/header variants."""
    url = f"{jf_url}{path}"
    headers = {**jellyfin_headers(api_key), "Content-Type": "application/json", "Accept": "application/json"}

    attempts: List[Tuple[str, Dict[str, str], Dict[str, Any]]] = [
        ("headers", headers, {}),
        ("headers+api_key", headers, {"api_key": api_key}),
        ("api_key_only", {"Content-Type": "application/json", "Accept": "application/json"}, {"api_key": api_key}),
    ]

    last_exc: Optional[Exception] = None
    last_resp: Optional[requests.Response] = None
    for label, hdrs, params in attempts:
        try:
            r = requests.post(url, headers=hdrs, params=params or None, json=payload, timeout=timeout)
            last_resp = r
            r.raise_for_status()
            # Jellyfin may return JSON or empty
            try:
                return r.json()
            except Exception:
                return {"status_code": r.status_code, "text": (r.text or "").strip()}
        except Exception as e:
            last_exc = e
            if last_resp is not None and 400 <= last_resp.status_code < 500:
                break

    body = ""
    status = ""
    if last_resp is not None:
        status = f"HTTP {last_resp.status_code}"
        try:
            body = (last_resp.text or "").strip()
        except Exception:
            body = ""
        if len(body) > 2000:
            body = body[:2000] + "…"

    raise RuntimeError(f"Jellyfin POST failed ({status}): {last_exc}. Response body: {body}")


def jf_post_binary(jf_url: str, api_key: str, path: str, content: bytes, content_type: str, timeout: int = 20) -> None:
    """Upload image to Jellyfin.

    IMPORTANT: Jellyfin's SetItemImage endpoint expects the request body to be the image bytes BASE64-encoded
    (server-side stack traces show Base64 decoding via FromBase64Transform). Sending raw binary will trigger
    System.FormatException / invalid Base64.
    """

    url = f"{jf_url}{path}"

    # Jellyfin expects base64 body (ASCII). Keep Content-Type as the *image* mime type so Jellyfin can infer extension.
    payload = base64.b64encode(content)

    base_headers = {"Content-Type": content_type, "Accept": "application/json", **jellyfin_headers(api_key)}

    attempts: List[Tuple[str, Dict[str, str], Dict[str, Any]]] = [
        ("headers", base_headers, {}),
        # Some installs accept api_key in the query string.
        ("headers+api_key", base_headers, {"api_key": api_key}),
        # Fallback: minimal auth via query string only.
        ("api_key_only", {"Content-Type": content_type, "Accept": "application/json"}, {"api_key": api_key}),
    ]

    last_exc: Optional[Exception] = None
    last_resp: Optional[requests.Response] = None
    for label, headers, params in attempts:
        try:
            r = requests.post(url, headers=headers, params=params or None, data=payload, timeout=timeout)
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
        try:
            body = (last_resp.text or "").strip()
        except Exception:
            body = ""
        if len(body) > 2000:
            body = body[:2000] + "…"

    raise RuntimeError(f"Jellyfin image upload failed ({status}): {last_exc}. Response body: {body}")



def jf_search_people(jf_url: str, api_key: str, name: str, timeout: int, user_id: str = "") -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "searchTerm": name,
        "limit": 20,
        "includePeople": True,
        "includeItemTypes": "Person",
    }
    if user_id:
        params["userId"] = user_id

    data = jf_get(jf_url, api_key, "/Search/Hints", params=params, timeout=timeout)
    return data.get("SearchHints") or data.get("Hints") or []


def pick_person_id(hints: List[Dict[str, Any]], performer_name: str) -> Optional[str]:
    target = normalize_name(performer_name)
    for h in hints:
        n = normalize_name(h.get("Name") or h.get("name") or "")
        if n == target:
            return str(h.get("Id") or h.get("id"))
    for h in hints:
        iid = h.get("Id") or h.get("id")
        if iid:
            return str(iid)
    return None


# -------------------------
# Jellyfin overview builder (from Stash performer fields)
# -------------------------


def jf_extract_stash_id_from_item(item: Dict[str, Any]) -> str:
    """Extract Stash performer id from Jellyfin Person metadata (best-effort)."""
    # ProviderIds
    try:
        prov = item.get("ProviderIds") or {}
        if isinstance(prov, dict):
            for k, v in prov.items():
                kk = str(k or "").lower().replace(" ", "").replace("_", "")
                if kk in ("stash", "stashid", "stashperformerid", "stashperformer", "stashperfid"):
                    s = str(v or "").strip()
                    mm = re.search(r"(\d+)", s)
                    if mm:
                        return mm.group(1)
    except Exception:
        pass

    # Tags
    try:
        tags = item.get("Tags") or []
        if isinstance(tags, list):
            for t in tags:
                mm = re.search(r"stash\s*[:#\-]?\s*(\d+)", str(t), re.I)
                if mm:
                    return mm.group(1)
    except Exception:
        pass

    # Text fields
    for field in ("Overview", "OriginalTitle"):
        try:
            txt = str(item.get(field) or "")
        except Exception:
            txt = ""
        if not txt:
            continue
        mm = re.search(r"stash\s*(?:performer\s*)?id\s*[:#]\s*(\d+)", txt, re.I)
        if mm:
            return mm.group(1)
        mm = re.search(r"/performers/(\d+)", txt, re.I)
        if mm:
            return mm.group(1)

    return ""


def jf_person_id_verified(
    jf_url: str,
    api_key: str,
    person_id: str,
    stash_id: str,
    timeout: int,
    user_id: str = "",
) -> Tuple[bool, str]:
    """Return (is_acceptable, found_stash_id_marker)."""
    item = jf_get_item_dto(jf_url, api_key, person_id, timeout=timeout, user_id=user_id)
    found = jf_extract_stash_id_from_item(item)
    if found and str(found) != str(stash_id):
        return (False, found)
    return (True, found)


def pick_person_id_with_stash_verification(
    jf_url: str,
    api_key: str,
    hints: List[Dict[str, Any]],
    performer_name: str,
    stash_id: str,
    timeout: int,
    user_id: str = "",
) -> Optional[str]:
    """Pick a Jellyfin Person Id with additional verification using Stash id markers (if present)."""
    if not hints:
        return None

    target = normalize_name(performer_name)

    # Prefer exact normalized name matches first
    ordered: List[Dict[str, Any]] = []
    for h in hints:
        n = normalize_name(h.get("Name") or h.get("name") or "")
        if n == target:
            ordered.append(h)
    for h in hints:
        if h not in ordered:
            ordered.append(h)

    mismatched_ids: set = set()
    has_any_marker = False

    for h in ordered[:8]:
        pid = str(h.get("Id") or h.get("id") or "").strip()
        if not pid:
            continue
        ok, found = jf_person_id_verified(jf_url, api_key, pid, stash_id, timeout, user_id=user_id)
        if found:
            has_any_marker = True
        if ok and found and str(found) == str(stash_id):
            return pid
        if (not ok) and found:
            mismatched_ids.add(pid)

    if has_any_marker and mismatched_ids:
        filtered: List[Dict[str, Any]] = []
        for h in hints:
            pid = str(h.get("Id") or h.get("id") or "").strip()
            if pid and pid in mismatched_ids:
                continue
            filtered.append(h)
        return pick_person_id(filtered, performer_name)

    return pick_person_id(hints, performer_name)

def _human_bool(v: Any) -> str:
    if isinstance(v, bool):
        return "Да" if v else "Нет"
    s = _s(v).strip()
    if s.lower() in ("true", "yes", "y", "1", "on"):
        return "Да"
    if s.lower() in ("false", "no", "n", "0", "off"):
        return "Нет"
    return s


def _join_list(v: Any, sep: str = ", ") -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        parts = []
        for x in v:
            xs = _s(x).strip()
            if xs:
                parts.append(xs)
        return sep.join(parts)
    return _s(v).strip()


def _format_urls(v: Any) -> List[str]:
    out: List[str] = []
    if not v:
        return out
    if isinstance(v, str):
        s = v.strip()
        return [s] if s else []
    if isinstance(v, list):
        for it in v:
            if isinstance(it, str):
                s = it.strip()
                if s:
                    out.append(s)
                continue
            if isinstance(it, dict):
                url = _s(it.get("url") or it.get("URL")).strip()
                if not url:
                    continue
                t = _s(it.get("type") or it.get("Type")).strip()
                site = ""
                if isinstance(it.get("site"), dict):
                    site = _s(it["site"].get("name") or it["site"].get("Name")).strip()
                label = site or t
                out.append(f"{label}: {url}" if label else url)
    return out


def build_jellyfin_overview_from_stash(performer: Dict[str, Any]) -> str:
    """Build a readable Overview text for Jellyfin from Stash performer fields.

    Requirements:
    - Each section starts on a new line.
    - Sections are separated by a blank line (double newline).
    - URLs listed consecutively must also be separated by a blank line.
    - Section titles are in English and prefixed with icons.
    """

    ICONS: Dict[str, str] = {
        "Details": "📝",
        "Aliases": "🏷️",
        "Ethnicity": "🌍",
        "Hair Color": "💇",
        "Eye Color": "👀",
        "Height (cm)": "↕️",
        "Weight (kg)": "⚖️",
        "Penis Length (cm)": "🍆",
        "Circumcised": "✂️",
        "Measurements": "📊",
        "Artificial Breasts": "🧪",
        "Tattoos": "🖋️",
        "Piercings": "📌",
        "Career Length": "🗓️",
        "URLs": "🌐",
    }

    def _label(lbl: str) -> str:
        ic = ICONS.get(lbl, "")
        return f"{ic} {lbl}" if ic else lbl

    def _clean(s: Any) -> str:
        if s is None:
            return ""
        if isinstance(s, (int, float)):
            return str(s)
        return str(s).strip()

    blocks: List[str] = []

    def add(label: str, value: Any) -> None:
        v = _clean(value)
        if not v:
            return
        blocks.append(f"{_label(label)}: {v}")

    # Core text fields
    add("Details", performer.get("details") or performer.get("Details"))

    # Aliases (list or string)
    aliases = performer.get("aliases") or performer.get("Aliases") or performer.get("alias_list") or performer.get("alias") or performer.get("Alias")
    if isinstance(aliases, list):
        aliases = ", ".join([_clean(a) for a in aliases if _clean(a)])
    add("Aliases", aliases)

    add("Ethnicity", performer.get("ethnicity"))
    add("Hair Color", performer.get("hair_color"))
    add("Eye Color", performer.get("eye_color"))

    # Measurements / attributes
    add("Height (cm)", performer.get("height_cm") or performer.get("heightCm") or performer.get("height"))
    add("Weight (kg)", performer.get("weight_kg") or performer.get("weightKg") or performer.get("weight"))
    add("Penis Length (cm)", performer.get("penis_length_cm") or performer.get("penisLengthCm") or performer.get("penis_length") or performer.get("penisLength"))
    add("Circumcised", performer.get("circumcised"))
    add("Measurements", performer.get("measurements"))
    add("Artificial Breasts", performer.get("fake_tits"))
    add("Tattoos", performer.get("tattoos"))
    add("Piercings", performer.get("piercings"))
    add("Career Length", performer.get("career_length"))

    # URLs (list of urls)
    urls = performer.get("urls") or performer.get("Urls") or performer.get("URLS") or []
    url_list: List[str] = []
    if isinstance(urls, list):
        for u in urls:
            uu = ""
            if isinstance(u, dict):
                uu = _clean(u.get("url") or u.get("URL") or u.get("link") or u.get("value"))
                typ = _clean(u.get("type") or u.get("Type"))
                if uu and typ:
                    uu = f"{typ}: {uu}"
            else:
                uu = _clean(u)
            if uu:
                url_list.append(uu)
    elif isinstance(urls, dict):
        for k, v in urls.items():
            kk = _clean(k)
            vv = _clean(v)
            if vv and kk:
                url_list.append(f"{kk}: {vv}")
            elif vv:
                url_list.append(vv)
    else:
        u = _clean(urls)
        if u:
            url_list.append(u)

    if url_list:
        blocks.append(f"{_label('URLs')}:\n" + "\n\n".join(url_list))

    return "\n\n".join(blocks).strip()

def _extract_aliases_str(performer: Dict[str, Any]) -> str:
    aliases = (
        performer.get("aliases")
        or performer.get("Aliases")
        or performer.get("alias_list")
        or performer.get("aliasList")
        or performer.get("alias")
        or performer.get("Alias")
    )
    if not aliases:
        return ""
    if isinstance(aliases, list):
        parts = [str(a).strip() for a in aliases if str(a).strip()]
        return ", ".join(parts)
    return str(aliases).strip()


def _extract_birthdate(performer: Dict[str, Any]) -> str:
    return (
        _s(performer.get("birthdate"))
        or _s(performer.get("birth_date"))
        or _s(performer.get("birthDate"))
        or _s(performer.get("date_of_birth"))
        or _s(performer.get("dateOfBirth"))
        or _s(performer.get("dob"))
    ).strip()


def _extract_deathdate(performer: Dict[str, Any]) -> str:
    return (
        _s(performer.get("deathdate"))
        or _s(performer.get("death_date"))
        or _s(performer.get("deathDate"))
        or _s(performer.get("date_of_death"))
        or _s(performer.get("dateOfDeath"))
        or _s(performer.get("dod"))
    ).strip()


def _extract_country_name(performer: Dict[str, Any]) -> str:
    c = (
        performer.get("country")
        or performer.get("birth_country")
        or performer.get("birthCountry")
        or performer.get("birthplace")
        or performer.get("birth_place")
        or performer.get("birthPlace")
    )
    if not c:
        return ""
    if isinstance(c, dict):
        return _s(c.get("name") or c.get("Name")).strip()
    return _s(c).strip()


def _date_only(v: Any) -> str:
    """Normalize a date-like value to YYYY-MM-DD (best-effort)."""
    s = _s(v).strip()
    if not s:
        return ""
    # Common ISO forms
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"(\d{4})/(\d{2})/(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # Year only
    m = re.search(r"(\d{4})", s)
    if m:
        return f"{m.group(1)}-01-01"
    return ""


def _jf_datetime_z(date_yyyy_mm_dd: str) -> str:
    d = _date_only(date_yyyy_mm_dd)
    return f"{d}T00:00:00.0000000Z" if d else ""

def jf_get_users(jf_url: str, api_key: str, timeout: int = 20) -> List[Dict[str, Any]]:
    data = jf_get(jf_url, api_key, "/Users", timeout=timeout)
    return data if isinstance(data, list) else []


def jf_pick_default_user_id(users: List[Dict[str, Any]]) -> str:
    # Prefer administrator account
    for u in users:
        try:
            if ((u.get("Policy") or {}).get("IsAdministrator")):
                return str(u.get("Id") or "")
        except Exception:
            pass
    if users:
        return str(users[0].get("Id") or "")
    return ""


def jf_get_item_dto(jf_url: str, api_key: str, item_id: str, timeout: int, user_id: str = "") -> Dict[str, Any]:
    """Fetch an item DTO for update.

    We prefer the user-level endpoint (/Users/{uid}/Items/{id}) because some Jellyfin builds log
    server-side errors for GET /Items/{id} even when the id is valid, which is noisy but not fatal.
    """

    # 1) User-level endpoint (preferred)
    uid = user_id
    if not uid:
        try:
            uid = jf_pick_default_user_id(jf_get_users(jf_url, api_key, timeout=timeout))
        except Exception:
            uid = ""
    if uid:
        try:
            data = jf_get(jf_url, api_key, f"/Users/{uid}/Items/{item_id}", timeout=timeout)
            if isinstance(data, dict):
                return data
        except Exception:
            pass

    # 2) Server-level endpoint (fallback)
    data = jf_get(jf_url, api_key, f"/Items/{item_id}", timeout=timeout)
    if isinstance(data, dict):
        return data

    raise RuntimeError("Could not fetch Jellyfin item DTO.")


def jf_update_person_metadata(
    jf_url: str,
    api_key: str,
    item_id: str,
    timeout: int,
    user_id: str = "",
    overview: Optional[str] = None,
    original_title: Optional[str] = None,
    premiere_date: Optional[str] = None,
    end_date: Optional[str] = None,
    production_year: Optional[int] = None,
    production_locations: Optional[List[str]] = None,
) -> None:
    """Update Jellyfin person metadata by POSTing a full DTO.

    We always fetch the current DTO and POST it back with selected fields replaced.
    This avoids server-side null handling bugs seen with minimal payloads.

    Field mapping (Jellyfin Web):
      - Original name     -> OriginalTitle
      - Birth date        -> PremiereDate
      - Death date        -> EndDate
      - Birth year        -> ProductionYear
      - Birth place       -> ProductionLocations[0]

    Clearing behavior:
      - If end_date is provided as an empty string, EndDate will be cleared (set to null).
    """

    item = jf_get_item_dto(jf_url, api_key, item_id, timeout=timeout, user_id=user_id)
    item2 = dict(item)

    changed = False

    def _eq(a: Any, b: Any) -> bool:
        return a == b

    if overview is not None:
        if not _eq(item2.get("Overview") or "", overview):
            item2["Overview"] = overview
            changed = True

    if original_title is not None:
        if not _eq(item2.get("OriginalTitle") or "", original_title):
            item2["OriginalTitle"] = original_title
            changed = True

    if premiere_date is not None:
        # Allow clearing if empty string is passed
        target = premiere_date if premiere_date else None
        if not _eq(item2.get("PremiereDate"), target):
            item2["PremiereDate"] = target
            changed = True

    if end_date is not None:
        # Allow clearing if empty string is passed
        target = end_date if end_date else None
        if not _eq(item2.get("EndDate"), target):
            item2["EndDate"] = target
            changed = True

    if production_year is not None:
        try:
            py_val = int(production_year)
        except Exception:
            py_val = None
        if py_val is not None and not _eq(item2.get("ProductionYear"), py_val):
            item2["ProductionYear"] = py_val
            changed = True

    if production_locations is not None:
        target = list(production_locations) if production_locations else []
        if not _eq(item2.get("ProductionLocations") or [], target):
            item2["ProductionLocations"] = target
            changed = True

    if not changed:
        # Nothing to update (also avoids unnecessary server log noise).
        return

    # Ensure certain collections exist to avoid server-side null handling bugs
    for k in ("Tags", "Genres", "Studios", "People", "MediaStreams", "ImageTags", "LockedFields"):
        if k in item2 and item2[k] is None:
            item2[k] = []
    if "ProductionLocations" in item2 and item2["ProductionLocations"] is None:
        item2["ProductionLocations"] = []

    jf_post_json(jf_url, api_key, f"/Items/{item_id}", item2, timeout=timeout)

def jf_update_person_overview(jf_url: str, api_key: str, item_id: str, overview: str, timeout: int, user_id: str = "") -> None:
    """Backward compatible wrapper."""
    jf_update_person_metadata(jf_url, api_key, item_id, timeout=timeout, user_id=user_id, overview=overview)


# -------------------------
# Main
# -------------------------

def get_performer_id_from_input(inp: Dict[str, Any]) -> Optional[int]:
    args = inp.get("args") or {}
    pid = args.get("performer_id") or args.get("performerId")
    if pid is not None:
        try:
            return int(pid)
        except Exception:
            return None
    hc = args.get("hookContext") or args.get("hook_context") or {}
    hid = hc.get("id") or hc.get("ID")
    if hid is None:
        return None
    try:
        return int(hid)
    except Exception:
        return None


def apply_defaults(settings: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(settings or {})
    out.setdefault("update_image", True)
    out.setdefault("update_overview", True)
    out.setdefault("reencode_images", True)
    out.setdefault("image_upload_format", "png")
    out.setdefault("ffmpeg_path", "ffmpeg")
    out.setdefault("timeout_seconds", 20)
    out.setdefault("dry_run", False)
    out.setdefault("jellyfin_user_id", "")
    return out


def main() -> None:
    inp = read_input()
    args = inp.get("args") or {}
    sc = inp.get("server_connection") or inp.get("serverConnection") or {}
    stash_base = stash_base_from_server_connection(sc)
    cookie = stash_cookie_from_server_connection(sc)

    plugin_dir = stash_plugin_dir_from_server_connection(sc)
    plugin_id = stash_plugin_id_from_plugin_dir(plugin_dir) or "stash_jellyfin_performer_sync"

    settings_from_payload = extract_settings_from_payload(inp)
    settings_from_conf: Dict[str, Any] = {}
    debug: Dict[str, Any] = {}
    try:
        settings_from_conf, debug = fetch_plugin_settings_from_stash(stash_base, cookie, _num(args.get("timeout_seconds"), 20), plugin_id)
    except Exception as e:
        debug = {"settings_fetch_error": str(e), "plugin_id": plugin_id, "plugin_dir": plugin_dir, "cookie_present": bool(cookie), "stash_base": stash_base}

    settings = apply_defaults({**settings_from_conf, **settings_from_payload})

    jf_url = _s(settings.get("jellyfin_url")).rstrip("/")
    jf_key = _s(settings.get("jellyfin_api_key"))
    jf_user = _s(settings.get("jellyfin_user_id"))

    update_image = _bool(settings.get("update_image"), True)
    update_overview = _bool(settings.get("update_overview"), True)
    add_jf_profile_url = _bool(settings.get("add_jellyfin_profile_url"), False)
    reencode = _bool(settings.get("reencode_images"), True)
    upload_fmt = _s(settings.get("image_upload_format") or "png").strip().lower()
    ffmpeg_path = _s(settings.get("ffmpeg_path") or "ffmpeg").strip()
    timeout = _num(settings.get("timeout_seconds"), 20)
    dry_run = _bool(settings.get("dry_run"), False)

    if not jf_url or not jf_key:
        # IMPORTANT: Stash often logs only the "error" string; include diagnostics there.
        diag_parts = []
        if plugin_id:
            diag_parts.append(f"plugin_id={plugin_id}")
        if plugin_dir:
            diag_parts.append(f"plugin_dir={plugin_dir}")
        diag_parts.append(f"cookie={'yes' if cookie else 'no'}")
        if debug.get("plugins_shape"):
            diag_parts.append(f"plugins_shape={debug.get('plugins_shape')}")
        if debug.get("plugins_scalar_query_error"):
            diag_parts.append(f"plugins_scalar_query_error={debug.get('plugins_scalar_query_error')}")
        if debug.get("plugins_list_query_error"):
            diag_parts.append(f"plugins_list_query_error={debug.get('plugins_list_query_error')}")
        if debug.get("plugins_keys_sample"):
            diag_parts.append(f"plugins_keys_sample={debug.get('plugins_keys_sample')}")
        if debug.get("matched_key"):
            diag_parts.append(f"matched_key={debug.get('matched_key')}")
        if debug.get("matched_name"):
            diag_parts.append(f"matched_name={debug.get('matched_name')}")
        if settings_from_payload:
            diag_parts.append(f"payload_settings_keys={sorted(list(settings_from_payload.keys()))}")

        diag = " | ".join(diag_parts)
        jprint({
            "ok": False,
            "error": f"Missing Jellyfin settings (jellyfin_url / jellyfin_api_key). [{diag}]",
            "output": {"debug": debug, "settings_from_conf_keys": sorted(list(settings_from_conf.keys()))[:50]},
        })
        return

    performer_id = get_performer_id_from_input(inp)
    if performer_id is None:
        jprint({"ok": False, "error": "No performer id in hookContext/args"})
        return

    try:
        performer = get_performer(stash_base, cookie, performer_id, timeout)
        name = performer.get("name") or ""
        image_path = performer.get("image_path") or ""
        has_image = bool(image_path)

        mapping = load_map()
        stash_id = str(performer.get("id") or performer_id)
        jf_person_id = mapping.get(stash_id, "")

        # If we already have a cached mapping, and Jellyfin metadata contains a Stash id marker,
        # ensure it matches the current Stash performer id. If it mismatches, drop mapping and re-search.
        if jf_person_id:
            try:
                ok_map, found_map = jf_person_id_verified(jf_url, jf_key, jf_person_id, stash_id, timeout, user_id=jf_user)
                if (not ok_map) and found_map:
                    mapping.pop(stash_id, None)
                    save_map(mapping)
                    jf_person_id = ""
            except Exception:
                pass

        if not jf_person_id:
            hints = jf_search_people(jf_url, jf_key, name, timeout, user_id=jf_user)
            jf_person_id = pick_person_id_with_stash_verification(jf_url, jf_key, hints, name, stash_id, timeout, user_id=jf_user) or ""
            if jf_person_id:
                mapping[stash_id] = jf_person_id
                save_map(mapping)

        if not jf_person_id:
            jprint({"ok": False, "error": f"Jellyfin person not found for '{name}' (stash id={stash_id})"})
            return

        actions: List[str] = []
        # Optional: save Jellyfin performer profile URL back into Stash performer Links/URLs
        if add_jf_profile_url:
            jf_profile = jf_person_web_url(jf_url, jf_person_id)
            if dry_run:
                actions.append(f"[DRY RUN] Would add Jellyfin profile URL to Stash performer links: {jf_profile}")
            else:
                try:
                    changed = stash_add_jellyfin_profile_url_to_performer(stash_base, cookie, timeout, performer_id, performer, jf_profile)
                    if changed:
                        actions.append("Saved Jellyfin profile URL to Stash performer links.")
                    else:
                        actions.append("Stash performer links already contain the Jellyfin profile URL (no change).")
                except Exception as _e:
                    actions.append(f"Failed to save Jellyfin URL to Stash: {_e}")


        # 1) Update Jellyfin person text fields (Overview + Original title + Birth/Death + Birthplace)
        if update_overview:
            overview = build_jellyfin_overview_from_stash(performer)

            aliases_str = _extract_aliases_str(performer)
            birth_raw = _extract_birthdate(performer)
            death_raw = _extract_deathdate(performer)
            birth_date = _date_only(birth_raw)
            death_date = _date_only(death_raw)

            premiere_date = _jf_datetime_z(birth_date) if birth_date else ""
            end_date = _jf_datetime_z(death_date) if death_date else ""
            birth_year = int(birth_date[:4]) if birth_date else None

            birth_place = _extract_country_name(performer)
            production_locations = [birth_place] if birth_place else None

            should_clear_end_date = (not death_date)

            needs_update = any([
                bool(overview),
                bool(aliases_str),
                bool(premiere_date),
                bool(end_date),
                should_clear_end_date,  # clear Jellyfin EndDate if Stash has no death date
                birth_year is not None,
                bool(production_locations),
            ])

            if needs_update:
                if dry_run:
                    actions.append(f"[DRY RUN] Would update Jellyfin person fields for {jf_person_id} (overview/original title/dates/birthplace).")
                else:
                    jf_update_person_metadata(
                        jf_url,
                        jf_key,
                        jf_person_id,
                        timeout=timeout,
                        user_id=jf_user,
                        overview=overview if overview else None,
                        original_title=aliases_str if aliases_str else None,
                        premiere_date=premiere_date if premiere_date else None,
                        end_date=end_date,  # empty string clears EndDate

                        production_year=birth_year,
                        production_locations=production_locations,
                    )
                    actions.append("Updated Jellyfin performer overview + person fields.")
            else:
                actions.append("No requested performer fields found in Stash (skipped Jellyfin overview/field update).")
        else:
            actions.append("update_overview is disabled (skipped Jellyfin overview/field update).")

        # 2) Update Primary image (optional)
        if update_image:
            if not has_image:
                actions.append("Stash performer has no image (skipped Jellyfin image upload).")
            else:
                img_bytes, img_ct = fetch_stash_image(stash_base, cookie, image_path, timeout)

                out_bytes, out_ct = img_bytes, img_ct
                if reencode:
                    conv = reencode_image(img_bytes, upload_fmt, ffmpeg_path)
                    if conv:
                        out_bytes, out_ct = conv

                if dry_run:
                    actions.append(
                        f"[DRY RUN] Would upload image to Jellyfin person {jf_person_id} ({out_ct}, {len(out_bytes)} bytes)."
                    )
                else:
                    jf_post_binary(
                        jf_url, jf_key, f"/Items/{jf_person_id}/Images/Primary", out_bytes, out_ct, timeout=timeout
                    )
                    actions.append(
                        f"Uploaded performer image to Jellyfin person {jf_person_id} ({out_ct}, {len(out_bytes)} bytes)."
                    )
        else:
            actions.append("update_image is disabled (skipped Jellyfin image upload).")

        jprint({"ok": True, "output": {"message": " ".join(actions)}})
    except Exception as e:
        jprint({"ok": False, "error": str(e)})


if __name__ == "__main__":
    main()