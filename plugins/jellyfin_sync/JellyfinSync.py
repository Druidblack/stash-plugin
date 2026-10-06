#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Jellyfin Sync v0.3.26

Scene sync:
- Scene.Update.Post -> use Jellyfin ItemId from scene URLs when present.
- If no Jellyfin URL exists, fall back to the original v0.2.x scene search methods
  (exact path, title/filename variants, Search/Hints with date/path/performer narrowing).

Performer sync:
- Performer.Create.Post / Performer.Update.Post -> read Jellyfin PersonId only from
  performer URLs -> update that exact Jellyfin Person metadata and/or Primary image.
- Performer lookup remains direct-URL-only; no performer name/alias search or local mapping is used.

Playback position sync:
- Optional UI-assisted operation copies Stash Scene.resume_time (seconds) to the exact
  Jellyfin item UserData.PlaybackPositionTicks for the configured Jellyfin user.
- No watched/play-count state is changed.
"""

import base64
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
import stashapi.log as log
from stashapi.stashapp import StashInterface

try:
    import JellyfinCoverGenerator as covergen
except Exception as _cover_import_error:
    covergen = None
else:
    _cover_import_error = None


PLUGIN_VERSION = "0.3.26"
ITEM_ID_RE = re.compile(r"^[0-9a-fA-F]{32}$")
HTTP_TIMEOUT = 40
PROCESS_PERFORMERS_JOB_TEXT = "process performers"
DEFERRED_FAVORITE_INITIAL_DELAY_SECONDS = 1.0
DEFERRED_FAVORITE_POLL_SECONDS = 0.75
DEFERRED_FAVORITE_RUNNING_WAIT_SECONDS = 180.0
DEFERRED_FAVORITE_MAX_REQUEUES = 12
COVER_UPLOAD_DELAY_SECONDS = 5
GENERATED_COVERS_SUBDIR = "generated"
_PERFORMER_FIELDS_CACHE: Optional[Dict[str, Tuple[str, str]]] = None
_VERBOSE_LOGGING = False


def _verbose(message: str) -> None:
    """Diagnostic details (in Stash's log only when verbose mode is enabled)."""
    if _VERBOSE_LOGGING:
        log.info(message)


def _info(message: str) -> None:
    """Actionable changes and concise task progress."""
    log.info(message)



# ---------------------------------------------------------------------------
# Generic helpers / direct Jellyfin URL parsing
# ---------------------------------------------------------------------------

def _s(v: Any) -> str:
    return "" if v is None else str(v)


def _bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _cover_upload_delay_seconds(settings: Dict[str, Any]) -> float:
    raw = settings.get("coverUploadDelaySeconds")
    try:
        value = float(raw if raw is not None else 5.0)
    except (TypeError, ValueError):
        value = 5.0
    if value < 0:
        value = 0.0
    if value > 300:
        value = 300.0
    return value


def _excluded_cover_directories(settings: Dict[str, Any]) -> List[str]:
    """Configured absolute Stash *media* paths, one per line or separated by ;.

    The values are compared against Stash scene.files[].path, NOT the host OS
    paths seen by Jellyfin.  Normalize Windows backslashes for both sides.
    """
    raw = settings.get("coverExcludedDirectories") or ""
    if isinstance(raw, (list, tuple)):
        lines = [str(item) for item in raw]
    else:
        lines = re.split(r"[\r\n;]+", str(raw))
    result: List[str] = []
    for line in lines:
        path = str(line).strip().strip('"').strip("'").strip()
        if not path:
            continue
        path = re.sub(r"/+", "/", path.replace("\\", "/"))
        normalized = path.rstrip("/") or "/"
        if normalized not in result:
            result.append(normalized)
    return result


def _excluded_cover_directory(scene: Dict[str, Any], settings: Dict[str, Any]) -> Optional[str]:
    """Return the matching directory (or unknown-path marker) for auto/bulk work.

    A directory matches only itself and its descendants, never similarly named
    siblings such as /movies-2 for /movies.  If exclusions are configured but a
    scene has no media path, fail closed to protect movies with custom posters.
    """
    roots = _excluded_cover_directories(settings)
    if not roots:
        return None
    paths = [
        str(f.get("path") or "").strip()
        for f in (scene.get("files") or [])
        if isinstance(f, dict) and f.get("path")
    ]
    if not paths and scene.get("path"):
        paths = [str(scene["path"]).strip()]
    if not paths:
        return "<missing Stash media path>"
    for original in paths:
        item = re.sub(r"/+", "/", original.replace("\\", "/")).rstrip("/") or "/"
        windows_path = bool(re.match(r"^[A-Za-z]:/", item)) or original.startswith("\\\\")
        for root in roots:
            case_insensitive = windows_path or bool(re.match(r"^[A-Za-z]:/", root))
            path_cmp = item.casefold() if case_insensitive else item
            root_cmp = root.casefold() if case_insensitive else root
            if root_cmp == "/":
                matches = path_cmp.startswith("/")
            else:
                matches = path_cmp == root_cmp or path_cmp.startswith(root_cmp + "/")
            if matches:
                return root
    return None


def _iter_entity_urls(entity: Dict[str, Any]) -> Iterable[str]:
    """Yield URL strings from current/older Stash URL field shapes."""
    raw = entity.get("urls")
    if raw is None:
        raw = entity.get("Urls") or entity.get("URLS")

    if isinstance(raw, list):
        for value in raw:
            if isinstance(value, str):
                url = value.strip()
            elif isinstance(value, dict):
                url = str(
                    value.get("url")
                    or value.get("URL")
                    or value.get("link")
                    or value.get("value")
                    or ""
                ).strip()
            else:
                url = ""
            if url:
                yield url

    elif isinstance(raw, dict):
        for value in raw.values():
            if isinstance(value, str) and value.strip():
                yield value.strip()

    elif isinstance(raw, str):
        for line in raw.splitlines():
            if line.strip():
                yield line.strip()

    # Older Stash builds may expose a single `url` field instead of `urls`.
    single = entity.get("url") or entity.get("URL")
    if isinstance(single, str):
        for line in single.splitlines():
            if line.strip():
                yield line.strip()


def _extract_jellyfin_item_id_from_url(url: str) -> Optional[str]:
    """Extract a Jellyfin item/person id from known direct URL forms."""
    if not url:
        return None

    match = re.search(r"\bjellyfin/items/([0-9a-fA-F]{32})\b", url, flags=re.IGNORECASE)
    if match:
        return match.group(1)

    match = re.search(r"/Items/([0-9a-fA-F]{32})(?:\b|/|\?)", url, flags=re.IGNORECASE)
    if match:
        return match.group(1)

    if "#" in url:
        fragment = url.split("#", 1)[1]
        if "details" in fragment.lower() and "?" in fragment:
            query = fragment.split("?", 1)[1]
            values = parse_qs(query)
            item_id = (values.get("id") or values.get("Id") or [None])[0]
            if isinstance(item_id, str) and ITEM_ID_RE.fullmatch(item_id):
                return item_id

    return None


def _find_direct_jellyfin_id(entity: Dict[str, Any], what: str) -> Optional[str]:
    for url in _iter_entity_urls(entity):
        item_id = _extract_jellyfin_item_id_from_url(url)
        if item_id:
            _verbose(f"Found Jellyfin id in Stash {what} URL: {item_id}")
            return item_id
    return None


# ---------------------------------------------------------------------------
# Scene fallback search helpers restored from the original plugin
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    """Normalize strings for comparisons.

    Besides whitespace/lowercasing, we normalize a few punctuation variants that
    often differ between Stash (scraped titles) and Jellyfin (filename-derived
    titles), such as smart quotes and the Unicode ellipsis.
    """
    s = (s or "").strip()

    # Normalize ellipsis and long dot runs.
    s = s.replace("…", "...")
    s = re.sub(r"\.{3,}", "...", s)

    # Normalize common quote/apostrophe characters.
    s = s.translate(str.maketrans({
        "“": '"', "”": '"', "„": '"', "‟": '"',
        "‘": "'", "’": "'", "‚": "'", "‛": "'",
        "—": "-", "–": "-",
        "\u00A0": " ",
    }))

    s = re.sub(r"\s+", " ", s)
    return s.lower()


def _title_search_variants(s: str) -> List[str]:
    """Generate a small set of alternative title variants for Jellyfin search.

    Covers common differences like:
      “We Could Just Share…”  vs  "We Could Just Share..."
    """
    s0 = (s or "").strip()
    if not s0:
        return []

    out: List[str] = []

    def _add(x: str):
        x = (x or "").strip()
        if x and x not in out:
            out.append(x)

    _add(s0)

    # Punctuation-normalized variant (straight quotes + '...')
    s1 = s0.replace("…", "...")
    s1 = re.sub(r"\.{3,}", "...", s1)
    s1 = s1.translate(str.maketrans({
        "“": '"', "”": '"', "„": '"', "‟": '"',
        "‘": "'", "’": "'", "‚": "'", "‛": "'",
        "—": "-", "–": "-",
        "\u00A0": " ",
    }))
    s1 = re.sub(r"\s+", " ", s1).strip()
    _add(s1)

    # If we have three dots, also try a Unicode ellipsis (some sources keep it).
    if "..." in s1:
        _add(s1.replace("...", "…"))

    return out


def _basename_no_ext(path: str) -> str:
    if not path:
        return ""
    base = os.path.basename(path)
    return os.path.splitext(base)[0]


def _strip_quality_suffix(name: str) -> str:
    """Return a filename-like title without trailing quality markers.

    Jellyfin sometimes derives an item's Name from the filename, but may
    strip a trailing quality marker like " - [WEBDL-1080p]". When we search by
    Stash's filename (which still contains that marker), the search can fail.

    We keep this deliberately conservative:
    - removes extension (if provided)
    - removes a trailing " - [ ... ]" or "[ ... ]" block
    - trims a dangling separator at the end
    """
    if not name:
        return ""

    s = str(name).strip()
    s = os.path.splitext(s)[0]

    # Remove trailing quality tags in square brackets.
    s2 = re.sub(r"\s*-\s*\[[^\]]+\]\s*$", "", s)
    s2 = re.sub(r"\s*\[[^\]]+\]\s*$", "", s2)

    # Cleanup leftover trailing separators.
    s2 = re.sub(r"\s*[-–—]\s*$", "", s2).strip()
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2


_TRAIL_PUNCT_CHARS = set('.!?…,:;"\'“”„‟‘’‚‛()[]{}<>«»')


def _strip_trailing_punct(name: str) -> str:
    """Strip trailing punctuation/quotes from a title.

    Jellyfin sometimes drops terminal punctuation when deriving titles from
    filenames (e.g. an ellipsis at the end). As a fallback we try search terms
    without terminal punctuation so:
        "She Sounds Just Like You…"  ->  "She Sounds Just Like You"
    """
    if not name:
        return ""

    s = str(name).strip()
    # Strip trailing punctuation/quotes/brackets.
    while s:
        s2 = s.rstrip()
        if not s2:
            s = ""
            break
        if s2[-1] in _TRAIL_PUNCT_CHARS:
            s = s2[:-1].rstrip()
            continue
        break

    s = re.sub(r"\s+", " ", s).strip()
    return s


def _derive_truncated_filename_terms(filename_no_ext: str) -> List[str]:
    """Return extra Jellyfin search terms derived from the filename.

    In some cases Jellyfin stores a *shortened* item Name (especially before
    any metadata identification happens). Example:
      "2026-02-01 - Studio - February 2026 Something - S31-E4 - [WEBDL-2160p]"
    may end up as:
      "2026-02-01 - Studio - February"

    This helper generates progressively shorter candidates so we can still
    resolve the itemId.
    """
    base = _strip_quality_suffix(filename_no_ext)
    if not base:
        return []

    out: List[str] = []

    def _add(x: str):
        x = (x or "").strip()
        if x and x not in out and x != base:
            out.append(x)

    # 1) Remove a trailing season/episode token when present.
    no_ep = re.sub(r"\s*-\s*S\d{1,3}\s*[-_ ]?E\d{1,3}\s*$", "", base, flags=re.IGNORECASE)
    no_ep = re.sub(r"\s*-\s*E\d{1,3}\s*$", "", no_ep, flags=re.IGNORECASE)
    no_ep = re.sub(r"\s*[-–—]\s*$", "", no_ep).strip()
    _add(no_ep)

    # 2) Use first 3 " - " segments (very common Jellyfin truncation pattern).
    segs = [s.strip() for s in base.split(" - ") if (s or "").strip()]

    # Drop trailing episode-like segments if they survived splitting.
    while segs and re.fullmatch(r"S\d{1,3}\s*[-_ ]?E\d{1,3}", segs[-1], flags=re.IGNORECASE):
        segs.pop()
    while segs and re.fullmatch(r"E\d{1,3}", segs[-1], flags=re.IGNORECASE):
        segs.pop()

    if len(segs) >= 3:
        first3 = " - ".join(segs[:3]).strip()
        _add(first3)

        # 3) If the 3rd segment is long, Jellyfin may keep only part of it.
        #    - Often the first word (e.g. a month name like "February").
        #    - In some cases Jellyfin appears to stop at the first digit within the 3rd segment
        #      (e.g. "February 2026 ..." -> "February"). If the segment contains any digits,
        #      we truncate from the first digit onward.
        third = segs[2]
        third_short = ""

        m = re.search(r"\d", third)
        if m:
            third_short = third[: m.start()].strip()
            third_short = re.sub(r"\s*[-–—]\s*$", "", third_short).strip()

        if not third_short:
            words = [w for w in re.split(r"\s+", third) if w]
            if words:
                w0 = words[0]
                # Keep month name or generic first word.
                third_short = w0

        if third_short:
            short3 = f"{segs[0]} - {segs[1]} - {third_short}".strip()
            _add(short3)

    return out


def _stash_scene_primary_file_path(scene: dict) -> str:
    """Best-effort extraction of a primary file path from a Stash scene."""
    files = scene.get("files") or []
    for f in files:
        if isinstance(f, dict) and f.get("path"):
            return f["path"]
    if scene.get("path"):
        return scene["path"]
    return ""


def _parse_iso_date(value: str) -> Optional[datetime.date]:
    """Parse YYYY-MM-DD from a string.

    Accepts full ISO timestamps too, using only the date part.
    """
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Common case: "YYYY-MM-DD"
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except Exception:
            return None

    return None


def _extract_leading_date(value: str) -> Optional[datetime.date]:
    """Extract a leading YYYY-MM-DD date from a filename/title."""
    if not value:
        return None
    s = str(value).strip()
    m = re.match(r"^(\d{4}-\d{2}-\d{2})\b", s)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception:
        return None


def _scene_date_candidates(scene: dict, stash_path: str, scene_title: str) -> List[datetime.date]:
    """Return acceptable dates for matching.

    Primary source: Stash scene.date.
    Fallbacks: leading YYYY-MM-DD in filename or title.

    Tolerance: also allow (date - 1 day), because Jellyfin иногда сохраняет дату на день раньше.
    """
    d = _parse_iso_date(scene.get("date") or "")
    if not d:
        d = _extract_leading_date(_basename_no_ext(stash_path or ""))
    if not d:
        d = _extract_leading_date(scene_title or "")
    if not d:
        return []
    return [d, (d - timedelta(days=1))]


def _candidate_item_date(item: Dict[str, Any]) -> Optional[datetime.date]:
    """Best-effort candidate date from Jellyfin search result.

    Order:
    1) PremiereDate (if present)
    2) leading YYYY-MM-DD in Path basename
    3) leading YYYY-MM-DD in Name
    """
    d = _parse_iso_date(item.get("PremiereDate") or item.get("premiereDate") or "")
    if d:
        return d

    p = item.get("Path") or item.get("path") or ""
    if p:
        bn = _basename_no_ext(p)
        d2 = _extract_leading_date(bn)
        if d2:
            return d2

    nm = item.get("Name") or item.get("name") or ""
    if nm:
        d3 = _extract_leading_date(nm)
        if d3:
            return d3

    return None


def _scene_performer_names(scene: dict) -> List[str]:
    """Extract performer names from Stash scene (best-effort)."""
    out: List[str] = []
    for p in (scene.get("performers") or []):
        if isinstance(p, dict):
            name = (p.get("name") or "").strip()
            if name and name not in out:
                out.append(name)
        elif isinstance(p, str):
            name = p.strip()
            if name and name not in out:
                out.append(name)
    return out


def _basename_matches_stash(item_path: str, stash_path: str) -> int:
    """Return match strength between item path and stash path basenames."""
    stash_bn_raw = _norm(_basename_no_ext(stash_path))
    if not stash_bn_raw:
        return 0
    bn = _norm(_basename_no_ext(item_path or ""))
    if not bn:
        return 0
    if bn == stash_bn_raw:
        return 3
    if stash_bn_raw in bn or bn in stash_bn_raw:
        return 1
    return 0


def _build_headers(api_key: str) -> dict:
    # Jellyfin accepts X-Emby-Token and/or Authorization MediaBrowser header.
    # Some clients also send X-MediaBrowser-Token.
    return {
        "X-Emby-Token": api_key,
        "X-MediaBrowser-Token": api_key,
        "Authorization": f'MediaBrowser Token="{api_key}"',
        "Accept": "application/json",
    }


def jellyfin_get(base_url: str, api_key: str, path: str, params: Optional[dict] = None, verify_tls: bool = True) -> requests.Response:
    url = base_url.rstrip("/") + path
    return requests.get(url, headers=_build_headers(api_key), params=params or {}, timeout=20, verify=verify_tls)


def jellyfin_pick_user_id(base_url: str, api_key: str, verify_tls: bool) -> Optional[str]:
    """Pick a reasonable userId automatically (prefer admin). Used only for fallback search."""
    r = jellyfin_get(base_url, api_key, "/Users", verify_tls=verify_tls)
    if not r.ok:
        log.error(f"Jellyfin /Users failed: HTTP {r.status_code}: {r.text}")
        return None

    try:
        users = r.json()
    except Exception:
        log.error("Jellyfin /Users returned non-JSON response")
        return None

    for u in users:
        try:
            if u.get("Policy", {}).get("IsAdministrator"):
                return u.get("Id")
        except Exception:
            pass

    if users:
        return users[0].get("Id")
    return None


def jellyfin_search_item_user_scope(base_url: str, api_key: str, user_id: str, search_term: str, limit: int, verify_tls: bool) -> List[Dict[str, Any]]:
    """Fallback search under /Users/{userId}/Items."""
    params = {
        "Recursive": "true",
        "IncludeItemTypes": "Video",
        "SearchTerm": search_term,
        # We request a couple of extra fields so we can disambiguate duplicates
        # by date (PremiereDate) when multiple items share the same Name.
        "Fields": "Path,PremiereDate",
        "Limit": str(limit),
    }
    r = jellyfin_get(base_url, api_key, f"/Users/{user_id}/Items", params=params, verify_tls=verify_tls)
    if not r.ok:
        log.error(f"Jellyfin search failed: HTTP {r.status_code}: {r.text}")
        return []

    try:
        data = r.json()
    except Exception:
        log.error("Jellyfin search returned non-JSON response")
        return []

    items = data.get("Items") or []
    if items:
        return items

    # Fallback: if Jellyfin omits terminal punctuation in titles, retry without it.
    t2 = _strip_trailing_punct(search_term)
    if t2 and t2 != search_term:
        params["SearchTerm"] = t2
        r2 = jellyfin_get(base_url, api_key, f"/Users/{user_id}/Items", params=params, verify_tls=verify_tls)
        if r2.ok:
            try:
                data2 = r2.json()
                items2 = data2.get("Items") or []
                if items2:
                    _verbose(f"Fallback search without trailing punctuation: '{t2}'")
                    return items2
            except Exception:
                pass

    return []


def jellyfin_search_hints(base_url: str, api_key: str, user_id: Optional[str], search_term: str, limit: int, verify_tls: bool) -> List[Dict[str, Any]]:
    """Search via /Search/Hints (often available even when other search endpoints are restricted).

    Returns a list of hints (dicts). Item id is typically in fields like Id or ItemId.
    """
    if not search_term:
        return []

    params = {
        "SearchTerm": search_term,
        "Limit": str(limit),
    }
    uid = (user_id or '').strip()
    if uid:
        params["UserId"] = uid

    r = jellyfin_get(base_url, api_key, "/Search/Hints", params=params, verify_tls=verify_tls)
    if not r.ok:
        log.warning(f"Jellyfin /Search/Hints failed: HTTP {r.status_code}: {r.text}")
        return []

    try:
        data = r.json()
    except Exception:
        log.warning("Jellyfin /Search/Hints returned non-JSON response")
        return []

    # Jellyfin may return either a list or an object with 'SearchHints'
    hints: List[Dict[str, Any]] = []
    if isinstance(data, list):
        hints = data
    elif isinstance(data, dict):
        hints = data.get('SearchHints') or data.get('Items') or []
    else:
        hints = []

    if hints:
        return hints

    # Fallback: retry without terminal punctuation (e.g. ellipsis at the end).
    t2 = _strip_trailing_punct(search_term)
    if t2 and t2 != search_term:
        params["SearchTerm"] = t2
        r2 = jellyfin_get(base_url, api_key, "/Search/Hints", params=params, verify_tls=verify_tls)
        if r2.ok:
            try:
                data2 = r2.json()
                hints2: List[Dict[str, Any]] = []
                if isinstance(data2, list):
                    hints2 = data2
                elif isinstance(data2, dict):
                    hints2 = data2.get('SearchHints') or data2.get('Items') or []
                if hints2:
                    _verbose(f"Fallback hints without trailing punctuation: '{t2}'")
                    return hints2
            except Exception:
                pass

    return []


def _hint_get_item_id(h: Dict[str, Any]) -> Optional[str]:
    for key in ("Id", "ItemId", "ItemID", "itemId", "id"):
        v = h.get(key)
        if isinstance(v, str) and re.fullmatch(r"[0-9a-fA-F]{32}", v):
            return v
    return None


def collect_hint_ids(hints: List[Dict[str, Any]], stash_path: str, search_term: str, scene_title: str) -> List[str]:
    """Return ordered hint itemIds for disambiguation.

    We keep the same priority as pick_best_hint but return all candidates, not just the first.
    """
    bn_raw = _norm(_basename_no_ext(stash_path))
    bn_clean = _norm(_strip_quality_suffix(_basename_no_ext(stash_path)))
    title_raw = _norm(scene_title or "")
    title_clean = _norm(_strip_quality_suffix(scene_title or ""))
    term_norms = set(_norm(v) for v in _title_search_variants(search_term) if v)

    exact_fn: List[str] = []
    exact_title: List[str] = []
    exact_term: List[str] = []
    loose: List[str] = []

    for h in hints:
        name = _norm(h.get('Name') or h.get('name') or "")
        if not name:
            continue
        hid = _hint_get_item_id(h)
        if not hid:
            continue

        if bn_raw and name == bn_raw:
            if hid not in exact_fn:
                exact_fn.append(hid)
            continue
        if bn_clean and name == bn_clean:
            if hid not in exact_fn:
                exact_fn.append(hid)
            continue
        if title_raw and name == title_raw:
            if hid not in exact_title:
                exact_title.append(hid)
            continue
        if title_clean and name == title_clean:
            if hid not in exact_title:
                exact_title.append(hid)
            continue
        if term_norms and name in term_norms:
            if hid not in exact_term:
                exact_term.append(hid)
            continue
        if bn_raw and (bn_raw in name or name in bn_raw):
            if hid not in loose:
                loose.append(hid)
            continue
        if bn_clean and (bn_clean in name or name in bn_clean):
            if hid not in loose:
                loose.append(hid)
            continue
        if title_raw and (title_raw in name or name in title_raw):
            if hid not in loose:
                loose.append(hid)
            continue
        if title_clean and (title_clean in name or name in title_clean):
            if hid not in loose:
                loose.append(hid)
            continue

    # priority order
    out: List[str] = []
    for bucket in (exact_fn, exact_title, exact_term, loose):
        for x in bucket:
            if x not in out:
                out.append(x)
    return out


def narrow_items_for_scene(
    items: List[Dict[str, Any]],
    stash_path: str,
    scene: dict,
    scene_title: str,
    search_term: str,
) -> List[Dict[str, Any]]:
    """Narrow Jellyfin search results to the best candidates.

    This is primarily used to avoid wrong matches when multiple items share
    the same (or very similar) title in Jellyfin.

    Strategy (in order):
    1) Prefer items whose Path basename matches the Stash scene's filename.
    2) Prefer exact Name match to the searched term (with small punctuation variants).
    3) Prefer matching date (scene date or scene date - 1 day). The candidate date
       is taken from PremiereDate, or derived from Path/Name leading YYYY-MM-DD.
    """
    if not items:
        return []

    candidates = [it for it in items if isinstance(it, dict) and it.get("Id")]
    if len(candidates) <= 1:
        return candidates

    # 1) Path basename match strength
    strengths = []
    best_s = 0
    for it in candidates:
        s = _basename_matches_stash(it.get("Path") or "", stash_path)
        strengths.append(s)
        best_s = max(best_s, s)
    if best_s > 0:
        candidates2 = [it for it, s in zip(candidates, strengths) if s == best_s]
        if candidates2:
            candidates = candidates2
        if len(candidates) <= 1:
            return candidates

    # 2) Exact Name match to search term (normalized)
    term_norms = set(_norm(v) for v in _title_search_variants(search_term) if v)
    if term_norms:
        exact_name = [it for it in candidates if _norm(it.get("Name") or it.get("name") or "") in term_norms]
        if exact_name:
            candidates = exact_name
        if len(candidates) <= 1:
            return candidates

    # 3) Date narrowing (scene date or -1 day)
    acceptable_dates = set(_scene_date_candidates(scene, stash_path=stash_path, scene_title=scene_title))
    if acceptable_dates:
        by_date = [it for it in candidates if _candidate_item_date(it) in acceptable_dates]
        if by_date:
            candidates = by_date

    return candidates


def _collection_to_include_item_types(collection_type: Optional[str]) -> str:
    if not collection_type:
        return "VideoFile,Movie"
    ct = collection_type.lower()
    if ct == "tvshows":
        return "Episode"
    if ct == "books":
        return "Book"
    if ct == "music":
        return "Audio"
    if ct == "movie":
        return "VideoFile,Movie"
    return "VideoFile,Movie"


def jellyfin_virtual_folders(base_url: str, api_key: str, verify_tls: bool) -> Optional[List[Dict[str, Any]]]:
    r = jellyfin_get(base_url, api_key, "/Library/VirtualFolders", verify_tls=verify_tls)
    if not r.ok:
        # Some setups return 403 if token lacks permissions.
        log.warning(f"Jellyfin /Library/VirtualFolders failed: HTTP {r.status_code}: {r.text}")
        return None
    try:
        return r.json()
    except Exception:
        log.warning("Jellyfin /Library/VirtualFolders returned non-JSON response")
        return None


def match_virtual_folders(vfolders: List[Dict[str, Any]], file_path: str) -> List[Dict[str, Any]]:
    matched: List[Dict[str, Any]] = []
    if not file_path:
        return matched

    for vf in vfolders:
        locations = vf.get("Locations") or vf.get("locations") or []
        for loc in locations:
            if not isinstance(loc, str) or not loc:
                continue
            # Make matching tolerant to missing trailing slashes
            if file_path == loc or file_path.startswith(loc.rstrip("/") + "/"):
                matched.append(vf)
                break

    return matched


def jellyfin_find_item_id_by_exact_path(
    base_url: str,
    api_key: str,
    user_id: Optional[str],
    file_path: str,
    vfolders: List[Dict[str, Any]],
    item_limit: int,
    max_pages: int,
    verify_tls: bool,
) -> Optional[str]:
    """Find itemId by enumerating items within matched libraries and comparing Item.Path."""
    if not file_path:
        return None

    matched = match_virtual_folders(vfolders, file_path)
    if not matched:
        return None

    for vf in matched:
        parent_id = vf.get("ItemId") or vf.get("item_id") or vf.get("itemId")
        collection_type = vf.get("CollectionType") or vf.get("collection_type")
        if not parent_id:
            continue

        include_types = _collection_to_include_item_types(collection_type)

        base_params = {
            "Recursive": "true",
            "Fields": "Path",
            "EnableImages": "false",
            "EnableTotalRecordCount": "false",
            "ParentId": str(parent_id),
            "Limit": str(item_limit),
        }
        if include_types:
            base_params["IncludeItemTypes"] = include_types

        # Page through items until we find exact match
        for page in range(max_pages):
            params = dict(base_params)
            params["StartIndex"] = str(page * item_limit)

            r = jellyfin_get(base_url, api_key, "/Items", params=params, verify_tls=verify_tls)
            if (not r.ok) and user_id:
                r = jellyfin_get(base_url, api_key, f"/Users/{user_id}/Items", params=params, verify_tls=verify_tls)
            if not r.ok:
                log.warning(
                    f"Jellyfin /Items failed (ParentId={parent_id}, page={page}): HTTP {r.status_code}: {r.text}"
                )
                break

            try:
                data = r.json()
            except Exception:
                log.warning("Jellyfin /Items returned non-JSON response")
                break

            items = data.get("Items") or []
            for it in items:
                if it.get("Path") == file_path:
                    return it.get("Id")

            if len(items) < item_limit:
                break

    return None


def jellyfin_get_item_details(
    base_url: str,
    api_key: str,
    item_id: str,
    user_id: Optional[str],
    verify_tls: bool,
) -> Optional[Dict[str, Any]]:
    """Fetch minimal item details needed for disambiguation.

    Returns a dict containing at least: Id, Name, Path, PremiereDate (when available).
    Prefers user-scoped endpoints because some servers reject /Items/{id}.
    """
    if not item_id:
        return None

    uid = (user_id or '').strip()
    if not uid:
        uid = jellyfin_pick_user_id(base_url, api_key, verify_tls=verify_tls) or ''

    params = {"Fields": "Path,PremiereDate"}

    def _try(ep: str) -> Optional[Dict[str, Any]]:
        r = jellyfin_get(base_url, api_key, ep, params=params, verify_tls=verify_tls)
        if not r.ok:
            return None
        try:
            return r.json()
        except Exception:
            return None

    data = None
    if uid:
        data = _try(f"/Users/{uid}/Items/{item_id}")
    if not data:
        data = _try(f"/Items/{item_id}")

    if not isinstance(data, dict):
        return None

    # Some endpoints may return MediaSources.Path rather than top-level Path
    p = data.get("Path")
    if not p:
        ms = data.get("MediaSources") or []
        if ms and isinstance(ms, list) and isinstance(ms[0], dict):
            p = ms[0].get("Path")
            if p:
                data["Path"] = p

    if not data.get("Id"):
        data["Id"] = item_id
    return data

# ---------------------------------------------------------------------------
# Stash GraphQL helpers for performer data
# ---------------------------------------------------------------------------

def _stash_base_from_server_connection(sc: Dict[str, Any]) -> str:
    scheme = sc.get("Scheme") or sc.get("scheme") or "http"
    host = sc.get("Host") or sc.get("host") or "localhost"
    port = sc.get("Port") or sc.get("port") or 9999
    if host == "0.0.0.0":
        host = "localhost"
    return f"{scheme}://{host}:{port}"


def _stash_cookie_from_server_connection(sc: Dict[str, Any]) -> str:
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
            value = str(x.get("Value") or x.get("value") or "").strip()
            if name and value:
                return f"{name}={value}"
            raw = x.get("Raw") or x.get("raw") or ""
            return raw.strip() if isinstance(raw, str) else ""
        return ""

    if isinstance(c, list):
        return "; ".join(p for p in (_one(x) for x in c) if p)
    return _one(c)


def _gql_post(stash_base: str, cookie: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    headers: Dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
    if cookie:
        headers["Cookie"] = cookie
    response = requests.post(
        f"{stash_base}/graphql",
        headers=headers,
        json={"query": query, "variables": variables},
        timeout=HTTP_TIMEOUT,
    )
    text = (response.text or "").strip()
    try:
        data = response.json() if text else {}
    except Exception:
        data = {"raw": text}
    if isinstance(data, dict) and data.get("errors"):
        raise RuntimeError(f"Stash GraphQL errors: {data['errors']}")
    if response.status_code >= 400:
        raise RuntimeError(f"Stash GraphQL HTTP {response.status_code}: {text[:1200]}")
    return data


def _unwrap_gql_type(t: Any) -> Tuple[str, str]:
    cur = t or {}
    while isinstance(cur, dict) and cur.get("kind") in ("NON_NULL", "LIST") and cur.get("ofType"):
        cur = cur.get("ofType")
    return str((cur or {}).get("kind") or ""), str((cur or {}).get("name") or "")


def _introspect_performer_fields(stash_base: str, cookie: str) -> Dict[str, Tuple[str, str]]:
    global _PERFORMER_FIELDS_CACHE
    if _PERFORMER_FIELDS_CACHE is not None:
        return _PERFORMER_FIELDS_CACHE

    query = """
    query IntrospectPerformer {
      __type(name: "Performer") {
        fields {
          name
          type {
            kind
            name
            ofType { kind name ofType { kind name ofType { kind name } } }
          }
        }
      }
    }
    """
    data = _gql_post(stash_base, cookie, query, {})
    fields = (((data.get("data") or {}).get("__type") or {}).get("fields") or [])
    out: Dict[str, Tuple[str, str]] = {}
    for field in fields:
        if not isinstance(field, dict) or not field.get("name"):
            continue
        out[str(field["name"])] = _unwrap_gql_type(field.get("type") or {})
    _PERFORMER_FIELDS_CACHE = out
    return out


def _get_performer(stash_base: str, cookie: str, performer_id: str) -> Dict[str, Any]:
    base_fields = ["id", "name", "image_path"]
    desired_map: Dict[str, List[str]] = {
        "details": ["details"],
        "aliases": ["aliases", "alias_list", "aliasList"],
        "birthdate": ["birthdate", "birth_date", "birthDate", "date_of_birth", "dateOfBirth", "dob"],
        "deathdate": ["deathdate", "death_date", "deathDate", "date_of_death", "dateOfDeath", "dod"],
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
        "career_start": ["career_start", "careerStart"],
        "career_end": ["career_end", "careerEnd"],
        "career_length": ["career_length", "careerLength"],
        "urls": ["urls"],
        "url": ["url"],
        "favorite": ["favorite"],
    }

    available = _introspect_performer_fields(stash_base, cookie)
    selection: List[str] = list(base_fields)
    chosen: Dict[str, str] = {}
    for canon, candidates in desired_map.items():
        for candidate in candidates:
            if candidate in available:
                chosen[canon] = candidate
                break

    for canon, actual in chosen.items():
        if canon == "urls":
            kind, base = available.get(actual, ("", ""))
            if kind == "OBJECT" or base in ("URL", "Url", "PerformerURL", "URLFragment"):
                selection.append(f"{actual} {{ url }}")
            else:
                selection.append(actual)
        elif canon == "country":
            kind, base = available.get(actual, ("", ""))
            if kind == "OBJECT" or base in ("Country", "PerformerCountry"):
                selection.append(f"{actual} {{ name }}")
            else:
                selection.append(actual)
        else:
            selection.append(actual)

    query = f"""
    query FindPerformer($id: ID!) {{
      findPerformer(id: $id) {{
        {' '.join(selection)}
      }}
    }}
    """
    data = _gql_post(stash_base, cookie, query, {"id": str(performer_id)})
    performer = (data.get("data") or {}).get("findPerformer")
    if not performer:
        raise RuntimeError(f"Stash performer {performer_id} not found.")
    return performer


# ---------------------------------------------------------------------------
# Performer metadata conversion
# ---------------------------------------------------------------------------

def _build_jellyfin_overview_from_stash(performer: Dict[str, Any]) -> str:
    icons: Dict[str, str] = {
        "Details": "📝", "Aliases": "🏷️", "Ethnicity": "🌍", "Hair Color": "💇",
        "Eye Color": "👀", "Height (cm)": "↕️", "Weight (kg)": "⚖️",
        "Penis Length (cm)": "🍆", "Circumcised": "✂️", "Measurements": "📊",
        "Artificial Breasts": "🧪", "Tattoos": "🖋️", "Piercings": "📌",
        "Career Start": "▶️", "Career End": "⏹️", "Career Length": "🗓️", "URLs": "🌐",
    }

    def clean(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def label(name: str) -> str:
        icon = icons.get(name, "")
        return f"{icon} {name}" if icon else name

    blocks: List[str] = []

    def add(name: str, value: Any) -> None:
        text = clean(value)
        if text:
            blocks.append(f"{label(name)}: {text}")

    add("Details", performer.get("details") or performer.get("Details"))

    aliases = (
        performer.get("aliases") or performer.get("Aliases") or performer.get("alias_list")
        or performer.get("aliasList") or performer.get("alias") or performer.get("Alias")
    )
    if isinstance(aliases, list):
        aliases = ", ".join(clean(x) for x in aliases if clean(x))
    add("Aliases", aliases)

    add("Ethnicity", performer.get("ethnicity"))
    add("Hair Color", performer.get("hair_color") or performer.get("hairColor"))
    add("Eye Color", performer.get("eye_color") or performer.get("eyeColor"))
    add("Height (cm)", performer.get("height_cm") or performer.get("heightCm") or performer.get("height"))
    add("Weight (kg)", performer.get("weight_kg") or performer.get("weightKg") or performer.get("weight"))
    add("Penis Length (cm)", performer.get("penis_length_cm") or performer.get("penisLengthCm") or performer.get("penis_length") or performer.get("penisLength"))
    add("Circumcised", performer.get("circumcised"))
    add("Measurements", performer.get("measurements"))
    add("Artificial Breasts", performer.get("fake_tits") or performer.get("fakeTits"))
    add("Tattoos", performer.get("tattoos"))
    add("Piercings", performer.get("piercings"))
    career_start = performer.get("career_start") or performer.get("careerStart")
    career_end = performer.get("career_end") or performer.get("careerEnd")
    add("Career Start", career_start)
    add("Career End", career_end)

    # Backward compatibility with Stash < 0.31, where career_length was a
    # single field. Avoid showing both the legacy field and the new fields.
    if not clean(career_start) and not clean(career_end):
        add("Career Length", performer.get("career_length") or performer.get("careerLength"))

    url_list = list(_iter_entity_urls(performer))
    if url_list:
        blocks.append(f"{label('URLs')}:\n" + "\n\n".join(url_list))

    return "\n\n".join(blocks).strip()


def _extract_aliases_str(performer: Dict[str, Any]) -> str:
    aliases = (
        performer.get("aliases") or performer.get("Aliases") or performer.get("alias_list")
        or performer.get("aliasList") or performer.get("alias") or performer.get("Alias")
    )
    if isinstance(aliases, list):
        return ", ".join(str(a).strip() for a in aliases if str(a).strip())
    return _s(aliases).strip()


def _extract_birthdate(performer: Dict[str, Any]) -> str:
    return (
        _s(performer.get("birthdate")) or _s(performer.get("birth_date"))
        or _s(performer.get("birthDate")) or _s(performer.get("date_of_birth"))
        or _s(performer.get("dateOfBirth")) or _s(performer.get("dob"))
    ).strip()


def _extract_deathdate(performer: Dict[str, Any]) -> str:
    return (
        _s(performer.get("deathdate")) or _s(performer.get("death_date"))
        or _s(performer.get("deathDate")) or _s(performer.get("date_of_death"))
        or _s(performer.get("dateOfDeath")) or _s(performer.get("dod"))
    ).strip()


def _extract_country_name(performer: Dict[str, Any]) -> str:
    country = (
        performer.get("country") or performer.get("birth_country") or performer.get("birthCountry")
        or performer.get("birthplace") or performer.get("birth_place") or performer.get("birthPlace")
    )
    if isinstance(country, dict):
        return _s(country.get("name") or country.get("Name")).strip()
    return _s(country).strip()


def _date_only(value: Any) -> str:
    text = _s(value).strip()
    if not text:
        return ""
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = re.search(r"(\d{4})/(\d{2})/(\d{2})", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = re.search(r"(\d{4})", text)
    if match:
        return f"{match.group(1)}-01-01"
    return ""


def _jf_datetime_z(value: str) -> str:
    date = _date_only(value)
    return f"{date}T00:00:00.0000000Z" if date else ""


# ---------------------------------------------------------------------------
# Image handling
# ---------------------------------------------------------------------------

def _detect_image_mime(data: bytes, header_ct: str) -> str:
    ct = (header_ct or "").split(";", 1)[0].strip().lower()
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "image/gif"

    # Stash can legitimately expose performer images/default artwork as SVG.
    # Recognize both the response Content-Type and the actual XML/SVG payload.
    head = data[:2048].lstrip().lower()
    if ct in ("image/svg+xml", "image/svg") or head.startswith(b"<svg") or (
        head.startswith(b"<?xml") and b"<svg" in head
    ):
        return "image/svg+xml"

    if ct == "image/jpg":
        return "image/jpeg"
    if ct in ("image/jpeg", "image/png", "image/webp", "image/gif"):
        return ct
    return ""


def _looks_like_html(data: bytes) -> bool:
    head = data[:256].lstrip().lower()
    return head.startswith(b"<!doctype") or head.startswith(b"<html") or b"<head" in head or b"<body" in head


def _fetch_stash_image(stash_base: str, cookie: str, image_path: str) -> Tuple[bytes, str]:
    url = image_path
    if url.startswith("/"):
        url = stash_base + url
    headers: Dict[str, str] = {"Accept": "image/*,*/*"}
    if cookie:
        headers["Cookie"] = cookie
    response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    data = response.content or b""
    header_ct = response.headers.get("Content-Type") or ""
    if header_ct.lower().startswith("text/html") or _looks_like_html(data):
        raise RuntimeError("Stash returned HTML instead of performer image.")
    content_type = _detect_image_mime(data, header_ct)
    if not content_type:
        raise RuntimeError(f"Unrecognized Stash performer image (Content-Type={header_ct}).")
    return data, content_type


def _reencode_image_to_png(data: bytes, content_type: str = "") -> Optional[Tuple[bytes, str]]:
    """Best-effort conversion of Stash performer artwork to PNG.

    SVG needs an explicit rasterizer because Pillow does not decode it. Other
    raster formats are handled by Pillow first, then ffmpeg as a fallback.
    """
    ct = (content_type or "").split(";", 1)[0].strip().lower()
    head = data[:2048].lstrip().lower()
    is_svg = ct in ("image/svg+xml", "image/svg") or head.startswith(b"<svg") or (
        head.startswith(b"<?xml") and b"<svg" in head
    )

    if is_svg:
        try:
            import cairosvg  # type: ignore
            png = cairosvg.svg2png(bytestring=data)
            if png and png.startswith(b"\x89PNG\r\n\x1a\n"):
                return png, "image/png"
        except Exception as exc:
            _verbose(f"Could not convert Stash performer SVG to PNG with CairoSVG: {exc}")
        return None

    try:
        from PIL import Image  # type: ignore
        import io
        with Image.open(io.BytesIO(data)) as image:
            output = io.BytesIO()
            image.save(output, format="PNG")
            return output.getvalue(), "image/png"
    except Exception:
        pass

    in_path = ""
    out_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".img") as source:
            source.write(data)
            source.flush()
            in_path = source.name
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as target:
            out_path = target.name
        subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-i", in_path, "-frames:v", "1", out_path],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with open(out_path, "rb") as file:
            converted = file.read()
        if converted:
            return converted, "image/png"
        return None
    except Exception:
        return None
    finally:
        for path in (in_path, out_path):
            if path:
                try:
                    os.unlink(path)
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# Jellyfin API helpers
# ---------------------------------------------------------------------------

def _jellyfin_headers(api_key: str) -> Dict[str, str]:
    media_browser = (
        'MediaBrowser Client="Stash", Device="Stash", '
        'DeviceId="stash-jellyfin-sync", '
        f'Version="{PLUGIN_VERSION}", Token="{api_key}"'
    )
    return {
        "X-Emby-Token": api_key,
        "X-MediaBrowser-Token": api_key,
        "X-Emby-Authorization": media_browser,
        "Authorization": media_browser,
        "Accept": "application/json",
        "User-Agent": f"StashJellyfinSync/{PLUGIN_VERSION}",
    }


def _jf_get(base_url: str, api_key: str, path: str, verify_tls: bool, params: Optional[Dict[str, Any]] = None) -> Any:
    response = requests.get(
        f"{base_url}{path}",
        headers=_jellyfin_headers(api_key),
        params=params or {},
        timeout=HTTP_TIMEOUT,
        verify=verify_tls,
    )
    response.raise_for_status()
    return response.json()


def _jf_post_json(base_url: str, api_key: str, path: str, payload: Dict[str, Any], verify_tls: bool) -> None:
    url = f"{base_url}{path}"
    headers = {**_jellyfin_headers(api_key), "Content-Type": "application/json"}
    attempts = [
        (headers, None),
        (headers, {"api_key": api_key}),
        ({"Content-Type": "application/json", "Accept": "application/json"}, {"api_key": api_key}),
    ]
    last_error = ""
    for attempt_headers, params in attempts:
        response = requests.post(
            url,
            headers=attempt_headers,
            params=params,
            json=payload,
            timeout=HTTP_TIMEOUT,
            verify=verify_tls,
        )
        if response.ok:
            return
        last_error = f"HTTP {response.status_code}: {(response.text or '')[:1000]}"
        if 400 <= response.status_code < 500:
            break
    raise RuntimeError(f"Jellyfin metadata update failed: {last_error}")


def _jf_post_image(base_url: str, api_key: str, person_id: str, data: bytes, content_type: str, verify_tls: bool) -> None:
    url = f"{base_url}/Items/{person_id}/Images/Primary"
    payload = base64.b64encode(data)
    headers = {**_jellyfin_headers(api_key), "Content-Type": content_type}
    attempts = [
        (headers, None),
        (headers, {"api_key": api_key}),
        ({"Content-Type": content_type, "Accept": "application/json"}, {"api_key": api_key}),
    ]
    last_error = ""
    for attempt_headers, params in attempts:
        response = requests.post(
            url,
            headers=attempt_headers,
            params=params,
            data=payload,
            timeout=HTTP_TIMEOUT,
            verify=verify_tls,
        )
        if response.ok:
            return
        last_error = f"HTTP {response.status_code}: {(response.text or '')[:1000]}"
        if 400 <= response.status_code < 500:
            break
    raise RuntimeError(f"Jellyfin performer image upload failed: {last_error}")


def _jf_list_users(base_url: str, api_key: str, verify_tls: bool) -> List[Dict[str, Any]]:
    data = _jf_get(base_url, api_key, "/Users", verify_tls)
    if not isinstance(data, list):
        return []
    return [user for user in data if isinstance(user, dict) and str(user.get("Id") or "").strip()]


def _configured_user_id(settings: Dict[str, Any]) -> Optional[str]:
    user_id = str(settings.get("jellyfinUserId") or "").strip()
    if not user_id:
        return None
    if not ITEM_ID_RE.fullmatch(user_id):
        raise RuntimeError(
            "Configured Jellyfin user ID is invalid. Expected a 32-character Jellyfin GUID without dashes."
        )
    return user_id


def _resolve_item_read_user_id(
    base_url: str,
    api_key: str,
    verify_tls: bool,
    configured_user_id: Optional[str],
) -> str:
    """Resolve a user context for Jellyfin 12 item reads without ever using an empty Guid."""
    if configured_user_id:
        return configured_user_id

    users = _jf_list_users(base_url, api_key, verify_tls)
    if not users:
        raise RuntimeError(
            "Jellyfin returned no users. Configure 'Jellyfin user ID' in the plugin settings."
        )

    ordered = sorted(
        users,
        key=lambda user: 0 if ((user.get("Policy") or {}).get("IsAdministrator")) else 1,
    )
    user_id = str(ordered[0].get("Id") or "").strip()
    _verbose(
        f"Jellyfin user ID is not configured; using {user_id} only as the item-read context."
    )
    return user_id


def _resolve_favorite_user_id(
    base_url: str,
    api_key: str,
    verify_tls: bool,
    configured_user_id: Optional[str],
) -> Optional[str]:
    """Favorites are per-user. Auto-select only when the server has exactly one user."""
    if configured_user_id:
        return configured_user_id

    users = _jf_list_users(base_url, api_key, verify_tls)
    if len(users) == 1:
        user_id = str(users[0].get("Id") or "").strip()
        _verbose(f"Auto-selected the only Jellyfin user {user_id} for favorite synchronization.")
        return user_id

    if len(users) > 1:
        log.warning(
            "Favorite synchronization skipped because Jellyfin has multiple users and "
            "'Jellyfin user ID' is not configured."
        )
    else:
        log.warning("Favorite synchronization skipped because no Jellyfin user could be resolved.")
    return None


def _jf_probe_item_by_direct_id(
    base_url: str,
    api_key: str,
    item_id: str,
    verify_tls: bool,
    user_id: Optional[str] = None,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Probe one exact Jellyfin item without turning a stale URL into an error.

    Returns ("ok", item), ("not_found", None), or ("error", None).  Jellyfin 12
    requires a real user context for /Items/{id}, so use the same resolution logic as
    _jf_get_item_dto.  The legacy endpoint is kept for compatibility.
    """
    try:
        read_user_id = _resolve_item_read_user_id(
            base_url, api_key, verify_tls, user_id
        )
    except Exception as exc:
        _verbose(f"Could not resolve Jellyfin user for direct item probe {item_id}: {exc}")
        return "error", None

    headers = _jellyfin_headers(api_key)
    primary_url = f"{base_url}/Items/{item_id}"
    try:
        response = requests.get(
            primary_url,
            headers=headers,
            params={"userId": read_user_id},
            timeout=HTTP_TIMEOUT,
            verify=verify_tls,
        )
    except requests.RequestException as exc:
        _verbose(f"Direct Jellyfin item probe failed for {item_id}: {exc}")
        return "error", None

    if response.ok:
        try:
            data = response.json()
        except ValueError:
            return "error", None
        return ("ok", data) if isinstance(data, dict) else ("error", None)

    if response.status_code != 404:
        _verbose(
            f"Direct Jellyfin item probe returned HTTP {response.status_code} for {item_id}."
        )
        return "error", None

    # Compatibility fallback for older Jellyfin/Emby-compatible servers.
    legacy_url = f"{base_url}/Users/{read_user_id}/Items/{item_id}"
    try:
        legacy = requests.get(
            legacy_url,
            headers=headers,
            timeout=HTTP_TIMEOUT,
            verify=verify_tls,
        )
    except requests.RequestException as exc:
        _verbose(f"Legacy Jellyfin item probe failed for {item_id}: {exc}")
        return "error", None

    if legacy.ok:
        try:
            data = legacy.json()
        except ValueError:
            return "error", None
        return ("ok", data) if isinstance(data, dict) else ("error", None)
    if legacy.status_code == 404:
        return "not_found", None

    _verbose(
        f"Legacy Jellyfin item probe returned HTTP {legacy.status_code} for {item_id}."
    )
    return "error", None


def _jf_get_item_dto(
    base_url: str,
    api_key: str,
    item_id: str,
    verify_tls: bool,
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    # Jellyfin 12 requires a real user context for GET /Items/{id}. Calling the
    # endpoint without userId can reach UserManager.GetUserById(Guid.Empty) and
    # generate the server-side "Guid can't be empty" error.
    read_user_id = _resolve_item_read_user_id(base_url, api_key, verify_tls, user_id)

    try:
        data = _jf_get(
            base_url,
            api_key,
            f"/Items/{item_id}",
            verify_tls,
            params={"userId": read_user_id},
        )
        if isinstance(data, dict):
            return data
    except requests.RequestException:
        pass

    # Compatibility fallback for older Jellyfin/Emby-compatible servers.
    try:
        data = _jf_get(
            base_url,
            api_key,
            f"/Users/{read_user_id}/Items/{item_id}",
            verify_tls,
        )
        if isinstance(data, dict):
            return data
    except requests.RequestException:
        pass

    raise RuntimeError(f"Jellyfin item {item_id} could not be read by direct id.")


def _jf_get_favorite_state(
    base_url: str,
    api_key: str,
    item_id: str,
    user_id: str,
    verify_tls: bool,
) -> bool:
    """Read the current per-user favorite state for an exact Jellyfin item."""
    item = _jf_get_item_dto(
        base_url, api_key, item_id, verify_tls, user_id
    )
    user_data = item.get("UserData") or {}
    if not isinstance(user_data, dict):
        return False
    return _bool(user_data.get("IsFavorite"), False)


def _jf_set_favorite(
    base_url: str,
    api_key: str,
    item_id: str,
    user_id: str,
    favorite: bool,
    verify_tls: bool,
) -> bool:
    """Synchronize Jellyfin favorite state, writing only when it actually differs.

    The state check is important when another Jellyfin plugin mirrors user-data
    changes back to Stash. Without it, that write-back creates another
    Scene/Performer.Update.Post hook and an unnecessary second POST/DELETE.
    """
    try:
        current_favorite = _jf_get_favorite_state(
            base_url, api_key, item_id, user_id, verify_tls
        )
    except Exception as exc:
        log.error(
            f"Could not read Jellyfin favorite state for item {item_id}; "
            f"favorite update skipped to avoid a feedback loop: {exc}"
        )
        return False

    if current_favorite == favorite:
        state = "favorite" if favorite else "not favorite"
        _verbose(
            f"Jellyfin item {item_id} is already {state} for user {user_id}; "
            "favorite request skipped."
        )
        return True

    method = requests.post if favorite else requests.delete
    headers = _jellyfin_headers(api_key)

    # Modern route used by Jellyfin 12/current SDKs.
    url = f"{base_url}/UserFavoriteItems/{item_id}"
    try:
        response = method(
            url,
            headers=headers,
            params={"userId": user_id},
            timeout=HTTP_TIMEOUT,
            verify=verify_tls,
        )
    except requests.RequestException as exc:
        log.error(f"Jellyfin favorite request failed for item {item_id}: {exc}")
        return False

    if response.ok:
        action = "added to" if favorite else "removed from"
        _info(f"Jellyfin favorite updated: {action} favorites (item {item_id[:8]}…).")
        return True

    # Compatibility fallback for older Jellyfin/Emby-compatible servers.
    if response.status_code == 404:
        legacy_url = f"{base_url}/Users/{user_id}/FavoriteItems/{item_id}"
        try:
            legacy = method(
                legacy_url,
                headers=headers,
                timeout=HTTP_TIMEOUT,
                verify=verify_tls,
            )
        except requests.RequestException as exc:
            log.error(f"Jellyfin legacy favorite request failed for item {item_id}: {exc}")
            return False
        if legacy.ok:
            action = "added to" if favorite else "removed from"
            _info(f"Jellyfin favorite updated: {action} favorites (item {item_id[:8]}…).")
            return True
        response = legacy

    log.error(
        f"Jellyfin favorite update failed for item {item_id}: "
        f"HTTP {response.status_code}: {(response.text or '')[:1000]}"
    )
    return False


def _jf_get_user_data(
    base_url: str,
    api_key: str,
    item_id: str,
    user_id: str,
    verify_tls: bool,
) -> Dict[str, Any]:
    """Read per-user data for one exact Jellyfin item."""
    try:
        data = _jf_get(
            base_url,
            api_key,
            f"/UserItems/{item_id}/UserData",
            verify_tls,
            params={"userId": user_id},
        )
        if isinstance(data, dict):
            return data
    except requests.RequestException:
        pass

    # Compatibility fallback for older Jellyfin/Emby-compatible servers.
    try:
        data = _jf_get(
            base_url,
            api_key,
            f"/Users/{user_id}/Items/{item_id}/UserData",
            verify_tls,
        )
        if isinstance(data, dict):
            return data
    except requests.RequestException:
        pass

    # Last fallback: an item read also carries UserData when a user context is supplied.
    item = _jf_get_item_dto(base_url, api_key, item_id, verify_tls, user_id)
    data = item.get("UserData") or {}
    if isinstance(data, dict):
        return data
    return {}


def _jf_set_playback_position(
    base_url: str,
    api_key: str,
    item_id: str,
    user_id: str,
    resume_seconds: float,
    verify_tls: bool,
) -> bool:
    """Copy Stash resume position to Jellyfin without changing watched/favorite state."""
    try:
        seconds = max(0.0, float(resume_seconds or 0.0))
    except (TypeError, ValueError):
        seconds = 0.0

    ticks = int(round(seconds * 10_000_000.0))

    try:
        current = _jf_get_user_data(base_url, api_key, item_id, user_id, verify_tls)
    except Exception as exc:
        log.error(f"Could not read Jellyfin user data for playback sync on item {item_id}: {exc}")
        return False

    try:
        current_ticks = int(current.get("PlaybackPositionTicks") or 0)
    except (TypeError, ValueError):
        current_ticks = 0

    # One-second tolerance prevents duplicate writes caused by pause + navigation/ended events.
    if abs(current_ticks - ticks) < 10_000_000:
        _verbose(
            f"Jellyfin playback position already matches Stash for item {item_id}: "
            f"{seconds:.1f}s; update skipped."
        )
        return True

    # Keep all writable per-user state returned by Jellyfin so this operation changes
    # only PlaybackPositionTicks. In particular, do not alter Played/PlayCount/IsFavorite.
    writable_fields = (
        "IsFavorite",
        "ItemId",
        "Key",
        "LastPlayedDate",
        "Likes",
        "PlayCount",
        "Played",
        "PlayedPercentage",
        "Rating",
        "UnplayedItemCount",
    )
    payload: Dict[str, Any] = {
        key: current[key]
        for key in writable_fields
        if key in current
    }
    payload["PlaybackPositionTicks"] = ticks

    headers = {**_jellyfin_headers(api_key), "Content-Type": "application/json"}
    url = f"{base_url}/UserItems/{item_id}/UserData"
    try:
        response = requests.post(
            url,
            headers=headers,
            params={"userId": user_id},
            json=payload,
            timeout=HTTP_TIMEOUT,
            verify=verify_tls,
        )
    except requests.RequestException as exc:
        log.error(f"Jellyfin playback-position request failed for item {item_id}: {exc}")
        return False

    if not response.ok and response.status_code == 404:
        legacy_url = f"{base_url}/Users/{user_id}/Items/{item_id}/UserData"
        try:
            response = requests.post(
                legacy_url,
                headers=headers,
                json=payload,
                timeout=HTTP_TIMEOUT,
                verify=verify_tls,
            )
        except requests.RequestException as exc:
            log.error(f"Jellyfin legacy playback-position request failed for item {item_id}: {exc}")
            return False

    if not response.ok:
        log.error(
            f"Jellyfin playback-position update failed for item {item_id}: "
            f"HTTP {response.status_code}: {(response.text or '')[:1000]}"
        )
        return False

    minutes, remaining_seconds = divmod(int(seconds), 60)
    _info(
        f"Playback position saved to Jellyfin: {minutes}:{remaining_seconds:02d} "
        f"(item {item_id[:8]}…)."
    )
    return True


def _resolve_scene_item_for_playback(
    stash: StashInterface,
    scene: Dict[str, Any],
    settings: Dict[str, Any],
) -> Optional[str]:
    """Resolve an exact scene item for playback sync and recover stale direct URLs.

    Playback sync is invoked via runPluginOperation, so a stale Jellyfin URL must not
    turn a harmless player event into exit status 1.  Direct URLs still win when
    valid; only a confirmed 404 is allowed to trigger the safe fallback search.
    """
    base_url = settings["jellyfinBaseUrl"]
    api_key = settings["jellyfinApiKey"]
    verify_tls = settings["verifyTls"]

    item_id = _find_direct_jellyfin_id(scene, "scene")
    if item_id:
        status, _item = _jf_probe_item_by_direct_id(
            base_url,
            api_key,
            item_id,
            verify_tls,
            settings.get("jellyfinUserId"),
        )
        if status == "ok":
            return item_id

        if status == "error":
            # A temporary Jellyfin/network/auth problem is not evidence that the
            # stored URL is stale.  Skip this player event and retry next time.
            log.warning(
                f"Playback sync skipped for scene {scene.get('id')}: Jellyfin item "
                f"{item_id} could not be verified right now."
            )
            return None

        stale_item_id = item_id
        log.warning(
            f"Playback sync: Jellyfin item {stale_item_id} from scene {scene.get('id')} "
            "no longer exists; trying the safe fallback search for a replacement."
        )
        replacement_id = _fallback_search_scene_item(scene, settings)
        if not replacement_id or replacement_id == stale_item_id:
            log.warning(
                f"Playback sync skipped for scene {scene.get('id')}: no replacement "
                "Jellyfin item was found for the outdated URL."
            )
            return None

        replacement_status, _replacement = _jf_probe_item_by_direct_id(
            base_url,
            api_key,
            replacement_id,
            verify_tls,
            settings.get("jellyfinUserId"),
        )
        if replacement_status != "ok":
            log.warning(
                f"Playback sync skipped for scene {scene.get('id')}: fallback candidate "
                f"{replacement_id} could not be verified in Jellyfin."
            )
            return None

        _replace_stale_jellyfin_scene_url(
            stash, scene, stale_item_id, replacement_id, settings
        )
        return replacement_id

    if _entity_has_jellyfin_url(scene, base_url):
        log.warning(
            "Playback sync skipped: scene contains a Jellyfin-looking URL but no valid "
            "32-character ItemId could be extracted."
        )
        return None

    _verbose("Playback sync: scene has no Jellyfin URL; using the safe fallback search once.")
    item_id = _fallback_search_scene_item(scene, settings)
    if item_id:
        probe_status, _item = _jf_probe_item_by_direct_id(
            base_url,
            api_key,
            item_id,
            verify_tls,
            settings.get("jellyfinUserId"),
        )
        if probe_status != "ok":
            log.warning(
                f"Playback sync skipped for scene {scene.get('id')}: fallback item "
                f"{item_id} could not be verified in Jellyfin."
            )
            return None
        _store_fallback_jellyfin_url(stash, scene, item_id, settings)
    return item_id

def _handle_playback_position_operation(
    stash: StashInterface,
    settings: Dict[str, Any],
    args: Dict[str, Any],
) -> Dict[str, Any]:
    if not _bool(settings.get("syncPlaybackPosition"), False):
        return {"ok": True, "skipped": "disabled"}

    scene_id = str(args.get("sceneId") or args.get("scene_id") or "").strip()
    if not scene_id:
        return {"ok": False, "error": "sceneId is required"}

    scene = stash.find_scene(scene_id)
    if not scene:
        return {"ok": False, "error": f"Scene {scene_id} not found"}

    if _bool(settings.get("skipUnorganized"), False) and not scene.get("organized"):
        return {"ok": True, "skipped": "unorganized"}

    source = str(args.get("source") or "ui").strip() or "ui"

    # Stash is the source of truth for resume position. Older UI builds of this
    # plugin could briefly observe currentTime=0 (or a few seconds) while Video.js
    # recreated its <video> element on pause/navigation and send that transient value
    # to Jellyfin. Always compare the UI value with the freshly persisted Stash
    # scene.resume_time and prefer the persisted value unless this operation came
    # directly from the successful sceneSaveActivity mutation and both values agree.
    try:
        stored_resume = max(0.0, float(scene.get("resume_time") or 0.0))
    except (TypeError, ValueError):
        stored_resume = 0.0

    raw_resume = args.get("resumeTime")
    try:
        supplied_resume = max(0.0, float(raw_resume)) if raw_resume is not None else stored_resume
    except (TypeError, ValueError):
        supplied_resume = stored_resume

    authoritative_activity = source.startswith("sceneSaveActivity")
    if authoritative_activity and abs(supplied_resume - stored_resume) <= 2.0:
        resume_seconds = supplied_resume
    else:
        resume_seconds = stored_resume
        if raw_resume is not None and abs(supplied_resume - stored_resume) > 2.0:
            _verbose(
                f"Ignored transient UI playback position for scene {scene_id}: "
                f"ui={supplied_resume:.1f}s, Stash={stored_resume:.1f}s, source={source}."
            )

    _verbose(
        f"Playback-position sync request received: sceneId={scene_id} "
        f"resume={resume_seconds:.1f}s source={source}."
    )

    item_id = _resolve_scene_item_for_playback(stash, scene, settings)
    if not item_id:
        return {"ok": True, "skipped": "item-not-resolved"}

    user_id = _resolve_favorite_user_id(
        settings["jellyfinBaseUrl"],
        settings["jellyfinApiKey"],
        settings["verifyTls"],
        settings.get("jellyfinUserId"),
    )
    if not user_id:
        return {"ok": True, "skipped": "user-not-resolved"}

    ok = _jf_set_playback_position(
        settings["jellyfinBaseUrl"],
        settings["jellyfinApiKey"],
        item_id,
        user_id,
        resume_seconds,
        settings["verifyTls"],
    )
    if not ok:
        # Player activity is opportunistic.  Keep an actual Jellyfin error in the
        # log, but do not surface it as runPluginOperation exit status 1; the next
        # pause/activity event can retry automatically.
        return {
            "ok": True,
            "skipped": "playback-write-failed",
            "sceneId": scene_id,
            "itemId": item_id,
            "resumeTime": resume_seconds,
        }
    return {
        "ok": True,
        "sceneId": scene_id,
        "itemId": item_id,
        "resumeTime": resume_seconds,
    }


def _refresh_jellyfin_item(
    base_url: str,
    api_key: str,
    item_id: str,
    verify_tls: bool,
    *,
    refresh_images: bool = True,
) -> str:
    """Queue a Jellyfin FullRefresh.

    Returns one of: ``ok``, ``not_found``, ``error``.  Keeping ``not_found``
    separate lets the scene hook recover from stale Jellyfin URLs without
    treating the hook as a plugin failure.
    """
    url = f"{base_url}/Items/{item_id}/Refresh"
    params = {
        "metadataRefreshMode": "FullRefresh",
        "imageRefreshMode": "FullRefresh" if refresh_images else "None",
        "replaceAllMetadata": "true",
        "replaceAllImages": "true" if refresh_images else "false",
        "recursive": "true",
    }
    try:
        response = requests.post(
            url,
            headers=_jellyfin_headers(api_key),
            params=params,
            timeout=HTTP_TIMEOUT,
            verify=verify_tls,
        )
    except requests.RequestException as exc:
        log.error(f"Jellyfin refresh request failed: {exc}")
        return "error"

    if response.ok:
        _verbose(f"Jellyfin metadata refresh queued for item {item_id}: HTTP {response.status_code}")
        return "ok"
    if response.status_code == 404:
        return "not_found"
    if response.status_code in (401, 403):
        log.error(f"Jellyfin rejected the API key: HTTP {response.status_code}.")
    else:
        log.error(f"Jellyfin refresh failed for item {item_id}: HTTP {response.status_code}: {response.text}")
    return "error"


def _update_jellyfin_person_metadata(
    base_url: str,
    api_key: str,
    person_id: str,
    verify_tls: bool,
    current_item: Dict[str, Any],
    performer: Dict[str, Any],
) -> bool:
    item = dict(current_item)
    changed = False

    overview = _build_jellyfin_overview_from_stash(performer)
    aliases = _extract_aliases_str(performer)
    birth_date = _date_only(_extract_birthdate(performer))
    death_date = _date_only(_extract_deathdate(performer))
    premiere_date = _jf_datetime_z(birth_date) if birth_date else None
    end_date = _jf_datetime_z(death_date) if death_date else None
    birth_year = int(birth_date[:4]) if birth_date else None
    birthplace = _extract_country_name(performer)

    # Preserve the behavior of the source performer plugin: missing optional
    # Stash values do not erase existing Jellyfin values. EndDate is the one
    # intentional exception: no death date in Stash means the person is alive,
    # so a stale Jellyfin EndDate is cleared.
    if overview and (item.get("Overview") or "") != overview:
        item["Overview"] = overview
        changed = True
    if aliases and (item.get("OriginalTitle") or "") != aliases:
        item["OriginalTitle"] = aliases
        changed = True
    if premiere_date is not None and item.get("PremiereDate") != premiere_date:
        item["PremiereDate"] = premiere_date
        changed = True
    if item.get("EndDate") != end_date:
        item["EndDate"] = end_date
        changed = True
    if birth_year is not None and item.get("ProductionYear") != birth_year:
        item["ProductionYear"] = birth_year
        changed = True
    if birthplace:
        target_locations = [birthplace]
        if (item.get("ProductionLocations") or []) != target_locations:
            item["ProductionLocations"] = target_locations
            changed = True

    # Jellyfin's "Enabled Fields" UI is backed by BaseItemDto.LockedFields.
    # Only values from MetadataField can be locked individually.  Of the
    # performer fields written by this plugin, Overview and ProductionLocations
    # are individually lockable.  Preserve all existing user locks and add only
    # the fields for which Stash actually provided authoritative data.
    existing_locked = item.get("LockedFields") or []
    locked_fields = []
    seen_locks = set()
    for value in existing_locked:
        field = str(value or "").strip()
        if field and field not in seen_locks:
            locked_fields.append(field)
            seen_locks.add(field)

    desired_locks = []
    if overview:
        desired_locks.append("Overview")
    if birthplace:
        desired_locks.append("ProductionLocations")

    for field in desired_locks:
        if field not in seen_locks:
            locked_fields.append(field)
            seen_locks.add(field)
            changed = True

    item["LockedFields"] = locked_fields

    # Lock the whole Jellyfin Person after Stash has supplied performer metadata.
    # This corresponds to Jellyfin's metadata-editor option
    # "Lock this item to prevent future changes" and prevents metadata providers
    # from overwriting Stash-authored values that cannot be individually represented
    # in LockedFields (for example birth/death dates and OriginalTitle aliases).
    # Manual/API updates from this plugin remain possible.
    if item.get("LockData") is not True:
        item["LockData"] = True
        changed = True

    if not changed:
        return False

    for key in ("Tags", "Genres", "Studios", "People", "MediaStreams", "ImageTags"):
        if key in item and item[key] is None:
            item[key] = []
    if item.get("ProductionLocations") is None:
        item["ProductionLocations"] = []

    _jf_post_json(base_url, api_key, f"/Items/{person_id}", item, verify_tls)
    return True



# ---------------------------------------------------------------------------
# Scene resolution / fallback orchestration
# ---------------------------------------------------------------------------

def _entity_has_jellyfin_url(entity: Dict[str, Any], base_url: str) -> bool:
    """Return True when an entity already contains a Jellyfin-looking URL.

    Fallback search is intentionally disabled in that case. If a Jellyfin link is
    present but malformed, guessing by title/path could silently bind the wrong item.
    """
    base_netloc = (urlparse(base_url).netloc or "").lower()
    for raw_url in _iter_entity_urls(entity):
        url = str(raw_url or "").strip()
        if not url:
            continue
        if _extract_jellyfin_item_id_from_url(url):
            return True

        lower = url.lower()
        parsed = urlparse(url)
        same_server = bool(base_netloc and (parsed.netloc or "").lower() == base_netloc)
        looks_like_detail = (
            "/web/index.html" in lower
            and "details" in lower
            and ("?id=" in lower or "&id=" in lower)
        )
        if same_server and looks_like_detail:
            return True
        if "jellyfin/items/" in lower:
            return True
    return False


def _fallback_search_scene_item(
    scene: Dict[str, Any],
    settings: Dict[str, Any],
) -> Optional[str]:
    """Resolve a Jellyfin video only when the Stash scene has no Jellyfin URL.

    This is the original search strategy, retained as a fallback:
      1. exact filesystem Path inside matching Jellyfin VirtualFolders;
      2. user-scoped title/filename search;
      3. /Search/Hints;
      4. path/name/date narrowing and performer-assisted disambiguation.
    """
    base_url = settings["jellyfinBaseUrl"]
    api_key = settings["jellyfinApiKey"]
    verify_tls = settings["verifyTls"]

    stash_path = _stash_scene_primary_file_path(scene)
    scene_title = str(scene.get("title") or "").strip()

    configured_user_id = str(settings.get("jellyfinUserId") or "").strip()
    user_id = configured_user_id
    if not user_id:
        user_id = jellyfin_pick_user_id(base_url, api_key, verify_tls=verify_tls) or ""
        if user_id:
            _verbose(f"Fallback search: auto-selected Jellyfin user {user_id}.")

    # Original defaults; intentionally hidden to keep the plugin settings simple.
    search_limit = 25
    item_limit = 1000
    max_pages = 50

    # 1) Exact path in a matching Jellyfin library.
    if stash_path:
        vfolders = jellyfin_virtual_folders(base_url, api_key, verify_tls=verify_tls)
        if vfolders:
            item_id = jellyfin_find_item_id_by_exact_path(
                base_url,
                api_key,
                user_id or None,
                stash_path,
                vfolders,
                item_limit=item_limit,
                max_pages=max_pages,
                verify_tls=verify_tls,
            )
            if item_id:
                _verbose(f"Fallback search matched Jellyfin item by exact path: {item_id}")
                return item_id

    # 2) Original title / filename variants.
    filename_raw = (_basename_no_ext(stash_path) or "").strip()
    filename_clean = _strip_quality_suffix(filename_raw)
    title_clean = _strip_quality_suffix(scene_title)

    terms: List[str] = []

    def _add_terms(value: str) -> None:
        for variant in _title_search_variants(value):
            if variant and variant not in terms:
                terms.append(variant)

    _add_terms(scene_title)
    _add_terms(filename_raw)
    _add_terms(filename_clean)
    _add_terms(title_clean)
    for term in _derive_truncated_filename_terms(filename_raw):
        _add_terms(term)

    if not terms:
        log.warning("Fallback search cannot run: scene has neither a usable title nor filename.")
        return None

    performers = _scene_performer_names(scene)

    # 2a) User-scoped /Users/{id}/Items search.
    if user_id:
        for term in terms:
            items = jellyfin_search_item_user_scope(
                base_url, api_key, user_id, term, search_limit, verify_tls
            )
            narrowed = narrow_items_for_scene(items, stash_path, scene, scene_title, term)

            if len(narrowed) > 1 and performers:
                log.warning(
                    f"Fallback search found {len(narrowed)} candidates for '{term}'. "
                    "Trying performer-assisted disambiguation."
                )
                found: Optional[Dict[str, Any]] = None
                for performer in performers[:3]:
                    query = f"{term} {performer}".strip()
                    for query_variant in _title_search_variants(query):
                        items2 = jellyfin_search_item_user_scope(
                            base_url,
                            api_key,
                            user_id,
                            query_variant,
                            search_limit,
                            verify_tls,
                        )
                        narrowed2 = narrow_items_for_scene(
                            items2, stash_path, scene, scene_title, query_variant
                        )
                        if len(narrowed2) == 1:
                            found = narrowed2[0]
                            break
                    if found:
                        break
                if found:
                    narrowed = [found]

            if len(narrowed) == 1:
                item_id = str(narrowed[0].get("Id") or "").strip()
                if ITEM_ID_RE.fullmatch(item_id):
                    _verbose(
                        f"Fallback search matched Jellyfin item by title/filename '{term}': {item_id}"
                    )
                    return item_id

            if len(narrowed) > 1:
                candidate_ids = [x.get("Id") for x in narrowed if x.get("Id")]
                log.warning(
                    f"Fallback search remains ambiguous for '{term}'; "
                    f"candidates={candidate_ids}. This term will be skipped."
                )

    # 2b) /Search/Hints fallback.
    for term in terms:
        hints = jellyfin_search_hints(
            base_url, api_key, user_id or None, term, search_limit, verify_tls
        )
        candidate_ids = collect_hint_ids(
            hints, stash_path, search_term=term, scene_title=scene_title
        )
        if not candidate_ids:
            continue

        details: List[Dict[str, Any]] = []
        for candidate_id in candidate_ids[:10]:
            detail = jellyfin_get_item_details(
                base_url,
                api_key,
                candidate_id,
                user_id or None,
                verify_tls=verify_tls,
            )
            if detail:
                details.append(detail)

        narrowed = narrow_items_for_scene(details, stash_path, scene, scene_title, term)

        if len(narrowed) > 1 and performers and user_id:
            log.warning(
                f"Fallback hints found {len(narrowed)} candidates for '{term}'. "
                "Trying performer-assisted disambiguation."
            )
            found = None
            for performer in performers[:3]:
                query = f"{term} {performer}".strip()
                for query_variant in _title_search_variants(query):
                    hints2 = jellyfin_search_hints(
                        base_url,
                        api_key,
                        user_id,
                        query_variant,
                        search_limit,
                        verify_tls,
                    )
                    candidate_ids2 = collect_hint_ids(
                        hints2,
                        stash_path,
                        search_term=query_variant,
                        scene_title=scene_title,
                    )
                    if not candidate_ids2:
                        continue

                    details2: List[Dict[str, Any]] = []
                    for candidate_id2 in candidate_ids2[:10]:
                        detail2 = jellyfin_get_item_details(
                            base_url,
                            api_key,
                            candidate_id2,
                            user_id,
                            verify_tls=verify_tls,
                        )
                        if detail2:
                            details2.append(detail2)

                    narrowed2 = narrow_items_for_scene(
                        details2, stash_path, scene, scene_title, query_variant
                    )
                    if len(narrowed2) == 1:
                        found = narrowed2[0]
                        break
                if found:
                    break
            if found:
                narrowed = [found]

        if len(narrowed) == 1:
            item_id = str(narrowed[0].get("Id") or "").strip()
            if ITEM_ID_RE.fullmatch(item_id):
                _verbose(f"Fallback /Search/Hints matched Jellyfin item '{term}': {item_id}")
                return item_id

        if len(narrowed) > 1:
            ids = [x.get("Id") for x in narrowed if x.get("Id")]
            log.warning(
                f"Fallback /Search/Hints remains ambiguous for '{term}'; "
                f"candidates={ids}. This term will be skipped."
            )

    log.warning("Fallback search did not find an unambiguous Jellyfin item.")
    return None


def _jellyfin_server_id(
    base_url: str,
    api_key: str,
    verify_tls: bool,
) -> str:
    try:
        data = _jf_get(base_url, api_key, "/System/Info", verify_tls)
    except Exception as exc:
        log.warning(f"Could not read Jellyfin server ID for storing scene URL: {exc}")
        return ""
    if not isinstance(data, dict):
        return ""
    return str(
        data.get("Id")
        or data.get("ServerId")
        or data.get("ServerID")
        or ""
    ).strip()


def _store_fallback_jellyfin_url(
    stash: StashInterface,
    scene: Dict[str, Any],
    item_id: str,
    settings: Dict[str, Any],
) -> None:
    """Persist a successfully resolved fallback match so future hooks are direct."""
    existing = list(_iter_entity_urls(scene))
    if any(_extract_jellyfin_item_id_from_url(url) == item_id for url in existing):
        return

    base_url = settings["jellyfinBaseUrl"].rstrip("/")
    server_id = _jellyfin_server_id(
        base_url, settings["jellyfinApiKey"], settings["verifyTls"]
    )
    url = f"{base_url}/web/index.html#/details?id={item_id}"
    if server_id:
        url += f"&serverId={server_id}"

    try:
        stash.update_scenes({
            "ids": [str(scene["id"])],
            "urls": {"mode": "ADD", "values": [url]},
        })
        _info(f"Jellyfin link added to Stash scene {scene.get('id')}.")
    except Exception as exc:
        try:
            stash.update_scenes({
                "ids": [str(scene["id"])],
                "urls": [url],
                "urls_mode": "ADD",
            })
            _info(f"Jellyfin link added to Stash scene {scene.get('id')}.")
        except Exception as exc2:
            log.warning(
                "Fallback item was found, but its Jellyfin URL could not be stored in Stash: "
                f"{exc}; fallback update also failed: {exc2}"
            )



def _replace_stale_jellyfin_scene_url(
    stash: StashInterface,
    scene: Dict[str, Any],
    stale_item_id: str,
    new_item_id: str,
    settings: Dict[str, Any],
) -> bool:
    """Replace URLs that point at a stale Jellyfin ItemId with the new direct URL.

    Stash BulkUpdateStrings supports REMOVE/ADD.  Other non-Jellyfin URLs are
    untouched.  If removal fails we deliberately do not add another Jellyfin URL,
    because leaving two direct links would make future resolution ambiguous.
    """
    existing = list(_iter_entity_urls(scene))
    stale_urls = [
        url for url in existing
        if _extract_jellyfin_item_id_from_url(url) == stale_item_id
    ]

    base_url = settings["jellyfinBaseUrl"].rstrip("/")
    server_id = _jellyfin_server_id(
        base_url, settings["jellyfinApiKey"], settings["verifyTls"]
    )
    new_url = f"{base_url}/web/index.html#/details?id={new_item_id}"
    if server_id:
        new_url += f"&serverId={server_id}"

    try:
        if stale_urls:
            stash.update_scenes({
                "ids": [str(scene["id"])],
                "urls": {"mode": "REMOVE", "values": stale_urls},
            })
        if not any(_extract_jellyfin_item_id_from_url(url) == new_item_id for url in existing):
            stash.update_scenes({
                "ids": [str(scene["id"])],
                "urls": {"mode": "ADD", "values": [new_url]},
            })
        _info(f"Scene {scene.get('id')}: outdated Jellyfin link replaced with the current item.")
        return True
    except Exception as exc:
        log.warning(
            f"Scene {scene.get('id')}: current Jellyfin item was found, but the outdated "
            f"Stash URL could not be replaced: {exc}"
        )
        return False



# ---------------------------------------------------------------------------
# Optional Jellyfin cover generation (adapted from Jellyfin Cover Generator)
# ---------------------------------------------------------------------------

def _jf_fetch_binary_image(
    base_url: str,
    api_key: str,
    item_id: str,
    image_type: str,
    verify_tls: bool,
) -> Optional[Tuple[bytes, str]]:
    """Best-effort fetch of an existing Jellyfin image (used as logo fallback)."""
    try:
        response = requests.get(
            f"{base_url}/Items/{item_id}/Images/{image_type}",
            headers={**_jellyfin_headers(api_key), "Accept": "image/*,*/*"},
            timeout=HTTP_TIMEOUT,
            verify=verify_tls,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.content or b""
        if not data:
            return None
        content_type = (response.headers.get("Content-Type") or "application/octet-stream").split(";", 1)[0]
        return data, content_type
    except Exception as exc:
        log.debug(f"Could not fetch Jellyfin {image_type} image for cover generation: {exc}")
        return None


def _jf_upload_primary_cover(
    base_url: str,
    api_key: str,
    item_id: str,
    image_data: bytes,
    content_type: str,
    verify_tls: bool,
) -> None:
    """Upload a generated poster as Jellyfin Primary image."""
    payload = base64.b64encode(image_data)
    path = f"/Items/{item_id}/Images/Primary"
    url = f"{base_url}{path}"
    headers = {
        **_jellyfin_headers(api_key),
        "Content-Type": content_type,
        "Accept": "application/json",
    }

    # Jellyfin versions differ slightly in which authentication form the image
    # endpoint accepts, so keep the same conservative fallback sequence as the
    # source cover-generator plugin.
    attempts = [
        (headers, {}),
        (headers, {"api_key": api_key}),
        ({"Content-Type": content_type, "Accept": "application/json"}, {"api_key": api_key}),
    ]
    last_error: Optional[Exception] = None
    last_response: Optional[requests.Response] = None
    for attempt_headers, params in attempts:
        try:
            response = requests.post(
                url,
                headers=attempt_headers,
                params=params,
                data=payload,
                timeout=HTTP_TIMEOUT,
                verify=verify_tls,
            )
            last_response = response
            response.raise_for_status()
            return
        except Exception as exc:
            last_error = exc
            if last_response is not None and 400 <= last_response.status_code < 500:
                break

    status = f"HTTP {last_response.status_code}" if last_response is not None else "no response"
    body = ((last_response.text or "")[:1200] if last_response is not None else "")
    raise RuntimeError(f"Jellyfin Primary image upload failed ({status}): {last_error}. {body}")


def _plugin_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _generated_covers_dir() -> str:
    out_dir = os.path.join(_plugin_dir(), GENERATED_COVERS_SUBDIR)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def _generated_cover_path(scene_id: str, content_type: str = "image/jpeg") -> str:
    ext = ".jpg"
    ct = (content_type or "").lower().strip()
    if "png" in ct:
        ext = ".png"
    elif "webp" in ct:
        ext = ".webp"
    return os.path.join(_generated_covers_dir(), f"scene_{scene_id}{ext}")


def _save_generated_cover_local(scene_id: str, poster_data: bytes, content_type: str) -> str:
    path = _generated_cover_path(str(scene_id), content_type)
    with open(path, "wb") as fh:
        fh.write(poster_data)
    return path


def _detect_image_content_type_from_path(path: str) -> str:
    lower = str(path).lower()
    if lower.endswith(".png"):
        return "image/png"
    if lower.endswith(".webp"):
        return "image/webp"
    return "image/jpeg"


def _collect_generated_cover_files(folder: str) -> List[Tuple[str, str]]:
    files: List[Tuple[str, str]] = []
    if not os.path.isdir(folder):
        return files
    for name in sorted(os.listdir(folder)):
        match = re.fullmatch(r"scene_(\d+)\.(jpg|jpeg|png|webp)", name, flags=re.IGNORECASE)
        if not match:
            continue
        scene_id = match.group(1)
        files.append((scene_id, os.path.join(folder, name)))
    return files


def _generate_and_upload_scene_cover(
    json_input: Dict[str, Any],
    settings: Dict[str, Any],
    scene_id: str,
    item_id: str,
    metadata_refresh_started_at: Optional[float] = None,
    force_enabled: bool = False,
    apply_upload_delay: bool = True,
) -> Optional[bool]:
    """Generate a portrait poster from a Stash screenshot, save it locally, and upload it.

    Every generated poster is saved to pluginDir/generated. Automatic scene-update
    calls can wait after a Jellyfin metadata refresh before upload; manual calls can
    disable that delay and upload the newly generated poster immediately.
    """
    if (not force_enabled) and not _bool(settings.get("generateJellyfinCovers"), False):
        return True

    if covergen is None:
        log.error(f"Cover generation is enabled, but its helper could not be imported: {_cover_import_error}")
        return False

    sc = json_input.get("server_connection") or json_input.get("serverConnection") or {}
    stash_base = _stash_base_from_server_connection(sc)
    cookie = _stash_cookie_from_server_connection(sc)

    try:
        scene = covergen.get_scene(stash_base, cookie, str(scene_id), HTTP_TIMEOUT)
        cover_path = str(((scene.get("paths") or {}).get("screenshot") or "")).strip()
        if not cover_path:
            log.warning(f"Scene {scene_id} has no Stash screenshot; Jellyfin cover generation skipped.")
            return True

        try:
            source_data, source_ct = covergen.fetch_stash_image(
                stash_base, cookie, cover_path, HTTP_TIMEOUT
            )
        except Exception as exc:
            log.warning(
                f"Scene {scene_id}: Stash screenshot is unavailable or is not an image; "
                "cover generation skipped."
            )
            _verbose(f"Scene {scene_id}: source screenshot fetch failed: {exc}")
            return None

        # Stash can return a placeholder/empty payload with an image Content-Type
        # when a scene has no actual screenshot.  Validate the bytes with Pillow
        # before entering the poster generator so this is a normal skip rather
        # than an UnidentifiedImageError / runPluginOperation failure.
        try:
            probe = covergen.load_image(source_data)
            try:
                probe.close()
            except Exception:
                pass
        except Exception as exc:
            log.warning(
                f"Scene {scene_id}: Stash screenshot contains no usable image; "
                "cover generation skipped."
            )
            _verbose(
                f"Scene {scene_id}: screenshot payload could not be decoded "
                f"({source_ct}, {len(source_data)} bytes): {exc}"
            )
            return None

        _verbose(
            f"Generating Jellyfin cover for scene {scene_id} from Stash screenshot "
            f"({source_ct}, {len(source_data)} bytes)."
        )

        studio = scene.get("studio") or {}
        studio_name = str(studio.get("name") or "") if isinstance(studio, dict) else ""
        logo_data: Optional[bytes] = None
        logo_source = "none"

        for candidate in covergen.iter_studio_logo_candidates(
            studio if isinstance(studio, dict) else {}
        ):
            logo_path = str(candidate.get("image_path") or "").strip()
            if not logo_path:
                continue
            try:
                logo_data, _logo_ct = covergen.fetch_stash_image(
                    stash_base, cookie, logo_path, HTTP_TIMEOUT
                )
                logo_source = str(candidate.get("source") or "studio_image")
                break
            except Exception as exc:
                log.debug(
                    f"Could not use {candidate.get('source')} for scene {scene_id}: {exc}"
                )

        if logo_data is None:
            fallback_logo = _jf_fetch_binary_image(
                settings["jellyfinBaseUrl"],
                settings["jellyfinApiKey"],
                item_id,
                "Logo",
                settings["verifyTls"],
            )
            if fallback_logo:
                logo_data, _logo_ct = fallback_logo
                logo_source = "jellyfin_item_logo"

        poster_data, poster_ct, crop_info, logo_info = covergen.build_poster(
            source_data,
            logo_data,
            800,
            1200,
            50,
            100,
            3,
            92,
            studio_name=studio_name,
            logo_source_label=logo_source,
            use_studio_name_when_logo_missing=False,
            logo_trim_transparent_padding=False,
            logo_visibility_boost=False,
            logo_background_opacity=0,
        )
        _verbose(
            f"Generated Jellyfin cover for scene {scene_id}: 800x1200, "
            f"crop={crop_info}; logo={json.dumps(logo_info, ensure_ascii=False)}"
        )

        saved_path = _save_generated_cover_local(str(scene_id), poster_data, poster_ct)
        _verbose(f"Saved generated Jellyfin cover locally for scene {scene_id}: {saved_path}")

        if apply_upload_delay:
            import time
            configured_delay = _cover_upload_delay_seconds(settings)
            refresh_started = (
                metadata_refresh_started_at
                if metadata_refresh_started_at is not None
                else time.monotonic()
            )
            elapsed = max(0.0, time.monotonic() - refresh_started)
            remaining_delay = max(0.0, configured_delay - elapsed)
            if remaining_delay > 0:
                _verbose(
                    f"Waiting {remaining_delay:.1f}s before uploading generated cover to Jellyfin "
                    f"item {item_id} so the metadata refresh can finish first."
                )
                time.sleep(remaining_delay)
        else:
            _verbose(
                f"Manual cover generation for scene {scene_id}: uploading immediately "
                f"without metadata refresh or delay."
            )

        _jf_upload_primary_cover(
            settings["jellyfinBaseUrl"],
            settings["jellyfinApiKey"],
            item_id,
            poster_data,
            poster_ct,
            settings["verifyTls"],
        )
        _info(f"Scene {scene_id}: cover created, saved and uploaded to Jellyfin.")
        return True
    except Exception as exc:
        log.error(f"Jellyfin cover generation/upload failed for scene {scene_id}: {exc}")
        return False


def _upload_saved_generated_covers_operation(
    stash: StashInterface,
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    folder = _generated_covers_dir()
    files = _collect_generated_cover_files(folder)
    _info(f"Saved covers upload started: {len(files)} files found.")

    uploaded = 0
    skipped = 0
    failed = 0
    results: List[Dict[str, Any]] = []

    for scene_id, path in files:
        try:
            scene = stash.find_scene(scene_id)
            if not scene:
                skipped += 1
                results.append({"sceneId": scene_id, "path": path, "uploaded": False, "reason": "scene-not-found"})
                log.warning(f"Generated cover upload skipped for scene {scene_id}: scene not found in Stash.")
                continue

            excluded_dir = _excluded_cover_directory(scene, settings)
            if excluded_dir:
                skipped += 1
                results.append({"sceneId": scene_id, "path": path, "uploaded": False, "reason": "excluded-directory"})
                _verbose(f"Saved cover re-upload skipped for scene {scene_id}: excluded directory {excluded_dir}.")
                continue

            if _bool(settings.get("skipUnorganized"), False) and not scene.get("organized"):
                skipped += 1
                results.append({"sceneId": scene_id, "path": path, "uploaded": False, "reason": "unorganized"})
                _verbose(f"Generated cover upload skipped for scene {scene_id}: scene is not organized.")
                continue

            item_id = _find_direct_jellyfin_id(scene, "scene")
            resolved_by_fallback = False
            if not item_id:
                if _entity_has_jellyfin_url(scene, settings["jellyfinBaseUrl"]):
                    skipped += 1
                    results.append({"sceneId": scene_id, "path": path, "uploaded": False, "reason": "invalid-jellyfin-url"})
                    log.warning(f"Generated cover upload skipped for scene {scene_id}: Jellyfin-looking URL exists but no valid ItemId could be extracted.")
                    continue
                item_id = _fallback_search_scene_item(scene, settings)
                if not item_id:
                    skipped += 1
                    results.append({"sceneId": scene_id, "path": path, "uploaded": False, "reason": "item-not-resolved"})
                    log.warning(f"Generated cover upload skipped for scene {scene_id}: Jellyfin item could not be resolved.")
                    continue
                resolved_by_fallback = True

            with open(path, "rb") as fh:
                img_bytes = fh.read()
            content_type = _detect_image_content_type_from_path(path)
            _jf_upload_primary_cover(
                settings["jellyfinBaseUrl"],
                settings["jellyfinApiKey"],
                item_id,
                img_bytes,
                content_type,
                settings["verifyTls"],
            )
            if resolved_by_fallback:
                _store_fallback_jellyfin_url(stash, scene, item_id, settings)
            uploaded += 1
            _verbose(f"Uploaded saved generated cover for scene {scene_id} to Jellyfin item {item_id} from {path}.")
            results.append({"sceneId": scene_id, "path": path, "uploaded": True, "itemId": item_id})
        except Exception as exc:
            failed += 1
            results.append({"sceneId": scene_id, "path": path, "uploaded": False, "error": str(exc)})
            log.error(f"Failed to upload saved generated cover for scene {scene_id} ({path}): {exc}")

    _info(f"Saved covers upload completed: {uploaded} uploaded, {skipped} skipped, {failed} failed.")
    return {
        "ok": failed == 0,
        "generatedFolder": folder,
        "files": len(files),
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }


def _handle_generate_scene_cover_operation(
    json_input: Dict[str, Any],
    settings: Dict[str, Any],
    stash: StashInterface,
    args: Dict[str, Any],
    *,
    store_fallback_url: bool = True,
) -> Dict[str, Any]:
    """Generate/save/upload one scene cover without refreshing Jellyfin metadata.

    This is the path used by the scene-toolbar button and the manual task. It is
    intentionally independent of the automatic Scene.Update.Post sequence:
    resolve exact item -> generate poster -> save local copy -> upload immediately.
    """
    scene_id = str(args.get("sceneId") or args.get("scene_id") or "").strip()
    if not scene_id:
        return {"ok": False, "error": "sceneId is required"}

    scene = stash.find_scene(scene_id)
    if not scene:
        return {"ok": False, "error": f"Scene {scene_id} not found"}

    if _bool(settings.get("skipUnorganized"), False) and not scene.get("organized"):
        return {"ok": True, "skipped": "unorganized"}

    base_url = settings["jellyfinBaseUrl"]
    item_id = _find_direct_jellyfin_id(scene, "scene")
    resolved_by_fallback = False

    if not item_id:
        if _entity_has_jellyfin_url(scene, base_url):
            return {
                "ok": True,
                "skipped": "invalid-jellyfin-url",
                "message": "Scene contains a Jellyfin-looking URL but no valid ItemId could be extracted.",
            }

        _verbose(
            f"Manual cover generation for scene {scene_id}: no Jellyfin URL; "
            "using safe fallback search."
        )
        item_id = _fallback_search_scene_item(scene, settings)
        if not item_id:
            return {"ok": True, "skipped": "item-not-resolved"}
        resolved_by_fallback = True

    # A bulk operation must not write a fallback URL here: the resulting
    # Scene.Update.Post hook would also queue a metadata refresh and an
    # automatic cover generation, violating the manual-only bulk contract.
    if resolved_by_fallback and store_fallback_url:
        _store_fallback_jellyfin_url(stash, scene, item_id, settings)

    _verbose(
        f"Manual cover generation requested for scene {scene_id}, Jellyfin item {item_id}. "
        "Metadata refresh will NOT be requested."
    )
    cover_result = _generate_and_upload_scene_cover(
        json_input,
        settings,
        str(scene_id),
        item_id,
        metadata_refresh_started_at=None,
        force_enabled=True,
        apply_upload_delay=False,
    )

    if cover_result is None:
        return {
            "ok": True,
            "skipped": "missing-or-invalid-screenshot",
            "sceneId": str(scene_id),
            "itemId": item_id,
            "metadataRefreshRequested": False,
            "uploadDelayApplied": False,
            "coverGeneratedAndUploaded": False,
            "resolvedByFallback": resolved_by_fallback,
        }

    return {
        "ok": bool(cover_result),
        "sceneId": str(scene_id),
        "itemId": item_id,
        "metadataRefreshRequested": False,
        "uploadDelayApplied": False,
        "coverGeneratedAndUploaded": bool(cover_result),
        "resolvedByFallback": resolved_by_fallback,
    }


def _handle_generate_all_scene_covers_operation(
    json_input: Dict[str, Any],
    stash: StashInterface,
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    """Generate posters for the entire Stash scene library, using manual mode.

    Paginate server-side to bound memory, never queue a Jellyfin metadata refresh,
    never wait for the automatic cover delay, and never update metadata/favorites.
    Directory exclusions and the existing organized-scenes option apply.
    """
    sc = json_input.get("server_connection") or json_input.get("serverConnection") or {}
    stash_base = _stash_base_from_server_connection(sc)
    cookie = _stash_cookie_from_server_connection(sc)
    page_size = 100
    page = 1
    total = 0
    processed = 0
    uploaded = 0
    skipped = 0
    failed = 0
    reasons: Dict[str, int] = {}
    exclusions = _excluded_cover_directories(settings)
    _info("Bulk Jellyfin cover generation started.")
    _verbose(f"Excluded directories: {exclusions or 'none'}; no metadata refresh or upload delay.")

    query = """
    query JellyfinSyncAllCoverScenes($filter: FindFilterType!) {
      findScenes(filter: $filter) {
        count
        scenes {
          id
          organized
          files { path }
          paths { screenshot }
        }
      }
    }
    """

    while True:
        data = _gql_post(
            stash_base,
            cookie,
            query,
            {"filter": {"page": page, "per_page": page_size}},
        )
        page_result = ((data.get("data") or {}).get("findScenes") or {})
        scenes = page_result.get("scenes") or []
        if page == 1:
            total = int(page_result.get("count") or len(scenes))
            _info(f"Bulk cover generation: {total} scenes to inspect.")
        if not scenes:
            break

        for listed_scene in scenes:
            scene_id = str(listed_scene.get("id") or "").strip()
            if not scene_id:
                continue
            processed += 1
            reason: Optional[str] = None
            excluded_dir = _excluded_cover_directory(listed_scene, settings)
            if excluded_dir:
                reason = f"excluded-directory: {excluded_dir}"
            elif _bool(settings.get("skipUnorganized"), False) and not listed_scene.get("organized"):
                reason = "unorganized"
            elif not str(((listed_scene.get("paths") or {}).get("screenshot") or "")).strip():
                reason = "missing-screenshot"

            if reason:
                skipped += 1
                reason_key = reason.split(":", 1)[0]
                reasons[reason_key] = reasons.get(reason_key, 0) + 1
                _verbose(f"Bulk cover scene {scene_id}: skipped ({reason}).")
                continue

            try:
                # Unlike the single-scene button, never store a newly discovered
                # Jellyfin URL: writing a scene URL fires Scene.Update.Post, which
                # would queue the undesired automatic metadata refresh.
                result = _handle_generate_scene_cover_operation(
                    json_input,
                    settings,
                    stash,
                    {"sceneId": scene_id},
                    store_fallback_url=False,
                )
                if result.get("skipped"):
                    skipped += 1
                    reason_key = str(result["skipped"])
                    reasons[reason_key] = reasons.get(reason_key, 0) + 1
                    _verbose(f"Bulk cover scene {scene_id}: skipped ({reason_key}).")
                elif result.get("ok") and result.get("coverGeneratedAndUploaded"):
                    uploaded += 1
                    _verbose(f"Bulk cover scene {scene_id}: generated, saved and uploaded.")
                else:
                    failed += 1
                    log.error(f"Bulk cover scene {scene_id}: failed: {result}.")
            except Exception as exc:
                failed += 1
                log.error(f"Bulk cover scene {scene_id}: failed: {exc}.")

        _info(
            f"Bulk covers: {processed}/{total} checked, "
            f"{uploaded} uploaded, {skipped} skipped, {failed} failed."
        )
        page += 1
        if processed >= total or len(scenes) < page_size:
            break

    summary = {
        "ok": failed == 0,
        "total": total,
        "processed": processed,
        "generatedAndUploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "skipReasons": reasons,
        "excludedDirectories": exclusions,
        "metadataRefreshRequested": False,
        "uploadDelayApplied": False,
    }
    _info(f"Bulk cover generation completed: {uploaded} uploaded, {skipped} skipped, {failed} failed.")
    _verbose(f"Bulk Jellyfin cover generation details: {json.dumps(summary, ensure_ascii=False)}")
    return summary


def _handle_scene_update(json_input: Dict[str, Any], stash: StashInterface, settings: Dict[str, Any], scene_id: str) -> int:
    scene = stash.find_scene(scene_id)
    if not scene:
        log.error(f"Scene {scene_id} not found.")
        return 1

    if _bool(settings.get("skipUnorganized"), False) and not scene.get("organized"):
        _verbose("Scene is not organized; skipping.")
        return 0

    base_url = settings["jellyfinBaseUrl"]
    api_key = settings["jellyfinApiKey"]
    verify_tls = settings["verifyTls"]

    # Direct URL normally wins. If it has become stale (Jellyfin returns 404),
    # recover by running the same safe fallback resolver and replace the stale
    # Stash URL only after a new unambiguous item has been found.
    item_id = _find_direct_jellyfin_id(scene, "scene")
    resolved_by_fallback = False
    stale_item_id: Optional[str] = None

    if not item_id:
        if _entity_has_jellyfin_url(scene, base_url):
            log.warning(
                "Scene contains a Jellyfin-looking URL, but no valid 32-character ItemId "
                "could be extracted. Fallback search is intentionally disabled to avoid "
                "binding a different video."
            )
            return 0

        _verbose(
            "Scene has no Jellyfin URL. Starting original fallback search "
            "(exact path -> title/filename -> Search/Hints)."
        )
        item_id = _fallback_search_scene_item(scene, settings)
        if not item_id:
            return 0
        resolved_by_fallback = True

    # Directory exclusions protect only our generated/re-uploaded custom covers.
    # They must NOT weaken the normal Jellyfin metadata refresh: metadata and
    # provider images are always refreshed/replaced here.
    excluded_dir = _excluded_cover_directory(scene, settings)
    import time
    refresh_started_at = time.monotonic()
    refresh_status = _refresh_jellyfin_item(
        base_url, api_key, item_id, verify_tls, refresh_images=True
    )

    if refresh_status == "not_found":
        stale_item_id = item_id
        log.warning(
            f"Scene {scene_id}: Jellyfin item from the stored URL no longer exists; "
            "trying the safe fallback search for a replacement."
        )
        replacement_id = _fallback_search_scene_item(scene, settings)
        if not replacement_id or replacement_id == stale_item_id:
            log.warning(
                f"Scene {scene_id}: no replacement Jellyfin item was found. "
                "The outdated URL was left unchanged for a future retry."
            )
            return 0

        item_id = replacement_id
        refresh_started_at = time.monotonic()
        refresh_status = _refresh_jellyfin_item(
            base_url, api_key, item_id, verify_tls, refresh_images=True
        )
        if refresh_status == "not_found":
            log.warning(
                f"Scene {scene_id}: fallback found Jellyfin item {item_id}, but it also "
                "no longer exists. Synchronization skipped."
            )
            return 0
        if refresh_status != "ok":
            return 1

        resolved_by_fallback = True
        _replace_stale_jellyfin_scene_url(
            stash, scene, stale_item_id, item_id, settings
        )

    elif refresh_status != "ok":
        return 1

    refresh_ok = True

    # Stash scene ratings are stored on a 1-100 scale. Exactly 100 (5 stars)
    # maps to Jellyfin Favorite; every lower/empty rating maps to not favorite.
    raw_rating = scene.get("rating100")
    if raw_rating is None and scene.get("rating") is not None:
        try:
            raw_rating = int(scene.get("rating")) * 20
        except (TypeError, ValueError):
            raw_rating = 0
    try:
        scene_favorite = int(raw_rating or 0) >= 100
    except (TypeError, ValueError):
        scene_favorite = False

    favorite_user_id = _resolve_favorite_user_id(
        base_url, api_key, verify_tls, settings.get("jellyfinUserId")
    )
    favorite_ok = True
    if favorite_user_id:
        favorite_ok = _jf_set_favorite(
            base_url, api_key, item_id, favorite_user_id, scene_favorite, verify_tls
        )

    # Once the fallback has produced an unambiguous result, save the Jellyfin URL.
    # Subsequent updates therefore use the fast/direct path and never search again.
    if resolved_by_fallback and stale_item_id is None:
        _store_fallback_jellyfin_url(stash, scene, item_id, settings)

    if excluded_dir:
        _verbose(
            f"Automatic generated cover skipped for scene {scene_id}: "
            f"media directory is excluded ({excluded_dir}). "
            "Full Jellyfin metadata and image refresh still ran normally."
        )
        cover_ok = True
    else:
        cover_result = _generate_and_upload_scene_cover(
            json_input,
            settings,
            str(scene_id),
            item_id,
            refresh_started_at,
            force_enabled=False,
            apply_upload_delay=True,
        )
        # No/invalid Stash screenshot is a normal cover skip, not a plugin failure.
        cover_ok = True if cover_result is None else bool(cover_result)

    return 0 if (refresh_ok and favorite_ok and cover_ok) else 1


def _queue_deferred_performer_favorite_task(
    json_input: Dict[str, Any],
    performer_id: str,
    retry: int = 0,
) -> Optional[str]:
    """Queue favorite sync outside the post-hook.

    The task is intentionally unnamed so it does not appear as another manual
    task in the plugin settings.  It is a normal Stash job and therefore does
    not keep Performer.Update.Post open while Process Performers is running.
    """
    sc = json_input.get("server_connection") or json_input.get("serverConnection") or {}
    stash_base = _stash_base_from_server_connection(sc)
    cookie = _stash_cookie_from_server_connection(sc)

    mutation = """
    mutation JellyfinSyncQueuePerformerFavorite($pluginId: ID!, $description: String!, $args: Map) {
      runPluginTask(plugin_id: $pluginId, description: $description, args_map: $args)
    }
    """
    variables = {
        "pluginId": "JellyfinSync",
        "description": f"Jellyfin Sync: deferred performer favorite ({performer_id})",
        "args": {
            "mode": "deferredPerformerFavorite",
            "performerId": str(performer_id),
            "retry": int(retry),
        },
    }
    data = _gql_post(stash_base, cookie, mutation, variables)
    job_id = (((data or {}).get("data") or {}).get("runPluginTask"))
    if job_id:
        _verbose(
            f"Performer {performer_id}: deferred favorite synchronization queued "
            f"as Stash job {job_id}."
        )
        return str(job_id)
    return None


def _stash_process_performers_jobs(stash_base: str, cookie: str) -> List[Dict[str, Any]]:
    query = """
    query JellyfinSyncActiveJobs {
      jobQueue { id status description }
    }
    """
    data = _gql_post(stash_base, cookie, query, {})
    jobs = (((data or {}).get("data") or {}).get("jobQueue")) or []
    result: List[Dict[str, Any]] = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        description = str(job.get("description") or "")
        status = str(job.get("status") or "").upper()
        if PROCESS_PERFORMERS_JOB_TEXT in description.casefold() and status in {
            "READY", "RUNNING", "STOPPING"
        }:
            result.append(job)
    return result


def _handle_deferred_performer_favorite_operation(
    json_input: Dict[str, Any],
    settings: Dict[str, Any],
    args: Dict[str, Any],
) -> Dict[str, Any]:
    """Sync performer favorite only after the external Process Performers job.

    READY jobs are handled by re-queueing this task at the end of the Stash job
    queue instead of waiting and potentially preventing the READY job from
    starting. RUNNING/STOPPING jobs are polled until they finish. After that we
    re-read the performer from Stash and use its final favorite value.
    """
    import time

    performer_id = str(args.get("performerId") or args.get("performer_id") or "").strip()
    if not performer_id:
        return {"ok": False, "error": "performerId is required"}

    try:
        retry = int(args.get("retry") or 0)
    except (TypeError, ValueError):
        retry = 0

    sc = json_input.get("server_connection") or json_input.get("serverConnection") or {}
    stash_base = _stash_base_from_server_connection(sc)
    cookie = _stash_cookie_from_server_connection(sc)

    # Give the task that reacts to the same favorite click a moment to enter
    # Stash's jobQueue before we inspect it.
    time.sleep(DEFERRED_FAVORITE_INITIAL_DELAY_SECONDS)

    jobs = _stash_process_performers_jobs(stash_base, cookie)
    ready_jobs = [j for j in jobs if str(j.get("status") or "").upper() == "READY"]
    if ready_jobs:
        if retry >= DEFERRED_FAVORITE_MAX_REQUEUES:
            log.warning(
                f"Performer {performer_id}: Process Performers is still queued after "
                f"{retry} retries; Jellyfin favorite synchronization skipped for now."
            )
            return {"ok": True, "skipped": "process-performers-still-ready"}

        new_job_id = _queue_deferred_performer_favorite_task(
            json_input, performer_id, retry=retry + 1
        )
        if not new_job_id:
            return {"ok": False, "error": "could not requeue deferred performer favorite sync"}
        return {
            "ok": True,
            "requeued": True,
            "jobId": new_job_id,
            "reason": "process-performers-ready",
        }

    deadline = time.monotonic() + DEFERRED_FAVORITE_RUNNING_WAIT_SECONDS
    while True:
        running = [
            j for j in jobs
            if str(j.get("status") or "").upper() in {"RUNNING", "STOPPING"}
        ]
        if not running:
            break
        if time.monotonic() >= deadline:
            if retry < DEFERRED_FAVORITE_MAX_REQUEUES:
                new_job_id = _queue_deferred_performer_favorite_task(
                    json_input, performer_id, retry=retry + 1
                )
                return {
                    "ok": True,
                    "requeued": bool(new_job_id),
                    "jobId": new_job_id,
                    "reason": "process-performers-running-timeout",
                }
            log.warning(
                f"Performer {performer_id}: timed out waiting for Process Performers; "
                "Jellyfin favorite synchronization skipped for now."
            )
            return {"ok": True, "skipped": "process-performers-timeout"}

        time.sleep(DEFERRED_FAVORITE_POLL_SECONDS)
        jobs = _stash_process_performers_jobs(stash_base, cookie)

    # Process Performers is no longer active. Read the final Stash state now,
    # not the transient value that existed when the heart was clicked.
    performer = _get_performer(stash_base, cookie, performer_id)
    person_id = _find_direct_jellyfin_id(performer, "performer")
    if not person_id:
        _verbose(
            f"Performer {performer_id} has no supported Jellyfin URL; "
            "deferred favorite sync skipped."
        )
        return {"ok": True, "skipped": "no-jellyfin-url"}

    if "favorite" not in performer:
        log.warning(
            f"Performer {performer_id}: Stash favorite field is unavailable after "
            "Process Performers; favorite sync skipped."
        )
        return {"ok": True, "skipped": "favorite-unavailable"}

    user_id = _resolve_favorite_user_id(
        settings["jellyfinBaseUrl"],
        settings["jellyfinApiKey"],
        settings["verifyTls"],
        settings.get("jellyfinUserId"),
    )
    if not user_id:
        return {"ok": True, "skipped": "user-not-resolved"}

    favorite = _bool(performer.get("favorite"), False)
    ok = _jf_set_favorite(
        settings["jellyfinBaseUrl"],
        settings["jellyfinApiKey"],
        person_id,
        user_id,
        favorite,
        settings["verifyTls"],
    )
    return {
        "ok": bool(ok),
        "performerId": performer_id,
        "itemId": person_id,
        "favorite": favorite,
    }


def _is_performer_favorite_only_update(hook_context: Dict[str, Any]) -> bool:
    """Return True when the update mutation changed only Performer.favorite.

    Stash exposes `inputFields` in update hookContext.  We intentionally ignore
    technical identity fields so an update containing only `id` + `favorite` is
    treated as a lightweight favorite-only event.  If inputFields is missing
    (for example some scan/internal operations), fall back to the full performer
    sync path rather than guessing.
    """
    fields = hook_context.get("inputFields")
    if not isinstance(fields, list) or not fields:
        return False

    normalized = {str(field).strip() for field in fields if str(field).strip()}
    normalized.discard("id")
    normalized.discard("clientMutationId")
    return normalized == {"favorite"}


def _handle_performer_update(
    json_input: Dict[str, Any],
    settings: Dict[str, Any],
    performer_id: str,
    favorite_only: bool = False,
) -> int:
    sc = json_input.get("server_connection") or json_input.get("serverConnection") or {}
    stash_base = _stash_base_from_server_connection(sc)
    cookie = _stash_cookie_from_server_connection(sc)
    performer = _get_performer(stash_base, cookie, performer_id)

    person_id = _find_direct_jellyfin_id(performer, "performer")
    if not person_id:
        _verbose(
            f"Performer {performer_id} has no supported Jellyfin URL. "
            "Skipping without name/alias search."
        )
        return 0

    base_url = settings["jellyfinBaseUrl"]
    api_key = settings["jellyfinApiKey"]
    verify_tls = settings["verifyTls"]

    configured_user_id = settings.get("jellyfinUserId")

    # A heart/favorite toggle is intentionally lightweight.  Do not read/update
    # performer metadata and do not fetch/upload the image for favorite-only
    # Performer.Update.Post hooks.  This keeps the hook fast and avoids triggering
    # unrelated performer-processing work in Stash/other plugins.
    if favorite_only:
        if "favorite" not in performer:
            log.warning(
                f"Performer {performer_id}: Stash favorite field is unavailable; "
                "favorite-only sync skipped."
            )
            return 0

        favorite_user_id = _resolve_favorite_user_id(
            base_url, api_key, verify_tls, configured_user_id
        )
        if not favorite_user_id:
            return 0

        performer_favorite = _bool(performer.get("favorite"), False)
        return 0 if _jf_set_favorite(
            base_url, api_key, person_id, favorite_user_id, performer_favorite, verify_tls
        ) else 1

    current_item = _jf_get_item_dto(
        base_url, api_key, person_id, verify_tls, configured_user_id
    )
    item_type = str(current_item.get("Type") or "").strip()
    if item_type and item_type.lower() != "person":
        log.error(
            f"Jellyfin id {person_id} from performer URL is Type={item_type}, not Person. Skipping."
        )
        return 1

    name = performer.get("name") or performer_id
    if settings["updatePerformerMetadata"]:
        changed = _update_jellyfin_person_metadata(
            base_url, api_key, person_id, verify_tls, current_item, performer
        )
        if changed:
            _info(f"Performer {name}: metadata updated in Jellyfin.")
        else:
            _verbose(f"Jellyfin performer metadata already up to date for {name} ({person_id}).")

    if settings["updatePerformerImage"]:
        image_path = str(performer.get("image_path") or "").strip()
        if image_path:
            try:
                image_data, content_type = _fetch_stash_image(stash_base, cookie, image_path)
                converted = _reencode_image_to_png(image_data, content_type)
                if converted:
                    image_data, content_type = converted
                elif content_type == "image/svg+xml":
                    # A malformed/unsupported SVG should not fail the whole performer hook.
                    log.warning(
                        f"Performer {name}: Stash SVG image could not be converted; "
                        "Jellyfin image update skipped."
                    )
                    image_data = b""

                if image_data:
                    _jf_post_image(base_url, api_key, person_id, image_data, content_type, verify_tls)
                    _info(f"Performer {name}: image uploaded to Jellyfin.")
            except Exception as exc:
                # Image issues are non-fatal: metadata/favorite sync must continue.
                log.warning(
                    f"Performer {name}: Stash image could not be processed; "
                    f"Jellyfin image update skipped ({exc})."
                )
        else:
            _verbose(f"Performer {name} has no Stash image; Jellyfin image was not changed.")

    # Stash performer heart/favorite is synchronized to the exact Jellyfin
    # Person referenced by the performer URL. No name/alias search is used.
    if "favorite" in performer:
        favorite_user_id = _resolve_favorite_user_id(
            base_url, api_key, verify_tls, configured_user_id
        )
        if favorite_user_id:
            performer_favorite = _bool(performer.get("favorite"), False)
            if not _jf_set_favorite(
                base_url, api_key, person_id, favorite_user_id, performer_favorite, verify_tls
            ):
                return 1
    else:
        log.warning("Stash Performer.favorite field is unavailable; performer favorite sync skipped.")

    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    json_input = json.load(sys.stdin)
    server_connection = json_input["server_connection"]
    stash = StashInterface(server_connection)

    config = stash.get_configuration()
    settings: Dict[str, Any] = {
        "jellyfinBaseUrl": "http://localhost:8096",
        "jellyfinApiKey": "",
        "jellyfinUserId": "",
        "verifyTls": False,
        "skipUnorganized": False,
        "updatePerformerMetadata": True,
        "updatePerformerImage": True,
        "syncPlaybackPosition": False,
        "generateJellyfinCovers": False,
        "coverExcludedDirectories": "",
        "verboseLogging": False,
    }
    settings.update((config.get("plugins") or {}).get("JellyfinSync") or {})
    global _VERBOSE_LOGGING
    _VERBOSE_LOGGING = _bool(settings.get("verboseLogging"), False)

    settings["jellyfinBaseUrl"] = str(settings.get("jellyfinBaseUrl") or "").strip().rstrip("/")
    settings["jellyfinApiKey"] = str(settings.get("jellyfinApiKey") or "").strip()
    settings["jellyfinUserId"] = str(settings.get("jellyfinUserId") or "").strip()
    settings["verifyTls"] = _bool(settings.get("verifyTls"), False)
    settings["skipUnorganized"] = _bool(settings.get("skipUnorganized"), False)
    settings["updatePerformerMetadata"] = _bool(settings.get("updatePerformerMetadata"), True)
    settings["updatePerformerImage"] = _bool(settings.get("updatePerformerImage"), True)
    settings["syncPlaybackPosition"] = _bool(settings.get("syncPlaybackPosition"), False)
    settings["generateJellyfinCovers"] = _bool(settings.get("generateJellyfinCovers"), False)
    settings["coverUploadDelaySeconds"] = _cover_upload_delay_seconds(settings)

    if not settings["jellyfinBaseUrl"] or not settings["jellyfinApiKey"]:
        log.error("Missing Jellyfin base URL or API key in plugin settings.")
        return 1

    if settings["jellyfinUserId"] and not ITEM_ID_RE.fullmatch(settings["jellyfinUserId"]):
        log.error("Invalid Jellyfin user ID in plugin settings; expected a 32-character GUID without dashes.")
        return 1

    args = json_input.get("args") or {}

    # Synchronous operation used by the UI player integration. Raw plugin
    # operations must write {"output": ...} to stdout.
    if args.get("mode") == "syncPlaybackPosition":
        try:
            result = _handle_playback_position_operation(stash, settings, args)
            sys.stdout.write(json.dumps({"output": result}, ensure_ascii=False))
            return 0 if result.get("ok", False) else 1
        except requests.HTTPError as exc:
            response = exc.response
            message = (
                f"HTTP {response.status_code}: {(response.text or '')[:1200]}"
                if response is not None
                else f"HTTP error: {exc}"
            )
            sys.stdout.write(json.dumps({"error": message}, ensure_ascii=False))
            return 1
        except Exception as exc:
            sys.stdout.write(json.dumps({"error": str(exc)}, ensure_ascii=False))
            return 1

    if args.get("mode") == "uploadGeneratedCovers":
        try:
            result = _upload_saved_generated_covers_operation(stash, settings)
            sys.stdout.write(json.dumps({"output": result}, ensure_ascii=False))
            return 0 if result.get("ok", False) else 1
        except requests.HTTPError as exc:
            response = exc.response
            message = (
                f"HTTP {response.status_code}: {(response.text or '')[:1200]}"
                if response is not None
                else f"HTTP error: {exc}"
            )
            sys.stdout.write(json.dumps({"error": message}, ensure_ascii=False))
            return 1
        except Exception as exc:
            sys.stdout.write(json.dumps({"error": str(exc)}, ensure_ascii=False))
            return 1

    if args.get("mode") == "generateSceneCover":
        try:
            result = _handle_generate_scene_cover_operation(json_input, settings, stash, args)
            sys.stdout.write(json.dumps({"output": result}, ensure_ascii=False))
            return 0 if result.get("ok", False) else 1
        except requests.HTTPError as exc:
            response = exc.response
            message = (
                f"HTTP {response.status_code}: {(response.text or '')[:1200]}"
                if response is not None
                else f"HTTP error: {exc}"
            )
            sys.stdout.write(json.dumps({"error": message}, ensure_ascii=False))
            return 1
        except Exception as exc:
            sys.stdout.write(json.dumps({"error": str(exc)}, ensure_ascii=False))
            return 1

    if args.get("mode") == "generateAllSceneCovers":
        try:
            result = _handle_generate_all_scene_covers_operation(json_input, stash, settings)
            sys.stdout.write(json.dumps({"output": result}, ensure_ascii=False))
            return 0 if result.get("ok", False) else 1
        except Exception as exc:
            log.error(f"Bulk Jellyfin cover generation failed: {exc}")
            sys.stdout.write(json.dumps({"error": str(exc)}, ensure_ascii=False))
            return 1

    if args.get("mode") == "deferredPerformerFavorite":
        try:
            result = _handle_deferred_performer_favorite_operation(json_input, settings, args)
            sys.stdout.write(json.dumps({"output": result}, ensure_ascii=False))
            return 0 if result.get("ok", False) else 1
        except Exception as exc:
            log.error(f"Deferred performer favorite sync failed: {exc}")
            sys.stdout.write(json.dumps({"error": str(exc)}, ensure_ascii=False))
            return 1

    hook_context = args.get("hookContext") or {}
    hook_type = hook_context.get("type")
    object_id = hook_context.get("id")

    if not hook_type or not object_id:
        _verbose("No supported hook context; nothing to do.")
        return 0

    try:
        if hook_type == "Scene.Update.Post":
            return _handle_scene_update(json_input, stash, settings, str(object_id))

        if hook_type in ("Performer.Create.Post", "Performer.Update.Post"):
            favorite_only = (
                hook_type == "Performer.Update.Post"
                and _is_performer_favorite_only_update(hook_context)
            )
            if favorite_only:
                _verbose(
                    f"Performer {object_id}: favorite-only update detected; "
                    "deferring Jellyfin favorite sync until Process Performers finishes."
                )
                try:
                    job_id = _queue_deferred_performer_favorite_task(
                        json_input, str(object_id), retry=0
                    )
                    if not job_id:
                        log.warning(
                            f"Performer {object_id}: could not queue deferred favorite sync; "
                            "no immediate Jellyfin write was attempted."
                        )
                except Exception as exc:
                    log.warning(
                        f"Performer {object_id}: could not queue deferred favorite sync; "
                        f"no immediate Jellyfin write was attempted ({exc})."
                    )
                return 0

            return _handle_performer_update(
                json_input, settings, str(object_id), favorite_only=False
            )

        _verbose(f"Unsupported hook type {hook_type}; nothing to do.")
        return 0
    except requests.HTTPError as exc:
        response = exc.response
        if response is not None:
            log.error(f"HTTP error {response.status_code}: {(response.text or '')[:1200]}")
        else:
            log.error(f"HTTP error: {exc}")
        return 1
    except Exception as exc:
        log.error(f"Jellyfin Sync failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
