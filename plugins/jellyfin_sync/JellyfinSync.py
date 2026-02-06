import json
import os
import re
import sys
from urllib.parse import parse_qs
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

import requests
import stashapi.log as log
from stashapi.stashapp import StashInterface


# ---------------------------
# Helpers
# ---------------------------

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


_MONTH_WORDS_EN = {
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
}


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


def _extract_jellyfin_item_id_from_url(url: str) -> Optional[str]:
    """Extracts a Jellyfin ItemId from common URL formats or our marker format."""
    if not url:
        return None

    # our stored marker: jellyfin/items/<ItemId>
    m = re.search(r"\bjellyfin/items/([0-9a-fA-F]{32})\b", url)
    if m:
        return m.group(1)

    # Jellyfin web hash routes:
    #   /web/#/details?id=<ItemId>
    #   /web/index.html#!/details?id=<ItemId>
    if "#/details" in url or "#!/details" in url:
        frag = url.split("#", 1)[1]
        q = frag.split("?", 1)[1] if "?" in frag else ""
        qs = parse_qs(q)
        item_id = (qs.get("id") or qs.get("Id") or [None])[0]
        if item_id and re.fullmatch(r"[0-9a-fA-F]{32}", item_id):
            return item_id

    # REST pattern:
    m = re.search(r"/Items/([0-9a-fA-F]{32})\b", url)
    if m:
        return m.group(1)

    return None


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


def _pick_best_item_by_title_date(items: List[Dict[str, Any]], scene: dict, stash_path: str, expected_name: str) -> Optional[Dict[str, Any]]:
    """Pick the best item from search results using extra scene data.

    Strategy:
    1) Prefer exact basename match by file path (when available).
    2) If multiple candidates remain, filter by scene date (same day or one day earlier).
    3) If still multiple, prefer exact name match.
    4) Fallback to first item.
    """
    if not items:
        return None

    # 1) Score by basename match strength.
    scored: List[Tuple[int, Dict[str, Any]]] = []
    for it in items:
        p = it.get("Path") or ""
        scored.append((_basename_matches_stash(p, stash_path), it))
    scored.sort(key=lambda x: x[0], reverse=True)

    best_score = scored[0][0]
    candidates = [it for score, it in scored if score == best_score] if best_score > 0 else [it for _, it in scored]

    # 2) Filter by date if we have scene date.
    acceptable_dates = _scene_date_candidates(scene)
    if acceptable_dates:
        filtered = []
        for it in candidates:
            d = _candidate_item_date(it)
            if d and d in acceptable_dates:
                filtered.append(it)
        if len(filtered) == 1:
            return filtered[0]
        if len(filtered) > 1:
            candidates = filtered

    # 3) Prefer exact name match (normalized)
    exp = _norm(expected_name or "")
    if exp:
        exact = []
        for it in candidates:
            nm = _norm(it.get("Name") or it.get("name") or "")
            if nm == exp:
                exact.append(it)
        if len(exact) == 1:
            return exact[0]
        if len(exact) > 1:
            candidates = exact

    return candidates[0] if candidates else None


def _rewrite_prefix(path: str, prefix_from: str, prefix_to: str) -> str:
    """Simple prefix rewrite for path mapping between Stash and Jellyfin."""
    path = path or ""
    pf = (prefix_from or "").rstrip("/")
    pt = (prefix_to or "").rstrip("/")
    if not pf or not pt:
        return path

    # Normalize to forward slashes for prefix check, but keep original for output.
    # Jellyfin typically uses platform-native paths; in Linux both are '/'.
    if path.startswith(pf + "/") or path == pf:
        return pt + path[len(pf):]
    return path


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


def jellyfin_post(base_url: str, api_key: str, path: str, params: Optional[dict] = None, verify_tls: bool = True) -> requests.Response:
    url = base_url.rstrip("/") + path
    return requests.post(url, headers=_build_headers(api_key), params=params or {}, timeout=40, verify=verify_tls)


def jellyfin_post_json(base_url: str, api_key: str, path: str, payload: dict, verify_tls: bool = True) -> requests.Response:
    url = base_url.rstrip("/") + path
    headers = _build_headers(api_key)
    headers["Content-Type"] = "application/json"
    return requests.post(url, headers=headers, json=payload, timeout=40, verify=verify_tls)


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
                    log.info(f"Fallback search without trailing punctuation: '{t2}'")
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
                    log.info(f"Fallback hints without trailing punctuation: '{t2}'")
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


def pick_best_hint(hints: List[Dict[str, Any]], stash_path: str, scene_title: str) -> Optional[str]:
    """Pick best matching hint by name.

    Uses both the raw filename (without extension) and a "quality-stripped" variant
    (e.g. removes a trailing " - [WEBDL-1080p]") because Jellyfin may store the
    item's Name without that suffix for videos that have not been identified yet.
    """
    bn_raw = _norm(_basename_no_ext(stash_path))
    bn_clean = _norm(_strip_quality_suffix(_basename_no_ext(stash_path)))
    title_raw = _norm(scene_title or "")
    title_clean = _norm(_strip_quality_suffix(scene_title or ""))

    # Prefer exact name match to filename (raw/clean), then exact match to title.
    exact_fn = []
    exact_title = []
    loose = []

    for h in hints:
        name = _norm(h.get('Name') or h.get('name') or "")
        if not name:
            continue
        hid = _hint_get_item_id(h)
        if not hid:
            continue

        if bn_raw and name == bn_raw:
            exact_fn.append(hid)
        elif bn_clean and name == bn_clean:
            exact_fn.append(hid)
        elif title_raw and name == title_raw:
            exact_title.append(hid)
        elif title_clean and name == title_clean:
            exact_title.append(hid)
        elif bn_raw and (bn_raw in name or name in bn_raw):
            loose.append(hid)
        elif bn_clean and (bn_clean in name or name in bn_clean):
            loose.append(hid)
        elif title_raw and (title_raw in name or name in title_raw):
            loose.append(hid)
        elif title_clean and (title_clean in name or name in title_clean):
            loose.append(hid)

    if exact_fn:
        return exact_fn[0]
    if exact_title:
        return exact_title[0]
    if loose:
        return loose[0]
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

def pick_best_match(items: List[Dict[str, Any]], stash_path: str) -> Optional[Dict[str, Any]]:
    stash_bn = _norm(_basename_no_ext(stash_path))
    if not stash_bn:
        return None

    exact = []
    loose = []
    for it in items:
        path = it.get("Path") or ""
        bn = _norm(_basename_no_ext(path))
        if not bn:
            continue
        if bn == stash_bn:
            exact.append(it)
        elif stash_bn in bn or bn in stash_bn:
            loose.append(it)

    if exact:
        return exact[0]
    if loose:
        return loose[0]
    return None


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


# ---------------------------
# Jellyfin VirtualFolders + path matching (based on patterns used by other tools)
# ---------------------------

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


def jellyfin_notify_updated_media(base_url: str, api_key: str, file_path: str, update_type: str, verify_tls: bool) -> bool:
    """POST /Library/Media/Updated with a single path update."""
    if not file_path:
        return False

    payload = {
        "Updates": [
            {
                "Path": file_path,
                "UpdateType": update_type or "Modified",
            }
        ]
    }

    r = jellyfin_post_json(base_url, api_key, "/Library/Media/Updated", payload=payload, verify_tls=verify_tls)
    if not r.ok:
        log.error(f"Jellyfin point-scan failed: HTTP {r.status_code}: {r.text}")
        return False

    # Jellyfin typically returns 204 No Content for success.
    log.info(f"Jellyfin point-scan accepted: HTTP {r.status_code}")
    return True


def jellyfin_get_item_path(
    base_url: str,
    api_key: str,
    item_id: str,
    user_id: Optional[str],
    verify_tls: bool,
) -> Optional[str]:
    """
    Best-effort: fetch the real filesystem path for an itemId from Jellyfin.

    Notes:
    - Some servers reject /Items/{id} with HTTP 400 ("Error processing request") unless scoped to a user.
    - Prefer /Users/{userId}/Items/{id} when possible.
    """
    if not item_id:
        return None

    tried = []

    def _try(endpoint: str) -> Optional[dict]:
        tried.append(endpoint)
        r = jellyfin_get(base_url, api_key, endpoint, params={"Fields": "Path"}, verify_tls=verify_tls)
        if not r.ok:
            return None
        try:
            return r.json()
        except Exception:
            return None

    # 1) Try user-scoped item details
    uid = (user_id or "").strip()
    if not uid:
        uid = jellyfin_pick_user_id(base_url, api_key, verify_tls=verify_tls) or ""

    if uid:
        data = _try(f"/Users/{uid}/Items/{item_id}")
        if data:
            p = data.get("Path")
            if isinstance(p, str) and p:
                return p
            ms = data.get("MediaSources") or []
            if ms and isinstance(ms, list) and isinstance(ms[0], dict):
                p2 = ms[0].get("Path")
                if isinstance(p2, str) and p2:
                    return p2

    # 2) Fallback: server-scoped endpoint
    data = _try(f"/Items/{item_id}")
    if not data:
        # Last resort: log the most likely failing endpoint
        r = jellyfin_get(base_url, api_key, f"/Items/{item_id}", params={"Fields": "Path"}, verify_tls=verify_tls)
        log.warning(f"Jellyfin item path lookup failed. Tried: {', '.join(tried) or '(none)'}; "
                    f"last HTTP {r.status_code}: {r.text}")
        return None

    p = data.get("Path")
    if isinstance(p, str) and p:
        return p

    ms = data.get("MediaSources") or []
    if ms and isinstance(ms, list) and isinstance(ms[0], dict):
        p2 = ms[0].get("Path")
        if isinstance(p2, str) and p2:
            return p2

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


def jellyfin_get_server_id(base_url: str, api_key: str, verify_tls: bool) -> Optional[str]:
    """Fetch Jellyfin serverId for building stable Web UI links."""
    for ep in ("/System/Info", "/System/Info/Public"):
        r = jellyfin_get(base_url, api_key, ep, verify_tls=verify_tls)
        if not r.ok:
            continue
        try:
            data = r.json()
        except Exception:
            continue
        sid = data.get("Id") or data.get("ServerId") or data.get("ServerID")
        if isinstance(sid, str) and sid:
            return sid
    return None


def jellyfin_build_web_url(web_base_url: str, item_id: str, server_id: Optional[str], template: Optional[str]) -> str:
    """
    Build a clickable Jellyfin Web UI URL for an item.
    If server_id is missing, we fall back to a link without serverId.
    """
    base = (web_base_url or "").rstrip("/")
    tpl = (template or "").strip()

    if not base:
        # fallback to relative path
        base = ""

    if not tpl:
        # Use the hashbang style that works with the user's example
        tpl = "{base}/web/index.html#!/details?id={itemId}&serverId={serverId}"

    if server_id:
        try:
            return tpl.format(base=base, itemId=item_id, serverId=server_id)
        except Exception:
            return f"{base}/web/index.html#!/details?id={item_id}&serverId={server_id}"

    # No server_id known – omit it
    return f"{base}/web/index.html#!/details?id={item_id}"


# ---------------------------
# Main
# ---------------------------

json_input = json.load(sys.stdin)
FRAGMENT_SERVER = json_input["server_connection"]
stash = StashInterface(FRAGMENT_SERVER)

config = stash.get_configuration()
settings: Dict[str, Any] = {
    "jellyfinBaseUrl": "http://localhost:8096",
    "jellyfinApiKey": "",
    "jellyfinUserId": "",
    "verifyTls": False,

    # If true, plugin ignores scenes not marked as "organized" in Stash.
    # Requested default: OFF.
    "skipUnorganized": False,

    # --- Action toggles ---
    # Fast point scan by file path (Jellyfin: /Library/Media/Updated)
    "scanUpdatedMedia": True,
    "scanUpdateType": "Modified",

    # Fetch only missing metadata/images for the matched item (Jellyfin: /Items/{id}/Refresh with Default modes)
    "refreshMissingMetadata": True,

    # Force metadata/image refresh (you can control modes below)
    "refreshMetadata": True,
    "metadataRefreshMode": "FullRefresh",
    "imageRefreshMode": "FullRefresh",
    "replaceAllMetadata": True,
    "replaceAllImages": True,

    # --- Path matching helpers ---
    "pathRewriteFrom": "",
    "pathRewriteTo": "",
    "useVirtualFolders": True,
    "itemQueryLimit": "1000",
    "findByPathMaxPages": "50",

    # --- Store links into scene.urls ---
    # Store a clickable Jellyfin Web UI link in scene.urls (recommended ON)
    "storeJellyfinUrl": True,
    # Optionally store an internal marker (jellyfin/items/<ItemId>) for debugging; recommended OFF
    "storeJellyfinMarkerUrl": False,
    # Optional overrides for Web UI link building
    "jellyfinWebBaseUrl": "",
    "jellyfinServerId": "",
    "jellyfinWebUrlTemplate": "",

    # --- Fallback name search ---
    # If Jellyfin derived the title from the filename, it may have stripped a trailing
    # quality marker like " - [WEBDL-1080p]". When enabled, we also try a cleaned
    # filename variant in fallback searches.
    "fallbackFilenameNoQuality": True,

    # Additional fallback: Jellyfin sometimes truncates long names to
    # "<date> - <studio> - <month>" (or first-word of 3rd segment).
    # When enabled, we generate those shortened variants from the filename.
    "fallbackFilenameTruncated": True,
    "searchLimit": "25",
}

if "JellyfinSync" in (config.get("plugins") or {}):
    settings.update(config["plugins"]["JellyfinSync"])

# Some advanced switches are intentionally hidden from the Stash UI.
# Enforce the preferred defaults regardless of previously saved UI values.
settings.update({
    "useVirtualFolders": True,              # always ON
    "scanUpdatedMedia": True,               # always ON
    "scanUpdateType": "Modified",           # fixed
    "storeJellyfinMarkerUrl": False,        # always OFF
    "refreshMissingMetadata": True,         # always ON (but ignored if refreshMetadata is ON)
    "refreshMetadata": True,                # always ON
    "metadataRefreshMode": "FullRefresh",   # fixed
    "imageRefreshMode": "FullRefresh",      # fixed
    "replaceAllMetadata": True,             # always ON
    "replaceAllImages": True,               # always ON
})

# normalize some types (UI may store as strings)
try:
    search_limit = int(settings.get("searchLimit") or 25)
except Exception:
    search_limit = 25

try:
    item_limit = int(settings.get("itemQueryLimit") or 1000)
except Exception:
    item_limit = 1000

try:
    max_pages = int(settings.get("findByPathMaxPages") or 50)
except Exception:
    max_pages = 50

verify_tls = bool(settings.get("verifyTls"))

# Early exit: if this hook is only a URL update (avoid loops if we store marker)
hc = (json_input.get("args") or {}).get("hookContext") or {}
if hc:
    input_fields = hc.get("inputFields") or []
    if "urls" in input_fields and len(input_fields) <= 2:
        log.info("Hook looks like a URL-only update, nothing to do.")
        sys.exit(0)

scene_id = hc.get("id")
if not scene_id:
    log.info("No hookContext.id; nothing to do.")
    sys.exit(0)

if hc.get("type") != "Scene.Update.Post":
    log.info(f"Unsupported hook type {hc.get('type')}; exiting.")
    sys.exit(0)

scene = stash.find_scene(scene_id)
if not scene:
    log.error(f"Scene {scene_id} not found.")
    sys.exit(1)

if settings.get("skipUnorganized") and not scene.get("organized"):
    log.info("Scene is not organized; skipping.")
    sys.exit(0)

base_url = (settings.get("jellyfinBaseUrl") or "").rstrip("/")
api_key = (settings.get("jellyfinApiKey") or "").strip()
if not base_url or not api_key:
    log.error("Missing jellyfinBaseUrl or jellyfinApiKey in plugin settings.")
    sys.exit(1)

stash_path = _stash_scene_primary_file_path(scene)
jellyfin_path = _rewrite_prefix(
    stash_path,
    prefix_from=settings.get("pathRewriteFrom") or "",
    prefix_to=settings.get("pathRewriteTo") or "",
)

# If we manage to resolve the Jellyfin item, we may also know the *exact* filesystem path Jellyfin sees.
item_path: Optional[str] = None

# Decide whether we need to resolve the item id.
need_item_id = (bool(settings.get("refreshMetadata"))
               or bool(settings.get("refreshMissingMetadata"))
               or bool(settings.get("storeJellyfinUrl"))
               or bool(settings.get("storeJellyfinMarkerUrl")))

# Resolve userId once (helps on servers where /Items needs user scoping)
resolved_user_id = (settings.get("jellyfinUserId") or "").strip()
if not resolved_user_id and (need_item_id or settings.get("scanUpdatedMedia")):
    resolved_user_id = jellyfin_pick_user_id(base_url, api_key, verify_tls=verify_tls) or ""
    if resolved_user_id:
        log.info(f"Auto-picked Jellyfin userId={resolved_user_id}")

# 1) Try parse existing Jellyfin marker from scene.urls
item_id: Optional[str] = None
for u in (scene.get("urls") or []):
    item_id = _extract_jellyfin_item_id_from_url(u)
    if item_id:
        break

if item_id and settings.get("scanUpdatedMedia"):
    # If we already know itemId from a stored marker, fetch the real path Jellyfin sees.
    item_path = jellyfin_get_item_path(base_url, api_key, item_id, resolved_user_id or None, verify_tls=verify_tls)
    if item_path:
        log.info(f"Resolved Jellyfin item path from itemId: {item_path}")

# 2) If needed and not found, try exact path matching via VirtualFolders
if need_item_id and not item_id and settings.get("useVirtualFolders") and jellyfin_path:
    vfolders = jellyfin_virtual_folders(base_url, api_key, verify_tls=verify_tls)
    if vfolders:
        item_id = jellyfin_find_item_id_by_exact_path(
            base_url,
            api_key,
            resolved_user_id or None,
            jellyfin_path,
            vfolders,
            item_limit=item_limit,
            max_pages=max_pages,
            verify_tls=verify_tls,
        )
        if item_id:
            log.info(f"Matched Jellyfin itemId by exact path: {item_id}")
            # By definition, this was an exact match on file_path == Jellyfin's Path
            item_path = jellyfin_path

# 3) Fallback search by filename/title (best-effort)
if need_item_id and not item_id:
    title = (scene.get("title") or "").strip()
    filename_raw = (_basename_no_ext(stash_path) or "").strip()
    filename_clean = _strip_quality_suffix(filename_raw)
    title_clean = _strip_quality_suffix(title)

    # Build search terms in a predictable order.
    # We also expand each candidate with small punctuation variants to cover
    # differences like smart quotes/ellipsis vs straight quotes/three dots.
    #
    # 1) Scene title (preferred)
    # 2) Raw filename (no extension)
    # 3) Filename without trailing quality markers (e.g. " - [WEBDL-1080p]")
    # 4) Cleaned scene title (rarely needed, but cheap)
    terms: List[str] = []

    def _add_terms(t: str):
        for v in _title_search_variants(t):
            if v and v not in terms:
                terms.append(v)

    _add_terms(title)
    _add_terms(filename_raw)

    if settings.get("fallbackFilenameNoQuality"):
        _add_terms(filename_clean)
        _add_terms(title_clean)

    # Extra fallbacks based on common Jellyfin truncation patterns.
    # Example: "2026-02-01 - Studio - February 2026 ..." may become
    #          "2026-02-01 - Studio - February".
    if settings.get("fallbackFilenameTruncated"):
        for t in _derive_truncated_filename_terms(filename_raw):
            _add_terms(t)

    if not terms:
        log.warning("No filename/title available for fallback search.")
    else:
        user_id = (resolved_user_id or "").strip()
        performers = _scene_performer_names(scene)

        # A) Try user-scoped /Users/{id}/Items searches (returns Path field on many servers)
        if user_id:
            for idx, t in enumerate(terms):
                if idx >= 2 and t in (filename_clean, title_clean):
                    log.info(f"Fallback search: trying cleaned name '{t}'")
                items = jellyfin_search_item_user_scope(base_url, api_key, user_id, t, search_limit, verify_tls)
                narrowed = narrow_items_for_scene(items, stash_path, scene, title, t)

                # If we have multiple candidates after applying path/title/date narrowing,
                # try to disambiguate by performer name (search term + actor).
                if len(narrowed) > 1 and performers:
                    log.warning(
                        f"Multiple Jellyfin matches for '{t}' ({len(narrowed)} candidates). "
                        "Trying performer-assisted search to avoid wrong mapping."
                    )
                    found = None
                    for perf in performers[:3]:
                        q = f"{t} {perf}".strip()
                        for qv in _title_search_variants(q):
                            items2 = jellyfin_search_item_user_scope(base_url, api_key, user_id, qv, search_limit, verify_tls)
                            narrowed2 = narrow_items_for_scene(items2, stash_path, scene, title, qv)
                            if len(narrowed2) == 1:
                                found = narrowed2[0]
                                break
                        if found:
                            break
                    if found:
                        narrowed = [found]

                if len(narrowed) == 1:
                    best = narrowed[0]
                    item_id = best.get("Id")
                    p = best.get("Path")
                    if isinstance(p, str) and p:
                        item_path = p
                    log.info(f"Matched Jellyfin itemId by fallback search: {item_id}")
                    break

                if len(narrowed) > 1:
                    # Still ambiguous: refuse to guess.
                    cand_ids = [it.get('Id') for it in narrowed if it.get('Id')]
                    log.warning(
                        f"Ambiguous Jellyfin match for '{t}' even after date/performer narrowing; "
                        f"candidates={cand_ids}. Skipping this term."
                    )

        # B) If still not found, try /Search/Hints (works on some servers where /Items search isn't helpful)
        if not item_id:
            for idx, t in enumerate(terms):
                if idx >= 2 and t in (filename_clean, title_clean):
                    log.info(f"Fallback hints: trying cleaned name '{t}'")
                hints = jellyfin_search_hints(base_url, api_key, user_id or None, t, search_limit, verify_tls)
                # Collect candidate ids by name relevance.
                cand_ids = collect_hint_ids(hints, stash_path, search_term=t, scene_title=title)
                if not cand_ids:
                    continue

                # Fetch minimal details for candidates (Path/PremiereDate) to apply the same narrowing logic.
                details: List[Dict[str, Any]] = []
                for cid in cand_ids[:10]:
                    det = jellyfin_get_item_details(base_url, api_key, cid, user_id or None, verify_tls=verify_tls)
                    if det:
                        details.append(det)

                narrowed = narrow_items_for_scene(details, stash_path, scene, title, t)

                # Same disambiguation fallback: performer-assisted search
                if len(narrowed) > 1 and performers and user_id:
                    log.warning(
                        f"Multiple Jellyfin hint matches for '{t}' ({len(narrowed)} candidates). "
                        "Trying performer-assisted search to avoid wrong mapping."
                    )
                    found = None
                    for perf in performers[:3]:
                        q = f"{t} {perf}".strip()
                        for qv in _title_search_variants(q):
                            hints2 = jellyfin_search_hints(base_url, api_key, user_id or None, qv, search_limit, verify_tls)
                            cand2 = collect_hint_ids(hints2, stash_path, search_term=qv, scene_title=title)
                            if not cand2:
                                continue
                            details2 = []
                            for cid2 in cand2[:10]:
                                det2 = jellyfin_get_item_details(base_url, api_key, cid2, user_id or None, verify_tls=verify_tls)
                                if det2:
                                    details2.append(det2)
                            narrowed2 = narrow_items_for_scene(details2, stash_path, scene, title, qv)
                            if len(narrowed2) == 1:
                                found = narrowed2[0]
                                break
                        if found:
                            break
                    if found:
                        narrowed = [found]

                if len(narrowed) == 1:
                    item_id = narrowed[0].get("Id")
                    p = narrowed[0].get("Path")
                    if isinstance(p, str) and p:
                        item_path = p
                    log.info(f"Matched Jellyfin itemId by /Search/Hints: {item_id}")
                    break

                if len(narrowed) > 1:
                    cand_ids2 = [it.get('Id') for it in narrowed if it.get('Id')]
                    log.warning(
                        f"Ambiguous Jellyfin hint match for '{t}' even after date/performer narrowing; "
                        f"candidates={cand_ids2}. Skipping this term."
                    )

# 4) If we need itemId but still don't have one, we can still run point-scan (by path)


# DEBUG: log why itemId is missing when required
if need_item_id and not item_id:
    su = scene.get('urls') or []
    if su:
        log.info('No Jellyfin itemId resolved; scene.urls present but none matched known patterns.')
    else:
        log.info('No Jellyfin itemId resolved; scene.urls is empty.')
# ---- Point scan (fast) ----
scan_ok = True
if settings.get("scanUpdatedMedia"):
    scan_path = (item_path or jellyfin_path or "").strip()
    if scan_path:
        if item_path and jellyfin_path and item_path != jellyfin_path:
            log.info(
                "Using Jellyfin-reported path for point-scan: "
                f"{item_path} (mapped from Stash: {jellyfin_path})"
            )
        else:
            log.info(f"Jellyfin point-scan (Library/Media/Updated): {scan_path}")

        scan_ok = jellyfin_notify_updated_media(
            base_url,
            api_key,
            scan_path,
            update_type=str(settings.get("scanUpdateType") or "Modified"),
            verify_tls=verify_tls,
        )
    else:
        log.warning("scanUpdatedMedia enabled, but no file path is available for this scene.")

# ---- Store Jellyfin links in Stash scene.urls ----
urls_existing = set(scene.get("urls") or [])
urls_to_add: List[str] = []

# Store a clickable Web UI link (recommended)
if item_id and settings.get("storeJellyfinUrl"):
    web_base = (settings.get("jellyfinWebBaseUrl") or "").strip() or base_url
    web_base = web_base.rstrip("/")
    server_id = (settings.get("jellyfinServerId") or "").strip()
    if not server_id:
        server_id = jellyfin_get_server_id(base_url, api_key, verify_tls=verify_tls) or ""
    tpl = settings.get("jellyfinWebUrlTemplate")
    web_url = jellyfin_build_web_url(web_base, item_id, server_id or None, tpl)
    if web_url and web_url not in urls_existing:
        log.info(f"Storing Jellyfin Web URL into scene.urls: {web_url}")
        urls_to_add.append(web_url)

# Optionally store an internal marker (useful for debugging)
if item_id and settings.get("storeJellyfinMarkerUrl"):
    marker = f"jellyfin/items/{item_id}"
    if marker not in urls_existing:
        log.info(f"Storing Jellyfin marker URL into scene.urls: {marker}")
        urls_to_add.append(marker)

if urls_to_add:
    # Stash GraphQL expects urls as BulkUpdateStrings, not a raw list.
    # Depending on Stash/stashapi version, either of the following forms may work.
    try:
        stash.update_scenes({
            "ids": [scene["id"]],
            "urls": {"mode": "ADD", "values": urls_to_add},
        })
    except Exception as e:
        log.warning(f"Failed to store urls using BulkUpdateStrings form: {e}. Trying urls_mode fallback...")
        stash.update_scenes({
            "ids": [scene["id"]],
            "urls": urls_to_add,
            "urls_mode": "ADD",
        })


# ---- Optional metadata refresh ----
refresh_ok = True
if settings.get("refreshMetadata"):
    if not item_id:
        log.warning("refreshMetadata enabled, but Jellyfin itemId not found. Only point-scan (if enabled) was attempted.")
        refresh_ok = False
    else:
        # Build refresh parameters
        metadata_mode = (settings.get("metadataRefreshMode") or "").strip()
        image_mode = (settings.get("imageRefreshMode") or "").strip()
        # For explicit Refresh Metadata, default to FullRefresh (autopulse behavior)
        metadata_mode = metadata_mode or "FullRefresh"
        image_mode = image_mode or "FullRefresh"
        replace_all_metadata = bool(settings.get("replaceAllMetadata"))
        replace_all_images = bool(settings.get("replaceAllImages"))
        # Jellyfin docs: ReplaceAll* only applicable if the corresponding refresh mode is FullRefresh.
        # To avoid wiping images/metadata without re-downloading, force FullRefresh when ReplaceAll flags are enabled.
        if replace_all_metadata and metadata_mode != "FullRefresh":
                        metadata_mode = "FullRefresh"
        if replace_all_images and image_mode != "FullRefresh":
                        image_mode = "FullRefresh"
        # Some servers only fetch images reliably when metadata is also a FullRefresh (provider IDs / identification).
        if replace_all_images and metadata_mode != "FullRefresh":
                        metadata_mode = "FullRefresh"

        params = {
            "metadataRefreshMode": metadata_mode,
            "imageRefreshMode": image_mode,
            "replaceAllMetadata": "true" if replace_all_metadata else "false",
            "replaceAllImages": "true" if replace_all_images else "false",
            "recursive": "true",
        }

        log.info(f"Refreshing Jellyfin item via POST /Items/{item_id}/Refresh")
        r = jellyfin_post(base_url, api_key, f"/Items/{item_id}/Refresh", params=params, verify_tls=verify_tls)
        if not r.ok:
            log.error(f"Jellyfin refresh failed: HTTP {r.status_code}: {r.text}")
            refresh_ok = False


# ---- Optional: fetch only missing metadata/images ----
# If refreshMetadata is enabled, it takes precedence over refreshMissingMetadata.
if (not settings.get("refreshMetadata")) and settings.get("refreshMissingMetadata"):
    if not item_id:
        log.warning("refreshMissingMetadata enabled, but Jellyfin itemId not found. Only point-scan (if enabled) was attempted.")
        refresh_ok = False
    else:
        params = {
            "metadataRefreshMode": "Default",
            "imageRefreshMode": "Default",
            "replaceAllMetadata": "false",
            "replaceAllImages": "false",
            "recursive": "true",
        }
        log.info(f"Refreshing Jellyfin item (missing metadata) via POST /Items/{item_id}/Refresh")
        r = jellyfin_post(base_url, api_key, f"/Items/{item_id}/Refresh", params=params, verify_tls=verify_tls)
        if not r.ok:
            log.error(f"Jellyfin refresh (missing metadata) failed: HTTP {r.status_code}: {r.text}")
            refresh_ok = False

# Exit code
if scan_ok and refresh_ok:
    log.info("Done.")
    sys.exit(0)

log.error("Completed with errors (see logs above).")
sys.exit(1)
