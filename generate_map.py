#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import re
import csv
import time
import json
from dataclasses import dataclass, asdict, replace
from datetime import datetime
from urllib.parse import urlparse, urljoin, parse_qs
from urllib import robotparser

import requests
from bs4 import BeautifulSoup

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.distance import geodesic

import folium
from folium.plugins import MarkerCluster, Fullscreen, LocateControl, MousePosition
from branca.element import MacroElement, Template


# ---------------- CONFIG ----------------

SITES = [
    "https://www.funda.nl/zoeken/koop?selected_area=[%22valkenburg-li,30km%22]&price=%22300000-900000%22&object_type=[%22apartment%22,%22house%22]&object_type_house=[%22villa%22,%22town_house%22,%22former_farm%22,%22bungalow%22]&object_type_apartment_orientation=[%22corridor_flat%22,%22service_flat%22,%22basement%22,%22sheltered%22,%22piano_nobile%22]&object_type_apartment=[%22ground_floor%22,%22flat_with_porch%22,%22with_external_access%22,%22house_with_porch%22,%22double_ground_floor%22,%22penthouse%22,%22mezzanine%22,%22maisonnette%22]&bedrooms=%220-2%22&search_result=1",
    "https://www.funda.nl/zoeken/koop?selected_area=[%22valkenburg-li,30km%22]&price=%22300000-900000%22&object_type=[%22apartment%22,%22house%22]&object_type_house=[%22villa%22,%22town_house%22,%22former_farm%22,%22bungalow%22]&object_type_apartment_orientation=[%22corridor_flat%22,%22service_flat%22,%22basement%22,%22sheltered%22,%22piano_nobile%22]&object_type_apartment=[%22ground_floor%22,%22flat_with_porch%22,%22with_external_access%22,%22house_with_porch%22,%22double_ground_floor%22,%22penthouse%22,%22mezzanine%22,%22maisonnette%22]&bedrooms=%220-2%22&search_result=2",
    "https://www.funda.nl/zoeken/koop?selected_area=[%22valkenburg-li,30km%22]&price=%22300000-900000%22&object_type=[%22apartment%22,%22house%22]&object_type_house=[%22villa%22,%22town_house%22,%22former_farm%22,%22bungalow%22]&object_type_apartment_orientation=[%22corridor_flat%22,%22service_flat%22,%22basement%22,%22sheltered%22,%22piano_nobile%22]&object_type_apartment=[%22ground_floor%22,%22flat_with_porch%22,%22with_external_access%22,%22house_with_porch%22,%22double_ground_floor%22,%22penthouse%22,%22mezzanine%22,%22maisonnette%22]&bedrooms=%220-2%22&search_result=3",
    "https://www.immoweb.be/nl/zoeken/huis-en-appartement/te-koop?countries=BE&maxBedroomCount=3&maxPrice=900000&minBedroomCount=2&minPrice=300000&postalCodes=3620,3621,3630,3770,3793,3798&propertySubtypes=BUNGALOW,APARTMENT_BLOCK,FARMHOUSE,CHALET,GROUND_FLOOR,PENTHOUSE,LOFT,SERVICE_FLAT,PAVILION,OTHER_PROPERTY,MIXED_USE_BUILDING,EXCEPTIONAL_PROPERTY&buildingConditions=GOOD,TO_BE_DONE_UP,AS_NEW,JUST_RENOVATED&page=1&orderBy=relevance"
]

KEYWORDS: list[str] = []

SLEEP_SEC_BETWEEN_SITES = 2.0
TIMEOUT = 25
USER_AGENT = "Mozilla/5.0 (compatible; ImmoWatchSnapshot/1.3)"

SLEEP_SEC_BETWEEN_DETAIL_PAGES = 0.6
SLEEP_SEC_BETWEEN_GEOCODE = 1.1

SLEEP_SEC_BETWEEN_IMMOWEB_DETAIL = 0.8
SLEEP_SEC_BETWEEN_FUNDA_DETAIL = 0.8

CENTER_NAME = "Valkenburg aan de Geul, Nederland"
CENTER_FALLBACK_LATLON = (50.8650, 5.8320)
RADIUS_KM = 40

OUT_SNAPSHOT_CSV = r"bungalows_snapshot.csv"
OUT_NEW_CSV = r"bungalows_new.csv"
OUT_MAP_HTML = r"bungalows_map.html"
GEO_CACHE_JSON = r"geo_cache.json"
REV_CACHE_JSON = r"reverse_cache.json"

COLUMNS = [
    "scraped_at", "source", "title", "price_text", "location_text", "since_text", "url",
    "municipality", "lat", "lon", "distance_km"
]


# --------------- MODEL ---------------

@dataclass(frozen=True)
class Row:
    scraped_at: str
    source: str
    title: str
    price_text: str
    location_text: str
    since_text: str
    url: str
    municipality: str = ""
    lat: float | None = None
    lon: float | None = None
    distance_km: float | None = None


# --------------- HELPERS ---------------

def robots_allows(url: str) -> bool:
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch(USER_AGENT, url)
    except Exception:
        return True

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def keyword_ok(text: str) -> bool:
    if not KEYWORDS:
        return True
    t = text.lower()
    return all(k.lower() in t for k in KEYWORDS)

def absolute_url(base: str, href: str) -> str:
    if href.startswith("http"):
        return href
    return urljoin(base, href)

def fetch(url: str, referer: str | None = None) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if referer:
        headers["Referer"] = referer

    r = requests.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=True, verify=False)
    r.raise_for_status()
    return r.text

def write_csv(path: str, rows: list[Row]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(COLUMNS)
        for r in rows:
            d = asdict(r)
            w.writerow([d.get(c, "") for c in COLUMNS])

def load_prev_urls(path: str) -> set[str]:
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            rdr = csv.DictReader(f, delimiter=";")
            return {(row.get("url") or "").strip() for row in rdr if (row.get("url") or "").startswith("http")}
    except FileNotFoundError:
        return set()

def load_json_cache(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}

def save_json_cache(path: str, cache: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# --------------- DOMAIN FILTERS ---------------

FUNDA_DETAIL_RE = re.compile(r"^https?://(www\.)?funda\.nl/detail/koop/.+/huis-.+/\d+/?", re.IGNORECASE)
IMMOWEB_DETAIL_RE = re.compile(r"^https?://(www\.)?immoweb\.be/nl/(advertentie|zoekertje)/", re.IGNORECASE)
IMMOWEB_ID_RE = re.compile(r"(\d{6,})")

def is_allowed_detail_url(site_host: str, low_url: str) -> bool:
    if "funda.nl" in site_host:
        return FUNDA_DETAIL_RE.match(low_url) is not None
    if "immoweb.be" in site_host:
        return (IMMOWEB_DETAIL_RE.match(low_url) is not None) and (IMMOWEB_ID_RE.search(low_url) is not None)
    return False


# ---------------- FUNDA: COORDS/ADDRESS FROM DETAIL + VALIDATION ----------------

def _safe_float(x) -> float | None:
    try:
        return float(str(x).strip())
    except Exception:
        return None

def normalize_latlon(lat: float | None, lon: float | None) -> tuple[float, float] | None:
    if lat is None or lon is None:
        return None
    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
        return None

    if abs(lat) > 90 and abs(lon) <= 90:
        lat, lon = lon, lat

    if abs(lat) > 90 or abs(lon) > 180:
        return None

    if not (49.0 <= lat <= 54.5 and 2.0 <= lon <= 8.5):
        return None

    return (lat, lon)

def funda_extract_latlon_from_detail(detail_html: str) -> tuple[float | None, float | None]:
    if not detail_html:
        return (None, None)

    soup = BeautifulSoup(detail_html, "lxml")

    for s in soup.select('script[type="application/ld+json"]'):
        txt = (s.string or "").strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue

        def walk(obj):
            if isinstance(obj, dict):
                geo = obj.get("geo")
                if isinstance(geo, dict):
                    lat = geo.get("latitude") or geo.get("lat")
                    lon = geo.get("longitude") or geo.get("lon") or geo.get("lng")
                    normed = normalize_latlon(_safe_float(lat), _safe_float(lon))
                    if normed:
                        return normed
                for v in obj.values():
                    got = walk(v)
                    if got:
                        return got
            elif isinstance(obj, list):
                for it in obj:
                    got = walk(it)
                    if got:
                        return got
            return None

        got = walk(data)
        if got:
            return got[0], got[1]

    meta_lat = None
    meta_lon = None
    for meta in soup.select("meta"):
        k = (meta.get("property") or meta.get("name") or "").lower()
        v = (meta.get("content") or "").strip()
        if not v:
            continue
        if any(x in k for x in ["place:location:latitude", "og:latitude", "latitude"]):
            meta_lat = _safe_float(v)
        if any(x in k for x in ["place:location:longitude", "og:longitude", "longitude"]):
            meta_lon = _safe_float(v)

    normed = normalize_latlon(meta_lat, meta_lon)
    if normed:
        return normed[0], normed[1]

    mlat = re.search(r'"latitude"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1', detail_html)
    mlon = re.search(r'"longitude"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1', detail_html)
    if mlat and mlon:
        normed = normalize_latlon(_safe_float(mlat.group(2)), _safe_float(mlon.group(2)))
        if normed:
            return normed[0], normed[1]

    mlat2 = re.search(r'"lat"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1', detail_html)
    mlon2 = re.search(r'"lng"\s*:\s*("?)(-?\d+(?:\.\d+)?)\1', detail_html)
    if mlat2 and mlon2:
        normed = normalize_latlon(_safe_float(mlat2.group(2)), _safe_float(mlon2.group(2)))
        if normed:
            return normed[0], normed[1]

    return (None, None)

def funda_extract_address_from_detail(detail_html: str) -> str:
    if not detail_html:
        return ""

    soup = BeautifulSoup(detail_html, "lxml")

    for s in soup.select('script[type="application/ld+json"]'):
        txt = (s.string or "").strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue

        def walk(obj):
            if isinstance(obj, dict):
                addr = obj.get("address")
                if isinstance(addr, dict):
                    street = norm(addr.get("streetAddress") or "")
                    pc = norm(addr.get("postalCode") or "")
                    city = norm(addr.get("addressLocality") or addr.get("addressRegion") or "")
                    parts = []
                    if street:
                        parts.append(street)
                    if pc or city:
                        parts.append(norm(f"{pc} {city}"))
                    if parts:
                        return ", ".join([p for p in parts if p]).strip(", ").strip()

                for v in obj.values():
                    got = walk(v)
                    if got:
                        return got
            elif isinstance(obj, list):
                for it in obj:
                    got = walk(it)
                    if got:
                        return got
            return ""

        got = walk(data)
        if got:
            return got

    return ""


# --------------- IMMOWEB: robust filtering + detail fallback ---------------

PRICE_RE = re.compile(r"€\s?[\d\.\,]+", re.IGNORECASE)

def immoweb_postal_set_from_url(search_url: str) -> set[str]:
    q = parse_qs(urlparse(search_url).query)
    pc_raw = (q.get("postalCodes", [""])[0] or "").strip()
    return {p.strip() for p in pc_raw.split(",") if p.strip().isdigit()}

def build_postcode_regex(postal_set: set[str]) -> re.Pattern | None:
    if not postal_set:
        return None
    return re.compile(r"\b(" + "|".join(sorted(map(re.escape, postal_set))) + r")\b")

def is_sponsored_text(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in ["gesponsord", "sponsored", "advertentie", "promoted", "promotion"])

def node_html_contains_ad_markers(node) -> bool:
    if not node:
        return False
    try:
        h = (str(node) or "").lower()
    except Exception:
        return False
    return any(k in h for k in [
        "gesponsord", "sponsored", "advertentie", "promoted",
        "data-sponsored", "data-ad", "adbadge", "ad-badge"
    ])

def pick_card_node(a):
    card = a.find_parent(["article", "li"])
    if card:
        return card
    node = a
    for _ in range(12):
        node = getattr(node, "parent", None)
        if not node:
            break
        try:
            t = norm(node.get_text(" ", strip=True))
        except Exception:
            continue
        if 80 <= len(t) <= 20000:
            return node
    return a.parent

def card_text_from_node(card_node) -> str:
    if not card_node:
        return ""
    try:
        return norm(card_node.get_text(" ", strip=True))
    except Exception:
        return ""

def extract_location_from_text(text: str, pc_re: re.Pattern | None) -> str:
    if not text:
        return ""
    postcode = ""
    if pc_re is not None:
        m = pc_re.search(text)
        if m:
            postcode = m.group(1)
    else:
        m = re.search(r"\b(\d{4})\b", text)
        if m:
            postcode = m.group(1)

    if not postcode:
        return ""

    mloc = re.search(r"\b" + re.escape(postcode) + r"\b\s+([A-Za-zÀ-ÿ'`\- ]{2,80})", text)
    if mloc:
        place = norm(mloc.group(1))
        place = re.split(r"\s+€|[|•\n]|\s+\d+\s*m²", place, flags=re.IGNORECASE)[0].strip()
        if place:
            return f"{postcode} {place}"
    return postcode

def immoweb_extract_price_from_detail(detail_html: str) -> str:
    if not detail_html:
        return ""
    soup = BeautifulSoup(detail_html, "lxml")

    for meta in soup.select("meta"):
        key = (meta.get("property") or meta.get("name") or "").lower()
        val = (meta.get("content") or "").strip()
        if not val:
            continue
        if "price" in key or "amount" in key:
            m = re.search(r"\b(\d{5,})\b", val.replace(".", "").replace(",", ""))
            if m:
                n = int(m.group(1))
                return f"€ {n:,}".replace(",", ".")

    for s in soup.select('script[type="application/ld+json"]'):
        txt = (s.string or "").strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue

        def walk(obj):
            if isinstance(obj, dict):
                offers = obj.get("offers")
                if isinstance(offers, dict):
                    p = offers.get("price")
                    if isinstance(p, (int, float)) or (isinstance(p, str) and re.fullmatch(r"\d{5,}", p.strip())):
                        return int(float(p))
                if isinstance(offers, list):
                    for it in offers:
                        n = walk({"offers": it})
                        if n:
                            return n
                p = obj.get("price")
                if isinstance(p, (int, float)) or (isinstance(p, str) and re.fullmatch(r"\d{5,}", p.strip())):
                    return int(float(p))
                for v in obj.values():
                    n = walk(v)
                    if n:
                        return n
            elif isinstance(obj, list):
                for it in obj:
                    n = walk(it)
                    if n:
                        return n
            return None

        n = walk(data)
        if n:
            return f"€ {n:,}".replace(",", ".")

    m = re.search(r'"price"\s*:\s*"?(\\d{5,})"?', detail_html)
    if m:
        n = int(m.group(1))
        return f"€ {n:,}".replace(",", ".")

    txt = norm(soup.get_text(" ", strip=True))
    mp = PRICE_RE.search(txt)
    if mp:
        return norm(mp.group(0))
    return ""

def immoweb_detail_passes_filters(detail_html: str, pc_re: re.Pattern | None) -> tuple[bool, str]:
    if not detail_html:
        return False, ""
    low = detail_html.lower()
    if "captcha" in low or "cloudflare" in low or "je bent bijna op de pagina" in low:
        return False, ""
    soup = BeautifulSoup(detail_html, "lxml")
    txt = norm(soup.get_text(" ", strip=True))
    if pc_re is not None and not pc_re.search(txt):
        return False, ""
    loc = extract_location_from_text(txt, pc_re)
    return True, loc

def immoweb_extract_title_from_detail(detail_html: str) -> str:
    soup = BeautifulSoup(detail_html, "lxml")
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        return norm(og["content"])
    h1 = soup.select_one("h1")
    if h1:
        return norm(h1.get_text(" ", strip=True))
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    return norm(title)[:120]

def passes_immoweb_server_filters(detail_html: str) -> bool:
    if not detail_html:
        return False

    soup = BeautifulSoup(detail_html, "lxml")
    txt = norm(soup.get_text(" "))
    low_txt = txt.lower()

    renovation_terms = [
        "te renoveren", "te verbouwen",
        "grote renovatie", "renovatie verplicht", "moet gerenoveerd"
    ]
    if any(term in low_txt for term in renovation_terms):
        return False

    bedroom_match = re.search(r'(\d+)\s*slaap(?:kamer|kamers?)(?:\(|s\))?', low_txt)
    if bedroom_match and int(bedroom_match.group(1)) > 3:
        print(f"[FILTER] Uitgesloten: {bedroom_match.group(0)} slaapkamers")
        return False

    bathroom_match = re.search(r'(\d+)\s*(badkamer|badkamers?|sdb?|wc|toiletten?|salle\s+de\s+bain)', low_txt)
    if bathroom_match and int(bathroom_match.group(1)) > 1:
        print(f"[FILTER] Uitgesloten: {bathroom_match.group(0)}")
        return False

    exclude_types = [
        "kot",
        "gemengd gebruik", "gemengde bestemming", "handels pand", "professioneel"
    ]
    if any(etype in low_txt for etype in exclude_types):
        print(f"[FILTER] Uitgesloten type: {next((etype for etype in exclude_types if etype in low_txt), 'onbekend')}")
        return False

    exclude_terms = ["kantoor", "winkel", "commercieel", "handelspand"]
    if any(term in low_txt for term in exclude_terms):
        return False

    return True

def scrape_immoweb_search_page(search_url: str, html: str) -> list[Row]:
    soup = BeautifulSoup(html, "lxml")
    now = datetime.utcnow().isoformat(timespec="seconds")
    site_host = urlparse(search_url).netloc.lower()

    postal_set = immoweb_postal_set_from_url(search_url)
    pc_re = build_postcode_regex(postal_set)

    anchors = soup.select('a[href*="/nl/advertentie/"], a[href*="/nl/zoekertje/"]')
    print(f"[IMMOWEB] anchors in HTML: {len(anchors)}")

    results: dict[str, Row] = {}
    seen: set[str] = set()

    c_bad_url = c_no_cardnode = c_no_price = c_sponsored = c_no_pc = c_detail_fail = 0
    c_detail_used_for_price = 0

    for a in anchors:
        href = a.get("href", "")
        if not href:
            continue

        url = absolute_url(search_url, href)
        low_url = url.lower()

        if not is_allowed_detail_url(site_host, low_url):
            c_bad_url += 1
            continue
        if url in seen:
            continue

        card_node = pick_card_node(a)
        if not card_node:
            c_no_cardnode += 1
            continue

        card_text = card_text_from_node(card_node)
        if not card_text:
            c_no_cardnode += 1
            continue

        if is_sponsored_text(card_text) or node_html_contains_ad_markers(card_node):
            c_sponsored += 1
            continue

        mp = PRICE_RE.search(card_text)
        price_text = ""
        if mp:
            price_text = norm(mp.group(0))
        else:
            attrs_blob = []
            try:
                for tag in card_node.find_all(True, limit=120):
                    for k in ("aria-label", "title"):
                        v = tag.get(k)
                        if v:
                            attrs_blob.append(v)
            except Exception:
                pass
            mp2 = PRICE_RE.search(" ".join(attrs_blob))
            if mp2:
                price_text = norm(mp2.group(0))

        detail_html = None
        if not price_text:
            try:
                detail_html = fetch(url, referer="https://www.immoweb.be/")
            except Exception:
                c_detail_fail += 1
                continue

            price_text = immoweb_extract_price_from_detail(detail_html)
            if price_text:
                c_detail_used_for_price += 1
            else:
                c_no_price += 1
                continue

            time.sleep(SLEEP_SEC_BETWEEN_IMMOWEB_DETAIL)

        location_text = extract_location_from_text(card_text, pc_re)
        has_pc_on_card = bool(location_text) and (pc_re.search(location_text) if pc_re else True)

        if pc_re is not None and not has_pc_on_card:
            if detail_html is None:
                try:
                    detail_html = fetch(url, referer="https://www.immoweb.be/")
                except Exception:
                    c_detail_fail += 1
                    continue
                time.sleep(SLEEP_SEC_BETWEEN_IMMOWEB_DETAIL)

            ok, loc_from_detail = immoweb_detail_passes_filters(detail_html, pc_re)
            if not ok:
                c_no_pc += 1
                continue
            location_text = loc_from_detail or location_text

        title = norm(a.get_text(" ", strip=True))
        if len(title) < 4:
            if detail_html is not None:
                title = immoweb_extract_title_from_detail(detail_html) or "Immoweb listing"
            else:
                title = norm(card_text.split("€")[0])[:120] or "Immoweb listing"

        seen.add(url)
        results.setdefault(url, Row(
            scraped_at=now,
            source=site_host,
            title=title,
            price_text=price_text,
            location_text=location_text,
            since_text="",
            url=url
        ))

    print(f"[IMMOWEB] filtered: bad_url={c_bad_url}, no_cardnode={c_no_cardnode}, no_price={c_no_price}, sponsored={c_sponsored}, no_pc/detail_fail={c_no_pc}/{c_detail_fail}, detail_used_for_price={c_detail_used_for_price}")

    clean_rows = []
    for row in results.values():
        if is_sponsored_text(row.title + " " + row.location_text):
            continue
        try:
            detail_html = fetch(row.url, referer="https://www.immoweb.be/")
            if not passes_immoweb_server_filters(detail_html):
                continue
        except Exception:
            continue
        clean_rows.append(row)
        time.sleep(0.3)
        if len(clean_rows) >= 30:
            break

    print(f"[IMMOWEB] keeping first {len(clean_rows)} homes (server-filters OK, max 30)")
    if len(clean_rows) < 30:
        print(f"[WARN] Minder dan 30 echte matches ({len(clean_rows)})")
    return clean_rows


# --------------- SCRAPE SEARCH PAGE (generic) ---------------

def scrape_search_page(search_url: str, html: str) -> list[Row]:
    site_host = urlparse(search_url).netloc.lower()

    if "immoweb.be" in site_host:
        return scrape_immoweb_search_page(search_url, html)

    soup = BeautifulSoup(html, "lxml")
    now = datetime.utcnow().isoformat(timespec="seconds")
    results: dict[str, Row] = {}

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        title = norm(a.get_text(" ", strip=True))
        if not href or len(title) < 4:
            continue

        url = absolute_url(search_url, href)
        low = url.lower()

        if any(x in low for x in ["mailto:", "tel:", "javascript:", "privacy", "cookie", "contact", "login", "inloggen"]):
            continue

        if not is_allowed_detail_url(site_host, low):
            continue

        card = a.find_parent(["article", "li", "div"])
        card_text = norm(card.get_text(" ", strip=True) if card else title)

        if not keyword_ok(title + " " + card_text):
            continue

        price_text = ""
        mp = re.search(r"€\s?[\d\.\,]+(?:\s*[a-z\.]+)?", card_text, re.IGNORECASE)
        if mp:
            price_text = norm(mp.group(0))

        location_text = ""
        for sel in [
            ".location", ".address", ".plaats", ".place",
            "[class*=location]", "[data-test*=location]",
            ".search-result__address", ".search-result__location",
            "[class*='address']", ".listing__location",
            ".object-header__address"
        ]:
            el = card.select_one(sel) if card else None
            if el:
                location_text = norm(el.get_text(" ", strip=True))
                break

        if not location_text:
            mloc = re.search(r"\b(\d{4}\s*[A-Z]{2})\b", title)
            if mloc:
                location_text = norm(mloc.group(1))

        if not location_text and "funda.nl" in site_host:
            mloc = re.search(r"funda\.nl/detail/koop/([^/]+)/", url.lower())
            if mloc:
                location_text = norm(mloc.group(1).replace("-", " "))

        since_text = ""
        ms = re.search(
            r"(\d+\s*(?:dagen?|uren?)\s*geleden|nieuw|vandaag|gisteren|\b\d{1,2}[-/]\d{1,2}[-/]\d{4}\b)",
            card_text,
            re.IGNORECASE
        )
        if ms:
            since_text = norm(ms.group(1))

        results.setdefault(url, Row(
            scraped_at=now,
            source=site_host,
            title=title,
            price_text=price_text,
            location_text=location_text,
            since_text=since_text,
            url=url
        ))

    return list(results.values())


# --------------- GEO ---------------

NL_POSTCODE_RE = re.compile(r"\b(\d{4})\s*([A-Z]{2})\b", re.IGNORECASE)
BE_POSTCODE_RE = re.compile(r"\b(\d{4})\b")

def country_from_source(source: str) -> str:
    return "BE" if "immoweb.be" in (source or "").lower() else "NL"

def extract_postcode_any(text: str, country: str) -> str:
    if country == "BE":
        m = BE_POSTCODE_RE.search(text or "")
        return m.group(1) if m else ""
    m = NL_POSTCODE_RE.search(text or "")
    return f"{m.group(1)} {m.group(2).upper()}" if m else ""

def extract_place_from_location_text(location_text: str) -> str:
    if not location_text:
        return ""
    m = re.search(r"\b\d{4}(?:\s*[A-Z]{2})?\s+([A-Za-zÀ-ÿ'`\- ]{2,})", location_text)
    if not m:
        return ""
    place = m.group(1)
    place = re.split(r"\s+€|[|•\n]|\s+\d+\s*m²|\s+k\.k\.|\s+v\.o\.n\.", place, flags=re.IGNORECASE)[0]
    return norm(place)

def extract_place_from_funda_url(url: str) -> str:
    m = re.search(r"funda\.nl/detail/koop/([^/]+)/", url.lower())
    if not m:
        return ""
    return m.group(1).replace("-", " ")

def extract_street_from_title(title: str) -> str:
    pc = extract_postcode_any(title, "NL")
    if not pc:
        return ""
    before = title.split(pc)[0]
    return norm(before)

def guess_address_variants(row: Row, allow_generic: bool = True) -> list[str]:
    country = country_from_source(row.source)
    suffix = "België" if country == "BE" else "Nederland"

    variants: list[str] = []

    pc = extract_postcode_any(row.location_text, country) or extract_postcode_any(row.title, country)
    place = extract_place_from_location_text(row.location_text)

    if not place and country == "NL":
        place = extract_place_from_funda_url(row.url)

    street = extract_street_from_title(row.title) if country == "NL" else ""

    if street and pc and place:
        variants.append(f"{street}, {pc} {place}, {suffix}")
    if pc and place:
        variants.append(f"{pc} {place}, {suffix}")
    if pc and not place:
        variants.append(f"{pc}, {suffix}")
    if place:
        variants.append(f"{place}, {suffix}")

    if allow_generic:
        if row.title:
            variants.append(f"{row.title}, {suffix}")
        variants.append(f"Limburg, {suffix}")

    out, seen = [], set()
    for v in variants:
        v = norm(v)
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out

def get_center_latlon(geocode) -> tuple[float, float]:
    loc = geocode(CENTER_NAME)
    if loc:
        return (loc.latitude, loc.longitude)
    return CENTER_FALLBACK_LATLON

def geocode_and_enrich_rows(rows: list[Row]) -> list[Row]:
    geolocator = Nominatim(user_agent="bungalow_mapper/1.3")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=SLEEP_SEC_BETWEEN_GEOCODE)
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=SLEEP_SEC_BETWEEN_GEOCODE)

    geo_cache = load_json_cache(GEO_CACHE_JSON)
    rev_cache = load_json_cache(REV_CACHE_JSON)

    center_latlon = get_center_latlon(geocode)

    enriched: list[Row] = []
    for r in rows:
        latlon: tuple[float, float] | None = None
        is_funda = ("funda.nl" in (r.source or "").lower())
        allow_generic = (not is_funda)

        if r.url in geo_cache:
            latlon = normalize_latlon(geo_cache[r.url].get("lat"), geo_cache[r.url].get("lon"))
            if latlon is None:
                geo_cache.pop(r.url, None)
                save_json_cache(GEO_CACHE_JSON, geo_cache)

        if not latlon and is_funda:
            try:
                detail_html = fetch(r.url, referer="https://www.funda.nl/")

                lat, lon = funda_extract_latlon_from_detail(detail_html)
                latlon = normalize_latlon(lat, lon)

                if latlon:
                    geo_cache[r.url] = {"lat": latlon[0], "lon": latlon[1]}
                    save_json_cache(GEO_CACHE_JSON, geo_cache)
                else:
                    addr = funda_extract_address_from_detail(detail_html)
                    if addr:
                        addr_full = norm(f"{addr}, Nederland")

                        if addr_full in geo_cache:
                            latlon = normalize_latlon(geo_cache[addr_full].get("lat"), geo_cache[addr_full].get("lon"))
                            if latlon is None:
                                geo_cache.pop(addr_full, None)
                                save_json_cache(GEO_CACHE_JSON, geo_cache)

                        if not latlon:
                            loc = geocode(addr_full)
                            if loc:
                                latlon = normalize_latlon(loc.latitude, loc.longitude)
                                if latlon:
                                    geo_cache[r.url] = {"lat": latlon[0], "lon": latlon[1]}
                                    geo_cache[addr_full] = {"lat": latlon[0], "lon": latlon[1]}
                                    save_json_cache(GEO_CACHE_JSON, geo_cache)

                time.sleep(SLEEP_SEC_BETWEEN_FUNDA_DETAIL)
            except Exception:
                pass

        if not latlon:
            for addr in guess_address_variants(r, allow_generic=allow_generic):
                if addr in geo_cache:
                    latlon = normalize_latlon(geo_cache[addr].get("lat"), geo_cache[addr].get("lon"))
                    if latlon:
                        break
                    geo_cache.pop(addr, None)
                    save_json_cache(GEO_CACHE_JSON, geo_cache)

        if not latlon:
            found = None
            for addr in guess_address_variants(r, allow_generic=allow_generic):
                loc = geocode(addr)
                if loc:
                    found = (loc.latitude, loc.longitude, addr)
                    break

            if not found:
                print("[GEO FAIL]", r.source, "|", r.title, "|", r.url)
                continue

            latlon = normalize_latlon(found[0], found[1])
            if not latlon:
                print("[BAD COORDS NOMINATIM]", r.source, "|", r.title, "|", r.url, "|", found[0], found[1], "|", found[2])
                continue

            geo_cache[r.url] = {"lat": latlon[0], "lon": latlon[1]}
            geo_cache[found[2]] = {"lat": latlon[0], "lon": latlon[1]}
            save_json_cache(GEO_CACHE_JSON, geo_cache)

        lat, lon = latlon
        latlon2 = normalize_latlon(lat, lon)
        if not latlon2:
            print("[BAD COORDS]", r.source, "|", r.title, "|", r.url, "|", lat, lon)
            continue
        lat, lon = latlon2

        dist_km = float(geodesic(center_latlon, (lat, lon)).km)
        if dist_km > RADIUS_KM:
            continue

        rev_key = f"{lat:.6f},{lon:.6f}"
        municipality = ""
        if rev_key in rev_cache:
            municipality = rev_cache[rev_key].get("municipality", "")
        else:
            try:
                loc = reverse((lat, lon), language="nl", zoom=10, addressdetails=True)
            except Exception:
                loc = None

            if loc and isinstance(loc.raw, dict):
                addr = loc.raw.get("address", {}) or {}
                municipality = (
                    addr.get("municipality")
                    or addr.get("city")
                    or addr.get("town")
                    or addr.get("village")
                    or addr.get("county")
                    or ""
                )
                rev_cache[rev_key] = {"municipality": municipality}
                save_json_cache(REV_CACHE_JSON, rev_cache)

        enriched.append(replace(
            r,
            municipality=municipality,
            lat=lat,
            lon=lon,
            distance_km=round(dist_km, 1)
        ))

    print(f"[GEO] binnen {RADIUS_KM:.0f} km: {len(enriched)} / {len(rows)}")
    return enriched


# --------------- MAP UI (dropdown + address search + legend) ---------------

def make_legend_html() -> str:
    return """
    <div style="
        position: fixed;
        bottom: 22px;
        right: 12px;
        z-index: 9999;
        background: rgba(255,255,255,0.92);
        border: 1px solid #bbb;
        border-radius: 8px;
        padding: 10px 12px;
        font-size: 12px;
        max-width: 290px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.12);
    ">
      <div style="font-weight:700; margin-bottom:6px;">Tips</div>
      <div>• Rechtsboven: kaartlagen (OSM / Satelliet).</div>
      <div>• Klik clusters om te “spiderfy-en”.</div>
      <div>• Zoeken: middenboven (type → enter).</div>
    </div>
    """

class MunicipalityJump(MacroElement):
    def __init__(self, map_name: str, options: list[dict]):
        super().__init__()
        self._name = "MunicipalityJump"
        self.map_name = map_name
        self.options = options
        self._template = Template("""
        {% macro html(this, kwargs) %}
        <div style="
            position: fixed;
            top: 12px;
            right: 12px;
            z-index: 9999;
            background: rgba(255,255,255,0.92);
            border: 1px solid #bbb;
            border-radius: 8px;
            padding: 10px 12px;
            font-size: 12px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.12);
            min-width: 220px;
        ">
          <div style="font-weight:700; margin-bottom:6px;">Ga naar gemeente</div>
          <select id="muniJumpSelect" style="width: 100%; padding: 6px; border-radius: 6px; border: 1px solid #ccc;">
            <option value="">— kies —</option>
            {% for opt in this.options %}
              <option value="{{ opt.lat }},{{ opt.lon }},{{ opt.zoom }}">{{ opt.name }}</option>
            {% endfor %}
          </select>
        </div>

        <script>
        (function(){
          var sel = document.getElementById("muniJumpSelect");
          if(!sel) return;
          sel.addEventListener("change", function(){
            var v = sel.value;
            if(!v) return;
            var parts = v.split(",");
            if(parts.length < 3) return;
            var lat = parseFloat(parts[0]);
            var lon = parseFloat(parts[1]);
            var zoom = parseInt(parts[2], 10);
            if(isNaN(lat) || isNaN(lon) || isNaN(zoom)) return;
            {{ this.map_name }}.setView([lat, lon], zoom, {animate:true});
          });
        })();
        </script>
        {% endmacro %}
        """)

class AddressSearchControl(MacroElement):
    """
    Leaflet Control Geocoder search bar (Nominatim) positioned top-center.
    Uses CDN JS/CSS in the resulting HTML (no python dependency).
    """
    def __init__(self, map_name: str):
        super().__init__()
        self._name = "AddressSearchControl"
        self.map_name = map_name
        self._template = Template("""
        {% macro header(this, kwargs) %}
        <link rel="stylesheet" href="https://unpkg.com/leaflet-control-geocoder/dist/Control.Geocoder.css" />
        <script src="https://unpkg.com/leaflet-control-geocoder/dist/Control.Geocoder.js"></script>
        <style>
          /* Make geocoder look nice and centered */
          .leaflet-control-geocoder {
            border-radius: 10px !important;
            overflow: hidden !important;
          }
          .leaflet-control-geocoder-form input {
            width: 360px !important;
            max-width: calc(100vw - 40px) !important;
          }
        </style>
        {% endmacro %}

        {% macro script(this, kwargs) %}
        (function(){
          var map = {{ this.map_name }};
          if(!map || !L || !L.Control || !L.Control.Geocoder) return;

          var geocoder = L.Control.geocoder({
            defaultMarkGeocode: true,
            placeholder: "Zoek adres / plaats…",
            geocoder: L.Control.Geocoder.nominatim()
          })
          .on('markgeocode', function(e) {
            var bbox = e.geocode && e.geocode.bbox;
            if(bbox){
              map.fitBounds(bbox);
            } else if(e.geocode && e.geocode.center){
              map.setView(e.geocode.center, 15);
            }
          })
          .addTo(map);

          // force top-center positioning
          var c = geocoder.getContainer();
          if(c){
            c.style.position = "fixed";
            c.style.top = "12px";
            c.style.left = "50%";
            c.style.transform = "translateX(-50%)";
            c.style.zIndex = 9999;
            c.style.boxShadow = "0 2px 10px rgba(0,0,0,0.12)";
            c.style.background = "rgba(255,255,255,0.92)";
            c.style.border = "1px solid #bbb";
            c.style.borderRadius = "10px";
          }
        })();
        {% endmacro %}
        """)

def compute_municipality_centroids(rows: list[Row]) -> list[dict]:
    buckets: dict[str, list[tuple[float, float]]] = {}
    for r in rows:
        if r.lat is None or r.lon is None:
            continue
        name = (r.municipality or "").strip() or "Onbekende gemeente"
        buckets.setdefault(name, []).append((r.lat, r.lon))

    opts: list[dict] = []
    for name, pts in buckets.items():
        lat = sum(p[0] for p in pts) / len(pts)
        lon = sum(p[1] for p in pts) / len(pts)
        zoom = 12 if len(pts) <= 5 else 11
        opts.append({"name": f"{name} ({len(pts)})", "lat": round(lat, 6), "lon": round(lon, 6), "zoom": zoom})

    opts.sort(key=lambda d: d["name"].lower())
    return opts


# --------------- MAP ---------------

def write_map(rows: list[Row]) -> None:
    rows = [r for r in rows if r.lat is not None and r.lon is not None]
    if not rows:
        print("[WARN] Geen punten om te plotten.")
        return

    avg_lat = sum(r.lat for r in rows) / len(rows)
    avg_lon = sum(r.lon for r in rows) / len(rows)

    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=9, control_scale=True, tiles=None)

    # Base layers
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap", control=True).add_to(m)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri World Imagery",
        name="Satelliet (Esri)",
        control=True,
        overlay=False,
    ).add_to(m)

    # UX plugins (measure removed)
    Fullscreen(position="topleft").add_to(m)
    LocateControl(position="topleft").add_to(m)
    MousePosition(position="bottomleft", separator=" | ", num_digits=5).add_to(m)

    # Listings cluster
    listings_cluster = MarkerCluster(
        name="Listings",
        options={
            "spiderfyOnMaxZoom": True,
            "showCoverageOnHover": False,
            "zoomToBoundsOnClick": True,
            "maxClusterRadius": 45,
            "spiderfyDistanceMultiplier": 1.8,
        },
    ).add_to(m)

    for r in rows:
        mun = r.municipality or "Onbekende gemeente"
        dist = f"{r.distance_km:.1f} km" if isinstance(r.distance_km, (int, float)) else ""

        popup_html = (
            f"<b>{r.title}</b><br>"
            f"{r.price_text or ''}<br>"
            f"<i>{mun}</i> — {dist}<br>"
            f"{r.location_text or ''}<br>"
            f"{r.since_text or ''}<br>"
            f"<a href='{r.url}' target='_blank'>link</a>"
        )
        tip = " | ".join([mun, r.price_text or "", r.since_text or ""]).strip(" |")

        folium.Marker(
            location=[r.lat, r.lon],
            popup=folium.Popup(popup_html, max_width=380),
            tooltip=tip,
        ).add_to(listings_cluster)

    # Fit bounds
    bounds = [[min(r.lat for r in rows), min(r.lon for r in rows)],
              [max(r.lat for r in rows), max(r.lon for r in rows)]]
    m.fit_bounds(bounds, padding=(20, 20))

    folium.LayerControl(collapsed=False).add_to(m)

    # Municipality jump dropdown (right top)
    muni_opts = compute_municipality_centroids(rows)
    m.add_child(MunicipalityJump(m.get_name(), muni_opts))

    # Address search bar (TOP CENTER)
    m.add_child(AddressSearchControl(m.get_name()))

    # Legend
    m.get_root().html.add_child(folium.Element(make_legend_html()))

    m.save(OUT_MAP_HTML)
    print(f"[INFO] Kaart geschreven: {OUT_MAP_HTML}")


# --------------- MAIN ---------------

def run() -> None:
    if not SITES:
        print("Geen sites in SITES[]")
        return

    prev_urls = load_prev_urls(OUT_SNAPSHOT_CSV)
    all_rows: list[Row] = []

    for i, search_url in enumerate(SITES):
        if not robots_allows(search_url):
            print(f"[SKIP robots] {search_url}")
            continue

        try:
            host = urlparse(search_url).netloc.lower()
            if "funda.nl" in host:
                referer = "https://www.funda.nl/"
            elif "immoweb.be" in host:
                referer = "https://www.immoweb.be/"
            else:
                referer = None

            html = fetch(search_url, referer=referer)
            rows = scrape_search_page(search_url, html)
            print(f"[INFO] {urlparse(search_url).netloc}: {len(rows)} detail links")
        except Exception as e:
            print(f"[ERROR] {search_url} -> {e}")
            rows = []

        all_rows.extend(rows)

        if i < len(SITES) - 1:
            time.sleep(SLEEP_SEC_BETWEEN_SITES)

    uniq: dict[str, Row] = {}
    for r in all_rows:
        uniq.setdefault(r.url, r)
    detail_rows = list(uniq.values())

    filtered_rows: list[Row] = []
    for r in detail_rows:
        if "funda.nl" in r.source or "immoweb.be" in r.source:
            filtered_rows.append(r)
            continue
        time.sleep(SLEEP_SEC_BETWEEN_DETAIL_PAGES)

    enriched_rows = geocode_and_enrich_rows(filtered_rows)

    write_csv(OUT_SNAPSHOT_CSV, enriched_rows)
    new_rows = [r for r in enriched_rows if r.url not in prev_urls]
    write_csv(OUT_NEW_CSV, new_rows)

    print(f"\nSnapshot (<= {RADIUS_KM:.0f} km): {len(enriched_rows)}")
    print(f"Nieuw sinds vorige run: {len(new_rows)}")
    print(f"- Snapshot: {OUT_SNAPSHOT_CSV}")
    print(f"- New:      {OUT_NEW_CSV}")

    write_map(enriched_rows)


if __name__ == "__main__":
    run()
