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
from urllib.parse import urlparse, urljoin
from urllib import robotparser

import requests
from bs4 import BeautifulSoup

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.distance import geodesic

import folium
from folium.plugins import MarkerCluster


# ---------------- CONFIG ----------------

SITES = [
    "https://www.funda.nl/zoeken/koop?selected_area=[%22regio-zuid-limburg,50km%22]&object_type=[%22house%22]&object_type_house=[%22bungalow%22]&publication_date=%2210%22&bedrooms=%222-%22",
    # "https://woonpleinlimburg.nl/zoek-woningen/koop/nederland/beschikbaar/woonhuis/bungalow",
]

KEYWORDS: list[str] = []  # niet nodig (zoek-URLs zijn al gefilterd)

SLEEP_SEC_BETWEEN_SITES = 2.0
TIMEOUT = 25
USER_AGENT = "Mozilla/5.0 (compatible; ImmoWatchSnapshot/1.3)"

SLEEP_SEC_BETWEEN_DETAIL_PAGES = 0.6  # je gebruikt verify=False, hou requests beperkt
SLEEP_SEC_BETWEEN_GEOCODE = 1.1       # Nominatim etiquette

# Radius filter
CENTER_NAME = "Valkenburg aan de Geul, Nederland"
CENTER_FALLBACK_LATLON = (50.8650, 5.8320)  # best-effort fallback
RADIUS_KM = 100.0

OUT_SNAPSHOT_CSV = "bungalows_snapshot.csv"
OUT_NEW_CSV = "bungalows_new.csv"
OUT_MAP_HTML = "bungalows_map.html"

# Caches
GEO_CACHE_JSON = "geo_cache.json"        # address/url -> {lat,lon}
REV_CACHE_JSON = "reverse_cache.json"    # "lat,lon" -> {municipality}

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

    # je kiest bewust voor verify=False
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
WOONPLEIN_DETAIL_RE = re.compile(r"^https?://woonpleinlimburg\.nl/koop/woonhuis/[^/]+/woonhuis-\d+-", re.IGNORECASE)


def is_allowed_detail_url(site_host: str, low_url: str) -> bool:
    if "funda.nl" in site_host:
        return FUNDA_DETAIL_RE.match(low_url) is not None
    if "woonpleinlimburg.nl" in site_host:
        return WOONPLEIN_DETAIL_RE.match(low_url) is not None
    return False


def is_bungalow_detail(url: str, site_host: str) -> bool:
    """Voor non-funda bronnen: best-effort bungalow check."""
    referer = "https://www.funda.nl/" if "funda.nl" in site_host else "https://woonpleinlimburg.nl/"
    try:
        html = fetch(url, referer=referer)
    except Exception:
        return False

    t = html.lower()
    if "je bent bijna op de pagina" in t or "captcha" in t or "cloudflare" in t:
        return False

    # Funda is al gefilterd via zoek-URL: daar gebruiken we dit niet.
    return "bungalow" in t


# --------------- SCRAPE SEARCH PAGE ---------------

def scrape_search_page(search_url: str, html: str) -> list[Row]:
    soup = BeautifulSoup(html, "lxml")
    now = datetime.utcnow().isoformat(timespec="seconds")
    site_host = urlparse(search_url).netloc.lower()

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
        for sel in [".location", ".address", ".plaats", ".place", "[class*=location]", "[data-test*=location]"]:
            el = card.select_one(sel) if card else None
            if el:
                location_text = norm(el.get_text(" ", strip=True))
                break
        if not location_text:
            mloc = re.search(r"\b\d{4}\s*[A-Z]{2}\b[^|•\n]{0,80}", card_text)
            if mloc:
                location_text = norm(mloc.group(0))

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


# --------------- GEO: address parsing, geocode, reverse, distance ---------------

POSTCODE_RE = re.compile(r"\b(\d{4})\s*([A-Z]{2})\b", re.IGNORECASE)

def extract_postcode(text: str) -> str:
    m = POSTCODE_RE.search(text or "")
    if not m:
        return ""
    return f"{m.group(1)} {m.group(2).upper()}"

def extract_place_from_location_text(location_text: str) -> str:
    """
    Pakt 'Helmond' uit '5704 DC Helmond € 450.000 ...'
    """
    if not location_text:
        return ""
    m = re.search(r"\b\d{4}\s*[A-Z]{2}\s+([A-Za-zÀ-ÿ'`\- ]{2,})", location_text)
    if not m:
        return ""
    # stop bij euro/oppervlakte/makelaar-ruis
    place = m.group(1)
    place = re.split(r"\s+€|\s+\d+\s*m²|\s+k\.k\.|\s+v\.o\.n\.|\s+[A-Z]\s+", place)[0]
    return norm(place)

def extract_place_from_funda_url(url: str) -> str:
    # https://www.funda.nl/detail/koop/helmond/huis-...  -> helmond
    m = re.search(r"funda\.nl/detail/koop/([^/]+)/", url.lower())
    if not m:
        return ""
    return m.group(1).replace("-", " ")

def extract_street_from_title(title: str) -> str:
    # "Rendierlaan 6 5704 DC Helmond" -> "Rendierlaan 6"
    pc = extract_postcode(title)
    if not pc:
        return ""
    before = title.split(pc)[0]
    return norm(before)


def extract_postcode(text: str) -> str:
    m = POSTCODE_RE.search(text or "")
    return f"{m.group(1)} {m.group(2)}" if m else ""


def guess_address_variants(row: Row) -> list[str]:
    variants: list[str] = []

    pc = extract_postcode(row.location_text) or extract_postcode(row.title)
    place = extract_place_from_location_text(row.location_text) or extract_place_from_funda_url(row.url)

    street = extract_street_from_title(row.title)

    # 1) beste: straat + postcode + plaats
    if street and pc and place:
        variants.append(f"{street}, {pc} {place}, Nederland")

    # 2) heel sterk: postcode + plaats (werkt bijna altijd goed)
    if pc and place:
        variants.append(f"{pc} {place}, Nederland")

    # 3) fallback: volledige title
    if row.title:
        variants.append(f"{row.title}, Nederland")

    # 4) fallback: plaats
    if place:
        variants.append(f"{place}, Nederland")

    # 5) laatste redmiddel (liever niet, maar ok)
    variants.append("Limburg, Nederland")

    # uniek + netjes
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
    """
    - geocode (lat/lon) met caching
    - reverse geocode gemeente met caching
    - distance filter (<= RADIUS_KM)
    """
    geolocator = Nominatim(user_agent="bungalow_mapper/1.3")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=SLEEP_SEC_BETWEEN_GEOCODE)
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=SLEEP_SEC_BETWEEN_GEOCODE)

    geo_cache = load_json_cache(GEO_CACHE_JSON)   # key -> {"lat":..,"lon":..}
    rev_cache = load_json_cache(REV_CACHE_JSON)   # "lat,lon" -> {"municipality":..}

    center_latlon = get_center_latlon(geocode)

    enriched: list[Row] = []
    for r in rows:
        # 1) lat/lon ophalen of geocoden
        latlon = None

        # eerst cache op URL, dan op address strings
        if r.url in geo_cache:
            latlon = (geo_cache[r.url]["lat"], geo_cache[r.url]["lon"])
        else:
            for addr in guess_address_variants(r):
                if addr in geo_cache:
                    latlon = (geo_cache[addr]["lat"], geo_cache[addr]["lon"])
                    break

        if not latlon:
            # geocode varianten proberen
            found = None
            for addr in guess_address_variants(r):
                loc = geocode(addr)
                if loc:
                    found = (loc.latitude, loc.longitude, addr)
                    break

            if not found:
                print("[GEO FAIL]", r.source, "|", r.title, "|", r.url)
                continue

            lat, lon, used_addr = found
            latlon = (lat, lon)
            # cache zowel op url als op gebruikte addr
            geo_cache[r.url] = {"lat": lat, "lon": lon}
            geo_cache[used_addr] = {"lat": lat, "lon": lon}
            save_json_cache(GEO_CACHE_JSON, geo_cache)

        lat, lon = latlon

        # 2) afstand filter
        dist_km = float(geodesic(center_latlon, (lat, lon)).km)
        if dist_km > RADIUS_KM:
            continue

        # 3) gemeente via reverse (met caching)
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
                # gemeente kan in NL als municipality / city / town / village binnenkomen
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


# --------------- MAP ---------------

def write_map(rows: list[Row]) -> None:
    rows = [r for r in rows if r.lat is not None and r.lon is not None]
    if not rows:
        print("[WARN] Geen punten om te plotten.")
        return

    avg_lat = sum(r.lat for r in rows if r.lat is not None) / len(rows)
    avg_lon = sum(r.lon for r in rows if r.lon is not None) / len(rows)

    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=9)
    cluster = MarkerCluster(name="Bungalows").add_to(m)

    for r in rows:
        mun = r.municipality or "Onbekende gemeente"
        dist = f"{r.distance_km:.1f} km" if isinstance(r.distance_km, (int, float)) else ""
        popup_html = (
            f"<b>{r.title}</b><br>"
            f"{r.price_text}<br>"
            f"<i>{mun}</i> — {dist}<br>"
            f"<a href='{r.url}' target='_blank'>link</a>"
        )
        folium.Marker(
            [r.lat, r.lon],
            popup=folium.Popup(popup_html, max_width=350),
            tooltip=f"{mun} | {r.price_text}".strip(" |")
        ).add_to(cluster)

    folium.LayerControl().add_to(m)
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
            referer = "https://www.funda.nl/" if "funda.nl" in search_url else "https://woonpleinlimburg.nl/"
            html = fetch(search_url, referer=referer)
            rows = scrape_search_page(search_url, html)
            print(f"[INFO] {urlparse(search_url).netloc}: {len(rows)} detail links")
        except Exception as e:
            print(f"[ERROR] {search_url} -> {e}")
            rows = []

        all_rows.extend(rows)

        if i < len(SITES) - 1:
            time.sleep(SLEEP_SEC_BETWEEN_SITES)

    # Dedup binnen deze run
    uniq: dict[str, Row] = {}
    for r in all_rows:
        uniq.setdefault(r.url, r)
    detail_rows = list(uniq.values())

    # Filter: funda = alles meenemen (zoekURL is al bungalow),
    # anderen = bungalow check
    filtered_rows: list[Row] = []
    for r in detail_rows:
        if "funda.nl" in r.source:
            filtered_rows.append(r)
            continue

        if is_bungalow_detail(r.url, r.source):
            filtered_rows.append(r)

        time.sleep(SLEEP_SEC_BETWEEN_DETAIL_PAGES)

    # Geocode + gemeente + afstand filter (<= 100 km)
    enriched_rows = geocode_and_enrich_rows(filtered_rows)

    # Snapshot CSV + New CSV (beiden na distance-filter)
    write_csv(OUT_SNAPSHOT_CSV, enriched_rows)
    new_rows = [r for r in enriched_rows if r.url not in prev_urls]
    write_csv(OUT_NEW_CSV, new_rows)

    print(f"\nSnapshot (<= {RADIUS_KM:.0f} km): {len(enriched_rows)}")
    print(f"Nieuw sinds vorige run: {len(new_rows)}")
    print(f"- Snapshot: {OUT_SNAPSHOT_CSV}")
    print(f"- New:      {OUT_NEW_CSV}")

    # Kaart = alle actuele hits binnen radius
    write_map(enriched_rows)


if __name__ == "__main__":

    run()
