import os
import re
import time
from typing import Optional, Dict, Any

import pymysql
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("MYSQL_HOST")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_USER = os.getenv("MYSQL_USER")
DB_PASS = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DB")

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
)

BASE_SEARCH_URL = "https://www.drillingedge.com/search"
SLEEP_SECS = 1.0
TIMEOUT = 20


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def http_get(session: requests.Session, url: str, params: Dict[str, str]) -> str:
    resp = session.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    resp.raise_for_status()
    time.sleep(SLEEP_SECS)
    return resp.text


def build_search_params(well_name: Optional[str], api10: Optional[str]) -> Dict[str, str]:
    return {
        "type": "wells",
        "operator_name": "",
        "well_name": well_name or "",
        "api_no": api10 or "",
        "lease_key": "",
        "state": "",
        "county": "",
        "section": "",
        "township": "",
        "range": "",
        "min_boe": "",
        "max_boe": "",
        "min_depth": "",
        "max_depth": "",
        "field_formation": "",
    }


def pick_best_well_url_from_search(html: str, api10: Optional[str], well_name: Optional[str]) -> Optional[str]:
    """
    Picks a likely /well/... link from the search results page.
    Strategy:
      - If api10 exists, prefer link where digits contain api10
      - Else, prefer link with token overlap on well name
    """
    soup = BeautifulSoup(html, "lxml")
    links = soup.find_all("a", href=True)

    candidates = []
    api_digits = api10 or ""
    wn_lower = (well_name or "").lower().strip()

    for a in links:
        href = a["href"]
        text = a.get_text(" ", strip=True) or ""
        blob = f"{href} {text}"
        blob_digits = re.sub(r"\D", "", blob)

        if href.rstrip("/") == "/wells":
            continue
        if "/wells/" not in href:
            continue

        score = 0
        if api_digits and api_digits in blob_digits:
            score += 10

        if (not api_digits) and wn_lower:
            tokens = [t for t in re.split(r"\s+", wn_lower) if len(t) >= 3]
            overlap = sum(1 for t in tokens if t in blob.lower())
            score += overlap

        if score > 0:
            full = href if href.startswith("http") else f"https://www.drillingedge.com{href}"
            candidates.append((score, full))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def extract_lat_lon_from_well_page(html: str) -> tuple[Optional[float], Optional[float]]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)

    # Pattern 1: "Latitude / Longitude 48.097836, -103.645192"
    m = re.search(
        r"Latitude\s*/\s*Longitude\s*([\-]?\d+(?:\.\d+)?)\s*,\s*([\-]?\d+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except Exception:
            pass

    # Pattern 2: separate labeled Latitude and Longitude
    m1 = re.search(r"\bLatitude\b\s*[:\s]\s*([\-]?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    m2 = re.search(r"\bLongitude\b\s*[:\s]\s*([\-]?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    if m1 and m2:
        try:
            return float(m1.group(1)), float(m2.group(1))
        except Exception:
            pass

    # Pattern 3: raw JS style lat/lng in HTML
    m3 = re.search(r"\b(lat|latitude)\b\W{0,15}([\-]?\d+(?:\.\d+)?)", html, flags=re.IGNORECASE)
    m4 = re.search(r"\b(lng|lon|longitude)\b\W{0,15}([\-]?\d+(?:\.\d+)?)", html, flags=re.IGNORECASE)
    if m3 and m4:
        try:
            return float(m3.group(2)), float(m4.group(2))
        except Exception:
            pass

    return None, None
    """
    Tries a couple of robust patterns.
    DrillingEdge pages often expose coordinates as 'Latitude'/'Longitude' labels or inside map metadata.
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)

    # Pattern 1: labeled Latitude / Longitude
    lat = None
    lon = None

    m1 = re.search(r"\bLatitude\b\s*[:\s]\s*(-?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    m2 = re.search(r"\bLongitude\b\s*[:\s]\s*(-?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    if m1 and m2:
        try:
            lat = float(m1.group(1))
            lon = float(m2.group(1))
            return lat, lon
        except Exception:
            pass

    # Pattern 2: look for something like "lat: XX.XXXX" and "lng: -YY.YYYY"
    m3 = re.search(r"\b(lat|latitude)\b\W{0,10}(-?\d+(?:\.\d+)?)", html, flags=re.IGNORECASE)
    m4 = re.search(r"\b(lng|lon|longitude)\b\W{0,10}(-?\d+(?:\.\d+)?)", html, flags=re.IGNORECASE)
    if m3 and m4:
        try:
            lat = float(m3.group(2))
            lon = float(m4.group(2))
            return lat, lon
        except Exception:
            pass

    return None, None


def fetch_candidates(conn, limit: int = 50):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, source_pdf, well_name_and_number, api_number_10, drillingedge_url, latitude, longitude
            FROM wells
            WHERE (latitude IS NULL OR longitude IS NULL)
            ORDER BY id
            LIMIT %s
            """,
            (limit,),
        )
        return cursor.fetchall()


def update_url(conn, well_id: int, url: str):
    with conn.cursor() as cursor:
        cursor.execute(
            "UPDATE wells SET drillingedge_url=%s WHERE id=%s",
            (url, well_id),
        )


def update_coords(conn, well_id: int, lat: float, lon: float):
    with conn.cursor() as cursor:
        cursor.execute(
            "UPDATE wells SET latitude=%s, longitude=%s WHERE id=%s",
            (lat, lon, well_id),
        )


def run(limit: int = 50):
    conn = get_connection()
    session = requests.Session()

    rows = fetch_candidates(conn, limit=limit)
    print(f"Found {len(rows)} wells missing coordinates (limit={limit}).")

    for i, row in enumerate(rows, start=1):
        well_id = row["id"]
        well_name = row.get("well_name_and_number")
        api10 = row.get("api_number_10")
        url = row.get("drillingedge_url")

        print(f"\n[{i}/{len(rows)}] id={well_id} pdf={row.get('source_pdf')} api10={api10} name={well_name}")

        try:
            # 1) Find well page URL if missing
            if not url:
                params = build_search_params(well_name, api10)
                search_html = http_get(session, BASE_SEARCH_URL, params=params)
                url = pick_best_well_url_from_search(search_html, api10, well_name)

                if not url:
                    print("  Could not find a /well page from search results. Skipping.")
                    continue

                update_url(conn, well_id, url)
                print(f"  Saved drillingedge_url: {url}")

            # 2) Fetch well page and parse coordinates
            well_html = http_get(session, url, params={})
            lat, lon = extract_lat_lon_from_well_page(well_html)

            if lat is None or lon is None:
                print("  Could not parse latitude/longitude from well page. Skipping.")
                continue

            update_coords(conn, well_id, lat, lon)
            print(f"  Updated coords: lat={lat}, lon={lon}")

        except requests.HTTPError as e:
            print(f"  HTTP error: {e}. Skipping.")
        except Exception as e:
            print(f"  Error: {e}. Skipping.")

    conn.close()
    print("\nEnrichment run complete.")


if __name__ == "__main__":
    run(limit=200)