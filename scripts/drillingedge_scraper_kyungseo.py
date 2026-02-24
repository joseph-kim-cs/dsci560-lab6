import os
import re
import time
import json
from typing import Optional, Dict, Tuple, List

import pymysql
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv(".env")

DB_HOST = os.getenv("MYSQL_HOST")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_USER = os.getenv("MYSQL_USER")
DB_PASS = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DB")

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
)

SEARCH_URL = "https://www.drillingedge.com/search"
WAIT = float(os.getenv("SCRAPER_SLEEP_SECS", "0.8"))
TIMEOUT = int(os.getenv("SCRAPER_TIMEOUT", "25"))

MAX_ROWS_ENV = os.getenv("SCRAPER_MAX_ROWS")
ONLY_ID_ENV = os.getenv("SCRAPER_ONLY_ID")
VERBOSE = os.getenv("SCRAPER_VERBOSE", "1") == "1"


def log(msg: str) -> None:
    if VERBOSE:
        print(msg)


def get_connection():
    if not DB_HOST or not DB_USER or not DB_PASS or not DB_NAME:
        raise RuntimeError("Missing DB env vars in .env (MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD)")
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def clean_name(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    x = x.strip()
    x = re.sub(r"\s+", " ", x)
    return x or None


def build_params(well_name: Optional[str], api10: Optional[str]) -> Dict[str, str]:
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


def http_get(session: requests.Session, url: str, params: Optional[Dict[str, str]] = None) -> str:
    r = session.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    if WAIT > 0:
        time.sleep(WAIT)
    return r.text


def pick_url_from_search(html: str, api10: Optional[str], well_name: Optional[str]) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = soup.find_all("a", href=True)

    api_digits = api10 or ""
    name = (well_name or "").lower()

    best_score = -1
    best_url = None

    for a in links:
        href = a["href"]
        if "/wells/" not in href and "/well" not in href:
            continue

        text = a.get_text(" ", strip=True) or ""
        blob = f"{href} {text}"
        blob_digits = re.sub(r"\D", "", blob)

        score = 0
        if api_digits and api_digits in blob_digits:
            score += 10

        if (not api_digits) and name:
            tokens = [t for t in re.split(r"\s+", name) if len(t) >= 3]
            overlap = sum(1 for t in tokens if t in blob.lower())
            score += overlap

        if score > best_score and score > 0:
            best_score = score
            best_url = href if href.startswith("http") else f"https://www.drillingedge.com{href}"

    return best_url


def parse_lat_lon(html: str) -> Tuple[Optional[float], Optional[float]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    minus = r"[-\u2212\u2013\u2014]?"

    def to_float(s: str) -> float:
        return float(s.replace("\u2212", "-").replace("\u2013", "-").replace("\u2014", "-"))

    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.get_text(strip=True) or ""
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        stack = [data]
        while stack:
            obj = stack.pop()
            if isinstance(obj, dict):
                geo = obj.get("geo")
                if isinstance(geo, dict):
                    lat = geo.get("latitude")
                    lon = geo.get("longitude")
                    try:
                        if lat is not None and lon is not None:
                            return float(lat), float(lon)
                    except Exception:
                        pass
                stack.extend(obj.values())
            elif isinstance(obj, list):
                stack.extend(obj)

    def try_patterns(blob: str) -> Optional[Tuple[float, float]]:
        m0 = re.search(
            rf"Latitude\s*/\s*Longitude\s*({minus}\d{{1,3}}\.\d+)\s*,\s*({minus}\d{{1,3}}\.\d+)",
            blob,
            re.IGNORECASE,
        )
        if m0:
            return to_float(m0.group(1)), to_float(m0.group(2))

        m1 = re.search(
            rf"Latitude\D+({minus}\d{{1,3}}\.\d+)\D+Longitude\D+({minus}\d{{1,3}}\.\d+)",
            blob,
            re.IGNORECASE,
        )
        if m1:
            return to_float(m1.group(1)), to_float(m1.group(2))

        m2 = re.search(
            rf"maps\?q=({minus}\d{{1,3}}\.\d+)\s*,\s*({minus}\d{{1,3}}\.\d+)",
            blob,
            re.IGNORECASE,
        )
        if m2:
            return to_float(m2.group(1)), to_float(m2.group(2))

        return None

    out = try_patterns(text)
    if out:
        return out[0], out[1]

    out = try_patterns(html)
    if out:
        return out[0], out[1]

    return None, None


def fetch_rows(conn) -> List[dict]:
    max_rows = int(MAX_ROWS_ENV) if MAX_ROWS_ENV else None

    sql = """
        SELECT id, well_name_and_number, api_number_10, drillingedge_url
        FROM wells
        WHERE (latitude IS NULL OR longitude IS NULL)
          AND (api_number_10 IS NOT NULL OR well_name_and_number IS NOT NULL OR drillingedge_url IS NOT NULL)
    """
    params: List[object] = []

    if ONLY_ID_ENV:
        sql += " AND id = %s"
        params.append(int(ONLY_ID_ENV))

    sql += """
        ORDER BY
          (drillingedge_url IS NULL) ASC,
          (api_number_10 IS NULL) ASC,
          (well_name_and_number IS NULL) ASC,
          id ASC
    """

    if max_rows is not None:
        sql += " LIMIT %s"
        params.append(max_rows)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def update_row(conn, well_id: int, url: Optional[str], lat: Optional[float], lon: Optional[float]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE wells
            SET drillingedge_url = COALESCE(%s, drillingedge_url),
                latitude = %s,
                longitude = %s
            WHERE id = %s
            """,
            (url, lat, lon, well_id),
        )


def main() -> None:
    conn = get_connection()
    session = make_session()

    rows = fetch_rows(conn)
    print(f"Rows to enrich: {len(rows)}")

    updated = 0
    skipped = 0
    errors = 0

    for idx, row in enumerate(rows, start=1):
        well_id = row["id"]
        api10 = row.get("api_number_10")
        well_name = clean_name(row.get("well_name_and_number"))
        existing_url = row.get("drillingedge_url")

        try:
            log(f"[{idx}/{len(rows)}] id={well_id} api10={api10} well={well_name}")

            if existing_url:
                well_url = existing_url
            else:
                params = build_params(well_name, api10)
                search_html = http_get(session, SEARCH_URL, params=params)
                well_url = pick_url_from_search(search_html, api10, well_name)

            if not well_url:
                print(f"[{idx}/{len(rows)}] no match (id={well_id})")
                skipped += 1
                continue

            well_html = http_get(session, well_url)
            lat, lon = parse_lat_lon(well_html)

            if lat is None or lon is None:
                update_row(conn, well_id, well_url, None, None)
                print(f"[{idx}/{len(rows)}] skipped (no coords) id={well_id}")
                skipped += 1
                continue

            update_row(conn, well_id, well_url, lat, lon)
            updated += 1
            log(f"[{idx}/{len(rows)}] updated id={well_id} lat={lat} lon={lon}")

        except Exception as e:
            errors += 1
            print(f"[{idx}/{len(rows)}] error (id={well_id}): {type(e).__name__}: {e}")
            continue

    conn.close()
    print(f"Done. Updated={updated}, Skipped={skipped}, Errors={errors}")


if __name__ == "__main__":
    main()
