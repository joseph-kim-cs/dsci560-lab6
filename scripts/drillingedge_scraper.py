import os
import re
import time
from typing import Optional, Dict, Any, Tuple

import pymysql
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("MYSQL_HOST")
DB_PORT = int(os.getenv("MYSQL_PORT")) 
DB_USER = os.getenv("MYSQL_USER")
DB_PASS = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DB")

def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT, 
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

# using a beautifulsoup4 scraper
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

SLEEP_SECS = 1.0
TIMEOUT = 20

BASE_SEARCH_URL = "https://www.drillingedge.com/search"


def clean_well_name(well_name_and_number: Optional[str]) -> Optional[str]:
    # helper to clean up the well name - remove extra whitespace, etc.
    if not well_name_and_number:
        return None
    s = well_name_and_number.strip()
    # collapse whitespace
    s = re.sub(r"\s+", " ", s)
    return s if s else None


def http_get(session: requests.Session, url: str, params: Dict[str, str]) -> str:
    resp = session.get(
        url,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    time.sleep(SLEEP_SECS)
    return resp.text


# build search params for bs4 using the search link fields
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
    # First try the api10 match, then fall back to well name token overlap if no API provided or found.
    # else just skip
    soup = BeautifulSoup(html, "lxml")
    links = soup.find_all("a", href=True)

    candidates: List[tuple[int, str]] = []
    api_digits = api10 or ""
    wn_lower = (well_name or "").lower()

    for a in links:
        href = a["href"]
        text = a.get_text(" ", strip=True) or ""
        blob = f"{href} {text}"
        blob_digits = re.sub(r"\D", "", blob)

        if "/well" not in href:
            continue

        score = 0
        if api_digits and api_digits in blob_digits:
            score += 10

        # if no API, try well name match
        if (not api_digits) and wn_lower:
            # loose token overlap
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