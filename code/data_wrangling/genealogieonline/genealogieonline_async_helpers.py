# =============================================================================
# genealogieonline_async_helpers.py  [HELPER — GENEALOGIEONLINE PIPELINE]
# Shared async infrastructure + response parsing for genealogieonline.nl.
# Used by: genealogieonline_step1_query_candidates.py
#
# API notes (verified live 2026-07-08): the documented name-search endpoint
# `/zoeken/index.php?q=<surname>&vn=<firstname>&gv=<yr>&gt=<yr>` is real and
# combinable with a birth-year window, BUT `vn=` needs a full first name --
# a bare initial ("vn=J") returns zero hits. Since candidates_panel mostly
# carries only initials, search leaves `vn=` empty (surname + year window
# only) and matches/scores initials client-side against each hit's full
# name, parsed from the search-result snippet "Name (YYYY-YYYY) >> tree".
# Pagination is 15 results/page; stop when a page returns no I<n>.php links.
# =============================================================================
import os
import re
from urllib.parse import urlencode

import aiohttp
from bs4 import BeautifulSoup

BASE_URL = "https://www.genealogieonline.nl"
SEARCH_URL = f"{BASE_URL}/zoeken/index.php"
USER_AGENT = "selection-politics-research/0.1 (academic; a.h.machielsen@uu.nl)"

PAGE_SIZE = 15

_PERSON_HREF_RE = re.compile(r"I\d+\.php$")
_SNIPPET_RE = re.compile(
    r"^(.*?)\s*\((\d{4}|\?{4})\s*[-–]\s*(?:(\d{4}|\?{4}))?\)\s*(?:»\s*(.*))?$")


def make_session(connector_limit: int = 8) -> aiohttp.ClientSession:
    connector = aiohttp.TCPConnector(limit=connector_limit, ssl=False)
    headers = {"User-Agent": USER_AGENT}
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    return aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout)


def search_url(surname: str, year_lo: int, year_hi: int, start: int = 0) -> str:
    params = {"q": surname, "vn": "", "gv": int(year_lo), "gt": int(year_hi),
              "start": start}
    return f"{SEARCH_URL}?{urlencode(params)}"


def parse_search_results(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    seen_urls = set()
    for a in soup.find_all("a", href=_PERSON_HREF_RE):
        url = a["href"]
        if not url.startswith("http"):
            url = BASE_URL + url
        if url in seen_urls:
            continue
        seen_urls.add(url)
        snippet = a.find_parent().get_text(strip=True)
        m = _SNIPPET_RE.match(snippet)
        if m:
            name, birth_raw, death_raw, source_tree = m.groups()
        else:
            name, birth_raw, death_raw, source_tree = snippet, None, None, None
        birth_year = int(birth_raw) if birth_raw and birth_raw.isdigit() else None
        death_year = int(death_raw) if death_raw and death_raw.isdigit() else None
        rows.append({
            "url": url, "person_name": name, "birth_year": birth_year,
            "death_year": death_year, "source_tree": source_tree,
        })
    return rows


DDL = """
CREATE TABLE IF NOT EXISTS query_progress (
    era        VARCHAR,
    key        VARCHAR,
    n_hits     INTEGER,
    queried_at TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (era, key)
);

CREATE TABLE IF NOT EXISTS hits (
    era         VARCHAR,
    key         VARCHAR,
    url         VARCHAR,
    person_name VARCHAR,
    birth_year  INTEGER,
    death_year  INTEGER,
    source_tree VARCHAR
);
"""


def init_db(con) -> None:
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
