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
#
# Person-page markup (verified live 2026-07-09, unchanged from the
# `examples/genealogie/ind_step04_scrape_genealogie.py` template this reuses):
# occupation lives in a `<ul class="nicelist"><li>Beroep: ...</li>` entry
# (present only for persons whose tree author recorded one -- absence is
# normal, not a parse failure); birth place is
# `<span itemprop="birthPlace">` -> nested `<meta itemprop="addressLocality">`;
# the male parent is a `<div itemprop="parent">` with a `gender` meta of
# "male" (a person may have 0-2 `parent` divs; only the male one is the
# lineage spine, matching ind_step06's father-only-spine rationale).
# =============================================================================
import os
import re
from urllib.parse import urljoin, urlencode

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


_BEROEP_RE = re.compile(r"[Bb]eroep\s*:")


def parse_beroep(soup: BeautifulSoup) -> str | None:
    for ul in soup.find_all("ul", class_="nicelist"):
        for li in ul.find_all("li"):
            text = li.get_text(" ", strip=True)
            if _BEROEP_RE.match(text):
                return _BEROEP_RE.sub("", text, count=1).strip().rstrip(".")
    return None


def parse_birth_place(soup: BeautifulSoup) -> str | None:
    bp_span = soup.find("span", attrs={"itemprop": "birthPlace"})
    if bp_span:
        loc = bp_span.find("meta", attrs={"itemprop": "addressLocality"})
        if loc:
            return loc.get("content")
    return None


def parse_father(soup: BeautifulSoup, base_url: str) -> tuple[str | None, str | None]:
    """Return (father_url, father_name) for the first male parent with a page."""
    for pdiv in soup.find_all(attrs={"itemprop": "parent"}):
        gm = pdiv.find("meta", attrs={"itemprop": "gender"})
        if not gm or gm.get("content") != "male":
            continue
        father_url = father_name = None
        um = pdiv.find("meta", attrs={"itemprop": "url"})
        if um:
            father_url = um.get("content")
            if father_url and not father_url.startswith("http"):
                father_url = urljoin(base_url, father_url)
        nm = pdiv.find("meta", attrs={"itemprop": "name"})
        if nm:
            father_name = nm.get("content")
        return father_url, father_name
    return None, None


def parse_person_page(html: str, url: str) -> dict:
    """Parse a genealogieonline person page: full name, beroep, birth place,
    father link. Returns {} for a 'gone' page (caller checks status first)."""
    soup = BeautifulSoup(html, "lxml")
    person_name_full = None
    nm = soup.find("meta", attrs={"itemprop": "name", "content": True})
    if nm:
        person_name_full = nm.get("content")
    father_url, father_name = parse_father(soup, url)
    return {
        "person_name_full": person_name_full,
        "beroep": parse_beroep(soup),
        "birth_place": parse_birth_place(soup),
        "father_url": father_url,
        "father_name": father_name,
    }


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

CREATE TABLE IF NOT EXISTS person_pages (
    url              VARCHAR PRIMARY KEY,
    person_name_full VARCHAR,
    beroep           VARCHAR,
    birth_place      VARCHAR,
    father_url       VARCHAR,
    father_name      VARCHAR,
    skip             BOOLEAN DEFAULT FALSE,   -- 404/403/unparseable
    fetched_at       TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS candidate_ancestors (
    era   VARCHAR,
    key   VARCHAR,
    url   VARCHAR,
    depth INTEGER,     -- 0 = candidate's own matched person, 1 = father, ...
    PRIMARY KEY (era, key, url)
);
"""


def init_db(con) -> None:
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
