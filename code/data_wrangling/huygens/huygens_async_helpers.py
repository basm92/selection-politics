# =============================================================================
# huygens_async_helpers.py  [HELPER — HUYGENS VERKIEZINGEN PIPELINE]
# Shared async infrastructure for scraping the Huygens "Verkiezingen Tweede
# Kamer 1848-1918" databank (resources.huygens.knaw.nl/verkiezingentweedekamer).
# Used by: huygens_step1_list_elections.py, huygens_step2_fetch_uitslagen.py
#
# Key exports:
#   TokenBucketRateLimiter   — polite req/sec limit against the Huygens server
#   make_session()           — aiohttp.ClientSession with correct headers
#   parse_listing_page(html) — rows from a databank/chronologisch listing page
#   parse_uitslag_page(html, uitslag_id) — (election dict, candidate rows)
#   init_db(con)             — create all DuckDB tables
# =============================================================================
import asyncio
import re
import time

import aiohttp
from bs4 import BeautifulSoup

BASE_URL = "https://resources.huygens.knaw.nl/verkiezingentweedekamer/databank"
USER_AGENT = "selection-politics-research/0.1 (academic; a.h.machielsen@uu.nl)"


# ---------------------------------------------------------------------------
# Rate limiter (same token-bucket design as examples/openarch)
# ---------------------------------------------------------------------------

class TokenBucketRateLimiter:
    """
    Async token-bucket rate limiter.

    Allows up to `rate` requests per second. Each call to `acquire()` consumes
    one token; if the bucket is empty it sleeps until a token is available.
    """

    def __init__(self, rate: float = 3.0):
        self.rate = rate
        self.tokens = rate
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self._last = now
            if self.tokens < 1.0:
                wait = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(wait)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0


def make_session(connector_limit: int = 6) -> aiohttp.ClientSession:
    """Return a configured aiohttp ClientSession."""
    connector = aiohttp.TCPConnector(limit=connector_limit)
    headers = {"User-Agent": USER_AGENT}
    timeout = aiohttp.ClientTimeout(total=60, connect=15)
    return aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _num(s: str | None) -> int | None:
    """'6,360' -> 6360; returns None for blank/non-numeric cells."""
    if s is None:
        return None
    s = s.strip().replace(",", "").replace(".", "")
    return int(s) if s.isdigit() else None


def _clean(s: str | None) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def parse_listing_page(html: str) -> tuple[int | None, list[dict]]:
    """
    Parse one databank/chronologisch listing page.

    Returns (total_count, rows) where total_count comes from the
    "Aantal verkiezingen N" header (None if absent) and each row has:
    uitslag_id, district, district_id, date_raw, type, electoraat, opkomst,
    stembriefjes, geldig, blanco.
    """
    m = re.search(r"Aantal verkiezingen\s+([\d,\.]+)", html)
    total = _num(m.group(1)) if m else None

    soup = BeautifulSoup(html, "lxml")
    rows = []
    for tr in soup.select("table.vertical tr"):
        tds = tr.find_all("td")
        if len(tds) < 8:
            continue
        uitslag_a = tds[1].find("a", href=re.compile(r"uitslag_ID=(\d+)"))
        if uitslag_a is None:
            continue  # header row
        uitslag_id = int(re.search(r"uitslag_ID=(\d+)", uitslag_a["href"]).group(1))
        district_a = tds[0].find("a", href=re.compile(r"District_ID=(\d+)"))
        district_id = (
            int(re.search(r"District_ID=(\d+)", district_a["href"]).group(1))
            if district_a else None
        )
        rows.append({
            "uitslag_id": uitslag_id,
            "district": _clean(tds[0].get_text()),
            "district_id": district_id,
            "date_raw": _clean(tds[1].get_text()),   # d/m/yyyy
            "type": _clean(tds[2].get_text()),
            "electoraat": _num(tds[3].get_text()),
            "opkomst": _num(tds[4].get_text()),
            "stembriefjes": _num(tds[5].get_text()),
            "geldig": _num(tds[6].get_text()),
            "blanco": _num(tds[7].get_text()),
        })
    return total, rows


# Header fields on an uitslag_per_verkiezing page -> column names.
_UITSLAG_FIELDS = {
    "District": ("district", _clean),
    "Verkiezingdatum": ("date_raw", _clean),
    "Type": ("type", _clean),
    "Omvang electoraat": ("electoraat", _num),
    "Opkomst": ("opkomst", _num),
    "Aantal stembriefjes": ("stembriefjes", _num),
    "Aantal stemmen geldig": ("geldig", _num),
    "Aantal stemmen blanco": ("blanco", _num),
    "Aantal zetels": ("zetels", _num),
    "Kiesdrempel": ("kiesdrempel", _num),
}


def parse_uitslag_page(html: str, uitslag_id: int) -> tuple[dict, list[dict]]:
    """
    Parse one uitslag_per_verkiezing page.

    Returns (election, candidates):
      election:   uitslag_id, district, district_id, date_raw, type,
                  electoraat, opkomst, stembriefjes, geldig, blanco,
                  zetels, kiesdrempel
      candidates: uitslag_id, rank, name_raw, persoon_id, affiliation,
                  votes, pct
    """
    soup = BeautifulSoup(html, "lxml")
    election: dict = {"uitslag_id": uitslag_id, "district_id": None}
    for k in ("district", "date_raw", "type", "electoraat", "opkomst",
              "stembriefjes", "geldig", "blanco", "zetels", "kiesdrempel"):
        election.setdefault(k, None)

    dist_a = soup.find("a", href=re.compile(r"verkiezingen_per_district\?District_ID=(\d+)"))
    if dist_a:
        election["district_id"] = int(
            re.search(r"District_ID=(\d+)", dist_a["href"]).group(1))

    candidates: list[dict] = []
    for table in soup.find_all("table"):
        header_text = _clean(table.get_text()) or ""
        # Header/stats table: two-column "label : value" rows
        if "Verkiezingdatum" in header_text and not election.get("date_raw"):
            for tr in table.find_all("tr"):
                # label sits in a <th>, the ":" and value in <td>s
                tds = [_clean(c.get_text()) for c in tr.find_all(["th", "td"])]
                if len(tds) >= 3 and tds[0]:
                    label = tds[0].rstrip(" :")
                    if label in _UITSLAG_FIELDS:
                        col, fn = _UITSLAG_FIELDS[label]
                        election[col] = fn(tds[2])
        # Candidate table: the "Uitslagen per deelnemer" banner sits in its own
        # single-row table; the actual candidate rows follow in an unlabeled
        # table whose header row is "# | Naam | Aanbevolen door | ...".
        first_tr = table.find("tr")
        first_cells = ([_clean(td.get_text()) for td in first_tr.find_all("td")]
                       if first_tr else [])
        if first_cells[:2] == ["#", "Naam"]:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 5:
                    continue
                rank_txt = _clean(tds[0].get_text())
                if not rank_txt or not re.match(r"^\d+\.?$", rank_txt):
                    continue
                name_td = tds[1]
                pers_a = name_td.find("a", href=re.compile(r"persoon_ID=(\d+)"))
                persoon_id = (
                    int(re.search(r"persoon_ID=(\d+)", pers_a["href"]).group(1))
                    if pers_a else None
                )
                # Affiliation cell carries stray template comments; strip them.
                aff = _clean(re.sub(r"Of dtml-sql toevoegen-->", "", tds[2].get_text()))
                pct_txt = _clean(tds[4].get_text())
                pct = None
                if pct_txt:
                    pm = re.search(r"([\d\.]+)\s*%", pct_txt)
                    pct = float(pm.group(1)) if pm else None
                candidates.append({
                    "uitslag_id": uitslag_id,
                    "rank": int(rank_txt.rstrip(".")),
                    "name_raw": _clean(name_td.get_text()),
                    "persoon_id": persoon_id,
                    "affiliation": aff,
                    "votes": _num(tds[3].get_text()),
                    "pct": pct,
                })
    return election, candidates


# ---------------------------------------------------------------------------
# DuckDB schema
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS list_progress (
    year INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS uitslag_index (
    uitslag_id   INTEGER PRIMARY KEY,
    district     TEXT,
    district_id  INTEGER,
    date_raw     TEXT,
    type         TEXT,
    electoraat   INTEGER,
    opkomst      INTEGER,
    stembriefjes INTEGER,
    geldig       INTEGER,
    blanco       INTEGER,
    list_year    INTEGER
);

CREATE TABLE IF NOT EXISTS fetched_uitslagen (
    uitslag_id INTEGER PRIMARY KEY,
    fetched_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS elections (
    uitslag_id   INTEGER PRIMARY KEY,
    district     TEXT,
    district_id  INTEGER,
    date_raw     TEXT,
    type         TEXT,
    electoraat   INTEGER,
    opkomst      INTEGER,
    stembriefjes INTEGER,
    geldig       INTEGER,
    blanco       INTEGER,
    zetels       INTEGER,
    kiesdrempel  INTEGER
);

CREATE TABLE IF NOT EXISTS candidates_raw (
    uitslag_id  INTEGER NOT NULL,
    rank        INTEGER,
    name_raw    TEXT,
    persoon_id  INTEGER,
    affiliation TEXT,
    votes       INTEGER,
    pct         DOUBLE
);
"""


def init_db(con) -> None:
    """Create all required tables in the DuckDB connection."""
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
