# =============================================================================
# openarch_async_helpers.py  [HELPER — OPENARCHIEF ASYNC PIPELINE]
# Shared async infrastructure for querying the OpenArchieven API.
# Used by: openarch_step1_survey_availability.py, openarch_step2_download_marriages.py
#
# Key exports:
#   TokenBucketRateLimiter  — enforces 4 req/sec API rate limit
#   make_session()          — aiohttp.ClientSession with correct headers
#   clean_municipality_name(name) — URL-safe name for eventplace filter
#   make_search_url(...)    — BS Huwelijk search endpoint URL
#   make_show_url(...)      — record show endpoint URL
#   parse_show_response(data, identifier, meta) — extract structured rows
# =============================================================================
import asyncio
import json
import time
import aiohttp

BASE_URL = "https://api.openarchieven.nl/1.1/"
USER_AGENT = "borders-of-belief-research/1.0 (academic; bas.machielsen@uu.nl)"


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class TokenBucketRateLimiter:
    """
    Async token-bucket rate limiter.

    Allows up to `rate` requests per second. Each call to `acquire()` consumes
    one token; if the bucket is empty it sleeps until a token is available.
    """

    def __init__(self, rate: float = 4.0):
        self.rate = rate          # tokens per second
        self.tokens = rate        # start full
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


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

def make_session(connector_limit: int = 10) -> aiohttp.ClientSession:
    """Return a configured aiohttp ClientSession."""
    connector = aiohttp.TCPConnector(limit=connector_limit, ssl=False)
    headers = {"User-Agent": USER_AGENT}
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    return aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def clean_municipality_name(name: str) -> str:
    """
    Clean a municipality name for use as the `eventplace` URL parameter.
    Mirrors the cleaning logic in helper_query_openarch_api.py.
    """
    return (
        name
        .replace(" En ", " en ")
        .replace(r"\bIj", "IJ")
        .replace(" Van ", " van ")
        .replace("'S", "'s")
        .replace("'s ", "'s-")
        .replace(" ", "+")
    )


def make_search_url(place: str, from_date: str, until_date: str, start: int = 0) -> str:
    """
    Build a BS Huwelijk search URL.

    The OpenArchieven API does not support standalone ``from``/``until`` query
    parameters for date filtering.  The correct way to filter by a year range
    is to append it to the ``name`` parameter in the form ``* YYYY-YYYY``.

    Args:
        place:      URL-cleaned municipality name (use clean_municipality_name first)
        from_date:  ISO date string, e.g. "1800-01-01"
        until_date: ISO date string, e.g. "1809-12-31"
        start:      pagination offset (0-based)
    """
    year_from = from_date[:4]
    year_until = until_date[:4]
    return (
        f"{BASE_URL}records/search.json"
        f"?name=*+{year_from}-{year_until}"
        f"&sourcetype=BS+Huwelijk"
        f"&eventplace={place}"
        f"&start={start}&number_show=50"
        f"&sort=6&relationtype=Bruid"
    )


def make_show_url(archive_code: str, identifier: str) -> str:
    """Build a record show URL."""
    return f"{BASE_URL}records/show.json?archive={archive_code}&identifier={identifier}"


# ---------------------------------------------------------------------------
# Response parser
# ---------------------------------------------------------------------------

def _extract_name(person: dict) -> tuple[str | None, str | None, str | None]:
    """Return (first_name, prefix_last_name, last_name) from a Person dict."""
    pname = person.get("PersonName")
    if pname is None:
        return None, None, None
    if isinstance(pname, list):
        pname = next((p for p in pname if isinstance(p, dict)), None)
    if not isinstance(pname, dict):
        return None, None, None
    return (
        pname.get("PersonNameFirstName") or pname.get("FirstName"),
        pname.get("PersonNamePrefixLastName") or pname.get("PrefixLastName"),
        pname.get("PersonNameLastName") or pname.get("LastName"),
    )


def _extract_date(date_obj) -> tuple[int | None, int | None, int | None]:
    """Return (year, month, day) from a date dict or None."""
    if not isinstance(date_obj, dict):
        return None, None, None
    def _int(v):
        try:
            return int(v) if v else None
        except (ValueError, TypeError):
            return None
    return _int(date_obj.get("Year")), _int(date_obj.get("Month")), _int(date_obj.get("Day"))


def _extract_source_date(source: dict) -> str | None:
    """Extract a date string from SourceDate or SourceIndexDate."""
    if not isinstance(source, dict):
        return None
    sd = source.get("SourceDate") or source.get("SourceIndexDate") or {}
    if isinstance(sd, dict):
        return sd.get("LiteralDate") or sd.get("From")
    return None


def parse_show_response(data: dict, identifier: str, meta: dict) -> list[dict]:
    """
    Parse a `show` API response into a list of row dicts, one per RelationEP entry.

    Args:
        data:       Parsed JSON from the show endpoint
        identifier: The record UUID
        meta:       Dict with keys: archive_code, amco, eventdate_year,
                    eventdate_month, eventdate_day, eventplace_raw

    Returns:
        List of row dicts (may be empty if data is malformed).
    """
    if not isinstance(data, dict):
        return []

    # Build person lookup by @pid
    persons = {}
    for p in data.get("Person", []):
        if isinstance(p, dict) and "@pid" in p:
            persons[p["@pid"]] = p

    source_date = _extract_source_date(data.get("Source", {}))

    rows = []
    for rel in data.get("RelationEP", []):
        if not isinstance(rel, dict):
            continue
        relation_type = rel.get("RelationType")
        person = persons.get(rel.get("PersonKeyRef"), {})

        first_name, prefix_last_name, last_name = _extract_name(person)

        bd = person.get("BirthDate") or {}
        birth_year, birth_month, birth_day = _extract_date(bd)

        age_obj = person.get("Age") or {}
        if isinstance(age_obj, dict):
            age_literal = age_obj.get("PersonAgeLiteral")
            age_years = age_obj.get("PersonAgeYears")
        else:
            age_literal, age_years = None, None

        profession = person.get("Profession")
        if isinstance(profession, list):
            profession = profession[0] if profession else None
        if profession is not None:
            profession = str(profession)

        rows.append({
            "identifier": identifier,
            "archive_code": meta.get("archive_code"),
            "amco": meta.get("amco"),
            "eventdate_year": meta.get("eventdate_year"),
            "eventdate_month": meta.get("eventdate_month"),
            "eventdate_day": meta.get("eventdate_day"),
            "eventplace_raw": meta.get("eventplace_raw"),
            "relation_type": relation_type,
            "person_pid": person.get("@pid"),
            "first_name": first_name,
            "prefix_last_name": prefix_last_name,
            "last_name": last_name,
            "profession": profession,
            "birth_year": birth_year,
            "birth_month": birth_month,
            "birth_day": birth_day,
            "age_literal": str(age_literal) if age_literal is not None else None,
            "age_years": str(age_years) if age_years is not None else None,
            "source_date": source_date,
        })

    return rows


# ---------------------------------------------------------------------------
# DuckDB schema helpers
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS survey_progress (
    amco         TEXT,
    name         TEXT,
    from_date    TEXT,
    until_date   TEXT,
    number_found INTEGER,
    PRIMARY KEY (amco, from_date)
);

CREATE TABLE IF NOT EXISTS list_progress (
    amco TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS list_progress_candidates (
    amco TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS search_index (
    identifier      TEXT PRIMARY KEY,
    archive_code    TEXT NOT NULL,
    amco            TEXT,
    eventplace_raw  TEXT,
    eventdate_year  INTEGER,
    eventdate_month INTEGER,
    eventdate_day   INTEGER,
    sourcetype      TEXT,
    url             TEXT
);

CREATE TABLE IF NOT EXISTS downloaded_identifiers (
    identifier TEXT PRIMARY KEY,
    fetched_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS marriages_raw (
    identifier       TEXT NOT NULL,
    archive_code     TEXT NOT NULL,
    amco             TEXT,
    eventdate_year   INTEGER,
    eventdate_month  INTEGER,
    eventdate_day    INTEGER,
    eventplace_raw   TEXT,
    relation_type    TEXT,
    person_pid       TEXT,
    first_name       TEXT,
    prefix_last_name TEXT,
    last_name        TEXT,
    profession       TEXT,
    birth_year       INTEGER,
    birth_month      INTEGER,
    birth_day        INTEGER,
    age_literal      TEXT,
    age_years        TEXT,
    source_date      TEXT,
    fetched_at       TIMESTAMP DEFAULT current_timestamp
);
"""


def init_db(con) -> None:
    """Create all required tables in the DuckDB connection."""
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
