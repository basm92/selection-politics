# =============================================================================
# openarch_async_helpers.py  [HELPER — OPENARCHIEVEN PIPELINE]
# Shared async infrastructure + response parsing for api.openarchieven.nl.
# Used by: openarch_step1_query_candidates.py
#
# API notes (verified live 2026-07-08): `records/search.json` takes a `name`
# param that is SURNAME-ONLY plus an optional embedded year range
# ("Colijn 1865-1875") -- multi-word full-name queries ("Hendrik Colijn
# 1865-1875") silently return 0 hits, so search must be surname+year and any
# first-name/initials matching done client-side against each hit's
# `personname`. For `sourcetype=BS Geboorte` (birth) and `BS Huwelijk`
# (marriage), the search-result docs already carry personname/eventdate/
# eventplace/relationtype -- no `records/show.json` detail call is needed
# for Phase 2b's identity-matching purpose (a detail call would additionally
# give profession/parents, deferred to Phase 3).
# =============================================================================
import os
from urllib.parse import urlencode

import aiohttp

BASE_URL = "https://api.openarchieven.nl/1.1/"
USER_AGENT = "selection-politics-research/0.1 (academic; a.h.machielsen@uu.nl)"


def make_session(connector_limit: int = 10) -> aiohttp.ClientSession:
    connector = aiohttp.TCPConnector(limit=connector_limit, ssl=False)
    headers = {"User-Agent": USER_AGENT}
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    return aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout)


def search_url(surname: str, year_lo: int, year_hi: int, sourcetype: str,
               start: int = 0, number_show: int = 50) -> str:
    name = f"{surname} {int(year_lo)}-{int(year_hi)}"
    params = {"name": name, "sourcetype": sourcetype,
              "start": start, "number_show": number_show}
    return f"{BASE_URL}records/search.json?{urlencode(params)}"


def parse_search_response(data: dict) -> tuple[int, list[dict]]:
    """Returns (number_found, hit-rows). Each row: identifier, archive_code,
    pid, personname, relationtype, event_year/month/day, eventplace, url."""
    resp = data.get("response", {})
    number_found = resp.get("number_found", 0)
    rows = []
    for doc in resp.get("docs", []):
        eventdate = doc.get("eventdate") or {}
        eventplace = doc.get("eventplace") or []
        rows.append({
            "identifier": doc.get("identifier"),
            "archive_code": doc.get("archive_code"),
            "pid": doc.get("pid"),
            "personname": doc.get("personname"),
            "relationtype": doc.get("relationtype"),
            "event_year": eventdate.get("year"),
            "event_month": eventdate.get("month"),
            "event_day": eventdate.get("day"),
            "eventplace": eventplace[0] if len(eventplace) == 1 else
                          (", ".join(eventplace) if eventplace else None),
            "url": doc.get("url"),
        })
    return number_found, rows


DDL = """
CREATE TABLE IF NOT EXISTS query_progress (
    era        VARCHAR,
    key        VARCHAR,
    sourcetype VARCHAR,
    n_hits     INTEGER,
    queried_at TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (era, key, sourcetype)
);

CREATE TABLE IF NOT EXISTS hits (
    era          VARCHAR,
    key          VARCHAR,
    sourcetype   VARCHAR,
    identifier   VARCHAR,
    archive_code VARCHAR,
    pid          VARCHAR,
    personname   VARCHAR,
    relationtype VARCHAR,
    event_year   INTEGER,
    event_month  INTEGER,
    event_day    INTEGER,
    eventplace   VARCHAR,
    url          VARCHAR
);
"""


def init_db(con) -> None:
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
