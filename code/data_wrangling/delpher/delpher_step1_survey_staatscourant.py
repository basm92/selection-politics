# =============================================================================
# delpher_step1_survey_staatscourant.py  [DELPHER PIPELINE - STEP 1]
# Input:  KB SRU API (jsru.kb.nl, collection DDD_artikel) + resolver.kb.nl
# Output: data/delpher/delpher.duckdb  (articles + query_progress tables)
#
# Survey the digitized "Nederlandsche staatscourant" in Delpher for the
# official election paperwork around every interwar Tweede Kamer general
# election: candidate lists (published after kandidaatstelling; old spelling
# "candidaten") and the Centraal Stembureau proces-verbaal with the official
# result (published 1-2 weeks after election day).
#
# CBS published no TK election statistics for 1918-1929, so these Staatscourant
# issues are the primary candidate-level source for those years (Phase 0/1
# finding). For each election a [-70 days, +40 days] window is searched with a
# set of keyword queries; article metadata AND the raw article OCR text are
# stored. The OCR is knowingly poor — it serves to locate the right pages;
# re-OCR of the scans happens later.
#
# Resumable: (election, keyword) queries in `query_progress` are skipped;
# article OCR is fetched only where ocr_text IS NULL.
#
# Usage:
#   uv run python code/data_wrangling/delpher/delpher_step1_survey_staatscourant.py
# =============================================================================
import asyncio
import datetime as dt
import os
import re
import sys
import xml.etree.ElementTree as ET

import aiohttp
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter, USER_AGENT

SRU_URL = "https://jsru.kb.nl/sru/sru"
RESOLVER = "https://resolver.kb.nl/resolve"
DB_PATH = "./data/delpher/delpher.duckdb"

RATE = 3.0
PAGE_SIZE = 50

ELECTIONS = {
    1918: "1918-07-03",
    1922: "1922-07-05",
    1925: "1925-07-01",
    1929: "1929-07-03",
    1933: "1933-04-26",
    1937: "1937-05-26",
}

# Both spelling eras + the institutional terms that head the official notices.
KEYWORDS = [
    '"candidatenlijst"', '"kandidatenlijst"', '"candidatenlijsten"',
    '"kandidatenlijsten"', '"lijsten van candidaten"',
    '"centraal stembureau"', '"proces-verbaal"',
    '"kandidaatstelling"', '"candidaatstelling"',
]

WINDOW_BEFORE = 70   # days before election: covers kandidaatstelling + lists
WINDOW_AFTER = 40    # days after: covers proces-verbaal + seat assignment

NS = {
    "srw": "http://www.loc.gov/zing/srw/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "ddd": "http://www.kb.nl/ddd",
}

DDL = """
CREATE TABLE IF NOT EXISTS query_progress (
    election_year INTEGER,
    keyword       TEXT,
    n_found       INTEGER,
    PRIMARY KEY (election_year, keyword)
);
CREATE TABLE IF NOT EXISTS articles (
    metadata_key  TEXT PRIMARY KEY,
    election_year INTEGER,
    title         TEXT,
    date          DATE,
    page          INTEGER,
    page_urn      TEXT,
    issue_urn     TEXT,
    ocr_text      TEXT
);
"""


def sru_query(keyword: str, frm: dt.date, to: dt.date) -> str:
    return (
        'papertitle exact "Nederlandsche staatscourant" AND '
        f'{keyword} AND date within "{frm:%d-%m-%Y} {to:%d-%m-%Y}"'
    )


async def sru_page(session, bucket, query: str, start: int) -> tuple[int, list[dict]]:
    params = {
        "version": "1.2", "operation": "searchRetrieve",
        "x-collection": "DDD_artikel", "recordSchema": "ddd",
        "maximumRecords": PAGE_SIZE, "startRecord": start, "query": query,
    }
    await bucket.acquire()
    async with session.get(SRU_URL, params=params) as resp:
        resp.raise_for_status()
        text = await resp.text()
    root = ET.fromstring(text)
    total = int(root.findtext("srw:numberOfRecords", "0", NS))
    rows = []
    for rec in root.iterfind(".//srw:recordData", NS):
        key = rec.findtext("ddd:metadataKey", None, NS)
        if not key:
            continue
        date_raw = (rec.findtext("dc:date", "", NS) or "")[:10]
        try:
            date = dt.datetime.strptime(date_raw, "%Y/%m/%d").date()
        except ValueError:
            date = None
        page_txt = rec.findtext("ddd:page", None, NS)
        rows.append({
            "metadata_key": key,
            "title": rec.findtext("dc:title", None, NS),
            "date": date,
            "page": int(page_txt) if page_txt and page_txt.isdigit() else None,
            "page_urn": rec.findtext("ddd:pageurl", None, NS),
            # metadataKey looks like MMKB08:000179138:mpeg21:a0001
            "issue_urn": ":".join(key.split(":")[:3]),
        })
    return total, rows


async def fetch_ocr(session, bucket, metadata_key: str) -> str | None:
    await bucket.acquire()
    try:
        async with session.get(RESOLVER,
                               params={"urn": f"{metadata_key}:ocr"}) as resp:
            if resp.status != 200:
                return None
            raw = await resp.text()
    except Exception:
        return None
    text = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", text).strip() or None


async def main() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)

    bucket = TokenBucketRateLimiter(RATE)
    timeout = aiohttp.ClientTimeout(total=120, connect=15)
    done = {(r[0], r[1]) for r in
            con.execute("SELECT election_year, keyword FROM query_progress").fetchall()}

    async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT}, timeout=timeout) as session:
        # Phase A: metadata survey
        for year, date_str in ELECTIONS.items():
            eday = dt.date.fromisoformat(date_str)
            frm = eday - dt.timedelta(days=WINDOW_BEFORE)
            to = eday + dt.timedelta(days=WINDOW_AFTER)
            for kw in KEYWORDS:
                if (year, kw) in done:
                    continue
                query = sru_query(kw, frm, to)
                total, rows = await sru_page(session, bucket, query, 1)
                got = list(rows)
                while len(got) < total:
                    _, more = await sru_page(session, bucket, query, len(got) + 1)
                    if not more:
                        break
                    got.extend(more)
                if got:
                    con.executemany(
                        """
                        INSERT INTO articles
                        (metadata_key, election_year, title, date, page, page_urn, issue_urn)
                        VALUES (?,?,?,?,?,?,?)
                        ON CONFLICT (metadata_key) DO NOTHING
                        """,
                        [(r["metadata_key"], year, r["title"], r["date"], r["page"],
                          r["page_urn"], r["issue_urn"]) for r in got],
                    )
                con.execute("INSERT OR REPLACE INTO query_progress VALUES (?,?,?)",
                            [year, kw, total])
                print(f"  {year} {kw}: {total} articles")

        # Phase B: article OCR for orientation
        todo = [r[0] for r in con.execute(
            "SELECT metadata_key FROM articles WHERE ocr_text IS NULL").fetchall()]
        print(f"OCR fetch: {len(todo)} articles")
        for i in range(0, len(todo), 25):
            batch = todo[i:i + 25]
            texts = await asyncio.gather(
                *(fetch_ocr(session, bucket, k) for k in batch))
            con.executemany(
                "UPDATE articles SET ocr_text = ? WHERE metadata_key = ?",
                [(t, k) for k, t in zip(batch, texts) if t])
            if (i // 25) % 10 == 0:
                print(f"  ocr {i + len(batch)}/{len(todo)}")

    print(con.execute(
        "SELECT election_year, COUNT(*), COUNT(DISTINCT issue_urn) "
        "FROM articles GROUP BY 1 ORDER BY 1").fetchall())
    con.close()


if __name__ == "__main__":
    asyncio.run(main())
