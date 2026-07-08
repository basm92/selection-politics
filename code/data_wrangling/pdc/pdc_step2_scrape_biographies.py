# =============================================================================
# pdc_step2_scrape_biographies.py  [PDC PIPELINE - STEP 2]
# Input:  data/pdc/pdc.duckdb  (biografie_urls from step 1)
# Output: data/pdc/pdc.duckdb
#           persons(url PK, slug, title, voornamen, titulatuur, geboorte_raw,
#                   overlijden_raw, partij_raw)
#           functions(url, section, subsection, seq, text)  -- every <li> in
#                   every ul.biolist on the page (Hoofdfuncties/beroepen,
#                   Nevenfuncties, Partijpolitieke functies, ambtstitel, ...),
#                   tagged with its nearest preceding h2/h3.biohdr. Step 3
#                   filters these for "lid ... Tweede Kamer der Staten-
#                   Generaal" entries to find Tweede Kamer membership spans.
#           fetch_progress(url PK, http_status, fetched_at)
#
# Parlement.com biography pages (Drupal, server-rendered) follow a fixed
# structure: h2 section headers (id="p1".."p9/p10"), h3.biohdr sub-labels,
# and either a single p.bioitem (scalar field) or a ul.biolist (list of
# dated entries "<role>, van <date> tot <date> (voor <district>)"). The free
# page is a "selectie" of the full career (e.g. "Hoofdfuncties/beroepen
# (17/20)") -- some minor entries may be missing, but Tweede Kamer terms are
# a hoofdfunctie and Personalia (birth/death) is a separate, complete section.
#
# Resumable: urls already in fetch_progress are skipped on rerun (including
# failed fetches, to avoid hammering a dead/404 page every run).
#
# Usage:
#   uv run python code/data_wrangling/pdc/pdc_step2_scrape_biographies.py
#   uv run python code/data_wrangling/pdc/pdc_step2_scrape_biographies.py --limit 50
# =============================================================================
import argparse
import asyncio
import os
import re
import sys

import duckdb
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter, make_session

OUT_DIR = "./data/pdc"
DB_PATH = os.path.join(OUT_DIR, "pdc.duckdb")

RATE = 2.0        # requests per second (small nonprofit server -- be polite)
CONCURRENCY = 4
FLUSH_BATCH = 100

DDL = """
CREATE TABLE IF NOT EXISTS persons (
    url            VARCHAR PRIMARY KEY,
    slug           VARCHAR,
    title          VARCHAR,
    voornamen      VARCHAR,
    titulatuur     VARCHAR,
    geboorte_raw   VARCHAR,
    overlijden_raw VARCHAR,
    partij_raw     VARCHAR
);
CREATE TABLE IF NOT EXISTS functions (
    url        VARCHAR,
    section    VARCHAR,
    subsection VARCHAR,
    seq        INTEGER,
    text       VARCHAR
);
CREATE TABLE IF NOT EXISTS fetch_progress (
    url        VARCHAR PRIMARY KEY,
    http_status INTEGER,
    fetched_at TIMESTAMP DEFAULT current_timestamp
);
"""

_PERSONALIA_LABELS = {
    "voornamen (roepnaam)": "voornamen",
    "titulatuur en naam": "titulatuur",
    "geboorteplaats en -datum": "geboorte_raw",
    "overlijdensplaats en -datum": "overlijden_raw",
}
_PARTIJ_LABELS = {"partij(en)", "stroming(en)"}


def _clean(s: str | None) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def parse_bio(html: str, url: str, slug: str) -> tuple[dict, list[dict]]:
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    title = _clean(title_tag.get_text()).split(" | ")[0] if title_tag else None

    person = {
        "url": url, "slug": slug, "title": title,
        "voornamen": None, "titulatuur": None,
        "geboorte_raw": None, "overlijden_raw": None, "partij_raw": None,
    }

    for h3 in soup.select("h3.biohdr"):
        label = _clean(h3.get_text())
        if label is None:
            continue
        label_l = label.lower()
        sib = h3.find_next_sibling()
        if sib is None or sib.name != "p":
            continue
        value = _clean(sib.get_text())
        if label_l in _PERSONALIA_LABELS:
            person[_PERSONALIA_LABELS[label_l]] = value
        elif label_l in _PARTIJ_LABELS and person["partij_raw"] is None:
            person["partij_raw"] = value

    functions: list[dict] = []
    for ul in soup.select("ul.biolist"):
        h2 = ul.find_previous("h2")
        h3 = ul.find_previous("h3", class_="biohdr")
        if h3 is not None and h3.find_previous("h2") is not h2:
            h3 = None  # h3 belongs to an earlier h2 section, not this ul's
        section = _clean(h2.get_text()) if h2 else None
        subsection = _clean(h3.get_text()) if h3 else None
        for seq, li in enumerate(ul.find_all("li", recursive=False)):
            text = _clean(li.get_text())
            if text:
                functions.append({
                    "url": url, "section": section, "subsection": subsection,
                    "seq": seq, "text": text,
                })
    return person, functions


async def fetch_one(session, bucket, sem, url: str, slug: str):
    async with sem:
        await bucket.acquire()
        try:
            async with session.get(url) as resp:
                status = resp.status
                if status != 200:
                    return url, status, None, None
                html = await resp.text()
        except Exception as e:
            print(f"  FAIL {url}: {e}")
            return url, None, None, None
    person, functions = parse_bio(html, url, slug)
    return url, status, person, functions


def flush(con, results: list) -> None:
    if not results:
        return
    con.execute("BEGIN")
    for url, status, person, functions in results:
        if person is not None:
            con.execute(
                """
                INSERT OR REPLACE INTO persons
                (url, slug, title, voornamen, titulatuur, geboorte_raw,
                 overlijden_raw, partij_raw)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                [person["url"], person["slug"], person["title"],
                 person["voornamen"], person["titulatuur"],
                 person["geboorte_raw"], person["overlijden_raw"],
                 person["partij_raw"]],
            )
            con.execute("DELETE FROM functions WHERE url = ?", [url])
            if functions:
                con.executemany(
                    "INSERT INTO functions VALUES (?,?,?,?,?)",
                    [(f["url"], f["section"], f["subsection"], f["seq"], f["text"])
                     for f in functions],
                )
        con.execute(
            "INSERT OR REPLACE INTO fetch_progress (url, http_status) VALUES (?,?)",
            [url, status],
        )
    con.execute("COMMIT")


async def main(limit: int | None) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    con.execute(DDL)

    todo = con.execute("""
        SELECT url, slug FROM biografie_urls
        WHERE url NOT IN (SELECT url FROM fetch_progress)
        ORDER BY url
    """).fetchall()
    if limit:
        todo = todo[:limit]
    print(f"Step 2: {len(todo)} biography pages to fetch")

    bucket = TokenBucketRateLimiter(RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    session = make_session()
    n_done = 0
    n_ok = 0
    try:
        for i in range(0, len(todo), FLUSH_BATCH):
            batch = todo[i:i + FLUSH_BATCH]
            results = await asyncio.gather(
                *(fetch_one(session, bucket, sem, url, slug) for url, slug in batch))
            flush(con, results)
            n_done += len(results)
            n_ok += sum(1 for r in results if r[2] is not None)
            print(f"  {n_done}/{len(todo)} pages ({n_ok} parsed ok)", flush=True)
    finally:
        await session.close()

    n_persons = con.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    n_functions = con.execute("SELECT COUNT(*) FROM functions").fetchone()[0]
    n_failed = con.execute(
        "SELECT COUNT(*) FROM fetch_progress WHERE http_status IS NULL OR http_status != 200"
    ).fetchone()[0]
    print(f"Done. persons={n_persons}, functions={n_functions}, failed={n_failed}")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    asyncio.run(main(args.limit))
