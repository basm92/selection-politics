# =============================================================================
# genealogieonline_step1_query_candidates.py  [GENEALOGIEONLINE - STEP 1] (2b)
# Input:  data/panel/candidate_roster.parquet  (panel step 3)
# Output: data/genealogieonline/genealogieonline.duckdb
#           query_progress(era, key)  -- resumable
#           hits(era, key, url, person_name, birth_year, death_year, source_tree)
#
# For every candidate, searches GenealogieOnline by surname + plausible
# birth-year window (vn= left blank -- see genealogieonline_async_helpers.py
# docstring for why initials can't be passed there); each hit's full name is
# stored as-is for client-side initials/first-name scoring in panel step 4.
# Paginates (15/page) up to MAX_PAGES per candidate to bound common-surname
# cost; stops early when a page returns no hits.
#
# Usage:
#   uv run python code/data_wrangling/genealogieonline/genealogieonline_step1_query_candidates.py
#   uv run python code/data_wrangling/genealogieonline/genealogieonline_step1_query_candidates.py --limit 50
# =============================================================================
import argparse
import asyncio
import os
import sys

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from genealogieonline_async_helpers import init_db, make_session, \
    parse_search_results, search_url
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter

DB_PATH = "./data/genealogieonline/genealogieonline.duckdb"
ROSTER_PATH = "./data/panel/candidate_roster.parquet"

RATE = 3.0
CONCURRENCY = 4
FLUSH_BATCH = 200
MAX_PAGES = 10  # cap at 150 hits for very common surnames
MAX_RETRY = 5


async def fetch_html(session, bucket, url: str):
    for attempt in range(MAX_RETRY):
        await bucket.acquire()
        async with session.get(url) as resp:
            if resp.status in (429, 503):
                await asyncio.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return await resp.text()
    raise RuntimeError(f"gave up after {MAX_RETRY} retries: {url}")


async def query_one(session, bucket, sem, era, key, surname, year_lo, year_hi):
    async with sem:
        rows: list[dict] = []
        failed = False
        for page in range(MAX_PAGES):
            url = search_url(surname, year_lo, year_hi, page * 15)
            try:
                html = await fetch_html(session, bucket, url)
            except Exception as e:
                print(f"  FAIL {era}/{key}: {e}")
                failed = True
                break
            page_rows = parse_search_results(html)
            if not page_rows:
                break
            rows.extend(page_rows)
            if len(page_rows) < 15:
                break
        return era, key, rows, failed


def flush(con, results: list) -> None:
    if not results:
        return
    con.execute("BEGIN")
    for era, key, rows, failed in results:
        if failed:
            continue
        con.execute(
            "INSERT OR REPLACE INTO query_progress (era, key, n_hits) VALUES (?,?,?)",
            [era, key, len(rows)])
        con.execute("DELETE FROM hits WHERE era=? AND key=?", [era, key])
        if rows:
            con.executemany(
                "INSERT INTO hits VALUES (?,?,?,?,?,?,?)",
                [(era, key, r["url"], r["person_name"], r["birth_year"],
                  r["death_year"], r["source_tree"]) for r in rows])
    con.execute("COMMIT")


async def main(limit: int | None) -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    init_db(con)

    roster = pd.read_parquet(ROSTER_PATH)
    roster = roster[roster["sn"] != ""]

    done = set(con.execute("SELECT era, key FROM query_progress").fetchall())
    # search on the ORIGINAL surname spelling (r.sn is normalised for scoring
    # only and breaks the search for y/ij names -- see the identical note in
    # openarch_step1_query_candidates.py). GenealogieOnline's q= additionally
    # returns 0 hits for a multi-word phrase ("Oldenhuis Gratama") even
    # though each word alone matches (verified live) -- use the surname's
    # last word only for compound surnames; OpenArchieven's `name` search
    # has no such problem and keeps the full phrase.
    todo = [(r.era, r.key, r.surname_raw.split()[-1], r.birth_year_lo, r.birth_year_hi)
            for r in roster.itertuples() if (r.era, r.key) not in done]
    if limit:
        todo = todo[:limit]
    print(f"Step 1: {len(todo)} candidates to query")

    bucket = TokenBucketRateLimiter(RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    session = make_session()
    n_done, n_hits = 0, 0
    try:
        for i in range(0, len(todo), FLUSH_BATCH):
            batch = todo[i:i + FLUSH_BATCH]
            results = await asyncio.gather(
                *(query_one(session, bucket, sem, era, key, sn, lo, hi)
                  for era, key, sn, lo, hi in batch))
            flush(con, results)
            n_done += len(results)
            n_hits += sum(len(r[2]) for r in results)
            print(f"  {n_done}/{len(todo)} candidates, {n_hits} hit-rows so far",
                  flush=True)
    finally:
        await session.close()

    n_q = con.execute("SELECT COUNT(*) FROM query_progress").fetchone()[0]
    n_h = con.execute("SELECT COUNT(*) FROM hits").fetchone()[0]
    print(f"Done. query_progress={n_q}, hits={n_h}")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    asyncio.run(main(args.limit))
