# =============================================================================
# openarch_step1_query_candidates.py  [OPENARCHIEVEN PIPELINE - STEP 1]  (2b)
# Input:  data/panel/candidate_roster.parquet  (panel step 3)
# Output: data/openarch/openarch.duckdb
#           query_progress(era, key, sourcetype)  -- resumable
#           hits(era, key, sourcetype, identifier, personname, relationtype,
#                event_year/month/day, eventplace, ...)
#
# For every candidate, searches OpenArchieven civil-registration records by
# surname + plausible birth-year window (see panel_step3_candidate_roster.py)
# against two sourcetypes: "BS Geboorte" (own birth record -- relationtype
# "Kind" identifies the candidate's own row vs a parent's) and "BS Huwelijk"
# (own marriage -- relationtype "Bruid"/"Bruidegom"). Both are cheap
# search-only calls: no records/show.json detail fetch here (profession/
# parents are deferred to Phase 3 for the pairs accepted after scoring).
# Paginates up to MAX_PAGES per (candidate, sourcetype) to bound the cost of
# common surnames.
#
# Usage:
#   uv run python code/data_wrangling/openarch/openarch_step1_query_candidates.py
#   uv run python code/data_wrangling/openarch/openarch_step1_query_candidates.py --limit 50
# =============================================================================
import argparse
import asyncio
import os
import sys

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from openarch_async_helpers import init_db, make_session, parse_search_response, \
    search_url
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter

DB_PATH = "./data/openarch/openarch.duckdb"
ROSTER_PATH = "./data/panel/candidate_roster.parquet"

RATE = 3.0        # deliberately under the API's ~4/s cap -- 4.0 drew 429s
CONCURRENCY = 4
FLUSH_BATCH = 200
PAGE_SIZE = 50
MAX_PAGES = 10  # cap at 500 hits for very common surnames
MAX_RETRY = 5


async def fetch_json(session, bucket, url: str):
    for attempt in range(MAX_RETRY):
        await bucket.acquire()
        async with session.get(url) as resp:
            if resp.status in (429, 503):
                await asyncio.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return await resp.json()
    raise RuntimeError(f"gave up after {MAX_RETRY} retries: {url}")


SOURCETYPES = ["BS Geboorte", "BS Huwelijk"]


async def query_one(session, bucket, sem, era, key, surname, year_lo, year_hi,
                     sourcetype):
    async with sem:
        rows: list[dict] = []
        start = 0
        number_found = None
        failed = False
        for _ in range(MAX_PAGES):
            url = search_url(surname, year_lo, year_hi, sourcetype, start, PAGE_SIZE)
            try:
                data = await fetch_json(session, bucket, url)
            except Exception as e:
                print(f"  FAIL {era}/{key}/{sourcetype}: {e}")
                failed = True
                break
            number_found, page_rows = parse_search_response(data)
            rows.extend(page_rows)
            start += PAGE_SIZE
            if start >= number_found or not page_rows:
                break
        return era, key, sourcetype, number_found or 0, rows, failed


def flush(con, results: list) -> None:
    if not results:
        return
    con.execute("BEGIN")
    for era, key, sourcetype, n_hits, rows, failed in results:
        if failed:
            continue  # leave out of query_progress so a rerun retries it
        con.execute(
            "INSERT OR REPLACE INTO query_progress (era, key, sourcetype, n_hits) "
            "VALUES (?,?,?,?)", [era, key, sourcetype, n_hits])
        con.execute(
            "DELETE FROM hits WHERE era=? AND key=? AND sourcetype=?",
            [era, key, sourcetype])
        if rows:
            con.executemany(
                """INSERT INTO hits
                   (era, key, sourcetype, identifier, archive_code, pid,
                    personname, relationtype, event_year, event_month,
                    event_day, eventplace, url)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(era, key, sourcetype, r["identifier"], r["archive_code"],
                  r["pid"], r["personname"], r["relationtype"], r["event_year"],
                  r["event_month"], r["event_day"], r["eventplace"], r["url"])
                 for r in rows])
    con.execute("COMMIT")


async def main(limit: int | None) -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    init_db(con)

    roster = pd.read_parquet(ROSTER_PATH)
    roster = roster[roster["sn"] != ""]

    todo = []
    for row in roster.itertuples():
        for st in SOURCETYPES:
            # search on the ORIGINAL surname spelling/spacing (row.sn is
            # aggressively normalised for scoring -- concatenated multi-word
            # surnames and y/ij-folded spellings both return zero hits from
            # the API, which does its own, different, tokenisation)
            todo.append((row.era, row.key, row.surname_raw, row.birth_year_lo,
                         row.birth_year_hi, st))
    done = set(con.execute(
        "SELECT era, key, sourcetype FROM query_progress").fetchall())
    todo = [t for t in todo if (t[0], t[1], t[5]) not in done]
    if limit:
        todo = todo[:limit]
    print(f"Step 1: {len(todo)} (candidate, sourcetype) queries to run")

    bucket = TokenBucketRateLimiter(RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    session = make_session()
    n_done, n_hits = 0, 0
    try:
        for i in range(0, len(todo), FLUSH_BATCH):
            batch = todo[i:i + FLUSH_BATCH]
            results = await asyncio.gather(
                *(query_one(session, bucket, sem, era, key, sn, lo, hi, st)
                  for era, key, sn, lo, hi, st in batch))
            flush(con, results)
            n_done += len(results)
            n_hits += sum(len(r[4]) for r in results)
            print(f"  {n_done}/{len(todo)} queries, {n_hits} hit-rows so far",
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
