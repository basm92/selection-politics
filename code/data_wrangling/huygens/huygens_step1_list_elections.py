# =============================================================================
# huygens_step1_list_elections.py  [HUYGENS PIPELINE - STEP 1]
# Input:  live site resources.huygens.knaw.nl/verkiezingentweedekamer
# Output: data/huygens/huygens.duckdb  (uitslag_index + list_progress tables)
#
# Enumerate every district-election event (uitslag_ID) in the databank by
# paginating the chronological listing one year at a time (all types at once:
# the empty type= filter returns every event). Each listing row already
# carries district, district_ID, date, type and the turnout statistics, so
# step 2 only has to fetch the per-election candidate tables.
#
# Resumable: years already present in `list_progress` are skipped on rerun.
#
# Usage:
#   uv run python code/data_wrangling/huygens/huygens_step1_list_elections.py
#   uv run python code/data_wrangling/huygens/huygens_step1_list_elections.py --from-year 1900
# =============================================================================
import argparse
import asyncio
import os
import sys

import duckdb

sys.path.insert(0, os.path.dirname(__file__))
from huygens_async_helpers import (
    BASE_URL,
    TokenBucketRateLimiter,
    init_db,
    make_session,
    parse_listing_page,
)

DB_PATH = "./data/huygens/huygens.duckdb"

RATE = 3.0        # requests per second (politeness cap, no published limit)
# The site's own "volgende" links use start=26, but pages hold 25 rows — the
# off-by-one in their link silently drops one event per page. Step by 25.
PAGE_STEP = 25

FIRST_YEAR = 1848
LAST_YEAR = 1918


def listing_url(year: int, start: int) -> str:
    return (
        f"{BASE_URL}/chronologisch/index_html"
        f"?beginjaar={year}&eindjaar={year}&type=&start={start}"
    )


async def fetch_year(session, bucket, year: int) -> list[dict]:
    """Paginate one year's listing; return all parsed rows."""
    rows: list[dict] = []
    total = None
    start = 0
    while True:
        await bucket.acquire()
        async with session.get(listing_url(year, start)) as resp:
            resp.raise_for_status()
            html = await resp.text()
        page_total, page_rows = parse_listing_page(html)
        if page_total is not None:
            total = page_total
        rows.extend(page_rows)
        if not page_rows or total is None or len(rows) >= total:
            break
        start += PAGE_STEP
    if total is not None and len(rows) != total:
        print(f"  WARNING {year}: listed total {total} but parsed {len(rows)} rows")
    return rows


async def main(from_year: int) -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    init_db(con)

    done = {r[0] for r in con.execute("SELECT year FROM list_progress").fetchall()}
    years = [y for y in range(from_year, LAST_YEAR + 1) if y not in done]
    print(f"Step 1: {len(years)} years to list ({len(done)} already done)")

    bucket = TokenBucketRateLimiter(RATE)
    session = make_session()
    n_total = 0
    try:
        for year in years:
            rows = await fetch_year(session, bucket, year)
            for r in rows:
                r["list_year"] = year
            con.executemany(
                """
                INSERT OR REPLACE INTO uitslag_index
                (uitslag_id, district, district_id, date_raw, type, electoraat,
                 opkomst, stembriefjes, geldig, blanco, list_year)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (r["uitslag_id"], r["district"], r["district_id"],
                     r["date_raw"], r["type"], r["electoraat"], r["opkomst"],
                     r["stembriefjes"], r["geldig"], r["blanco"], r["list_year"])
                    for r in rows
                ],
            ) if rows else None
            con.execute("INSERT OR IGNORE INTO list_progress VALUES (?)", [year])
            n_total += len(rows)
            print(f"  {year}: {len(rows)} events")
    finally:
        await session.close()

    n_index = con.execute("SELECT COUNT(*) FROM uitslag_index").fetchone()[0]
    print(f"Done. {n_total} events listed this run; uitslag_index holds {n_index}.")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-year", type=int, default=FIRST_YEAR)
    args = ap.parse_args()
    asyncio.run(main(args.from_year))
