# =============================================================================
# huygens_step2_fetch_uitslagen.py  [HUYGENS PIPELINE - STEP 2]
# Input:  data/huygens/huygens.duckdb  (uitslag_index from step 1)
# Output: data/huygens/huygens.duckdb  (elections + candidates_raw tables)
#
# Fetch every uitslag_per_verkiezing page listed in `uitslag_index` and parse
# the election header (adds zetels + kiesdrempel over the step-1 listing data)
# and the per-candidate result table (name, persoon_ID, affiliation, votes,
# vote share). Elected status is NOT derived here — that happens in the panel
# assembly step, where threshold/runoff logic can see all rounds of a contest.
#
# Resumable: uitslag_IDs present in `fetched_uitslagen` are skipped on rerun.
# Parquet export happens in the panel assembly step.
#
# Usage:
#   uv run python code/data_wrangling/huygens/huygens_step2_fetch_uitslagen.py
#   uv run python code/data_wrangling/huygens/huygens_step2_fetch_uitslagen.py --limit 50
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
    parse_uitslag_page,
)

DB_PATH = "./data/huygens/huygens.duckdb"

RATE = 3.0        # requests per second
CONCURRENCY = 6   # max simultaneous open connections
FLUSH_BATCH = 100 # write to DuckDB after this many parsed pages


def uitslag_url(uitslag_id: int) -> str:
    return f"{BASE_URL}/uitslag_per_verkiezing?uitslag_ID={uitslag_id}"


async def fetch_one(session, bucket, sem, uitslag_id: int):
    async with sem:
        await bucket.acquire()
        try:
            async with session.get(uitslag_url(uitslag_id)) as resp:
                resp.raise_for_status()
                html = await resp.text()
        except Exception as e:
            print(f"  FAIL uitslag_ID={uitslag_id}: {e}")
            return None
    return parse_uitslag_page(html, uitslag_id)


def flush(con, parsed: list) -> None:
    """Write a batch of (election, candidates) tuples inside one transaction."""
    if not parsed:
        return
    con.execute("BEGIN")
    for election, candidates in parsed:
        con.execute(
            """
            INSERT OR REPLACE INTO elections
            (uitslag_id, district, district_id, date_raw, type, electoraat,
             opkomst, stembriefjes, geldig, blanco, zetels, kiesdrempel)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [election["uitslag_id"], election["district"],
             election["district_id"], election["date_raw"], election["type"],
             election["electoraat"], election["opkomst"],
             election["stembriefjes"], election["geldig"], election["blanco"],
             election["zetels"], election["kiesdrempel"]],
        )
        con.execute("DELETE FROM candidates_raw WHERE uitslag_id = ?",
                    [election["uitslag_id"]])
        if candidates:
            con.executemany(
                """
                INSERT INTO candidates_raw
                (uitslag_id, rank, name_raw, persoon_id, affiliation, votes, pct)
                VALUES (?,?,?,?,?,?,?)
                """,
                [(c["uitslag_id"], c["rank"], c["name_raw"], c["persoon_id"],
                  c["affiliation"], c["votes"], c["pct"]) for c in candidates],
            )
        con.execute("INSERT OR IGNORE INTO fetched_uitslagen (uitslag_id) VALUES (?)",
                    [election["uitslag_id"]])
    con.execute("COMMIT")


async def main(limit: int | None) -> None:
    con = duckdb.connect(DB_PATH)
    init_db(con)

    todo = [r[0] for r in con.execute(
        """
        SELECT uitslag_id FROM uitslag_index
        WHERE uitslag_id NOT IN (SELECT uitslag_id FROM fetched_uitslagen)
        ORDER BY uitslag_id
        """
    ).fetchall()]
    if limit:
        todo = todo[:limit]
    print(f"Step 2: {len(todo)} uitslag pages to fetch")

    bucket = TokenBucketRateLimiter(RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    session = make_session()
    n_done = 0
    n_cands = 0
    try:
        for i in range(0, len(todo), FLUSH_BATCH):
            batch = todo[i:i + FLUSH_BATCH]
            results = await asyncio.gather(
                *(fetch_one(session, bucket, sem, uid) for uid in batch))
            parsed = [r for r in results if r is not None]
            flush(con, parsed)
            n_done += len(parsed)
            n_cands += sum(len(c) for _, c in parsed)
            print(f"  {n_done}/{len(todo)} pages, {n_cands} candidate rows",
                  flush=True)
    finally:
        await session.close()

    n_el = con.execute("SELECT COUNT(*) FROM elections").fetchone()[0]
    n_ca = con.execute("SELECT COUNT(*) FROM candidates_raw").fetchone()[0]
    print(f"Done. elections={n_el}, candidates_raw={n_ca}")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    asyncio.run(main(args.limit))
