# =============================================================================
# openarch_step2_fetch_details.py  [OPENARCHIEVEN PIPELINE - STEP 2]  (Phase 3)
# Input:  data/panel/candidate_person_pairs.parquet  (panel step 4)
#         data/openarch/openarch.duckdb               (hits table, step 1)
# Output: data/openarch/openarch.duckdb
#           detail_progress(archive_code, identifier)  -- resumable
#           detail_records(archive_code, identifier, relation_type,
#                           first_name, prefix_last_name, last_name,
#                           profession, age_literal, event_type, event_year,
#                           eventplace)  -- one row per RelationEP entry
#
# Scope (deliberately narrow -- see phase_2_and_onward.md Phase 3 + hand-
# labelling in docs/agent_memory/phase2b-candidate-linkage.md): calling
# records/show.json for every one of the 821,781 openarch pairs would spend
# most of its budget on near-certain wrong matches (top-scored-pair precision
# was only 13% for common surnames in the calibration sample). Instead this
# fetches the SINGLE best-scoring openarch pair per candidate, restricted to
# score >= 0.7 (the threshold validated in Phase 2b) -- about 2,700
# candidates. Each such pair's record (a BS Geboorte or BS Huwelijk event)
# is fetched once via records/show.json to recover the candidate's own
# profession and, via RelationType, their father's (see
# openarch_async_helpers.parse_show_response for the schema).
#
# Usage:
#   uv run python code/data_wrangling/openarch/openarch_step2_fetch_details.py
#   uv run python code/data_wrangling/openarch/openarch_step2_fetch_details.py --limit 50
# =============================================================================
import argparse
import asyncio
import os
import sys

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from openarch_async_helpers import init_db, make_session, parse_show_response, \
    show_url
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter

DB_PATH = "./data/openarch/openarch.duckdb"
PAIRS_PATH = "./data/panel/candidate_person_pairs.parquet"

SCORE_THRESHOLD = 0.7
RATE = 3.0
CONCURRENCY = 4
FLUSH_BATCH = 200
MAX_RETRY = 5


async def fetch_json(session, bucket, url: str):
    for attempt in range(MAX_RETRY):
        await bucket.acquire()
        try:
            async with session.get(url) as resp:
                if resp.status in (429, 503):
                    await asyncio.sleep(2 ** attempt)
                    continue
                if resp.status == 404:
                    return {}
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except Exception:
            if attempt == MAX_RETRY - 1:
                raise
            await asyncio.sleep(2 ** attempt)
    return {}


def best_pairs_todo(con) -> pd.DataFrame:
    """Best-scoring openarch pair per candidate, score >= threshold, joined
    to hits for archive_code -- excluding records already fetched."""
    pairs = pd.read_parquet(PAIRS_PATH)
    oa = pairs[pairs["source"] == "openarch"].copy()
    oa = oa[oa["score"] >= SCORE_THRESHOLD]
    oa = oa.sort_values("score", ascending=False).drop_duplicates(
        subset=["era", "key"], keep="first"
    )

    hits = con.execute(
        "SELECT DISTINCT era, key, identifier, archive_code FROM hits"
    ).df()
    todo = oa.merge(
        hits, left_on=["era", "key", "person_ref"],
        right_on=["era", "key", "identifier"], how="inner",
    )
    done = con.execute("SELECT archive_code, identifier FROM detail_progress").df()
    if not done.empty:
        todo = todo.merge(done, on=["archive_code", "identifier"], how="left", indicator=True)
        todo = todo[todo["_merge"] == "left_only"].drop(columns="_merge")
    return todo.drop_duplicates(subset=["archive_code", "identifier"])[
        ["archive_code", "identifier"]
    ]


async def run(limit: int = 0) -> None:
    con = duckdb.connect(DB_PATH)
    init_db(con)

    todo = best_pairs_todo(con)
    if limit:
        todo = todo.head(limit)
    print(f"openarch step2: {len(todo)} records to fetch (best pair/candidate, "
          f"score>={SCORE_THRESHOLD})")
    if todo.empty:
        con.close()
        return

    bucket = TokenBucketRateLimiter(rate=RATE)
    session = make_session()
    sem = asyncio.Semaphore(CONCURRENCY)

    record_buf: list[tuple] = []
    progress_buf: list[tuple] = []
    done = 0

    def flush():
        nonlocal record_buf, progress_buf
        if record_buf:
            con.executemany(
                "INSERT INTO detail_records VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                record_buf,
            )
            record_buf = []
        if progress_buf:
            con.executemany(
                "INSERT OR IGNORE INTO detail_progress (archive_code, identifier) VALUES (?,?)",
                progress_buf,
            )
            progress_buf = []

    async def process(archive_code: str, identifier: str):
        nonlocal done
        async with sem:
            url = show_url(archive_code, identifier)
            data = await fetch_json(session, bucket, url)
            rows = parse_show_response(data)
            for r in rows:
                record_buf.append((
                    archive_code, identifier, r["relation_type"],
                    r["first_name"], r["prefix_last_name"], r["last_name"],
                    r["profession"], r["age_literal"], r["event_type"],
                    int(r["event_year"]) if r["event_year"] else None,
                    r["eventplace"],
                ))
            progress_buf.append((archive_code, identifier))
            done += 1
            if done % FLUSH_BATCH == 0:
                flush()
                print(f"  ... {done}/{len(todo)} records fetched")

    for start in range(0, len(todo), FLUSH_BATCH):
        batch = todo.iloc[start:start + FLUSH_BATCH]
        await asyncio.gather(*[
            process(row.archive_code, row.identifier) for row in batch.itertuples()
        ])
        flush()

    await session.close()
    n_records = con.execute("SELECT COUNT(*) FROM detail_records").fetchone()[0]
    n_progress = con.execute("SELECT COUNT(*) FROM detail_progress").fetchone()[0]
    n_profession = con.execute(
        "SELECT COUNT(*) FROM detail_records WHERE profession IS NOT NULL"
    ).fetchone()[0]
    print(f"Done: {n_progress} records fetched total, {n_records} RelationEP rows, "
          f"{n_profession} with a profession string.")
    con.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    asyncio.run(run(limit=args.limit))


if __name__ == "__main__":
    main()
