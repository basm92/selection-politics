# =============================================================================
# genealogieonline_step2_fetch_person_pages.py  [GENEALOGIEONLINE - STEP 2]  (Phase 3)
# Input:  data/panel/candidate_person_pairs.parquet  (panel step 4)
# Output: data/genealogieonline/genealogieonline.duckdb
#           person_pages(url, person_name_full, beroep, birth_place,
#                        father_url, father_name, skip)  -- resumable node table
#           candidate_ancestors(era, key, url, depth)    -- depth 0 = the
#             candidate's own matched person, 1 = father, 2 = grandfather,
#             3 = great-grandfather (MAX_DEPTH below)
#
# Scope (see openarch_step2_fetch_details.py's docstring for the same
# reasoning): only the single best-scoring genealogieonline pair per
# candidate, score >= 0.7, gets fetched -- about 2,700 candidates' worth of
# depth-0 seeds, not all 459,557 raw search hits.
#
# MAX_DEPTH=3 (~grandfather-to-great-grandfather) is the ancestor window
# needed for status_step2_dynasty_lineage.py's dynasty definition: two
# candidates share a dynasty if their ancestor chains meet within a combined
# depth of 3 (father-son=1, grandfather-grandson=2, first cousins via a
# shared grandfather=2+2 -- filtered to <=3 downstream, so great-grandfather-
# sharing pairs are NOT dynasty per that cutoff but the chain is fetched here
# so the cutoff can be revisited without re-scraping).
#
# BFS by generation: fetch all depth-0 URLs, harvest their father_url into
# depth-1 seeds, fetch those, etc. Each URL is fetched at most once
# regardless of how many candidates/depths reference it (person_pages is
# keyed on url).
#
# Usage:
#   uv run python code/data_wrangling/genealogieonline/genealogieonline_step2_fetch_person_pages.py
#   uv run python code/data_wrangling/genealogieonline/genealogieonline_step2_fetch_person_pages.py --limit 50
# =============================================================================
import argparse
import asyncio
import os
import sys

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from genealogieonline_async_helpers import init_db, make_session, parse_person_page
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter

DB_PATH = "./data/genealogieonline/genealogieonline.duckdb"
PAIRS_PATH = "./data/panel/candidate_person_pairs.parquet"

SCORE_THRESHOLD = 0.7
MAX_DEPTH = 3
RATE = 5.0
CONCURRENCY = 6
FLUSH_BATCH = 200
MAX_RETRY = 5


async def fetch_html(session, bucket, url: str):
    for attempt in range(MAX_RETRY):
        await bucket.acquire()
        try:
            async with session.get(url) as resp:
                if resp.status in (404, 403):
                    return "gone", None
                if resp.status in (429, 503):
                    await asyncio.sleep(2 ** attempt)
                    continue
                resp.raise_for_status()
                return "ok", await resp.text(errors="replace")
        except Exception:
            if attempt == MAX_RETRY - 1:
                return "transient", None
            await asyncio.sleep(2 ** attempt)
    return "transient", None


def seed_depth0(con) -> None:
    """Insert the best-scoring genealogieonline pair per candidate (score >=
    threshold) into candidate_ancestors at depth 0, if not already present."""
    pairs = pd.read_parquet(PAIRS_PATH)
    go = pairs[pairs["source"] == "genealogieonline"].copy()
    go = go[go["score"] >= SCORE_THRESHOLD]
    go = go.sort_values("score", ascending=False).drop_duplicates(
        subset=["era", "key"], keep="first"
    )
    rows = list(go[["era", "key", "person_ref"]].itertuples(index=False, name=None))
    con.executemany(
        "INSERT OR IGNORE INTO candidate_ancestors (era, key, url, depth) VALUES (?,?,?,0)",
        rows,
    )


async def fetch_batch(urls: list[str]) -> None:
    """Fetch and persist a batch of not-yet-fetched person_pages URLs."""
    con = duckdb.connect(DB_PATH)
    bucket = TokenBucketRateLimiter(rate=RATE)
    session = make_session()
    sem = asyncio.Semaphore(CONCURRENCY)
    buf: list[tuple] = []
    done = 0

    def flush():
        nonlocal buf
        if buf:
            con.executemany(
                """INSERT OR IGNORE INTO person_pages
                   (url, person_name_full, beroep, birth_place, father_url,
                    father_name, skip)
                   VALUES (?,?,?,?,?,?,?)""",
                buf,
            )
            buf = []

    async def process(url: str):
        nonlocal done
        async with sem:
            status, html = await fetch_html(session, bucket, url)
            if status == "gone":
                buf.append((url, None, None, None, None, None, True))
            elif status == "ok":
                info = parse_person_page(html, url)
                buf.append((
                    url, info["person_name_full"], info["beroep"],
                    info["birth_place"], info["father_url"], info["father_name"],
                    False,
                ))
            else:
                return  # transient: leave unfetched for a rerun
            done += 1
            if done % FLUSH_BATCH == 0:
                flush()
                print(f"    ... {done}/{len(urls)} pages fetched")

    for start in range(0, len(urls), FLUSH_BATCH):
        batch = urls[start:start + FLUSH_BATCH]
        await asyncio.gather(*[process(u) for u in batch])
        flush()

    await session.close()
    con.close()


async def run(limit: int = 0) -> None:
    con = duckdb.connect(DB_PATH)
    init_db(con)
    seed_depth0(con)
    con.close()

    for depth in range(MAX_DEPTH + 1):
        con = duckdb.connect(DB_PATH)
        todo = con.execute(
            """
            SELECT DISTINCT ca.url FROM candidate_ancestors ca
            LEFT JOIN person_pages pp ON pp.url = ca.url
            WHERE ca.depth = ? AND pp.url IS NULL
            """,
            [depth],
        ).df()["url"].tolist()
        con.close()

        if limit:
            todo = todo[:limit]
        print(f"depth {depth}: {len(todo)} person pages to fetch")
        if todo:
            await fetch_batch(todo)

        if depth < MAX_DEPTH:
            con = duckdb.connect(DB_PATH)
            next_seeds = con.execute(
                """
                SELECT DISTINCT ca.era, ca.key, pp.father_url
                FROM candidate_ancestors ca
                JOIN person_pages pp ON pp.url = ca.url
                WHERE ca.depth = ? AND pp.father_url IS NOT NULL AND pp.skip = FALSE
                """,
                [depth],
            ).df()
            if not next_seeds.empty:
                rows = list(next_seeds.itertuples(index=False, name=None))
                con.executemany(
                    "INSERT OR IGNORE INTO candidate_ancestors (era, key, url, depth) "
                    f"VALUES (?,?,?,{depth + 1})",
                    rows,
                )
            con.close()

    con = duckdb.connect(DB_PATH, read_only=True)
    n_pages = con.execute("SELECT COUNT(*) FROM person_pages WHERE skip=FALSE").fetchone()[0]
    n_beroep = con.execute(
        "SELECT COUNT(*) FROM person_pages WHERE beroep IS NOT NULL"
    ).fetchone()[0]
    n_edges = con.execute("SELECT COUNT(*) FROM candidate_ancestors").fetchone()[0]
    print(f"Done: {n_pages} person pages fetched, {n_beroep} with a beroep string, "
          f"{n_edges} candidate_ancestors rows.")
    con.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0,
                        help="cap pages fetched per depth (smoke test)")
    args = parser.parse_args()
    asyncio.run(run(limit=args.limit))


if __name__ == "__main__":
    main()
