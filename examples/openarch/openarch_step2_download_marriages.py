# =============================================================================
# openarch_step2_download_marriages.py  [OPENARCHIEF PIPELINE - STEP 2]
# Input:  data/hdng/HDNG_v4.txt
#         data/openarchive/marriages.duckdb  (optionally pre-populated by step 1)
# Output: data/openarchive/marriages.duckdb  (search_index + marriages_raw tables)
#
# Two-phase async download pipeline:
#
#   Phase A (list):  Paginate the search endpoint per municipality to collect
#                    all record identifiers into `search_index`.
#
#   Phase B (fetch): Call the `show` endpoint for every identifier not yet in
#                    `downloaded_identifiers`, parse the structured response,
#                    and store in `marriages_raw`.
#
# Both phases are fully resumable: re-running the script picks up where it left off.
# Parquet export happens in step 3 (after religion classification).
#
# Usage:
#   uv run python code/data_wrangling/openarch/openarch_step2_download_marriages.py --phase list
#   uv run python code/data_wrangling/openarch/openarch_step2_download_marriages.py --phase fetch
#   uv run python code/data_wrangling/openarch/openarch_step2_download_marriages.py --phase all
#
# Optional flags:
#   --from-date YYYY-MM-DD   (default: 1800-01-01)
#   --until-date YYYY-MM-DD  (default: 1940-12-31)
# =============================================================================
import argparse
import asyncio
import json
import os
import sys
import time

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from openarch_async_helpers import (
    TokenBucketRateLimiter,
    clean_municipality_name,
    init_db,
    make_search_url,
    make_session,
    make_show_url,
    parse_show_response,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = "./data/openarchive/marriages.duckdb"
HDNG_PATH = "./data/hdng/HDNG_v4.txt"

RATE = 4.0        # requests per second (API limit)
CONCURRENCY = 8   # max simultaneous open connections
LIST_BATCH = 2000 # flush search_index after this many new records
FETCH_BATCH = 500 # flush marriages_raw after this many new records


# ---------------------------------------------------------------------------
# Municipality loader (same logic as step 1)
# ---------------------------------------------------------------------------

def load_municipalities(hdng_path: str) -> list[tuple[str, str]]:
    hdng = pd.read_csv(hdng_path, dtype={"amco": str})
    filtered = hdng[
        hdng["description"].str.contains("Bevolking", na=False)
        & (hdng["information"] == "totaal")
        & (hdng["year"] == 1889)
    ]
    grouped = (
        filtered
        .groupby(["amco", "name"], as_index=False)
        .agg({"value": "sum"})
    )
    idx = grouped.groupby("amco")["value"].idxmax()
    names = grouped.loc[idx, ["amco", "name"]].reset_index(drop=True)
    names["name"] = names["name"].str.lower().str.title()
    return list(zip(names["amco"], names["name"]))


def load_candidates(candidates_path: str) -> list[tuple[str, str]]:
    """Load the annotated candidates file (step 1c output).

    Returns a list of (amco, selected_name) pairs, skipping rows where
    selected_name is blank (i.e. the user chose to exclude that municipality).
    """
    df = pd.read_csv(candidates_path, dtype={"amco": str})
    if "selected_name" not in df.columns:
        raise ValueError(
            f"Candidates file {candidates_path} has no 'selected_name' column. "
            "Run openarch_step1c_generate_annotation.py first."
        )
    # Drop rows the user blanked out
    df = df[df["selected_name"].notna() & (df["selected_name"].str.strip() != "")]
    pairs = list(zip(df["amco"], df["selected_name"].str.strip()))
    print(
        f"Loaded {len(pairs):,} municipalities from candidates file "
        f"({len(df) - len(pairs):,} skipped due to blank selected_name)."
    )
    return pairs


# ---------------------------------------------------------------------------
# Phase A: List (populate search_index)
# ---------------------------------------------------------------------------

async def fetch_search_page(
    session,
    limiter: TokenBucketRateLimiter,
    sem: asyncio.Semaphore,
    place: str,
    from_date: str,
    until_date: str,
    start: int,
) -> list[dict]:
    """Fetch one page of search results; return list of doc dicts."""
    url = make_search_url(place, from_date, until_date, start=start)
    async with sem:
        await limiter.acquire()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
            return data.get("response", {}).get("docs", [])
        except Exception as e:
            print(f"  [warn] search page start={start} place={place}: {e}", flush=True)
            return []


async def paginate_municipality(
    session,
    limiter: TokenBucketRateLimiter,
    sem: asyncio.Semaphore,
    amco: str,
    name: str,
    from_date: str,
    until_date: str,
) -> tuple[str, list[dict]]:
    """Collect all search-result docs for one municipality."""
    place = clean_municipality_name(name)
    url0 = make_search_url(place, from_date, until_date, start=0)

    # First page
    async with sem:
        await limiter.acquire()
        try:
            async with session.get(url0) as resp:
                if resp.status != 200:
                    return amco, []
                data = await resp.json(content_type=None)
        except Exception as e:
            print(f"  [warn] {name}: {e}", flush=True)
            return amco, []

    number_found = data.get("response", {}).get("number_found", 0)
    docs = data.get("response", {}).get("docs", [])

    if number_found > 50:
        # Fetch remaining pages concurrently
        page_starts = list(range(50, number_found, 50))
        tasks = [
            fetch_search_page(session, limiter, sem, place, from_date, until_date, s)
            for s in page_starts
        ]
        pages = await asyncio.gather(*tasks)
        for page_docs in pages:
            docs.extend(page_docs)

    return amco, docs


def _doc_to_row(doc: dict, amco: str) -> dict | None:
    """Convert a search-result doc to a search_index row dict."""
    identifier = doc.get("identifier")
    archive_code = doc.get("archive_code")
    if not identifier or not archive_code:
        return None

    # Only include records with exactly one eventplace (unambiguous)
    eventplace = doc.get("eventplace", [])
    if not isinstance(eventplace, list) or len(eventplace) != 1:
        return None

    ed = doc.get("eventdate", {})
    if not isinstance(ed, dict):
        ed = {}

    def _int(v):
        try:
            return int(v) if v else None
        except (ValueError, TypeError):
            return None

    return {
        "identifier": identifier,
        "archive_code": archive_code,
        "amco": amco,
        "eventplace_raw": json.dumps(eventplace),
        "eventdate_year": _int(ed.get("year")),
        "eventdate_month": _int(ed.get("month")),
        "eventdate_day": _int(ed.get("day")),
        "sourcetype": doc.get("sourcetype"),
        "url": doc.get("url"),
    }


async def list_phase(
    con,
    municipalities: list[tuple[str, str]],
    from_date: str,
    until_date: str,
    progress_table: str = "list_progress",
) -> None:
    """Phase A: paginate all municipalities and populate search_index.

    progress_table controls which tracking table is used — pass
    "list_progress_candidates" when running the supplementary candidates pass
    so it does not interfere with the original list_progress state.
    """

    # Skip already-processed municipalities
    done_amcos = {
        row[0]
        for row in con.execute(f"SELECT amco FROM {progress_table}").fetchall()
    }
    todo = [(amco, name) for amco, name in municipalities if amco not in done_amcos]
    print(
        f"List phase [{progress_table}]: "
        f"{len(todo):,} municipalities to index ({len(done_amcos):,} already done)."
    )

    if not todo:
        print("Nothing to do in list phase.")
        return

    limiter = TokenBucketRateLimiter(rate=RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    buffer: list[tuple] = []
    n_muni = 0
    n_records = 0
    t0 = time.monotonic()

    async with make_session(connector_limit=CONCURRENCY) as session:
        for amco, name in todo:
            amco_result, docs = await paginate_municipality(
                session, limiter, sem, amco, name, from_date, until_date
            )
            n_muni += 1

            rows = [_doc_to_row(doc, amco_result) for doc in docs]
            rows = [r for r in rows if r is not None]

            for r in rows:
                buffer.append((
                    r["identifier"], r["archive_code"], r["amco"],
                    r["eventplace_raw"], r["eventdate_year"],
                    r["eventdate_month"], r["eventdate_day"],
                    r["sourcetype"], r["url"],
                ))

            n_records += len(rows)

            if len(buffer) >= LIST_BATCH:
                con.executemany(
                    "INSERT OR IGNORE INTO search_index VALUES (?,?,?,?,?,?,?,?,?)",
                    buffer,
                )
                buffer.clear()

            # Mark municipality as done
            con.execute(
                f"INSERT OR IGNORE INTO {progress_table} VALUES (?)", [amco_result]
            )

            elapsed = time.monotonic() - t0
            rate = n_muni / elapsed if elapsed > 0 else 0
            remaining = len(todo) - n_muni
            eta = remaining / rate / 60 if rate > 0 else float("inf")
            print(
                f"  [{n_muni:,}/{len(todo):,}] {name}: {len(rows):,} records  "
                f"(total {n_records:,})  ETA {eta:.0f} min",
                flush=True,
            )

    if buffer:
        con.executemany(
            "INSERT OR IGNORE INTO search_index VALUES (?,?,?,?,?,?,?,?,?)",
            buffer,
        )

    total_indexed = con.execute("SELECT COUNT(*) FROM search_index").fetchone()[0]
    print(f"List phase complete. search_index contains {total_indexed:,} records.")


# ---------------------------------------------------------------------------
# Phase B: Fetch (populate marriages_raw)
# ---------------------------------------------------------------------------

async def fetch_show_record(
    session,
    limiter: TokenBucketRateLimiter,
    sem: asyncio.Semaphore,
    identifier: str,
    archive_code: str,
    meta: dict,
) -> tuple[str, list[dict] | None]:
    """Fetch and parse one show-endpoint record. Returns (identifier, rows_or_None)."""
    url = make_show_url(archive_code, identifier)
    async with sem:
        await limiter.acquire()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return identifier, None
                data = await resp.json(content_type=None)
            rows = parse_show_response(data, identifier, meta)
            return identifier, rows
        except Exception as e:
            print(f"  [warn] show {identifier}: {e}", flush=True)
            return identifier, None


async def fetch_phase(con) -> None:
    """Phase B: fetch show endpoint for all pending identifiers."""

    pending = con.execute("""
        SELECT si.identifier, si.archive_code, si.amco,
               si.eventdate_year, si.eventdate_month, si.eventdate_day,
               si.eventplace_raw
        FROM search_index si
        LEFT JOIN downloaded_identifiers di ON si.identifier = di.identifier
        WHERE di.identifier IS NULL
    """).fetchall()

    total = len(pending)
    print(f"Fetch phase: {total:,} records to download.")

    if total == 0:
        print("Nothing to do in fetch phase.")
        return

    limiter = TokenBucketRateLimiter(rate=RATE)
    sem = asyncio.Semaphore(CONCURRENCY)

    rows_buffer: list[tuple] = []
    done_buffer: list[str] = []
    n_done = 0
    n_failed = 0
    t0 = time.monotonic()

    async def bounded_fetch(row):
        identifier, archive_code, amco, ey, em, ed_day, ep_raw = row
        meta = {
            "archive_code": archive_code,
            "amco": amco,
            "eventdate_year": ey,
            "eventdate_month": em,
            "eventdate_day": ed_day,
            "eventplace_raw": ep_raw,
        }
        return await fetch_show_record(session, limiter, sem, identifier, archive_code, meta)

    async with make_session(connector_limit=CONCURRENCY) as session:
        coros = [bounded_fetch(row) for row in pending]

        for coro in asyncio.as_completed(coros):
            identifier, rows = await coro
            n_done += 1

            if rows is None:
                n_failed += 1
            else:
                for r in rows:
                    rows_buffer.append((
                        r["identifier"], r["archive_code"], r["amco"],
                        r["eventdate_year"], r["eventdate_month"], r["eventdate_day"],
                        r["eventplace_raw"], r["relation_type"], r["person_pid"],
                        r["first_name"], r["prefix_last_name"], r["last_name"],
                        r["profession"],
                        r["birth_year"], r["birth_month"], r["birth_day"],
                        r["age_literal"], r["age_years"], r["source_date"],
                    ))
                done_buffer.append(identifier)

            if len(done_buffer) >= FETCH_BATCH:
                _flush(con, rows_buffer, done_buffer)
                rows_buffer.clear()
                done_buffer.clear()

                elapsed = time.monotonic() - t0
                rate = n_done / elapsed if elapsed > 0 else 0
                eta = (total - n_done) / rate / 60 if rate > 0 else float("inf")
                print(
                    f"  {n_done:,}/{total:,} ({100*n_done/total:.1f}%)  "
                    f"{rate:.1f} req/s  failed={n_failed:,}  ETA {eta:.0f} min",
                    flush=True,
                )

    # Final flush
    if rows_buffer or done_buffer:
        _flush(con, rows_buffer, done_buffer)

    fetched = con.execute("SELECT COUNT(DISTINCT identifier) FROM marriages_raw").fetchone()[0]
    person_rows = con.execute("SELECT COUNT(*) FROM marriages_raw").fetchone()[0]
    print(
        f"Fetch phase complete. {fetched:,} records fetched, "
        f"{person_rows:,} person-role rows stored, {n_failed:,} failed (will retry on re-run)."
    )



def _flush(con, rows_buffer: list[tuple], done_buffer: list[str]) -> None:
    """Flush buffers to DuckDB and checkpoint."""
    if rows_buffer:
        try:
            con.executemany(
                """INSERT INTO marriages_raw VALUES
                   (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,current_timestamp)""",
                rows_buffer,
            )
        except Exception:
            n_skipped = 0
            for row in rows_buffer:
                try:
                    con.execute(
                        """INSERT INTO marriages_raw VALUES
                           (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,current_timestamp)""",
                        row,
                    )
                except Exception as e:
                    print(f"  [warn] skipping invalid row ({row[0]}): {e}", flush=True)
                    n_skipped += 1
            if n_skipped:
                print(f"  [warn] skipped {n_skipped} invalid rows in this batch", flush=True)
    if done_buffer:
        con.executemany(
            "INSERT OR IGNORE INTO downloaded_identifiers VALUES (?, current_timestamp)",
            [(i,) for i in done_buffer],
        )
    con.execute("CHECKPOINT")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Download BS Huwelijk records from OpenArchieven")
    p.add_argument(
        "--phase",
        choices=["list", "fetch", "all"],
        default="all",
        help="Which phase(s) to run (default: all)",
    )
    p.add_argument(
        "--from-date",
        default="1811-01-01",
        help="Start date for record search, YYYY-MM-DD (default: 1811-01-01; BS records only exist from 1811)",
    )
    p.add_argument(
        "--until-date",
        default="1940-12-31",
        help="End date for record search, YYYY-MM-DD (default: 1940-12-31)",
    )
    p.add_argument(
        "--candidates-file",
        default=None,
        metavar="PATH",
        help=(
            "Path to the annotated candidates CSV (step 1c output). "
            "When provided, the list phase runs ONLY for the municipalities in "
            "this file, using a separate 'list_progress_candidates' tracking table "
            "so the original list_progress state is untouched. "
            "The fetch phase always processes all pending identifiers regardless."
        ),
    )
    return p.parse_args()


def main():
    args = parse_args()

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    print(f"Connecting to DuckDB at {DB_PATH}...")
    con = duckdb.connect(DB_PATH)
    init_db(con)

    if args.candidates_file:
        # Supplementary candidates pass — list phase only runs for the
        # annotated municipalities; original municipalities are untouched.
        if not os.path.exists(args.candidates_file):
            print(f"Error: candidates file not found: {args.candidates_file}")
            sys.exit(1)
        candidates = load_candidates(args.candidates_file)

        if args.phase in ("list", "all"):
            print(f"\n=== Phase A (candidates): List ({args.from_date} → {args.until_date}) ===")
            asyncio.run(
                list_phase(
                    con, candidates, args.from_date, args.until_date,
                    progress_table="list_progress_candidates",
                )
            )
    else:
        # Standard pass — original HDNG municipalities.
        municipalities = load_municipalities(HDNG_PATH)
        print(f"Loaded {len(municipalities):,} municipalities from HDNG.")

        if args.phase in ("list", "all"):
            print(f"\n=== Phase A: List ({args.from_date} → {args.until_date}) ===")
            asyncio.run(list_phase(con, municipalities, args.from_date, args.until_date))

    if args.phase in ("fetch", "all"):
        print("\n=== Phase B: Fetch ===")
        asyncio.run(fetch_phase(con))

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
