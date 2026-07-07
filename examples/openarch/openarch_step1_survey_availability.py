# =============================================================================
# openarch_step1_survey_availability.py  [OPENARCHIEF PIPELINE - STEP 1]
# Input:  data/hdng/HDNG_v4.txt  (municipality names)
# Output: data/openarchive/marriages.duckdb  (survey_progress table)
#         data/openarchive/survey_availability.parquet
#
# For each Dutch municipality × decade (1800–1939), queries the OpenArchieven
# API search endpoint (page 0 only, reads `number_found`) to build a coverage
# map of available BS Huwelijk records.
#
# ~12,000–15,000 requests total; ~50–60 min at 4 req/sec.
# Fully resumable: already-queried (amco, decade) pairs are skipped.
#
# Run:
#   uv run python code/data_wrangling/openarch/openarch_step1_survey_availability.py
# =============================================================================
import asyncio
import os
import sys
import json
import time
import pandas as pd
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from openarch_async_helpers import (
    TokenBucketRateLimiter,
    make_session,
    clean_municipality_name,
    make_search_url,
    init_db,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = "./data/openarchive/marriages.duckdb"
PARQUET_PATH = "./data/openarchive/survey_availability.parquet"
HDNG_PATH = "./data/hdng/HDNG_v4.txt"

# Decades to survey: (from_date, until_date) pairs.
# BS civil registration started in 1811; pre-1811 decades will return 0 records
# but are included so the survey clearly shows the data floor.
DECADES = [
    (f"{y}-01-01", f"{y+9}-12-31")
    for y in range(1800, 1940, 10)
]

RATE = 4.0       # requests per second (API limit)
CONCURRENCY = 8  # max simultaneous open connections
BATCH_SIZE = 200 # flush to DuckDB after this many results


# ---------------------------------------------------------------------------
# Municipality loader
# ---------------------------------------------------------------------------

def load_municipalities(hdng_path: str) -> list[tuple[str, str]]:
    """
    Return list of (amco, canonical_name) from HDNG data.
    Uses 1889 total population records to pick the most-common name per amco.
    """
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
    # Title-case for API compatibility
    names["name"] = names["name"].str.lower().str.title()
    return list(zip(names["amco"], names["name"]))


# ---------------------------------------------------------------------------
# Async survey logic
# ---------------------------------------------------------------------------

async def fetch_count(
    session: "aiohttp.ClientSession",
    limiter: TokenBucketRateLimiter,
    sem: asyncio.Semaphore,
    amco: str,
    name: str,
    from_date: str,
    until_date: str,
) -> tuple[str, str, str, str, int]:
    """
    Fetch page 0 of a search query and return (amco, name, from_date, until_date, number_found).
    Returns 0 for number_found on any error.
    """
    place = clean_municipality_name(name)
    url = make_search_url(place, from_date, until_date, start=0)

    async with sem:
        await limiter.acquire()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return amco, name, from_date, until_date, 0
                data = await resp.json(content_type=None)
            number_found = data.get("response", {}).get("number_found", 0)
            return amco, name, from_date, until_date, int(number_found)
        except Exception as e:
            print(f"  [warn] {name} {from_date[:4]}: {e}", flush=True)
            return amco, name, from_date, until_date, 0


async def run_survey(con, municipalities: list[tuple[str, str]]) -> None:
    """Run the full survey, skipping already-done (amco, from_date) pairs."""

    # Load already-completed pairs
    done = set(
        con.execute("SELECT amco, from_date FROM survey_progress").fetchall()
    )
    print(f"Already surveyed: {len(done):,} (amco, decade) pairs.")

    # Build task list
    tasks_args = [
        (amco, name, fd, ud)
        for amco, name in municipalities
        for fd, ud in DECADES
        if (amco, fd) not in done
    ]
    total = len(tasks_args)
    print(f"Remaining: {total:,} queries across {len(municipalities):,} municipalities × {len(DECADES)} decades.")

    if total == 0:
        print("Nothing to do.")
        return

    limiter = TokenBucketRateLimiter(rate=RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    buffer: list[tuple] = []
    n_done = 0
    t0 = time.monotonic()

    async with make_session(connector_limit=CONCURRENCY) as session:
        coros = [
            fetch_count(session, limiter, sem, amco, name, fd, ud)
            for amco, name, fd, ud in tasks_args
        ]

        for coro in asyncio.as_completed(coros):
            amco, name, from_date, until_date, number_found = await coro
            buffer.append((amco, name, from_date, until_date, number_found))
            n_done += 1

            if len(buffer) >= BATCH_SIZE:
                con.executemany(
                    "INSERT OR IGNORE INTO survey_progress VALUES (?, ?, ?, ?, ?)",
                    buffer,
                )
                buffer.clear()
                elapsed = time.monotonic() - t0
                rate = n_done / elapsed
                eta = (total - n_done) / rate / 60 if rate > 0 else float("inf")
                print(
                    f"  {n_done:,}/{total:,} ({100*n_done/total:.1f}%)  "
                    f"{rate:.1f} req/s  ETA {eta:.0f} min",
                    flush=True,
                )

    # Final flush
    if buffer:
        con.executemany(
            "INSERT OR IGNORE INTO survey_progress VALUES (?, ?, ?, ?, ?)",
            buffer,
        )
    print(f"Survey complete: {n_done:,} queries processed.")


# ---------------------------------------------------------------------------
# Output: parquet + console summary
# ---------------------------------------------------------------------------

def export_results(con) -> None:
    """Write survey_availability.parquet and print a pivot table."""
    os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)

    # Export parquet
    con.execute(f"""
        COPY (
            SELECT amco, name,
                   CAST(LEFT(from_date, 4) AS INTEGER) AS decade_start,
                   until_date,
                   number_found
            FROM survey_progress
            ORDER BY amco, from_date
        ) TO '{PARQUET_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print(f"\nParquet written to {PARQUET_PATH}")

    # Pivot for console output
    pivot = con.execute("""
        SELECT
            amco,
            name,
            SUM(number_found) AS total,
            SUM(CASE WHEN LEFT(from_date,4)='1800' THEN number_found ELSE 0 END) AS "1800s",
            SUM(CASE WHEN LEFT(from_date,4)='1810' THEN number_found ELSE 0 END) AS "1810s",
            SUM(CASE WHEN LEFT(from_date,4)='1820' THEN number_found ELSE 0 END) AS "1820s",
            SUM(CASE WHEN LEFT(from_date,4)='1830' THEN number_found ELSE 0 END) AS "1830s",
            SUM(CASE WHEN LEFT(from_date,4)='1840' THEN number_found ELSE 0 END) AS "1840s",
            SUM(CASE WHEN LEFT(from_date,4)='1850' THEN number_found ELSE 0 END) AS "1850s",
            SUM(CASE WHEN LEFT(from_date,4)='1860' THEN number_found ELSE 0 END) AS "1860s",
            SUM(CASE WHEN LEFT(from_date,4)='1870' THEN number_found ELSE 0 END) AS "1870s",
            SUM(CASE WHEN LEFT(from_date,4)='1880' THEN number_found ELSE 0 END) AS "1880s",
            SUM(CASE WHEN LEFT(from_date,4)='1890' THEN number_found ELSE 0 END) AS "1890s",
            SUM(CASE WHEN LEFT(from_date,4)='1900' THEN number_found ELSE 0 END) AS "1900s",
            SUM(CASE WHEN LEFT(from_date,4)='1910' THEN number_found ELSE 0 END) AS "1910s",
            SUM(CASE WHEN LEFT(from_date,4)='1920' THEN number_found ELSE 0 END) AS "1920s",
            SUM(CASE WHEN LEFT(from_date,4)='1930' THEN number_found ELSE 0 END) AS "1930s"
        FROM survey_progress
        GROUP BY amco, name
        ORDER BY total DESC
    """).df()

    grand_total = pivot["total"].sum()
    print(f"\nTotal records found across all municipalities and decades: {grand_total:,}\n")
    print("Top 50 municipalities by total available records:")
    print(pivot.head(50).to_string(index=False))

    nonzero = pivot[pivot["total"] > 0]
    zero = pivot[pivot["total"] == 0]
    print(f"\n{len(nonzero):,} / {len(pivot):,} municipalities have at least one record.")
    if not zero.empty:
        print(f"\n{len(zero):,} municipalities returned 0 records across all decades.")
        print("These likely have name mismatches with the API's eventplace index:")
        print(zero[["amco", "name"]].to_string(index=False))
        # Also write a CSV of zero-result municipalities for manual inspection
        zero_path = "./data/openarchive/survey_zero_results.csv"
        zero[["amco", "name"]].to_csv(zero_path, index=False)
        print(f"  (saved to {zero_path})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    print("Loading municipality list from HDNG...")
    municipalities = load_municipalities(HDNG_PATH)
    print(f"  {len(municipalities):,} municipalities found.")

    print(f"Connecting to DuckDB at {DB_PATH}...")
    con = duckdb.connect(DB_PATH)
    init_db(con)

    print("Starting availability survey...")
    asyncio.run(run_survey(con, municipalities))

    print("Exporting results...")
    export_results(con)

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
