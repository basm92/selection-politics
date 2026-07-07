# =============================================================================
# cbs_step2_download_scans.py  [CBS SCANS PIPELINE - STEP 2]
# Input:  data/cbs/cbs.duckdb  (scan_index from step 1)
# Output: data/cbs/cbs.duckdb  (volume_ranges, scan_files tables)
#         data/cbs/scans/<year>/<filename>.jpg  (full-resolution page scans)
#
# The step-1 listing parse yields the volume year for only a fraction of the
# items (the listing HTML interleaves labels unreliably), but Atlantis assigns
# contiguous item ids per volume and every detail page carries the volume's
# first/last item id in its "collection-navigation" block. So:
#
#   1. For each year with at least one year-labelled item, fetch ONE detail
#      page, read the volume's [first_id, last_id] range, and stamp that year
#      onto every scan_index row in the range (volume_ranges table).
#   2. For every item in the target years, fetch its detail page to get the
#      image filename + media file id, then download the full-size scan via
#      HttpHandler/<filename>?file=<file_id>  (~700 KB JPEG, ~2060×2904 px).
#
# Target years: TK volumes that exist at historisch.cbs.nl and matter here —
# 1933 + 1937 (interwar candidate-level tables; CBS published no TK statistics
# for 1918-1929) and 1901-1913 (validation set for the Huygens scrape).
#
# Resumable: files already on disk (and recorded in scan_files) are skipped.
#
# Usage:
#   uv run python code/data_wrangling/cbs/cbs_step2_download_scans.py
#   uv run python code/data_wrangling/cbs/cbs_step2_download_scans.py --years 1933,1937
# =============================================================================
import argparse
import asyncio
import os
import re
import sys

import aiohttp
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter, USER_AGENT

BASE = "https://historisch.cbs.nl"
DB_PATH = "./data/cbs/cbs.duckdb"
SCAN_DIR = "./data/cbs/scans"

RATE = 2.0
TARGET_YEARS = [1901, 1905, 1909, 1913, 1933, 1937]

DDL = """
CREATE TABLE IF NOT EXISTS volume_ranges (
    year     INTEGER PRIMARY KEY,
    first_id BIGINT,
    last_id  BIGINT,
    n_items  INTEGER
);
CREATE TABLE IF NOT EXISTS scan_files (
    item_id  BIGINT PRIMARY KEY,
    year     INTEGER,
    filename TEXT,
    file_id  BIGINT,
    path     TEXT,
    bytes    BIGINT
);
"""


async def fetch_detail(session, bucket, item_id: int) -> str:
    # nav_id/index params make the server render the collection-navigation
    # block (first/last item of the volume); without them it is omitted.
    await bucket.acquire()
    async with session.get(
            f"{BASE}/detail.php",
            params={"nav_id": "0-1", "index": 3, "id": item_id}) as resp:
        resp.raise_for_status()
        return await resp.text()


def parse_detail(html: str) -> dict:
    out = {"first_id": None, "last_id": None, "filename": None, "file_id": None}
    m = re.search(r'title="Eerste resultaat"[^>]*[?&;]id=(\d+)', html)
    if m:
        out["first_id"] = int(m.group(1))
    m = re.search(r'title="Laatste resultaat"[^>]*[?&;]id=(\d+)', html)
    if m:
        out["last_id"] = int(m.group(1))
    m = re.search(r'HttpHandler/([^"?]+\.jpg)\?icoon=(\d+)', html)
    if m:
        out["filename"], out["file_id"] = m.group(1), int(m.group(2))
    return out


async def resolve_ranges(con, session, bucket, years: list[int]) -> None:
    known = {r[0] for r in con.execute("SELECT year FROM volume_ranges").fetchall()}
    for year in years:
        if year in known:
            continue
        row = con.execute(
            "SELECT item_id FROM scan_index WHERE year = ? "
            "ORDER BY (image_seq IS NULL), image_seq LIMIT 1", [year]
        ).fetchone()
        if row is None:
            print(f"  {year}: no labelled item in scan_index, cannot resolve range")
            continue
        d = parse_detail(await fetch_detail(session, bucket, row[0]))
        # The viewer omits the Eerste/Laatste link when the current item IS the
        # first/last of the volume, so fall back to the queried item itself.
        d["first_id"] = d["first_id"] or row[0]
        d["last_id"] = d["last_id"] or row[0]
        if d["last_id"] < d["first_id"]:
            print(f"  {year}: nonsensical range from item {row[0]}, skipped")
            continue
        n = d["last_id"] - d["first_id"] + 1
        con.execute("INSERT OR REPLACE INTO volume_ranges VALUES (?,?,?,?)",
                    [year, d["first_id"], d["last_id"], n])
        con.execute(
            "UPDATE scan_index SET year = ? WHERE item_id BETWEEN ? AND ?",
            [year, d["first_id"], d["last_id"]])
        print(f"  {year}: items {d['first_id']}..{d['last_id']} ({n} pages)")


async def download_year(con, session, bucket, year: int) -> None:
    rng = con.execute(
        "SELECT first_id, last_id FROM volume_ranges WHERE year = ?", [year]
    ).fetchone()
    if rng is None:
        return
    have = {r[0] for r in con.execute(
        "SELECT item_id FROM scan_files WHERE path IS NOT NULL").fetchall()}
    todo = [i for i in range(rng[0], rng[1] + 1) if i not in have]
    if not todo:
        print(f"  {year}: complete")
        return
    outdir = os.path.join(SCAN_DIR, str(year))
    os.makedirs(outdir, exist_ok=True)
    print(f"  {year}: {len(todo)} scans to download")
    for item_id in todo:
        try:
            d = parse_detail(await fetch_detail(session, bucket, item_id))
            if not (d["filename"] and d["file_id"]):
                print(f"    {item_id}: no media reference, skipped")
                continue
            path = os.path.join(outdir, d["filename"])
            if not os.path.exists(path) or os.path.getsize(path) < 10_000:
                await bucket.acquire()
                async with session.get(
                        f"{BASE}/HttpHandler/{d['filename']}",
                        params={"file": d["file_id"]}) as resp:
                    resp.raise_for_status()
                    data = await resp.read()
                if len(data) < 10_000:
                    print(f"    {item_id}: empty response for file={d['file_id']}")
                    continue
                with open(path, "wb") as f:
                    f.write(data)
            con.execute(
                "INSERT OR REPLACE INTO scan_files VALUES (?,?,?,?,?,?)",
                [item_id, year, d["filename"], d["file_id"], path,
                 os.path.getsize(path)])
        except Exception as e:
            print(f"    {item_id}: FAIL {e}")


async def main(years: list[int]) -> None:
    con = duckdb.connect(DB_PATH)
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)

    bucket = TokenBucketRateLimiter(RATE)
    timeout = aiohttp.ClientTimeout(total=180, connect=15)
    async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT}, timeout=timeout) as session:
        print("Resolving volume ranges:")
        await resolve_ranges(con, session, bucket, years)
        print("Downloading scans:")
        for year in years:
            await download_year(con, session, bucket, year)

    print(con.execute(
        "SELECT year, COUNT(*), SUM(bytes)//1048576 AS mb FROM scan_files "
        "GROUP BY year ORDER BY year").fetchall())
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=str, default=",".join(map(str, TARGET_YEARS)))
    args = ap.parse_args()
    asyncio.run(main([int(y) for y in args.years.split(",")]))
