# =============================================================================
# cbs_step1_index_scans.py  [CBS SCANS PIPELINE - STEP 1]
# Input:  live site historisch.cbs.nl, collection
#         "STATISTIEK DER VERKIEZINGEN / TWEEDE KAMER" (2,618 page images)
# Output: data/cbs/cbs.duckdb  (scan_index + page_progress tables)
#
# Enumerate every page image in the Tweede Kamer election-statistics
# collection by paginating the browse listing. The listing is session-stateful
# (nav_id lives in a cookie session), so one aiohttp session with a cookie jar
# is reused across pages, fetched sequentially at a gentle rate — the server's
# own search times out under load ("De zoekvraag duurde te lang").
#
# Each listing entry yields: item_id (xml-beschrijving id), title
# ("Statistiek der verkiezingen YYYY-YYYY : N"), volume label with page count,
# and image sequence number. Year is parsed from the title. Full filenames and
# media file ids live on the detail pages and are collected in step 2.
#
# Resumable: listing pages already in `page_progress` are skipped on rerun.
#
# Usage:
#   uv run python code/data_wrangling/cbs/cbs_step1_index_scans.py
# =============================================================================
import asyncio
import os
import re
import sys

import aiohttp
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter, USER_AGENT

BASE = "https://historisch.cbs.nl"
COLLECTION_URL = f"{BASE}/STATISTIEK%20DER%20VERKIEZINGEN/TWEEDE%20KAMER"
DB_PATH = "./data/cbs/cbs.duckdb"

RATE = 1.0          # req/sec — the CBS search backend is fragile
RESULTS_PER_PAGE = 14

DDL = """
CREATE TABLE IF NOT EXISTS page_progress (
    page INTEGER PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS scan_index (
    item_id   BIGINT PRIMARY KEY,
    title     TEXT,
    volume    TEXT,
    year      INTEGER,
    image_seq INTEGER
);
"""


def parse_listing(html: str) -> list[dict]:
    """
    Pull (item_id, title, volume, year, image_seq) tuples out of one listing
    page. Entries look like:
      Bekijk detail van "Statistiek der verkiezingen 1937-1937 : 21"
      ... HttpHandler/icoon.ico?icoonfromxmlbeschr=396168722 ...
      ... Statistiek der verkiezingen Tweede Kamer 1937 (68 p.) image 21 ...
    """
    entries = []
    # Split on the per-result icon handler; each chunk before it has the title.
    blocks = re.split(r'icoonfromxmlbeschr=(\d+)', html)
    # blocks: [pre, id1, mid1, id2, mid2, ...]; title for idN sits in the text
    # before it, volume/image info after it.
    for i in range(1, len(blocks) - 1, 2):
        item_id = int(blocks[i])
        before = blocks[i - 1]
        after = blocks[i + 1]
        tm = re.findall(
            r'Bekijk detail van "Statistiek der verkiezingen ([^"]+)"', before)
        title = tm[-1] if tm else None
        vm = re.search(
            r'(Statistiek der verkiezingen[^<]*?\(\d+ p\.\))[^<]*?image\s+(\d+)',
            after)
        volume = vm.group(1).strip() if vm else None
        image_seq = int(vm.group(2)) if vm else None
        year = None
        if title:
            ym = re.search(r'(\d{4})-\d{4}', title)
            year = int(ym.group(1)) if ym else None
        entries.append({
            "item_id": item_id, "title": title, "volume": volume,
            "year": year, "image_seq": image_seq,
        })
    return entries


async def main() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)

    done = {r[0] for r in con.execute("SELECT page FROM page_progress").fetchall()}
    bucket = TokenBucketRateLimiter(RATE)
    timeout = aiohttp.ClientTimeout(total=120, connect=15)
    jar = aiohttp.CookieJar()
    async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT}, timeout=timeout,
            cookie_jar=jar) as session:
        # First request establishes the session and reveals the total count.
        await bucket.acquire()
        async with session.get(COLLECTION_URL) as resp:
            resp.raise_for_status()
            html = await resp.text()
        m = re.search(r"([\d\.]+) Resultaten", html)
        total = int(m.group(1).replace(".", "")) if m else 0
        n_pages = -(-total // RESULTS_PER_PAGE)
        print(f"Step 1: {total} images across {n_pages} listing pages "
              f"({len(done)} pages already done)")

        if 1 not in done:
            entries = parse_listing(html)
            store(con, 1, entries)
            print(f"  page 1: {len(entries)} entries")

        for page in range(2, n_pages + 1):
            if page in done:
                continue
            for attempt in range(3):
                await bucket.acquire()
                async with session.get(
                        COLLECTION_URL, params={"nav_id": "0-0", "page": page}) as resp:
                    resp.raise_for_status()
                    html = await resp.text()
                if "duurde te lang" not in html:
                    break
                await asyncio.sleep(10 * (attempt + 1))
            entries = parse_listing(html)
            if not entries:
                print(f"  page {page}: EMPTY (server timeout?) — will retry on rerun")
                continue
            store(con, page, entries)
            if page % 10 == 0:
                n = con.execute("SELECT COUNT(*) FROM scan_index").fetchone()[0]
                print(f"  page {page}/{n_pages}: index at {n} items")

    n = con.execute("SELECT COUNT(*) FROM scan_index").fetchone()[0]
    print(f"Done. scan_index holds {n} items.")
    print(con.execute(
        "SELECT year, COUNT(*) FROM scan_index GROUP BY year ORDER BY year"
    ).fetchall())
    con.close()


def store(con, page: int, entries: list[dict]) -> None:
    con.executemany(
        "INSERT OR REPLACE INTO scan_index VALUES (?,?,?,?,?)",
        [(e["item_id"], e["title"], e["volume"], e["year"], e["image_seq"])
         for e in entries],
    )
    con.execute("INSERT OR IGNORE INTO page_progress VALUES (?)", [page])


if __name__ == "__main__":
    asyncio.run(main())
