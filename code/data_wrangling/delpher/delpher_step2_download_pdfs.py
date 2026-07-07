# =============================================================================
# delpher_step2_download_pdfs.py  [DELPHER PIPELINE - STEP 2]
# Input:  data/delpher/delpher.duckdb  (articles from step 1)
# Output: data/delpher/staatscourant/<election_year>/<issue>.pdf
#         data/delpher/delpher.duckdb  (issue_pdfs table)
#
# Download the complete scanned issue PDF for every distinct Staatscourant
# issue surfaced by the step-1 survey (resolver.kb.nl ?urn=<issue>:pdf).
# Whole issues, not article crops: the official candidate lists and
# proces-verbaal tables span multiple pages and the article-level OCR
# segmentation is unreliable — the PDFs are the archival ground truth that a
# later, better OCR pass will re-read (per project decision 2026-07-07).
#
# Resumable: issues with a valid PDF on disk (recorded in issue_pdfs) are
# skipped on rerun.
#
# Usage:
#   uv run python code/data_wrangling/delpher/delpher_step2_download_pdfs.py
# =============================================================================
import asyncio
import os
import sys

import aiohttp
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter, USER_AGENT

RESOLVER = "https://resolver.kb.nl/resolve"
DB_PATH = "./data/delpher/delpher.duckdb"
PDF_DIR = "./data/delpher/staatscourant"

RATE = 0.5   # issue PDFs are ~5-10 MB; stay well below KB's comfort threshold

DDL = """
CREATE TABLE IF NOT EXISTS issue_pdfs (
    issue_urn     TEXT PRIMARY KEY,
    election_year INTEGER,
    path          TEXT,
    bytes         BIGINT
);
"""


async def main() -> None:
    con = duckdb.connect(DB_PATH)
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)

    todo = con.execute("""
        SELECT issue_urn, MIN(election_year) AS year, MIN(date) AS date
        FROM articles
        WHERE issue_urn NOT IN (SELECT issue_urn FROM issue_pdfs)
        GROUP BY issue_urn ORDER BY year, date
    """).fetchall()
    print(f"Step 2: {len(todo)} issue PDFs to download")

    bucket = TokenBucketRateLimiter(RATE)
    timeout = aiohttp.ClientTimeout(total=600, connect=20)
    n_ok = 0
    async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT}, timeout=timeout) as session:
        for issue_urn, year, date in todo:
            outdir = os.path.join(PDF_DIR, str(year))
            os.makedirs(outdir, exist_ok=True)
            fname = issue_urn.replace(":", "_") + ".pdf"
            path = os.path.join(outdir, fname)
            try:
                if not (os.path.exists(path) and os.path.getsize(path) > 50_000):
                    await bucket.acquire()
                    async with session.get(
                            RESOLVER, params={"urn": f"{issue_urn}:pdf"}) as resp:
                        if resp.status != 200:
                            print(f"  {issue_urn}: HTTP {resp.status}")
                            continue
                        data = await resp.read()
                    if not data.startswith(b"%PDF"):
                        print(f"  {issue_urn}: not a PDF ({len(data)} bytes)")
                        continue
                    with open(path, "wb") as f:
                        f.write(data)
                con.execute("INSERT OR REPLACE INTO issue_pdfs VALUES (?,?,?,?)",
                            [issue_urn, year, path, os.path.getsize(path)])
                n_ok += 1
                if n_ok % 20 == 0:
                    print(f"  {n_ok}/{len(todo)} done")
            except Exception as e:
                print(f"  {issue_urn}: FAIL {e}")

    print(con.execute(
        "SELECT election_year, COUNT(*), SUM(bytes)//1048576 AS mb "
        "FROM issue_pdfs GROUP BY 1 ORDER BY 1").fetchall())
    con.close()


if __name__ == "__main__":
    asyncio.run(main())
