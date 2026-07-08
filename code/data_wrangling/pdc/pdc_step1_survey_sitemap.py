# =============================================================================
# pdc_step1_survey_sitemap.py  [PDC PIPELINE - STEP 1]
# Input:  https://www.parlement.com/sitemap.xml  (Drupal simple_sitemap, paged
#         via ?page=N; N discovered from the sitemap index)
# Output: data/pdc/pdc.duckdb
#           biografie_urls(url PRIMARY KEY, slug, discovered_at)
#
# Method: Parlement.com (formerly PDC, Parlementair Documentatie Centrum) has
# no dedicated "Tweede Kamerleden 1848-heden" index page (checked: a guessed
# URL 404s). Person biographies instead live at flat slugs /biografie/<slug>
# and are only discoverable via the sitemap. This step harvests every
# /biografie/ URL from the sitemap (~5,891 across ~5,000+ people sitewide,
# per phase_2_and_onward.md's estimate) into a resumable index; step 2 then
# fetches + parses each one and step 3 filters to Tweede Kamer members
# active 1848-1940.
#
# Usage:
#   uv run python code/data_wrangling/pdc/pdc_step1_survey_sitemap.py
# =============================================================================
import asyncio
import os
import re
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter, make_session

SITEMAP_INDEX = "https://www.parlement.com/sitemap.xml"

OUT_DIR = "./data/pdc"
DB_PATH = os.path.join(OUT_DIR, "pdc.duckdb")

RATE = 2.0  # requests per second (small nonprofit server -- be polite)

DDL = """
CREATE TABLE IF NOT EXISTS biografie_urls (
    url           VARCHAR PRIMARY KEY,
    slug          VARCHAR,
    discovered_at TIMESTAMP DEFAULT current_timestamp
);
"""


async def fetch(session, limiter, url: str) -> str:
    await limiter.acquire()
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.text()


async def run() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    con.execute(DDL)

    limiter = TokenBucketRateLimiter(rate=RATE)
    async with make_session() as session:
        xml = await fetch(session, limiter, SITEMAP_INDEX)
        pages = re.findall(r"<loc>([^<]+)</loc>", xml)
        print(f"sitemap index: {len(pages)} paged sitemaps")

        found = 0
        for page_url in pages:
            xml = await fetch(session, limiter, page_url)
            locs = re.findall(r"<loc>([^<]+)</loc>", xml)
            rows = []
            for loc in locs:
                m = re.match(r"https://www\.parlement\.com/biografie/([^/?#]+)$", loc)
                if m:
                    rows.append((loc, m.group(1)))
            if rows:
                con.executemany(
                    "INSERT INTO biografie_urls VALUES (?, ?, current_timestamp) "
                    "ON CONFLICT (url) DO NOTHING",
                    rows,
                )
            found += len(rows)
            print(f"  {page_url}: {len(locs)} urls, {len(rows)} biografie/")

    total = con.execute("SELECT COUNT(*) FROM biografie_urls").fetchone()[0]
    print(f"\ntotal biografie URLs discovered this run: {found}")
    print(f"total biografie URLs in index: {total}")
    con.close()


if __name__ == "__main__":
    asyncio.run(run())
