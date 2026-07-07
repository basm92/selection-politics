# =============================================================================
# nlgis_step1_download_maps.py  [NLGIS PIPELINE - STEP 1]
# Input:  live API https://nlgis.nl/api/maps?year=YYYY
# Output: data/nlgis/maps/YYYY.topojson  (one full-country TopoJSON per year)
#
# Download the municipality boundary TopoJSON for every year 1848-1940.
# Query by year only: the API's `province` parameter returns an empty body
# (verified in Phase 0), and the full-country file is small (~400 KB).
#
# Resumable: a year is skipped when its file already exists and parses as
# TopoJSON with at least one geometry.
#
# Usage:
#   uv run python code/data_wrangling/nlgis/nlgis_step1_download_maps.py
# =============================================================================
import asyncio
import json
import os
import sys

import aiohttp

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter, USER_AGENT

API_URL = "https://nlgis.nl/api/maps?year={year}"
OUT_DIR = "./data/nlgis/maps"

FIRST_YEAR = 1848
LAST_YEAR = 1940
RATE = 3.0


def have_valid(path: str) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) < 10_000:
        return False
    try:
        with open(path) as f:
            d = json.load(f)
        return bool(d["objects"]["nld"]["geometries"])
    except Exception:
        return False


async def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    todo = [y for y in range(FIRST_YEAR, LAST_YEAR + 1)
            if not have_valid(os.path.join(OUT_DIR, f"{y}.topojson"))]
    print(f"Step 1: {len(todo)} year maps to download")

    bucket = TokenBucketRateLimiter(RATE)
    timeout = aiohttp.ClientTimeout(total=120, connect=15)
    async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT}, timeout=timeout) as session:
        for year in todo:
            await bucket.acquire()
            async with session.get(API_URL.format(year=year)) as resp:
                resp.raise_for_status()
                text = await resp.text()
            d = json.loads(text)
            n = len(d["objects"]["nld"]["geometries"])
            path = os.path.join(OUT_DIR, f"{year}.topojson")
            with open(path, "w") as f:
                f.write(text)
            print(f"  {year}: {n} municipalities")
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
