# =============================================================================
# aieeda_step1_ingest.py  [AIEEDA PIPELINE - STEP 1]
# Input:  OSF project qs3dg (Archive of Interwar Europe Election Data &
#         Assemblies), zip asset AIEEDA-zip-archive-250314.zip
# Output: data/aieeda/AIEEDA-zip-archive-250314.zip   (kept for provenance)
#         data/aieeda/aieeda.duckdb
#           nl_municipal_party   — municipality × party votes, TK 1922-1937
#           elections_national   — AIEEDA national elections table (all countries)
#           parties              — AIEEDA party attribute table (all countries)
#
# Method: download the published zip once (skipped if already on disk, so the
# step is resumable/idempotent), read the Netherlands subnational CSV and the
# two national tables straight from the archive, and load them into DuckDB
# with a provenance column. No scraping: AIEEDA is a finished academic dataset
# (DOI 10.17605/OSF.IO/QS3DG, v1 2025-03-14).
#
# Usage:
#   uv run python code/data_wrangling/aieeda/aieeda_step1_ingest.py
# =============================================================================
import io
import os
import urllib.request
import zipfile

import duckdb
import pandas as pd

ZIP_URL = "https://osf.io/download/wghua/"
DATA_DIR = "./data/aieeda"
ZIP_PATH = os.path.join(DATA_DIR, "AIEEDA-zip-archive-250314.zip")
DB_PATH = os.path.join(DATA_DIR, "aieeda.duckdb")

NL_CSV = "data/NL/AIEEDA-Netherlands-subnat-v1.csv"
ELECTIONS_CSV = "data/AIEEDA-elections-v1.csv"
PARTIES_CSV = "data/AIEEDA-parties-v1.csv"

PROVENANCE = "AIEEDA v1 (OSF qs3dg, 2025-03-14)"


def download_zip() -> None:
    if os.path.exists(ZIP_PATH) and os.path.getsize(ZIP_PATH) > 1_000_000:
        print(f"zip already present: {ZIP_PATH}")
        return
    print(f"downloading {ZIP_URL} ...")
    req = urllib.request.Request(ZIP_URL, headers={"User-Agent": "selection-politics-research/0.1"})
    with urllib.request.urlopen(req, timeout=600) as resp, open(ZIP_PATH, "wb") as f:
        f.write(resp.read())
    print(f"saved {os.path.getsize(ZIP_PATH):,} bytes")


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    download_zip()

    zf = zipfile.ZipFile(ZIP_PATH)

    nl = pd.read_csv(io.BytesIO(zf.read(NL_CSV)))
    # AIEEDA writes literal "NA" strings; let pandas coerce numerics itself
    nl["votes"] = pd.to_numeric(nl["votes"], errors="coerce")
    nl["seats"] = pd.to_numeric(nl["seats"], errors="coerce")
    nl["provenance"] = PROVENANCE

    elections = pd.read_csv(io.BytesIO(zf.read(ELECTIONS_CSV)))
    parties = pd.read_csv(io.BytesIO(zf.read(PARTIES_CSV)))

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE OR REPLACE TABLE nl_municipal_party AS SELECT * FROM nl")
    con.execute("CREATE OR REPLACE TABLE elections_national AS SELECT * FROM elections")
    con.execute("CREATE OR REPLACE TABLE parties AS SELECT * FROM parties")

    n, = con.execute("SELECT COUNT(*) FROM nl_municipal_party").fetchone()
    dates = con.execute(
        "SELECT election_date, COUNT(*) , COUNT(DISTINCT unit_name) "
        "FROM nl_municipal_party GROUP BY 1 ORDER BY 1").fetchall()
    print(f"nl_municipal_party: {n:,} rows")
    for d, rows, munis in dates:
        print(f"  {d}: {rows:,} rows, {munis} municipalities")
    con.close()


if __name__ == "__main__":
    main()
