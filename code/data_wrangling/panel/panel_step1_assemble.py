# =============================================================================
# panel_step1_assemble.py  [PANEL PIPELINE - STEP 1]
# Input:  data/huygens/huygens.duckdb      (elections + candidates_raw)
#         data/aieeda/aieeda.duckdb        (nl_municipal_party)
#         data/nlgis/crosswalk.duckdb      (municipality_years)
# Output: data/panel/panel.duckdb
#           candidates_panel      — candidate × district-election, 1848-1918,
#                                   with derived `elected` + provenance
#           elections             — election-level covariates (copy)
#           persons               — one row per Huygens persoon_id
#           municipal_party_panel — municipality × party × election 1922-1937
#                                   (party-level; candidate-level post-1917
#                                   awaits Staatscourant/CBS transcription)
#         data/panel/*.parquet  (one per table)
#
# Elected derivation (district era):
#   - runoff rounds (`herstemming`): the top-`zetels` candidates by votes win.
#   - single-candidate `*/enkelvoudig` events: elected unopposed.
#   - all other first rounds: elected iff votes >= kiesdrempel (absolute
#     majority), capped at the top-`zetels` candidates; remaining seats went
#     to a runoff which appears as its own uitslag row.
#   The flag is per-round; a person losing round 1 but winning the runoff is
#   `elected` on the runoff row (aggregate per contest downstream as needed).
#
# Name parsing: leading noble/academic titles (mr., dr., jhr., baron, ...)
# are split into `titles`; the remainder becomes `name_clean` for Phase 2
# entity resolution. Titles are kept — they proxy for law degrees/nobility.
#
# Usage:
#   uv run python code/data_wrangling/panel/panel_step1_assemble.py
# =============================================================================
import os
import re

import duckdb

HUYGENS_DB = "./data/huygens/huygens.duckdb"
AIEEDA_DB = "./data/aieeda/aieeda.duckdb"
NLGIS_DB = "./data/nlgis/crosswalk.duckdb"
OUT_DIR = "./data/panel"
DB_PATH = os.path.join(OUT_DIR, "panel.duckdb")

# Leading title tokens seen in Huygens name strings (order-insensitive,
# repeatable: "jhr.mr.", "dr. H.J.A.M.", "baron van ..." keeps 'van' in name).
TITLE_TOKENS = {
    "mr", "dr", "jhr", "ir", "prof", "ds", "mgr", "baron", "graaf", "ridder",
    "jkvr", "kapt", "gen", "lt", "kol", "majoor",
}


def split_titles(name_raw: str | None) -> tuple[str | None, str | None]:
    """'jhr.mr. O.Q.J.J. van Swinderen' -> ('jhr|mr', 'O.Q.J.J. van Swinderen')"""
    if not name_raw:
        return None, None
    s = name_raw.strip()
    titles = []
    while True:
        m = re.match(r"^([A-Za-z]+)\.\s*", s)
        if m and m.group(1).lower() in TITLE_TOKENS:
            titles.append(m.group(1).lower())
            s = s[m.end():]
            continue
        m2 = re.match(r"^(baron|graaf|ridder)\s+", s, re.IGNORECASE)
        if m2:
            titles.append(m2.group(1).lower())
            s = s[m2.end():]
            continue
        break
    return ("|".join(titles) or None), (s.strip() or None)


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    con.execute(f"ATTACH '{HUYGENS_DB}' AS hg (READ_ONLY)")
    con.execute(f"ATTACH '{AIEEDA_DB}' AS ai (READ_ONLY)")
    con.execute(f"ATTACH '{NLGIS_DB}' AS nl (READ_ONLY)")

    # --- elections (parse date once) ---------------------------------------
    con.execute("""
        CREATE OR REPLACE TABLE elections AS
        SELECT uitslag_id, district, district_id, type,
               strptime(date_raw, '%d/%m/%Y')::DATE AS election_date,
               CAST(split_part(date_raw, '/', 3) AS INT) AS year,
               electoraat, opkomst, stembriefjes, geldig, blanco,
               zetels, kiesdrempel
        FROM hg.elections
    """)

    # --- candidate rows with elected flag ----------------------------------
    con.execute("""
        CREATE OR REPLACE TABLE candidates_panel AS
        WITH ranked AS (
            SELECT c.*, e.type, e.election_date, e.year, e.district,
                   e.district_id, e.zetels, e.kiesdrempel,
                   ROW_NUMBER() OVER (
                       PARTITION BY c.uitslag_id
                       ORDER BY c.votes DESC NULLS LAST, c.rank
                   ) AS vote_order,
                   COUNT(*) OVER (PARTITION BY c.uitslag_id) AS n_candidates
            FROM hg.candidates_raw c
            JOIN elections e USING (uitslag_id)
        )
        SELECT
            uitslag_id, persoon_id, name_raw, affiliation, votes, pct,
            rank, vote_order, n_candidates,
            district, district_id, type, election_date, year,
            zetels, kiesdrempel,
            CASE
                WHEN type = 'herstemming'
                    THEN vote_order <= COALESCE(zetels, 1)
                WHEN type LIKE '%enkelvoudig%' AND n_candidates = 1
                    THEN TRUE
                ELSE votes >= kiesdrempel
                     AND vote_order <= COALESCE(zetels, 1)
            END AS elected,
            'huygens_verkiezingentweedekamer' AS provenance
        FROM ranked
    """)

    # --- title/name split (small table, do it in Python) -------------------
    rows = con.execute(
        "SELECT DISTINCT name_raw FROM candidates_panel").fetchall()
    con.execute("""
        CREATE OR REPLACE TABLE name_split (
            name_raw TEXT PRIMARY KEY, titles TEXT, name_clean TEXT)
    """)
    con.executemany(
        "INSERT INTO name_split VALUES (?,?,?)",
        [(nr, *split_titles(nr)) for (nr,) in rows if nr],
    )
    con.execute("""
        CREATE OR REPLACE TABLE candidates_panel AS
        SELECT c.*, s.titles, s.name_clean
        FROM candidates_panel c LEFT JOIN name_split s USING (name_raw)
    """)
    con.execute("DROP TABLE name_split")

    # --- persons ------------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE TABLE persons AS
        SELECT persoon_id,
               arg_max(name_clean, year)  AS name_clean_last,
               arg_max(titles, year)      AS titles_last,
               COUNT(*)                   AS n_candidacies,
               SUM(elected::INT)          AS n_wins,
               MIN(year)                  AS first_year,
               MAX(year)                  AS last_year,
               COUNT(DISTINCT district)   AS n_districts
        FROM candidates_panel
        GROUP BY persoon_id
    """)

    # --- AIEEDA municipal party panel (post-1917, party-level) -------------
    con.execute("""
        CREATE OR REPLACE TABLE municipal_party_panel AS
        SELECT country_name, election_date::DATE AS election_date,
               EXTRACT(year FROM election_date::DATE)::INT AS year,
               unit_name, unit_name_old, elec_district, constituency_id,
               party_name_short, party_id, votes, seats, provenance
        FROM ai.nl_municipal_party
    """)

    # --- municipality crosswalk copy ----------------------------------------
    con.execute("""
        CREATE OR REPLACE TABLE municipality_years AS
        SELECT * FROM nl.municipality_years
    """)
    con.execute("""
        CREATE OR REPLACE TABLE municipality_transitions AS
        SELECT * FROM nl.transitions
    """)

    # --- parquet exports -----------------------------------------------------
    for tbl in ("candidates_panel", "elections", "persons",
                "municipal_party_panel", "municipality_years",
                "municipality_transitions"):
        con.execute(f"COPY {tbl} TO '{OUT_DIR}/{tbl}.parquet' (FORMAT PARQUET)")

    # --- report ---------------------------------------------------------------
    print("candidates per decade (district era):")
    print(con.execute("""
        SELECT year//10*10 AS decade, COUNT(*) AS candidacies,
               COUNT(DISTINCT persoon_id) AS persons,
               SUM(elected::INT) AS elected_rows
        FROM candidates_panel GROUP BY 1 ORDER BY 1
    """).fetchdf().to_string(index=False))
    print("\nelected rows vs. seats sanity (algemeen+periodiek+herstemming):")
    print(con.execute("""
        SELECT type, SUM(elected::INT) AS n_elected, COUNT(*) AS n_rows
        FROM candidates_panel GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf().to_string(index=False))
    print("\nmunicipal_party_panel:",
          con.execute("SELECT COUNT(*) FROM municipal_party_panel").fetchone()[0],
          "rows")
    con.close()


if __name__ == "__main__":
    main()
