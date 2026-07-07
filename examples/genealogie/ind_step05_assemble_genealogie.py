"""
ind_step05_assemble_genealogie.py

One-time MERGE step. Assembles the single canonical `genealogie.duckdb` from the
two historical crawls so that everything downstream reads as if the data had been
collected in one 1500-1900 run:

  * socmob_pairs.duckdb       1750-1900, occupation-filtered (oc=*), node `person_urls`,
                              assembled father->son `pairs` table (Phase D/E).
  * births_1500_1800.duckdb   1500-1800, NOT occupation-filtered, node `births`,
                              father->son pairs only available via the lineage spine.

This script is pure SQL over the two attached DBs — NO network. It produces the
schema that the unified scraper (ind_step04_scrape_genealogie.py) would have
written from scratch.

Design / provenance notes (the two crawls are not symmetric — documented, not hidden):
  * 128k person URLs appear in BOTH crawls. `persons` deduplicates by url,
    COALESCE-ing fields and preferring the socmob (enriched) row; `crawl` marks
    'socmob_1750_1900' | 'births_1500_1800' | 'both'.
  * The current analysis sample is the socmob `pairs` table verbatim
    (source='socmob_1750_1900'). It is copied unchanged so today's results do
    not move.
  * The 1500-1800 expansion (source='births_1500_1800') is built from the births
    lineage spine (`lineage_pairs`, both beroep present) — a DIFFERENT
    construction than Phase D/E — and is deduplicated so a son already present as
    a socmob pair son is not re-added.
  * `amco`: for socmob pairs it was resolved from the birth_place text via
    muni_names; for births pairs it is the pn= search municipality. Both denote
    the son's birth municipality.

dynasty_id / generation_depth are attached to `pairs` afterwards by
ind_step06_build_lineages.py (which must be run on genealogie.duckdb next).

Inputs:
    data/genealogieonline/socmob_pairs.duckdb
    data/genealogieonline/births_1500_1800.duckdb

Output:
    data/genealogieonline/genealogie.duckdb
      tables: persons, person_children, pairs, muni_names,
              target_municipalities, search_progress

Usage (from project root):
    uv run python code/data_wrangling/genealogie/ind_step05_assemble_genealogie.py
"""
from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT / "data" / "genealogieonline"
SOCMOB_DB = DATA_DIR / "socmob_pairs.duckdb"
BIRTHS_DB = DATA_DIR / "births_1500_1800.duckdb"
OUT_DB = DATA_DIR / "genealogie.duckdb"

CRAWL_SOCMOB = "socmob_1750_1900"
CRAWL_BIRTHS = "births_1500_1800"


def assemble() -> None:
    for p in (SOCMOB_DB, BIRTHS_DB):
        if not p.exists():
            raise SystemExit(f"Required input DB missing: {p}")

    if OUT_DB.exists():
        log.warning("Removing existing %s (rebuilding from scratch)", OUT_DB.name)
        OUT_DB.unlink()

    con = duckdb.connect(str(OUT_DB))
    con.execute(f"ATTACH '{SOCMOB_DB}' AS s (READ_ONLY)")
    con.execute(f"ATTACH '{BIRTHS_DB}' AS b (READ_ONLY)")

    # ------------------------------------------------------------------
    # 1. persons — union of person_urls (socmob) and births, deduped by url.
    #    FULL OUTER JOIN so we keep URLs unique to either crawl; COALESCE
    #    prefers the socmob row (richer Phase C enrichment) where both exist.
    # ------------------------------------------------------------------
    log.info("Building persons …")
    con.execute(f"""
        CREATE TABLE persons AS
        SELECT
            COALESCE(s.url, b.url)                       AS url,
            COALESCE(s.amco, b.amco)                     AS amco,
            b.place_norm                                 AS place_norm,
            COALESCE(s.person_name, b.person_name)       AS person_name,
            COALESCE(b.person_name_full, s.person_name)  AS person_name_full,
            COALESCE(s.birth_year, b.birth_year)         AS birth_year,
            COALESCE(s.birth_place, b.birth_place)       AS birth_place,
            b.source_tree                                AS source_tree,
            COALESCE(s.beroep, b.beroep)                 AS beroep,
            COALESCE(s.had_beroep, b.had_beroep)         AS had_beroep,
            COALESCE(s.father_url, b.father_url)         AS father_url,
            COALESCE(s.father_name, b.father_name)       AS father_name,
            COALESCE(s.fetched, b.fetched)               AS fetched,
            COALESCE(s.skip, b.skip)                      AS skip,
            CASE
                WHEN s.url IS NOT NULL AND b.url IS NOT NULL THEN 'both'
                WHEN s.url IS NOT NULL THEN '{CRAWL_SOCMOB}'
                ELSE '{CRAWL_BIRTHS}'
            END                                          AS crawl
        FROM s.person_urls s
        FULL OUTER JOIN b.births b ON s.url = b.url
    """)
    n_persons = con.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    n_both = con.execute("SELECT COUNT(*) FROM persons WHERE crawl='both'").fetchone()[0]
    log.info("  persons: %d rows (%d in both crawls)", n_persons, n_both)

    # ------------------------------------------------------------------
    # 2. person_children — union of both edge tables, deduped on (parent, child).
    # ------------------------------------------------------------------
    log.info("Building person_children …")
    con.execute("""
        CREATE TABLE person_children AS
        SELECT parent_url, child_url, any_value(child_name) AS child_name
        FROM (
            SELECT parent_url, child_url, child_name FROM s.person_children
            UNION ALL
            SELECT parent_url, child_url, child_name FROM b.person_children
        )
        GROUP BY parent_url, child_url
    """)
    n_edges = con.execute("SELECT COUNT(*) FROM person_children").fetchone()[0]
    log.info("  person_children: %d edges", n_edges)

    # ------------------------------------------------------------------
    # 3. muni_names (socmob only) + target_municipalities (union) + search_progress.
    # ------------------------------------------------------------------
    con.execute("CREATE TABLE muni_names AS SELECT * FROM s.muni_names")
    con.execute(f"""
        CREATE TABLE target_municipalities AS
        SELECT
            COALESCE(s.amco, b.amco)               AS amco,
            COALESCE(s.name, b.name)               AS name,
            s.name_norm                            AS name_norm,
            COALESCE(b.place_norm, s.name_norm)    AS place_norm,
            s.openarch_pairs                       AS openarch_pairs,
            s.search_name_override                 AS search_name_override
        FROM s.target_municipalities s
        FULL OUTER JOIN b.target_municipalities b ON s.amco = b.amco
    """)
    con.execute("""
        CREATE TABLE search_progress AS
        SELECT amco, gv, gt, max(n_results) AS n_results, bool_and(done) AS done
        FROM (
            SELECT amco, gv, gt, n_results, done FROM s.search_progress
            UNION ALL
            SELECT amco, gv, gt, n_results, done FROM b.search_progress
        )
        GROUP BY amco, gv, gt
    """)
    log.info("  muni_names: %d, target_municipalities: %d, search_progress: %d",
             con.execute("SELECT COUNT(*) FROM muni_names").fetchone()[0],
             con.execute("SELECT COUNT(*) FROM target_municipalities").fetchone()[0],
             con.execute("SELECT COUNT(*) FROM search_progress").fetchone()[0])

    # ------------------------------------------------------------------
    # 4. pairs — canonical father->son beroep pairs, source-flagged + widened.
    #    (a) socmob pairs verbatim; (b) births lineage-pairs, deduped on son_url,
    #    with amco / birth_year backfilled from the births node table.
    #    dynasty_id / generation_depth are added later by ind_step06.
    # ------------------------------------------------------------------
    log.info("Building pairs …")
    con.execute(f"""
        CREATE TABLE pairs AS
        SELECT
            son_url, amco,
            son_name, son_beroep, son_birth_year, son_birth_place,
            father_url, father_name, father_beroep,
            father_birth_year, father_birth_place,
            '{CRAWL_SOCMOB}' AS source
        FROM s.pairs
        UNION ALL
        SELECT
            lp.son_url,
            sb.amco                          AS amco,
            lp.son_name,    lp.son_beroep,    sb.birth_year AS son_birth_year,
            lp.son_birth_place,
            lp.father_url,  lp.father_name,   lp.father_beroep,
            fb.birth_year                    AS father_birth_year,
            lp.father_birth_place,
            '{CRAWL_BIRTHS}' AS source
        FROM b.lineage_pairs lp
        LEFT JOIN b.births sb ON lp.son_url    = sb.url
        LEFT JOIN b.births fb ON lp.father_url = fb.url
        WHERE lp.son_beroep IS NOT NULL
          AND lp.father_beroep IS NOT NULL
          AND lp.son_url NOT IN (SELECT son_url FROM s.pairs)
    """)
    # dynasty columns are populated by ind_step06; create them now so the schema
    # is stable regardless of run order.
    con.execute("ALTER TABLE pairs ADD COLUMN dynasty_id VARCHAR")
    con.execute("ALTER TABLE pairs ADD COLUMN generation_depth INTEGER")

    n_socmob = con.execute(
        f"SELECT COUNT(*) FROM pairs WHERE source='{CRAWL_SOCMOB}'"
    ).fetchone()[0]
    n_births = con.execute(
        f"SELECT COUNT(*) FROM pairs WHERE source='{CRAWL_BIRTHS}'"
    ).fetchone()[0]
    log.info("  pairs: %d total (%d socmob verbatim, %d new births)",
             n_socmob + n_births, n_socmob, n_births)

    con.close()
    log.info("Done. Wrote %s", OUT_DB.relative_to(ROOT))
    log.info("Next: run ind_step06_build_lineages.py (lineages + pairs dynasty enrichment), "
             "then ind_step05_clean_genealogieonline.py.")


if __name__ == "__main__":
    assemble()
