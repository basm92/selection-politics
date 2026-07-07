"""
ind_step10_multigen_chains.py

Builds the grandfather-father-son *multigenerational chain* dataset behind the
Keller/Shiue non-Markov mobility robustness check (see
`notes/multigen_markov_robustness_plan.md` and
`notes/keller_shiue_multigenerational_mobility.md`).

Why this exists
---------------
All mobility results in the paper are two-generation (father-son). Shiue's
five-generation Tongcheng estimates show that iterating the father-son slope
underpredicts direct grandfather-grandson persistence (b2 > b1^2), and that the
ratio rho = b2/b1 recovers latent persistence free of measurement attenuation.
Testing whether the Protestant mobility advantage survives at the grandparent
horizon requires linked three-generation chains, which this script extracts by
self-joining the `lineage_edges` child->father table of `genealogie.duckdb`
twice and attaching person attributes for each generation.

The HISCO/HISCLASS join is deliberately LEFT TO R
(`code/analysis/appendix/robust_multigen_markov.R`) so it reuses
`muni_step7_gol_human_capital.R`'s exact crosswalk idiom; here occupations are
only normalised via `profession_lookup.parquet` (offline lookup — no LLM calls).

Output
------
  data/genealogieonline/multigen_chains.parquet
    one row per (son_url, father_url, grandfather_url) triple in which at least
    the son AND the grandfather carry an occupation string, with per-generation:
      {son,father,gf}_url, {son,father,gf}_beroep_raw, {son,father,gf}_beroep_clean,
      {son,father,gf}_birth_year, {son,father,gf}_amco,
    plus son_birth_place, dynasty_id, son_source_tree.

Gotchas handled:
  * `profession_lookup.raw` is NOT unique -> dedupe on raw before the join.
  * the same url can appear under several dynasty_ids in `lineages` -> take min().
  * the DB is DVC-tracked -> opened strictly read-only; output is a derived
    parquet that is NOT DVC-tracked.

Usage (from project root):
    uv run python code/data_wrangling/genealogie/ind_step10_multigen_chains.py
"""
import logging
from pathlib import Path

import duckdb
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = Path("data/genealogieonline/genealogie.duckdb")
LOOKUP_PATH = Path("data/genealogieonline/profession_lookup.parquet")
OUT_PATH = Path("data/genealogieonline/multigen_chains.parquet")

CHAIN_SQL = """
WITH e AS (
    SELECT url, father_url
    FROM lineage_edges
    WHERE father_url IS NOT NULL
),
dyn AS (  -- a url can occur under several dynasty_ids; pick a deterministic one
    SELECT url, MIN(dynasty_id) AS dynasty_id
    FROM lineages
    GROUP BY url
)
SELECT DISTINCT
    s.url          AS son_url,
    s.father_url   AS father_url,
    f.father_url   AS gf_url,
    ps.beroep      AS son_beroep_raw,
    pf.beroep      AS father_beroep_raw,
    pgf.beroep     AS gf_beroep_raw,
    ps.birth_year  AS son_birth_year,
    pf.birth_year  AS father_birth_year,
    pgf.birth_year AS gf_birth_year,
    ps.amco        AS son_amco,
    pf.amco        AS father_amco,
    pgf.amco       AS gf_amco,
    ps.birth_place AS son_birth_place,
    ps.source_tree AS son_source_tree,
    dyn.dynasty_id AS dynasty_id
FROM e s
JOIN e f            ON s.father_url = f.url
LEFT JOIN persons ps  ON s.url        = ps.url
LEFT JOIN persons pf  ON s.father_url = pf.url
LEFT JOIN persons pgf ON f.father_url = pgf.url
LEFT JOIN dyn         ON s.url        = dyn.url
WHERE ps.had_beroep AND pgf.had_beroep
"""


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)

    n_triples = con.execute(
        """
        WITH e AS (SELECT url, father_url FROM lineage_edges
                   WHERE father_url IS NOT NULL)
        SELECT COUNT(*) FROM e s JOIN e f ON s.father_url = f.url
        """
    ).fetchone()[0]
    log.info("raw grandfather-father-son triples in lineage_edges: %s", f"{n_triples:,}")

    chains = con.execute(CHAIN_SQL).df()
    con.close()
    log.info("triples with son AND grandfather occupation: %s", f"{len(chains):,}")

    # Normalise occupation strings via the offline lookup (dedupe raw first).
    lookup = (
        pd.read_parquet(LOOKUP_PATH)
        .drop_duplicates(subset="raw")
        .rename(columns={"cleaned_profession": "clean"})
    )
    raw_to_clean = dict(zip(lookup["raw"], lookup["clean"]))

    def clean_col(raw: pd.Series) -> pd.Series:
        mapped = raw.map(raw_to_clean)
        fallback = raw.str.lower().str.strip()
        return mapped.fillna(fallback)

    for gen in ("son", "father", "gf"):
        chains[f"{gen}_beroep_clean"] = clean_col(chains[f"{gen}_beroep_raw"])

    n_all3 = chains["father_beroep_raw"].notna().sum()
    n_amco = chains["son_amco"].notna().sum()
    log.info("of these, with father occupation too: %s", f"{n_all3:,}")
    log.info("of these, with son amco (analysable): %s", f"{n_amco:,}")

    chains = chains[chains["son_amco"].notna()].reset_index(drop=True)
    chains.to_parquet(OUT_PATH, index=False)
    log.info("wrote %s rows -> %s", f"{len(chains):,}", OUT_PATH)


if __name__ == "__main__":
    main()
