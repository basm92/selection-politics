"""
ind_step09_merge_external_status_sources.py

Merge the OpenArchieven occupation-keyword harvest (openarch_step4) into the surname
status panel, producing the *enriched* panel that the precision-boosted
gol_surname_persistence.R reads.

The external rows are schema-harmonised to `surname_persons.parquet` so HISCAM is
joined once, uniformly, in R (external `beroep_clean` = the query term, which already
matches the HISCO `Original` vocabulary). Each row carries a `source` flag so the
analysis can add a source fixed effect and a balanced single-source robustness — the
guard against a differential-source confound across the border.

Inputs:
  data/genealogieonline/surname_persons.parquet     (ind_step08; source="genealogie")
  data/openarchive/occupations.duckdb  occ_individuals (openarch_step4; source="openarch")
Output:
  data/genealogieonline/surname_persons_enriched.parquet  (union, identical schema + source/event_year)

Cohort handling: external records carry an EVENT year (marriage/registration), not a
birth year, so birth_year is approximated by a per-eventtype offset (marriage ≈ −28,
other ≈ −35); event_year is retained for an offset-sensitivity robustness.

Usage (from project root):
    uv run python code/data_wrangling/genealogie/ind_step09_merge_external_status_sources.py
"""
import logging
from pathlib import Path

import duckdb
import pandas as pd

from ind_step08_surname_status_panel import parse_surname

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

PANEL = Path("data/genealogieonline/surname_persons.parquet")
OCC_DB = Path("data/openarchive/occupations.duckdb")
OUT = Path("data/genealogieonline/surname_persons_enriched.parquet")

MARRIAGE_OFFSET = 28   # groom age at marriage ≈ birth = event − 28
OTHER_OFFSET = 35      # adult in a notarial/other act ≈ birth = event − 35

# Columns of surname_persons.parquet (ind_step08), in order.
PANEL_COLS = ["url", "amco", "birth_year", "beroep_raw", "beroep_clean",
              "dynasty_id", "generation_depth", "religion", "surname", "surname_key",
              "pat_suffix", "pat_s", "has_tussenvoegsel", "n_name_tokens"]


def build() -> None:
    base = pd.read_parquet(PANEL)
    base["source"] = "genealogie"
    base["event_year"] = pd.NA

    if not OCC_DB.exists():
        log.warning("%s not found — writing genealogie-only enriched panel.", OCC_DB)
        base.to_parquet(OUT, index=False)
        log.info("Wrote %s (%d rows, genealogie only)", OUT, len(base))
        return

    con = duckdb.connect(str(OCC_DB), read_only=True)
    occ = con.execute("""
        SELECT amco, term, personname, event_year, relationtype, eventtype, identifier
        FROM occ_individuals
        WHERE personname IS NOT NULL AND event_year IS NOT NULL
    """).fetch_df()
    con.close()
    log.info("OpenArch occupation rows: %d", len(occ))

    # Dedup the same person recurring across records (same surname-name, occ, place, ~year).
    occ = occ.drop_duplicates(subset=["amco", "term", "personname", "event_year"])
    log.info("  after person-level dedup: %d", len(occ))

    # Parse surname from personname (same parser as the genealogy panel).
    parsed = {n: parse_surname(n) for n in occ["personname"].dropna().unique()}
    p = occ["personname"].map(parsed)
    occ = occ[p.notna()].copy()
    p = p[p.notna()]
    occ["surname"] = p.map(lambda x: x[0])
    occ["surname_key"] = p.map(lambda x: x[1])
    occ["pat_suffix"] = p.map(lambda x: x[2])
    occ["pat_s"] = p.map(lambda x: x[3])
    occ["has_tussenvoegsel"] = p.map(lambda x: x[4])
    occ["n_name_tokens"] = p.map(lambda x: x[5])
    occ = occ[occ["surname_key"].fillna("") != ""].copy()

    # Event year -> approximate birth year (per eventtype).
    is_marriage = occ["eventtype"].fillna("").str.contains("rouwen|uweli", case=False, regex=True)
    occ["birth_year"] = (occ["event_year"]
                         - is_marriage.map({True: MARRIAGE_OFFSET, False: OTHER_OFFSET})).astype("Int64")

    ext = pd.DataFrame({
        "url": occ["identifier"],
        "amco": occ["amco"].astype(str),
        "birth_year": occ["birth_year"],
        "beroep_raw": occ["term"],
        "beroep_clean": occ["term"],            # = HISCO Original vocabulary
        "dynasty_id": pd.NA,
        "generation_depth": pd.NA,
        "religion": pd.NA,
        "surname": occ["surname"],
        "surname_key": occ["surname_key"],
        "pat_suffix": occ["pat_suffix"],
        "pat_s": occ["pat_s"],
        "has_tussenvoegsel": occ["has_tussenvoegsel"],
        "n_name_tokens": occ["n_name_tokens"],
        "source": "openarch",
        "event_year": occ["event_year"].astype("Int64"),
    })

    out = pd.concat([base[PANEL_COLS + ["source", "event_year"]], ext], ignore_index=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)

    log.info("Wrote %s (%d rows: genealogie=%d, openarch=%d)",
             OUT, len(out), (out.source == "genealogie").sum(), (out.source == "openarch").sum())
    log.info("  distinct surname keys: %d", out["surname_key"].nunique())


if __name__ == "__main__":
    build()
