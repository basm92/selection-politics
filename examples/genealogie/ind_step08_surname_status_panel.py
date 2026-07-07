"""
ind_step08_surname_status_panel.py

Builds the person-level *surname status panel* that underlies the surname
status-persistence (Clark grouped-elasticity) mobility measure — alternative #4 of
`notes/mobility_without_occupations_brainstorm.md`.

Why this exists
---------------
Pre-1800 father->son *occupation* pairs are too sparse for an individual IGE
(see notes/pre1800_socmob_source_search.md). Clark's surname method sidesteps this:
it tracks how a *surname's* mean status across successive birth cohorts regresses
toward the population mean. The status signal here comes from OUR OWN occupations via
the HISCO->HISCAM crosswalk (this is what distinguishes #4 from the external-register
method #1). The method is non-degenerate because each cohort of a surname is measured
from *different* occupation-bearing individuals, so it survives the ~10% occupation
density of early-modern records — we only need *some* occupation-bearing members per
surname x cohort cell, not the same individuals linked father-to-son.

This script does the heavy, network-free extraction: from the occupation-bearing
persons in `genealogie.duckdb` it (1) parses a surname out of the free-text
`person_name_full` (handling tussenvoegsels, quoted nicknames, parenthetical
alternate spellings) and flags patronymic-pattern surnames (the southern
surname-fixation threat — Brabant used patronymics until ~1811), (2) attaches the
cleaned profession string via `profession_lookup.parquet`, and (3) carries
`amco` / `birth_year` / `dynasty_id` / `generation_depth` / `religion`. The HISCAM
join is deliberately LEFT TO R (`gol_surname_persistence.R`) so it reuses
`muni_step7_gol_human_capital.R`'s exact crosswalk idiom.

Output
------
  data/genealogieonline/surname_persons.parquet
    one row per occupation-bearing person (birth_year 1500-1900, amco present), with:
      url, surname, surname_key, amco, birth_year,
      beroep_raw, beroep_clean,
      dynasty_id, generation_depth, religion,
      pat_suffix (bool), pat_s (bool), has_tussenvoegsel (bool), n_name_tokens (int)

Gotcha handled: `profession_lookup.raw` is NOT unique, so we dedupe it on `raw`
before the join (a naive join double-counts persons ~2x).

Usage (from project root):
    uv run python code/data_wrangling/genealogie/ind_step08_surname_status_panel.py
"""
import logging
import re
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
OUT_PATH = Path("data/genealogieonline/surname_persons.parquet")

YEAR_LO, YEAR_HI = 1500, 1900

# Dutch surname particles ("tussenvoegsels"). The surname is taken to start at the
# first of these that appears after the given name(s).
TUSSENVOEGSELS = {
    "van", "de", "der", "den", "ten", "ter", "te", "op", "in", "het", "'t", "t",
    "von", "du", "des", "la", "le", "el", "vande", "vander", "aan", "bij", "uit",
    "uijt", "onder", "boven", "voor", "over", "tot", "'s", "ver", "ver.",
}

# Patronymic-style endings: "Pietersen", "Jansz", "Hendriksdr", "...dochter".
_PAT_SUFFIX = re.compile(r"(sen|sz|szn|szoon|sdr|sdochter|dr|dochter)$")

_PAREN = re.compile(r"\([^)]*\)")      # parenthetical alternate spellings
_NICK = re.compile(r'"[^"]*"')         # double-quoted nicknames
_MULTISPACE = re.compile(r"\s+")
_PUNCT = re.compile(r"[.,]")


def _clean_name(s: str) -> str:
    s = _PAREN.sub(" ", s)
    s = _NICK.sub(" ", s)
    s = _MULTISPACE.sub(" ", s).strip()
    return s


def _surname_key(surname_raw: str) -> str:
    """Normalised grouping key: lowercase, drop punctuation/apostrophes, collapse spaces.
    Particles are kept (so 'van Loon' != 'Loon') but normalised."""
    k = surname_raw.lower().replace("'", "")
    k = _PUNCT.sub("", k)
    k = _MULTISPACE.sub(" ", k).strip()
    return k


def parse_surname(full: str | None):
    """Return (surname_raw, surname_key, pat_suffix, pat_s, has_tussenvoegsel, n_tokens)
    or None when nothing parseable."""
    if not full or not isinstance(full, str):
        return None
    name = _clean_name(full)
    if not name:
        return None
    toks = [t for t in name.split(" ") if t]
    if not toks:
        return None
    low = [t.lower().strip(".,") for t in toks]

    start = None
    for i, t in enumerate(low):
        if i == 0:
            continue  # a leading particle is part of the given-name region, skip
        if t in TUSSENVOEGSELS:
            start = i
            break

    if start is not None:
        surname_toks = toks[start:]
        has_tv = True
    else:
        surname_toks = [toks[-1]]
        has_tv = False

    surname_raw = " ".join(surname_toks)
    core = surname_toks[-1].lower().strip(".,'")
    pat_suffix = bool(_PAT_SUFFIX.search(core))
    pat_s = (not pat_suffix) and core.endswith("s")
    return surname_raw, _surname_key(surname_raw), pat_suffix, pat_s, has_tv, len(toks)


def build() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"{DB_PATH} not found — fetch via `uv run dvc pull` first.")

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Occupation-bearing persons with geography + birth year, cleaned profession joined
    # from the (deduped!) profession_lookup, plus lineage + religion metadata.
    log.info("Querying occupation-bearing persons (%d-%d) ...", YEAR_LO, YEAR_HI)
    df = con.execute(
        f"""
        WITH lookup AS (
            SELECT lower(trim(raw)) AS raw_l, any_value(cleaned_profession) AS beroep_clean
            FROM read_parquet('data/genealogieonline/profession_lookup.parquet')
            GROUP BY 1
        )
        SELECT p.url,
               p.person_name_full,
               p.amco,
               p.birth_year,
               p.beroep              AS beroep_raw,
               lk.beroep_clean       AS beroep_clean,
               l.dynasty_id,
               l.generation_depth,
               p.religion
        FROM persons p
        LEFT JOIN lookup lk   ON lower(trim(p.beroep)) = lk.raw_l
        LEFT JOIN lineages l  ON p.url = l.url
        WHERE p.beroep IS NOT NULL
          AND p.amco IS NOT NULL
          AND p.birth_year BETWEEN {YEAR_LO} AND {YEAR_HI}
        """
    ).fetch_df()
    con.close()
    log.info("  %d occupation-bearing persons", len(df))

    # Parse surname over unique name strings (cheaper than per-row), then map back.
    log.info("Parsing surnames ...")
    uniq = df["person_name_full"].dropna().unique()
    parsed = {n: parse_surname(n) for n in uniq}
    cols = df["person_name_full"].map(parsed)
    df["surname"] = cols.map(lambda x: x[0] if x else None)
    df["surname_key"] = cols.map(lambda x: x[1] if x else None)
    df["pat_suffix"] = cols.map(lambda x: x[2] if x else None)
    df["pat_s"] = cols.map(lambda x: x[3] if x else None)
    df["has_tussenvoegsel"] = cols.map(lambda x: x[4] if x else None)
    df["n_name_tokens"] = cols.map(lambda x: x[5] if x else None)

    df = df[df["surname_key"].notna() & (df["surname_key"] != "")].copy()
    df = df.drop(columns=["person_name_full"])

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)

    # ---- sanity summary ----
    n = len(df)
    log.info("Wrote %s (%d rows)", OUT_PATH, n)
    log.info("  cleaned-profession matched: %d (%.1f%%)",
             df["beroep_clean"].notna().sum(), 100 * df["beroep_clean"].notna().mean())
    log.info("  has_tussenvoegsel: %.1f%% | pat_suffix: %.1f%% | pat_s: %.1f%%",
             100 * df["has_tussenvoegsel"].mean(),
             100 * df["pat_suffix"].mean(), 100 * df["pat_s"].mean())
    log.info("  distinct surname keys: %d", df["surname_key"].nunique())
    log.info("  birth_year coverage 1500-1800: %d | 1800-1900: %d",
             ((df.birth_year >= 1500) & (df.birth_year < 1800)).sum(),
             ((df.birth_year >= 1800) & (df.birth_year <= 1900)).sum())


if __name__ == "__main__":
    build()
