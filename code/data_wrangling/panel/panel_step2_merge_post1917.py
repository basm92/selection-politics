# =============================================================================
# panel_step2_merge_post1917.py  [PANEL PIPELINE - STEP 2]
# Input:  data/panel/panel.duckdb     (candidates_panel = district era, step 1)
#         data/delpher/delpher.duckdb  (kandidatenlijsten, voorkeur_stemmen,
#                                        lijst_uitslagen, gekozen — steps 5/6/6b)
# Output: data/panel/panel.duckdb
#           candidates_panel        — UNIFIED candidate rows 1848-1937, with an
#                                     `era` column; district cols NULL for PR
#                                     rows and vice-versa (superset schema)
#           persons_post1917        — one row per (year, person) PR candidacy
#                                     summary (kieskring coverage, elected)
#           elections_post1917      — per (year) PR election summary
#           gekozen_unmatched       — elected members not matched to a
#                                     candidate list (OCR/spelling residue)
#         data/panel/*.parquet  (re-exported: candidates_panel + the new tables)
#
# Grain of the PR (1918-1937) candidacy row = one (year, kieskring, lijst,
#   positie) entry from the official candidate lists (kandidatenlijsten),
#   enriched with:
#     - `votes`      = that candidate's PREFERENCE votes in that kieskring
#                      (voorkeur_stemmen), NULL where the vote table block
#                      failed its checksum / was not parsed;
#     - `stemcijfer` = the list total for that (kieskring, lijst);
#     - `elected`    = whether this PERSON won a seat in this election
#                      (propagated across all their kieskring rows — PR seats
#                      are person-level, not kieskring-level; see below).
#
# `elected` derivation (PR era): the `gekozen` table lists the ~100 seated
#   members per election by surname + initials + residence. We match each to a
#   candidate-list person (surname normalised for the Staatscourant's inverted
#   tussenvoegsel form "Molen, van der" -> "van der Molen"; tiered on full
#   initials, then first initial + residence, then first initial). Every
#   matched person is flagged `elected` on ALL their candidacy rows that year;
#   unmatched seats are logged in `gekozen_unmatched`.
#
# Person identity is PROVISIONAL here (surname+initials within the Staatscourant
#   only) — cross-source / cross-era resolution is Phase 2. `persoon_id` is
#   district-era only; PR rows carry `person_key = 'sc:<surname>|<initials>'`.
#
# Usage:
#   uv run python code/data_wrangling/panel/panel_step2_merge_post1917.py
#   (idempotent: rebuilds the PR rows from delpher each run; district rows are
#    read back from candidates_panel by provenance, so step 1 must run first.)
# =============================================================================
import os
import re
import unicodedata

import duckdb
import pandas as pd

OUT_DIR = "./data/panel"
DB_PATH = os.path.join(OUT_DIR, "panel.duckdb")
DELPHER_DB = "./data/delpher/delpher.duckdb"

PR_ELECTION_DATES = {
    1918: "1918-07-03", 1922: "1922-07-05", 1925: "1925-07-01",
    1929: "1929-07-03", 1933: "1933-04-26", 1937: "1937-05-26",
}
SEATS = 100  # Tweede Kamer seats each PR election


# --- name normalisation helpers ---------------------------------------------
def _strip(s) -> str:
    """lowercase, drop diacritics, keep letters only (NaN/None -> '')."""
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z]", "", s.lower())


def norm_surname_kl(surname: str | None) -> str:
    """kandidatenlijsten.surname is already natural order ('van der Molen')."""
    return _strip(surname) if surname else ""


def norm_surname_gekozen(name_raw: str | None) -> str:
    """gekozen inverts the tussenvoegsel: 'Molen, van der' -> 'vandermolen';
    also drops a married-name 'geb. X' suffix before the comma part."""
    if not isinstance(name_raw, str):
        return ""
    if "," in name_raw:
        main, rest = name_raw.split(",", 1)
        rest = re.sub(r"geb\..*", "", rest)  # drop "geb. Bruins"
        name_raw = f"{rest} {main}"
    return _strip(name_raw)


def norm_ini(initials: str | None) -> str:
    return _strip(initials) if initials else ""


def _lev(a: str, b: str) -> int:
    """Levenshtein edit distance (small strings)."""
    if a == b:
        return 0
    if not a or not b:
        return len(a) + len(b)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1,
                           prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def build_elected_set(kl: pd.DataFrame, gek: pd.DataFrame) -> tuple[set, list]:
    """Return (set of elected (year, surname_norm, ini_norm) person keys,
    list of unmatched gekozen rows)."""
    kl = kl.copy()
    kl["sn"] = kl["surname"].map(norm_surname_kl)
    kl["ini"] = kl["initials"].map(norm_ini)
    kl["res"] = kl["residence"].map(lambda r: _strip(r) if r else "")

    elected: set = set()
    unmatched: list = []
    for year, g in gek.groupby("year"):
        ky = kl[kl["year"] == year]
        # index helpers
        by_full = ky.groupby(["sn", "ini"])
        for _, row in g.iterrows():
            sn = norm_surname_gekozen(row["name_raw"])
            ini = norm_ini(row["initials"])
            res = _strip(row["residence"]) if pd.notna(row["residence"]) else ""
            fi = ini[:1]
            # tier 1: surname + full initials
            if (sn, ini) in by_full.groups:
                elected.add((year, sn, ini))
                continue
            # tier 2: candidates sharing surname + first initial
            cand = ky[(ky["sn"] == sn) & (ky["ini"].str[:1] == fi)]
            # tier 3: fuzzy surname (OCR digit/letter slips) + first initial,
            # edit distance <=2 for len>=5 else <=1
            if len(cand) == 0 and sn:
                tol = 2 if len(sn) >= 5 else 1
                pool = ky[ky["ini"].str[:1] == fi]
                dists = pool["sn"].map(lambda s: _lev(s, sn))
                cand = pool[dists <= tol]
            if len(cand) == 0:
                unmatched.append((year, row["name_raw"], row["initials"],
                                  row["residence"]))
                continue
            # disambiguate by residence when >1
            if len(cand) > 1 and res:
                res_hit = cand[cand["res"] == res]
                if len(res_hit) >= 1:
                    cand = res_hit
            # flag every surviving candidate person (usually 1)
            for _, c in cand.iterrows():
                elected.add((year, c["sn"], c["ini"]))
    return elected, unmatched


# --- unified schema (column, sql-type) --------------------------------------
UNIFIED_COLS = [
    ("era", "VARCHAR"), ("year", "INTEGER"), ("election_date", "DATE"),
    ("provenance", "VARCHAR"),
    ("persoon_id", "INTEGER"), ("person_key", "VARCHAR"),
    ("name_raw", "VARCHAR"), ("name_clean", "VARCHAR"),
    ("titles", "VARCHAR"), ("surname", "VARCHAR"), ("initials", "VARCHAR"),
    ("votes", "INTEGER"), ("pct", "DOUBLE"), ("rank", "INTEGER"),
    ("vote_order", "BIGINT"), ("n_candidates", "BIGINT"),
    ("elected", "BOOLEAN"),
    # district era
    ("uitslag_id", "INTEGER"), ("district", "VARCHAR"),
    ("district_id", "INTEGER"), ("type", "VARCHAR"),
    ("zetels", "INTEGER"), ("kiesdrempel", "INTEGER"),
    ("affiliation", "VARCHAR"),
    # PR era
    ("kieskring_no", "INTEGER"), ("kieskring_name", "VARCHAR"),
    ("lijst_no", "VARCHAR"), ("positie", "INTEGER"),
    ("residence", "VARCHAR"), ("stemcijfer", "INTEGER"),
    ("list_checksum_ok", "BOOLEAN"), ("votes_checksum_ok", "BOOLEAN"),
]


def main() -> None:
    con = duckdb.connect(DB_PATH)
    con.execute(f"ATTACH '{DELPHER_DB}' AS dp (READ_ONLY)")

    # --- PR candidacy rows (kieskring x lijst x positie) --------------------
    # dedup voorkeur_stemmen / lijst_uitslagen (block-spanning pages create a
    # few dup keys): prefer checksum_ok, then higher votes/stemcijfer.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _vk AS
        SELECT year, kieskring_no, lijst_no, positie, votes, checksum_ok
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY year, kieskring_no, lijst_no, positie
                ORDER BY checksum_ok DESC, votes DESC NULLS LAST) rn
            FROM dp.voorkeur_stemmen) WHERE rn = 1
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _lu AS
        SELECT year, kieskring_no, lijst_no, stemcijfer, checksum_ok
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY year, kieskring_no, lijst_no
                ORDER BY checksum_ok DESC, stemcijfer DESC NULLS LAST) rn
            FROM dp.lijst_uitslagen) WHERE rn = 1
    """)

    pr = con.execute("""
        SELECT k.year, k.kieskring_no, k.kieskring_name, k.lijst_no, k.positie,
               k.name_raw, k.surname, k.initials, k.titles, k.residence,
               vk.votes, vk.checksum_ok AS votes_checksum_ok,
               lu.stemcijfer, lu.checksum_ok AS list_checksum_ok
        FROM dp.kandidatenlijsten k
        LEFT JOIN _vk vk USING (year, kieskring_no, lijst_no, positie)
        LEFT JOIN _lu lu USING (year, kieskring_no, lijst_no)
    """).fetchdf()

    gek = con.execute(
        "SELECT year, name_raw, initials, residence FROM dp.gekozen").fetchdf()

    # --- elected flag -------------------------------------------------------
    elected_set, unmatched = build_elected_set(
        pr[["year", "surname", "initials", "residence"]], gek)

    pr["_sn"] = pr["surname"].map(norm_surname_kl)
    pr["_ini"] = pr["initials"].map(norm_ini)
    pr["elected"] = [
        (y, sn, ini) in elected_set
        for y, sn, ini in zip(pr["year"], pr["_sn"], pr["_ini"])
    ]
    pr["person_key"] = "sc:" + pr["_sn"] + "|" + pr["_ini"]
    pr["election_date"] = pr["year"].map(PR_ELECTION_DATES)
    pr["name_clean"] = (
        pr["initials"].fillna("").str.strip() + " " + pr["surname"].fillna("")
    ).str.strip()
    pr["era"] = "pr_1918_1937"
    pr["provenance"] = "staatscourant_delpher"

    con.register("pr_df", pr)

    # --- unified candidates_panel ------------------------------------------
    # district columns carried straight through from step-1 candidates_panel
    DIST_PASSTHROUGH = {
        "year", "election_date", "provenance", "persoon_id", "name_raw",
        "name_clean", "titles", "votes", "pct", "rank", "vote_order",
        "n_candidates", "elected", "uitslag_id", "district", "district_id",
        "type", "zetels", "kiesdrempel", "affiliation",
    }

    def dist_expr(c: str, t: str) -> str:
        if c == "era":
            return "'district_1848_1918' AS era"
        if c == "person_key":
            return "'hg:' || CAST(persoon_id AS VARCHAR) AS person_key"
        if c in DIST_PASSTHROUGH:
            return f"{c} AS {c}"
        return f"CAST(NULL AS {t}) AS {c}"

    dist_select = ", ".join(dist_expr(c, t) for c, t in UNIFIED_COLS)

    pr_select = ", ".join(
        f"CAST({c} AS {t}) AS {c}" if c in pr.columns
        else f"CAST(NULL AS {t}) AS {c}"
        for c, t in UNIFIED_COLS
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE candidates_panel AS
        SELECT {dist_select}
        FROM candidates_panel
        WHERE provenance LIKE 'huygens%'
        UNION ALL BY NAME
        SELECT {pr_select} FROM pr_df
    """)

    # --- PR person-election summary ----------------------------------------
    con.execute("""
        CREATE OR REPLACE TABLE persons_post1917 AS
        SELECT year, person_key,
               any_value(surname)   AS surname,
               any_value(initials)  AS initials,
               any_value(titles)    AS titles,
               any_value(residence) AS residence,
               COUNT(*)                       AS n_candidacies,
               COUNT(DISTINCT kieskring_no)   AS n_kieskringen,
               MIN(positie)                   AS best_list_position,
               SUM(COALESCE(votes, 0))        AS pref_votes_total,
               BOOL_OR(elected)               AS elected
        FROM candidates_panel WHERE era = 'pr_1918_1937'
        GROUP BY year, person_key
    """)

    # --- PR election summary -----------------------------------------------
    con.execute(f"""
        CREATE OR REPLACE TABLE elections_post1917 AS
        SELECT year, MIN(election_date) AS election_date, {SEATS} AS seats,
               COUNT(DISTINCT kieskring_no) AS n_kieskringen,
               COUNT(DISTINCT (kieskring_no, lijst_no)) AS n_lists,
               COUNT(*) AS n_candidacy_rows,
               COUNT(DISTINCT person_key) AS n_persons,
               SUM(elected::INT) AS n_elected_rows
        FROM candidates_panel WHERE era = 'pr_1918_1937'
        GROUP BY year ORDER BY year
    """)

    # --- unmatched elected (data-quality residue) --------------------------
    con.execute("""CREATE OR REPLACE TABLE gekozen_unmatched
                   (year INTEGER, name_raw VARCHAR, initials VARCHAR,
                    residence VARCHAR)""")
    if unmatched:
        con.executemany(
            "INSERT INTO gekozen_unmatched VALUES (?,?,?,?)", unmatched)

    # --- parquet re-export --------------------------------------------------
    for tbl in ("candidates_panel", "persons_post1917",
                "elections_post1917", "gekozen_unmatched"):
        con.execute(f"COPY {tbl} TO '{OUT_DIR}/{tbl}.parquet' (FORMAT PARQUET)")

    # --- report -------------------------------------------------------------
    print("=== unified candidates_panel by era/decade ===")
    print(con.execute("""
        SELECT era, year//10*10 AS decade, COUNT(*) AS rows,
               SUM(elected::INT) AS elected_rows
        FROM candidates_panel GROUP BY 1,2 ORDER BY 2
    """).fetchdf().to_string(index=False))

    print("\n=== PR elections summary ===")
    print(con.execute(
        "SELECT * FROM elections_post1917 ORDER BY year").fetchdf().to_string(index=False))

    print("\n=== elected persons vs 100 seats (persons_post1917) ===")
    print(con.execute("""
        SELECT year, COUNT(*) AS persons,
               SUM(elected::INT) AS elected_persons
        FROM persons_post1917 GROUP BY year ORDER BY year
    """).fetchdf().to_string(index=False))

    n_unm = con.execute("SELECT COUNT(*) FROM gekozen_unmatched").fetchone()[0]
    print(f"\ngekozen seats not matched to a candidate list: {n_unm}")
    if n_unm:
        print(con.execute(
            "SELECT * FROM gekozen_unmatched ORDER BY year").fetchdf().to_string(index=False))

    print("\n=== PR list-vote national totals (stemcijfer sum, distinct lists) ===")
    print(con.execute("""
        SELECT year, SUM(stemcijfer) AS total_list_votes
        FROM (SELECT DISTINCT year, kieskring_no, lijst_no, stemcijfer
              FROM candidates_panel WHERE era='pr_1918_1937')
        GROUP BY year ORDER BY year
    """).fetchdf().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
