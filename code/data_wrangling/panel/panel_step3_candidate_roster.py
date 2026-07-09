# =============================================================================
# panel_step3_candidate_roster.py  [PANEL PIPELINE - STEP 3]  (= Phase 2b prep)
# Input:  data/panel/panel.duckdb        (candidates_panel)
#         data/panel/mp_anchor.parquet   (Phase 2a; birth year ground truth
#                                          for matched elected persons)
# Output: data/panel/candidate_roster.parquet
#           one row per distinct CANDIDATE (not candidacy-row): persoon_id for
#           the district era, person_key for the PR era. Grain matches
#           mp_anchor's `key` column. Carries the fields the Phase 2b
#           OpenArchieven/GenealogieOnline search pipelines need per
#           candidate: normalised surname/initials for name search, a
#           plausible birth-year window for date filtering, and a normalised
#           place (district principal town, pre-1918; residence, post-1917)
#           for the geography scoring feature.
#
# Birth-year window: passive suffrage was fixed at age 30 for the entire
# 1848-1937 study window (Grondwet 1814/1848, unchanged until lowered to 25
# in 1963 -- verified 2026-07-08), so birth_year <= year_min - 30 always
# holds. There is no hard upper age limit; we use a generous soft cap of 75
# at the candidate's LAST candidacy as the lower bound on birth_year. Where
# mp_anchor already has a PDC-sourced birth_year (reliable, structured
# field), that exact year is used instead of the heuristic window.
#
# Usage:
#   uv run python code/data_wrangling/panel/panel_step3_candidate_roster.py
# =============================================================================
import os
import sys

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from name_match_utils import norm_ini, norm_place, norm_surname, parse_name, \
    strip_district_suffix

DB_PATH = "./data/panel/panel.duckdb"
OUT_DIR = "./data/panel"

MAX_AGE = 75  # soft plausibility cap, not a constitutional rule
MIN_AGE = 30  # passive suffrage age, constant 1848-1937


def build_district_roster(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute("""
        SELECT CAST(persoon_id AS VARCHAR) AS key,
               any_value(name_clean) AS name_for_parse,
               any_value(district) AS district_raw,
               MIN(year) AS year_min, MAX(year) AS year_max,
               BOOL_OR(elected) AS elected
        FROM candidates_panel
        WHERE era = 'district_1848_1918'
        GROUP BY persoon_id
    """).fetchdf()
    df["era"] = "district_1848_1918"
    ini_sn = df["name_for_parse"].map(lambda n: parse_name(n))
    df["initials_raw"] = ini_sn.map(lambda t: t[0])
    df["surname_raw"] = ini_sn.map(lambda t: t[1])
    df["place_raw"] = df["district_raw"].map(strip_district_suffix)
    return df[["era", "key", "surname_raw", "initials_raw", "place_raw",
               "year_min", "year_max", "elected"]]


def build_pr_roster(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute("""
        SELECT person_key AS key,
               any_value(surname) AS surname_raw,
               any_value(initials) AS initials_raw,
               any_value(residence) AS place_raw,
               MIN(year) AS year_min, MAX(year) AS year_max,
               BOOL_OR(elected) AS elected
        FROM candidates_panel
        WHERE era = 'pr_1918_1937'
        GROUP BY person_key
    """).fetchdf()
    df["era"] = "pr_1918_1937"
    return df[["era", "key", "surname_raw", "initials_raw", "place_raw",
               "year_min", "year_max", "elected"]]


def main() -> None:
    con = duckdb.connect(DB_PATH, read_only=True)
    roster = pd.concat(
        [build_district_roster(con), build_pr_roster(con)], ignore_index=True)
    con.close()

    mp = pd.read_parquet(f"{OUT_DIR}/mp_anchor.parquet")[
        ["era", "key", "birth_year", "death_year"]]
    roster = roster.merge(mp, on=["era", "key"], how="left")

    # anchored candidates still get a small buffer, not an exact year: 19th-c.
    # civil-registration records can be off by a year from PDC's stated birth
    # year (e.g. a birth registered in January for a late-December baby), and
    # a zero-width window would make the search itself miss the true record
    # (not just score it lower) -- the diff-based feat_year scoring already
    # penalises near-exact years appropriately, so a search-side buffer here
    # costs nothing precision-wise.
    ANCHOR_BUFFER = 2
    roster["birth_year_lo"] = roster["birth_year"] - ANCHOR_BUFFER
    roster["birth_year_hi"] = roster["birth_year"] + ANCHOR_BUFFER
    no_anchor = roster["birth_year"].isna()
    roster.loc[no_anchor, "birth_year_lo"] = roster.loc[no_anchor, "year_max"] - MAX_AGE
    roster.loc[no_anchor, "birth_year_hi"] = roster.loc[no_anchor, "year_min"] - MIN_AGE
    # a handful of very long candidacy spans (>45yr) invert the naive window
    # (age-30-at-first vs age-75-at-last become jointly infeasible) -- widen
    # defensively rather than search an empty/negative range.
    inverted = roster["birth_year_lo"] > roster["birth_year_hi"]
    roster.loc[inverted, "birth_year_lo"] = roster.loc[inverted, "birth_year_hi"] - 5
    roster["has_birth_anchor"] = ~no_anchor

    roster["sn"] = roster["surname_raw"].map(norm_surname)
    roster["ini"] = roster["initials_raw"].map(norm_ini)
    roster["fi"] = roster["ini"].str[:1]
    roster["place_norm"] = roster["place_raw"].map(norm_place)

    roster = roster[[
        "era", "key", "surname_raw", "initials_raw", "sn", "ini", "fi",
        "place_raw", "place_norm", "year_min", "year_max", "elected",
        "has_birth_anchor", "birth_year", "death_year",
        "birth_year_lo", "birth_year_hi",
    ]]
    roster.to_parquet(f"{OUT_DIR}/candidate_roster.parquet", index=False)

    print(f"candidate_roster.parquet: {len(roster)} distinct candidates")
    print(roster.groupby("era").size().to_string())
    print(f"with a PDC birth-year anchor: {roster['has_birth_anchor'].sum()} "
          f"({roster['has_birth_anchor'].mean():.1%})")
    print(f"missing surname parse: {(roster['sn'] == '').sum()}")
    print(f"missing place: {(roster['place_norm'] == '').sum()}")
    print("\nbirth-year window width distribution (heuristic rows only):")
    w = (roster.loc[no_anchor.values, "birth_year_hi"]
         - roster.loc[no_anchor.values, "birth_year_lo"])
    print(w.describe().to_string())


if __name__ == "__main__":
    main()
