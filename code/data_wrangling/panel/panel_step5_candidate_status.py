# =============================================================================
# panel_step5_candidate_status.py  [PANEL PIPELINE - STEP 5]  (Phase 3)
# Input:  data/panel/candidate_roster.parquet          (panel step 3)
#         data/panel/candidate_person_pairs.parquet    (panel step 4)
#         data/panel/panel.duckdb  candidates_panel     (titles)
#         data/openarch/openarch.duckdb  hits, detail_records
#                                                        (openarch step 1-2)
#         data/genealogieonline/genealogieonline.duckdb  candidate_ancestors,
#                                                        person_pages
#                                                        (genealogieonline step 1-2)
#         data/panel/beroep_hisco_matches.parquet        (status step 1)
#         data/panel/dynasty_edges.parquet, dynasty_candidates.parquet
#                                                        (status step 2)
# Output: data/panel/candidate_status.parquet
#           era, key, elected, year_min, year_max, titles,
#           own_beroep_openarch, own_beroep_genealogieonline, own_beroep,
#             own_hisco, own_hisclass, own_hisclass_5, own_hiscam_nl,
#           father_beroep_openarch, father_beroep_genealogieonline,
#             father_beroep, father_hisco, father_hisclass, father_hisclass_5,
#             father_hiscam_nl,
#           dynasty_id, n_dynasty_relatives, prior_relative_any,
#             prior_relative_elected, later_relative_any, later_relative_elected
#
# Occupational status: for the same best-pair openarch record used in
# openarch_step2 (score >= 0.7), the candidate's OWN role in that record
# (hits.relationtype -- "Kind"/"Bruidegom"/"Bruid") identifies which
# detail_records row is the candidate and which is the father ("Vader" for a
# birth record, "Vader van de bruidegom/bruid" for a marriage record).
# GenealogieOnline gives the same two (own beroep at candidate_ancestors
# depth=0, father beroep at depth=1) directly from person_pages. Where both
# sources have a beroep, both are kept (own_beroep_openarch/_genealogieonline)
# alongside a coalesced `own_beroep` (genealogieonline preferred: its beroep
# field is usually a settled-career description vs. openarch's single-event
# marriage/birth-record snapshot) for convenience.
#
# CHECKPOINT numbers are printed at the end -- coverage of own/father HISCLASS
# and dynasty membership, to report before moving to Phase 4 (wealth).
#
# Usage:
#   uv run python code/data_wrangling/panel/panel_step5_candidate_status.py
# =============================================================================
import os

import duckdb
import pandas as pd

ROSTER_PATH = "./data/panel/candidate_roster.parquet"
PAIRS_PATH = "./data/panel/candidate_person_pairs.parquet"
PANEL_DB = "./data/panel/panel.duckdb"
OPENARCH_DB = "./data/openarch/openarch.duckdb"
GENEALOGIE_DB = "./data/genealogieonline/genealogieonline.duckdb"
HISCO_MATCHES_PATH = "./data/panel/beroep_hisco_matches.parquet"
DYNASTY_EDGES_PATH = "./data/panel/dynasty_edges.parquet"
DYNASTY_CANDIDATES_PATH = "./data/panel/dynasty_candidates.parquet"
OUT_PATH = "./data/panel/candidate_status.parquet"

SCORE_THRESHOLD = 0.7

_FATHER_RELATION = {
    "Kind": "Vader",
    "Bruidegom": "Vader van de bruidegom",
    "Bruid": "Vader van de bruid",
}


def best_pairs(source: str) -> pd.DataFrame:
    pairs = pd.read_parquet(PAIRS_PATH)
    sub = pairs[(pairs["source"] == source) & (pairs["score"] >= SCORE_THRESHOLD)].copy()
    return sub.sort_values("score", ascending=False).drop_duplicates(
        subset=["era", "key"], keep="first"
    )[["era", "key", "person_ref"]]


def load_titles() -> pd.DataFrame:
    con = duckdb.connect(PANEL_DB, read_only=True)
    district = con.execute("""
        SELECT 'district_1848_1918' AS era, CAST(persoon_id AS VARCHAR) AS key,
               any_value(titles) AS titles
        FROM candidates_panel WHERE era = 'district_1848_1918'
        GROUP BY persoon_id
    """).df()
    pr = con.execute("""
        SELECT 'pr_1918_1937' AS era, person_key AS key, any_value(titles) AS titles
        FROM candidates_panel WHERE era = 'pr_1918_1937'
        GROUP BY person_key
    """).df()
    con.close()
    return pd.concat([district, pr], ignore_index=True)


def load_openarch_status() -> pd.DataFrame:
    """own/father profession per candidate from the best-pair openarch record."""
    oa_pairs = best_pairs("openarch")
    if oa_pairs.empty or not os.path.exists(OPENARCH_DB):
        return pd.DataFrame(columns=["era", "key", "own_beroep_openarch", "father_beroep_openarch"])

    con = duckdb.connect(OPENARCH_DB, read_only=True)
    hits = con.execute(
        "SELECT DISTINCT era, key, identifier, archive_code, relationtype FROM hits"
    ).df()
    detail = con.execute(
        "SELECT archive_code, identifier, relation_type, profession FROM detail_records"
    ).df()
    con.close()

    merged = oa_pairs.merge(
        hits, left_on=["era", "key", "person_ref"], right_on=["era", "key", "identifier"],
        how="inner",
    )
    merged["father_relation"] = merged["relationtype"].map(_FATHER_RELATION)

    own = merged.merge(
        detail, left_on=["archive_code", "identifier", "relationtype"],
        right_on=["archive_code", "identifier", "relation_type"], how="left",
    )[["era", "key", "profession"]].rename(columns={"profession": "own_beroep_openarch"})

    father = merged.merge(
        detail, left_on=["archive_code", "identifier", "father_relation"],
        right_on=["archive_code", "identifier", "relation_type"], how="left",
    )[["era", "key", "profession"]].rename(columns={"profession": "father_beroep_openarch"})

    out = own.merge(father, on=["era", "key"], how="outer")
    return out.drop_duplicates(subset=["era", "key"])


def load_genealogieonline_status() -> pd.DataFrame:
    """own beroep at candidate_ancestors depth=0, father beroep at depth=1,
    restricted to candidates whose best genealogieonline pair clears
    SCORE_THRESHOLD -- candidate_ancestors itself may hold a WIDER set (it's
    seeded by genealogieonline_step2's own SCORE_THRESHOLD, which can drift
    out of sync with this script's if only one of the two is edited; a
    hand-labelled spot-check of the 0.5-0.7 band measured ~30% precision, so
    this filter is not optional)."""
    if not os.path.exists(GENEALOGIE_DB):
        return pd.DataFrame(columns=["era", "key", "own_beroep_genealogieonline",
                                      "father_beroep_genealogieonline"])
    qualifying = best_pairs("genealogieonline")[["era", "key"]]
    if qualifying.empty:
        return pd.DataFrame(columns=["era", "key", "own_beroep_genealogieonline",
                                      "father_beroep_genealogieonline"])

    con = duckdb.connect(GENEALOGIE_DB, read_only=True)
    con.register("qualifying", qualifying)
    own = con.execute("""
        SELECT ca.era, ca.key, pp.beroep AS own_beroep_genealogieonline
        FROM candidate_ancestors ca
        JOIN person_pages pp ON pp.url = ca.url
        JOIN qualifying q ON q.era = ca.era AND q.key = ca.key
        WHERE ca.depth = 0
    """).df()
    father = con.execute("""
        SELECT ca.era, ca.key, pp.beroep AS father_beroep_genealogieonline
        FROM candidate_ancestors ca
        JOIN person_pages pp ON pp.url = ca.url
        JOIN qualifying q ON q.era = ca.era AND q.key = ca.key
        WHERE ca.depth = 1
    """).df()
    con.close()
    out = own.merge(father, on=["era", "key"], how="outer")
    return out.drop_duplicates(subset=["era", "key"])


def attach_hisco(df: pd.DataFrame, beroep_col: str, prefix: str,
                  hisco: pd.DataFrame) -> pd.DataFrame:
    import re
    import unicodedata

    def norm(s):
        if not isinstance(s, str) or not s:
            return None
        s = unicodedata.normalize("NFKD", s)
        s = "".join(c for c in s if not unicodedata.combining(c))
        s = re.sub(r"[^a-zA-Z\s]", " ", s.lower())
        return re.sub(r"\s+", " ", s).strip()

    df = df.copy()
    df["_norm"] = df[beroep_col].map(norm)

    # Distinct RAW strings can share a normalised form (case/punctuation
    # variants, e.g. "Bakker" vs "bakker.") -- dedupe on beroep_norm first so
    # the merge stays many-to-one and doesn't fan out candidate rows.
    hisco_dedup = (
        hisco.rename(columns={"beroep_norm": "_norm"})
        .sort_values("match_score", ascending=False)
        .drop_duplicates(subset=["_norm"], keep="first")
    )
    merged = df.merge(hisco_dedup, on="_norm", how="left")
    return merged.rename(columns={
        "hisco": f"{prefix}_hisco", "hisclass": f"{prefix}_hisclass",
        "hisclass_5": f"{prefix}_hisclass_5", "hiscam_nl": f"{prefix}_hiscam_nl",
        "match_method": f"{prefix}_match_method",
    }).drop(columns=["_norm", "beroep_raw", "match_score"], errors="ignore")


def build() -> None:
    roster = pd.read_parquet(ROSTER_PATH)[["era", "key", "elected", "year_min", "year_max"]]
    titles = load_titles()
    oa_status = load_openarch_status()
    go_status = load_genealogieonline_status()

    out = roster.merge(titles, on=["era", "key"], how="left")
    out = out.merge(oa_status, on=["era", "key"], how="left")
    out = out.merge(go_status, on=["era", "key"], how="left")

    out["own_beroep"] = out["own_beroep_genealogieonline"].combine_first(out["own_beroep_openarch"])
    out["father_beroep"] = out["father_beroep_genealogieonline"].combine_first(out["father_beroep_openarch"])

    if os.path.exists(HISCO_MATCHES_PATH):
        hisco = pd.read_parquet(HISCO_MATCHES_PATH)
        out = attach_hisco(out, "own_beroep", "own", hisco)
        out = attach_hisco(out, "father_beroep", "father", hisco)

    if os.path.exists(DYNASTY_CANDIDATES_PATH):
        dyn = pd.read_parquet(DYNASTY_CANDIDATES_PATH)
        out = out.merge(dyn, on=["era", "key"], how="left")
        out["n_dynasty_relatives"] = out["n_dynasty_relatives"].fillna(0).astype(int)
        for col in ["prior_relative_any", "prior_relative_elected",
                    "later_relative_any", "later_relative_elected"]:
            out[col] = out[col].fillna(False).astype(bool)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_parquet(OUT_PATH, index=False)

    n = len(out)
    print(f"Wrote {OUT_PATH}: {n} candidates")
    print(f"  own_beroep coverage: {out['own_beroep'].notna().sum()}/{n} "
          f"({out['own_beroep'].notna().mean():.1%})")
    if "own_hisclass" in out.columns:
        classified = out["own_hisclass"].notna() & (out["own_hisclass"] != -1)
        print(f"  own HISCLASS classified: {classified.sum()}/{n} ({classified.mean():.1%})")
    print(f"  father_beroep coverage: {out['father_beroep'].notna().sum()}/{n} "
          f"({out['father_beroep'].notna().mean():.1%})")
    if "father_hisclass" in out.columns:
        classified = out["father_hisclass"].notna() & (out["father_hisclass"] != -1)
        print(f"  father HISCLASS classified: {classified.sum()}/{n} ({classified.mean():.1%})")
    if "dynasty_id" in out.columns:
        n_dyn = out["dynasty_id"].notna().sum()
        print(f"  candidates in a dynasty group: {n_dyn}/{n} ({n_dyn/n:.1%})")
        print(f"  prior_relative_any: {out['prior_relative_any'].sum()}, "
              f"later_relative_any: {out['later_relative_any'].sum()}")
    print(f"  titles present: {out['titles'].notna().sum()}/{n} "
          f"({out['titles'].notna().mean():.1%})")


if __name__ == "__main__":
    build()
