# =============================================================================
# panel_step6_electoral_environment.py  [PANEL PIPELINE - STEP 6]  (Phase 4)
# Input:  data/panel/candidates_panel.parquet  (panel step 1+2, unified
#           1848-1937 district+PR panel)
# Output: data/panel/electoral_environment.parquet
#           era, key, year, race_key (district: uitslag_id; PR: kieskring_name
#             |lijst_no), plus era-specific columns documented below.
#
# Row grain is CANDIDACY (era, key, year, race_key), NOT candidate -- unlike
# candidate_status.parquet (one row per roster candidate, aggregated across
# their whole career), electoral environment varies every time a candidate
# stands, so it must stay at the same grain as candidates_panel itself. Join
# back to candidates_panel on (era, key-equivalent, year, race identifiers)
# for analysis; join to candidate_status on (era, key) for status covariates.
#
# District era (1848-1918), grouped by uitslag_id (one race/round):
#   race_n_candidates, race_total_votes, enc (effective number of candidates,
#     Laakso-Taagepera 1/sum(share^2))
#   is_runoff_round (type == 'herstemming')
#   had_runoff (bool, this district+year combo includes both a non-runoff AND
#     a herstemming round -- i.e. no one cleared kiesdrempel in round 1)
#   margin_to_kiesdrempel_votes/_pct (own votes/pct minus the majority
#     threshold; only meaningful in non-runoff rounds, kiesdrempel is a
#     first-round-only majority test, NULL in herstemming rounds since the
#     duckdb source column is itself NULL there)
#   margin_of_victory_pct (RACE-level, not candidate-level: pct(rank==zetels)
#     minus pct(rank==zetels+1), i.e. the vote-share gap between the most
#     marginal winner and the most marginal loser -- the Lee(2008)-style
#     closeness measure for a close-election RD. NULL when the race has
#     <=zetels candidates, i.e. uncontested/no losing side exists)
#   is_marginal_winner / is_marginal_loser (this candidate IS the rank==zetels
#     / rank==zetels+1 observation that margin_of_victory_pct was computed
#     from -- the actual RD analysis sample is is_marginal_winner |
#     is_marginal_loser, not "everyone in a race with a small margin")
#
# PR era (1918-1937), grouped by (kieskring_name, lijst_no, year) = one list:
#   list_length (n candidates on this list)
#   relative_position (positie / list_length, 0=top of list)
#   list_total_votes (sum of candidate preference votes on this list --
#     A PROXY for list/party strength: candidate_panel carries NO party name
#     for the PR era (affiliation is 100% NULL, see CLAUDE.md data-sources
#     table), so this is the best available stand-in, and it is a lower bound
#     on true list votes since preference votes were optional pre-1937 and
#     most electors just vote the top name)
#   list_rank_in_kieskring (rank of list_total_votes among lists competing in
#     the same kieskring+year -- larger party proxy)
#   elected_cutoff_position (max positie with elected==True on this list --
#     descriptive only: Dutch interwar PR seats were awarded overwhelmingly
#     by LIST ORDER, which the party chose, not by a random/vote-margin
#     cutoff, so position-based "margins" here are NOT a clean RD running
#     variable the way district-era vote margins are -- see design memo)
#
# Usage:
#   uv run python code/data_wrangling/panel/panel_step6_electoral_environment.py
# =============================================================================
import os

import duckdb
import pandas as pd

PANEL_DB = "./data/panel/panel.duckdb"
OUT_PATH = "./data/panel/electoral_environment.parquet"


def build_district(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    d = con.execute("""
        SELECT CAST(persoon_id AS VARCHAR) AS key, year, district_id, district,
               uitslag_id, type, rank, zetels, kiesdrempel, votes, pct, elected
        FROM candidates_panel
        WHERE era = 'district_1848_1918'
    """).df()
    d["era"] = "district_1848_1918"
    d["race_key"] = d["uitslag_id"].astype(str)

    # race-level aggregates
    race = d.groupby("uitslag_id").agg(
        race_n_candidates=("key", "size"),
        race_total_votes=("votes", "sum"),
    ).reset_index()
    d = d.merge(race, on="uitslag_id", how="left")
    d["_share"] = d["votes"] / d["race_total_votes"]
    enc = (
        d.groupby("uitslag_id")["_share"]
        .apply(lambda s: 1.0 / (s.dropna() ** 2).sum() if s.notna().any() else pd.NA)
        .rename("enc")
    )
    d = d.merge(enc, on="uitslag_id", how="left")
    d = d.drop(columns=["_share"])

    d["is_runoff_round"] = d["type"] == "herstemming"
    d["margin_to_kiesdrempel_votes"] = d["votes"] - d["kiesdrempel"]
    d["margin_to_kiesdrempel_pct"] = (
        (d["votes"] - d["kiesdrempel"]) / d["race_total_votes"] * 100
    )

    had_runoff = (
        d.groupby(["district_id", "year"])["type"]
        .apply(lambda s: ("herstemming" in set(s)) and (len(set(s) - {"herstemming"}) > 0))
        .rename("had_runoff")
    )
    d = d.merge(had_runoff, on=["district_id", "year"], how="left")

    # race-level RD closeness: pct at rank==zetels vs rank==zetels+1
    def race_margin(g: pd.DataFrame) -> pd.Series:
        zetels = g["zetels"].iloc[0]
        winner = g.loc[g["rank"] == zetels, "pct"]
        loser = g.loc[g["rank"] == zetels + 1, "pct"]
        if winner.empty or loser.empty:
            return pd.Series({"margin_of_victory_pct": pd.NA})
        return pd.Series({"margin_of_victory_pct": winner.iloc[0] - loser.iloc[0]})

    margin = d.groupby("uitslag_id").apply(race_margin, include_groups=False).reset_index()
    d = d.merge(margin, on="uitslag_id", how="left")
    d["is_marginal_winner"] = d["rank"] == d["zetels"]
    d["is_marginal_loser"] = d["rank"] == d["zetels"] + 1

    keep = [
        "era", "key", "year", "race_key", "district_id", "district", "type",
        "race_n_candidates", "race_total_votes", "enc", "is_runoff_round",
        "had_runoff", "kiesdrempel", "margin_to_kiesdrempel_votes",
        "margin_to_kiesdrempel_pct", "margin_of_victory_pct",
        "is_marginal_winner", "is_marginal_loser", "rank", "zetels", "votes",
        "pct", "elected",
    ]
    return d[keep]


def build_pr(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    p = con.execute("""
        SELECT person_key AS key, year, kieskring_name, lijst_no, positie,
               votes, pct, elected
        FROM candidates_panel
        WHERE era = 'pr_1918_1937'
    """).df()
    p["era"] = "pr_1918_1937"
    p["race_key"] = p["kieskring_name"].astype(str) + "|" + p["lijst_no"].astype(str)

    list_stats = p.groupby(["kieskring_name", "lijst_no", "year"]).agg(
        list_length=("key", "size"),
        list_total_votes=("votes", "sum"),
        elected_cutoff_position=("positie", lambda s: s[p.loc[s.index, "elected"]].max()
                                  if p.loc[s.index, "elected"].any() else pd.NA),
    ).reset_index()
    p = p.merge(list_stats, on=["kieskring_name", "lijst_no", "year"], how="left")
    p["relative_position"] = p["positie"] / p["list_length"]

    kieskring_totals = p.drop_duplicates(subset=["kieskring_name", "lijst_no", "year"]).groupby(
        ["kieskring_name", "year"]
    )["list_total_votes"].sum().rename("kieskring_total_votes").reset_index()
    p = p.merge(kieskring_totals, on=["kieskring_name", "year"], how="left")

    list_rank = (
        p.drop_duplicates(subset=["kieskring_name", "lijst_no", "year"])
        .sort_values(["kieskring_name", "year", "list_total_votes"], ascending=[True, True, False])
        .assign(list_rank_in_kieskring=lambda df: df.groupby(["kieskring_name", "year"]).cumcount() + 1)
        [["kieskring_name", "lijst_no", "year", "list_rank_in_kieskring"]]
    )
    p = p.merge(list_rank, on=["kieskring_name", "lijst_no", "year"], how="left")

    keep = [
        "era", "key", "year", "race_key", "kieskring_name", "lijst_no",
        "positie", "list_length", "relative_position", "list_total_votes",
        "list_rank_in_kieskring", "kieskring_total_votes",
        "elected_cutoff_position", "votes", "pct", "elected",
    ]
    return p[keep]


def build() -> None:
    con = duckdb.connect(PANEL_DB, read_only=True)
    district = build_district(con)
    pr = build_pr(con)
    con.close()

    out = pd.concat([district, pr], ignore_index=True)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_parquet(OUT_PATH, index=False)

    n = len(out)
    print(f"Wrote {OUT_PATH}: {n} candidacy-rows "
          f"({len(district)} district, {len(pr)} PR)")
    print(f"  district: had_runoff races (candidacy-rows): "
          f"{district['had_runoff'].sum()}/{len(district)}")
    close = district["margin_of_victory_pct"].abs() <= 5
    print(f"  district: |margin_of_victory_pct|<=5pp candidacy-rows: "
          f"{close.sum()}/{district['margin_of_victory_pct'].notna().sum()} "
          f"non-null margins")
    print(f"  district: is_marginal_winner|is_marginal_loser rows: "
          f"{(district['is_marginal_winner'] | district['is_marginal_loser']).sum()}")
    print(f"  pr: mean list_length: {pr['list_length'].mean():.1f}, "
          f"mean relative_position: {pr['relative_position'].mean():.2f}")


if __name__ == "__main__":
    build()
