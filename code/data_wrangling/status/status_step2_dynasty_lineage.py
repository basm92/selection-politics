# =============================================================================
# status_step2_dynasty_lineage.py  [STATUS PIPELINE - STEP 2]  (Phase 3)
# Input:  data/genealogieonline/genealogieonline.duckdb  candidate_ancestors,
#           person_pages  (genealogieonline step 2)
#         data/panel/candidate_roster.parquet  (year_min/year_max, elected)
# Output: data/panel/dynasty_edges.parquet
#           era_a, key_a, era_b, key_b, shared_url, depth_a, depth_b,
#           combined_depth, relation_label, same_person_flag
#         data/panel/dynasty_candidates.parquet
#           era, key, dynasty_id, n_dynasty_relatives,
#           prior_relative_any, prior_relative_elected,
#           later_relative_any, later_relative_elected
#
# Dynasty definition (per Phase 2b/3 checkpoint discussion): two candidates
# belong to the same dynasty if their PATRILINEAL ancestor chains (father ->
# father -> ..., the only line genealogieonline_step2 follows, matching
# ind_step06's gender-clean spine rationale) meet at a common node within a
# COMBINED depth of 3 -- i.e. depth_a + depth_b <= 3 where depth 0 is the
# candidate's own matched person. This covers parent-child (1), grandparent-
# grandchild (2), siblings/shared-father (1+1=2), great-grandparent-
# descendant (3), and uncle-nephew/shared-grandfather (1+2=3). It does NOT
# cover first cousins (2+2=4) or anything via the maternal line -- a real
# coverage gap, documented rather than silently widened.
#
# depth_a=0 AND depth_b=0 (two DIFFERENT candidates resolved to the identical
# genealogieonline url) is not dynasty evidence -- it means panel step 4's
# entity resolution matched two distinct real people to one tree node, almost
# certainly a false-positive linkage, not a shared ancestor. These are kept
# in dynasty_edges with same_person_flag=TRUE for QA but excluded from the
# connected-components / per-candidate indicators below.
#
# Connected components (Union-Find) group candidates transitively (A-B share
# an ancestor, B-C share a different one -> A,B,C are one dynasty), matching
# ind_step06's dynasty_id = root-of-tree convention loosely (here: canonical
# id = the lexicographically smallest (era,key) in the component).
#
# Usage:
#   uv run python code/data_wrangling/status/status_step2_dynasty_lineage.py
# =============================================================================
import os

import duckdb
import pandas as pd

GENEALOGIE_DB = "./data/genealogieonline/genealogieonline.duckdb"
ROSTER_PATH = "./data/panel/candidate_roster.parquet"
EDGES_OUT = "./data/panel/dynasty_edges.parquet"
CANDIDATES_OUT = "./data/panel/dynasty_candidates.parquet"

MAX_COMBINED_DEPTH = 3

_RELATION_LABELS = {
    (0, 0): "same_matched_person",
    (0, 1): "parent-child",
    (0, 2): "grandparent-grandchild",
    (0, 3): "great-grandparent-descendant",
    (1, 1): "siblings_or_shared_father",
    (1, 2): "uncle-nephew_or_shared_grandfather",
}


class UnionFind:
    def __init__(self):
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def build_edges(con) -> pd.DataFrame:
    raw = con.execute("""
        SELECT a.era AS era_a, a.key AS key_a, b.era AS era_b, b.key AS key_b,
               a.url AS shared_url, a.depth AS depth_a, b.depth AS depth_b
        FROM candidate_ancestors a
        JOIN candidate_ancestors b
          ON a.url = b.url
         AND (a.era, a.key) < (b.era, b.key)
        WHERE a.depth + b.depth <= ?
    """, [MAX_COMBINED_DEPTH]).df()

    if raw.empty:
        return raw

    raw["combined_depth"] = raw["depth_a"] + raw["depth_b"]
    raw["same_person_flag"] = (raw["depth_a"] == 0) & (raw["depth_b"] == 0)
    raw["relation_label"] = raw.apply(
        lambda r: _RELATION_LABELS.get(
            (min(r.depth_a, r.depth_b), max(r.depth_a, r.depth_b)), "other"
        ),
        axis=1,
    )
    return raw


def build_candidate_indicators(edges: pd.DataFrame, roster: pd.DataFrame) -> pd.DataFrame:
    roster = roster.set_index(["era", "key"])
    real_edges = edges[~edges["same_person_flag"]]

    uf = UnionFind()
    all_nodes = set()
    for r in real_edges.itertuples():
        a, b = f"{r.era_a}|{r.key_a}", f"{r.era_b}|{r.key_b}"
        uf.union(a, b)
        all_nodes.add(a)
        all_nodes.add(b)

    relatives: dict[str, set[str]] = {}
    for r in real_edges.itertuples():
        a, b = f"{r.era_a}|{r.key_a}", f"{r.era_b}|{r.key_b}"
        relatives.setdefault(a, set()).add(b)
        relatives.setdefault(b, set()).add(a)

    rows = []
    for node in all_nodes:
        era, key = node.split("|", 1)
        dynasty_id = uf.find(node)
        rels = relatives.get(node, set())

        try:
            own = roster.loc[(era, key)]
            own_year_min, own_year_max = own["year_min"], own["year_max"]
        except KeyError:
            own_year_min, own_year_max = None, None

        prior_any = prior_elected = later_any = later_elected = False
        for rel in rels:
            rel_era, rel_key = rel.split("|", 1)
            try:
                r_row = roster.loc[(rel_era, rel_key)]
            except KeyError:
                continue
            if own_year_min is not None and pd.notna(r_row["year_max"]) and r_row["year_max"] < own_year_min:
                prior_any = True
                if bool(r_row["elected"]):
                    prior_elected = True
            if own_year_max is not None and pd.notna(r_row["year_min"]) and r_row["year_min"] > own_year_max:
                later_any = True
                if bool(r_row["elected"]):
                    later_elected = True

        rows.append({
            "era": era, "key": key, "dynasty_id": dynasty_id,
            "n_dynasty_relatives": len(rels),
            "prior_relative_any": prior_any, "prior_relative_elected": prior_elected,
            "later_relative_any": later_any, "later_relative_elected": later_elected,
        })
    return pd.DataFrame(rows)


def build() -> None:
    con = duckdb.connect(GENEALOGIE_DB, read_only=True)
    edges = build_edges(con)
    con.close()

    os.makedirs(os.path.dirname(EDGES_OUT), exist_ok=True)
    edges.to_parquet(EDGES_OUT, index=False)
    print(f"Wrote {EDGES_OUT}: {len(edges)} edges "
          f"({int(edges['same_person_flag'].sum()) if not edges.empty else 0} "
          f"same-person QA flags)")

    roster = pd.read_parquet(ROSTER_PATH)
    candidates = build_candidate_indicators(edges, roster)
    candidates.to_parquet(CANDIDATES_OUT, index=False)

    n_dynasty = candidates["dynasty_id"].nunique()
    print(f"Wrote {CANDIDATES_OUT}: {len(candidates)} candidates in "
          f"{n_dynasty} dynasty groups")
    print(f"  prior_relative_any: {candidates['prior_relative_any'].sum()}, "
          f"later_relative_any: {candidates['later_relative_any'].sum()}")


if __name__ == "__main__":
    build()
