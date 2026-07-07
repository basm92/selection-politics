"""
ind_step06_build_lineages.py

Builds within-tree father->son lineages ("which dynasty do you belong to") from a
genealogieonline DuckDB. Network-free post-processing — run it after the crawls.

Works on the merged database (and still on either legacy crawl DB):
  * genealogie.duckdb         node table = persons      (1500-1900, merged)
  * socmob_pairs.duckdb       node table = person_urls  (1750-1900 profession-holders)
  * births_1500_1800.duckdb   node table = births       (1500-1800, after Phase C)

All expose, per person URL: a male-parent link (`father_url`), `beroep`, `birth_place`,
plus a `person_children` (parent -> male child) edge table.

If a `pairs` table is present (the merged DB), each son's dynasty_id /
generation_depth is written back onto it after the lineages are built.

Why `father_url` is the spine (not `person_children`):
  father->son requires a *male* parent. `father_url` is extracted only from a parent whose
  schema.org gender is "male", so it is gender-correct and gives <=1 father per node — a
  clean forest. `person_children` only guarantees the *child* is male (the parent's gender
  is unconstrained: ~58% of its parents are gender-unknown, an unknown share mothers), so it
  is NOT folded in wholesale. We augment the spine only with person_children edges whose
  parent is *known male* (appears as some node's father_url) and whose child has no
  father_url of its own — gender-clean, recovers a few % extra edges.

Outputs (materialised into the same DB):
  * lineage_edges   (child_url, father_url)             one row per father->son edge
  * lineages        (url, father_url, birth_place, beroep, dynasty_id, generation_depth)
                    dynasty_id = topmost ancestor URL (root of the tree the person sits in);
                    generation_depth = number of father-steps from that root.
  * lineage_pairs   VIEW: a socmob_pairs-style father/son view carrying both birth places
                    and professions (subset with both beroep IS NOT NULL == the classic pairs).

Recursion examples (run against the built tables):
  -- All ancestors of one person, oldest first:
  WITH RECURSIVE anc(url, father_url, step) AS (
      SELECT url, father_url, 0 FROM lineage_edges WHERE url = :person
      UNION ALL
      SELECT e.url, e.father_url, a.step+1
      FROM anc a JOIN lineage_edges e ON a.father_url = e.url
  ) SELECT * FROM anc;
  -- Everyone in the same dynasty:
  SELECT * FROM lineages WHERE dynasty_id = :root ORDER BY generation_depth;

Usage (from project root):
    uv run python code/data_wrangling/genealogie/ind_step06_build_lineages.py            # both DBs
    uv run python code/data_wrangling/genealogie/ind_step06_build_lineages.py data/genealogieonline/births_1500_1800.duckdb
"""
import argparse
import logging
import sys
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_DBS = [
    Path("data/genealogieonline/genealogie.duckdb"),
]

# GEDCOM data occasionally contains ancestry loops (data-entry errors). Cap the
# walk so the recursive CTE cannot spin forever; the path array also stops cycles.
MAX_DEPTH = 25


def _tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {r[0] for r in con.execute("SHOW TABLES").fetchall()}


def _columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}


def _detect_node_table(con: duckdb.DuckDBPyConnection) -> str:
    tabs = _tables(con)
    if "persons" in tabs:
        return "persons"
    if "births" in tabs:
        return "births"
    if "person_urls" in tabs:
        return "person_urls"
    raise SystemExit("No recognised node table (need `persons`, `births` or `person_urls`).")


def build(db_path: Path) -> None:
    if not db_path.exists():
        log.warning("Skipping %s — file not found", db_path)
        return

    con = duckdb.connect(str(db_path))
    node = _detect_node_table(con)
    cols = _columns(con, node)

    if "father_url" not in cols:
        raise SystemExit(
            f"{db_path.name}: `{node}` has no `father_url` column — run the Phase C "
            "enrichment crawl first."
        )
    has_children = "person_children" in _tables(con)
    has_full_name = "person_name_full" in cols

    def name_of(alias: str) -> str:
        """Best available display name, qualified by table alias."""
        if has_full_name:
            return f"COALESCE({alias}.person_name_full, {alias}.person_name)"
        return f"{alias}.person_name"

    log.info("%s: node table = %s (%s person_children)",
             db_path.name, node, "with" if has_children else "no")

    # ------------------------------------------------------------------
    # 1. Father->son edge set: father_url spine + known-male augmentation,
    #    deduped to <=1 father per child (guarantees the forest property).
    # ------------------------------------------------------------------
    aug = ""
    if has_children:
        aug = f"""
            UNION ALL
            SELECT pc.child_url AS url, pc.parent_url AS father_url
            FROM   person_children pc
            JOIN   {node} cc ON pc.child_url = cc.url
            WHERE  cc.father_url IS NULL
              AND  pc.parent_url IN (SELECT DISTINCT father_url FROM {node}
                                     WHERE father_url IS NOT NULL)
        """

    con.execute(f"""
        CREATE OR REPLACE TABLE lineage_edges AS
        WITH raw AS (
            SELECT url, father_url FROM {node} WHERE father_url IS NOT NULL
            {aug}
        ),
        ranked AS (
            SELECT url, father_url,
                   ROW_NUMBER() OVER (PARTITION BY url ORDER BY father_url) AS rn
            FROM raw
            WHERE url IS NOT NULL AND father_url IS NOT NULL AND url <> father_url
        )
        SELECT url, father_url FROM ranked WHERE rn = 1
    """)
    n_edges = con.execute("SELECT COUNT(*) FROM lineage_edges").fetchone()[0]
    log.info("  lineage_edges: %d father->son edges", n_edges)

    # ------------------------------------------------------------------
    # 2. Walk each node up to its root → (dynasty_id, generation_depth).
    #    Seed = every URL that appears as a child or as a father (roots
    #    included). Path array + MAX_DEPTH guard against cycles.
    # ------------------------------------------------------------------
    con.execute(f"""
        CREATE OR REPLACE TABLE lineages AS
        WITH RECURSIVE
        nodes AS (
            SELECT url AS u FROM lineage_edges
            UNION
            SELECT father_url FROM lineage_edges
        ),
        up(start_url, cur, depth, path) AS (
            SELECT u, u, 0, [u] FROM nodes
            UNION ALL
            SELECT up.start_url, e.father_url, up.depth + 1,
                   list_append(up.path, e.father_url)
            FROM   up
            JOIN   lineage_edges e ON up.cur = e.url
            WHERE  up.depth < {MAX_DEPTH}
              AND  NOT list_contains(up.path, e.father_url)
        ),
        rooted AS (
            SELECT start_url AS url,
                   arg_max(cur, depth) AS dynasty_id,
                   max(depth)          AS generation_depth
            FROM up
            GROUP BY start_url
        )
        SELECT r.url,
               e.father_url,
               n.birth_place,
               n.beroep,
               r.dynasty_id,
               r.generation_depth
        FROM   rooted r
        LEFT JOIN lineage_edges e ON r.url = e.url
        LEFT JOIN {node} n        ON r.url = n.url
    """)

    # ------------------------------------------------------------------
    # 3. socmob_pairs-style father/son view (carries both birth places).
    # ------------------------------------------------------------------
    con.execute(f"""
        CREATE OR REPLACE VIEW lineage_pairs AS
        SELECT e.url               AS son_url,
               {name_of('sn')}     AS son_name,
               sn.beroep           AS son_beroep,
               sn.birth_place      AS son_birth_place,
               e.father_url        AS father_url,
               {name_of('fn')}     AS father_name,
               fn.beroep           AS father_beroep,
               fn.birth_place      AS father_birth_place
        FROM   lineage_edges e
        LEFT JOIN {node} sn ON e.url = sn.url
        LEFT JOIN {node} fn ON e.father_url = fn.url
    """)

    # ------------------------------------------------------------------
    # Summary / sanity.
    # ------------------------------------------------------------------
    n_nodes  = con.execute("SELECT COUNT(*) FROM lineages").fetchone()[0]
    n_dyn    = con.execute("SELECT COUNT(DISTINCT dynasty_id) FROM lineages").fetchone()[0]
    max_d    = con.execute("SELECT max(generation_depth) FROM lineages").fetchone()[0]
    hit_cap  = con.execute(
        f"SELECT COUNT(*) FROM lineages WHERE generation_depth >= {MAX_DEPTH}"
    ).fetchone()[0]
    deep3    = con.execute(
        "SELECT COUNT(*) FROM lineages WHERE generation_depth >= 3"
    ).fetchone()[0]
    # dynasty_id should be a true root (never itself a child with a father edge);
    # the few exceptions are ancestry loops in the source GEDCOM data.
    cyclic   = con.execute(
        "SELECT COUNT(*) FROM lineages WHERE dynasty_id IN (SELECT url FROM lineage_edges)"
    ).fetchone()[0]
    log.info("  lineages: %d people, %d dynasties, max depth %s, %d people >=3 generations deep",
             n_nodes, n_dyn, max_d, deep3)
    if hit_cap:
        log.warning("  %d people hit the depth cap (%d) — likely ancestry loops in source data",
                    hit_cap, MAX_DEPTH)
    if cyclic:
        log.warning("  %d people sit in ancestry loops (dynasty_id not a clean root) — "
                    "cyclic source data, left as-is", cyclic)

    # ------------------------------------------------------------------
    # 4. Merged DB only: write each son's dynasty_id / generation_depth back
    #    onto the canonical `pairs` table (keyed on son_url), so the lineage
    #    metadata travels with the analysis pairs / cleaned parquet.
    # ------------------------------------------------------------------
    if "pairs" in _tables(con):
        con.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS dynasty_id VARCHAR")
        con.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS generation_depth INTEGER")
        con.execute("""
            UPDATE pairs
            SET dynasty_id = l.dynasty_id,
                generation_depth = l.generation_depth
            FROM lineages l
            WHERE pairs.son_url = l.url
        """)
        enriched_pairs = con.execute(
            "SELECT COUNT(*) FROM pairs WHERE dynasty_id IS NOT NULL"
        ).fetchone()[0]
        log.info("  pairs: dynasty_id/generation_depth set on %d rows", enriched_pairs)

    con.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dbs", nargs="*", type=Path, default=DEFAULT_DBS,
                    help="DuckDB file(s) to process (default: both genealogieonline DBs).")
    args = ap.parse_args()
    for db in (args.dbs or DEFAULT_DBS):
        build(Path(db))
    log.info("Done.")


if __name__ == "__main__":
    main()
