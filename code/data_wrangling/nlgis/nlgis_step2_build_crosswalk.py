# =============================================================================
# nlgis_step2_build_crosswalk.py  [NLGIS PIPELINE - STEP 2]
# Input:  data/nlgis/maps/YYYY.topojson  (from step 1)
# Output: data/nlgis/crosswalk.duckdb
#           municipality_years — (year, amco, name, cbscode) long panel
#           amco_spells        — first/last observed year + name per amco
#           transitions        — dissolved amco -> successor amco (centroid
#                                containment in the first year of absence),
#                                and new amco -> predecessor amco
#         data/nlgis/municipality_years.parquet
#         data/nlgis/amco_spells.parquet
#         data/nlgis/transitions.parquet
#
# Method: decode the quantized TopoJSON per year (delta-decoded arcs ×
# transform), build shapely polygons keyed on amco (amsterdamcode). A
# municipality that disappears between year Y and Y+1 is linked to the
# municipality whose Y+1 polygon contains its year-Y representative point
# (merger). A municipality that appears in Y+1 is linked back to the polygon
# that contained its representative point in year Y (split/secession). Pure
# renames keep their amco and are visible as multiple names within one
# amco spell in `municipality_years`.
#
# Usage:
#   uv run python code/data_wrangling/nlgis/nlgis_step2_build_crosswalk.py
# =============================================================================
import json
import os

import duckdb
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon
from shapely.prepared import prep

MAPS_DIR = "./data/nlgis/maps"
OUT_DIR = "./data/nlgis"
DB_PATH = os.path.join(OUT_DIR, "crosswalk.duckdb")

FIRST_YEAR = 1848
LAST_YEAR = 1940


# ---------------------------------------------------------------------------
# Minimal TopoJSON decoder (quantized, delta-encoded arcs)
# ---------------------------------------------------------------------------

def decode_arcs(topo: dict) -> list[list[tuple[float, float]]]:
    sx, sy = topo["transform"]["scale"]
    tx, ty = topo["transform"]["translate"]
    arcs = []
    for arc in topo["arcs"]:
        x = y = 0
        pts = []
        for dx, dy in arc:
            x += dx
            y += dy
            pts.append((x * sx + tx, y * sy + ty))
        arcs.append(pts)
    return arcs


def ring_coords(ring_arcs: list[int], arcs) -> list[tuple[float, float]]:
    pts: list[tuple[float, float]] = []
    for idx in ring_arcs:
        seg = arcs[idx] if idx >= 0 else list(reversed(arcs[~idx]))
        if pts and pts[-1] == seg[0]:
            pts.extend(seg[1:])
        else:
            pts.extend(seg)
    return pts


def geom_to_shape(geom: dict, arcs):
    """TopoJSON Polygon/MultiPolygon -> shapely geometry (or None)."""
    def poly(rings):
        outer = ring_coords(rings[0], arcs)
        if len(outer) < 4:
            return None
        holes = [ring_coords(r, arcs) for r in rings[1:]]
        holes = [h for h in holes if len(h) >= 4]
        try:
            p = Polygon(outer, holes)
            return p if not p.is_empty else None
        except Exception:
            return None

    if geom["type"] == "Polygon":
        return poly(geom["arcs"])
    if geom["type"] == "MultiPolygon":
        parts = [p for p in (poly(rings) for rings in geom["arcs"]) if p]
        return MultiPolygon(parts) if parts else None
    return None


def load_year(year: int) -> dict[str, dict]:
    """Return {amco: {name, cbscode, shape}} for one year map."""
    with open(os.path.join(MAPS_DIR, f"{year}.topojson")) as f:
        topo = json.load(f)
    arcs = decode_arcs(topo)
    out = {}
    for g in topo["objects"]["nld"]["geometries"]:
        props = g.get("properties", {})
        amco = str(props.get("amsterdamcode"))
        shape = geom_to_shape(g, arcs)
        if shape is not None and not shape.is_valid:
            shape = shape.buffer(0)  # heal self-intersections from decoding
        out[amco] = {
            "name": props.get("name"),
            "cbscode": props.get("cbscode"),
            "shape": shape,
        }
    return out


# ---------------------------------------------------------------------------
# Crosswalk construction
# ---------------------------------------------------------------------------

def main() -> None:
    years = list(range(FIRST_YEAR, LAST_YEAR + 1))
    panel_rows = []
    maps: dict[int, dict[str, dict]] = {}
    for y in years:
        maps[y] = load_year(y)
        for amco, rec in maps[y].items():
            panel_rows.append((y, amco, rec["name"], rec["cbscode"]))
        if y % 10 == 0:
            print(f"  loaded {y}: {len(maps[y])} municipalities")

    panel = pd.DataFrame(panel_rows, columns=["year", "amco", "name", "cbscode"])

    spells = (
        panel.groupby("amco")
        .agg(first_year=("year", "min"), last_year=("year", "max"),
             n_years=("year", "nunique"),
             names=("name", lambda s: sorted(set(s.dropna()))))
        .reset_index()
    )
    spells["names"] = spells["names"].apply(" | ".join)

    # Transitions: compare consecutive years
    transitions = []
    for y0, y1 in zip(years[:-1], years[1:]):
        m0, m1 = maps[y0], maps[y1]
        gone = set(m0) - set(m1)
        new = set(m1) - set(m0)
        if not gone and not new:
            continue
        prepared1 = {a: prep(r["shape"]) for a, r in m1.items() if r["shape"]}
        prepared0 = {a: prep(r["shape"]) for a, r in m0.items() if r["shape"]}
        for amco in gone:
            rec = m0[amco]
            link, method = None, "unresolved"
            if rec["shape"] is not None:
                pt = rec["shape"].representative_point()
                hits = [a for a, pg in prepared1.items() if pg.contains(pt)]
                if hits:
                    link, method = hits[0], "point_in_successor"
            transitions.append((y0, y1, "dissolved", amco, rec["name"],
                                link, m1[link]["name"] if link else None, method))
        for amco in new:
            rec = m1[amco]
            link, method = None, "unresolved"
            if rec["shape"] is not None:
                pt = rec["shape"].representative_point()
                hits = [a for a, pg in prepared0.items() if pg.contains(pt)]
                if hits:
                    link, method = hits[0], "point_in_predecessor"
            transitions.append((y0, y1, "created", amco, rec["name"],
                                link, m0[link]["name"] if link else None, method))

    trans = pd.DataFrame(transitions, columns=[
        "year_before", "year_after", "event", "amco", "name",
        "linked_amco", "linked_name", "method"])

    os.makedirs(OUT_DIR, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE OR REPLACE TABLE municipality_years AS SELECT * FROM panel")
    con.execute("CREATE OR REPLACE TABLE amco_spells AS SELECT * FROM spells")
    con.execute("CREATE OR REPLACE TABLE transitions AS SELECT * FROM trans")
    con.execute(f"COPY municipality_years TO '{OUT_DIR}/municipality_years.parquet' (FORMAT PARQUET)")
    con.execute(f"COPY amco_spells TO '{OUT_DIR}/amco_spells.parquet' (FORMAT PARQUET)")
    con.execute(f"COPY transitions TO '{OUT_DIR}/transitions.parquet' (FORMAT PARQUET)")

    print(f"municipality_years: {len(panel):,} rows "
          f"({panel['amco'].nunique()} distinct amco)")
    print(f"transitions: {len(trans)} events "
          f"({(trans['method'] != 'unresolved').sum()} resolved)")
    print(trans["event"].value_counts().to_string())
    con.close()


if __name__ == "__main__":
    main()
