# =============================================================================
# pdc_step3_build_mp_anchor.py  [PDC PIPELINE - STEP 3]  (= Phase 2a)
# Input:  data/pdc/pdc.duckdb          (persons, functions -- step 2)
#         data/panel/panel.duckdb      (candidates_panel -- panel step 1/2)
# Output: data/pdc/pdc.duckdb
#           mp_candidates  -- PDC persons with >=1 Tweede Kamer membership
#                             span overlapping 1848-1940 (from `functions`
#                             "lid/buitengewoon lid Tweede Kamer der
#                             Staten-Generaal, van X tot Y" entries)
#         data/panel/mp_anchor.parquet
#           one row per matched (era, key) elected person:
#           era, key (persoon_id for district era, person_key for PR era),
#           pdc_url, match_tier, titulatuur, birth_date, birth_place,
#           birth_year, death_date, death_place, death_year, party_raw,
#           tk_year_min, tk_year_max
#         data/panel/mp_anchor_unmatched.parquet
#           elected persons (era, key, name, year_min, year_max) with no
#           PDC match, for Phase 2b review
#
# Method: candidates_panel's `elected` rows are grouped to one row per person
# (persoon_id for 1848-1918, person_key for 1918-1937) with a name and the
# span of years they won a seat. Each is matched against `mp_candidates` by
# normalised (surname, initials): exact match (tier 1), else same surname +
# first initial disambiguated by year-span overlap (tier 2), else Levenshtein
# <=2 surname fuzzy match + first initial (tier 3, catches OCR/typo slips on
# either side). Name normalisation strips diacritics, non-letters, and a
# fixed set of noble-rank words (jhr, ridder, baron, graaf, ...) that PDC and
# Huygens include inconsistently.
#
# Usage:
#   uv run python code/data_wrangling/pdc/pdc_step3_build_mp_anchor.py
# =============================================================================
import os
import re
import unicodedata

import duckdb
import pandas as pd

PDC_DB = "./data/pdc/pdc.duckdb"
PANEL_DB = "./data/panel/panel.duckdb"
OUT_DIR = "./data/panel"

STUDY_START, STUDY_END = 1848, 1940

_MONTHS = {
    "januari": 1, "februari": 2, "maart": 3, "april": 4, "mei": 5, "juni": 6,
    "juli": 7, "augustus": 8, "september": 9, "oktober": 10, "november": 11,
    "december": 12,
}

_NOBLE_WORDS = {"jhr", "jonkheer", "ridder", "baron", "barones", "graaf",
                "gravin", "freule"}

_TITLE_TOKEN_RE = re.compile(r"^([A-Za-z]{2,6}\.)+$")
# Dutch initials are usually a single letter ("J.") but digraph name sounds
# are abbreviated as two letters ("Th." Theodoor, "Ch." Christiaan, "Ph."
# Philip) -- allow an optional lowercase second letter per initial unit.
_INITIALS_TOKEN_RE = re.compile(r"^([A-Z][a-z]?\.){1,8}$")

_TK_RANGE_RE = re.compile(
    r"^(?:buitengewoon )?lid Tweede Kamer der Staten-Generaal,\s*"
    r"van\s+(?P<van>.+?)\s+tot\s+(?P<tot>.+?)(?:\s*\((?P<extra>.+)\))?$"
)
_TK_ONGOING_RE = re.compile(
    r"^(?:buitengewoon )?lid Tweede Kamer der Staten-Generaal,\s*"
    r"vanaf\s+(?P<van>.+?)(?:\s*\((?P<extra>.+)\))?$"
)


# --- name parsing / normalisation --------------------------------------------
def parse_name(titulatuur: str | None, voornamen: str | None = None):
    """'Dr.Mr. J.R. Thorbecke' -> ('J.R.', 'Thorbecke'); strips a redundant
    appended voornamen tail some PDC pages carry in `titulatuur en naam`."""
    if not isinstance(titulatuur, str) or not titulatuur:
        return None, None
    toks = titulatuur.split()
    i = 0
    while i < len(toks) and _TITLE_TOKEN_RE.match(toks[i]):
        i += 1
    initials = None
    if i < len(toks) and _INITIALS_TOKEN_RE.match(toks[i]):
        initials = toks[i]
        i += 1
    surname = " ".join(toks[i:]).strip()
    if isinstance(voornamen, str) and voornamen and surname.endswith(voornamen):
        surname = surname[: -len(voornamen)].strip()
    return initials, (surname or None)


def norm_surname(s: str | None) -> str:
    """Normalise for cross-source comparison: strip diacritics/case/noble
    rank words, and fold the historical Dutch y/ij spelling variance
    ('Ravesteijn'/'Ravesteyn', 'Duijs'/'Duys') to a single canonical form."""
    if not isinstance(s, str) or not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    words = re.findall(r"[a-zA-Z]+", s.lower())
    words = [w for w in words if w not in _NOBLE_WORDS]
    return "".join(words).replace("ij", "y")


def norm_ini(s: str | None) -> str:
    if not isinstance(s, str) or not s:
        return ""
    return re.sub(r"[^A-Za-z]", "", s).upper()


def _lev(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a or not b:
        return len(a) + len(b)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


# --- date parsing -------------------------------------------------------------
def _year(s: str | None) -> int | None:
    if not s:
        return None
    m = re.search(r"\d{4}", s)
    return int(m.group()) if m else None


def parse_place_date(raw: str | None):
    """'Zwolle, 14 januari 1798' -> ('Zwolle', '1798-01-14', 1798)."""
    if not isinstance(raw, str) or not raw.strip():
        return None, None, None
    raw = raw.strip()
    if "," in raw:
        place, date_part = raw.rsplit(",", 1)
        place, date_part = place.strip() or None, date_part.strip()
    elif re.search(r"\d{4}", raw):
        place, date_part = None, raw
    else:
        place, date_part = raw, ""
    m = re.search(r"(?:(\d{1,2})\s+([a-zA-Z]+)\s+)?(\d{4})", date_part)
    if not m:
        return place, None, None
    day, month_name, year = m.groups()
    year = int(year)
    date_iso = None
    if day and month_name:
        month = _MONTHS.get(month_name.lower())
        if month:
            date_iso = f"{year:04d}-{month:02d}-{int(day):02d}"
    return place, date_iso, year


# --- step 3a: mp_candidates (PDC persons who were ever a TK member 1848-1940)
def build_mp_candidates(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    persons = con.execute(
        "SELECT url, slug, titulatuur, voornamen, geboorte_raw, overlijden_raw, "
        "partij_raw FROM persons").fetchdf()
    functions = con.execute(
        "SELECT url, text FROM functions").fetchdf()

    spans: dict[str, list[tuple[int | None, int | None]]] = {}
    for url, text in zip(functions["url"], functions["text"]):
        m = _TK_RANGE_RE.match(text)
        if m:
            spans.setdefault(url, []).append((_year(m.group("van")), _year(m.group("tot"))))
            continue
        m = _TK_ONGOING_RE.match(text)
        if m:
            spans.setdefault(url, []).append((_year(m.group("van")), 9999))

    rows = []
    for url, sp in spans.items():
        years = [y for pair in sp for y in pair if y is not None]
        if not years:
            continue
        y_min, y_max = min(years), max(years)
        if y_max < STUDY_START or y_min > STUDY_END:
            continue
        rows.append({"url": url, "tk_year_min": y_min, "tk_year_max": y_max})
    tk = pd.DataFrame(rows)

    mp = persons.merge(tk, on="url", how="inner")
    ini_sn = mp["titulatuur"].combine(mp["voornamen"], parse_name)
    mp["initials_raw"] = ini_sn.map(lambda t: t[0])
    mp["surname_raw"] = ini_sn.map(lambda t: t[1])
    mp["sn"] = mp["surname_raw"].map(norm_surname)
    mp["ini"] = mp["initials_raw"].map(norm_ini)
    mp["fi"] = mp["ini"].str[:1]

    bd = mp["geboorte_raw"].map(parse_place_date)
    mp["birth_place"] = bd.map(lambda t: t[0])
    mp["birth_date"] = bd.map(lambda t: t[1])
    mp["birth_year"] = bd.map(lambda t: t[2])
    dd = mp["overlijden_raw"].map(parse_place_date)
    mp["death_place"] = dd.map(lambda t: t[0])
    mp["death_date"] = dd.map(lambda t: t[1])
    mp["death_year"] = dd.map(lambda t: t[2])
    return mp


# --- step 3b: elected-person roster from candidates_panel --------------------
def build_elected_persons(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    district = con.execute("""
        SELECT 'district_1848_1918' AS era, CAST(persoon_id AS VARCHAR) AS key,
               any_value(name_clean) AS name_for_parse,
               MIN(year) AS year_min, MAX(year) AS year_max
        FROM panel.candidates_panel
        WHERE elected AND era = 'district_1848_1918'
        GROUP BY persoon_id
    """).fetchdf()
    district[["ini_raw", "sn_raw"]] = pd.DataFrame(
        district["name_for_parse"].map(lambda n: parse_name(n)).tolist(),
        index=district.index)

    pr = con.execute("""
        SELECT 'pr_1918_1937' AS era, person_key AS key,
               any_value(surname) AS sn_raw, any_value(initials) AS ini_raw,
               MIN(year) AS year_min, MAX(year) AS year_max
        FROM panel.candidates_panel
        WHERE elected AND era = 'pr_1918_1937'
        GROUP BY person_key
    """).fetchdf()

    ep = pd.concat([district[["era", "key", "sn_raw", "ini_raw", "year_min", "year_max"]],
                     pr[["era", "key", "sn_raw", "ini_raw", "year_min", "year_max"]]],
                    ignore_index=True)
    ep["sn"] = ep["sn_raw"].map(norm_surname)
    ep["ini"] = ep["ini_raw"].map(norm_ini)
    ep["fi"] = ep["ini"].str[:1]
    return ep


def _year_overlap(a_min, a_max, b_min, b_max) -> int:
    """Overlap length in years (>=0); negative gap turned into 0."""
    return max(0, min(a_max, b_max) - max(a_min, b_min) + 1)


def match(ep: pd.DataFrame, mp: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    by_full = mp.groupby(["sn", "ini"]).indices
    by_fi = mp.groupby(["sn", "fi"]).indices

    matches = []
    unmatched = []
    for row in ep.itertuples():
        sn, ini, fi = row.sn, row.ini, row.fi
        cand_idx = None
        tier = None
        if (sn, ini) in by_full:
            cand_idx = list(by_full[(sn, ini)])
            tier = 1
        elif (sn, fi) in by_fi:
            cand_idx = list(by_fi[(sn, fi)])
            tier = 2
        else:
            pool = mp[mp["fi"] == fi]
            if len(pool) and sn:
                tol = 2 if len(sn) >= 5 else 1
                dists = pool["sn"].map(lambda s: _lev(s, sn))
                hit = pool[dists <= tol]
                if len(hit):
                    cand_idx = list(hit.index)
                    tier = 3
        if cand_idx is None:
            unmatched.append(row)
            continue
        cands = mp.loc[cand_idx]
        if len(cands) > 1:
            overlaps = cands.apply(
                lambda c: _year_overlap(row.year_min, row.year_max,
                                         c["tk_year_min"], c["tk_year_max"]), axis=1)
            best = overlaps.idxmax()
            cands = cands.loc[[best]]
        c = cands.iloc[0]
        matches.append({
            "era": row.era, "key": row.key, "match_tier": tier,
            "pdc_url": c["url"], "titulatuur": c["titulatuur"],
            "birth_date": c["birth_date"], "birth_place": c["birth_place"],
            "birth_year": c["birth_year"], "death_date": c["death_date"],
            "death_place": c["death_place"], "death_year": c["death_year"],
            "party_raw": c["partij_raw"],
            "tk_year_min": c["tk_year_min"], "tk_year_max": c["tk_year_max"],
        })
    matched_df = pd.DataFrame(matches)
    unmatched_df = pd.DataFrame([{
        "era": r.era, "key": r.key, "name_raw": r.sn_raw, "initials_raw": r.ini_raw,
        "year_min": r.year_min, "year_max": r.year_max,
    } for r in unmatched])
    return matched_df, unmatched_df


def main() -> None:
    con = duckdb.connect(PDC_DB)
    con.execute(f"ATTACH '{PANEL_DB}' AS panel (READ_ONLY)")

    mp = build_mp_candidates(con)
    print(f"mp_candidates: {len(mp)} PDC persons with a TK span overlapping "
          f"{STUDY_START}-{STUDY_END}")

    con.register("mp_df", mp)
    con.execute("CREATE OR REPLACE TABLE mp_candidates AS SELECT * FROM mp_df")

    ep = build_elected_persons(con)
    print(f"elected persons in candidates_panel: {len(ep)} "
          f"({(ep['era']=='district_1848_1918').sum()} district, "
          f"{(ep['era']=='pr_1918_1937').sum()} PR)")

    matched, unmatched = match(ep, mp)

    os.makedirs(OUT_DIR, exist_ok=True)
    matched.to_parquet(f"{OUT_DIR}/mp_anchor.parquet", index=False)
    unmatched.to_parquet(f"{OUT_DIR}/mp_anchor_unmatched.parquet", index=False)

    print("\n=== match rate by era ===")
    rep = ep.merge(matched[["era", "key"]].assign(matched=True),
                    on=["era", "key"], how="left")
    rep["matched"] = rep["matched"].fillna(False)
    print(rep.groupby("era")["matched"].agg(["sum", "count", "mean"])
          .rename(columns={"sum": "matched", "count": "total", "mean": "rate"})
          .to_string())

    print("\n=== match tier breakdown ===")
    print(matched["match_tier"].value_counts().sort_index().to_string())

    print(f"\nmp_anchor.parquet: {len(matched)} rows")
    print(f"mp_anchor_unmatched.parquet: {len(unmatched)} rows")
    con.close()


if __name__ == "__main__":
    main()
