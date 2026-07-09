# =============================================================================
# panel_step4_candidate_person_pairs.py  [PANEL PIPELINE - STEP 4]  (Phase 2b)
# Input:  data/panel/candidate_roster.parquet        (panel step 3)
#         data/openarch/openarch.duckdb               (openarch step 1)
#         data/genealogieonline/genealogieonline.duckdb (genealogieonline step 1)
# Output: data/panel/candidate_person_pairs.parquet
#           candidate_key (era, key), source ('openarch'/'genealogieonline'),
#           person_ref (openarch record `identifier`; genealogieonline
#           person-page `url`), score, feature_* columns, plus the raw
#           matched name/year/place for audit.
#         data/panel/candidate_person_pairs_summary.parquet
#           one row per candidate: n_pairs, best_score, has_anchor
#
# Confidence-scored, NOT filtered: every OpenArchieven "own record" hit
# (BS Geboorte relationtype=Kind; BS Huwelijk relationtype=Bruid/Bruidegom)
# and every GenealogieOnline search hit is kept as a candidate_person_pairs
# row with a score -- thresholding happens downstream (Phase 3+), matching
# phase_2_and_onward.md's "confidence-scored pairs, not filtered matches".
#
# Score = weighted sum of:
#   feat_surname   - normalised surname match (exact=1.0, else 1-lev/len)
#   feat_initials  - candidate's initials vs. initials derived from the
#                    hit's given names (splitting the hit's full name on the
#                    candidate's own searched surname)
#   feat_year      - birth-year agreement: exact diff-based score against
#                    mp_anchor's PDC birth_year when available, else a
#                    boolean "falls in the heuristic [lo,hi] window" score
#                    (openarch marriage hits, which carry a marriage year
#                    rather than a birth year, get a weaker plausible-age
#                    check instead)
#   feat_place     - normalised place match (openarch eventplace vs.
#                    district/residence; genealogieonline search hits carry
#                    no place at the search-list level, so this feature is
#                    NULL there and the weight is redistributed)
# Weights are a reasonable starting point (documented below), meant to be
# refit after the hand-labelled calibration sample (phase_2_and_onward.md).
#
# Usage:
#   uv run python code/data_wrangling/panel/panel_step4_candidate_person_pairs.py
# =============================================================================
import os
import re
import sys

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from name_match_utils import lev, norm_ini, norm_place, norm_surname

OUT_DIR = "./data/panel"
ROSTER_PATH = f"{OUT_DIR}/candidate_roster.parquet"
OPENARCH_DB = "./data/openarch/openarch.duckdb"
GENEALOGIE_DB = "./data/genealogieonline/genealogieonline.duckdb"

WEIGHTS = {"surname": 0.40, "initials": 0.30, "year": 0.20, "place": 0.10}

OWN_RELATIONTYPE = {
    "BS Geboorte": {"Kind"},
    "BS Huwelijk": {"Bruid", "Bruidegom"},
}


_INVALID_HIT_RE = re.compile(
    r"levenloos|doodgeboren|^\d+e:.*;\s*\d+e:", re.IGNORECASE)


def is_invalid_hit(fullname: str | None) -> bool:
    """Stillborn/deceased-infant registrations and OpenArchieven's combined
    '1e: X; 2e: Y' remarriage-listing format can't be the candidate
    themselves -- exclude outright rather than merely scoring them down."""
    return bool(fullname) and bool(_INVALID_HIT_RE.search(fullname))


def split_given_surname(fullname: str, target_surname_norm: str) -> tuple[list[str], list[str]]:
    """Split a 'Given Names Surname' string into (given_tokens, surname_tokens)
    by finding the trailing window whose normalised form matches the
    candidate's own normalised surname (falls back to 'last token only' if
    no window matches, e.g. a spelling variant the search backend accepted).
    Window capped at 6 tokens to cover long compound noble surnames (e.g.
    "van der Maesen de Sombreff" is 5 words)."""
    tokens = fullname.split()
    for k in range(min(6, len(tokens)), 0, -1):
        if norm_surname(" ".join(tokens[-k:])) == target_surname_norm:
            return tokens[:-k], tokens[-k:]
    if len(tokens) > 1:
        return tokens[:-1], tokens[-1:]
    return [], tokens


def initials_from_tokens(tokens: list[str]) -> str:
    return "".join(t[0].upper() for t in tokens if t and t[0].isalpha())


def score_surname(hit_surname_norm: str, cand_sn: str) -> float:
    if not hit_surname_norm or not cand_sn:
        return 0.0
    if hit_surname_norm == cand_sn:
        return 1.0
    d = lev(hit_surname_norm, cand_sn)
    return max(0.0, 1 - d / max(len(hit_surname_norm), len(cand_sn)))


def score_initials(derived_ini: str, cand_ini: str) -> float:
    """Fraction of the CANDIDATE's initials that the hit's given names cover,
    scaled down slightly (0.9x) when the hit has extra/fewer initials than
    the candidate so a full, exact-length match still ranks highest. A hit
    covering only 1 of 3 candidate initials scores much lower than one
    covering 2 of 3 (a flat 'any prefix match' bonus previously treated both
    the same -- found during hand-labelled calibration, 2026-07-08)."""
    if not derived_ini or not cand_ini:
        return 0.0
    if derived_ini == cand_ini:
        return 1.0
    n_match = sum(1 for a, b in zip(derived_ini, cand_ini) if a == b)
    coverage = n_match / len(cand_ini)
    if derived_ini.startswith(cand_ini) or cand_ini.startswith(derived_ini):
        return 0.9 * coverage + 0.1  # still rewarded, but scaled by coverage
    return n_match / max(len(derived_ini), len(cand_ini))


def score_place(hit_place_norm: str, cand_place_norm: str) -> float | None:
    if not hit_place_norm or not cand_place_norm:
        return None
    if hit_place_norm == cand_place_norm:
        return 1.0
    d = lev(hit_place_norm, cand_place_norm)
    return max(0.0, 1 - d / max(len(hit_place_norm), len(cand_place_norm)))


def weighted_score(feats: dict) -> float:
    """Weighted average of the present features, then GATED by surname and
    initials: a plain average lets a hit with a wrong surname or completely
    wrong initials (e.g. a female first name against what's almost certainly
    a male 19th-c. MP) still score 0.6-0.9 whenever the other features line
    up by coincidence -- hand-labelled calibration (2026-07-08) found this
    was the single largest source of false matches, especially for common
    surnames. Both gates are needed: a single-initial candidate ("A.") gives
    ANY same-first-letter hit a perfect initials score regardless of surname,
    so a bad surname match must also be suppressed on its own. Each gate
    floors at 0.3x so a partial match still surfaces for review rather than
    being hard-excluded, per the project's confidence-scored, not filtered,
    principle."""
    present = {k: v for k, v in feats.items() if v is not None}
    if not present:
        return 0.0
    w = {k: WEIGHTS[k] for k in present}
    tot_w = sum(w.values())
    avg = sum(present[k] * w[k] for k in present) / tot_w
    gate = 1.0
    for key in ("surname", "initials"):
        val = present.get(key)
        if val is not None:
            gate *= 0.3 + 0.7 * val
    return avg * gate


def build_openarch_pairs(roster: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect(OPENARCH_DB, read_only=True)
    hits = con.execute("""
        SELECT era, key, sourcetype, identifier, personname, relationtype,
               event_year, eventplace
        FROM hits
    """).fetchdf()
    con.close()

    own_mask = hits.apply(
        lambda r: r["relationtype"] in OWN_RELATIONTYPE.get(r["sourcetype"], set()),
        axis=1)
    hits = hits[own_mask & ~hits["personname"].map(is_invalid_hit)]
    hits = hits.merge(roster, on=["era", "key"], how="inner")
    if hits.empty:
        return hits

    rows = []
    for r in hits.itertuples():
        given, surname_tokens = split_given_surname(r.personname or "", r.sn)
        derived_ini = norm_ini(initials_from_tokens(given))
        feat_surname = score_surname(norm_surname(" ".join(surname_tokens)), r.sn)
        feat_initials = score_initials(derived_ini, r.ini)

        feat_year = None
        if r.sourcetype == "BS Geboorte" and r.event_year:
            if r.has_birth_anchor and pd.notna(r.birth_year):
                feat_year = max(0.0, 1 - abs(r.event_year - r.birth_year) / 5)
            else:
                feat_year = 1.0 if r.birth_year_lo <= r.event_year <= r.birth_year_hi else 0.2
        elif r.sourcetype == "BS Huwelijk" and r.event_year:
            approx_birth = r.birth_year if r.has_birth_anchor and pd.notna(r.birth_year) \
                else (r.birth_year_lo + r.birth_year_hi) / 2
            age_at_marriage = r.event_year - approx_birth
            feat_year = 1.0 if 16 <= age_at_marriage <= 70 else 0.2

        feat_place = score_place(norm_place(r.eventplace), r.place_norm)

        feats = {"surname": feat_surname, "initials": feat_initials,
                 "year": feat_year, "place": feat_place}
        rows.append({
            "era": r.era, "key": r.key, "source": "openarch",
            "person_ref": r.identifier, "score": weighted_score(feats),
            "feat_surname": feat_surname, "feat_initials": feat_initials,
            "feat_year": feat_year, "feat_place": feat_place,
            "hit_name": r.personname, "hit_year": r.event_year,
            "hit_place": r.eventplace, "record_type": r.sourcetype,
        })
    return pd.DataFrame(rows)


def build_genealogieonline_pairs(roster: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect(GENEALOGIE_DB, read_only=True)
    hits = con.execute("""
        SELECT era, key, url, person_name,
               birth_year AS hit_birth_year, death_year AS hit_death_year
        FROM hits
    """).fetchdf()
    con.close()

    hits = hits[~hits["person_name"].map(is_invalid_hit)]
    hits = hits.merge(roster, on=["era", "key"], how="inner")
    if hits.empty:
        return hits

    rows = []
    for r in hits.itertuples():
        given, surname_tokens = split_given_surname(r.person_name or "", r.sn)
        derived_ini = norm_ini(initials_from_tokens(given))
        feat_surname = score_surname(norm_surname(" ".join(surname_tokens)), r.sn)
        feat_initials = score_initials(derived_ini, r.ini)

        feat_year = None
        if pd.notna(r.hit_birth_year):
            if r.has_birth_anchor and pd.notna(r.birth_year):
                feat_year = max(0.0, 1 - abs(r.hit_birth_year - r.birth_year) / 5)
            else:
                feat_year = 1.0 if r.birth_year_lo <= r.hit_birth_year <= r.birth_year_hi else 0.2

        feats = {"surname": feat_surname, "initials": feat_initials,
                 "year": feat_year, "place": None}
        rows.append({
            "era": r.era, "key": r.key, "source": "genealogieonline",
            "person_ref": r.url, "score": weighted_score(feats),
            "feat_surname": feat_surname, "feat_initials": feat_initials,
            "feat_year": feat_year, "feat_place": None,
            "hit_name": r.person_name, "hit_year": r.hit_birth_year,
            "hit_place": None, "record_type": "genealogieonline_tree",
        })
    return pd.DataFrame(rows)


def main() -> None:
    roster = pd.read_parquet(ROSTER_PATH)
    roster = roster[roster["sn"] != ""]

    oa_pairs = build_openarch_pairs(roster)
    go_pairs = build_genealogieonline_pairs(roster)
    pairs = pd.concat([oa_pairs, go_pairs], ignore_index=True)
    pairs = pairs.sort_values(["era", "key", "score"], ascending=[True, True, False])
    pairs.to_parquet(f"{OUT_DIR}/candidate_person_pairs.parquet", index=False)

    summary = (pairs.groupby(["era", "key"])["score"]
               .agg(n_pairs="count", best_score="max").reset_index())
    summary = roster[["era", "key", "elected", "has_birth_anchor"]].merge(
        summary, on=["era", "key"], how="left")
    summary["n_pairs"] = summary["n_pairs"].fillna(0).astype(int)
    summary.to_parquet(f"{OUT_DIR}/candidate_person_pairs_summary.parquet", index=False)

    print(f"candidate_person_pairs: {len(pairs)} rows "
          f"({len(oa_pairs)} openarch, {len(go_pairs)} genealogieonline)")
    print(f"candidates with >=1 pair: {(summary['n_pairs'] > 0).sum()}/{len(summary)} "
          f"({(summary['n_pairs'] > 0).mean():.1%})")
    for thr in (0.5, 0.6, 0.7, 0.8):
        share = (summary["best_score"] >= thr).mean()
        print(f"  share with best_score >= {thr}: {share:.1%}")
    print("\nby era:")
    print(summary.groupby("era").agg(
        n=("key", "count"),
        has_pair=("n_pairs", lambda s: (s > 0).mean()),
        mean_best=("best_score", "mean")).to_string())
    print("\nwinner vs loser (elected) best_score >=0.7 rate:")
    print(summary.groupby("elected")["best_score"].apply(
        lambda s: (s >= 0.7).mean()).to_string())


if __name__ == "__main__":
    main()
