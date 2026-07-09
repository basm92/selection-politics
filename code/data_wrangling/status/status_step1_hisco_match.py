# =============================================================================
# status_step1_hisco_match.py  [STATUS PIPELINE - STEP 1]  (Phase 3)
# Input:  data/openarch/openarch.duckdb          detail_records.profession
#         data/genealogieonline/genealogieonline.duckdb  person_pages.beroep
#         examples/hisco/hsn2013a_hisco_comma.csv  (HSN HISCO conversion dict)
# Output: data/panel/beroep_hisco_matches.parquet
#           beroep_raw, beroep_norm, hisco, hisclass, hisclass_5, hiscam_nl,
#           match_method ('exact_standard'/'exact_original'/'fuzzy_standard'),
#           match_score (1.0 for exact; 1 - lev/maxlen for fuzzy)
#
# Network-free. Matches every DISTINCT occupation string harvested in
# status/openarch/genealogieonline step 2 (not every occurrence) against the
# HSN dictionary's 95,298 (Original -> Standard -> HISCO/HISCLASS/HISCAM_NL)
# rows: `Original` is near-unique raw OCR text (poor exact-match target for
# our own already-legible strings), `Standard` is the normalised occupation
# title (34,258 distinct) and matches much better. Strategy, in order:
#   1. exact match on normalised text against Standard
#   2. exact match on normalised text against Original (catches archaic
#      spellings that happen to appear verbatim in the raw OCR corpus)
#   3. fuzzy fallback (Levenshtein ratio, `name_match_utils.lev`) against
#      Standard, bucketed by first letter + length to keep it fast; accepted
#      only at ratio >= FUZZY_THRESHOLD, kept as a confidence-scored match
#      per project convention (not silently dropped, not silently trusted).
# Unmatched strings are kept with hisco=NULL so coverage can be reported
# rather than hidden.
#
# Usage:
#   uv run python code/data_wrangling/status/status_step1_hisco_match.py
# =============================================================================
import os
import re
import sys
import unicodedata
from collections import defaultdict

import duckdb
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "panel"))
from name_match_utils import lev

HISCO_CSV = "./examples/hisco/hsn2013a_hisco_comma.csv"
OPENARCH_DB = "./data/openarch/openarch.duckdb"
GENEALOGIE_DB = "./data/genealogieonline/genealogieonline.duckdb"
OUT_PATH = "./data/panel/beroep_hisco_matches.parquet"

FUZZY_THRESHOLD = 0.82
LEN_TOLERANCE = 2


def norm(s: str | None) -> str:
    if not isinstance(s, str) or not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-zA-Z\s]", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


_BRON_RE = re.compile(r"\bBron\s*\d+.*$", re.I)
_LEADING_BEROEP_RE = re.compile(r"^\s*[Bb]eroep\s*:\s*")
_LEADING_DATE_RE = re.compile(
    r"^\(?\s*(van|vanaf)?\s*\d{4}(\s*(tot|-|,)\s*\d{4})*\s*\)?\s*", re.I
)


def clean_beroep(raw: str) -> str:
    """Strip citation/date-range noise genealogieonline beroep strings carry
    (e.g. 'van 1917 tot 1947 Arbeider, landbouwer. Bron 1') before matching --
    a few pages also leave a second, nested 'Beroep:' label that the scraper's
    li-detection didn't strip."""
    s = _LEADING_BEROEP_RE.sub("", raw)
    s = _BRON_RE.sub("", s)
    s = _LEADING_DATE_RE.sub("", s)
    return s.strip(" .-")


def split_clauses(s: str) -> list[str]:
    """Compound career descriptions ('landbouwer, schilder') -> clauses, so
    the first (usually primary) occupation can be tried on its own."""
    parts = re.split(r"[,;/]| en ", s)
    return [p.strip(" .-") for p in parts if p.strip(" .-")]


def collect_beroep_strings() -> pd.Series:
    strings = []
    if os.path.exists(OPENARCH_DB):
        con = duckdb.connect(OPENARCH_DB, read_only=True)
        strings += con.execute(
            "SELECT DISTINCT profession FROM detail_records WHERE profession IS NOT NULL"
        ).df()["profession"].tolist()
        con.close()
    if os.path.exists(GENEALOGIE_DB):
        con = duckdb.connect(GENEALOGIE_DB, read_only=True)
        strings += con.execute(
            "SELECT DISTINCT beroep FROM person_pages WHERE beroep IS NOT NULL"
        ).df()["beroep"].tolist()
        con.close()
    return pd.Series(sorted(set(strings)), dtype=str)


def build_vocab(hisco: pd.DataFrame, col: str) -> dict[str, int]:
    """normalised(col) -> row index of the first occurrence with a
    classified (HISCLASS != -1) HISCO code, else the first occurrence."""
    vocab: dict[str, int] = {}
    classified: dict[str, int] = {}
    for idx, val in hisco[col].items():
        key = norm(val)
        if not key:
            continue
        if key not in vocab:
            vocab[key] = idx
        if key not in classified and hisco.at[idx, "HISCLASS"] != -1:
            classified[key] = idx
    vocab.update(classified)
    return vocab


def build_length_buckets(vocab: dict[str, int]) -> dict[tuple[str, int], list[str]]:
    buckets: dict[tuple[str, int], list[str]] = defaultdict(list)
    for key in vocab:
        if not key:
            continue
        buckets[(key[0], len(key))].append(key)
    return buckets


def fuzzy_match(query: str, buckets: dict[tuple[str, int], list[str]]) -> tuple[str | None, float]:
    if not query:
        return None, 0.0
    best_key, best_score = None, 0.0
    for dl in range(-LEN_TOLERANCE, LEN_TOLERANCE + 1):
        candidates = buckets.get((query[0], len(query) + dl))
        if not candidates:
            continue
        for cand in candidates:
            maxlen = max(len(query), len(cand))
            score = 1 - lev(query, cand) / maxlen
            if score > best_score:
                best_key, best_score = cand, score
    return best_key, best_score


def build() -> None:
    hisco = pd.read_csv(HISCO_CSV)
    # fillna (not astype(str)) so a genuinely missing Original/Standard
    # normalises to "" (skipped by build_vocab) instead of the literal
    # string "nan" polluting the vocabulary.
    hisco["Original"] = hisco["Original"].fillna("")
    hisco["Standard"] = hisco["Standard"].fillna("")

    standard_vocab = build_vocab(hisco, "Standard")
    original_vocab = build_vocab(hisco, "Original")
    buckets = build_length_buckets(standard_vocab)

    beroep = collect_beroep_strings()
    print(f"Distinct beroep strings to match: {len(beroep)}")

    rows = []
    n_exact_std = n_exact_orig = n_fuzzy = n_none = 0
    for raw in beroep:
        cleaned = clean_beroep(raw)
        key = norm(cleaned)

        # Try the full (cleaned) string first, then its first clause (for
        # compound career descriptions) -- in that order, exact before fuzzy.
        candidates = [key]
        clauses = split_clauses(cleaned)
        if clauses:
            first_key = norm(clauses[0])
            if first_key and first_key not in candidates:
                candidates.append(first_key)

        idx, method, score = None, None, 0.0
        for cand_key in candidates:
            if cand_key in standard_vocab:
                idx, method, score = standard_vocab[cand_key], "exact_standard", 1.0
                break
            if cand_key in original_vocab:
                idx, method, score = original_vocab[cand_key], "exact_original", 1.0
                break
        if idx is None:
            match_key, match_score = fuzzy_match(key, buckets)
            if match_key and match_score >= FUZZY_THRESHOLD:
                idx, method, score = standard_vocab[match_key], "fuzzy_standard", match_score

        if method == "exact_standard":
            n_exact_std += 1
        elif method == "exact_original":
            n_exact_orig += 1
        elif method == "fuzzy_standard":
            n_fuzzy += 1
        else:
            n_none += 1

        if idx is not None:
            r = hisco.loc[idx]
            rows.append({
                "beroep_raw": raw, "beroep_norm": key,
                "hisco": int(r["HISCO"]), "hisclass": int(r["HISCLASS"]),
                "hisclass_5": int(r["HISCLASS_5"]),
                "hiscam_nl": float(r["HISCAM_NL"]) if pd.notna(r["HISCAM_NL"]) else None,
                "match_method": method, "match_score": score,
            })
        else:
            rows.append({
                "beroep_raw": raw, "beroep_norm": key,
                "hisco": None, "hisclass": None, "hisclass_5": None,
                "hiscam_nl": None, "match_method": None, "match_score": 0.0,
            })

    out = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH}: {len(out)} rows "
          f"(exact_standard={n_exact_std}, exact_original={n_exact_orig}, "
          f"fuzzy={n_fuzzy}, unmatched={n_none})")
    classified = out["hisclass"].notna() & (out["hisclass"] != -1)
    print(f"  classified (HISCLASS != -1): {classified.sum()}/{len(out)} "
          f"({classified.mean():.1%})")


if __name__ == "__main__":
    build()
