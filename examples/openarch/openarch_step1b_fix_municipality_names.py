# =============================================================================
# openarch_step1b_fix_municipality_names.py  [OPENARCHIEF PIPELINE - STEP 1b]
# Input:  data/openarchive/survey_zero_results.csv
#         (produced by openarch_step1_survey_availability.py)
#         Columns: amco, name, status  (status = "zero_results" | "very_few_results")
# Output: data/openarchive/survey_name_candidates.csv
#         Columns: amco, original_name, candidate_name, number_found, status
#
# For every municipality that returned 0 records in step 1, this script
# generates a set of name variants (case changes, prefix removal, syllable
# truncation, IJ/Y substitutions, hyphen/space variants, etc.) and queries
# the OpenArchieven API for each one. Variants that return at least one
# BS Huwelijk record are exported as candidate names.
#
# Run:
#   uv run python code/data_wrangling/openarch/openarch_step1b_fix_municipality_names.py
#
# Optional flags:
#   --input PATH    path to zero-results CSV (default: data/openarchive/survey_zero_results.csv)
#   --output PATH   path to candidates CSV   (default: data/openarchive/survey_name_candidates.csv)
#   --from-date     YYYY-MM-DD start of BS Huwelijk range to test (default: 1850-01-01)
#   --until-date    YYYY-MM-DD end   of BS Huwelijk range to test (default: 1900-12-31)
# =============================================================================
import argparse
import asyncio
import os
import re
import sys
import time
import unicodedata

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from openarch_async_helpers import (
    TokenBucketRateLimiter,
    make_session,
    make_search_url,
)

RATE = 4.0        # requests per second (API limit)
CONCURRENCY = 8   # max simultaneous connections


# ---------------------------------------------------------------------------
# Variant generator
# ---------------------------------------------------------------------------

# Common Dutch municipal name prefixes that may not appear in the API index
_PREFIXES = [
    "'s-", "'S-",
    "de ", "De ",
    "den ", "Den ",
    "het ", "Het ",
    "van ", "Van ",
    "van de ", "Van De ", "van den ", "Van Den ", "van der ", "Van Der ",
    "in de ", "In De ", "in den ", "In Den ",
    "aan de ", "Aan De ", "aan den ", "Aan Den ",
    "aan het ", "Aan Het ",
    "op den ", "Op Den ", "op de ", "Op De ",
    "bij de ", "Bij De ", "bij den ", "Bij Den ",
    "onder de ", "Onder De ",
    "ter ", "Ter ",
    "ten ", "Ten ",
]


def _syllable_prefixes(name: str) -> list[str]:
    """
    Return prefixes up to the first 1–3 consonant-vowel transitions.
    These approximate 'first syllable', 'first two syllables', etc.
    """
    vowels = set("aeiouAEIOUëïöüËÏÖÜ")
    breaks = []
    in_vowel = name[0] in vowels if name else False
    for i, ch in enumerate(name[1:], 1):
        is_v = ch in vowels
        if is_v != in_vowel:
            in_vowel = is_v
            if not is_v:          # transition vowel → consonant = syllable end
                breaks.append(i)
            if len(breaks) == 3:
                break
    return [name[:b] for b in breaks if b >= 3]


def _strip_diacritics(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def generate_variants(name: str) -> list[str]:
    """
    Generate candidate name strings to try as the `eventplace` parameter.
    Returns a deduplicated list, excluding the original (already tried).
    """
    candidates: set[str] = set()

    def add(*names):
        for n in names:
            n = n.strip()
            if n and len(n) >= 2:
                candidates.add(n)

    # ---- Case variants ----
    add(name.lower(), name.upper(), name.title(), name.capitalize())

    # ---- Diacritics stripped ----
    add(_strip_diacritics(name), _strip_diacritics(name).title())

    # ---- IJ / Y substitutions (very common in Dutch) ----
    for src, dst in [("IJ", "Y"), ("ij", "y"), ("Y", "IJ"), ("y", "ij")]:
        add(name.replace(src, dst),
            name.replace(src, dst).title())

    # ---- Hyphen ↔ space ↔ nothing ----
    if "-" in name:
        no_hyphen_space = name.replace("-", " ")
        no_hyphen_none  = name.replace("-", "")
        add(no_hyphen_space, no_hyphen_space.title(),
            no_hyphen_none,  no_hyphen_none.title())
        # Try each hyphen-separated part individually
        for part in name.split("-"):
            add(part, part.title(), part.lower())

    # ---- Space ↔ hyphen ----
    if " " in name:
        hyphenated = name.replace(" ", "-")
        add(hyphenated, hyphenated.title())

    # ---- Word-level splits ----
    words = name.split()
    if len(words) >= 2:
        add(words[0],           # first word
            words[0].title(),
            words[0].lower(),
            words[-1],          # last word
            words[-1].title(),
            " ".join(words[1:]),          # drop first word
            " ".join(words[1:]).title(),
            " ".join(words[:-1]),         # drop last word
            " ".join(words[:-1]).title(),
        )
    if len(words) >= 3:
        add(words[1],           # middle word(s)
            " ".join(words[1:-1]).title(),
        )

    # ---- Strip common Dutch prefixes ----
    for prefix in _PREFIXES:
        if name.startswith(prefix):
            rest = name[len(prefix):]
            add(rest, rest.title(), rest.lower(), rest.capitalize())
        if name.lower().startswith(prefix.lower()):
            rest = name[len(prefix):]
            add(rest, rest.title())

    # ---- Syllable approximations ----
    for syl in _syllable_prefixes(name):
        add(syl, syl.title(), syl.lower())

    # ---- Character prefix truncations ----
    for n in [3, 4, 5, 6]:
        add(name[:n], name[:n].title(), name[:n].lower())

    # ---- 'En' → conjunction split (e.g. "Bergen en Zoom" → "Bergen", "Zoom") ----
    for sep in [" en ", " En ", " EN ", " und ", " and "]:
        if sep in name:
            parts = name.split(sep)
            for p in parts:
                add(p, p.title(), p.strip())

    # ---- Remove parenthetical suffixes (e.g. "Tilburg (NB)") ----
    clean_parens = re.sub(r"\s*\(.*?\)\s*", "", name).strip()
    if clean_parens != name:
        add(clean_parens, clean_parens.title())

    # ---- Common abbreviation expansions ----
    subs = {
        "St.": "Sint",
        "St ": "Sint ",
        "Gld": "Gelderland",
        "Nh": "Noord-Holland",
        "Zh": "Zuid-Holland",
    }
    for abbr, full in subs.items():
        if abbr in name:
            add(name.replace(abbr, full), name.replace(abbr, full).title())

    # Remove the original name (already tried, returned 0)
    candidates.discard(name)
    candidates.discard(name.strip())

    # Remove empty / too-short
    return sorted(c for c in candidates if len(c) >= 2)


# ---------------------------------------------------------------------------
# API probe
# ---------------------------------------------------------------------------

async def probe_variant(
    session,
    limiter: TokenBucketRateLimiter,
    sem: asyncio.Semaphore,
    amco: str,
    original_name: str,
    candidate: str,
    from_date: str,
    until_date: str,
    status: str = "zero_results",
) -> dict | None:
    """
    Query the API with `eventplace=candidate`. Returns a result dict if
    number_found > 0, else None.
    """
    # URL-encode spaces as + for the eventplace parameter
    place = candidate.replace(" ", "+")
    url = make_search_url(place, from_date, until_date, start=0)

    async with sem:
        await limiter.acquire()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
            n = data.get("response", {}).get("number_found", 0)
            if n > 0:
                return {
                    "amco": amco,
                    "original_name": original_name,
                    "candidate_name": candidate,
                    "number_found": int(n),
                    "status": status,
                }
            return None
        except Exception as e:
            print(f"  [warn] probe '{candidate}': {e}", flush=True)
            return None


async def run_probes(
    zero_df: pd.DataFrame,
    from_date: str,
    until_date: str,
) -> list[dict]:
    """Run all probes and collect candidates with number_found > 0."""

    # Build full task list: (amco, original_name, candidate, status)
    tasks_args = []
    for _, row in zero_df.iterrows():
        amco = str(row["amco"])
        name = str(row["name"])
        status = str(row.get("status", "zero_results"))
        variants = generate_variants(name)
        for v in variants:
            tasks_args.append((amco, name, v, status))

    total = len(tasks_args)
    n_muni = len(zero_df)
    print(
        f"Probing {total:,} name variants across {n_muni:,} municipalities "
        f"(avg {total/n_muni:.0f} variants each)..."
    )

    limiter = TokenBucketRateLimiter(rate=RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    results: list[dict] = []
    n_done = 0
    t0 = time.monotonic()

    async with make_session(connector_limit=CONCURRENCY) as session:
        coros = [
            probe_variant(session, limiter, sem, amco, name, candidate, from_date, until_date, status)
            for amco, name, candidate, status in tasks_args
        ]

        for coro in asyncio.as_completed(coros):
            result = await coro
            n_done += 1
            if result is not None:
                results.append(result)

            if n_done % 100 == 0:
                elapsed = time.monotonic() - t0
                rate = n_done / elapsed if elapsed > 0 else 0
                eta = (total - n_done) / rate / 60 if rate > 0 else float("inf")
                hits = len(results)
                print(
                    f"  {n_done:,}/{total:,} ({100*n_done/total:.1f}%)  "
                    f"{rate:.1f} req/s  hits={hits:,}  ETA {eta:.0f} min",
                    flush=True,
                )

    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_summary(candidates_df: pd.DataFrame, zero_df: pd.DataFrame) -> None:
    """Print a human-readable summary of matches found."""
    matched_amcos = set(candidates_df["amco"].unique())
    unmatched = zero_df[~zero_df["amco"].astype(str).isin(matched_amcos)]

    print(f"\n{'='*70}")
    print(f"Results: {len(matched_amcos):,} / {len(zero_df):,} municipalities found under a different name.")
    print(f"         {len(unmatched):,} municipalities found no match (likely not digitized).\n")

    if not candidates_df.empty:
        # Best candidate per municipality (highest number_found)
        best = (
            candidates_df
            .sort_values("number_found", ascending=False)
            .groupby("amco", as_index=False)
            .first()
            .sort_values("number_found", ascending=False)
        )
        print("Best candidate per municipality (sorted by record count):")
        cols = ["amco", "original_name", "candidate_name", "number_found", "status"]
        print(best[[c for c in cols if c in best.columns]].to_string(index=False))

    if not unmatched.empty:
        has_status = "status" in unmatched.columns
        print(f"\nMunicipalities with no match at all (likely absent from OpenArchieven):")
        cols = ["amco", "name", "status"] if has_status else ["amco", "name"]
        print(unmatched[[c for c in cols if c in unmatched.columns]].to_string(index=False))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Find correct OpenArchieven eventplace names for zero-result municipalities"
    )
    p.add_argument(
        "--input",
        default="./data/openarchive/survey_zero_results.csv",
        help="Path to zero-results CSV from step 1",
    )
    p.add_argument(
        "--output",
        default="./data/openarchive/survey_name_candidates.csv",
        help="Path for output candidates CSV",
    )
    p.add_argument(
        "--from-date",
        default="1850-01-01",
        help="Start of date range to probe (default: 1850-01-01)",
    )
    p.add_argument(
        "--until-date",
        default="1900-12-31",
        help="End of date range to probe (default: 1900-12-31)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(args.input):
        print(f"Error: input file not found: {args.input}")
        print("Run openarch_step1_survey_availability.py first.")
        sys.exit(1)

    zero_df = pd.read_csv(args.input, dtype={"amco": str})
    if "status" not in zero_df.columns:
        zero_df["status"] = "zero_results"
    n_zero = (zero_df["status"] == "zero_results").sum()
    n_few  = (zero_df["status"] == "very_few_results").sum()
    print(
        f"Loaded {len(zero_df):,} municipalities from {args.input} "
        f"({n_zero} zero_results, {n_few} very_few_results)."
    )

    if zero_df.empty:
        print("No zero-result municipalities — nothing to do.")
        return

    # Show a sample of the variants to be tried
    sample_name = zero_df["name"].iloc[0]
    sample_variants = generate_variants(sample_name)
    print(f"\nExample variants for '{sample_name}' ({len(sample_variants)} total):")
    for v in sample_variants[:20]:
        print(f"  '{v}'")
    if len(sample_variants) > 20:
        print(f"  ... and {len(sample_variants) - 20} more")
    print()

    candidates = asyncio.run(run_probes(zero_df, args.from_date, args.until_date))

    if not candidates:
        print("No candidates found.")
        out_df = pd.DataFrame(columns=["amco", "original_name", "candidate_name", "number_found", "status"])
    else:
        out_df = (
            pd.DataFrame(candidates)
            .sort_values(["amco", "number_found"], ascending=[True, False])
            .reset_index(drop=True)
        )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    out_df.to_csv(args.output, index=False)
    print(f"\nCandidates written to {args.output} ({len(out_df):,} rows).")

    print_summary(out_df, zero_df)


if __name__ == "__main__":
    main()
