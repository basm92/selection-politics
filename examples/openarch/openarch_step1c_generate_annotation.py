# =============================================================================
# openarch_step1c_generate_annotation.py  [OPENARCHIEF PIPELINE - STEP 1c]
# Input:  data/openarchive/survey_name_candidates.csv
#         (produced by openarch_step1b_fix_municipality_names.py)
# Output: data/openarchive/survey_name_selected.csv
#         Columns: amco, original_name, selected_name, number_found, status, notes
#
# Pre-populates one row per municipality with the best candidate name
# (highest number_found). Flags ambiguous cases where multiple municipalities
# resolve to the same name — those require manual review before running step 2.
#
# Workflow:
#   1. Run this script to generate survey_name_selected.csv
#   2. Open survey_name_selected.csv and review rows marked "AMBIGUOUS" in notes
#      — decide whether the resolved name is acceptable for each amco, or clear
#        selected_name to skip that municipality entirely
#   3. Run step 2 with --candidates-file data/openarchive/survey_name_selected.csv
#
# Run:
#   uv run python code/data_wrangling/openarch/openarch_step1c_generate_annotation.py
#
# Optional flags:
#   --input PATH    path to candidates CSV  (default: data/openarchive/survey_name_candidates.csv)
#   --output PATH   path for selected CSV   (default: data/openarchive/survey_name_selected.csv)
# =============================================================================
import argparse
import os
import sys

import pandas as pd


def parse_args():
    p = argparse.ArgumentParser(
        description="Generate annotation file from step-1b candidates"
    )
    p.add_argument(
        "--input",
        default="./data/openarchive/survey_name_candidates.csv",
        help="Path to candidates CSV from step 1b",
    )
    p.add_argument(
        "--output",
        default="./data/openarchive/survey_name_selected.csv",
        help="Path for the annotation CSV",
    )
    return p.parse_args()


def main():
    args = parse_args()

    if not os.path.exists(args.input):
        print(f"Error: input file not found: {args.input}")
        print("Run openarch_step1b_fix_municipality_names.py first.")
        sys.exit(1)

    cands = pd.read_csv(args.input, dtype={"amco": str})
    print(f"Loaded {len(cands):,} candidate rows for {cands['amco'].nunique():,} municipalities.")

    # Best candidate per municipality = highest number_found, ties broken by
    # first occurrence (candidates are already sorted descending by number_found
    # within each amco by step 1b).
    best = (
        cands
        .sort_values("number_found", ascending=False)
        .groupby("amco", as_index=False)
        .first()
        [["amco", "original_name", "candidate_name", "number_found", "status"]]
        .rename(columns={"candidate_name": "selected_name"})
    )

    # Detect ambiguous cases: same selected_name claimed by multiple amcos.
    # These are municipalities whose names are genuinely non-unique in the API
    # (e.g. "Bergen" for Bergen op Zoom, Bergen NH, and Bergen L all return the
    # same 3238 records).  The user must decide whether to keep, change, or
    # blank out the name for each amco.
    name_counts = best.groupby("selected_name")["amco"].transform("count")
    best["notes"] = ""
    best.loc[name_counts > 1, "notes"] = (
        "AMBIGUOUS: "
        + name_counts[name_counts > 1].astype(str)
        + " municipalities share this name — verify or clear selected_name"
    )

    # Sort for easy review: ambiguous first, then by status and amco
    best["_sort"] = best["notes"].str.startswith("AMBIGUOUS").astype(int)
    best = (
        best
        .sort_values(["_sort", "status", "amco"], ascending=[False, True, True])
        .drop(columns="_sort")
        .reset_index(drop=True)
    )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    best.to_csv(args.output, index=False)

    n_ambiguous = (best["notes"] != "").sum()
    n_clean = (best["notes"] == "").sum()
    print(f"\nWritten to {args.output}:")
    print(f"  {n_clean:,} municipalities with a clear best candidate")
    print(f"  {n_ambiguous:,} municipalities flagged as AMBIGUOUS — review required")

    if n_ambiguous:
        print("\nAmbiguous municipalities:")
        print(
            best[best["notes"] != ""]
            [["amco", "original_name", "selected_name", "number_found", "status"]]
            .to_string(index=False)
        )
        print(
          "\nFor each ambiguous row, either:\n"
          "  - Keep selected_name if you are confident it is the right place\n"
          "  - Replace selected_name with a more specific variant (check step-1b candidates)\n"
          "  - Clear selected_name (empty string) to skip this municipality\n"
        )

    print("\nNext step: review the file, then run:")
    print("  uv run python code/data_wrangling/openarch/openarch_step2_download_marriages.py \\")
    print("    --phase list --candidates-file data/openarchive/survey_name_selected.csv")


if __name__ == "__main__":
    main()
