# Data wrangling pipelines — Phase 1

Run order (all scripts are resumable; rerunning skips completed work):

```bash
# 1. Huygens district-era candidate database (1848-1918)
uv run python code/data_wrangling/huygens/huygens_step1_list_elections.py
uv run python code/data_wrangling/huygens/huygens_step2_fetch_uitslagen.py

# 2. AIEEDA interwar municipal × party panel (1922-1937)
uv run python code/data_wrangling/aieeda/aieeda_step1_ingest.py

# 3. NLGIS municipality crosswalk (1848-1940)
uv run python code/data_wrangling/nlgis/nlgis_step1_download_maps.py
uv run python code/data_wrangling/nlgis/nlgis_step2_build_crosswalk.py

# 4. CBS Statistiek der Verkiezingen page scans (1901-1913 validation,
#    1933+1937 interwar candidate tables)
uv run python code/data_wrangling/cbs/cbs_step1_index_scans.py
uv run python code/data_wrangling/cbs/cbs_step2_download_scans.py

# 5. Delpher Staatscourant: official candidate lists + proces-verbaal
#    (primary candidate-level source 1918-1929; CBS published nothing there)
uv run python code/data_wrangling/delpher/delpher_step1_survey_staatscourant.py
uv run python code/data_wrangling/delpher/delpher_step2_download_pdfs.py

# 6. Assemble panel + parquet exports
#    step 1: district era (1848-1918) from Huygens + AIEEDA + NLGIS
#    step 2: merge post-1917 PR rows (Staatscourant) -> unified 1848-1937
uv run python code/data_wrangling/panel/panel_step1_assemble.py
uv run python code/data_wrangling/panel/panel_step2_merge_post1917.py

# 7. PDC/parlement.com biographies -> mp_anchor (Phase 2a; birth/death dates
#    for elected MPs, used as an entity-resolution anchor in Phase 2b)
uv run python code/data_wrangling/pdc/pdc_step1_survey_sitemap.py
uv run python code/data_wrangling/pdc/pdc_step2_scrape_biographies.py
uv run python code/data_wrangling/pdc/pdc_step3_build_mp_anchor.py

# 8. Candidate roster + OpenArchieven/GenealogieOnline linkage (Phase 2b)
#    step1 (openarch) and step1 (genealogieonline) can run in parallel
uv run python code/data_wrangling/panel/panel_step3_candidate_roster.py
uv run python code/data_wrangling/openarch/openarch_step1_query_candidates.py
uv run python code/data_wrangling/genealogieonline/genealogieonline_step1_query_candidates.py
uv run python code/data_wrangling/panel/panel_step4_candidate_person_pairs.py
```

Outputs land in `data/<source>/` (DuckDB + raw files); the analysis-ready
tables are `data/panel/*.parquet`.

Post-1917 candidate rows ARE now in `candidates_panel` (panel step 2). They come
from the Delpher Staatscourant OCR/parse pipeline (delpher steps 3-6b, which
transcribe the archived issue PDFs into `data/delpher/delpher.duckdb`). Run
those before panel step 2:

```bash
uv run python code/data_wrangling/delpher/delpher_step3_locate_pages.py
uv run python code/data_wrangling/delpher/delpher_step4_ocr_pages.py            # Gemini OCR, costs ~$1
uv run python code/data_wrangling/delpher/delpher_step5_parse_kandidatenlijsten.py
uv run python code/data_wrangling/delpher/delpher_step6_parse_uitslagen.py       # rule-based baseline
uv run python code/data_wrangling/delpher/delpher_step6b_llm_parse_uitslagen.py   # LLM hybrid, costs ~$0.30
```

`candidates_panel` now spans 1848-1937 with an `era` column
(`district_1848_1918` vs `pr_1918_1937`); district columns are NULL on PR rows
and vice-versa. Post-1917 grain is one (year, kieskring, lijst, positie)
candidacy carrying preference `votes`, list `stemcijfer`, `residence`, and an
`elected` flag propagated from the person-level seat allocation. See
`persons_post1917` for the deduplicated person-election view and
`gekozen_unmatched` for the ~11 seated members OCR left unlinkable.

## Phase 2a: MP anchor (PDC/parlement.com)

`data/panel/mp_anchor.parquet` links elected persons in `candidates_panel`
(`persoon_id` for the district era, `person_key` for the PR era) to a
parlement.com biography: birth/death date+place and party. Built from
~5,849 scraped PDC biography pages (`data/pdc/pdc.duckdb`), filtered to the
914 persons with a Tweede Kamer membership span overlapping 1848-1940, then
matched to the 921 elected persons in the panel by normalised surname +
initials (exact, then same-surname/first-initial, then Levenshtein-fuzzy).
Match rate ~89% (821/921); unmatched persons are in
`data/panel/mp_anchor_unmatched.parquet` for Phase 2b review. Known gaps:
a handful of PDC coverage misses (e.g. some inter-war left-wing figures),
non-standard membership records (e.g. brief ceremonial reappointments
recorded only as free-text trivia, not a structured function entry), and a
pre-existing mojibake encoding bug in some Huygens `name_clean` values
(`Ã` in place of `Æ`/diacritics) inherited from Phase 1.

## Phase 2b: candidate → genealogical person linkage

`data/panel/candidate_person_pairs.parquet` links every distinct candidate
in `candidates_panel` (5,507 people, deduped from 35,615 candidacy rows) to
plausible OpenArchieven civil-registration records and GenealogieOnline
family-tree persons, confidence-scored rather than filtered. `panel_step3`
builds the shared candidate roster (birth-year search window: exact from
`mp_anchor` where available, else `[year_max-75, year_min-30]`, since
passive suffrage was constant at age 30 for 1848-1937). `openarch` and
`genealogieonline` step1 scripts then search each candidate's surname
(**must be the raw spelling** — searching the scoring-normalised form
silently zeroed ~40% of candidates, see
`docs/agent_memory/phase2b-candidate-linkage.md`) and `panel_step4` scores
every hit on surname/initials/year/place agreement, gated so a wrong
first-name/initials or wrong surname can't be "rescued" by the other
features lining up by coincidence. 95% of candidates have ≥1 pair; score
≥0.7 for 72.9% of winners vs 43.7% of losers (the expected direction — no
birth-date anchor and common surnames make losers harder to link). Score
weights were tuned against a 100-sample hand-labelled set (AI-labelled, not
independently human-verified) stratified across famous MPs, obscure losers,
and common surnames.
