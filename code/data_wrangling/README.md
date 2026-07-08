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
