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
uv run python code/data_wrangling/panel/panel_step1_assemble.py
```

Outputs land in `data/<source>/` (DuckDB + raw files); the analysis-ready
tables are `data/panel/*.parquet`.

Post-1917 candidate-level rows are NOT yet in `candidates_panel`: they require
OCR/transcription of the CBS scans (1933, 1937) and Staatscourant PDFs
(1918-1929) collected by pipelines 4-5. The scans/PDFs are archived losslessly
so a modern OCR pass can be run without re-scraping (project decision
2026-07-07).
