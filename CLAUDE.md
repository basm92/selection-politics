# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Multi-phase research data-engineering project: **"Politicians' Status and Entry into Politics — Dutch Lower House (Tweede Kamer), 1848–1940."** Builds a linked candidate × election panel, resolves candidates to genealogical records, classifies occupational/dynastic status, and studies how the 1917 district→PR electoral reform affected political selection.

**Current checkpoint:** Unified `candidates_panel` now spans **1848–1937**. Phase 1 district era (1848–1918, Huygens) plus the post-1917 PR era (1918–1937) transcribed from Staatscourant PDFs via the Delpher OCR pipeline (delpher steps 3–6b) and merged by `panel_step2_merge_post1917.py`. Party-level municipal panel 1922–1937 (AIEEDA) is ingested. Phase 2a (`data/panel/mp_anchor.parquet`, PDC/parlement.com biographies for elected MPs) is done — ~89% match rate. Phase 2b (`data/panel/candidate_person_pairs.parquet`, OpenArchieven + GenealogieOnline candidate↔person linkage) is done — 95% of the 5,507 distinct candidates have ≥1 pair, winner/loser match-rate gap (72.9% vs 43.7% at score≥0.7) matches the expected direction. Phase 3 (`data/panel/candidate_status.parquet`, occupational HISCO/HISCLASS + dynastic status) is done — score≥0.7 (best pair per candidate per source, ~2,700/5,507 candidates each). Briefly widened to score≥0.5 for more coverage, then **reverted** after a 30-pair hand-labelled spot-check measured only ~30% precision in the 0.5-0.7 band (vs. 71%/49%/13% by strata at 0.7+) — see `docs/agent_memory/phase3-occupational-dynastic-status.md` and `docs/agent_memory/phase3_spotcheck_0.5_0.7_band.csv`. Final numbers: 17.8% own-occupation coverage, 17.0% father-occupation coverage, 4.5% in a detected dynasty group. This is the Phase 3 CHECKPOINT — next up is Phase 4 (wealth, `phase_2_and_onward.md`).

Key driving documents:
- `prompt.md` — full project brief, phases 0–5
- `archived/phase0_feasibility_report.md` — per-source coverage audit (completed 2026-07-07; findings absorbed into the data-sources table below)
- `phase_2_and_onward.md` — detailed phase 2–5 spec with checkpoints
- `post_1917_candidates.md` — transcription pipeline spec for Staatscourant PDFs

## Build & run

```bash
# Everything runs via uv (Python 3.13+). No install step — uv syncs automatically.
uv run python code/data_wrangling/<source>/<script>.py

# Run order for Phase 1 (see code/data_wrangling/README.md):
uv run python code/data_wrangling/huygens/huygens_step1_list_elections.py
uv run python code/data_wrangling/huygens/huygens_step2_fetch_uitslagen.py
uv run python code/data_wrangling/aieeda/aieeda_step1_ingest.py
uv run python code/data_wrangling/nlgis/nlgis_step1_download_maps.py
uv run python code/data_wrangling/nlgis/nlgis_step2_build_crosswalk.py
uv run python code/data_wrangling/cbs/cbs_step1_index_scans.py
uv run python code/data_wrangling/cbs/cbs_step2_download_scans.py
uv run python code/data_wrangling/delpher/delpher_step1_survey_staatscourant.py
uv run python code/data_wrangling/delpher/delpher_step2_download_pdfs.py
uv run python code/data_wrangling/panel/panel_step1_assemble.py
```

All scripts are **idempotent/resumable**: rerunning skips already-completed work (tracked in DuckDB progress tables). No test suite exists yet.

## Architecture: house style for pipelines

Every pipeline follows a pattern established in `examples/openarch/` and `examples/genealogie/`:

1. **Numbered step scripts** under `code/data_wrangling/<source>/` with a docstring header: Input / Output / Method / Usage.
2. **Async rate-limited scraping** using `TokenBucketRateLimiter` (token-bucket, imported from the Huygens helpers or redefined) + `aiohttp.ClientSession` with polite `User-Agent` (`selection-politics-research/0.1`).
3. **Resumable progress tracked in DuckDB** — progress tables record which items are done; reruns skip them. Never re-fetch on rerun.
4. **Parquet exports** for analysis-ready tables in `data/panel/` and `data/<source>/`.
5. **Provenance column** on assembled tables recording which source produced each row.

### Key cross-pipeline imports

Pipelines import the rate limiter from `huygens_async_helpers.py` via `sys.path.insert`:
```python
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter, USER_AGENT
```

### Data flow

```
source-specific scripts  →  data/<source>/<source>.duckdb  (raw)
panel assembly           →  data/panel/panel.duckdb         (normalized)
                         →  data/panel/*.parquet            (analysis-ready)
```

NLGIS is a cross-cutting resource: `data/nlgis/crosswalk.duckdb` provides municipality identity across years (mergers/splits/renames resolved via geometric point-in-polygon).

## Data sources & their quirks

| Source | Era | Granularity | Access | Quirks |
|---|---|---|---|---|
| Huygens Verkiezingen TK | 1848–1918 | candidate × district-election | HTML scrape, ~2,870 pages | No birth data; no explicit `elected` flag (derive from threshold/runoff logic); `persoon_ID` stable across candidacies |
| AIEEDA (OSF qs3dg) | 1922–1937 | municipality × party votes | Single zip download, ready CSV | 76,546 rows, no candidates — party-level only |
| Delpher Staatscourant | 1918–1937 | Candidate lists + results per kieskring | SRU API (jsru.kb.nl) + PDF download | Primary candidate source post-1917; OCR is broken (diacritics, split initials); PDFs archived for re-OCR. 1922 has NO CBS alternative |
| CBS historisch.cbs.nl | 1918–1937 (but gaps) | Scanned page images | Session-stateful HTML; JPEG scans ~2060×2904px | 1922 never published by CBS; 1918–1929 is party-level only in CBS volumes. The 1933/1937 scans contain municipality×party tables, NOT candidate tables |
| NLGIS maps API | 1848–1940 | Municipality polygons | `GET /api/maps?year=YYYY` → TopoJSON | `province` param broken (empty response); query by year, filter client-side. TopoJSON is quantized+delta-encoded — decoded manually with shapely |
| HIP-NL | Utrecht 1909 only | Person-level tax class + income | SPARQL, no auth | NOT a national income source — pilot dataset. Use for Utrecht case study at most |
| OpenArchieven API | 19th–early 20th c. | Civil registry person records | REST API 1.1 (`api.openarchieven.nl`), `records/search.json?name=<surname>+<yr>-<yr>` | `name` is SURNAME-ONLY (a combined firstname+surname query returns 0 hits) but DOES accept multi-word surnames verbatim; search-result docs already carry personname/date/place/relationtype, no `show.json` detail call needed for identity matching |
| GenealogieOnline | 1500–1900+ | Family trees with beroep/lineage | `/zoeken/index.php?q=&vn=&gv=&gt=` | `vn=` needs a FULL first name (a bare initial returns 0 hits); `q=` does NOT handle a multi-word surname phrase (search the surname's last word only) |
| PDC/parlement.com | 1848–present | MP biographies: birth/death, party, offices | No index page; biographies live at flat `/biografie/<slug>` URLs discoverable only via `sitemap.xml` (~5,849 pages) | Free page is a "selectie" of the full career (some minor functions may be truncated); some historical figures (esp. inter-war left-wing MPs) have no bio at all; Dutch y/ij spelling variants across sources need folding when name-matching |

## Key design decisions

- **Confidence-scored matching, not filtered matches** (Phase 2): Keep all candidate↔person pairs with scores; downstream analysis thresholds. Score calibration via hand-labelled samples.
- **Post-1917 candidate rows are print-locked**: CBS scans and Staatscourant PDFs are archived losslessly on disk (`data/delpher/staatscourant/`, `data/cbs/scans/`) so OCR can be re-run without re-scraping.
- **1917 reform as identification strategy**: District→PR switch + staged suffrage extensions (1887, 1896, 1917 universal male, 1922 female) observed within one linked panel.
- **"Elected" derivation** (pre-1918): Runoff rounds (`herstemming`) — top-`zetels` by votes; `*/enkelvoudig` — elected unopposed; first rounds — votes ≥ kiesdrempel, capped at top-`zetels`. Validated against PDC MP lists.

## Memory notes

Operational notes (endpoints, scrape quirks, build state, in-flight decisions) live **in the repo** at `docs/agent_memory/` — see its `README.md` index. Check there before re-scraping or re-verifying sources, and update those files when facts change. Completed mid-task handoffs are moved to `archived/` once their work lands (e.g. `archived/step6_llm_parsing_plan.md`, the step-6 LLM-parse plan — done). Claude's local memory system (`~/.claude/projects/.../memory/`) only holds pointers to these repo files.

## Data regeneration

Large raw data files are excluded from git via `.gitignore`. To rebuild from scratch:

```bash
# NLGIS municipality maps (93 × ~400 KB TopoJSON, 1848–1940)
uv run python code/data_wrangling/nlgis/nlgis_step1_download_maps.py

# AIEEDA interwar election data (34 MB zip — download from OSF, ingest to DuckDB)
uv run python code/data_wrangling/aieeda/aieeda_step1_ingest.py

# Delpher Staatscourant survey + PDF download (~1.3 GB of issue scans)
uv run python code/data_wrangling/delpher/delpher_step1_survey_staatscourant.py
uv run python code/data_wrangling/delpher/delpher_step2_download_pdfs.py

# CBS historical election statistics page scans (~263 MB)
uv run python code/data_wrangling/cbs/cbs_step1_index_scans.py
uv run python code/data_wrangling/cbs/cbs_step2_download_scans.py

# Post-1917 candidate transcription (Delpher OCR pipeline; steps 4 & 6b spend
# ~$1.30 of Gemini flash-lite — ask before running). Reads the archived
# Staatscourant PDFs, writes data/delpher/delpher.duckdb.
uv run python code/data_wrangling/delpher/delpher_step3_locate_pages.py
uv run python code/data_wrangling/delpher/delpher_step4_ocr_pages.py
uv run python code/data_wrangling/delpher/delpher_step5_parse_kandidatenlijsten.py
uv run python code/data_wrangling/delpher/delpher_step6_parse_uitslagen.py
uv run python code/data_wrangling/delpher/delpher_step6b_llm_parse_uitslagen.py

# Full panel reassembly (reads DuckDBs, writes data/panel/*.parquet)
# step 1 = district era 1848-1918; step 2 = merge post-1917 -> unified 1848-1937
uv run python code/data_wrangling/panel/panel_step1_assemble.py
uv run python code/data_wrangling/panel/panel_step2_merge_post1917.py

# Phase 2a: PDC/parlement.com MP biographies -> mp_anchor (~5,849 pages,
# ~50 min at 2 req/s; writes data/pdc/pdc.duckdb + data/panel/mp_anchor*.parquet)
uv run python code/data_wrangling/pdc/pdc_step1_survey_sitemap.py
uv run python code/data_wrangling/pdc/pdc_step2_scrape_biographies.py
uv run python code/data_wrangling/pdc/pdc_step3_build_mp_anchor.py

# Phase 2b: candidate roster + OpenArchieven/GenealogieOnline linkage
# (5,507 distinct candidates; ~1.5-2hr combined at 3 req/s each, run in
# parallel; writes data/openarch/openarch.duckdb (~212MB, excluded),
# data/genealogieonline/genealogieonline.duckdb, and
# data/panel/candidate_person_pairs*.parquet)
uv run python code/data_wrangling/panel/panel_step3_candidate_roster.py
uv run python code/data_wrangling/openarch/openarch_step1_query_candidates.py
uv run python code/data_wrangling/genealogieonline/genealogieonline_step1_query_candidates.py
uv run python code/data_wrangling/panel/panel_step4_candidate_person_pairs.py

# Phase 3: occupational/dynastic status (only the best-scoring pair per
# candidate, score>=0.7, gets detail-page fetches -- ~10-15 min each; writes
# detail_records/person_pages/candidate_ancestors into the existing openarch/
# genealogieonline duckdbs, then data/panel/beroep_hisco_matches.parquet,
# dynasty_edges*.parquet, and candidate_status.parquet)
uv run python code/data_wrangling/openarch/openarch_step2_fetch_details.py
uv run python code/data_wrangling/genealogieonline/genealogieonline_step2_fetch_person_pages.py
uv run python code/data_wrangling/status/status_step1_hisco_match.py
uv run python code/data_wrangling/status/status_step2_dynasty_lineage.py
uv run python code/data_wrangling/panel/panel_step5_candidate_status.py
```

**When new data artifacts are added** (new parquets, DuckDBs, or downloaded files committed to git), update this section with the commands to regenerate them, and update the list above if any source's size or file count changes materially.

## What's committed vs. excluded

| Committed (small, regenerable with effort) | Excluded (large, regenerable) |
|---|---|
| `data/panel/*.parquet` (unified 1848-1937 `candidates_panel` + `persons_post1917`, `elections_post1917`, `gekozen_unmatched`, `mp_anchor`, `mp_anchor_unmatched`, `candidate_roster`, `candidate_person_pairs`, `candidate_person_pairs_summary`, `beroep_hisco_matches`, `dynasty_edges`, `dynasty_candidates`, `candidate_status`) + `data/panel/panel.duckdb` | `data/delpher/` — PDFs (~1.3 GB) + `delpher.duckdb` (OCR/parse tables) |
| `data/huygens/huygens.duckdb` — scraped candidate data | `data/cbs/scans/` — JPEG page scans (~263 MB) |
| `data/aieeda/aieeda.duckdb` — ingested municipal party panel | `data/aieeda/*.zip` — OSF download (34 MB) |
| `data/nlgis/maps/*.topojson` — municipality boundaries | `data/openarch/openarch.duckdb` — raw search hits (~212 MB) |
| `data/nlgis/crosswalk.duckdb` — municipality crosswalk | |
| `data/cbs/cbs.duckdb` — scan index metadata (not the JPEGs) | |
| `data/pdc/pdc.duckdb` (~13 MB) — scraped PDC biography pages + parsed functions | |
| `data/genealogieonline/genealogieonline.duckdb` (~32 MB) — raw search hits | |

## Security

`examples/.env` contains Google and OpenAI API keys. Do not commit this file. It is not used by the current Phase 1 pipelines — those keys were for earlier example work.
