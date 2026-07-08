# Post-1917 Staatscourant transcription state

Post-1917 candidate transcription (spec: `post_1917_candidates.md`), built
2026-07-07/08. Related: [phase1-pipeline-state.md](phase1-pipeline-state.md).

- Pipeline: delpher steps 3-6 + panel step 2 in `code/data_wrangling/`.
- Step 3 locator: issue PDFs carry the Delpher OCR as embedded text layer
  (same OCR as articles.ocr_text) → pages assigned to target articles by
  verbatim line overlap (>=15% of lines, >=5 hits). 616 target pages across
  the 14 key issues (100 kandidatenlijst + 516 uitslag). Non-election
  bijvoegsel pages (PRIJSCOURANT goods tables, ministry tables) correctly
  excluded; the uitslag bijvoegsels are nearly ALL candidate-level content
  (preference votes per kieskring, seat allocation, elected + ranked
  non-elected lists).
- Step 4 re-OCR: gemini-3.1-flash-lite (user-authorized 2026-07-07, key in
  `examples/.env`), 300 dpi grayscale JPEG via pdftoppm, temperature 0,
  layout prompt (columns top-to-bottom, one entry per line, " | " table
  cells). Smoke test: ~1.4k input + ~2.8k output tokens/page, quality good
  (names/votes/diacritics correct; wrapped married names like "Breedvelt
  geb. de Waal" can split across lines). Raw output archived per page in
  ocr_pages (resumable) — parsing is offline/free thereafter.
- Gemini OCR formatting varies per page (pipe-tables vs plain lines) —
  parsers must normalize pipes/whitespace first.
- DuckDB is single-writer: cannot query delpher.duckdb while step 4 runs.
- Validation sources for elected sets: nl.wikipedia "Lijst van Tweede
  Kamerleden <term>" pages (per-term member lists) + national party totals
  on "Tweede Kamerverkiezingen <year>" pages. No PDC data on disk.
- Party labels do NOT appear in the candidate-list publications (lists
  identified by number + candidates only, pre-1933 especially); party_label
  in lijst_uitslagen may stay NULL and be resolved later via lijstengroep +
  PDC.
- 2026-07-08 CHECKPOINT (post-1917 done): `panel_step2_merge_post1917.py`
  merges the delpher tables into a UNIFIED `candidates_panel` spanning
  1848-1937 (`era` column; district cols NULL on PR rows and vice-versa).
  35,615 rows = 8,506 district + 27,109 PR candidacies. PR grain = (year,
  kieskring, lijst, positie) with preference `votes`, list `stemcijfer`,
  `residence`; `elected` is PERSON-level (propagated across a person's
  kieskring rows — PR seats are person-level). New tables:
  `persons_post1917` (dedup person-election view), `elections_post1917`,
  `gekozen_unmatched`. Elected match rate vs 100 seats: 100/98/99/97/97/98
  (surname reorder for inverted tussenvoegsel + tiered full/first-initial +
  residence + Levenshtein<=2 fuzzy). 11 seats OCR-unlinkable (married "geb."
  names, patronymics "Vos, Azn", truncated OCR, a few list blocks that
  failed OCR) — logged in `gekozen_unmatched`, recoverable in Phase 2.
  Person identity is PROVISIONAL (`person_key='sc:<surname>|<initials>'`);
  cross-era/source resolution is Phase 2. NEXT: Phase 2 (`phase_2_and_onward.md`)
  — wait for user before starting.
- (superseded) 2026-07-08: step 6 rule-based parser reached block coverage
  0.976–0.998, checksum ok_rate 0.47–0.84, gekozen 100/100 except 99 for
  1922+1933. Handles 7 OCR layout variants (incl. 3-column de-interleave,
  merged 1925 rows, renumbered 1922 lijsten, edit-distance name matching).
  NEXT: `step6_llm_parsing_plan.md` in the repo root documents the agreed
  plan — a targeted text-only Gemini structured-output pass over
  `ocr_pages` for the remaining missing/checksum-failed blocks (validated
  by step-5 alignment + stemcijfer checksums), then panel_step2 merge +
  CHECKPOINT. Ask user before spending Gemini calls.
