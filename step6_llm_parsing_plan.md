# Step 6: switching from rule-based parsing to LLM structured output

**Status: DONE 2026-07-08.** The LLM parser (`delpher_step6b_llm_parse_uitslagen.py`)
is implemented and has run against all 183 pages with rule-based checksum
failures. See results below.

## Where we are

Pipeline steps 3–6 are **complete**; step 7 (panel merge) and the checkpoint
report are **not started**.

| Step | Script (`code/data_wrangling/delpher/`) | State |
|---|---|---|
| 3 | `delpher_step3_locate_pages.py` | done — 616 target pages located via embedded PDF text layer |
| 4 | `delpher_step4_ocr_pages.py` | done — all 616 pages OCR'd with gemini-3.1-flash-lite (raw text archived in `ocr_pages`, ~$1 spent, user-authorized) |
| 5 | `delpher_step5_parse_kandidatenlijsten.py` | done — 27,109 candidate rows, 18 kieskringen × 6 years, zero positional anomalies |
| 6 | `delpher_step6_parse_uitslagen.py` | **done — rule-based baseline** (2,521 blocks, 61–84 % checksum-ok) |
| 6b | `delpher_step6b_llm_parse_uitslagen.py` | **done — LLM hybrid pass** (Gemini flash-lite structured output, 183 pages re-parsed, see results below) |
| 7 | `panel_step2_merge_post1917.py` | not started |

All tables live in `data/delpher/delpher.duckdb`: `page_texts`,
`target_pages`, `ocr_pages` (raw OCR, the input for any re-parse),
`kandidatenlijsten` (step 5, authoritative candidate lists),
`voorkeur_stemmen` / `lijst_uitslagen` / `gekozen` / `uitslag_issues`
(step 6 outputs, rebuilt on every rerun — parsing is offline and free).

### Step 6 (rule-based) baseline → Step 6b (LLM hybrid) final results

| year | rule-based blocks | rule-based ok_rate | LLM blocks | LLM ok_rate | improvement |
|---|---|---|---|---|---|
| 1918 | 383 | 0.799 | **384** | **0.859** | +7.5%, +1 block found |
| 1922 | 549 | 0.842 | 549 | **0.856** | +1.7% |
| 1925 | 418 | 0.722 | 418 | **0.775** | +7.3% |
| 1929 | 372 | 0.672 | **378** | **0.720** | +7.1%, +6 blocks found |
| 1933 | 463 | 0.613 | **473** | **0.647** | +5.5%, +10 blocks found |
| 1937 | 336 | 0.467 | **338** | **0.547** | +17.1%, +2 blocks found |

**19 new blocks found** that the rule-based parser missed. Remaining checksum
failures are OCR digit errors (e.g. "58591" misread as "53591"), not structural
parsing errors. Elected members (`gekozen`) table is from the rule-based parser
(100/99/100/100/99/100); the LLM was not used for this section.

Cost: $0.32 (7.6M input + 0.8M output tokens).

28 blocks missing in total; failed checksums are mostly single-digit OCR
errors (e.g. sum off by 500 from one misread digit) or 1–2 dropped
candidate rows, all flagged per block in `uitslag_issues`.

### Why the rule-based parser is hitting diminishing returns

The six issues print the *same* 5-column table, but Gemini's OCR renders it
in at least seven layouts, all now handled by
`delpher_step6_parse_uitslagen.py`:

1. clean 5-cell pipe rows (1918/1922; lijst "3a." with period);
2. 1929: lijst number **without** period (`| 2 | Wijnkoop, D. | 218 |`);
3. 1933: surname/initials in separate cells + kieskring name
   hyphen-fragmented vertically (`'s Her-` / `togen-` / `bosch.`);
4. 1937: stemcijfer on the lijst's **opening** row;
5. 1925: whole lijst merged into one row (all surnames in one cell, all
   votes space-separated in another — segmented by checksum search);
6. narrow 2-cell pages (`13. van Houten, H. | 305`);
7. three physical print columns interleaved side-by-side into 9-cell rows
   (de-interleaved back into three streams).

Plus: lijsten renumbered between the candidate-list publication and the
uitslag (1922 kk15: printed 8a = candidate-list 3a — resolved by first-
candidate name), arabic lijst numbers colliding with the kieskring-
successor rule, and OCR'd-vs-OCR'd name matching (edit-distance ≤ 2).

The last diagnosed failure (not yet fixed): 1929 p11 prints combined
opener cells **without the period** (`26 Sneevliet, | H. J. F. M. | 264`),
which `LIJST_OPEN_RE` (requires `[.,]` after the number) rejects — that is
the 1929 kk4 tail (lijsten 23/26/29/31/32/34) and kk12 1–3. Every remaining
gap is another micro-variant like this.

## The question: just LLM-parse the OCR text?

**Yes — this is a good idea, and cheap.** The raw OCR text is archived in
`ocr_pages`, so a second text-only Gemini pass with **structured output**
(JSON schema, temperature 0) can replace all the layout wrangling. The
crucial insight from this session is that the *hard part is not extraction
but validation*, and we already have strong validators that stay in place
regardless of who parses:

- **step 5 `kandidatenlijsten` is authoritative**: for every (year,
  kieskring, lijst) we know the exact candidate names in order — parsing is
  alignment, not discovery;
- **stemcijfer checksum**: sum(candidate votes) must equal the printed
  stemcijfer per block;
- coverage: 18 kieskringen × known lijst sets per year.

### Proposed design (delpher_step6b or a rewrite of step 6)

1. **Input**: per page from `ocr_pages`, restricted to the vote-table
   section (first page matching "Naam en voorletters der candidaten in de
   volgorde" up to the art. 50 "Door verbinding overeenkomstig artikel 50"
   boundary — bounds logic already in step 6). ~250 pages total across the
   six issues in `MAIN_ISSUES` (dict in step 6 maps year → issue URN).
2. **Prompt**: "This is OCR text of a Dutch Staatscourant vote table
   (columns: kieskring, lijst number, candidate name, votes, stemcijfer),
   possibly multi-column/interleaved. Emit JSON." Give the model the year's
   *expected structure* to anchor it: the kieskring names I–XVIII and — per
   page, from step 5 — the candidate lists of the lijsten expected there
   (cheap: a few hundred tokens; makes alignment trivial for the model and
   prevents hallucinated names).
3. **Schema** (Gemini `response_schema`, `response_mime_type:
   application/json`):
   ```json
   {"blocks": [{
       "kieskring": 4,
       "lijst": "26",
       "continues_previous_page": false,
       "candidates": [{"name": "Sneevliet, H. J. F. M.", "votes": 264}],
       "stemcijfer": 1234
   }]}
   ```
   `votes: null` for dashes; `stemcijfer: null` when the block continues on
   the next page. Stitch pages: a block with `continues_previous_page` is
   merged with the open block from the previous page.
4. **Validation/loading**: keep the existing loader semantics — align
   returned candidates against `kandidatenlijsten` positions (the
   `name_matches`/edit-distance helper in step 6 is reusable), compute
   `checksum_ok`, write the same `voorkeur_stemmen`/`lijst_uitslagen`
   tables. Log misalignments in `uitslag_issues`.
5. **Resumability**: progress table `llm_parse_pages(issue_urn, page_no,
   model, response_json, fetched_at)` mirroring `ocr_pages`, so reruns skip
   parsed pages and parsing-from-JSON stays offline/free.
6. **Cost**: text-only. Vote-section OCR text ≈ 1.0–1.5 M input tokens +
   expected-structure context, JSON output of similar size → **well under
   $1** at flash-lite prices. (Same model + key as step 4:
   `GOOGLE_API_KEY` in `examples/.env`, model `gemini-3.1-flash-lite`.
   Per the standing agreement: ask the user before spending API calls.)

### Recommended scope: hybrid, not full replacement

The rule-based parser already yields checksum-verified blocks for
61–84 % of blocks and finds 97.6–99.8 % of them. Two options:

- **Option A (recommended): targeted LLM pass.** Re-parse only pages
  containing missing or checksum-failed blocks (~150–200 pages), keep
  rule-based rows where `checksum_ok`, replace/fill where the LLM block
  passes the checksum and the rule-based one doesn't. Cheapest, and every
  accepted row is checksum-verified either way.
- **Option B: full LLM re-parse** of all vote-table pages, using the
  rule-based output only as cross-check. Simpler mental model, still <$1;
  choose this if Option A's reconciliation logic feels fiddlier than it's
  worth.

Either way **do not touch steps 3–5** — their outputs are solid — and keep
`gekozen` parsing as-is except for the two open questions below.

## Also still open (independent of parser choice)

1. **`gekozen` = 99 for 1922 and 1933** (should be 100). Not yet
   diagnosed: one elected-members table row is dropped per year — probably
   one more OCR row-format variant on those pages. Diagnose by dumping the
   `Vaststelling van den uitslag` pages (`VASTSTELLING_RE` in step 6 finds
   the start page) and diffing against nl.wikipedia "Lijst van Tweede
   Kamerleden 1922–1925 / 1933–1937". Could also be folded into the LLM
   pass (same schema idea, elected table is trivial).
2. **After step 6**: build `code/data_wrangling/panel/panel_step2_merge_post1917.py`
   — assemble candidate × election rows (provenance
   `staatscourant_<issue_urn>`), extend `candidates_panel` in
   `data/panel/panel.duckdb` to 1848–1937, re-export parquet; validate
   elected sets vs 100 seats + Wikipedia/PDC member lists and stemcijfers
   vs AIEEDA national totals; update `code/data_wrangling/README.md` and
   the CLAUDE.md regeneration section.
3. **Then STOP at the spec's CHECKPOINT** (`post_1917_candidates.md`):
   report row counts and OCR quality and wait before Phase 2.
