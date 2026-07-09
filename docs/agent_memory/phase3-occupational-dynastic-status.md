# Phase 3: occupational & dynastic status

Built 2026-07-09 (spec: `phase_2_and_onward.md`, Phase 3). Pipeline, in order:

1. `code/data_wrangling/openarch/openarch_step2_fetch_details.py` -- fetches
   `records/show.json` for the single best-scoring OpenArchieven pair per
   candidate (score >= 0.5), extracting the candidate's own profession and
   (via `RelationType`) their father's, from the same civil-registry record.
2. `code/data_wrangling/genealogieonline/genealogieonline_step2_fetch_person_pages.py`
   -- fetches the best-scoring GenealogieOnline person page per candidate
   (score >= 0.5), then follows the male-parent (`father_url`) chain 3
   generations back (father, grandfather, great-grandfather), storing every
   page visited once (deduped across candidates) in `person_pages`, and the
   per-candidate ancestor mapping in `candidate_ancestors(era, key, url, depth)`.
3. `code/data_wrangling/status/status_step1_hisco_match.py` -- matches every
   distinct harvested beroep/profession string to HISCO/HISCLASS/HISCAM_NL via
   the HSN dictionary (`examples/hisco/hsn2013a_hisco_comma.csv`).
4. `code/data_wrangling/status/status_step2_dynasty_lineage.py` -- shared-
   ancestor dynasty detection over `candidate_ancestors` (combined depth <= 3),
   union-find grouping, prior/later-relative flags.
5. `code/data_wrangling/panel/panel_step5_candidate_status.py` -- final
   assembly into `data/panel/candidate_status.parquet`.

## Scope decision (score >= 0.7, best pair only) -- FINAL, after a reversal

Hand-labelling in Phase 2b showed top-scored-pair precision was 71% for
famous MPs, 49% for obscure losers, but only **13% for common surnames** --
so fetching detail pages for every one of the 1.28M `candidate_person_pairs`
rows would spend most of the budget on near-certain wrong matches. Steps 1-2
above fetch only the single best pair per candidate, at score >= 0.7. This
is a real coverage ceiling, not a bug -- candidates with no qualifying pair
get no occupational data by construction.

**Threshold history (do not re-widen without a fresh spot-check)**: shipped
at 0.7, briefly widened to **score >= 0.5** for more coverage (~3,015
openarch / ~2,928 genealogieonline candidates of 5,507), then **reverted to
0.7** after a 30-pair hand-labelled spot-check of the 0.5-0.7 band measured
only **~30% precision** (9/30: 8 clear TRUE + 1 unsure-lean-true), far below
even the "obscure loser" strata figure (49%) from Phase 2b. Labelled sample
+ reasoning saved at `docs/agent_memory/phase3_spotcheck_0.5_0.7_band.csv`
for auditability (AI-derived judgement, not independently human-verified,
same caveat as the Phase 2b calibration set).

**What actually made most of those pairs wrong**: the scoring gate's floor
(`0.3 + 0.7*feature`, see Phase 2b memory) lets a pair through at 0.5-0.7 on
surname+year agreement alone even when the GIVEN NAME is completely wrong --
most damningly, **7/15 of the openarch failures were outright gender
mismatches** (candidate matched to an unambiguously female first name like
"Steijntje", "Gertruda", "Aafke", "Aagje" for what must be a male pre-1918
district candidate) that the scorer has no feature to catch at all. The
genealogieonline band did somewhat better (5/15 clear TRUE) but still far
below 0.7+ strata precision; several of its true positives needed real
disambiguation work (a hyphenated given name "Eilard-Jacobus" my naive
initials-splitter mishandled; a "Jhr." nobility title apparently folded into
the initials field; one candidate ruled TRUE only via outside historical
knowledge -- "Rutgers V.H." is almost certainly Victor Hugo Rutgers, a real
ARP minister, which also let me rule out a competing wrong same-surname
hit). **Do not re-widen this threshold without redoing a spot-check and
either fixing the gender-blind spot in `panel_step4_candidate_person_pairs.py`'s
scoring or accepting the same ~30% precision.**

`code/data_wrangling/panel/panel_step5_candidate_status.py` and
`code/data_wrangling/status/status_step2_dynasty_lineage.py` both
RE-FILTER to the qualifying (score>=0.7) candidate set at read time rather
than trusting whatever is currently seeded in `candidate_ancestors` --
this matters because that table is cumulative (`INSERT OR IGNORE`, never
pruned) and was seeded wider during the 0.5 experiment. `person_pages` and
`candidate_ancestors` in the committed `genealogieonline.duckdb` may
therefore contain more rows than the final `candidate_status.parquet`
actually uses -- harmless surplus, not stale data in use, but don't assume
row counts in those raw tables reflect the assembled table's scope.

## API/parsing notes (verified live 2026-07-09)

- **OpenArchieven `records/show.json?archive=<code>&identifier=<id>`**:
  returns `Person[]` (each `@pid`, `PersonName`, optional `Profession`/`Age`),
  `Event` (type/date/place), `RelationEP[]` linking `PersonKeyRef` to a
  `RelationType` string ("Bruidegom"/"Bruid", "Vader/Moeder van de
  bruidegom/bruid" for BS Huwelijk; "Kind"/"Vader"/"Moeder" for BS Geboorte).
  `Profession` is present on adults in marriage records far more often than in
  birth records (a birth record's parents are often not given an occupation
  at all). Father-relation lookup: `Kind`->`Vader`, `Bruidegom`->`Vader van de
  bruidegom`, `Bruid`->`Vader van de bruid`.
- **GenealogieOnline person pages**: markup unchanged from the
  `examples/genealogie/ind_step04_scrape_genealogie.py` template this reuses
  (`_parse_beroep`/`_parse_birth_place`/`_parse_father` ported verbatim into
  `genealogieonline_async_helpers.py`). Occupation lives in a
  `<ul class="nicelist"><li>Beroep: ...</li>` entry (absent on most pages --
  normal, not a parse failure: only 2,367/8,401 fetched pages, ~28%, had one).
  The male parent is a `<div itemprop="parent">` with a `gender` meta of
  "male"; only that line is followed (gender-clean patrilineal spine, same
  rationale as `ind_step06_build_lineages.py`) -- **this means dynasty
  detection here is patrilineal-only and will miss maternal-line or
  marriage-based ties.**
- GenealogieOnline beroep strings are messy free text (compound careers,
  "Bron N" source citations, leading date ranges like "van 1917 tot 1947
  Arbeider, landbouwer. Bron 1"). `status_step1`'s `clean_beroep()` strips
  citation/date-range noise and tries the first clause of a comma/semicolon-
  split compound description as a fallback match target -- this alone lifted
  classified coverage from 46.9% to ~65% of distinct beroep strings (stable
  across both the 0.7 and 0.5-threshold runs: 65.8% of 1,664 strings, then
  65.1% of 2,472). Many of
  the remaining unmatched strings are political/military office titles
  ("Lid van de Tweede Kamer", "generaal majoor artillerie") that are
  genuinely outside HISCO's occupational vocabulary, not a matching failure --
  a few others are mojibake-encoded (rare, one tree's non-UTF8 export).
  (These string-level counts are over the WIDER raw fetch left in the
  duckdbs from the 0.5 experiment, not re-filtered to 0.7 -- harmless, since
  `status_step1` just builds a beroep->HISCO lookup table; the CHECKPOINT
  numbers below are the ones that matter and ARE filtered to 0.7.)

## HISCO matching strategy

`Original` (95,298 rows) is near-unique raw OCR text -- a poor exact-match
target for our own already-legible strings. `Standard` (34,258 distinct) is
the normalised occupation title and matches much better. Order: exact match
on normalised text against `Standard`, then `Original`, then a Levenshtein
fuzzy fallback (`name_match_utils.lev`, bucketed by first-letter+length to
keep ~1,700 distinct strings x 34k vocab tractable) at ratio >= 0.82. A
one-off gotcha: `Standard`/`Original` have a handful of blank cells --
`.astype(str)` would turn those into the literal string `"nan"` and pollute
the vocabulary; use `.fillna("")` instead (skipped by the empty-key guard).

## Dynasty definition and a real finding from the data

Two candidates are the same dynasty if their patrilineal ancestor chains meet
within a **combined depth of 3** (father-son=1, grandfather-grandson=2,
siblings via shared father=1+1=2, great-grandparent-descendant=3, uncle-
nephew via shared grandfather=1+2=3; first cousins at 2+2=4 are NOT covered
-- a deliberate, documented cutoff, not a bug). Connected components (union-
find) group transitively into `dynasty_id`.

`depth_a=0 AND depth_b=0` (two *different* roster candidates resolved to the
*identical* GenealogieOnline person) is flagged `same_person_flag=TRUE` and
excluded from dynasty edges -- inspecting the (final, 0.7-filtered) 113 such
flags found two distinct, both real, phenomena:
- **83/113 are cross-era**: the same actual politician campaigned both before
  and after the 1917 reform (e.g. "Verkouteren H." 1897-1913 district era and
  "Verkouteren H." 1918 PR era) -- `candidate_roster` keys are era-scoped
  (`persoon_id` vs `person_key`), so the same person legitimately gets two
  roster rows. This is NOT a dynasty (it's one person, not two relatives) but
  it IS a free cross-era candidate-identity signal that Phase 5 (or a future
  step) could exploit to link a candidate's full 1848-1937 career across the
  reform, not currently done anywhere else in the panel.
- **30/113 are within-era**: two roster rows for what looks like the same
  real person, split by a minor initials/surname-prefix parsing variance
  (e.g. "Doude van Troostwijk H.J." vs "H. J.", "Boer P." vs "de Boer P.",
  "Swierstra N." vs "N. Tj."). This is an upstream `candidate_roster`/source
  dedup gap (Huygens `persoon_ID` or Delpher person-key assignment did not
  merge these), not a Phase 3 bug -- worth a future cleanup pass, not fixed
  here.

## CHECKPOINT numbers (`candidate_status.parquet`, 5,507 candidates, score>=0.7, FINAL)

- `own_beroep` coverage: 982/5,507 (17.8%); own HISCLASS classified: 667
  (12.1%)
- `father_beroep` coverage: 937/5,507 (17.0%); father HISCLASS classified:
  697 (12.7%)
- Candidates in a dynasty group: 247/5,507 (4.5%); prior_relative_any: 100;
  later_relative_any: 98
- `titles` (mr./dr./jhr./baron -- already in `candidates_panel`, no
  scraping needed) present: 1,378/5,507 (25.0%)

(Dynasty/father-occupation numbers here are slightly HIGHER than the very
first 0.7 checkpoint -- 2.7%/16.2% -- not because the threshold changed back
incompletely, but because the 0.5-widening run's fetch pass incidentally
retried and completed a handful of ancestor-chain pages that had failed
transiently on the very first pass; `qualifying_candidates()` in
`status_step2` and `best_pairs()` in `panel_step5` both confirm the
qualifying candidate SET is identical to the original 0.7 run, 1,866
genealogieonline candidates -- only the underlying page data got more
complete, not the scope.)

These are low in absolute terms -- a direct consequence of the score>=0.7
scope decision (only ~50% of candidates have a qualifying pair at all, and
of those only a minority of source pages carry a beroep or a resolvable
father link). Report this ceiling explicitly if these numbers feed the
paper; do not present 17.8%/17.0% as "our occupational data quality" without
that context.
