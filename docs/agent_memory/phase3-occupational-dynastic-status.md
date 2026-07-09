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

## Scope decision (score >= 0.5, best pair only)

Hand-labelling in Phase 2b showed top-scored-pair precision was 71% for
famous MPs, 49% for obscure losers, but only **13% for common surnames** --
so fetching detail pages for every one of the 1.28M `candidate_person_pairs`
rows would spend most of the budget on near-certain wrong matches. Steps 1-2
above fetch only the single best pair per candidate. This is a real coverage
ceiling, not a bug -- candidates with no qualifying pair get no occupational
data by construction.

**Threshold history**: shipped initially at score >= 0.7 (~2,700 candidates/
source), then widened to **score >= 0.5** at the user's request for more
coverage (~3,015 openarch / ~2,928 genealogieonline candidates of 5,507).
This trades some *unquantified* extra precision loss for coverage -- Phase
2b's hand-labelling checked precision by strata (famous MP/obscure loser/
common surname), not by score bin, so there is no direct evidence for how
much noisier the 0.5-0.7 band is versus 0.7+. A ~30-pair hand-label spot
check of that band was offered but not requested; do one before leaning on
the expanded coverage for causal claims in the paper. All numbers below are
post-widening (0.5).

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
excluded from dynasty edges -- inspecting the (post-widening) 159 such flags
found the same two distinct, both real, phenomena as the initial 69-flag
check, in the same rough proportion:
- **97/159 are cross-era**: the same actual politician campaigned both before
  and after the 1917 reform (e.g. "Verkouteren H." 1897-1913 district era and
  "Verkouteren H." 1918 PR era) -- `candidate_roster` keys are era-scoped
  (`persoon_id` vs `person_key`), so the same person legitimately gets two
  roster rows. This is NOT a dynasty (it's one person, not two relatives) but
  it IS a free cross-era candidate-identity signal that Phase 5 (or a future
  step) could exploit to link a candidate's full 1848-1937 career across the
  reform, not currently done anywhere else in the panel.
- **62/159 are within-era**: two roster rows for what looks like the same
  real person, split by a minor initials/surname-prefix parsing variance
  (e.g. "Doude van Troostwijk H.J." vs "H. J.", "Boer P." vs "de Boer P.",
  "Swierstra N." vs "N. Tj."). This is an upstream `candidate_roster`/source
  dedup gap (Huygens `persoon_ID` or Delpher person-key assignment did not
  merge these), not a Phase 3 bug -- worth a future cleanup pass, not fixed
  here.

## CHECKPOINT numbers (`candidate_status.parquet`, 5,507 candidates, score>=0.5)

- `own_beroep` coverage: 1,166/5,507 (21.2%); own HISCLASS classified: 796
  (14.5%)
- `father_beroep` coverage: 1,208/5,507 (21.9%); father HISCLASS classified:
  907 (16.5%)
- Candidates in a dynasty group: 329/5,507 (6.0%); prior_relative_any: 127;
  later_relative_any: 123
- `titles` (mr./dr./jhr./baron -- already in `candidates_panel`, no
  scraping needed) present: 1,378/5,507 (25.0%)

(For reference, the initial score>=0.7 run gave: own_beroep 17.8%/HISCLASS
12.1%, father_beroep 16.2%/HISCLASS 12.2%, dynasty 2.7%.)

These are low in absolute terms -- a direct consequence of the score>=0.5
scope decision above (only ~55% of candidates have a qualifying pair at all,
and of those only a minority of source pages carry a beroep or a resolvable
father link). Report this ceiling explicitly if these numbers feed the
paper; do not present 21.2%/21.9% as "our occupational data quality" without
that context, and flag that the 0.5-0.7 band's precision is unverified
(see the threshold-history note above).
