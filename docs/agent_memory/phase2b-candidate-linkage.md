# Phase 2b: candidate -> genealogical person linkage

Built 2026-07-08 (spec: `phase_2_and_onward.md`, Phase 2b). Pipeline:
`code/data_wrangling/panel/panel_step3_candidate_roster.py` (shared roster) ->
`code/data_wrangling/openarch/openarch_step1_query_candidates.py` +
`code/data_wrangling/genealogieonline/genealogieonline_step1_query_candidates.py`
(source scrapers, run independently/in parallel) ->
`code/data_wrangling/panel/panel_step4_candidate_person_pairs.py` (scoring).

## Scope

`candidate_roster.parquet` dedupes `candidates_panel`'s 35,615 candidacy ROWS
down to 5,507 distinct CANDIDATES (1,868 district era by `persoon_id`, 3,639
PR era by `person_key`) -- linkage runs at the person level, matching
`mp_anchor`'s grain. 821 of these (15%) already have a PDC birth-year anchor
from `mp_anchor` (Phase 2a); the rest get a heuristic birth-year window from
candidacy years (`[year_max-75, year_min-30]`, widened defensively when a
>45yr candidacy span would otherwise invert the window -- passive suffrage
was constant at age 30 for the whole 1848-1937 span, verified 2026-07-08, so
`year_min-30` is a real upper bound on birth_year; 75 is a soft, not
constitutional, plausibility cap on age at last candidacy).

## API endpoints (verified live 2026-07-08)

- **OpenArchieven** `records/search.json?name=<SURNAME>+<yr>-<yr>&sourcetype=BS+Geboorte|BS+Huwelijk`:
  search is SURNAME-ONLY (a combined "firstname surname" query silently
  returns 0 hits) but DOES support multi-word surnames verbatim ("Oldenhuis
  Gratama" finds the exact record). Search-result docs already carry
  personname/eventdate/eventplace/relationtype -- no `records/show.json`
  detail call needed for identity scoring (profession/parents deferred to
  Phase 3). `relationtype` distinguishes the candidate's OWN record ("Kind"
  for birth, "Bruid"/"Bruidegom" for marriage) from a parent's or witness's
  row under the same search hit.
- **GenealogieOnline** `/zoeken/index.php?q=<SURNAME>&vn=<firstname>&gv=<yr>&gt=<yr>`:
  `vn=` needs a FULL first name -- a bare initial ("vn=J") returns 0 hits, so
  search leaves `vn=` blank and matches initials client-side against each
  hit's full name (parsed from the "Name (YYYY-YYYY) >> tree" snippet, regex
  in `genealogieonline_async_helpers.py`). Unlike OpenArchieven, this site's
  `q=` does NOT handle a multi-word surname phrase (0 hits for "Oldenhuis
  Gratama" combined, though each word alone matches) -- search uses only the
  surname's LAST word for compound surnames.

## The dominant bug (found via the winner/loser gap reversing)

First full run showed elected/winners matching WORSE than losers (47% vs
56% at score>=0.7), the opposite of `phase_2_and_onward.md`'s expectation.
Root cause: both scrapers searched using `sn` (the surname normalised for
SCORING -- concatenated multi-word surnames, y/ij spelling folded) instead
of `surname_raw` (original spelling/spacing). This returned **zero hits for
~40% of all candidates** -- anyone with a multi-word surname ("Oldenhuis
Gratama" -> "oldenhuisgratama", unsearchable) or an "ij"-spelled name
("Nedermeijer" -> "nedermeyer", 0 hits vs 12 for the correct spelling) --
hitting compound/aristocratic surnames (common among district-era MPs)
hardest. Fixed by searching on `surname_raw` (full phrase for OpenArchieven,
last word only for GenealogieOnline) and redoing the full scrape. Candidates
with >=1 pair jumped 65%->95%; winner/loser gap flipped to the expected
direction. **Lesson: never use the scoring-normalised form of a field to
build an external search query -- normalisation and searchability are
different concerns.**

A secondary, smaller bug: anchored candidates' birth-year search window was
initially `[birth_year, birth_year]` (zero width) -- any record differing by
even 1 year from PDC's stated birth year would be invisible to the search
itself, not just scored lower. Widened to +/-2 years.

## Score calibration (hand-labelled, 2026-07-08)

100 candidates sampled stratified across famous MP (`elected` + `mp_anchor`
ground truth, n=35), obscure loser (no anchor, uncommon surname, n=35), and
common surname (>=8 candidates sharing a normalised surname, n=30). Labelled
by Claude's own best-effort judgement (cross-checked against `mp_anchor`
birth/death dates where available) at the user's direction -- **this is an
AI-derived calibration set, not independently human-verified**, and should
be treated as weaker evidence than a human-labelled set if used in the
paper. Results: famous_mp 71% true / obscure_loser 49% true / common_surname
13% true (top-scored pair only) -- confirms losers and common surnames are
much harder to link, as expected.

Findings that changed the scoring code in `panel_step4_candidate_person_pairs.py`:
- **Dominant false-match pattern**: `weighted_score()` was a plain weighted
  AVERAGE, so a hit with a completely wrong first name/initials (e.g. a
  female name matched to what's almost certainly a male 19th-c. candidate)
  could still score 0.6-0.9 whenever surname+year happened to align by
  coincidence -- good features "rescued" a bad one. Fixed with a
  multiplicative GATE on both `feat_surname` and `feat_initials`
  (`gate = (0.3+0.7*surname) * (0.3+0.7*initials)`, floor 0.3x each so a
  partial match still surfaces rather than being hard-excluded).
- Single-initial candidates ("A.") give ANY same-first-letter hit a perfect
  initials score regardless of surname -- this is why the surname gate is
  needed in addition to the initials gate, not instead of it.
- `score_initials`'s old flat "0.85 for any prefix-consistent match" bonus
  didn't scale with HOW MUCH of the initials sequence was covered (1-of-3
  scored the same as 2-of-3); rewritten to scale by coverage fraction.
- Stillborn/deceased-infant registrations ("Levenloos [...] Kind",
  "doodgeboren") and OpenArchieven's combined "1e: X; 2e: Y" remarriage-
  listing format can't be the candidate themselves -- excluded outright
  (`is_invalid_hit()`) rather than merely scored down.
- `split_given_surname`'s trailing-window search was capped at 4 tokens,
  missing long compound noble surnames (5 words, e.g. "van der Maesen de
  Sombreff") -- extended to 6.
- Remaining known limitation, NOT fixed: single-initial candidates are
  fundamentally gender-ambiguous from initials alone (can't distinguish
  "Cornelis" from "Cornelia" given just "C."); several hand-labelled ties
  between a male and female hit scoring identically reflect this, not a bug.
- A few upstream data-quality artifacts surfaced during labelling (place
  text leaking into the `initials` column for a handful of PR-era rows,
  e.g. "Jan"/"'s Gravenhage" instead of dot-initials; a female honorific
  "Mej." not stripped from one candidate's initials) are pre-existing issues
  in `candidates_panel`'s Delpher-parsed columns, out of scope for this
  pipeline -- worth a future cleanup pass.

## Final numbers (`candidate_person_pairs.parquet`, 1,281,338 pairs)

- Candidates with >=1 pair: 5,232/5,506 (95.0%)
- Share of candidates with best pair score >=0.7: 48.6% overall
- Winner vs loser gap (score>=0.7): elected 72.9% vs non-elected 43.7% --
  the expected direction and a much larger gap than the initial (buggy) run
  showed, confirming losers are substantially harder to link (no birth-date
  anchor, common surnames, initials-only names).
- `candidate_person_pairs_unmatched` is NOT a separate file -- unmatched
  candidates are simply rows in `candidate_person_pairs_summary.parquet`
  with `n_pairs=0` (274 candidates, 5.0%).

Confidence-scored, not filtered, per project convention: every OpenArchieven
own-identity hit and GenealogieOnline search hit is kept with its score;
thresholding is a downstream (Phase 3+) decision.
