# Phase 4: electoral environment + empirical design memo

Built 2026-07-09 (spec: `phase_2_and_onward.md`, Phase 4). This phase has no
new scraping -- both deliverables read only already-assembled panel data.

## Electoral environment pipeline

`code/data_wrangling/panel/panel_step6_electoral_environment.py` reads
`data/panel/panel.duckdb`'s `candidates_panel` and writes
`data/panel/electoral_environment.parquet` at **candidacy grain**
(era, key, year, race_key) -- NOT candidate grain like
`candidate_status.parquet`. This matters: electoral environment varies every
time someone stands, so joining it to `candidate_status` requires going
through `candidates_panel` on (era, key, year, race identifiers), not a
direct (era, key) join.

District era (grouped by `uitslag_id` = one race/round):
- `margin_of_victory_pct`: RACE-level (not candidate-level) RD closeness --
  pct at rank==zetels minus pct at rank==zetels+1, i.e. the vote-share gap
  between the most marginal winner and most marginal loser. NULL when the
  race has <=zetels candidates (uncontested, no losing side to compare).
- `is_marginal_winner`/`is_marginal_loser`: flags the actual two rows the
  margin above was computed from -- this is the real RD analysis sample,
  not "everyone in a race with a small margin."
- `margin_to_kiesdrempel_pct`: distance to the FIRST-ROUND majority
  threshold (kiesdrempel is a raw vote count in the source data, ~50% of
  the race's total votes +1); NULL in herstemming (runoff) rounds since
  kiesdrempel itself is a first-round-only concept.
- `had_runoff`: whether this district+year combo has BOTH a non-runoff and
  a herstemming round in `candidates_panel` -- i.e. no one cleared
  kiesdrempel in round 1. True for 4,105/8,506 district-era candidacy-rows.
- `enc`: Laakso-Taagepera effective number of candidates within the race.

PR era (grouped by kieskring_name + lijst_no + year = one list):
- `relative_position` = positie / list_length.
- `list_total_votes`: sum of candidate preference votes on the list -- a
  PROXY for party/list strength. **`candidates_panel`'s `affiliation` column
  is 100% NULL for every PR-era row** (Delpher/Staatscourant OCR never
  captured a party name, only list numbers) -- there is no real party label
  anywhere in this panel for 1918-1937, so this vote-sum proxy is the best
  available stand-in, and it under-counts true list votes since preference
  votes were optional and most electors just voted the top name.
- `elected_cutoff_position`: max positie with elected==True on the list --
  DESCRIPTIVE ONLY. Interwar Dutch PR seats were awarded overwhelmingly by
  list order (a party choice), not a vote-margin cutoff, so this is NOT a
  valid RD running variable the way the district-era vote margin is -- see
  design memo section 3 (Design A) for why the PR era has no close-contest
  RD design in this project.

## Design memo

`phase4_design_memo.md` (repo root, not under docs/ -- follows the existing
root-level convention of `phase_2_and_onward.md`/`post_1917_candidates.md`
as driving documents). Literature scan finding that matters most: the
causal effect of officeholding on later dynasty formation is
**positive/large under personal-vote, candidate-centered rules**
(Querubin/Philippines, Rossi/Argentina, Dal Bó et al./US) and **null under
party-centered/list rules** (Van Coppenolle/UK, Fiva-Smith/Norway) -- the
1917 Dutch district-to-PR switch moves the SAME country across exactly this
axis, which is the project's actual value-add (not a new estimator).

Three designs, ranked:
- **A (primary)**: pre-1918 close-election RD on `later_relative_any`. Real
  N ceiling: of the 1,204 distinct candidates ever flagged
  `is_marginal_winner`/`is_marginal_loser`, only 653 have a qualifying
  (score>=0.7) GenealogieOnline link at all -- and that shrinks further with
  bandwidth (297 candidates at |margin_of_victory_pct|<=5pp). Base rate of
  `later_relative_any` in the linked subsample is ~12% (77/653). **Do not
  join the RD sample against `candidate_status.parquet`'s
  `later_relative_any` column via `.notna()` to check coverage** -- that
  column is `fillna(False)`-populated for EVERY roster candidate regardless
  of whether a qualifying genealogy link exists, so `.notna()` always
  returns 100% and silently hides the real coverage ceiling. Check
  `candidate_person_pairs.parquet` (source=='genealogieonline',
  score>=0.7) directly instead, as the design memo's numbers do.
- **B (corroborating)**: family fixed effects, 115 dynasty groups (247
  candidates), 64 of which span BOTH eras (same family, one member each
  side of 1917) -- the useful subset for testing whether the within-family
  effect changed after the reform. Only 31/115 groups have any within-group
  variation in `elected` at all.
- **C (framing only, not causal)**: full-panel descriptive DiD, status x
  post-1917 interaction on `elected`. Largest N (35,615 candidacy-rows) but
  no exogenous status variation -- report the raw cross-tab (dynastic
  candidates: elected 32.9%->62.5% pre-1918, 6.4%->36.0% post-1917) as
  motivation, not as a causal estimate.

Next: estimation itself (not started). No further data construction is
planned before that -- this was the FINAL CHECKPOINT.
