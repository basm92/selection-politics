# Phase 4 design memo: dynasty construction & empirical strategy

FINAL CHECKPOINT per `phase_2_and_onward.md` Phase 4 — written before any
causal estimation. Ranks three empirical designs against the panel actually
assembled (not an idealized one), with coverage/power numbers pulled from
`data/panel/candidate_status.parquet` (Phase 3) and the new
`data/panel/electoral_environment.parquet` (this phase,
`code/data_wrangling/panel/panel_step6_electoral_environment.py`).

**Overarching question**: does status (occupational, dynastic) increase the
likelihood of selection into politics and of winning, and how did the 1917
district→PR rupture change who selects in? Wealth is out of scope (HIP-NL
dropped, see CLAUDE.md).

## 1. Literature scan

The workhorse identification strategy across this literature is a
**close-election regression discontinuity** comparing candidates who barely
won their first race to candidates who barely lost it, using either
subsequent officeholding by relatives or the candidate's own re-election as
the outcome:

- **Dal Bó, Dal Bó & Snyder (2009)**, *Political Dynasties*, Review of
  Economic Studies 76(1):115–142. US Congress since 1789; IV on tenure
  length (not close-election RD) shows longer-serving legislators are more
  likely to have relatives enter Congress later — "political power is
  self-perpetuating."
- **Querubin (2016)**, *Family and Politics: Dynastic Persistence in the
  Philippines*, QJPS 11(2):151–181. Close-election RD: candidates who barely
  win their first race are ~5x more likely to have a relative in office
  later than candidates who barely lose. Personal-vote, candidate-centered
  electoral system.
- **Rossi (2017)**, *Self-Perpetuation of Political Power: Evidence from a
  Natural Experiment in Argentina*, Economic Journal 127(605):F455–F473.
  Natural experiment (randomly assigned term lengths in the first post-1983
  Argentine Congress) rather than close-election RD, but the same causal
  question — longer tenure → higher probability of a relative's later
  candidacy.
- **Van Coppenolle (2017)**, *Political Dynasties in the UK House of
  Commons: The Null Effect of Narrow Electoral Selection*, LSQ 42(3):
  449–475. Same close-election RD design as Querubin, applied to the UK
  House of Commons post-1832 — finds a **null** effect. Centralized-party,
  not purely personal-vote, environment.
- **Fiva & Smith**, *Political Dynasties and the Incumbency Advantage in
  Party-Centered Environments*, APSR. Norway 1945–2013, closed-list PR,
  RD design on incumbency: ~7% dynasty share (comparable to the US) but
  **no causal effect** of incumbency on dynasty formation — echoes Van
  Coppenolle's null in a different party-centered system.
- **Smith (2018)**, *Dynasties and Democracy: The Inherited Incumbency
  Advantage in Japan*, Stanford UP. Comparative theory (22 democracies +
  Japan candidate-level data): dynastic persistence is highest and most
  causally self-perpetuating where the electoral system rewards a
  **personal vote** (SNTV-era Japan, Philippines, US), and comparatively
  muted where party lists mediate access to the ballot.
- **Berlinski & Dewan (2011)**, *The Political Consequences of Franchise
  Extension: Evidence from the Second Reform Act*, QJPS 6(3-4):329–376, and
  **Berlinski, Dewan & Van Coppenolle (2014)**, *Franchise Extension and the
  British Aristocracy*, LSQ 39(4). UK 1867 Reform Act: franchise extension
  raised opposition candidacy but did not dislodge the aristocracy from
  Parliament or Cabinet — suffrage extension changed *competition*, not
  *composition*, at least not quickly.

**The pattern that matters for us**: the causal effect of officeholding on
future dynasty formation is **positive and large under personal-vote,
candidate-centered rules** (Philippines, Argentina, US) and **null under
party-centered/list rules** (UK, Norway). This is exactly the axis the 1917
Dutch reform moves the country across — district-plurality with a genuine
personal vote (1848–1918) to closed-ish party lists (1918–1937). No single
country in the literature above provides a *within-panel, same-electorate*
test of this personal-vote vs. party-list contrast; that is the Dutch
value-add, not a new close-election estimator. The franchise-extension
literature (Berlinski et al.) is the second relevant strand: it warns that
naive pre/post comparisons around a reform can be confounded by whichever
enfranchisement wave coincides with it — the Netherlands has three
suffrage extensions (1887, 1896, 1917 male-universal, 1922 female) that
must be handled as covariates/controls, not ignored, in any 1917 DiD.

## 2. Data assets available

- `candidate_status.parquet` (Phase 3, 5,507 roster candidates): own/father
  HISCLASS, dynasty membership (`dynasty_id`, patrilineal-only, depth≤3),
  `prior_relative_any/elected`, `later_relative_any/elected`. Scope ceiling:
  status data only exists for candidates with a score≥0.7 genealogy pair
  (~1,866/5,507 have a qualifying GenealogieOnline link; own/father-beroep
  coverage 17.8%/17.0%; dynasty membership 4.5%). See
  `docs/agent_memory/phase3-occupational-dynastic-status.md`.
- `electoral_environment.parquet` (this phase, 35,615 candidacy-rows): for
  the district era, per-race margin to the majority threshold
  (`margin_to_kiesdrempel_pct`), runoff structure (`is_runoff_round`,
  `had_runoff`), and the RD closeness measure
  (`margin_of_victory_pct` = vote-share gap between the marginal winner,
  rank==zetels, and marginal loser, rank==zetels+1, at race level) with
  `is_marginal_winner`/`is_marginal_loser` flags identifying the actual RD
  analysis sample. For the PR era: list position, list length, and a list
  vote-share **proxy** for party strength (no party name is available for
  any PR-era candidate — `affiliation` is 100% null in `candidates_panel`,
  a known Delpher-OCR gap, see CLAUDE.md data-sources table).

## 3. Three designs, ranked by identification strength given this data

### Design A (recommended primary): close-contest RD, pre-1918 district era

Following Querubin/Van Coppenolle/Fiva-Smith: compare candidates who barely
won (`is_marginal_winner`) to candidates who barely lost
(`is_marginal_loser`) the same race, at shrinking bandwidths of
`margin_of_victory_pct`, on the outcome `later_relative_any`/
`later_relative_elected` from `candidate_status.parquet`.

**Coverage** (joining the RD sample against the actual qualifying
GenealogieOnline link, not against the fillna(False)-populated
`later_relative_any` column, which defaults to False for undetermined
candidates too and would silently overstate N):

| bandwidth | candidacy-rows | distinct candidates | with qualifying genealogy link |
|---|---|---|---|
| any margin | 5,385 | 1,204 | 653 |
| ≤10pp | 1,752 | 637 | 372 |
| ≤5pp | 972 | 497 | 297 |
| ≤2pp | 446 | 309 | 187 |

Base rate of `later_relative_any` in the ≥0.7-link subsample is ~12%
(77/653 at full bandwidth). This is a **small-N, rare-outcome RD** — a few
hundred usable observations at the bandwidths where the RD assumption
(local randomness of the margin) is most credible. It can plausibly detect
a Querubin-sized effect (order of several-fold local jump) but not a
Fiva-Smith/Van-Coppenolle-sized null with much precision. Report this as an
explicit power ceiling, not a silent limitation.

Only ~50% of the RD sample clears the score≥0.7 genealogy-link gate at all
(653/1,204) — this is the same Phase 2b/3 coverage ceiling propagating
forward, not a new problem, but it halves the *usable* RD sample relative
to the *nominal* one and should be stated that way in any write-up.

**Why pre-1918 only**: the PR era's `elected_cutoff_position` (max list
position with `elected==True`) is not a clean RD running variable — Dutch
interwar PR seats were awarded overwhelmingly by list order, which the
party chose, not by a vote-margin cutoff outside the party's control (a few
preference-vote overrides existed but are not separable from party
placement decisions in the current data). A position-based RD in the PR
era would confound "close to being elected" with "the party trusted this
candidate," which is precisely the selection mechanism under study, not a
valid instrument for it.

### Design B: family fixed effects on the linked lineage panel

Compare candidates within the same `dynasty_id` group (patrilineal shared
ancestor, depth≤3) — e.g. does an occupational-status difference between a
father and son predict a difference in `elected`, holding the family fixed.

**Coverage**: 115 dynasty groups (247 candidates total), all with ≥2
members (median group size 2, max 5). **64 of the 115 groups span both the
district and PR eras** — i.e. the same family has a member on each side of
the 1917 reform, which is the single most useful subset for testing whether
the *within-family* dynastic advantage changed after 1917. Only 31/115
groups have within-group variation in the `elected` outcome at all (the
rest are all-elected or all-not-elected families, which a fixed-effects
model cannot use).

This is the **smallest-N design of the three** (115 groups, 31 with
outcome variation) and is patrilineal-only by construction (maternal-line
and marriage-based dynastic ties are invisible to it, a documented Phase 3
limitation). Best used as a **corroborating** design alongside A, not a
standalone primary result — e.g. to check that Design A's cross-sectional
RD finding is not solely a compositional artifact of which families show up
in the roster pre- vs. post-1917.

### Design C: descriptive DiD around 1917 (status × post-1917 interaction)

Full-panel regression of `elected` on dynasty/occupational status,
`post_1917`, and their interaction, with suffrage-wave controls
(1887/1896/1917/1922) and district/kieskring fixed effects.

**Coverage is the whole panel** (8,506 district-era + 27,109 PR-era
candidacy-rows) — by far the largest-N design — but status-covariate
coverage is uneven and this design has **no exogenous source of variation
in status itself**: dynastic/high-status candidates are not randomly
assigned to district vs. PR elections, so any status×era interaction is
descriptive of *selection patterns*, not causal evidence that the reform
*caused* a change in the return to status. Own/father-HISCLASS coverage is
reasonably balanced across eras (14.9%/15.6% district vs. 11.8%/11.8% PR)
but dynasty-membership coverage is not (7.3% district vs. 3.1% PR — expect
this, since later-observed descendants are mechanically less likely to have
been found yet for the more recent PR-era candidates; a covariate-coverage
gap, not an outcome-coverage gap, since `elected` itself is ~100% populated
throughout).

A raw (non-causal) cross-tab illustrates the pattern this design would
formalize: `elected` rate is 32.9%→62.5% (non-dynasty→dynasty) pre-1918 and
6.4%→36.0% post-1917 — the dynastic gap in the raw elected rate does not
shrink after 1917 despite the reform (list-based nomination could in
principle have diluted personal/family reputation). This is consistent
with, but far weaker evidence than, a genuine causal test — list position
is itself plausibly a mechanism (parties may place dynastic candidates
higher), which Design C's regression cannot separate from a direct
voter-side dynastic-preference effect without additional instruments.

**Use Design C as the framing/motivation regression** (largest N, sets up
the puzzle) but do not present its status×post-1917 coefficient as causal.

## 4. Recommendation

1. **Design A (pre-1918 close-election RD)** as the primary causal estimate
   — it is the design with a genuine source of as-good-as-random variation,
   matches the dominant identification strategy in the literature, and its
   small-N ceiling (≤5pp bandwidth: 297 candidates with outcome data) should
   be stated up front as a power limitation, not discovered after the fact.
2. **Design B (family fixed effects)**, restricted to the 64 dynasty groups
   spanning both eras, as a corroborating check on whether A's estimate
   looks different pre- vs. post-1917 within the same families.
3. **Design C (full-panel descriptive DiD)** as the motivating/framing
   regression only — report it, but do not lean on its interaction term for
   causal claims about the reform's effect on the return to status.

None of these designs can currently use a real post-1917 party label (no
party name in `candidates_panel`) — if that gap is worth closing before
estimation, it would need a lijst_no→party crosswalk (not attempted here;
AIEEDA's municipal-party panel does not carry candidate-level list numbers
and cannot supply this directly).
