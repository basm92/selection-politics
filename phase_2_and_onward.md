# Phases 2–5 — Entity Resolution, Status, Wealth, Empirical Strategy

**Precondition:** `post_1917_candidates.md` completed — `candidates_panel`
spans 1848–1937 with post-1917 rows carrying kieskring, list position and
**residence**. Do not start Phase 2 before that checkpoint is accepted.

Work phase by phase; each phase below ends with a CHECKPOINT (stop, report
match rates/coverage, wait). House style: `examples/` +
`code/data_wrangling/README.md` (numbered steps, Input/Output/Method headers,
token-bucket rate limiting, resumable DuckDB, parquet exports).

Assets already built (Phase 0/1, verified 2026-07-07):
- `data/panel/*.parquet` — candidates, elections, persons, AIEEDA municipal
  panel, municipality crosswalk (+ transitions).
- `data/nlgis/crosswalk.duckdb` — municipality_years (year×amco×name),
  transitions (mergers/splits resolved geometrically).
- Endpoints/quirks: see memory notes `phase0-source-endpoints` and
  `phase1-pipeline-state`, and `phase0_feasibility_report.md`.

---

## Phase 2a — PDC/Parlement.com anchor for elected MPs (new, do first)

Phase 0 flagged an asymmetry: Huygens has no birth data, so losers are harder
to link than winners. Fix what can be fixed cheaply:

- Scrape the condensed public PDC biographies on parlement.com for every
  Tweede Kamer member 1848–1940 (~5,000 bios sitewide; TK subset smaller):
  birth date/place, death date/place, party, offices. Be polite (their server
  is small); consider emailing PDC for a bulk extract in parallel — they
  accommodate academic requests.
- Link to `candidates_panel` via the elected rows (name+titles+year window;
  post-1917 also residence). Deliverable: `mp_anchor` table
  (persoon_id/post-1917 person key → birth date, birth place) + match rate.
- CHECKPOINT: report MP-anchor coverage (expect >90% of elected persons).

## Phase 2b — Candidate → genealogical person linkage

For each candidate (winners AND losers), query OpenArchieven and
GenealogieOnline using `examples/openarch` / `examples/genealogie` as method
templates (endpoints verified live in Phase 0; GenealogieOnline search is
`/zoeken/index.php?q=<surname>&vn=<firstname>&gv=&gt=` — the bare `/zoeken/`
returns an empty shell).

- **Confidence-scored pairs, not filtered matches**: emit
  `candidate_person_pairs(candidate_key, source, person_ref, score, feature_*)`
  keeping every plausible pair with its features:
  - name similarity (surname + initials; expand initials against full first
    names where PDC gives them);
  - birth-year agreement where an anchor exists (MPs via 2a; else a
    plausibility window from candidacy years: roughly age 30–75 at candidacy
    (the Grondwet set passive suffrage at 30 for this era — verify the exact
    rule per period before hard-coding the window);
  - geography: candidate district (pre-1918) or residence (post-1917) vs.
    record's event/birth place, **normalized through the NLGIS crosswalk**
    (`municipality_years` matches historical name variants to amco;
    `transitions` resolves merged municipalities);
  - record-type priors (a birth record ~b.1840s fits a candidate active
    1880s; marriage records give profession — capture `beroep` strings).
- Score calibration: hand-label ~100 candidate–pair samples across strata
  (famous MP / obscure loser / common surname) and fit or tune the weights;
  keep the labelled set in the repo for auditability.
- CHECKPOINT: distribution of top-pair scores, share of candidates with ≥1
  pair above a provisional threshold, winner-vs-loser match-rate gap (this
  gap is a core data limitation to report honestly in the paper).

## Phase 3 — Occupational & dynastic status

- Match `beroep` strings (from genealogie persons + openarch marriage
  records) to `examples/hisco/hsn2013a_hisco_comma.csv` → HISCO, then
  HISCLASS/HISCAM. Reuse the matching approach from
  `examples/genealogie/ind_step08_surname_status_panel.py` where applicable.
- Extend to linked relatives via `person_children` lineage edges (fathers,
  sons) from the genealogie pipeline output.
- Dynasty membership: verified lineage between candidates across time
  (shared ancestor within k generations), NOT shared surname. Deliverable:
  per-candidate dynasty indicators (prior relative candidate/MP, later
  relative candidate/MP) with the lineage evidence chain attached.
- The `titles` field in `candidates_panel` (mr./dr./jhr./baron) is an
  independent status proxy — tabulate it alongside HISCLASS.
- CHECKPOINT.

## Phase 4 — Dynasty construction & empirical strategy

**Wealth (formerly Phase 4) skipped by explicit decision, not oversight**:
HIP-NL only covers Utrecht city 1909 (18,339 tax observations) — too narrow
a base for a national panel — and the substitute proxies considered
(Memories van Successie probate indexes, Eerste Kamer-verkiesbaren lists)
were never pursued. The paper proceeds on occupational (HISCLASS/HISCAM)
and dynastic status only; state this limitation explicitly rather than
retrofitting a proxy later.

- Construct per-candidate measures: dynastic status, occupational status
  (own + father's HISCLASS/HISCAM), plus electoral environment (margin to
  kiesdrempel, runoff presence, district competitiveness pre-1918; list
  position and party strength post-1917).
- Literature scan before committing (Dal Bó–Dal Bó–Snyder; Querubin; Rossi;
  Van Coppenolle; Fiva–Smith on Norwegian/Japanese dynasties; Berlinski et
  al. on suffrage reforms): the Dutch value-add is the **1917 district→PR
  switch plus staged suffrage extensions (1887, 1896, 1917 universal male,
  1922 female)** observed within one linked candidate–genealogy panel.
- Propose 2–3 designs against the data actually assembled, e.g.:
  1. DiD around 1917: dynastic/high-status candidates' selection and success
     before vs. after the district system died (identification from the
     reform killing personal-vote districts);
  2. family fixed effects on the linked lineage panel (brother/father-son
     contrasts in political entry);
  3. close-contest discontinuities pre-1918 (margin around kiesdrempel /
     runoff winners vs. narrow losers) for the causal effect of office on
     descendants' entry (Van Coppenolle-style).
- Deliverable: a short design memo ranking the strategies by the
  identification the data supports, with power/coverage numbers from the
  actual panel. FINAL CHECKPOINT before any estimation.

**DONE (2026-07-09)**: electoral-environment measures built by
`code/data_wrangling/panel/panel_step6_electoral_environment.py` →
`data/panel/electoral_environment.parquet` (per-candidacy grain: district-era
race margins/runoff structure/RD closeness; PR-era list position/length and
a list-vote-share party-strength proxy, since no party name exists for any
PR-era candidate). Literature scan and the ranked design memo are at
`phase4_design_memo.md` (repo root). Verdict: (A, primary) pre-1918
close-election RD on `later_relative_any/elected`, small-N (297 candidates
with outcome data at ≤5pp bandwidth) — state this as a power ceiling; (B,
corroborating) family fixed effects on the 64 dynasty groups spanning both
eras; (C, motivating/framing only, NOT causal) full-panel descriptive DiD
around 1917. This is the Phase 4 CHECKPOINT and the FINAL CHECKPOINT before
any estimation — estimation itself has not started.

---

**Overarching question** (sharpen, don't force): does status (occupational,
dynastic) increase the likelihood of selection into politics and of winning,
and how did the 1917 institutional rupture change who selects in? (Wealth
dropped from this question by explicit decision -- see Phase 4 note above.)
Post-1917 losers now carry residences and list positions; pre-1918 losers
carry district vote shares — design measures that respect that asymmetry
rather than papering over it.
