# Agent memory (repo-local)

Operational notes for AI agents (and humans) working on this project:
verified endpoints, scraper quirks, build state, and in-flight decisions
that are not derivable from the code or git history. Canonical location —
update these files when facts change, and check them before re-scraping or
re-verifying sources.

- [phase0-source-endpoints.md](phase0-source-endpoints.md) — verified
  URLs/coverage for Huygens, AIEEDA, HIP-NL, NLGIS, GenealogieOnline
  (July 2026 audit)
- [phase1-pipeline-state.md](phase1-pipeline-state.md) — Phase 1 build
  state, scraper quirks (pagination bug, CBS handlers, SRU patterns),
  post-1917 decisions
- [post1917-transcription-state.md](post1917-transcription-state.md) —
  Staatscourant OCR pipeline (delpher steps 3–6), Gemini run decisions,
  parsing quirks, current step-6 status
- [phase2a-pdc-mp-anchor.md](phase2a-pdc-mp-anchor.md) — PDC/parlement.com
  biography scrape, bio-page structure, name-matching quirks (y/ij
  variants, digraph initials, noble-word stripping), mp_anchor match rate
- [phase2b-candidate-linkage.md](phase2b-candidate-linkage.md) — OpenArchieven
  + GenealogieOnline candidate linkage, API quirks, the surname-search bug
  (search must use the raw surname, not the scoring-normalised form —
  silently zeroed ~40% of candidates), hand-labelled score calibration
- [phase3-occupational-dynastic-status.md](phase3-occupational-dynastic-status.md)
  — detail-page fetch (openarch show.json, genealogieonline person pages +
  3-generation ancestor chain), HISCO/HISCLASS matching, dynasty detection
  via shared ancestors, the cross-era vs. within-era same-person finding
- [phase4-electoral-environment-and-design.md](phase4-electoral-environment-and-design.md)
  — per-candidacy electoral environment (district-era RD closeness/runoff
  structure, PR-era list position/party-strength proxy), literature scan,
  and the ranked empirical-design memo (`phase4_design_memo.md` at repo
  root); the `later_relative_any` fillna(False) coverage gotcha

Step 6 (LLM vote-table parse) is complete; its handoff note is archived at
`archived/step6_llm_parsing_plan.md`. Panel step 2 merged the post-1917 rows
into `candidates_panel` (1848–1937). Phase 2a (mp_anchor) is done at ~89%
match rate. Phase 2b (candidate_person_pairs) is done — 95% of candidates
have >=1 candidate-person pair, winner/loser gap (72.9% vs 43.7% at
score>=0.7) matches the expected direction. Phase 3 (candidate_status) is
done — own/father HISCLASS + dynasty indicators for the score>=0.7 subset
of candidates. Briefly widened to 0.5, then reverted after a hand-labelled
spot-check measured only ~30% precision in the 0.5-0.7 band (see phase3
memory + phase3_spotcheck_0.5_0.7_band.csv). Phase 4 (dynasty construction &
empirical strategy) is done -- electoral_environment.parquet built, a
literature scan run, and a ranked design memo written (see phase4 memory
above); the prior Phase 4 (wealth/HIP-NL) was dropped by explicit decision,
see `phase_2_and_onward.md`. This was the FINAL CHECKPOINT before
estimation -- estimation itself has not started.
