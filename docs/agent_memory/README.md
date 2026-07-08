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

See also `step6_llm_parsing_plan.md` at the repo root for the current
mid-task handoff (step 6 LLM-parse plan + remaining TODOs).
