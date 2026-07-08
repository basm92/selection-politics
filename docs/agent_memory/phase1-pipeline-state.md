# Phase 1 pipeline state

Phase 1 built 2026-07-07 (see `code/data_wrangling/README.md` for run
order). Related: [phase0-source-endpoints.md](phase0-source-endpoints.md).

Key facts not obvious from the code:

- Huygens listing pagination: pages hold 25 rows but the site's own
  "volgende" link steps by 26 — their bug silently drops one event per
  page. We step 25.
- Huygens uitslag pages: election header labels are in `<th>`; candidate
  rows live in an *unlabeled* table after the "Uitslagen per deelnemer"
  banner table (header row `# | Naam | ...`).
- Elected flag is derived (runoff top-N; enkelvoudig unopposed; else
  votes >= kiesdrempel capped at zetels) — validated on Troelstra (1893
  loss, 1897 triple runoff win) and Thorbecke (Leiden 1848).
- CBS historisch.cbs.nl: full-size scan = `HttpHandler/<file>.jpg?file=<media_id>`
  (the `?icoon=` param gives a 142px thumbnail; other param names return
  empty). detail.php needs `nav_id=0-1&index=3` params or the first/last
  navigation is omitted; "Eerste resultaat" link absent when viewing the
  first page itself. CBS server search facets time out easily; listing
  needs session cookies.
- CBS has NO Tweede Kamer statistics volumes for 1918/1922/1925/1929 (only
  1901-1913, 1933, 1937, post-war) — Staatscourant PDFs from Delpher are
  the primary candidate-level source for 1918-1929.
- Delpher SRU: jsru.kb.nl works unauthenticated; Staatscourant = papertitle
  exact "Nederlandsche staatscourant" in DDD_artikel. Old spelling matters:
  "candidaten" (c) pre-war. Issue PDF via resolver.kb.nl ?urn=<issue>:pdf.
  User decision: archive whole-issue PDFs even with bad OCR; re-OCR later.
- User supplied AIEEDA mid-search and prefers Delpher searched extensively.
- 14 items of the CBS scan index (listing page 187, a post-war volume)
  still missing due to persistent server timeout — harmless for scope.

Phase 1 COMPLETE (2026-07-07): candidates_panel 8,506 rows/1,868 persons
(1848-1918, elected derived); AIEEDA 76,546 municipal×party rows
(1922-1937); crosswalk 104,457 muni-years; CBS 632 scans (1901-1913, 1933,
1937); Delpher 219 Staatscourant issue PDFs (~1.3 GB, all 6 interwar
elections, incl. proces-verbaal supplements). Parquet exports in
data/panel/. Next (Phase 2): entity resolution; post-1917 candidate
transcription from scans → see
[post1917-transcription-state.md](post1917-transcription-state.md).
