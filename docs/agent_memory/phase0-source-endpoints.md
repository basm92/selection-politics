# Phase 0 source endpoints

Verified data-source endpoints and coverage facts from the Phase 0 audit
(2026-07-07); full report in `archived/phase0_feasibility_report.md`.

- Huygens election db: `https://resources.huygens.knaw.nl/verkiezingentweedekamer`
  (NOT verkiezingentweedekamer.huygens.knaw.nl — doesn't resolve). CSV:
  `databank/chronologisch/download.csv?beginjaar=&eindjaar=&type=`; candidates via
  `databank/uitslag_per_verkiezing?uitslag_ID=N` (~2,870 events, stable persoon_ID).
  No elected flag (derive via kiesdrempel/runoff), no birth data.
- AIEEDA: OSF `qs3dg`, zip download link `https://osf.io/download/wghua/`;
  NL file = municipality×party votes 1922/1925/1929/1933/1937, 76,546 rows.
  No 1918, no candidates.
- Post-1917 candidate-level data only in scans: CBS Statistiek der Verkiezingen
  (historisch.cbs.nl; 1918,1925,1929,1933,1937 — 1922 never published) and
  Parlement en Kiezer yearbooks on Delpher.
- HIP-NL SPARQL: `https://api.druid.datalegend.net/datasets/HIP-NL/HIP-NL/sparql`
  (no auth). Transcribed tax data = Utrecht 1909 only, 18,339 obs → demoted
  to case study.
- NLGIS: `https://nlgis.nl/api/maps?year=YYYY` works 1848–1940 (year-only;
  the province param returns empty).
- GenealogieOnline name search endpoint is
  `/zoeken/index.php?q=<surname>&vn=<first>&gv=&gt=` (bare `/zoeken/`
  returns an empty shell).

User confirmed AIEEDA as an accepted source mid-audit (interrupted a web
search to supply it). Checkpoint discipline: stop after each CHECKPOINT
phase per prompt.md.
