# Phase 0 — Feasibility & Data Scoping Audit

Date: 2026-07-07. All findings verified against live endpoints (probe evidence
noted per source). Project: Politicians' Status and Entry into Politics,
Dutch Lower House 1848–1940.

## Summary table

| Source | Years | Granularity | Access method | Known gaps | Verdict |
|---|---|---|---|---|---|
| Huygens Verkiezingen Tweede Kamer | 1848–1918 | candidate × district-election | CSV endpoint (election metadata) + HTML scrape (~2,870 pages, candidates) | no explicit "elected" flag (derivable); no birth data | **GO** |
| AIEEDA (OSF, `qs3dg`) | 1922–1937 (5 general elections) | municipality × party votes | single 34 MB zip download, ready-made CSV | no 1918; no candidates; no by-elections | **GO** (party-level panel) |
| Kiesraad verkiezingsuitslagen.nl | 1922–1937 (1918 "not yet available") | municipality × party | embedded JSON in HTML per election | documented lacunae (esp. 1922, no CBS statistics that year); no candidates | GO as cross-check only |
| CBS Statistiek der Verkiezingen (historisch.cbs.nl) | 1918, 1925, 1929, 1933, 1937 | candidate × kieskring | scanned page images (JPEG) → OCR/manual transcription | **1922 never published by CBS**; transcription effort | **GO, but costly** (only candidate-level source post-1917) |
| Parlement en Kiezer yearbooks (Delpher) | 1918–1929 | candidate lists, results | scanned/OCR'd volumes, manual extraction | OCR quality; volume-by-volume hunting | GO as gap-filler (esp. 1922) |
| Parlement.com / PDC | 1796–now | elected MPs only, biographies (birth date/place) | HTML scrape, condensed public bios; PDC accepts academic data requests | losing candidates absent | **GO** (linkage anchor for MPs) |
| HIP-NL (druid.datalegend.net) | **Utrecht city, 1909 only** (transcribed) | person-level tax class + income-bracket midpoint | SPARQL (Triply "speedy" endpoint), no auth | pilot stage: 18,339 observations, one city, one year | **LIMITED GO** — not a general income source |
| NLGIS maps API | 1848–1940, per year | municipality polygons + amco codes | `GET nlgis.nl/api/maps?year=YYYY`, TopoJSON | `province` param unreliable (empty response); use year-only | **GO** |
| OpenArchieven API | 19th–early 20th c. civil records | person records (BS certificates) | existing `examples/openarch` pipeline works as-is | needs disambiguation (by design, Phase 2 scoring) | **GO** |
| GenealogieOnline | family trees, 1500–1900+ | person pages with beroep, lineage | `/zoeken/index.php?q=<surname>&vn=<firstname>&gv=&gt=` | user-contributed, variable quality | **GO** |

## Per-source findings

### 1. Huygens "Verkiezingen Tweede Kamer 1848–1918"
Live at `https://resources.huygens.knaw.nl/verkiezingentweedekamer` (the
`verkiezingentweedekamer.huygens.knaw.nl` host in the project brief does not
resolve).

- **Election-level metadata is directly downloadable as CSV**:
  `databank/chronologisch/download.csv?beginjaar=&eindjaar=&type=` returns
  District, Dag, Maand, Jaar, Type, Electoraat, Opkomst, Stembriefjes,
  Geldig, Blanco. Row counts by type over 1848–1918: algemeen 1,113;
  herstemming 604; periodiek 569; tussentijds 461; algemeen/enkelvoudig 90;
  tussentijds/enkelvoudig 32; vacature 2; naverkiezing 0 → **≈2,870
  district-election events** total.
- **Candidate-level data requires HTML scraping** of
  `databank/uitslag_per_verkiezing?uitslag_ID=N` (one page per event; IDs
  enumerable from the chronological listing). Per candidate: name incl.
  titles (mr./dr. — usable as an education/status proxy), affiliation
  ("Aanbevolen door", e.g. Lib, AR, Ka), votes, vote share, and a **stable
  `persoon_ID`** linking to `uitslag_per_persoon` (full per-person candidacy
  history). Page also carries seats and the Kiesdrempel (majority threshold).
- **No explicit elected flag.** Derivable: in `algemeen`/`periodiek` rounds a
  candidate exceeding the Kiesdrempel is elected; otherwise the runoff
  (`herstemming`) winner is. Validate against PDC MP lists in Phase 1.
- No birth dates/places — Phase 2 linkage must work from name + district
  geography + candidacy years.
- Volume estimate: ~2,870 pages × ~4–6 candidates ≈ **10–15k candidate×election
  rows**, a trivial scrape volume at polite rates.

### 2. Post-1917 sources (cold search + user-supplied lead)
- **AIEEDA** (Archive of Interwar Europe Election Data & Assemblies,
  OSF DOI 10.17605/OSF.IO/QS3DG, public, launched 2025): verified by
  downloading the archive. `data/NL/AIEEDA-Netherlands-subnat-v1.csv` holds
  **76,546 rows = municipality × party votes** for the general elections of
  1922-07-05, 1925-07-01, 1929-07-03, 1933-04-26, 1937-05-26; 1,103 distinct
  municipalities; 122 party labels; plus period shapefiles (1922–1937) and
  national-level elections/parties/cabinets tables. **This removes most of the
  post-1917 build risk** for geography-linked party outcomes.
- **Kiesraad databank** (verkiezingsuitslagen.nl): per-gemeente party results
  embedded as JSON in election detail pages for 1922–1937. Own caveat printed
  on the 1922 page: CBS published no statistics in 1922, results were
  reassembled from municipal/provincial archives, "er zijn veel lacunes".
  TK1918 page: "uitslaggegevens (nog) niet beschikbaar". Use to cross-validate
  AIEEDA, not as primary.
- **Candidate-level post-1917** (the actual research need): only in print.
  CBS *Statistiek der Verkiezingen* volumes for **1918, 1925, 1929, 1933,
  1937** at historisch.cbs.nl — scanned page images, no structured data →
  OCR/manual transcription per kieskring (18 kieskringen × ~5 volumes;
  bounded but real effort). **1922 was never published by CBS**; fill from
  *Parlement en Kiezer* yearbooks (Delpher) and/or Staatscourant candidate
  lists. Preference votes per candidate per kieskring exist in these volumes.
- **PDC / Parlement.com**: condensed public biographies of every MP since
  1796 (5,000+ persons) incl. birth date/place, offices, party — scrapeable,
  and PDC entertains academic bulk requests. Covers **elected members only**;
  losing candidates post-1917 exist nowhere in structured form.
- **DNPP**: party-history documentation; no structured interwar candidate
  database found. Deprioritize.

### 3. HIP-NL (druid.datalegend.net/HIP-NL/HIP-NL)
Queryable without auth via SPARQL:
`https://api.druid.datalegend.net/datasets/HIP-NL/HIP-NL/sparql` (Triply
"speedy"; the dataset has no dedicated running service). 655,291 triples in 5
graphs (`taxes`, `taxSources`, `populationRegister`,
`populationRegisterSources`, `reconstruction`).

- `taxes`: 18,339 PersonObservations with familyName, initials, address,
  tax class, tax paid, **taxBracketMid** (income-bracket midpoint), wijk —
  name-identifiable heads of households.
- **All 18,339 transcribed tax observations trace to Utrecht city, 1909.**
  The `taxSources` inventory lists registers for many other municipalities
  (Graft, Baexem, Harlingen, Edam, Zierikzee, …, 1859–1920) but these are
  catalogued, not transcribed. HIP-NL is a 2025-started pilot.
- `reconstruction`: ~19,322 reconstructed persons with birthDate/givenName —
  linkage of tax records to population-register persons exists for the pilot.
- **Implication for Phase 4**: HIP-NL supports at most a *Utrecht-1909 case
  study* of candidate income, not systematic wealth enrichment. Treat as
  limitation; alternative wealth proxies to consider: Memories van Successie
  (probate) indexes via OpenArchieven, published lists of highest-taxed
  citizens (verkiesbaren Eerste Kamer), HISCAM as primary status measure.

### 4. NLGIS
`https://nlgis.nl/api/maps?year=YYYY` returns full-country TopoJSON with
`amsterdamcode` (amco), `cbscode`, and name per municipality. Verified for
1848/1860/1880/1900/1920/1940: municipality counts 1,209 → 1,138 → 1,127 →
1,121 → 1,110 → 1,054, consistent with known merger history; distinct amco
per feature; geometry ids change across years (boundary versions tracked).
The `year+province` combination returns an empty body — query by year only
and filter client-side. Sufficient for the Phase 1 municipality-identity
crosswalk. `api/data` endpoint exists but returned empty on probe; not needed.

### 5. OpenArchieven / GenealogieOnline spot-check
Six known politicians (Van Houten, Schaepman, Kuyper, Troelstra, Colijn,
De Savornin Lohman / De Beaufort):

- **OpenArchieven** full-name search: 41–256 records each (all six ≥41).
  Existing `examples/openarch` async pipeline works unchanged against the
  live 1.1 API.
- **GenealogieOnline** via `/zoeken/index.php` with surname (`q`), first name
  (`vn`) and a ±1-year birth window (`gv`/`gt`): 2–13 person-page hits each,
  all six non-empty. Note the working endpoint is `index.php`, not the bare
  `/zoeken/` used with `pn=` place search in the legacy crawler.
- Expected linkage regime: high recall, moderate precision → matches the
  Phase 2 plan of confidence-scored (not filtered) matches. Politicians'
  distinctive compound surnames should help; common-surname candidates
  (De Vries, Jansen) will carry low scores — accept and document.

## Go/no-go recommendation

- **Phase 1 core panel: GO.** 1848–1917 candidate×district from Huygens
  (scrape, small volume). Post-1917: municipal party-level panel from AIEEDA
  (download, done), cross-checked against Kiesraad JSON; candidate-level
  post-1917 from CBS scans + Parlement en Kiezer transcription — schedule as
  a distinct sub-step with its own effort budget, and treat 1922
  candidate-level as best-effort.
- **Phase 2 linkage: GO** with existing pipeline templates.
- **Phase 3 HISCO/HISCLASS: GO** (codebook present in `examples/hisco`).
- **Phase 4 income: SCOPE CHANGE.** HIP-NL ≠ national income source; plan a
  Utrecht-1909 case study plus probate/tax-list proxies, and say so in the
  paper's limitations.
- **Phase 5**: the 1917 district→PR + suffrage-extension variation survives
  the audit — pre/post candidate-level comparability is the binding
  constraint (post-1917 candidate data is the most expensive item).

## Scope changes flagged for later phases

1. **Post-1917 candidate names are print-locked.** AIEEDA/Kiesraad give party
   × municipality only. The candidate×election panel after 1917 depends on
   transcribing CBS/yearbook scans; the 1918 and 1922 elections are the
   weakest links (1918 absent from Kiesraad databank; 1922 absent from CBS).
2. **"Elected" must be derived pre-1918** (threshold/runoff logic) and
   validated against PDC.
3. **HIP-NL demoted** from general income source to single-city pilot.
4. **Huygens has no birth data** — Phase 2 match scoring cannot use birth-year
   agreement for candidates unless first anchored via PDC (elected MPs) or
   genealogical triangulation; expect asymmetric match quality between
   winners (PDC-anchored) and losers. This asymmetry matters for any
   winner/loser comparison design and must be handled explicitly.
