# Phase 2a: PDC/parlement.com MP anchor

Built 2026-07-08 (spec: `phase_2_and_onward.md`, Phase 2a). Pipeline:
`code/data_wrangling/pdc/pdc_step1_survey_sitemap.py` /
`pdc_step2_scrape_biographies.py` / `pdc_step3_build_mp_anchor.py`.

- **No dedicated TK-member index page on parlement.com.** A guessed URL
  (`/id/vh8lnhrouwvt/tweede_kamerleden_1848_heden`) 404s. Biographies live
  at flat `/biografie/<slug>` URLs, discoverable only via `sitemap.xml`
  (paged `?page=1..8`, ~5,849 biografie URLs found out of ~16,000 total
  sitemap URLs). Strategy: scrape every bio, then filter to people with a
  Tweede Kamer membership span overlapping 1848-1940 (914 of 5,849).
- **Bio page structure** (server-rendered Drupal, no JS needed): `h2`
  section headers (`Personalia`, `Partij/stroming`, `Hoofdfuncties/beroepen`,
  `Nevenfuncties`, ...), `h3.biohdr` sub-labels, value is either a single
  `p.bioitem` (scalar field) or `ul.biolist` (dated career entries: `"<role>,
  van <date> tot <date> (voor <district>)"`). Free page is a "selectie" —
  header shows e.g. "Hoofdfuncties/beroepen (17/20)" meaning some minor
  entries are hidden behind a paywall; Personalia (birth/death) is a
  separate, complete section unaffected by this truncation.
- **Rare edge case**: some TK memberships (esp. brief ceremonial
  reappointments around a ministerial swap, e.g. Beelaerts van Blokland
  1929/1933) show up only as free-text `Wetenswaardigheden` trivia, not a
  structured `lid Tweede Kamer der Staten-Generaal, van X tot Y` entry —
  step 3's regex won't catch these, so those persons are missing entirely
  from `mp_candidates` even though their PDC bio exists.
- **Name-matching quirks handled in step 3**: PDC's `titulatuur en naam`
  field sometimes redundantly appends the full `voornamen` after the
  surname (strip using the known `voornamen` value, not just whitespace);
  initials use single letters normally but digraph name sounds get 2-letter
  abbreviations (`Th.` Theodoor, `Ch.` Christiaan) — the initials regex must
  allow an optional lowercase second letter per unit; noble rank words
  (`jhr`, `ridder`, `baron`, `graaf`, ...) are inconsistently included
  between PDC and Huygens and are stripped before comparison; historical
  Dutch spelling varies `y`/`ij` (`Ravesteijn`/`Ravesteyn`, `Duijs`/`Duys`)
  — folded to `y` before comparison.
- **Known unfixed gap**: some Huygens `name_clean` values (district era,
  `candidates_panel`) carry a pre-existing mojibake encoding bug from Phase 1
  (`Ã` where `Æ`/diacritics should be, e.g. "Ã. baron van Mackay" for
  "Æ. baron van Mackay"/Æneas Mackay) — affects ~5 of the unmatched persons,
  not fixed here (would need re-decoding the original Huygens scrape).
- **Result**: `data/panel/mp_anchor.parquet`, 821/921 elected persons matched
  (~89%; 586/655 district era, 235/266 PR era). Tiers: 716 exact
  surname+initials, 67 same-surname+first-initial, 38 Levenshtein-fuzzy.
  `mp_anchor_unmatched.parquet` holds the 100 remaining for Phase 2b review.
- Rate limit used: 2 req/s (parlement.com is a small nonprofit server per
  `phase_2_and_onward.md`'s note about emailing PDC for a bulk extract).
