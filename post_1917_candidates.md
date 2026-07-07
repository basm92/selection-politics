# Post-1917 Candidate Rows — Transcription from Staatscourant PDFs

## Goal

Extend `candidates_panel` (data/panel/panel.duckdb, currently 1848–1918 only)
with candidate × election rows for the six interwar Tweede Kamer general
elections (1918, 1922, 1925, 1929, 1933, 1937), from the primary sources
already archived on disk in Phase 1. Work phase by phase; **CHECKPOINT at the
end** (stop, report counts and OCR quality, wait) before Phase 2 begins.

Follow the house style in `examples/` and `code/data_wrangling/` (numbered
step scripts with Input/Output/Method headers, resumable DuckDB progress,
parquet exports). `code/data_wrangling/README.md` documents what exists.

## What is on disk (Phase 1, verified 2026-07-07)

- `data/delpher/delpher.duckdb` — table `articles`: 298 Staatscourant articles
  (title, date, page, `page_urn`, `issue_urn`, rough `ocr_text`); table
  `issue_pdfs`: 219 complete issue PDFs with local paths.
- `data/delpher/staatscourant/<year>/<urn>.pdf` — the issue scans (~1.3 GB).
  Filenames are the issue URN with `:` → `_`.
- `data/cbs/scans/{1933,1937}/` — CBS "Statistisch overzicht verkiezingen"
  page scans. **These contain NO candidate-level tables** (municipality ×
  party only — verified via the page-level RDF snippets); use them to
  cross-validate AIEEDA vote totals, not for candidates.
- `data/cbs/scans/{1901,1905,1909,1913}/` — pre-PR "Statistiek der
  verkiezingen" volumes (district era); optional validation set for the
  Huygens-derived panel, not needed for this task.

## Where the candidates are (verified locations)

Every election has the same two-document structure in the Staatscourant.
OCR lengths below are from the `articles` table — use it to re-locate these
via `SELECT * FROM articles WHERE issue_urn = '...'`.

### A. Official candidate lists (ALL candidates, incl. losers)

Article "Verkiezing van de leden van de(r) Tweede Kamer der Staten-Generaal"
(the Centraal Stembureau chairman publishes the validated lists per
kieskring, per art. 51 Kieswet). Format per entry: list number within
kieskring, position on list, surname, initials (+ occasional mr./dr. title),
**place of residence** — keep residence, it is linkage gold for Phase 2.

| election | publication date | issue_urn | ocr chars |
|---|---|---|---|
| 1918-07-03 | 1918-06-13 | MMKB08:000179786:mpeg21 | 107,505 |
| 1922-07-05 | 1922-06-20 | MMKB08:000179055:mpeg21 | 167,762 |
| 1925-07-01 | 1925-06-11 | MMKB08:000180972:mpeg21 | 182,718 |
| 1929-07-03 | 1929-06-13 | MMKB08:000182756:mpeg21 | 146,801 |
| 1933-04-26 | 1933-04-05 | MMKB08:000181136:mpeg21 | 197,067 |
| 1937-05-26 | 1937-05-11 | MMKB08:000168898:mpeg21 | 172,620 |

### B. Official results (votes, seat allocation, elected candidates)

Published together in one issue per election:
"PROCES-VERBAAL van de zitting van het Centraal Stembureau" (stemcijfers per
lijst per kieskring; preference votes per candidate), "BESLUIT van het
Centraal Stembureau, bedoeld in artikel 97 der Kieswet" (106–138k chars),
"Verdeeling van de aan de lijstengroepen toegekende plaatsen",
"Vaststelling van den uitslag" (elected members with residence), and
"Aanwijzing van de candidaten, gekozen op de niet van een groep deel
uitmakende lijsten".

| election | results date | issue_urn |
|---|---|---|
| 1918 | 1918-07-15 | MMKB08:000179144:mpeg21 (95-page PDF: bijvoegsel bundled) |
| 1922 | 1922-07-19 | MMKB08:000178343:mpeg21 |
| 1925 | 1925-07-20 | MMKB08:000181037:mpeg21 |
| 1929 | 1929-07-13 | MMKB08:000161457:mpeg21 (+ MMKB08:000161498, 1929-07-30) |
| 1933 | 1933-05-06 | MMKB08:000181270:mpeg21 |
| 1937 | 1937-06-05 | MMKB08:000168915:mpeg21 (+ MMKB08:000168911, 1937-05-31) |

To find the right pages inside an issue PDF: `articles.page_urn` ends in
`pNNN` (page NNN of the issue); PDF page order follows issue page order.

## Suggested pipeline

1. **Locate & re-OCR.** For each key article: map its pages in the local
   issue PDF, render at ≥300 dpi (`pdftoppm`), and run a modern OCR pass
   (the Delpher OCR in `articles.ocr_text` is orientation-grade: broken
   diacritics, split initials — usable as a cross-check corpus, not as the
   primary transcription). The documents are typeset in narrow newspaper
   columns; column-aware OCR or LLM-assisted table reading will outperform
   plain Tesseract defaults. Archive raw OCR output per page (resumable).
2. **Parse candidate lists (A)** into `kandidatenlijsten(year, kieskring_no,
   kieskring_name, lijst_no, positie, name_raw, initials, titles, residence)`.
   Kieskringen are numbered 1–18 and named (e.g. "Kieskring 1
   ('s Hertogenbosch)"). Lists repeat across kieskringen — the same person
   often stands in several kieskringen; keep rows per kieskring AND build a
   deduplicated per-election person view (match on name+initials+residence).
3. **Parse results (B)** into `lijst_uitslagen(year, kieskring_no, lijst_no,
   party_label, stemcijfer)` and `gekozen(year, name_raw, residence,
   lijst_no/kieskring)`; extract preference votes per candidate where the
   proces-verbaal reports them.
4. **Assemble** candidate×election rows: candidacy (list position, kieskring
   coverage) + list votes + elected flag; provenance =
   `'staatscourant_<issue_urn>'`. Extend `panel_step1_assemble.py` (or add a
   `panel_step2_merge_post1917.py`) so `candidates_panel` spans 1848–1937;
   re-export parquet; report candidates per decade.
5. **Validate** before the checkpoint:
   - elected sets vs. parlement.com/PDC MP lists per election (exact-name
     match rate ≥ ~95% expected; investigate the remainder);
   - party stemcijfers summed over kieskringen vs. AIEEDA national totals
     and the CBS 1933/1937 scans;
   - candidate counts per election plausible (order 10²–10³; the 1933
     nomination doc is the largest at 197k OCR chars — 54 lists were entered
     that year, so expect a spike).

## Known traps

- Old spelling ("candidaten") throughout; `’s Gravenhage`/`'s-Gravenhage`
  variants; OCR renders `W.` as `VV.` and swallows list numbers ("LIJST n°.").
- 1922: no CBS statistics exist and the Kiesraad databank has documented
  lacunae — the Staatscourant is the ONLY complete source; treat its
  transcription quality as a first-class deliverable.
- 1918 uses the original 1917 Kieswet seat-allocation rules (lijstengroepen);
  the "Verdeeling" documents differ subtly across years — don't force one
  parser on all six years, validate per year.
- A person's list appears in multiple kieskringen with different vote counts:
  candidate-level "votes" is only well-defined per kieskring×lijst (or as
  preference votes). Model the panel accordingly (kieskring-level rows plus a
  person-level election summary) rather than flattening prematurely.
