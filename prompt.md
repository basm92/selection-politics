## Politicians' Status and Entry into Politics — Dutch Lower House, 1848–1940

### Context
This is a multi-phase research-data-engineering project. Work phase by phase.
After each phase marked "CHECKPOINT", stop, summarize what was found (coverage,
match rates, blockers), and wait before continuing — later phases depend on
what actually survives the earlier ones, and some of the sources below are
unverified leads, not confirmed inputs.

Follow the house style already established in `examples/` (openarch, genealogie
pipelines): numbered step scripts with a header docstring stating Input/Output/
Method, async rate-limited scraping with a token-bucket limiter, resumable
progress tracked in DuckDB (never re-fetch on rerun), and parquet exports for
downstream analysis.

---

### Phase 0 — Feasibility & Data Scoping Audit (CHECKPOINT before Phase 1)

Before committing to a full build, verify each data source actually delivers
what the later phases assume:

1. **Tweede Kamer candidates 1848–1917** (huygens.knaw.nl/verkiezingentweedekamer):
   confirm what fields are actually scrapable per candidate (name, district,
   year, party/grouping, elected y/n, vote count if present), and get a rough
   total-record count.
2. **Post-1917 candidate/vote data at municipal or district level**: no leads
   yet — search cold. Candidate places to check include (not exhaustive):
   Parlement.com / PDC biographical database, CBS historical election
   statistics ("Statistiek der Verkiezingen"), Kiesraad archives, Delpher-
   scanned yearbooks, DNPP (Documentatiecentrum Nederlandse Politieke
   Partijen). Report what's found, at what granularity, and how it would need
   to be captured (API, scrape, or manual transcription from scanned images).
3. **HIP-NL** (druid.datalegend.net/HIP-NL): confirm it's actually queryable
   (API vs. static dump), what geographic/temporal coverage it has, and
   whether records are name-identifiable enough to link to candidates at all.
4. **NLGIS** (nlgis.nl/api/maps): confirm the `year`/`province` query returns
   usable TopoJSON with amco codes across the full 1848–1940 span, including
   during periods of heavy municipal reorganization.
5. **OpenArchieven / GenealogieOnline coverage**: using the existing
   `examples/openarch` and `examples/genealogie` pipelines as reference, spot-
   check expected coverage/hit-rate for a small sample of known 19th-century
   surnames before assuming full-scale linkage will work.

**Deliverable:** a short coverage/feasibility report (table by source: years,
geographic granularity, access method, known gaps) plus a go/no-go
recommendation per source. Flag anything that changes the scope of later
phases (e.g., if HIP-NL turns out unusable, note it as a limitation rather
than silently dropping it).

---

### Phase 1 — Core Candidate Database, 1848–1940 (CHECKPOINT before Phase 2)

- Scrape the full 1848–1917 Tweede Kamer candidate database (district system).
- Build/scrape whatever post-1917 source(s) Phase 0 identified, at whatever
  granularity is actually available (municipal, district, or national if
  that's the best obtainable).
- Normalize into a single candidate × election-year panel: name, constituency,
  year, party, elected (y/n), votes received (where available), and a
  provenance field recording which source/method produced the row.
- Use NLGIS to build a municipality-identity crosswalk across years (handling
  mergers/splits/renames), so constituency names are comparable across the
  1848–1940 span and across the 1917 district→PR transition.

**Deliverable:** the candidate panel (DuckDB + parquet) and the municipality
crosswalk table, with a count of candidates recovered per decade.

---

### Phase 2 — Entity Resolution to Genealogical Sources

- For each candidate, query OpenArchieven and GenealogieOnline (using
  `examples/openarch` and `examples/genealogie` as the method template) to
  find plausible matching individuals.
- Produce **confidence-scored matches**, not a filtered validated subset: attach
  an explicit match probability/score per candidate–person pair (based on name
  similarity, birth year/place agreement, municipality overlap via the Phase 1
  crosswalk, etc.), and keep all candidate matches with their scores so
  downstream analysis can weight or threshold as needed.
- Use the NLGIS-based municipality crosswalk to help disambiguate matches
  where a candidate's constituency and a genealogical record's birthplace use
  different historical municipality names for the same place.

**Deliverable:** candidate-to-person linkage table with match scores and the
features that produced them (so the scoring is auditable, not a black box).

---

### Phase 3 — Occupational & Dynastic Status Classification

- Match `beroep` (occupation) strings captured via the genealogie pipeline to
  the HISCO codebook (`examples/hisco/hsn2013a_hisco_comma.csv`) to attach
  HISCO/HISCLASS/HISCAM scores to matched candidates and their linked
  relatives (fathers, sons) via the existing `person_children` lineage edges.
- Define political dynasty membership from verified genealogical lineage
  (shared ancestry between candidates across time), not just shared surname.

---

### Phase 4 — Income Enrichment (conditional on Phase 0 findings)

- If Phase 0 confirmed HIP-NL is usable: attempt name-based linkage to income
  records for matched candidates.
- If not usable: document this explicitly as a data limitation and consider
  what other wealth proxies were surfaced in Phase 0 (e.g., probate/tax
  records) as a substitute, rather than silently omitting a wealth measure.

---

### Phase 5 — Dynasty Construction & Empirical Strategy

Once the linked panel exists, refine the research question against what the
data actually supports:

- Construct measures of dynastic status (prior/subsequent relatives as
  candidates or MPs), occupational status (HISCLASS/HISCAM), and wealth
  (income data or proxy) for each candidate.
- Before finalizing an identification strategy, search the political-dynasty
  and political-selection literature (e.g., Dal Bó et al., Querubin, Rossi,
  Van Coppenolle's work on parliamentary dynasties) to identify what gap the
  Dutch case fills — the 1917 district→PR electoral reform and the staged
  suffrage extensions (1887, 1896, 1917, 1922) are plausible sources of
  institutional variation worth exploiting.
- Propose 2–3 candidate empirical strategies (e.g., difference-in-differences
  around the 1917 reform, family fixed effects, close-election-style
  discontinuities where usable) rather than committing to one before seeing
  what identification the assembled data can actually support.

**Overarching question:** Does wealth/status increase the likelihood of
election into politics, and what determines that relationship? Sharpen this
once Phases 0–4 establish what can actually be measured and at what
resolution — don't force the data to fit a strategy decided in advance.  
