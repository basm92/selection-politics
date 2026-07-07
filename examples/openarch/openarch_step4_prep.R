# openarch_step4_prep.R
# Prep for the OpenArchieven occupation-keyword harvest (precision-boost for the
# surname status-persistence measure). Produces two inputs for the harvester:
#   data/openarchive/occupation_vocab.csv  — status-balanced occupation keywords + HISCAM
#   data/openarchive/all_munis.csv         — all GOL-sample municipalities (both sides)
#
# Rationale: harvesting OpenArch by occupation keyword returns the occupation itself
# (the query term), so each hit's HISCAM is known directly from the term — no show
# calls, no LLM cleaning. The vocabulary is the set of canonical Dutch occupation
# strings that (a) actually occur in our genealogy data and (b) map to HISCAM, so it
# spans the real status distribution.
#
# Originally limited to a 20 km band around the Mechelen border; expanded to all
# GOL-sample municipalities so the enriched surname-persistence analysis can use
# near-border kernel weights rather than a hard geographic cutoff.

suppressMessages({library(tidyverse); library(sf); library(arrow)})

MIN_FREQ <- 30    # keep occupation terms occurring >= this often in the genealogy panel

# ── Occupation vocabulary (term + HISCAM), status-balanced by construction ──
hisco <- readr::read_delim("./data/hisco/hsn2013a_hisco_comma.csv", show_col_types = FALSE) |>
  filter(HISCLASS > 0, !is.na(HISCAM_NL)) |>
  mutate(orig_l = tolower(trimws(Original))) |>
  distinct(orig_l, .keep_all = TRUE) |>
  select(orig_l, HISCAM_NL)

vocab <- read_parquet("./data/genealogieonline/surname_persons.parquet") |>
  mutate(term = tolower(trimws(beroep_clean))) |>
  filter(!is.na(term), term != "") |>
  count(term, name = "freq") |>
  inner_join(hisco, by = c("term" = "orig_l")) |>
  filter(freq >= MIN_FREQ, nchar(term) >= 4) |>   # drop ultra-rare + too-short (noisy %match)
  arrange(desc(freq))

readr::write_csv(vocab, "./data/openarchive/occupation_vocab.csv")
cat(sprintf("occupation_vocab.csv: %d terms (freq>=%d)\n", nrow(vocab), MIN_FREQ))
cat("  HISCAM spread (deciles):\n"); print(round(quantile(vocab$HISCAM_NL, probs = seq(0,1,.1)), 1))
cat("  top 15 terms:\n"); print(vocab |> head(15))

# ── All GOL-sample municipalities, both sides ──
munis <- read_sf("./data/analysis/step7_gol_human_capital.geojson") |>
  st_drop_geometry() |>
  filter(!is.na(running), !is.na(name)) |>
  transmute(amco = as.character(acode), name, in_mechelen,
            running = round(running)) |>
  arrange(in_mechelen, abs(running))
readr::write_csv(munis, "./data/openarchive/all_munis.csv")
cat(sprintf("\nall_munis.csv: %d munis (Catholic=%d, Protestant=%d)\n",
            nrow(munis), sum(munis$in_mechelen), sum(!munis$in_mechelen)))
cat(sprintf("\nrough query budget: %d terms x %d munis = %s (town,occ) pre-checks\n",
            nrow(vocab), nrow(munis), format(nrow(vocab)*nrow(munis), big.mark=",")))
