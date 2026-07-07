"""
openarch_step4_occupation_individuals.py

Harvest pre-1811 occupation-bearing INDIVIDUALS from OpenArchieven to enrich the
surname status-persistence panel (precision boost; see the plan). The surname method
needs occupation-bearing individuals (name + occupation + place + rough date), NOT
father-son pairs — so pre-1811 church/notarial records that are dead ends for the IGE
are usable here.

Why keyword search (not a sourcetype harvest): probing showed an exhaustive
DTB+notarial show-fetch is ~1M+ calls (97-99% wasted on occupation-less records). The
OpenArch occupation search `name=%<beroep>` instead returns the occupation directly —
the query term IS the occupation — so each hit carries name + event year + place
WITHOUT a show call, and:
  * occupation = the query term  -> HISCAM known directly (vocab carries it),
  * amco       = the queried municipality -> no geocoding,
  * surname    parsed downstream (ind_step09) with the ind_step08 parser.

Scope: harvests ALL GOL-sample municipalities (both sides of the Mechelen border) so
the enriched analysis can use near-border kernel weights rather than a hard geographic
cutoff. Originally limited to a 20 km band; expanded to all ~1,138 municipalities.

Inputs (produced by openarch_step4_prep.R):
  data/openarchive/occupation_vocab.csv   (term, freq, HISCAM_NL)
  data/openarchive/all_munis.csv          (amco, name, in_mechelen, running)

Output: data/openarchive/occupations.duckdb
  occ_individuals(amco, muni_name, in_mechelen, term, hiscam, personname,
                  event_year, relationtype, eventtype, archive_code, identifier, eventplace)
  occ_progress(amco, term)   -- resumability: completed (municipality, term) cells

Two-phase per (municipality, term): the first search page returns number_found, so an
empty cell costs exactly one call. Resumable — re-run to continue.

Usage (from project root):
    uv run python code/data_wrangling/openarch/openarch_step4_occupation_individuals.py
    uv run python code/data_wrangling/openarch/openarch_step4_occupation_individuals.py --limit-munis 8   # smoke test
"""
import argparse
import asyncio
import csv
import logging
import urllib.parse
from pathlib import Path

import duckdb

from openarch_async_helpers import (
    BASE_URL, TokenBucketRateLimiter, make_session, clean_municipality_name,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

VOCAB_CSV = Path("data/openarchive/occupation_vocab.csv")
MUNI_CSV = Path("data/openarchive/all_munis.csv")
DB_PATH = Path("data/openarchive/occupations.duckdb")

YEAR_LO, YEAR_HI = 1500, 1810   # pre civil-registry
PAGE = 50                       # number_show
MAX_PAGES = 40                  # cap per (muni, term): up to 2000 hits — plenty for cells
RATE = 3.5                      # below the 4 req/s limit to avoid soft-throttling
CONCURRENCY = 4
MAX_RETRY = 4                   # retry 429/503/transient before giving up on a request
FLUSH_EVERY = 4000              # rows buffered before a DuckDB flush

# OpenArch eventplace is case-sensitive and wants proper Dutch capitalisation
# (e.g. "Bergen op Zoom", "'s-Hertogenbosch"); the geojson/HDNG names are UPPERCASE.
_PARTICLES = {"en", "van", "de", "der", "den", "ter", "te", "op", "aan", "bij",
              "uit", "over", "onder", "tot", "'t", "'s"}


def proper_case_nl(s: str) -> str:
    """UPPERCASE municipality name -> proper Dutch case (particles stay lower; capitalise
    each hyphen-separated part). 'BERGEN OP ZOOM' -> 'Bergen op Zoom'."""
    def cap(w):
        return w if w in _PARTICLES else "-".join(p.capitalize() for p in w.split("-"))
    return " ".join(cap(w) for w in s.strip().lower().split())


DDL = """
CREATE TABLE IF NOT EXISTS occ_individuals (
    amco         TEXT, muni_name TEXT, in_mechelen BOOLEAN,
    term         TEXT, hiscam DOUBLE,
    personname   TEXT, event_year INTEGER,
    relationtype TEXT, eventtype TEXT,
    archive_code TEXT, identifier TEXT, eventplace TEXT
);
CREATE TABLE IF NOT EXISTS occ_progress (amco TEXT, term TEXT, PRIMARY KEY (amco, term));
"""


def _search_url(place_clean: str, term: str, start: int) -> str:
    # name=%<term>+<lo>-<hi> filters profession; eventplace pins the municipality.
    name = urllib.parse.quote(f"%{term}")
    return (f"{BASE_URL}records/search.json?name={name}+{YEAR_LO}-{YEAR_HI}"
            f"&eventplace={place_clean}&start={start}&number_show={PAGE}&sort=6")


async def _get_json(session, limiter, url):
    """One GET with rate limiting + 429/503/transient retry. Returns parsed JSON or None."""
    for attempt in range(MAX_RETRY):
        await limiter.acquire()
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                if resp.status in (429, 503):
                    await asyncio.sleep(2 ** attempt)   # backoff: 1,2,4,8s
                    continue
                return None                              # other HTTP error: treat as no-retry
        except Exception:
            await asyncio.sleep(2 ** attempt)
    return None                                          # exhausted retries -> signal failure


async def _fetch_cell(session, limiter, muni, term, hiscam):
    """Return (rows, ok). ok=False if any page request failed (so the cell is NOT marked
    done and gets retried on re-run) — guards against silently recording 0 for a
    rate-limited cell."""
    place_clean = clean_municipality_name(proper_case_nl(muni["name"]))
    rows, start, pages = [], 0, 0
    while pages < MAX_PAGES:
        url = _search_url(place_clean, term, start)
        data = await _get_json(session, limiter, url)
        if data is None:
            return rows, False                            # request failed -> don't trust, retry later
        r = data.get("response", {}) if isinstance(data, dict) else {}
        docs = r.get("docs", []) or []
        for d in docs:
            ev = d.get("eventdate") or {}
            yr = ev.get("year") if isinstance(ev, dict) else None
            place = d.get("eventplace")
            if isinstance(place, list):
                place = place[0] if place else None
            rows.append((muni["amco"], muni["name"], muni["in_mechelen"], term, hiscam,
                         d.get("personname"), yr, d.get("relationtype"), d.get("eventtype"),
                         d.get("archive_code"), d.get("identifier"), place))
        pages += 1
        if (start + PAGE) >= int(r.get("number_found", 0)):
            break
        start += PAGE
    return rows, True


async def main(limit_munis: int | None):
    if not VOCAB_CSV.exists() or not MUNI_CSV.exists():
        raise SystemExit("Run openarch_step4_prep.R first (missing vocab/muni CSVs).")

    vocab = [(r["term"], float(r["HISCAM_NL"])) for r in csv.DictReader(VOCAB_CSV.open())]
    munis = [{"amco": r["amco"], "name": r["name"],
              "in_mechelen": r["in_mechelen"].strip().upper() == "TRUE"}
             for r in csv.DictReader(MUNI_CSV.open())]
    if limit_munis:
        munis = munis[:limit_munis]

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)
    done = {(a, t) for a, t in con.execute("SELECT amco, term FROM occ_progress").fetchall()}

    todo = [(m, t, h) for m in munis for (t, h) in vocab if (m["amco"], t) not in done]
    log.info("Harvest: %d munis x %d terms = %d cells; %d already done, %d to do",
             len(munis), len(vocab), len(munis) * len(vocab), len(done), len(todo))

    limiter = TokenBucketRateLimiter(rate=RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    buf_rows, buf_prog, n_cells, n_rows = [], [], 0, 0

    def flush():
        nonlocal buf_rows, buf_prog
        if buf_rows:
            con.executemany(
                "INSERT INTO occ_individuals VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", buf_rows)
        if buf_prog:
            con.executemany("INSERT OR IGNORE INTO occ_progress VALUES (?,?)", buf_prog)
        buf_rows, buf_prog = [], []

    n_failed = 0
    async with make_session(connector_limit=CONCURRENCY) as session:
        async def worker(m, t, h):
            async with sem:
                rows, ok = await _fetch_cell(session, limiter, m, t, h)
                return (m, t), rows, ok

        tasks = [asyncio.create_task(worker(m, t, h)) for (m, t, h) in todo]
        for fut in asyncio.as_completed(tasks):
            (m, t), rows, ok = await fut
            n_cells += 1
            if not ok:                       # failed request: keep rows out, leave cell un-done
                n_failed += 1
                continue
            buf_rows.extend(rows)
            buf_prog.append((m["amco"], t))  # only mark done on a clean response
            n_rows += len(rows)
            if len(buf_rows) >= FLUSH_EVERY:
                flush()
            if n_cells % 2000 == 0:
                log.info("  %d/%d cells, %d rows, %d failed (will retry)", n_cells, len(todo), n_rows, n_failed)
    flush()
    if n_failed:
        log.warning("%d cells failed after retries — re-run to retry them.", n_failed)
    tot = con.execute("SELECT COUNT(*) FROM occ_individuals").fetchone()[0]
    cath = con.execute("SELECT COUNT(*) FROM occ_individuals WHERE in_mechelen").fetchone()[0]
    log.info("Done. occ_individuals total=%d (Catholic=%d, Protestant=%d)", tot, cath, tot - cath)
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-munis", type=int, default=None, help="smoke test: first N munis")
    args = ap.parse_args()
    asyncio.run(main(args.limit_munis))
