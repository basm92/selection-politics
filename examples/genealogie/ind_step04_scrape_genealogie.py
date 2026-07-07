"""
ind_step04_scrape_genealogie.py

Unified genealogieonline.nl crawler — ONE 1500-1900 run for every target Dutch
municipality. This consolidates the two historical scrapers (the 1750-1900
occupation-pair crawl and the 1500-1800 births crawl) into a single pipeline
writing a single node table `persons` in `genealogie.duckdb`.

Relationship to the committed data
-----------------------------------
The dataset currently in the repo was NOT produced by this script — it was
produced by the two legacy crawls and stitched together (preserving the legacy
1750-1900 Phase-D/E `pairs` verbatim) by `ind_step05_assemble_genealogie.py`.
This script is the canonical entry point for a *future* from-scratch run, and it
writes exactly the `persons` / `person_children` schema that the merge produces,
so the rest of the pipeline (clean, lineages) is identical either way.

Method note: this unified crawler collects *all* persons in each birth window
(it does NOT filter on `oc=*`), records `had_beroep` per person, and leaves
father->son profession pairs to be derived from the gender-correct lineage spine
in `ind_step06_build_lineages.py` (the same construction used for the 1500-1800
era in the merge). This is uniform across the whole 1500-1900 range.

Pipeline:
  Phase A  Seed target municipalities from OpenArch marriages.duckdb
           (survey_progress), apply name-correction overrides, and queue
           50-year birth-year search windows over 1500-1900.
  Phase B  For each (municipality, window) search genealogieonline.nl, paginate
           all results, store one `persons` row per hit. Windows hitting the
           10 000-result hard cap shard into 10-year sub-windows automatically.
  Phase C  Fetch each person page; verify addressCountry=Nederland; extract
           beroep, full name, birth place, the male-parent (father) link and
           male children. Fully resumable via persons.fetched; 404/403 -> skip,
           transient failures left un-fetched for a rerun.

Output: data/genealogieonline/genealogie.duckdb
        (node table `persons`; edge table `person_children`)

Usage (from project root):
    uv run python code/data_wrangling/genealogie/ind_step04_scrape_genealogie.py
    uv run python code/data_wrangling/genealogie/ind_step04_scrape_genealogie.py --phase A
    uv run python code/data_wrangling/genealogie/ind_step04_scrape_genealogie.py --phase C --limit 500
    # then: ind_step06_build_lineages.py ; ind_step05_clean_genealogieonline.py
"""
import argparse
import asyncio
import csv
import gc
import logging
import re
import time
import unicodedata
from pathlib import Path
from urllib.parse import urlencode, urljoin

import aiohttp
import duckdb
import psutil
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL    = "https://www.genealogieonline.nl"
SEARCH_URL  = f"{BASE_URL}/zoeken/"
DB_PATH     = Path("data/genealogieonline/genealogie.duckdb")
OPENARCH_DB = Path("data/openarchive/marriages.duckdb")  # municipality list
OVERRIDES_CSV = Path("notes/zero_coverage_municipalities.csv")

USER_AGENT  = "borders-of-belief-research/1.0 (academic; a.h.machielsen@uu.nl)"

RATE        = 5.0     # max requests per second
CONCURRENCY = 8       # max simultaneous open connections
PAGE_SIZE   = 15      # results per page on genealogieonline
MAX_RESULTS = 10_000  # hard cap per search query
BATCH       = 500     # Phase C: person pages fetched per asyncio.gather (bounds memory)
LOG_EVERY   = 200

CRAWL_TAG   = "unified_1500_1900"

# 50-year initial windows over 1500-1900; any that hits the 10k cap is sharded
# into 10-year sub-windows automatically.
BIRTH_WINDOWS = [
    (1500, 1549), (1550, 1599),
    (1600, 1649), (1650, 1699),
    (1700, 1749), (1750, 1799),
    (1800, 1849), (1850, 1899),
]

# Municipalities whose default name_norm (apostrophes stripped to spaces) does
# not match genealogieonline's pn= filter. Override values validated by probing.
PLACE_NORM_OVERRIDES = {
    "11239": "stad aan t haringvliet",  # Stad Aan 'T Haringvliet
    "11022": "tull en t waal",          # Tull En 'T Waal
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_proc = psutil.Process()


def log_rss(label: str) -> None:
    rss_mb = _proc.memory_info().rss / 1024 / 1024
    log.info("  [mem] %s — RSS %.1f MB", label, rss_mb)


# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS target_municipalities (
    amco                 VARCHAR PRIMARY KEY,
    name                 VARCHAR,
    name_norm            VARCHAR,
    place_norm           VARCHAR,
    openarch_pairs       INTEGER,
    search_name_override VARCHAR
);

CREATE TABLE IF NOT EXISTS search_progress (
    amco        VARCHAR,
    gv          INTEGER,   -- birth year from (geboorte van)
    gt          INTEGER,   -- birth year until (geboorte tot)
    n_results   INTEGER DEFAULT -1,
    done        BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (amco, gv, gt)
);

CREATE TABLE IF NOT EXISTS persons (
    url              VARCHAR PRIMARY KEY,   -- /I<n>.php URL: unique per person-in-tree
    amco             VARCHAR,
    place_norm       VARCHAR,               -- the pn= search term used (= birth place)
    person_name      VARCHAR,               -- result-snippet name (may be truncated)
    person_name_full VARCHAR,               -- full name from page (Phase C)
    birth_year       INTEGER,
    birth_place      VARCHAR,               -- person's OWN page birth place (Phase C)
    source_tree      VARCHAR,               -- genealogy/tree title the entry came from
    beroep           VARCHAR,               -- raw Dutch profession string (Phase C)
    had_beroep       BOOLEAN,               -- convenience flag (beroep IS NOT NULL)
    father_url       VARCHAR,               -- male-parent page link (within tree)
    father_name      VARCHAR,
    fetched          BOOLEAN DEFAULT FALSE,  -- person page visited
    skip             BOOLEAN DEFAULT FALSE,  -- 404 / non-NL / unparseable
    crawl            VARCHAR
);

CREATE TABLE IF NOT EXISTS person_children (
    parent_url  VARCHAR,
    child_url   VARCHAR,
    child_name  VARCHAR,
    PRIMARY KEY (parent_url, child_url)
);
"""


def init_db(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in DDL.strip().split(";"):
        s = stmt.strip()
        if s:
            con.execute(s)


# ---------------------------------------------------------------------------
# Rate limiter (token bucket)
# ---------------------------------------------------------------------------
class TokenBucket:
    def __init__(self, rate: float = RATE):
        self.rate   = rate
        self.tokens = rate
        self._last  = time.monotonic()
        self._lock  = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now      = time.monotonic()
            elapsed  = now - self._last
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self._last  = now
            if self.tokens < 1.0:
                wait = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(wait)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0


def make_session() -> aiohttp.ClientSession:
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    timeout   = aiohttp.ClientTimeout(total=30, connect=10)
    return aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )


async def fetch(session: aiohttp.ClientSession, bucket: TokenBucket,
                url: str, retries: int = 3) -> tuple[str, str | None]:
    """Fetch a URL. Returns (status, html):
      "ok"        -> 200, html is the page text
      "gone"      -> 404 (missing) or 403 (restricted/private tree). Stable
                     per-URL; caller marks skipped, does not retry.
      "transient" -> timeout / connection error / 429 / 503 after retries.
                     Caller leaves the row un-fetched so a rerun retries it.
    """
    for attempt in range(retries):
        await bucket.acquire()
        try:
            async with session.get(url) as resp:
                if resp.status in (404, 403):
                    return "gone", None
                if resp.status in (429, 503):
                    wait = 2 ** attempt * 5
                    log.debug("Rate-limited (%d), waiting %ds", resp.status, wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                return "ok", await resp.text(errors="replace")
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == retries - 1:
                log.warning("Transient failure %s after %d tries: %s", url, retries, exc)
                return "transient", None
            await asyncio.sleep(2 ** attempt)
    return "transient", None  # exhausted 429/503 backoff


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _norm(name: str) -> str:
    """Lowercase, strip diacritics, collapse whitespace."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = nfkd.encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", ascii_str).strip().lower()


# "Name (1696-1758) » Tree title"  →  name, birth_year, tree
_SNIPPET_RE = re.compile(r"^(.*?)\s*\((\d{4}|\?{4})\s*[-–]\s*(?:\d{4}|\?{4})?\)\s*(?:»\s*(.*))?$")


def _parse_search_results(html: str) -> list[dict]:
    """Return list of {url, person_name, birth_year, source_tree} for each hit."""
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not re.search(r"/I\d+\.php$", href):
            continue
        url = href if href.startswith("http") else BASE_URL + href

        parent_text = a.parent.get_text(" ", strip=True) if a.parent else ""
        m = _SNIPPET_RE.match(parent_text)
        if m:
            person_name = m.group(1).strip() or None
            birth_year  = int(m.group(2)) if m.group(2).isdigit() else None
            source_tree = m.group(3).strip() if m.group(3) else None
        else:
            person_name = a.get_text(" ", strip=True) or None
            birth_year  = None
            source_tree = None

        results.append({
            "url": url,
            "person_name": person_name,
            "birth_year": birth_year,
            "source_tree": source_tree,
        })
    return results


def _parse_result_count(html: str) -> int:
    """Extract total result count from a search page (returns 0 if not found)."""
    m = re.search(r"van\s+<[^>]+>([\d\.]+)<", html, re.I)
    if m:
        try:
            return int(m.group(1).replace(".", ""))
        except ValueError:
            pass
    m = re.search(r"van\s+([\d\.]+)", html, re.I)
    if m:
        try:
            return int(m.group(1).replace(".", ""))
        except ValueError:
            pass
    return 0


# ---------------------------------------------------------------------------
# Person-page parsers (Phase C)
# ---------------------------------------------------------------------------
_BEROEP_RE = re.compile(r"[Bb]eroep\s*:", re.I)


def _parse_beroep(soup: BeautifulSoup) -> str | None:
    for ul in soup.find_all("ul", class_="nicelist"):
        for li in ul.find_all("li"):
            text = li.get_text(" ", strip=True)
            if _BEROEP_RE.match(text):
                return _BEROEP_RE.sub("", text, count=1).strip().rstrip(".")
    return None


def _parse_birth_place(soup: BeautifulSoup) -> str | None:
    bp_span = soup.find("span", attrs={"itemprop": "birthPlace"})
    if bp_span:
        loc = bp_span.find("meta", attrs={"itemprop": "addressLocality"})
        if loc:
            return loc.get("content")
    return None


def _parse_father(soup: BeautifulSoup, base_url: str) -> tuple[str | None, str | None]:
    """Return (father_url, father_name) for the first male parent with a page."""
    for pdiv in soup.find_all(attrs={"itemprop": "parent"}):
        gm = pdiv.find("meta", attrs={"itemprop": "gender"})
        if not gm or gm.get("content") != "male":
            continue
        father_url = father_name = None
        um = pdiv.find("meta", attrs={"itemprop": "url"})
        if um:
            father_url = um.get("content")
            if father_url and not father_url.startswith("http"):
                father_url = urljoin(base_url, father_url)
        nm = pdiv.find("meta", attrs={"itemprop": "name"})
        if nm:
            father_name = nm.get("content")
        return father_url, father_name
    return None, None


def _parse_children(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str | None]]:
    """Return (child_url, child_name) for each male child with a linked page."""
    children = []
    for cdiv in soup.find_all(attrs={"itemprop": "children"}):
        gm = cdiv.find("meta", attrs={"itemprop": "gender"})
        if not gm or gm.get("content") != "male":
            continue
        um = cdiv.find("meta", attrs={"itemprop": "url"})
        if not um:
            continue
        child_url = um.get("content")
        if not child_url:
            continue
        if not child_url.startswith("http"):
            child_url = urljoin(base_url, child_url)
        nm = cdiv.find("meta", attrs={"itemprop": "name"})
        children.append((child_url, nm.get("content") if nm else None))
    return children


# ---------------------------------------------------------------------------
# Phase A: seed target municipalities + search windows
# ---------------------------------------------------------------------------
def _apply_overrides(con: duckdb.DuckDBPyConnection) -> None:
    """Idempotently apply the two pn= overrides and any zero-coverage CSV fixes."""
    for amco, place in PLACE_NORM_OVERRIDES.items():
        con.execute(
            "UPDATE target_municipalities SET place_norm = ? WHERE amco = ? AND place_norm != ?",
            [place, amco, place],
        )
    if OVERRIDES_CSV.exists():
        with OVERRIDES_CSV.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                amco = row["amco"].strip()
                override = (row.get("suggested_search_name") or "").strip()
                if override:
                    con.execute(
                        "UPDATE target_municipalities "
                        "SET search_name_override = ?, place_norm = ? WHERE amco = ?",
                        [override, override, amco],
                    )


def phase_a(con: duckdb.DuckDBPyConnection) -> None:
    existing = con.execute("SELECT COUNT(*) FROM target_municipalities").fetchone()[0]
    pending  = con.execute("SELECT COUNT(*) FROM search_progress WHERE done=FALSE").fetchone()[0]

    if existing > 0:
        _apply_overrides(con)

    if existing > 0 and pending > 0:
        log.info("Phase A: %d municipalities already loaded, %d slots pending",
                 existing, pending)
        return

    if existing == 0:
        if not OPENARCH_DB.exists():
            log.error("Phase A: OpenArch DB not found at %s", OPENARCH_DB)
            return
        ocon = duckdb.connect(str(OPENARCH_DB), read_only=True)
        rows = ocon.execute(
            "SELECT DISTINCT amco, name FROM survey_progress WHERE name IS NOT NULL"
        ).fetchall()
        ocon.close()

        if not rows:
            log.warning("Phase A: no municipalities found in OpenArch DB")
            return

        con.executemany(
            "INSERT OR IGNORE INTO target_municipalities "
            "(amco, name, name_norm, place_norm) VALUES (?,?,?,?)",
            [(a, n, _norm(n), _norm(n)) for a, n in rows],
        )
        log.info("Phase A: %d municipalities seeded from %s", len(rows), OPENARCH_DB.name)
        _apply_overrides(con)

    munis = con.execute("SELECT amco FROM target_municipalities").fetchall()
    sp_rows = [(amco, gv, gt) for (amco,) in munis for gv, gt in BIRTH_WINDOWS]
    con.executemany(
        "INSERT OR IGNORE INTO search_progress (amco, gv, gt) VALUES (?,?,?)",
        sp_rows,
    )
    log.info("Phase A: %d search slots queued (%d munis × %d windows)",
             len(sp_rows), len(munis), len(BIRTH_WINDOWS))


# ---------------------------------------------------------------------------
# Phase B: search genealogieonline by municipality + birth window → persons
# ---------------------------------------------------------------------------
def _search_url(place_norm: str, gv: int, gt: int, start: int = 0) -> str:
    params = {
        "type":  "persoon",
        "pn":    place_norm,
        "gv":    gv,     # geboorte van (birth from)
        "gt":    gt,     # geboorte tot (birth until)
        "ta":    PAGE_SIZE,
        "start": start,
    }
    return f"{SEARCH_URL}?{urlencode(params)}"


async def phase_b(con: duckdb.DuckDBPyConnection) -> None:
    bucket  = TokenBucket()
    session = make_session()
    db_lock = asyncio.Lock()
    sem     = asyncio.Semaphore(CONCURRENCY)
    row_buf: list[tuple] = []
    processed = 0

    async def flush_buf():
        if row_buf:
            con.executemany(
                """
                INSERT OR IGNORE INTO persons
                    (url, amco, place_norm, person_name, birth_year, source_tree, crawl)
                VALUES (?,?,?,?,?,?,?)
                """,
                row_buf,
            )
            row_buf.clear()

    async def process_slot(amco: str, place: str, gv: int, gt: int):
        nonlocal processed
        async with sem:
            first_url = _search_url(place, gv, gt, 0)
            _, html = await fetch(session, bucket, first_url)
            if html is None:
                async with db_lock:
                    con.execute(
                        "UPDATE search_progress SET done=TRUE WHERE amco=? AND gv=? AND gt=?",
                        [amco, gv, gt],
                    )
                return

            total = _parse_result_count(html)

            if total >= MAX_RESULTS:
                sub_windows = [(sub_gv, min(sub_gv + 9, gt))
                               for sub_gv in range(gv, gt + 1, 10)]
                async with db_lock:
                    con.executemany(
                        "INSERT OR IGNORE INTO search_progress (amco, gv, gt) VALUES (?,?,?)",
                        [(amco, sgv, sgt) for sgv, sgt in sub_windows],
                    )
                    con.execute(
                        "UPDATE search_progress SET done=TRUE, n_results=? WHERE amco=? AND gv=? AND gt=?",
                        [total, amco, gv, gt],
                    )
                log.info("  Phase B: %s %d-%d hit 10k cap → sharded into %d sub-windows",
                         amco, gv, gt, len(sub_windows))
                return

            collected: list[tuple] = []
            results = _parse_search_results(html)
            collected.extend(
                (r["url"], amco, place, r["person_name"], r["birth_year"], r["source_tree"], CRAWL_TAG)
                for r in results
            )

            start = PAGE_SIZE
            while start < total and len(results) >= PAGE_SIZE:
                url  = _search_url(place, gv, gt, start)
                _, html = await fetch(session, bucket, url)
                if html is None:
                    break
                results = _parse_search_results(html)
                collected.extend(
                    (r["url"], amco, place, r["person_name"], r["birth_year"], r["source_tree"], CRAWL_TAG)
                    for r in results
                )
                start += PAGE_SIZE

            async with db_lock:
                row_buf.extend(collected)
                if len(row_buf) >= 2000:
                    await flush_buf()
                con.execute(
                    "UPDATE search_progress SET done=TRUE, n_results=? WHERE amco=? AND gv=? AND gt=?",
                    [len(collected), amco, gv, gt],
                )
                processed += 1
                if processed % LOG_EVERY == 0:
                    log.info("  Phase B: %d search slots processed", processed)
                    log_rss("Phase B")

    round_num = 0
    while True:
        todo = con.execute(
            """
            SELECT sp.amco,
                   COALESCE(t.search_name_override, t.place_norm) AS place_norm,
                   sp.gv, sp.gt
            FROM   search_progress sp
            JOIN   target_municipalities t ON sp.amco = t.amco
            WHERE  sp.done = FALSE
            """
        ).fetchall()

        if not todo:
            break

        round_num += 1
        log.info("Phase B round %d: %d search slots to process …", round_num, len(todo))

        await asyncio.gather(*[process_slot(a, p, gv, gt) for a, p, gv, gt in todo])

        async with db_lock:
            await flush_buf()

    await session.close()
    n = con.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    log.info("Phase B: complete — %d person entries collected", n)


# ---------------------------------------------------------------------------
# Phase C: fetch each person page → enrich persons + collect father/child links
# ---------------------------------------------------------------------------
async def phase_c(con: duckdb.DuckDBPyConnection, limit: int = 0) -> None:
    bucket  = TokenBucket()
    session = make_session()
    sem     = asyncio.Semaphore(CONCURRENCY)
    done    = 0

    # Snapshot this run's work into a temp table and iterate with a stable
    # LIMIT/OFFSET cursor: each URL is attempted at most once per run, the run
    # terminates when the snapshot is exhausted, and transient failures stay
    # fetched=FALSE for the next run's fresh snapshot.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE phase_c_todo AS
        SELECT url FROM persons WHERE fetched = FALSE AND skip = FALSE
    """)
    total_todo = con.execute("SELECT COUNT(*) FROM phase_c_todo").fetchone()[0]
    log.info("Phase C: %d person entries to enrich%s",
             total_todo, f" (limited to {limit} this run)" if limit else "")

    batch_transient = 0

    async def fetch_person(url: str):
        nonlocal done, batch_transient
        async with sem:
            status, html = await fetch(session, bucket, url)
            done += 1

            if status == "transient":
                batch_transient += 1
                return
            if status == "gone" or html is None:
                con.execute(
                    "UPDATE persons SET fetched=TRUE, skip=TRUE WHERE url=?", [url]
                )
                return

            soup = BeautifulSoup(html, "html.parser")

            countries = {
                m.get("content", "")
                for m in soup.find_all("meta", attrs={"itemprop": "addressCountry"})
            }
            if countries and "Nederland" not in countries:
                con.execute(
                    "UPDATE persons SET fetched=TRUE, skip=TRUE WHERE url=?", [url]
                )
                return

            beroep                  = _parse_beroep(soup)
            birth_place             = _parse_birth_place(soup)
            father_url, father_name = _parse_father(soup, url)

            person_name_full = None
            pnm = soup.find("meta", attrs={"itemprop": "name", "content": True})
            if pnm:
                person_name_full = pnm.get("content")

            children = _parse_children(soup, url)
            del soup

            con.execute(
                """
                UPDATE persons SET
                    fetched=TRUE, skip=FALSE,
                    had_beroep=?, beroep=?, birth_place=?,
                    person_name_full=?, father_url=?, father_name=?
                WHERE url=?
                """,
                [beroep is not None, beroep, birth_place,
                 person_name_full, father_url, father_name, url],
            )
            if children:
                con.executemany(
                    "INSERT OR IGNORE INTO person_children (parent_url, child_url, child_name) VALUES (?,?,?)",
                    [(url, cu, cn) for cu, cn in children],
                )
            if done % LOG_EVERY == 0:
                log.info("  Phase C: %d / %d persons processed this run", done, total_todo)
                log_rss("Phase C")

    offset    = 0
    processed = 0
    aborted   = False
    while True:
        if limit and processed >= limit:
            break
        size  = min(BATCH, limit - processed) if limit else BATCH
        batch = con.execute(
            "SELECT url FROM phase_c_todo LIMIT ? OFFSET ?", [size, offset]
        ).fetchall()
        if not batch:
            break

        batch_transient = 0
        await asyncio.gather(*[fetch_person(u) for (u,) in batch])

        # Circuit breaker: a near-total batch failure means the server is
        # throttling/blocking us — stop cleanly (rows stay fetched=FALSE).
        if batch_transient / len(batch) > 0.5:
            log.error("Phase C: %d/%d transient failures in a batch — server likely "
                      "throttling. Aborting; rerun --phase C to resume.",
                      batch_transient, len(batch))
            aborted = True
            break

        offset    += len(batch)
        processed += len(batch)
        gc.collect()

    await session.close()
    if not aborted:
        log.info("Phase C: this run processed %d entries from the snapshot", processed)
    enriched = con.execute(
        "SELECT COUNT(*) FROM persons WHERE fetched=TRUE AND skip=FALSE"
    ).fetchone()[0]
    with_beroep = con.execute(
        "SELECT COUNT(*) FROM persons WHERE had_beroep=TRUE"
    ).fetchone()[0]
    n_edges = con.execute("SELECT COUNT(*) FROM person_children").fetchone()[0]
    remaining = con.execute(
        "SELECT COUNT(*) FROM persons WHERE fetched=FALSE AND skip=FALSE"
    ).fetchone()[0]
    log.info("Phase C: %d enriched, %d with beroep, %d parent-child edges; %d entries remain "
             "(rerun --phase C to continue; crawl is DONE when this reaches 0)",
             enriched, with_beroep, n_edges, remaining)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--phase",
        choices=["A", "B", "C", "all"],
        default="all",
        help="Which phase to run (default: all → A, B, then C).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Phase C only: cap the number of person pages fetched this run "
             "(0 = no cap). Useful for smoke tests.",
    )
    args = parser.parse_args()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    init_db(con)

    run_all = args.phase == "all"

    if run_all or args.phase == "A":
        phase_a(con)

    if run_all or args.phase == "B":
        asyncio.run(phase_b(con))

    if run_all or args.phase == "C":
        asyncio.run(phase_c(con, limit=args.limit))

    con.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
