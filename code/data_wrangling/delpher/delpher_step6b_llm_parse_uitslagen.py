# =============================================================================
# delpher_step6b_llm_parse_uitslagen.py  [DELPHER PIPELINE - STEP 6b]
# Input:  data/delpher/delpher.duckdb  (ocr_pages + kandidatenlijsten from
#                                       step 5; optionally step 6 tables for
#                                       --targeted mode)
#         examples/.env                 (GOOGLE_API_KEY)
# Output: data/delpher/delpher.duckdb
#           voorkeur_stemmen — preference votes per candidate × kieskring ×
#                              lijst, aligned to kandidatenlijsten positions
#           lijst_uitslagen  — stemcijfer per kieskring × lijst, with checksum
#           gekozen          — elected members ("Vaststelling van den uitslag")
#           uitslag_issues   — audit log of mismatches the validator caught
#           llm_parse_pages  — progress table (resumable; skips done pages)
#
# LLM-based parser for the vote tables (BESLUIT / proces-verbaal) and elected
# members table in the Staatscourant results issues. Replaces the rule-based
# parser (delpher_step6_parse_uitslagen.py) for the vote tables; uses Gemini
# flash-lite with structured output (JSON schema, temperature 0) to handle the
# seven+ OCR layouts without per-layout code.
#
# The model receives the page OCR text plus the *expected* candidate names
# from step 5 (per kieskring × lijst), so extraction is alignment, not
# discovery — the model matches what it sees against known candidates.
# Every block is validated: sum(candidate votes) must equal stemcijfer.
#
# Two modes:
#   --targeted (default): only re-parse pages where the rule-based step 6
#       found checksum failures or missing blocks. Existing good rows are
#       kept; bad rows are replaced with LLM output where it passes checksum.
#   --full: re-parse all vote-table pages with the LLM, replacing all
#       rule-based rows.
#
# PAID API: uses GOOGLE_API_KEY from examples/.env. Text-only; well under $1
# at flash-lite prices. Ask the user before spending API calls (per standing
# agreement in the project CLAUDE.md).
#
# Usage:
#   uv run python code/data_wrangling/delpher/delpher_step6b_llm_parse_uitslagen.py
#   uv run python code/data_wrangling/delpher/delpher_step6b_llm_parse_uitslagen.py --full
#   uv run python code/data_wrangling/delpher/delpher_step6b_llm_parse_uitslagen.py --dry-run
#   uv run python code/data_wrangling/delpher/delpher_step6b_llm_parse_uitslagen.py --limit 5
# =============================================================================
import argparse
import asyncio
import json
import os
import re
import sys
import time
from collections import defaultdict

import aiohttp
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter

DB_PATH = "./data/delpher/delpher.duckdb"
ENV_PATH = "./examples/.env"
MODEL = "gemini-3.1-flash-lite"
API_URL = ("https://generativelanguage.googleapis.com/v1beta/models/"
           f"{MODEL}:generateContent")

RATE = 2.0            # requests/second
CONCURRENCY = 8
MAX_ATTEMPTS = 4
MAX_OUTPUT_TOKENS = 32768

# ---------------------------------------------------------------------------
# main results issue per election (same as step 6)
# ---------------------------------------------------------------------------
MAIN_ISSUES = {
    1918: "MMKB08:000179144:mpeg21",
    1922: "MMKB08:000178343:mpeg21",
    1925: "MMKB08:000181037:mpeg21",
    1929: "MMKB08:000161457:mpeg21",
    1933: "MMKB08:000181270:mpeg21",
    1937: "MMKB08:000168915:mpeg21",
}

# ---------------------------------------------------------------------------
# canonical kieskring names
# ---------------------------------------------------------------------------
ROMAN = {"i": 1, "v": 5, "x": 10}

KIESKRING_NAMES: dict[str, int] = {
    "hertogenbosch": 1, "tilburg": 2, "arnhem": 3, "nijmegen": 4,
    "rotterdam": 5, "gravenhage": 6, "leiden": 7, "dordrecht": 8,
    "amsterdam": 9, "helder": 10, "haarlem": 11, "middelburg": 12,
    "utrecht": 13, "leeuwarden": 14, "zwolle": 15, "groningen": 16,
    "assen": 17, "maastricht": 18,
}

KIESKRING_LABELS = {
    1: "I. 's-Hertogenbosch", 2: "II. Tilburg", 3: "III. Arnhem",
    4: "IV. Nijmegen", 5: "V. Rotterdam", 6: "VI. 's-Gravenhage",
    7: "VII. Leiden", 8: "VIII. Dordrecht", 9: "IX. Amsterdam",
    10: "X. Den Helder", 11: "XI. Haarlem", 12: "XII. Middelburg",
    13: "XIII. Utrecht", 14: "XIV. Leeuwarden", 15: "XV. Zwolle",
    16: "XVI. Groningen", 17: "XVII. Assen", 18: "XVIII. Maastricht",
}

# ---------------------------------------------------------------------------
# section-boundary regexes (reused from step 6)
# ---------------------------------------------------------------------------
TABLE_HDR_RE = re.compile(
    r"Naam en voorletters der candidaten in de volgorde", re.IGNORECASE)
SECTION_END_RE = re.compile(
    r"verbinding overeenkomstig artikel|lijstengroepen (zijn )?gevormd",
    re.IGNORECASE)
KIESDEELER_RE = re.compile(r"kiesdeeler", re.IGNORECASE)
VASTSTELLING_RE = re.compile(
    r"verklaart\s+(alsnu\s+)?benoemd\s+tot\s+leden", re.IGNORECASE)
ART105_RE = re.compile(r"artikel 10[45]|rangschikt het centraal",
                       re.IGNORECASE)


# ---------------------------------------------------------------------------
# name-matching helpers (reused from step 6)
# ---------------------------------------------------------------------------
def nrm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def edit_distance_le(a: str, b: str, k: int) -> bool:
    """True when levenshtein(a, b) <= k (banded DP, k is 1 or 2)."""
    if abs(len(a) - len(b)) > k:
        return False
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1,
                           prev[j - 1] + (ca != cb)))
        if min(cur) > k:
            return False
        prev = cur
    return prev[-1] <= k


def name_matches(expected_surname: str, line_name: str) -> bool:
    """Tolerant OCR name comparison: normalized containment either way, a
    shared prefix of >= 5 chars (>= 4 for short names), or a small edit
    distance."""
    a, b = nrm(expected_surname), nrm(line_name)
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    k = min(len(a), len(b), 5)
    if k >= 4 and a[:k] == b[:k]:
        return True
    n = min(len(a), len(b))
    if n >= 6 and edit_distance_le(a, b, 2 if n >= 9 else 1):
        return True
    return False


# ---------------------------------------------------------------------------
# JSON schemas for Gemini structured output
# ---------------------------------------------------------------------------
VOTE_BLOCKS_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "blocks": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "kieskring": {
                        "type": "INTEGER",
                        "description": "Kieskring number 1-18"
                    },
                    "lijst": {
                        "type": "STRING",
                        "description": "Lijst number, e.g. '3' or '3a'"
                    },
                    "continues_previous_page": {
                        "type": "BOOLEAN",
                        "description": "True if this block continues from "
                                       "the previous page"
                    },
                    "candidates": {
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "name": {
                                    "type": "STRING",
                                    "description": "Candidate name as "
                                                   "printed (surname, "
                                                   "initials/titles)"
                                },
                                "votes": {
                                    "type": "INTEGER",
                                    "nullable": True,
                                    "description": "Preference votes; "
                                                   "null for a dash/—"
                                }
                            },
                            "required": ["name", "votes"]
                        }
                    },
                    "stemcijfer": {
                        "type": "INTEGER",
                        "nullable": True,
                        "description": "Total stemcijfer for the lijst; "
                                       "null when the block continues on "
                                       "the next page"
                    }
                },
                "required": ["kieskring", "lijst", "continues_previous_page",
                             "candidates", "stemcijfer"]
            }
        }
    },
    "required": ["blocks"]
}

GEKOZEN_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "members": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "name": {
                        "type": "STRING",
                        "description": "Surname as printed"
                    },
                    "initials": {
                        "type": "STRING",
                        "description": "Initials and titles, "
                                       "e.g. 'mr. P. J. M.'"
                    },
                    "residence": {
                        "type": "STRING",
                        "description": "Residence as printed"
                    }
                },
                "required": ["name", "initials", "residence"]
            }
        }
    },
    "required": ["members"]
}


# ---------------------------------------------------------------------------
# prompt templates
# ---------------------------------------------------------------------------
def build_vote_prompt(year: int, kk_names: str,
                      candidate_refs: str) -> str:
    """Build the prompt for vote-table parsing with expected-structure
    context anchored to the step 5 kandidatenlijsten."""
    return f"""\
This is OCR text of a page from the Nederlandsche Staatscourant (Dutch
government gazette, {year} election results). It contains vote tables from the
"BESLUIT van het Centraal Stembureau" or "PROCES-VERBAAL" section.

The tables have columns: kieskring | lijst number | candidate name | votes |
stemcijfer. Some pages print three physical columns side by side; others print
a single column. Read every column top to bottom, then the next column to the
right.

Kieskring names and numbers:
{kk_names}

Expected candidate lists per kieskring (the official order from the candidate
publication; match against this — every candidate printed should align to one
of these positions):
{candidate_refs}

Extract every vote block (kieskring → lijst → candidates with votes →
stemcijfer). Rules:
- "—" or "–" (dash) in the votes column = no preference votes (votes: null).
- A block's stemcijfer is the total printed at the end of the block. When a
  block is split across pages, set stemcijfer: null on the first page and
  continues_previous_page: true on the second.
- Lijst numbers may have a letter suffix (e.g. "3a", "4b"). Keep them exactly
  as printed.
- OCR may split a candidate name across lines or mangle diacritics. Match
  against the expected candidate lists by surname and position.
- Skip boilerplate text, repeated table headers, and the "Verdeeling"
  (seat-allocation) section."""


def build_gekozen_prompt(year: int) -> str:
    """Build the prompt for elected-members parsing."""
    return f"""\
This is OCR text from the "Vaststelling van den uitslag" section of the
Nederlandsche Staatscourant ({year} election). It lists the 100 members
declared elected to the Tweede Kamer.

The table has columns: surname | initials/titles | residence. Names are in
alphabetical order. Extract every member. The table may be printed in two or
three columns side by side — read each column top to bottom.

Output each member as: surname (exactly as printed), initials (with titles
like mr., dr., jhr. if present), and residence."""


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL_PROGRESS = """
CREATE TABLE IF NOT EXISTS llm_parse_pages (
    issue_urn      TEXT,
    page_no        INTEGER,
    model          TEXT,
    section        TEXT,      -- 'vote_table' or 'gekozen'
    response_json  TEXT,      -- raw JSON response (for offline debugging)
    prompt_tokens  INTEGER,
    output_tokens  INTEGER,
    fetched_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (issue_urn, page_no, section)
);
"""

DDL_OUTPUT = """
CREATE TABLE IF NOT EXISTS voorkeur_stemmen (
    year          INTEGER,
    kieskring_no  INTEGER,
    lijst_no      TEXT,
    positie       INTEGER,
    name_raw      TEXT,
    name_ocr      TEXT,
    votes         INTEGER,
    checksum_ok   BOOLEAN,
    issue_urn     TEXT,
    page_no       INTEGER
);
CREATE TABLE IF NOT EXISTS lijst_uitslagen (
    year          INTEGER,
    kieskring_no  INTEGER,
    lijst_no      TEXT,
    stemcijfer    INTEGER,
    sum_votes     INTEGER,
    n_candidates  INTEGER,
    n_matched     INTEGER,
    checksum_ok   BOOLEAN,
    issue_urn     TEXT,
    page_no       INTEGER
);
CREATE TABLE IF NOT EXISTS gekozen (
    year          INTEGER,
    volgorde      INTEGER,
    name_raw      TEXT,
    initials      TEXT,
    residence     TEXT,
    issue_urn     TEXT,
    page_no       INTEGER
);
CREATE TABLE IF NOT EXISTS uitslag_issues (
    year      INTEGER,
    issue_urn TEXT,
    page_no   INTEGER,
    reason    TEXT,
    detail    TEXT
);
"""


# ---------------------------------------------------------------------------
# API key
# ---------------------------------------------------------------------------
def api_key() -> str:
    with open(ENV_PATH) as f:
        for line in f:
            if line.startswith("GOOGLE_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit(f"GOOGLE_API_KEY not found in {ENV_PATH}")


# ---------------------------------------------------------------------------
# page selection
# ---------------------------------------------------------------------------
def find_vote_section(con, year: str) -> tuple[str, list[tuple[int, str]]]:
    """Return (issue_urn, [(page_no, ocr_text), ...]) for the vote-table
    section of an election year."""
    urn = MAIN_ISSUES[year]
    pages = con.execute(
        "SELECT page_no, ocr_text FROM ocr_pages WHERE issue_urn = ? "
        "ORDER BY page_no", [urn]).fetchall()

    start = next((p for p, t in pages if TABLE_HDR_RE.search(t)), None)
    stop = next((p for p, t in pages if SECTION_END_RE.search(t)), None)
    if stop is None:
        stop = next((p for p, t in pages if KIESDEELER_RE.search(t)), None)
    if start is None or stop is None:
        return urn, []
    return urn, [(p, t) for p, t in pages if start <= p <= stop]


def find_gekozen_section(con, year: str) -> tuple[str, list[tuple[int, str]]]:
    """Return (issue_urn, [(page_no, ocr_text), ...]) for the elected-members
    section."""
    urn = MAIN_ISSUES[year]
    pages = con.execute(
        "SELECT page_no, ocr_text FROM ocr_pages WHERE issue_urn = ? "
        "ORDER BY page_no", [urn]).fetchall()

    start = next((p for p, t in pages if VASTSTELLING_RE.search(t)), None)
    if start is None:
        return urn, []
    # elected table spans at most 4 pages after the header
    return urn, [(p, t) for p, t in pages
                 if start <= p <= start + 3]


def targeted_pages(con, year: str,
                   all_vote_pages: list[tuple[int, str]]
                   ) -> set[int]:
    """Return page numbers to re-parse: those with checksum failures in the
    existing step 6 output. Adjacent pages are NOT included — the LLM handles
    cross-page blocks via continues_previous_page stitching."""
    urn = MAIN_ISSUES[year]
    try:
        bad = {r[0] for r in con.execute("""
            SELECT DISTINCT page_no FROM lijst_uitslagen
            WHERE issue_urn = ? AND NOT checksum_ok
        """, [urn]).fetchall()}
    except duckdb.CatalogException:
        return {p for p, _ in all_vote_pages}  # no step 6 output yet

    return bad


# ---------------------------------------------------------------------------
# candidate reference builder
# ---------------------------------------------------------------------------
def build_candidate_refs(con, year: int) -> str:
    """Build a compact reference of expected candidate lists per kieskring
    for the given year, to anchor the LLM."""
    rows = con.execute("""
        SELECT kieskring_no, lijst_no, positie, surname, initials
        FROM kandidatenlijsten
        WHERE year = ?
        ORDER BY kieskring_no, lijst_no, positie
    """, [year]).fetchall()

    by_kk: dict[int, dict[str, list[str]]] = defaultdict(
        lambda: defaultdict(list))
    for kk, ln, pos, sn, ini in rows:
        label = f"{sn}, {ini}" if ini else sn
        by_kk[kk][ln].append(label)

    parts = []
    for kk in sorted(by_kk):
        label = KIESKRING_LABELS.get(kk, f"Kieskring {kk}")
        parts.append(f"\n{label}:")
        for ln in sorted(by_kk[kk], key=_lijst_sort_key):
            names = ", ".join(by_kk[kk][ln])
            parts.append(f"  Lijst {ln}: {names}")
    return "\n".join(parts)


def _lijst_sort_key(ln: str) -> tuple:
    """Sort lijst numbers: numeric part first, then letter suffix."""
    m = re.match(r"(\d+)([a-z]*)", ln)
    if m:
        return (int(m.group(1)), m.group(2))
    return (999, ln)


# ---------------------------------------------------------------------------
# Gemini API call
# ---------------------------------------------------------------------------
async def transcribe_page(
        session: aiohttp.ClientSession, bucket: TokenBucketRateLimiter,
        key: str, prompt: str, schema: dict,
        temperature: float = 0.0
) -> tuple[str, int, int]:
    """Send a text-only prompt to Gemini with structured output schema.
    Returns (response_json_string, prompt_tokens, output_tokens)."""
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": MAX_OUTPUT_TOKENS,
            "response_mime_type": "application/json",
            "response_schema": schema,
        },
    }
    last_err = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        await bucket.acquire()
        try:
            async with session.post(
                    API_URL, json=body,
                    headers={"x-goog-api-key": key}) as resp:
                if resp.status in (429, 500, 502, 503, 504):
                    last_err = f"HTTP {resp.status}"
                    await asyncio.sleep(5 * attempt)
                    continue
                resp.raise_for_status()
                out = await resp.json()
            cand = out["candidates"][0]
            text = "".join(p.get("text", "")
                           for p in cand.get("content", {}).get("parts", []))
            usage = out.get("usageMetadata", {})
            return (text,
                    usage.get("promptTokenCount", 0),
                    usage.get("candidatesTokenCount", 0))
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_err = repr(e)
            await asyncio.sleep(5 * attempt)
    raise RuntimeError(f"gave up after {MAX_ATTEMPTS} attempts: {last_err}")


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------
def validate_and_store(con, year: int, urn: str,
                        all_blocks: list[dict],
                        expected: dict,
                        problems: list,
                        page_map: dict[int, int] | None = None
                        ) -> tuple[list, list]:
    """Validate blocks from all pages of one year against kandidatenlijsten.

    - Aligns returned candidates to expected positions per (kieskring, lijst)
    - Computes checksum: sum(votes) == stemcijfer
    - Writes to voorkeur_stemmen and lijst_uitslagen
    - Logs issues to uitslag_issues

    page_map maps block index -> page_no; if None, page_no from the block's
    own page_no field is used."""
    vk_rows, lu_rows = [], []
    for i, blk in enumerate(all_blocks):
        kk = blk.get("kieskring")
        ln = blk.get("lijst", "").strip().lower()
        total = blk.get("stemcijfer")
        cands = blk.get("candidates", [])
        pg = page_map[i] if page_map else blk.get("page_no")

        # normalize lijst number
        ln_clean = re.sub(r"[^a-z0-9]", "", ln)

        expected_cands = expected.get((year, kk, ln_clean), [])
        if not expected_cands:
            # try without letter suffix
            ln_num = re.match(r"(\d+)", ln_clean)
            if ln_num:
                for (yr2, kk2, ln2), v in expected.items():
                    if yr2 == year and kk2 == kk and ln2.startswith(ln_num.group(1)):
                        expected_cands = v
                        ln_clean = ln2
                        break
        if not expected_cands:
            problems.append((year, urn, pg, "unknown lijst",
                             f"kk{kk} lijst {ln}"))
            continue

        # align candidates to expected positions
        got: dict[int, tuple[str, int | None]] = {}
        exp_idx = 0
        for c in cands:
            cname = (c.get("name") or "").strip()
            cvotes = c.get("votes")
            if cname == "" and cvotes is not None and exp_idx > 0:
                # bare number: might be a continuation vote
                prev_pos = expected_cands[exp_idx - 1][0] if exp_idx > 0 else None
                if prev_pos is not None and prev_pos in got:
                    prev_name = got[prev_pos][0]
                    got[prev_pos] = (prev_name, cvotes)
                continue
            if not cname:
                continue
            # match against expected candidates
            matched = False
            surname_part = cname.split(",")[0].strip() if cname else ""
            for j in range(exp_idx, min(exp_idx + 2, len(expected_cands))):
                pos, sn, nr = expected_cands[j]
                if surname_part and name_matches(sn, surname_part):
                    if j == exp_idx + 1:
                        problems.append((year, urn, pg,
                                         "candidate row skipped by LLM",
                                         f"kk{kk} lijst {ln_clean} "
                                         f"pos {expected_cands[exp_idx][0]} "
                                         f"({expected_cands[exp_idx][1]})"))
                    got[pos] = (cname, cvotes)
                    exp_idx = j + 1
                    matched = True
                    break
            if not matched:
                # try matching any remaining expected candidate
                for j in range(exp_idx, len(expected_cands)):
                    pos, sn, nr = expected_cands[j]
                    if surname_part and name_matches(sn, surname_part):
                        problems.append((year, urn, pg,
                                         "candidate matched out of order",
                                         f"kk{kk} lijst {ln_clean}: "
                                         f"'{cname}' at pos {pos} "
                                         f"(expected pos {expected_cands[exp_idx][0]})"))
                        got[pos] = (cname, cvotes)
                        exp_idx = j + 1
                        matched = True
                        break
            if not matched:
                problems.append((year, urn, pg, "candidate not matched",
                                 f"kk{kk} lijst {ln_clean}: '{cname}'"))

        sumv = sum(v for _, v in got.values() if v is not None)
        ok = (total is not None and sumv == total
              and len(got) == len(expected_cands))

        # write rows
        for pos, sn, nr in expected_cands:
            name_ocr, votes = got.get(pos, (None, None))
            vk_rows.append((year, kk, ln_clean, pos, nr,
                            name_ocr, votes, ok, urn, pg))
        lu_rows.append((year, kk, ln_clean, total, sumv,
                        len(expected_cands), len(got), ok, urn, pg))

        if not ok:
            problems.append((year, urn, pg, "block checksum failed",
                             f"kk{kk} lijst {ln_clean}: "
                             f"sum={sumv} total={total} "
                             f"matched={len(got)}/{len(expected_cands)}"))

    return vk_rows, lu_rows


def validate_gekozen(members: list[dict], year: int, urn: str,
                     pg: int) -> list[tuple]:
    """Validate and format elected members."""
    rows = []
    for i, m in enumerate(members):
        name = (m.get("name") or "").strip(" .,")
        initials = (m.get("initials") or "").strip(" .,")
        residence = (m.get("residence") or "").strip(" .,")
        if name and initials:
            rows.append((year, i + 1, name, initials, residence, urn, pg))
    return rows


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
async def main() -> None:
    ap = argparse.ArgumentParser(
        description="LLM-based parsing of Staatscourant vote tables")
    ap.add_argument("--full", action="store_true",
                    help="Re-parse ALL vote-table pages (default: targeted)")
    ap.add_argument("--targeted", action="store_true", default=True,
                    help="Only re-parse pages with rule-based issues "
                         "(default)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show what would be done without API calls")
    ap.add_argument("--limit", type=int, default=None,
                    help="Limit number of pages to process")
    ap.add_argument("--gekozen", action="store_true",
                    help="Also LLM-parse the elected members section "
                         "(experimental; the rule-based parser is more "
                         "reliable for this)")
    args = ap.parse_args()
    if args.full:
        args.targeted = False

    con = duckdb.connect(DB_PATH)
    for stmt in DDL_PROGRESS.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)
    for stmt in DDL_OUTPUT.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)

    # load expected candidate lists
    expected: dict[tuple, list] = defaultdict(list)
    for kk, ln, pos, sn, nr, yr in con.execute("""
            SELECT kieskring_no, lijst_no, positie, surname, name_raw, year
            FROM kandidatenlijsten
            ORDER BY year, kieskring_no, lijst_no, positie
    """).fetchall():
        expected[(yr, kk, ln)].append((pos, sn, nr))

    kk_names_str = "\n".join(f"  {v[0]}. {v[1]}"
                             for v in sorted(KIESKRING_LABELS.items()))

    # ---- collect pages to process ----
    all_tasks: list[dict] = []   # {year, urn, page_no, ocr_text, section}

    for year in sorted(MAIN_ISSUES):
        urn, vote_pages = find_vote_section(con, year)
        if not vote_pages:
            print(f"{year}: vote section not found in {urn}")
            continue

        candidate_refs = build_candidate_refs(con, year)

        if args.targeted:
            target = targeted_pages(con, year, vote_pages)
            pages_to_do = [(p, t) for p, t in vote_pages if p in target]
            print(f"{year}: {len(pages_to_do)}/{len(vote_pages)} pages "
                  f"targeted for re-parse")
        else:
            pages_to_do = vote_pages
            print(f"{year}: {len(pages_to_do)} vote-table pages")

        for pg, txt in pages_to_do:
            # skip if already parsed
            already = con.execute(
                "SELECT 1 FROM llm_parse_pages "
                "WHERE issue_urn = ? AND page_no = ? AND section = 'vote_table'",
                [urn, pg]).fetchone()
            if already:
                continue
            prompt = build_vote_prompt(year, kk_names_str, candidate_refs)
            full_prompt = f"{prompt}\n\n--- PAGE TEXT ---\n{txt}"
            all_tasks.append({
                "year": year, "urn": urn, "page_no": pg,
                "prompt": full_prompt, "section": "vote_table",
                "schema": VOTE_BLOCKS_SCHEMA,
            })

        # gekozen section (opt-in only)
        if args.gekozen:
            gurn, gekozen_pages = find_gekozen_section(con, year)
            for pg, txt in gekozen_pages:
                already = con.execute(
                    "SELECT 1 FROM llm_parse_pages "
                    "WHERE issue_urn = ? AND page_no = ? "
                    "AND section = 'gekozen'",
                    [gurn, pg]).fetchone()
                if already:
                    continue
                prompt = build_gekozen_prompt(year)
                full_prompt = f"{prompt}\n\n--- PAGE TEXT ---\n{txt}"
                all_tasks.append({
                    "year": year, "urn": gurn, "page_no": pg,
                    "prompt": full_prompt, "section": "gekozen",
                    "schema": GEKOZEN_SCHEMA,
                })

    if args.limit:
        all_tasks = all_tasks[:args.limit]

    if not all_tasks:
        print("no pages to process (all done or no sections found)")
        print_quality(con)
        con.close()
        return

    total_input_est = sum(len(t["prompt"]) // 4 for t in all_tasks)
    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}"
          f"{len(all_tasks)} pages to process "
          f"(~{total_input_est:,} input tokens estimated)")
    vote_pages_ct = sum(1 for t in all_tasks if t["section"] == "vote_table")
    gekozen_ct = sum(1 for t in all_tasks if t["section"] == "gekozen")
    print(f"  vote_table: {vote_pages_ct}  gekozen: {gekozen_ct}")

    if args.dry_run:
        con.close()
        return

    print("\n⚠️  This will call the Gemini API (~${:.2f} estimated). "
          "Proceed? (y/N): ".format(
              total_input_est * 0.075 / 1_000_000), end="", flush=True)
    consent = input().strip().lower()

    if consent != "y":
        print("aborted")
        con.close()
        return

    key = api_key()
    bucket = TokenBucketRateLimiter(RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    lock = asyncio.Lock()
    stats = {"done": 0, "fail": 0, "in_tok": 0, "out_tok": 0}
    timeout = aiohttp.ClientTimeout(total=600, connect=20)

    # accumulate results per year for stitching
    year_blocks: dict[int, list[dict]] = defaultdict(list)
    year_gekozen: dict[int, list[tuple]] = defaultdict(list)
    problems: list[tuple] = []

    async def process(task: dict):
        nonlocal stats
        async with sem:
            try:
                resp_json, in_tok, out_tok = await transcribe_page(
                    session, bucket, key, task["prompt"], task["schema"])
            except Exception as e:
                async with lock:
                    stats["fail"] += 1
                print(f"  {task['urn']} p{task['page_no']}: FAIL {e}")
                return

            async with lock:
                stats["done"] += 1
                stats["in_tok"] += in_tok
                stats["out_tok"] += out_tok
                con.execute(
                    "INSERT OR REPLACE INTO llm_parse_pages "
                    "(issue_urn, page_no, model, section, response_json, "
                    " prompt_tokens, output_tokens) "
                    "VALUES (?,?,?,?,?,?,?)",
                    [task["urn"], task["page_no"], MODEL, task["section"],
                     resp_json, in_tok, out_tok])

                if task["section"] == "vote_table":
                    try:
                        parsed = json.loads(resp_json)
                    except json.JSONDecodeError:
                        problems.append((task["year"], task["urn"],
                                         task["page_no"],
                                         "json parse error",
                                         resp_json[:200]))
                        return
                    blocks = parsed.get("blocks", [])
                    # tag blocks with (page_no, idx) for stable sorting
                    for idx, blk in enumerate(blocks):
                        blk["_page_no"] = task["page_no"]
                        blk["_idx"] = idx
                    year_blocks[task["year"]].extend(blocks)
                elif task["section"] == "gekozen":
                    try:
                        parsed = json.loads(resp_json)
                    except json.JSONDecodeError:
                        problems.append((task["year"], task["urn"],
                                         task["page_no"],
                                         "json parse error (gekozen)",
                                         resp_json[:200]))
                        return
                    members = parsed.get("members", [])
                    rows = validate_gekozen(
                        members, task["year"], task["urn"], task["page_no"])
                    year_gekozen[task["year"]].extend(rows)

                if stats["done"] % 10 == 0:
                    print(f"  {stats['done']}/{len(all_tasks)} pages "
                          f"({stats['in_tok']:,} in / "
                          f"{stats['out_tok']:,} out tokens)")

    async with aiohttp.ClientSession(timeout=timeout) as session:
        await asyncio.gather(*[process(t) for t in all_tasks])

    print(f"\nAPI calls: {stats['done']} ok + {stats['fail']} failed, "
          f"{stats['in_tok']:,} in / {stats['out_tok']:,} out tokens")

    # ---- stitch and validate vote blocks ----
    for year in sorted(year_blocks):
        urn = MAIN_ISSUES[year]
        blocks = sorted(year_blocks[year], key=lambda b: (
            b.get("_page_no", 0),
            b.get("_idx", 0),
        ))

        # stitch: merge blocks that continue across pages
        stitched = []
        pending = None
        for blk in blocks:
            if blk.get("continues_previous_page"):
                if pending is not None:
                    pending["candidates"].extend(blk.get("candidates", []))
                    if blk.get("stemcijfer") is not None:
                        pending["stemcijfer"] = blk["stemcijfer"]
                        stitched.append(pending)
                        pending = None
                    # else: still no stemcijfer — keep pending
                else:
                    # no pending block to continue; treat as new
                    stitched.append(blk)
            else:
                if pending is not None:
                    # previous block never got its stemcijfer; close it
                    stitched.append(pending)
                if blk.get("stemcijfer") is None:
                    pending = blk
                else:
                    stitched.append(blk)
                    pending = None
        if pending is not None:
            stitched.append(pending)

        # remove old rows for pages we re-parsed
        re_parsed_pages = set()
        for blk in blocks:
            pg = blk.get("_page_no")
            if pg is not None:
                re_parsed_pages.add(pg)

        for pg in re_parsed_pages:
            con.execute(
                "DELETE FROM voorkeur_stemmen "
                "WHERE year = ? AND issue_urn = ? AND page_no = ?",
                [year, urn, pg])
            con.execute(
                "DELETE FROM lijst_uitslagen "
                "WHERE year = ? AND issue_urn = ? AND page_no = ?",
                [year, urn, pg])

        # build page_map for validation (maps stitched block index -> page_no)
        page_map = {}
        for i, blk in enumerate(stitched):
            page_map[i] = blk.get("_page_no")

        vk_rows, lu_rows = validate_and_store(
            con, year, urn, stitched, expected, problems, page_map)

        if vk_rows:
            con.executemany(
                "INSERT INTO voorkeur_stemmen VALUES (?,?,?,?,?,?,?,?,?,?)",
                vk_rows)
        if lu_rows:
            con.executemany(
                "INSERT INTO lijst_uitslagen VALUES (?,?,?,?,?,?,?,?,?,?)",
                lu_rows)
        print(f"{year}: {len(lu_rows)} blocks, {len(vk_rows)} candidate rows")

    # ---- store gekozen ----
    for year in sorted(year_gekozen):
        rows = year_gekozen[year]
        if rows:
            # deduplicate by (year, name, initials) — a page might be
            # processed twice or overlap with the rule-based output
            seen = set()
            deduped = []
            for r in rows:
                key = (r[0], r[2], r[3])
                if key not in seen:
                    seen.add(key)
                    deduped.append(r)
            # replace all gekozen rows for this year
            con.execute("DELETE FROM gekozen WHERE year = ?", [year])
            con.executemany(
                "INSERT INTO gekozen VALUES (?,?,?,?,?,?,?)", deduped)
            print(f"{year}: {len(deduped)} elected members")

    # store problems
    if problems:
        con.executemany(
            "INSERT INTO uitslag_issues VALUES (?,?,?,?,?)", problems)

    print_quality(con)
    con.close()


def print_quality(con) -> None:
    """Print quality metrics from the output tables."""
    try:
        print("\nblock checksum quality:")
        print(con.execute("""
            SELECT year, COUNT(*) AS blocks,
                   SUM(checksum_ok::INT) AS ok,
                   ROUND(AVG(checksum_ok::INT), 3) AS ok_rate
            FROM lijst_uitslagen GROUP BY 1 ORDER BY 1
        """).fetchdf().to_string(index=False))
    except Exception:
        print("  (no lijst_uitslagen data yet)")

    try:
        print("\nelected counts (expect 100):")
        print(con.execute(
            "SELECT year, COUNT(*) FROM gekozen GROUP BY 1 ORDER BY 1"
        ).fetchdf().to_string(index=False))
    except Exception:
        print("  (no gekozen data yet)")

    try:
        issues = con.execute("""
            SELECT year, reason, COUNT(*) FROM uitslag_issues
            GROUP BY 1,2 ORDER BY 1,3 DESC
        """).fetchdf()
        if len(issues):
            print("\nissue summary:")
            print(issues.to_string(index=False))
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
