# =============================================================================
# delpher_step6_parse_uitslagen.py  [DELPHER PIPELINE - STEP 6]
# Input:  data/delpher/delpher.duckdb  (ocr_pages + target_pages +
#                                       kandidatenlijsten from step 5)
# Output: data/delpher/delpher.duckdb
#           voorkeur_stemmen — preference votes per candidate × kieskring ×
#                              lijst (aligned to kandidatenlijsten positions)
#           lijst_uitslagen  — stemcijfer per kieskring × lijst, with the
#                              checksum sum(preference votes) == stemcijfer
#           gekozen          — the members declared elected ("Vaststelling
#                              van den uitslag"), name + residence
#           uitslag_issues   — audit log of blocks/lines the parser rejected
#
# Sources inside the results issues (all six elections share the layout):
#   1. The BESLUIT art. 97 / PROCES-VERBAAL vote tables: per kieskring, per
#      lijst, every candidate with preference votes, closed by the lijst's
#      stemcijfer. Candidate order equals the official candidate list, so
#      parsing is an ALIGNMENT against kandidatenlijsten (step 5), not free
#      text parsing; sum(votes) == stemcijfer is verified per block.
#      Section bounds: first page with the table header up to the first page
#      of the seat-allocation math (recognised by the word "kiesdeeler").
#   2. "Vaststelling van den uitslag": the '... verklaart benoemd tot leden'
#      table (surname | initials | residence), 100 members per election.
#
# The seat-allocation ("Verdeeling") tables, the Aanwijzing duplicates and
# the art. 105 ranking of non-elected candidates are NOT parsed here; the
# elected set and the vote counts above cover the panel's needs.
#
# Pure offline parsing — free to rerun; tables rebuilt from ocr_pages.
#
# Usage:
#   uv run python code/data_wrangling/delpher/delpher_step6_parse_uitslagen.py
# =============================================================================
import re
from collections import defaultdict

import duckdb

DB_PATH = "./data/delpher/delpher.duckdb"

# main results issue per election (supplements hold per-group revisions and
# are not needed for the elected set or the vote tables)
MAIN_ISSUES = {
    1918: "MMKB08:000179144:mpeg21",
    1922: "MMKB08:000178343:mpeg21",
    1925: "MMKB08:000181037:mpeg21",
    1929: "MMKB08:000161457:mpeg21",
    1933: "MMKB08:000181270:mpeg21",
    1937: "MMKB08:000168915:mpeg21",
}

ROMAN = {"i": 1, "v": 5, "x": 10}
KIESKRING_NAMES = {
    "hertogenbosch": 1, "tilburg": 2, "arnhem": 3, "nijmegen": 4,
    "rotterdam": 5, "gravenhage": 6, "leiden": 7, "dordrecht": 8,
    "amsterdam": 9, "helder": 10, "haarlem": 11, "middelburg": 12,
    "utrecht": 13, "leeuwarden": 14, "zwolle": 15, "groningen": 16,
    "assen": 17, "maastricht": 18,
}

TABLE_HDR_RE = re.compile(
    r"Naam en voorletters der candidaten in de volgorde", re.IGNORECASE)
KIESDEELER_RE = re.compile(r"kiesdeeler", re.IGNORECASE)
VASTSTELLING_RE = re.compile(r"verklaart\s+(alsnu\s+)?benoemd\s+tot\s+leden",
                             re.IGNORECASE)
ART105_RE = re.compile(r"artikel 10[45]|rangschikt het centraal",
                       re.IGNORECASE)

# kieskring context line: "III. Arnhem." / "Kieskring III (Arnhem)" /
# "I. 's Her-togen-bosch. (Vervolg.)" — recognised by the canonical name
KK_LINE_RE = re.compile(r"^\W{0,3}(?:Kieskring\s+)?([IVXY]+|\d{1,2})\s*[.,]",
                        re.IGNORECASE)

# lijst block opener: "9. | Idenburg, A. W. F. | 3 007" / "3a. Göbel, J. | 520"
LIJST_OPEN_RE = re.compile(r"^(\d{1,2})\s*([a-z]?)\s*[.,]\s*(.*)$")

NUM_TAIL_RE = re.compile(r"([\d][\d\s.]*?)\s*$")
DASH_VOTES_RE = re.compile(r"[—–-]\s*$")


def nrm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def roman_to_int(s: str) -> int | None:
    s = s.lower().replace("y", "v")
    if s.isdigit():
        n = int(s)
        return n if 1 <= n <= 18 else None
    total, prev = 0, 0
    for ch in reversed(s):
        v = ROMAN.get(ch)
        if v is None:
            return None
        total = total - v if v < prev else total + v
        prev = max(prev, v)
    return total if 1 <= total <= 18 else None


def parse_number(s: str) -> int | None:
    d = re.sub(r"[^\d]", "", s)
    if not d or len(d) > 6:      # largest kieskring stemcijfer is 5 digits
        return None
    return int(d)


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
    distance (both sides are OCR: 'Sneevliet' vs 'Snevliet')."""
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


DDL = """
CREATE OR REPLACE TABLE voorkeur_stemmen (
    year          INTEGER,
    kieskring_no  INTEGER,
    lijst_no      TEXT,
    positie       INTEGER,
    name_raw      TEXT,      -- name as printed in the kandidatenlijst
    name_ocr      TEXT,      -- name as read in the vote table (cross-check)
    votes         INTEGER,   -- NULL when the table prints a dash
    checksum_ok   BOOLEAN,   -- block passed sum(votes) == stemcijfer
    issue_urn     TEXT,
    page_no       INTEGER
);
CREATE OR REPLACE TABLE lijst_uitslagen (
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
CREATE OR REPLACE TABLE gekozen (
    year          INTEGER,
    volgorde      INTEGER,   -- row order in the alphabetical table
    name_raw      TEXT,
    initials      TEXT,
    residence     TEXT,
    issue_urn     TEXT,
    page_no       INTEGER
);
CREATE OR REPLACE TABLE uitslag_issues (
    year      INTEGER,
    issue_urn TEXT,
    page_no   INTEGER,
    reason    TEXT,
    detail    TEXT
);
"""


def load_pages(con, year):
    urn = MAIN_ISSUES[year]
    return urn, con.execute(
        "SELECT page_no, ocr_text FROM ocr_pages WHERE issue_urn = ? "
        "ORDER BY page_no", [urn]).fetchall()


def clean(line: str) -> list[str]:
    cells = [c.strip() for c in line.split("|")]
    return [c for c in cells if c]


# ---------------------------------------------------------------------------
# 1. vote tables (BESLUIT / proces-verbaal)
#
# All six issues print the same 5-column table (kieskring | lijst | candidate
# | votes | stemcijfer) but the OCR renders it differently per year:
#   1918/1922  one candidate per row, lijst as "3a.", stemcijfer alone on a
#              closing row in the last column
#   1929       lijst number WITHOUT trailing period ("| 2 | Wijnkoop, D. |")
#   1933       surname and initials in separate cells; the kieskring name is
#              hyphen-fragmented vertically over several rows ("'s Her-" /
#              "togen-" / "bosch.")
#   1937       stemcijfer printed on the OPENING row of each lijst
#   1925       some pages merge a whole lijst into ONE row: every surname in
#              one cell, every vote (space-separated) in another
# Lines with pipes are parsed cell-wise below; pipe-less lines fall back to
# the flat-text path.
# ---------------------------------------------------------------------------
LIJST_CELL_RE = re.compile(r"^(\d{1,2})\s*([a-z]?)\s*[.,]?$")
HAS_ALPHA_RE = re.compile(r"[A-Za-zÀ-ÿ]")
VOTE_CELL_RE = re.compile(r"^[\d\s.—–-]+$")
DASH_TOKEN_RE = re.compile(r"^[—–-]+$")
CELL_NOISE_RE = re.compile(
    r"^(Kies-?$|kring\.?$|Kieskring\.?$|Nummer (der|en letter der) lijst"
    r"|Naam en voorletters|Aantal(len)? stemmen|Stemcijfer|-{2,}$)",
    re.IGNORECASE)


def segment_votes(tokens: list[str], n: int, total: int | None):
    """Partition the space-separated vote sequence of a merged 1925 row into
    n per-candidate numbers (a number may span two tokens when the second is
    a 3-digit thousands group; a dash token is a missing count). Returns the
    list of votes or None when no unique segmentation fits the stemcijfer."""
    results = []

    def dfs(i, acc):
        if len(results) > 80:
            return
        if len(acc) == n:
            if i == len(tokens):
                results.append(acc[:])
            return
        if i >= len(tokens):
            return
        t = tokens[i]
        if DASH_TOKEN_RE.match(t):
            acc.append(None)
            dfs(i + 1, acc)
            acc.pop()
            return
        d = re.sub(r"[^\d]", "", t)
        if not d:
            return
        if len(d) <= 6:
            acc.append(int(d))
            dfs(i + 1, acc)
            acc.pop()
        if i + 1 < len(tokens) and len(d) <= 3:
            d2 = re.sub(r"[^\d]", "", tokens[i + 1])
            if len(d2) == 3:
                acc.append(int(d + d2))
                dfs(i + 2, acc)
                acc.pop()

    dfs(0, [])
    good = [r for r in results
            if total is not None and sum(v or 0 for v in r) == total]
    if len(good) == 1:
        return good[0]
    if len(results) == 1:
        return results[0]
    return None


SECTION_END_RE = re.compile(
    r"verbinding overeenkomstig artikel|lijstengroepen (zijn )?gevormd",
    re.IGNORECASE)


def deinterleave(lines: list[str]) -> list[str] | None:
    """Some pages carry three physical print columns that the OCR renders
    side-by-side: 9-cell rows of (kk | name | votes) x 3, three independent
    streams. Rebuild reading order: column 1 top-to-bottom, then 2, then 3.
    Returns rewritten lines (5-cell normal-form rows), or None when the page
    is not in this layout."""
    pipe_rows = [l for l in lines if "|" in l]
    wide = [l for l in pipe_rows if len(l.split("|")) >= 8]
    if len(pipe_rows) < 10 or len(wide) < 0.6 * len(pipe_rows):
        return None
    cols = [[], [], []]
    for raw in lines:
        if "|" not in raw:
            cols[0].append(raw)
            continue
        cells = [c.strip() for c in raw.split("|")]
        if len(cells) > 9:
            cells = cells[:8] + [" ".join(c for c in cells[8:] if c)]
        cells += [""] * (9 - len(cells))
        for g in range(3):
            a, b, c = cells[3 * g:3 * g + 3]
            if not (a or b or c):
                continue
            if b:
                cols[g].append(f"{a} | | {b} | {c} | ")
            elif c:
                # numeric-only row: keep the stemcijfer in the last column
                cols[g].append(f"{a} | | | | {c}")
            else:
                cols[g].append(f"{a} | | | | ")
    return cols[0] + cols[1] + cols[2]


def parse_vote_tables(con, year, expected, problems):
    urn, pages = load_pages(con, year)
    start = next((p for p, t in pages if TABLE_HDR_RE.search(t)), None)
    # the vote tables end where the art. 50 lijstengroepen section begins
    # (its "N | kieskring | lijst | stemcijfer" rows would masquerade as
    # lijst openers); the kiesdeeler page is the fallback bound
    stop = next((p for p, t in pages if SECTION_END_RE.search(t)), None)
    if stop is None:
        stop = next((p for p, t in pages if KIESDEELER_RE.search(t)), None)
    if start is None or stop is None:
        problems.append((year, urn, None, "vote-table bounds not found",
                         f"start={start} stop={stop}"))
        return [], []

    vk_rows, lu_rows = [], []
    kk = None
    kk_buf = ""       # accumulates hyphen-fragmented kieskring name cells
    block = None      # dict(lijst, cand_iter state) while inside a lijst
    opened = set()    # (kk, lijst) blocks already parsed

    def close_block(pg, total=None):
        nonlocal block
        if block is None:
            return
        if total is None:
            total = block.get("total")
        cands = expected.get((kk, block["lijst"]), [])
        got = block["got"]                    # positie -> (name_ocr, votes)
        sumv = sum(v for _, v in got.values() if v is not None)
        ok = total is not None and sumv == total and len(got) == len(cands)
        for positie, surname, name_raw in cands:
            name_ocr, votes = got.get(positie, (None, None))
            vk_rows.append((year, kk, block["lijst"], positie, name_raw,
                            name_ocr, votes, ok, urn, pg))
        lu_rows.append((year, kk, block["lijst"], total, sumv, len(cands),
                        len(got), ok, urn, pg))
        if not ok:
            problems.append((year, urn, pg, "block checksum failed",
                             f"kk{kk} lijst {block['lijst']}: "
                             f"sum={sumv} total={total} "
                             f"matched={len(got)}/{len(cands)}"))
        block = None

    def set_kk(pg, new_kk):
        nonlocal kk, kk_buf
        if new_kk != kk:
            close_block(pg)
            kk = new_kk
        kk_buf = ""

    def resolve_kk_fragment(pg, frag):
        """Feed one kieskring-column cell; fragments accumulate until the
        canonical name appears, a successor numeral advances immediately."""
        nonlocal kk_buf
        kk_buf = (kk_buf + " " + frag).strip()[-80:]
        n = re.sub(r"[^a-z]", "", kk_buf.lower())
        named = next((v for k, v in KIESKRING_NAMES.items() if k in n), None)
        if named:
            set_kk(pg, named)
            return
        m = KK_LINE_RE.match(frag)
        if m and not m.group(1).isdigit():
            # Roman numerals only: an arabic "3." here is a lijst opener,
            # not a kieskring header
            num = roman_to_int(m.group(1))
            if num is not None and \
                    (num == (kk or 0) + 1 or "vervolg" in n):
                set_kk(pg, num)

    def open_block(pg, lijst, total=None):
        nonlocal block
        close_block(pg)
        opened.add((kk, lijst))
        block = {"lijst": lijst, "next": 0, "got": {}, "pending": None,
                 "total": total}

    def match_renumbered(printed, first_name):
        """Some lijsten were renumbered between the candidate-list
        publication and the uitslag (1922 kk15: printed 8a, candidate list
        says 3a). Resolve by the first candidate's name; must be unique."""
        surname = first_name.split(",")[0].strip()
        hits = [ln for (k2, ln), cands in expected.items()
                if k2 == kk and (kk, ln) not in opened
                and name_matches(cands[0][1], surname)]
        if len(hits) == 1:
            problems.append((year, urn, None, "lijst renumbered",
                             f"kk{kk}: printed {printed} -> {hits[0]}"))
            return hits[0]
        return None

    def feed_parsed(pg, name_part, votes):
        """Shared candidate-row consumer: match name against the expected
        sequence (tolerating one OCR gap), track a pending vote count."""
        cands = expected.get((kk, block["lijst"]), [])
        surname_part = name_part.split(",")[0] if name_part else ""
        i = block["next"]
        for j in (i, i + 1):
            if j < len(cands) and surname_part and \
                    name_matches(cands[j][1], surname_part):
                if j == i + 1:
                    problems.append((year, urn, pg, "candidate row skipped",
                                     f"kk{kk} lijst {block['lijst']} "
                                     f"pos {cands[i][0]} ({cands[i][1]})"))
                block["got"][cands[j][0]] = (name_part, votes)
                block["next"] = j + 1
                block["pending"] = cands[j][0] if votes is None else None
                return True
        # bare number: votes for the pending candidate, or the block total
        if not name_part and votes is not None:
            if block["pending"] is not None:
                nm = block["got"][block["pending"]][0]
                block["got"][block["pending"]] = (nm, votes)
                block["pending"] = None
                return True
            if block["next"] >= len(cands):
                close_block(pg, total=votes)
            # otherwise a stray number (page furniture) — ignore
            return True
        # continuation fragment of a wrapped name (no new match, no number)
        if name_part and votes is None and block["pending"] is not None:
            return True
        return False

    def emit_merged(pg, lijst, names_cell, votes_cell, total):
        """A whole lijst printed as one row (1925 style)."""
        nonlocal block
        cands = expected[(kk, lijst)]
        close_block(pg)
        opened.add((kk, lijst))
        surnames = [s for s in (x.strip() for x in names_cell.split(","))
                    if s]
        votes = segment_votes(votes_cell.split(), len(cands), total)
        block = {"lijst": lijst, "next": len(cands), "got": {},
                 "pending": None, "total": total}
        for idx, (pos, sn, nr) in enumerate(cands):
            nm = surnames[idx] if idx < len(surnames) else None
            v = votes[idx] if votes else None
            block["got"][pos] = (nm, v)
        if votes is None:
            problems.append((year, urn, pg, "merged row not segmented",
                             f"kk{kk} lijst {lijst}: {len(cands)} cands, "
                             f"tokens: {votes_cell[:60]}"))
        close_block(pg, total=total)

    def handle_cells(pg, cells):
        """One pipe-table row, cell positions preserved."""
        nonlocal block
        if any(TABLE_HDR_RE.search(c) for c in cells if c):
            return
        # drop header/furniture cells but keep the rest of the row
        nz = [(i, c) for i, c in enumerate(cells)
              if c and not CELL_NOISE_RE.match(c)
              and not re.fullmatch(r"-{2,}", c)]
        if not nz:
            return
        rest = nz
        # cell 0 can be (a) a kieskring header/fragment, (b) a combined
        # "13. van Houten, H." opener cell (narrow 2-column pages), or
        # (c) a plain candidate-name cell (narrow pages). Wide rows
        # (>= 4 physical columns) always carry the kieskring column first.
        wide = len(cells) >= 4
        c0 = rest[0]
        if c0[0] == 0 and HAS_ALPHA_RE.search(c0[1]) and \
                not LIJST_CELL_RE.match(c0[1]):
            n0 = re.sub(r"[^a-z]", "", c0[1].lower())
            named = next((v for k, v in KIESKRING_NAMES.items() if k in n0),
                         None)
            if named is not None or re.match(r"Kieskring\b", c0[1], re.I):
                if named is not None:
                    set_kk(pg, named)
                rest = rest[1:]
            elif wide and not LIJST_OPEN_RE.match(c0[1]):
                resolve_kk_fragment(pg, c0[1])
                rest = rest[1:]
        if not rest or kk is None:
            return
        # combined "1. Lampetje Bzn., G." cell: split into lijst + name
        mo = LIJST_OPEN_RE.match(rest[0][1])
        if mo and mo.group(3).strip() and \
                not LIJST_CELL_RE.match(rest[0][1]):
            rest = [(rest[0][0], mo.group(1) + mo.group(2) + "."),
                    (rest[0][0], mo.group(3).strip())] + rest[1:]
        # lijst opener: a small number cell followed by a name cell
        lijst = None
        m = LIJST_CELL_RE.match(rest[0][1])
        if m and len(rest) > 1 and HAS_ALPHA_RE.search(rest[1][1]):
            cand = m.group(1) + m.group(2)
            if (kk, cand) in expected:
                lijst = cand
                rest = rest[1:]
            elif rest[0][0] <= 2:
                lijst = match_renumbered(cand, rest[1][1])
                if lijst is None:
                    # a lijst number we don't know — close rather than feed
                    # the following rows into the previous block
                    problems.append((year, urn, pg, "unknown lijst opener",
                                     f"kk{kk} lijst {cand}"))
                    close_block(pg)
                    return
                rest = rest[1:]
        name_cells = [c for _, c in rest if HAS_ALPHA_RE.search(c)]
        num_cells = [(i, c) for i, c in rest
                     if not HAS_ALPHA_RE.search(c) and VOTE_CELL_RE.match(c)]
        name_part = " ".join(name_cells).strip(" ,")
        votes = total = None
        if num_cells:
            first_num = num_cells[0][1]
            if not DASH_TOKEN_RE.match(first_num.replace(" ", "")):
                votes = parse_number(first_num)
            if len(num_cells) > 1:
                total = parse_number(num_cells[-1][1])

        if lijst is not None:
            cands = expected[(kk, lijst)]
            # merged whole-lijst row (1925): every surname in one cell,
            # every vote in one space-separated cell
            if len(cands) >= 2 and name_cells and \
                    name_cells[0].count(",") >= len(cands) and num_cells \
                    and len(num_cells[0][1].split()) >= len(cands):
                emit_merged(pg, lijst, name_cells[0], num_cells[0][1],
                            total if total is not None else votes)
                return
            # the opener must show the lijst's first candidate
            if name_part and not name_matches(cands[0][1],
                                              name_part.split(",")[0]):
                return
            open_block(pg, lijst, total=total)
            if name_part:
                feed_parsed(pg, name_part, votes)
            return

        if block is None:
            return
        if not name_part:
            # numeric-only row: the stemcijfer closes the block when it sits
            # in the final column or every candidate has been consumed
            if num_cells and votes is not None:
                is_last = num_cells[-1][0] == len(cells) - 1
                cands = expected.get((kk, block["lijst"]), [])
                if is_last or block["next"] >= len(cands):
                    close_block(pg, total=parse_number(num_cells[-1][1]))
                elif block["pending"] is not None:
                    feed_parsed(pg, "", votes)
            return
        feed_parsed(pg, name_part, votes)

    def try_open_flat(pg, line):
        """Open a lijst block from a pipe-less line; returns leftover text
        (the first candidate is printed on the opening line)."""
        m = LIJST_OPEN_RE.match(line)
        if not m:
            return None
        lijst = m.group(1) + m.group(2)
        rest = m.group(3).strip()
        if (kk, lijst) not in expected:
            lijst = match_renumbered(lijst, rest) if rest else None
            if lijst is None:
                return None
        first = expected[(kk, lijst)][0]
        # the opener must show the lijst's first candidate, otherwise the
        # number is a coincidence (e.g. a stray table cell)
        if rest and not name_matches(first[1], rest.split(",")[0]):
            return None
        open_block(pg, lijst)
        return rest

    def feed_flat(pg, text):
        """Flat-text candidate row: split the trailing number, then feed."""
        votes = None
        m = NUM_TAIL_RE.search(text)
        name_part = text
        if m and parse_number(m.group(1)) is not None:
            votes = parse_number(m.group(1))
            name_part = text[:m.start()].strip(" |,")
        elif DASH_VOTES_RE.search(text):
            name_part = DASH_VOTES_RE.sub("", text).strip(" |,")
        return feed_parsed(pg, name_part, votes)

    NOISE_RE = re.compile(
        r"^(Bijvoegsel|B[iy]jvoegsel|NEDERLANDSCHE|STAATSCOURANT"
        r"|PROCES-VERBAAL|BESLUIT\b|Aantal(len)? stemmen|Stemcijfer"
        r"|Kieskring\.?$|Nummer der lijst)", re.IGNORECASE)

    section_done = False
    for pg, txt in pages:
        if pg < start or section_done:
            continue
        if pg > stop:
            break
        fresh_page = True        # bare numbers at a page top are page numbers
        page_lines = txt.splitlines()
        multi = deinterleave(page_lines)
        if multi is not None:
            page_lines = multi
        for raw in page_lines:
            if SECTION_END_RE.search(raw):
                close_block(pg)
                section_done = True
                break
            if "|" in raw:
                cells = [re.sub(r"\s+", " ", c).strip()
                         for c in raw.split("|")]
                if any(cells):
                    fresh_page = False
                handle_cells(pg, cells)
                continue
            flat = re.sub(r"\s+", " ", raw).strip()
            if not flat:
                continue
            if fresh_page and re.match(r"^\d{1,3}$", flat):
                fresh_page = False
                continue
            fresh_page = False
            if NOISE_RE.match(flat) or TABLE_HDR_RE.search(flat) or \
                    CELL_NOISE_RE.match(flat):
                continue
            if re.match(r"^\d{1,3}$", flat) and block is None:
                continue
            # kieskring context (may share the line with a lijst opener)
            mkk = KK_LINE_RE.match(flat)
            if mkk:
                n = re.sub(r"[^a-z]", "", flat.lower())
                named = next((v for k, v in KIESKRING_NAMES.items()
                              if k in n), None)
                # Roman numerals only: an arabic "3. Obers ..." is a lijst
                # opener, not a kieskring header
                num = None if mkk.group(1).isdigit() else \
                    roman_to_int(mkk.group(1))
                # kieskringen run strictly I..XVIII through the section, so
                # a bare "II. Til-" fragment (hyphen-split name) still
                # advances when the numeral is the successor (num == kk is
                # a restated header: "II. Til- 7. Schokking ...")
                if named or (num and "vervolg" in n) or \
                        (num is not None and
                         num in ((kk or 0), (kk or 0) + 1)):
                    set_kk(pg, named or num)
                    # a lijst opener may share the physical row; it starts
                    # at the first digit (kieskring names have none)
                    md = re.search(r"\d", flat[mkk.end():])
                    if md:
                        tail = flat[mkk.end() + md.start():].strip(" |")
                        rest = try_open_flat(pg, tail)
                        if rest:
                            feed_flat(pg, rest)
                    continue
            if kk is None:
                continue
            rest = try_open_flat(pg, flat)
            if rest is not None:
                if rest:
                    feed_flat(pg, rest)
            elif block is not None:
                feed_flat(pg, flat)
    close_block(pages[-1][0])
    return vk_rows, lu_rows


# ---------------------------------------------------------------------------
# 2. elected members ("Vaststelling van den uitslag")
# ---------------------------------------------------------------------------
INITIALS_RE = re.compile(
    r"^(?:[A-Za-zÀ-ÿ]{1,4}\.\s*)+$")            # "J. W.", "Ch. L.", "Jhr. M."
NAME_INITIALS_RE = re.compile(
    r"^(.*?)[,.]?\s*((?:[A-Za-zÀ-ÿ]{1,4}\.\s*)+)$")


def parse_gekozen_row(line: str):
    """'Geer, de | D. J. | Arnhem.', 'Aalberse, P. J. M. | Voorburg.', or the
    1918 two-column layout 'Helsdingen, | W. P. G. | Den Haag. | Rugge, | E.
    | Groningen.' -> list of (name, initials, residence) tuples."""
    cells = [c.strip() for c in line.split("|") if c.strip()]
    out, used = [], set()
    for i, c in enumerate(cells):
        if i in used or i == 0 or i + 1 >= len(cells):
            continue
        if INITIALS_RE.match(c) and (i - 1) not in used:
            out.append((cells[i - 1].strip(" ,."), c.strip(),
                        cells[i + 1].strip(" .")))
            used.update({i - 1, i, i + 1})
    if out:
        return out
    if len(cells) == 2:
        m = NAME_INITIALS_RE.match(cells[0])
        if m and m.group(1).strip(" ,"):
            return [(m.group(1).strip(" ,."), m.group(2).strip(),
                     cells[1].strip(" ."))]
    return []


def parse_gekozen(con, year, problems):
    urn, pages = load_pages(con, year)
    start = next((p for p, t in pages if VASTSTELLING_RE.search(t)), None)
    if start is None:
        problems.append((year, urn, None, "vaststelling not found", ""))
        return []
    rows, started, done = [], False, False
    pending = None
    for pg, txt in pages:
        if pg < start or done:
            continue
        for raw in txt.splitlines():
            line = re.sub(r"\s+", " ", raw).strip()
            if not started:
                if VASTSTELLING_RE.search(line):
                    started = True
                continue
            if ART105_RE.search(line) or "Aanwijzing van de candidaten" in line:
                done = True
                break
            if re.match(r"^\|?\s*(Naam en voorletters|in alphabetische"
                        r"|Woonplaats)", line):
                continue
            parsed = parse_gekozen_row(line)
            if parsed:
                for p in parsed:
                    rows.append([year, len(rows) + 1, *p, urn, pg])
                pending = rows[-1]
            elif pending is not None and line.startswith("|"):
                # wrapped residence cell: "| | madeel)."
                frag = line.strip("| .")
                if frag:
                    pending[4] = (pending[4] + " " + frag).strip()
        if started and not done and pg > start + 4:
            done = True          # elected table never spans >5 pages
    return [tuple(r) for r in rows]


def main() -> None:
    con = duckdb.connect(DB_PATH)
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)

    expected = defaultdict(list)
    for kk, ln, pos, sn, nr, yr in con.execute("""
            SELECT kieskring_no, lijst_no, positie, surname, name_raw, year
            FROM kandidatenlijsten ORDER BY year, kieskring_no, lijst_no,
            positie""").fetchall():
        expected[(yr, kk, ln)].append((pos, sn, nr))

    problems = []
    for year in sorted(MAIN_ISSUES):
        exp_year = {(kk, ln): v for (yr, kk, ln), v in expected.items()
                    if yr == year}
        vk, lu = parse_vote_tables(con, year, exp_year, problems)
        gk = parse_gekozen(con, year, problems)
        if vk:
            con.executemany("INSERT INTO voorkeur_stemmen VALUES "
                            "(?,?,?,?,?,?,?,?,?,?)", vk)
        if lu:
            con.executemany("INSERT INTO lijst_uitslagen VALUES "
                            "(?,?,?,?,?,?,?,?,?,?)", lu)
        if gk:
            con.executemany("INSERT INTO gekozen VALUES (?,?,?,?,?,?,?)", gk)
        print(f"{year}: {len(lu)} lijst blocks, {len(vk)} candidate rows, "
              f"{len(gk)} elected")
    con.executemany("INSERT INTO uitslag_issues VALUES (?,?,?,?,?)",
                    problems)

    print("\nblock checksum quality:")
    print(con.execute("""
        SELECT year, COUNT(*) AS blocks,
               SUM(checksum_ok::INT) AS ok,
               ROUND(AVG(checksum_ok::INT), 3) AS ok_rate,
               SUM(stemcijfer) AS total_votes
        FROM lijst_uitslagen GROUP BY 1 ORDER BY 1
    """).fetchdf().to_string(index=False))
    print("\nelected counts (expect 100):")
    print(con.execute(
        "SELECT year, COUNT(*) FROM gekozen GROUP BY 1 ORDER BY 1"
    ).fetchdf().to_string(index=False))
    print("\nissue summary:")
    print(con.execute("""
        SELECT year, reason, COUNT(*) FROM uitslag_issues
        GROUP BY 1,2 ORDER BY 1,3 DESC
    """).fetchdf().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
