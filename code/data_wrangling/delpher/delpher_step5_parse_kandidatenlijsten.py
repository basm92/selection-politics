# =============================================================================
# delpher_step5_parse_kandidatenlijsten.py  [DELPHER PIPELINE - STEP 5]
# Input:  data/delpher/delpher.duckdb  (ocr_pages from step 4 + target_pages)
# Output: data/delpher/delpher.duckdb
#           kandidatenlijsten       — candidate × kieskring × lijst rows for
#                                     the six interwar elections (ALL nominated
#                                     candidates, incl. losers)
#           kandidatenlijst_issues  — lines the parser could not interpret
#                                     (audit table; drives parser iteration)
#
# Parses the re-OCR'd official candidate lists (art. 51 Kieswet publication by
# the Centraal Stembureau chairman): per kieskring (1-18), numbered lists, and
# entries "positie. Achternaam, [titels] voorletters, woonplaats.".
# Residence is deliberately kept verbatim — it is the linkage key for Phase 2
# genealogical matching.
#
# The same list (and person) recurs in several kieskringen; rows are kept per
# kieskring. Deduplicated per-election person views are built downstream in
# panel_step2_merge_post1917.py.
#
# Pure offline parsing — free to rerun; tables are rebuilt from ocr_pages on
# every run.
#
# Usage:
#   uv run python code/data_wrangling/delpher/delpher_step5_parse_kandidatenlijsten.py
# =============================================================================
import re

import duckdb

DB_PATH = "./data/delpher/delpher.duckdb"

TITLE_TOKENS = {"mr", "dr", "jhr", "ir", "prof", "ds", "mgr", "jkvr",
                "baron", "graaf", "ridder"}

ROMAN = {"i": 1, "v": 5, "x": 10}

# Canonical kieskring numbering (art. 32 Kieswet 1917, unchanged 1918-1937).
# The printed name is more reliable than the OCR'd numeral (1933: "XI" was
# read as "I"), so a recognized name overrides the number.
KIESKRING_NAMES = {
    "hertogenbosch": (1, "'s-Hertogenbosch"),
    "tilburg": (2, "Tilburg"),
    "arnhem": (3, "Arnhem"),
    "nijmegen": (4, "Nijmegen"),
    "rotterdam": (5, "Rotterdam"),
    "gravenhage": (6, "'s-Gravenhage"),
    "leiden": (7, "Leiden"),
    "dordrecht": (8, "Dordrecht"),
    "amsterdam": (9, "Amsterdam"),
    "helder": (10, "Den Helder"),
    "haarlem": (11, "Haarlem"),
    "middelburg": (12, "Middelburg"),
    "utrecht": (13, "Utrecht"),
    "leeuwarden": (14, "Leeuwarden"),
    "zwolle": (15, "Zwolle"),
    "groningen": (16, "Groningen"),
    "assen": (17, "Assen"),
    "maastricht": (18, "Maastricht"),
}


def canonical_kieskring(s: str) -> tuple[int, str] | None:
    n = re.sub(r"[^a-z]", "", s.lower())
    for key, val in KIESKRING_NAMES.items():
        if key in n:
            return val
    return None

# Page furniture / boilerplate that never belongs to an entry.
NOISE_RE = re.compile(
    r"^(bijvoegsel tot de|nederlandsche staatscourant|staatscourant\b"
    r"|de voorzitter van het centraal stembureau|gezien artikel"
    r"|maakt bekend|verkiezing\s*$|verkiezing van de leden"
    r"|van\s*$|de\s*$|der\s*$|leden\s*$|tweede\s*$|kamer\s*$"
    r"|staten-generaal|\(vervolg\.?\)|vervolg\b|---|===|n[°o]\.\s*\d+\s*$"
    r"|'s[- ]?gravenhage,?\s+(den\s+)?\d{1,2}\s"
    r"|de voorzitter voornoemd|aldus vastgesteld"
    r"|ter\s+alge?meene?\s+landsdrukkerij|plaatsvervangend voorzitter"
    r"|\d+\s*$)", re.IGNORECASE)

# Closing signature block ("J. OPPENHEIM.", "A. A. H. STRUYCKEN.", ...)
ALLCAPS_NOISE_RE = re.compile(r"^[A-Z][A-Z\s.,'’]{4,}$")

KIESKRING_RE = re.compile(
    r"^\W*kieskring\s+([ivx]+|\d+)\b\.?\s*"
    r"[(\[]?\s*(?:gemeente\s+)?([^)\]]*)[)\]]?", re.IGNORECASE)

LIJST_RE = re.compile(
    r"^\W*lijst\s*n?\s*[°o0]?\.?\s*(\d+)\s*([a-z]?)\b\.?\s*$", re.IGNORECASE)

ENTRY_START_RE = re.compile(r"^(\d{1,2})\.\s+(\S.*)$")

# Entries on typographically damaged print spots that the vision OCR
# consistently drops or merges (verified against the embedded Delpher OCR
# in page_texts, 2026-07-07). Applied after parsing: the DELETE keys remove
# the garbled parsed rows, the ENTRY rows are inserted in their place.
MANUAL_DELETES = [
    # (year, kieskring_no, lijst_no, positie)
    (1922, 8, "4d", 3),    # Rippe misnumbered 3 -> 5 (two entries skipped)
    (1933, 4, "29", 12),   # "13. Eschauzier" merged into the Boeye row
]
MANUAL_ENTRIES = [
    # (year, kieskring_no, lijst_no, positie, entry_text, issue_urn, page_no)
    (1922, 7, "19", 9, "Thiel geb. Wehrbein, Th., 's Gravenhage",
     "MMKB08:000179055:mpeg21", 15),
    (1922, 8, "4d", 3, "Vos, B. H., 's Gravenhage",
     "MMKB08:000179055:mpeg21", 15),
    (1922, 8, "4d", 4, "Frijda, J., Utrecht",
     "MMKB08:000179055:mpeg21", 15),
    (1922, 8, "4d", 5, "Rippe, J. H., Delft",
     "MMKB08:000179055:mpeg21", 15),
    (1933, 4, "29", 12, "Boeye, D. A. M. Q., 's-Gravenhage",
     "MMKB08:000181136:mpeg21", 8),
    (1933, 4, "29", 13, "Eschauzier, mr. P. C. L., 's-Gravenhage",
     "MMKB08:000181136:mpeg21", 8),
]


def roman_to_int(s: str) -> int | None:
    s = s.lower().replace("y", "v")  # OCR: XIY -> XIV
    if s.isdigit():
        return int(s)
    total, prev = 0, 0
    for ch in reversed(s):
        v = ROMAN.get(ch)
        if v is None:
            return None
        total = total - v if v < prev else total + v
        prev = max(prev, v)
    return total if 1 <= total <= 18 else None


def clean_line(line: str) -> str:
    s = line.replace("|", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def parse_entry(pos: int, body: str) -> dict | None:
    """'Deckers, dr. L. N., Eindhoven.' -> fields. None if hopeless."""
    body = body.strip().rstrip(".").strip()
    parts = [p.strip() for p in body.split(",") if p.strip()]
    if len(parts) < 2:
        return None
    surname = parts[0]
    residence = parts[-1] if len(parts) >= 3 else None
    middle = parts[1:-1] if len(parts) >= 3 else parts[1:]
    mid = " ".join(middle)
    # pull leading title tokens out of the initials segment
    titles = []
    while True:
        m = re.match(r"^([A-Za-z]+)\.?\s+", mid + " ")
        if m and m.group(1).lower() in TITLE_TOKENS:
            titles.append(m.group(1).lower())
            mid = mid[m.end():].strip() if m.end() <= len(mid) else ""
            continue
        break
    initials = mid.strip() or None
    return {
        "positie": pos,
        "name_raw": body,
        "surname": surname,
        "initials": initials,
        "titles": "|".join(titles) or None,
        "residence": residence,
    }


DDL = """
CREATE OR REPLACE TABLE kandidatenlijsten (
    year          INTEGER,
    kieskring_no  INTEGER,
    kieskring_name TEXT,
    lijst_no      TEXT,
    lijst_inferred BOOLEAN,
    positie       INTEGER,
    name_raw      TEXT,
    surname       TEXT,
    initials      TEXT,
    titles        TEXT,
    residence     TEXT,
    issue_urn     TEXT,
    page_no       INTEGER
);
CREATE OR REPLACE TABLE kandidatenlijst_issues (
    year      INTEGER,
    issue_urn TEXT,
    page_no   INTEGER,
    reason    TEXT,
    line      TEXT
);
"""


def main() -> None:
    con = duckdb.connect(DB_PATH)
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)

    pages = con.execute("""
        SELECT t.election_year, o.issue_urn, o.page_no, o.ocr_text
        FROM ocr_pages o
        JOIN (SELECT DISTINCT issue_urn, page_no, election_year
              FROM target_pages WHERE doc_role = 'kandidatenlijst') t
          USING (issue_urn, page_no)
        ORDER BY t.election_year, o.page_no
    """).fetchall()
    if not pages:
        print("no kandidatenlijst pages in ocr_pages yet — run step 4 first")
        return

    rows, problems = [], []
    state = {"kk_no": None, "kk_name": None, "lijst": None}
    buf = None          # (pos, text, issue_urn, page_no)
    last_pos = 0        # last emitted positie within the current lijst
    seg_id = 0          # segment counter; bumps on every lijst start
    anon_reset_segs = set()   # segments opened by a position reset

    def flush():
        nonlocal buf, last_pos
        if buf is None:
            return
        pos, text, urn, pg, yr = buf
        buf = None
        ent = parse_entry(pos, text)
        if ent is None:
            problems.append((yr, urn, pg, "unparseable entry",
                             f"{pos}. {text}"))
            return
        if state["kk_no"] is None:
            problems.append((yr, urn, pg, "entry outside kieskring/lijst",
                             f"{pos}. {text}"))
            return
        last_pos = ent["positie"]
        rows.append([yr, state["kk_no"], state["kk_name"], state["lijst"],
                     seg_id, ent["positie"], ent["name_raw"], ent["surname"],
                     ent["initials"], ent["titles"], ent["residence"],
                     urn, pg])

    cur_year = None
    for year, urn, pg, text in pages:
        if year != cur_year:      # elections never share a page
            flush()
            state = {"kk_no": None, "kk_name": None, "lijst": None}
            cur_year = year
        for raw in text.splitlines():
            line = clean_line(raw)
            if not line or NOISE_RE.match(line) or ALLCAPS_NOISE_RE.match(line):
                continue
            mk = KIESKRING_RE.match(line)
            if mk:
                flush()
                state["lijst"] = None
                seg_id += 1
                last_pos = 0
                no = roman_to_int(mk.group(1))
                canon = canonical_kieskring(mk.group(2))
                if canon:                      # printed name beats numeral
                    state["kk_no"], state["kk_name"] = canon
                    state["kk_pending"] = False
                elif no is not None:
                    state["kk_no"], state["kk_name"] = no, None
                    # 1918 headers wrap: "(gemeente en" / "hoofdstembureau
                    # Rotterdam)." — resolve the name from the next line(s)
                    state["kk_pending"] = True
                else:
                    problems.append((year, urn, pg, "bad kieskring no", line))
                continue
            if state.get("kk_pending") and buf is None:
                canon = canonical_kieskring(line)
                if canon:
                    if canon[0] != state["kk_no"]:
                        state["kk_no"] = canon[0]
                    state["kk_name"] = canon[1]
                    state["kk_pending"] = False
                    continue
            ml = LIJST_RE.match(line)
            if ml:
                flush()
                state["lijst"] = ml.group(1) + ml.group(2).lower()
                state["kk_pending"] = False
                seg_id += 1
                last_pos = 0
                continue
            me = ENTRY_START_RE.match(line)
            if me:
                flush()
                pos = int(me.group(1))
                if pos == 1 and last_pos >= 2:
                    # position reset without a LIJST header: the OCR dropped
                    # one — open an anonymous segment, number inferred later
                    problems.append((year, urn, pg,
                                     "lijst header missing (position reset)",
                                     f"1. {me.group(2)}"))
                    state["lijst"] = None
                    seg_id += 1
                    anon_reset_segs.add(seg_id)
                    last_pos = 0
                buf = (pos, me.group(2), urn, pg, year)
                continue
            if buf is not None:   # wrapped continuation of the entry
                buf = (buf[0], buf[1] + " " + line, buf[2], buf[3], buf[4])
            else:
                problems.append((year, urn, pg, "stray line", line))
    flush()

    # --- resolve anonymous segments (dropped LIJST headers) -----------------
    # A reset segment sandwiched between numeric lists n and n+2 within the
    # same kieskring must be list n+1; anything else stays NULL and is logged.
    seg_order: dict[tuple, list] = {}
    for r in rows:
        key = (r[0], r[1])                       # (year, kieskring_no)
        segs = seg_order.setdefault(key, [])
        if not segs or segs[-1][0] != r[4]:
            segs.append((r[4], r[3]))            # (seg_id, lijst_no)
    seg_fix: dict[int, str] = {}
    for key, segs in seg_order.items():
        for i, (sid, lijst) in enumerate(segs):
            if sid not in anon_reset_segs or lijst is not None:
                continue
            if 0 < i < len(segs) - 1:
                prev, nxt = segs[i - 1][1], segs[i + 1][1]
                if (prev and nxt and prev.isdigit() and nxt.isdigit()
                        and int(nxt) - int(prev) == 2):
                    seg_fix[sid] = str(int(prev) + 1)
    # Within a kieskring the lists are printed in ascending numeric order;
    # an adjacent duplicate number means the OCR misread one digit (1933:
    # "LIJST 16" read as a second "LIJST 17"). Repair when the neighbouring
    # numbers leave exactly one gap.
    for key, segs in seg_order.items():
        for i in range(1, len(segs)):
            a, b = segs[i - 1][1], segs[i][1]
            if not (a and b and a == b and a.isdigit()):
                continue
            n = int(a)
            below = segs[i - 2][1] if i >= 2 else "0"
            above = segs[i + 1][1] if i + 1 < len(segs) else None
            if below and below.isdigit() and int(below) < n - 1:
                seg_fix[segs[i - 1][0]] = str(n - 1)
                segs[i - 1] = (segs[i - 1][0], str(n - 1))
            elif above and above.isdigit() and int(above) > n + 1:
                seg_fix[segs[i][0]] = str(n + 1)
                segs[i] = (segs[i][0], str(n + 1))
            else:
                problems.append((key[0], None, None,
                                 "duplicate lijst number unresolved",
                                 f"kieskring {key[1]} lijst {a}"))

    n_unresolved = 0
    for r in rows:
        if r[4] in seg_fix:
            r[3] = seg_fix[r[4]]
            r.append(True)
        else:
            if r[3] is None:
                n_unresolved += 1
            r.append(False)
    if seg_fix:
        print(f"repaired {len(seg_fix)} lijst numbers (dropped headers / "
              f"misread duplicates); {n_unresolved} rows with NULL lijst_no")

    con.executemany(
        "INSERT INTO kandidatenlijsten "
        "(year, kieskring_no, kieskring_name, lijst_no, lijst_inferred, "
        " positie, name_raw, surname, initials, titles, residence, "
        " issue_urn, page_no) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(r[0], r[1], r[2], r[3], r[13], r[5], r[6], r[7], r[8], r[9],
          r[10], r[11], r[12]) for r in rows])

    # --- manual patches (see MANUAL_DELETES / MANUAL_ENTRIES above) --------
    for y, kk, ln, pos in MANUAL_DELETES:
        con.execute(
            "DELETE FROM kandidatenlijsten WHERE year=? AND kieskring_no=? "
            "AND lijst_no=? AND positie=?", [y, kk, ln, pos])
    for y, kk, ln, pos, text, urn, pg in MANUAL_ENTRIES:
        con.execute(
            "DELETE FROM kandidatenlijsten WHERE year=? AND kieskring_no=? "
            "AND lijst_no=? AND positie=?", [y, kk, ln, pos])
        ent = parse_entry(pos, text)
        kk_name = con.execute(
            "SELECT MAX(kieskring_name) FROM kandidatenlijsten "
            "WHERE year=? AND kieskring_no=?", [y, kk]).fetchone()[0]
        con.execute(
            "INSERT INTO kandidatenlijsten VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [y, kk, kk_name, ln, False, pos, ent["name_raw"], ent["surname"],
             ent["initials"], ent["titles"], ent["residence"], urn, pg])
    print(f"applied {len(MANUAL_ENTRIES)} manual entry patches")
    con.executemany(
        "INSERT INTO kandidatenlijst_issues VALUES (?,?,?,?,?)", problems)

    print(con.execute("""
        SELECT year, COUNT(*) AS entries,
               COUNT(DISTINCT kieskring_no) AS kieskringen,
               COUNT(DISTINCT (kieskring_no, lijst_no)) AS lijsten,
               COUNT(DISTINCT (surname, initials, residence)) AS persons_approx
        FROM kandidatenlijsten GROUP BY 1 ORDER BY 1
    """).fetchdf().to_string(index=False))
    print("\nparser issues by reason:")
    print(con.execute("""
        SELECT year, reason, COUNT(*) AS n
        FROM kandidatenlijst_issues GROUP BY 1,2 ORDER BY 1,3 DESC
    """).fetchdf().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
