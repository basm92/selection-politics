# =============================================================================
# delpher_step3_locate_pages.py  [DELPHER PIPELINE - STEP 3]
# Input:  data/delpher/delpher.duckdb          (articles + issue_pdfs)
#         data/delpher/staatscourant/<yr>/*.pdf (issue scans, step 2)
# Output: data/delpher/delpher.duckdb
#           page_texts   — embedded Delpher OCR text layer per PDF page
#           target_pages — pages of the 14 key issues that belong to the
#                          candidate-list / results articles (doc_role
#                          'kandidatenlijst' or 'uitslag')
#
# The issue PDFs carry the Delpher OCR as an embedded text layer. That OCR is
# orientation-grade (columns interleave, diacritics break), but it is the SAME
# OCR as articles.ocr_text, so individual print lines match near-verbatim.
# Pages are assigned to a target article when >= MIN_FRAC of their text-layer
# lines occur verbatim in the article OCR (whitespace/punctuation-insensitive).
# Measured separation on the 1918 nomination issue: target pages 0.61-0.83,
# non-target pages <= 0.03.
#
# Only pages classified here are sent to the (paid) re-OCR pass in step 4 —
# the 14 key issues hold 726 pages, of which only the election paperwork is
# needed.
#
# Resumable: issues whose page texts are already stored are not re-extracted;
# target_pages is derived and rebuilt per issue on every run (cheap, offline).
#
# Usage:
#   uv run python code/data_wrangling/delpher/delpher_step3_locate_pages.py
# =============================================================================
import os
import re
import subprocess

import duckdb

DB_PATH = "./data/delpher/delpher.duckdb"

# The two-document structure per election (post_1917_candidates.md, verified):
# role 'kandidatenlijst' = official validated lists per kieskring (art. 51
# Kieswet, ALL candidates incl. losers); role 'uitslag' = Centraal Stembureau
# results bundle (proces-verbaal, besluit art. 97, verdeeling, vaststelling,
# aanwijzing).
KEY_ISSUES = [
    (1918, "kandidatenlijst", "MMKB08:000179786:mpeg21"),
    (1922, "kandidatenlijst", "MMKB08:000179055:mpeg21"),
    (1925, "kandidatenlijst", "MMKB08:000180972:mpeg21"),
    (1929, "kandidatenlijst", "MMKB08:000182756:mpeg21"),
    (1933, "kandidatenlijst", "MMKB08:000181136:mpeg21"),
    (1937, "kandidatenlijst", "MMKB08:000168898:mpeg21"),
    (1918, "uitslag", "MMKB08:000179144:mpeg21"),
    (1922, "uitslag", "MMKB08:000178343:mpeg21"),
    (1925, "uitslag", "MMKB08:000181037:mpeg21"),
    (1929, "uitslag", "MMKB08:000161457:mpeg21"),
    (1929, "uitslag", "MMKB08:000161498:mpeg21"),
    (1933, "uitslag", "MMKB08:000181270:mpeg21"),
    (1937, "uitslag", "MMKB08:000168915:mpeg21"),
    (1937, "uitslag", "MMKB08:000168911:mpeg21"),
]

TARGET_TITLE_RE = {
    "kandidatenlijst": re.compile(r"^Verkiezing van de leden"),
    "uitslag": re.compile(
        r"^(PROCES-VERBAAL"
        r"|BESLUIT van het Centraal Stembureau"
        r"|Verdeeling van de aan de lijstengroepen"
        r"|Vaststelling van den uitslag"
        r"|Aanwijzing van de candidaten)"),
}

MIN_LINE_CHARS = 12   # normalized; shorter lines are too generic to match on
MIN_FRAC = 0.15       # inclusive: catches boundary pages where a doc starts
MIN_HITS = 5          # ... but demand a handful of verbatim lines

DDL = """
CREATE TABLE IF NOT EXISTS page_texts (
    issue_urn TEXT,
    page_no   INTEGER,
    n_chars   INTEGER,
    text      TEXT,
    PRIMARY KEY (issue_urn, page_no)
);
CREATE TABLE IF NOT EXISTS target_pages (
    issue_urn     TEXT,
    page_no       INTEGER,
    election_year INTEGER,
    doc_role      TEXT,
    metadata_key  TEXT,
    title         TEXT,
    match_frac    DOUBLE,
    n_lines       INTEGER,
    n_hits        INTEGER,
    PRIMARY KEY (issue_urn, page_no, metadata_key)
);
"""


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def n_pdf_pages(path: str) -> int:
    out = subprocess.run(["pdfinfo", path], capture_output=True, text=True,
                         check=True).stdout
    return int(re.search(r"^Pages:\s+(\d+)", out, re.M).group(1))


def page_text(path: str, page: int) -> str:
    return subprocess.run(
        ["pdftotext", "-f", str(page), "-l", str(page), path, "-"],
        capture_output=True, text=True, check=True).stdout


def main() -> None:
    con = duckdb.connect(DB_PATH)
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)

    for year, role, issue_urn in KEY_ISSUES:
        row = con.execute("SELECT path FROM issue_pdfs WHERE issue_urn = ?",
                          [issue_urn]).fetchone()
        if not row or not os.path.exists(row[0]):
            print(f"{issue_urn}: PDF missing — run step 2 first")
            continue
        path = row[0]
        n_pages = n_pdf_pages(path)

        # --- extract & archive the embedded text layer (resumable) ----------
        done = con.execute(
            "SELECT COUNT(*) FROM page_texts WHERE issue_urn = ?",
            [issue_urn]).fetchone()[0]
        if done < n_pages:
            con.execute("DELETE FROM page_texts WHERE issue_urn = ?",
                        [issue_urn])
            for p in range(1, n_pages + 1):
                txt = page_text(path, p)
                con.execute("INSERT INTO page_texts VALUES (?,?,?,?)",
                            [issue_urn, p, len(txt), txt])

        # --- match pages to the target articles of this issue ---------------
        arts = [
            (mk, title, norm(ocr))
            for mk, title, ocr in con.execute(
                "SELECT metadata_key, title, ocr_text FROM articles "
                "WHERE issue_urn = ? AND ocr_text IS NOT NULL",
                [issue_urn]).fetchall()
            if TARGET_TITLE_RE[role].match(title or "")
        ]
        if not arts:
            print(f"{issue_urn}: no target articles matched — check titles")
            continue

        con.execute("DELETE FROM target_pages WHERE issue_urn = ?",
                    [issue_urn])
        n_target = 0
        for p, txt in con.execute(
                "SELECT page_no, text FROM page_texts WHERE issue_urn = ? "
                "ORDER BY page_no", [issue_urn]).fetchall():
            lines = [n for n in (norm(l) for l in txt.splitlines())
                     if len(n) >= MIN_LINE_CHARS]
            if not lines:
                continue
            for mk, title, a in arts:
                hits = sum(l in a for l in lines)
                frac = hits / len(lines)
                if frac >= MIN_FRAC and hits >= MIN_HITS:
                    con.execute(
                        "INSERT INTO target_pages VALUES (?,?,?,?,?,?,?,?,?)",
                        [issue_urn, p, year, role, mk, title, frac,
                         len(lines), hits])
                    n_target += 1
        pages = con.execute(
            "SELECT COUNT(DISTINCT page_no) FROM target_pages "
            "WHERE issue_urn = ?", [issue_urn]).fetchone()[0]
        print(f"{issue_urn} ({year} {role}): {n_pages} pages in PDF, "
              f"{pages} target pages ({n_target} page-article matches)")

    print("\ntarget pages per election & role:")
    print(con.execute("""
        SELECT election_year, doc_role, COUNT(DISTINCT (issue_urn, page_no))
               AS pages
        FROM target_pages GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchdf().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
