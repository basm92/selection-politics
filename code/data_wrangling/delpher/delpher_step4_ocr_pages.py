# =============================================================================
# delpher_step4_ocr_pages.py  [DELPHER PIPELINE - STEP 4]
# Input:  data/delpher/delpher.duckdb           (target_pages from step 3)
#         data/delpher/staatscourant/<yr>/*.pdf (issue scans, step 2)
#         examples/.env                          (GOOGLE_API_KEY)
# Output: data/delpher/delpher.duckdb
#           ocr_pages — raw per-page transcription (the archival OCR corpus
#                       that steps 5-6 parse; never overwritten on rerun)
#
# Re-OCR of the 616 target pages located by step 3. The Delpher OCR
# (articles.ocr_text / the PDF text layer) interleaves the newspaper columns
# and breaks diacritics/initials, so it cannot be parsed into candidate rows;
# a vision-model pass reads the columns and tables in order. Each page is
# rendered from the archived issue PDF at 300 dpi (pdftoppm, grayscale JPEG)
# and transcribed by Gemini (gemini-3.1-flash-lite) with a
# layout-preserving prompt; the raw model output is archived per page.
#
# PAID API: uses GOOGLE_API_KEY from examples/.env. Run was authorized by the
# project owner (2026-07-07) for the target pages only. Token usage is stored
# per page in ocr_pages for cost accounting.
#
# Resumable: pages already in ocr_pages are skipped. --limit N stops after N
# pages (smoke tests); --dry-run only reports what would be sent.
# --redo-degenerate re-sends pages whose transcription collapsed into a
# repetition loop (MAX_TOKENS / endless empty "|" cells — a known failure
# mode on dense tables) using a fallback prompt without pipe separators.
#
# Usage:
#   uv run python code/data_wrangling/delpher/delpher_step4_ocr_pages.py \
#       [--limit N] [--dry-run] [--redo-degenerate]
# =============================================================================
import argparse
import asyncio
import base64
import os
import re
import subprocess
import sys
import tempfile

import aiohttp
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "huygens"))
from huygens_async_helpers import TokenBucketRateLimiter

DB_PATH = "./data/delpher/delpher.duckdb"
ENV_PATH = "./examples/.env"
MODEL = "gemini-3.1-flash-lite"
API_URL = ("https://generativelanguage.googleapis.com/v1beta/models/"
           f"{MODEL}:generateContent")

DPI = 300
RATE = 2.0          # requests/second
CONCURRENCY = 8
MAX_ATTEMPTS = 4

PROMPT = """\
Transcribe this scanned page of the Nederlandsche Staatscourant (a Dutch
government gazette, 1918-1937) completely and faithfully.

Rules:
- The page is typeset in narrow columns. Read each column top to bottom,
  then move to the next column to the right. Never interleave columns.
- Keep the original (pre-1947) Dutch spelling exactly as printed; do not
  modernize. Keep abbreviations, titles (mr., dr., jhr.) and punctuation.
- Preserve document structure on separate lines: headings such as
  "Kieskring I ('s Hertogenbosch)" and "LIJST n°. 4", and numbered candidate
  entries like "3. Deckers, dr. L. N., Eindhoven." one per line.
- For tables, output one table row per line with cells separated by " | ",
  in the order name | initials/details | numbers, following the printed
  columns. Repeat the table's header line once where the table starts.
- Transcribe numbers digit by digit exactly as printed (thousands may be
  spaced, e.g. "28 050").
- Output plain text only: no markdown, no commentary, no [illegible] guesses
  — use ~ for a character you truly cannot read.
"""

FALLBACK_PROMPT = PROMPT.replace(
    """- For tables, output one table row per line with cells separated by " | ",
  in the order name | initials/details | numbers, following the printed
  columns. Repeat the table's header line once where the table starts.""",
    """- For tables, write one table row per line as plain text with single
  spaces between the cells (name, initials, numbers). Never output empty
  cells or separator characters; skip blank cells entirely.""")

# Degenerate repetition loop: long runs of pipe/whitespace filler.
DEGENERATE_RE = re.compile(r"(\|[\s|]{0,3}){150,}")

DDL = """
CREATE TABLE IF NOT EXISTS ocr_pages (
    issue_urn      TEXT,
    page_no        INTEGER,
    model          TEXT,
    dpi            INTEGER,
    ocr_text       TEXT,
    finish_reason  TEXT,
    prompt_tokens  INTEGER,
    output_tokens  INTEGER,
    fetched_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (issue_urn, page_no)
);
"""


def api_key() -> str:
    with open(ENV_PATH) as f:
        for line in f:
            if line.startswith("GOOGLE_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit(f"GOOGLE_API_KEY not found in {ENV_PATH}")


def render_page(pdf_path: str, page_no: int, tmpdir: str) -> bytes:
    prefix = os.path.join(tmpdir, f"p{page_no}")
    subprocess.run(
        ["pdftoppm", "-r", str(DPI), "-gray", "-jpeg",
         "-jpegopt", "quality=90", "-f", str(page_no), "-l", str(page_no),
         pdf_path, prefix],
        check=True, capture_output=True)
    files = [f for f in os.listdir(tmpdir)
             if f.startswith(f"p{page_no}-") and f.endswith(".jpg")]
    assert len(files) == 1, f"expected 1 render, got {files}"
    path = os.path.join(tmpdir, files[0])
    with open(path, "rb") as f:
        data = f.read()
    os.unlink(path)
    return data


async def transcribe(session: aiohttp.ClientSession, bucket, key: str,
                     jpeg: bytes, prompt: str = PROMPT,
                     temperature: float = 0.0) -> tuple[str, str, int, int]:
    body = {
        "contents": [{"parts": [
            {"inline_data": {"mime_type": "image/jpeg",
                             "data": base64.b64encode(jpeg).decode()}},
            {"text": prompt},
        ]}],
        "generationConfig": {"temperature": temperature,
                             "maxOutputTokens": 32768},
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
            return (text, cand.get("finishReason", ""),
                    usage.get("promptTokenCount", 0),
                    usage.get("candidatesTokenCount", 0))
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_err = repr(e)
            await asyncio.sleep(5 * attempt)
    raise RuntimeError(f"gave up after {MAX_ATTEMPTS} attempts: {last_err}")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--redo-degenerate", action="store_true")
    ap.add_argument("--redo-pages", default=None,
                    help="comma-separated issue_urn:page_no to re-transcribe "
                         "with the completeness prompt (dropped-column fix)")
    args = ap.parse_args()

    con = duckdb.connect(DB_PATH)
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            con.execute(stmt)

    if args.redo_degenerate:
        bad = [(u, p) for u, p, t in con.execute(
            "SELECT issue_urn, page_no, ocr_text FROM ocr_pages "
            "WHERE finish_reason <> 'STOP'").fetchall()
            if DEGENERATE_RE.search(t or "")]
        print(f"re-sending {len(bad)} degenerate pages with fallback prompt")
        for u, p in bad:
            con.execute("DELETE FROM ocr_pages WHERE issue_urn=? AND "
                        "page_no=?", [u, p])

    if args.redo_pages:
        for spec in args.redo_pages.split(","):
            u, p = spec.rsplit(":", 1)
            con.execute("DELETE FROM ocr_pages WHERE issue_urn=? AND "
                        "page_no=?", [u, int(p)])

    prompt = PROMPT
    temperature = 0.0
    if args.redo_degenerate:
        prompt, temperature = FALLBACK_PROMPT, 0.2
    elif args.redo_pages:
        # observed failure: on very dense pages the model can silently drop
        # whole columns — spell out the completeness requirement
        prompt = PROMPT + (
            "\nThis page is very dense (it may hold several hundred numbered"
            "\nentries across 5-7 columns). Transcribe EVERY column from the"
            "\nfar left to the far right edge of the page; do not stop until"
            "\nthe bottom of the rightmost column. Skipping a column or a"
            "\nlist is a critical error.")
        temperature = 0.2

    todo = con.execute("""
        SELECT DISTINCT t.issue_urn, t.page_no, i.path
        FROM target_pages t JOIN issue_pdfs i USING (issue_urn)
        WHERE (t.issue_urn, t.page_no) NOT IN
              (SELECT issue_urn, page_no FROM ocr_pages)
        ORDER BY t.issue_urn, t.page_no
    """).fetchall()
    if args.limit:
        todo = todo[:args.limit]
    print(f"Step 4: {len(todo)} pages to OCR with {MODEL} at {DPI} dpi")
    if args.dry_run or not todo:
        coverage_qc(con)
        con.close()
        return

    key = api_key()
    bucket = TokenBucketRateLimiter(RATE)
    sem = asyncio.Semaphore(CONCURRENCY)
    lock = asyncio.Lock()
    stats = {"done": 0, "in_tok": 0, "out_tok": 0}
    timeout = aiohttp.ClientTimeout(total=600, connect=20)

    async def one(issue_urn, page_no, pdf_path, session, tmpdir):
        async with sem:
            jpeg = await asyncio.to_thread(
                render_page, pdf_path, page_no, tmpdir)
            try:
                text, finish, in_tok, out_tok = await transcribe(
                    session, bucket, key, jpeg, prompt, temperature)
            except Exception as e:
                print(f"  {issue_urn} p{page_no}: FAIL {e}")
                return
            async with lock:
                con.execute(
                    "INSERT OR REPLACE INTO ocr_pages "
                    "(issue_urn, page_no, model, dpi, ocr_text, "
                    " finish_reason, prompt_tokens, output_tokens) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    [issue_urn, page_no, MODEL, DPI, text, finish,
                     in_tok, out_tok])
                stats["done"] += 1
                stats["in_tok"] += in_tok
                stats["out_tok"] += out_tok
                if stats["done"] % 20 == 0:
                    print(f"  {stats['done']}/{len(todo)} pages "
                          f"({stats['in_tok']:,} in / "
                          f"{stats['out_tok']:,} out tokens)")
            if finish and finish != "STOP":
                print(f"  {issue_urn} p{page_no}: finishReason={finish} "
                      f"({out_tok} out tokens) — inspect")

    with tempfile.TemporaryDirectory() as tmpdir:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await asyncio.gather(*[
                one(u, p, path, session, tmpdir) for u, p, path in todo])

    print(f"\ndone: {stats['done']}/{len(todo)} pages, "
          f"{stats['in_tok']:,} input / {stats['out_tok']:,} output tokens")
    print(con.execute("""
        SELECT model, COUNT(*) AS pages, SUM(prompt_tokens) AS in_tok,
               SUM(output_tokens) AS out_tok,
               SUM(CASE WHEN finish_reason <> 'STOP' THEN 1 ELSE 0 END)
                   AS non_stop
        FROM ocr_pages GROUP BY 1
    """).fetchdf().to_string(index=False))
    coverage_qc(con)
    con.close()


def coverage_qc(con) -> None:
    """Flag pages whose transcription covers materially less content than
    the (bad but complete) embedded Delpher OCR — catches silently dropped
    columns. Digit groups are layout-independent, so their ratio is a robust
    volume proxy; numbered-entry counts double-check kandidatenlijst pages.
    Fix flagged pages with --redo-pages issue:page."""
    import re as _re
    rows = con.execute("""
        SELECT o.issue_urn, o.page_no, t.doc_role, o.ocr_text, p.text
        FROM ocr_pages o
        JOIN (SELECT DISTINCT issue_urn, page_no, doc_role
              FROM target_pages) t USING (issue_urn, page_no)
        JOIN page_texts p USING (issue_urn, page_no)
    """).fetchall()
    flagged = []
    for urn, pg, role, gem, emb in rows:
        e_dig = len(_re.findall(r"\d+", emb))
        g_dig = len(_re.findall(r"\d+", gem))
        bad = e_dig >= 40 and g_dig < 0.75 * e_dig
        if role == "kandidatenlijst":
            e_ent = len(_re.findall(r"^\s*\d{1,2}\.\s+\S", emb, _re.M))
            g_ent = len(_re.findall(r"^\s*\d{1,2}\.\s+\S", gem, _re.M))
            bad = bad or (e_ent >= 20 and g_ent < 0.75 * e_ent)
        if bad:
            flagged.append((urn, pg, role, e_dig, g_dig))
    if flagged:
        print(f"\nCOVERAGE WARNING — {len(flagged)} pages look incomplete "
              "(digit groups gemini vs delpher):")
        for urn, pg, role, e, g in flagged:
            print(f"  --redo-pages {urn}:{pg}   ({role}, {g}/{e})")
    else:
        print("\ncoverage QC: all transcribed pages within tolerance")


if __name__ == "__main__":
    asyncio.run(main())
