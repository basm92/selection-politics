"""
ind_step07_classify_religion.py  [GENEALOGIE PIPELINE - religion classification]

Classifies every person in genealogie.duckdb (`persons` table, ~3.7M rows) as
'Protestant', 'Katholiek', or 'Onbekend' from Dutch historical naming
conventions, using DeepSeek (deepseek-v4-flash) via langchain_deepseek.

Per-person signal
-----------------
Each person is classified from a small bundle (<= 6) of family names assembled
from the genealogie graph, in priority order DESCENDANTS-before-ANCESTORS:

    [self] + [direct children ...] + [father] + [grandfather]

deduplicated and capped at 6. The bundle is materialised once into a
`person_name_context` table so re-runs do not recompute the graph joins.

Storage / resumability
-----------------------
The source of truth during a run is an append-only side table

    religion_classifications(url PK, religion, status, classified_at)

`status` is 'ok' (religion holds the label, incl. a genuine 'Onbekend') or
'failed' (API error after retries — re-runnable). Pending = persons whose url is
absent from that table OR present with status='failed'. This keeps the hot path
to cheap inserts (no repeated full-table UPDATEs on the 3.7M-row persons table).

At the end of every run the side table is synced into two columns ON `persons`:

    religion         VARCHAR   -- the label (NULL until status='ok')
    religion_status  VARCHAR   -- NULL = not yet attempted, 'ok', or 'failed'

so the affiliation lives inside the database, while NULL (never attempted) stays
distinct from 'failed' (attempted, API error).

GO / NO-GO GATE
---------------
The repo's batched-JSON-with-idx-echo pattern is proven for OpenAI; DeepSeek's
JSON-mode behaviour at batch size 30 is NOT. ALWAYS validate first:

    uv run python code/data_wrangling/genealogie/ind_step07_classify_religion.py \
        --limit 100 --show-raw

Inspect that JSON parses, all idxs return, and labels are valid before the full
~3.7M-row run:

    uv run python code/data_wrangling/genealogie/ind_step07_classify_religion.py
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_deepseek import ChatDeepSeek

ROOT = Path(__file__).resolve().parents[3]
DB_PATH = ROOT / "data" / "genealogieonline" / "genealogie.duckdb"

MODEL = "deepseek-v4-flash"
BATCH_SIZE = 30        # persons per API call
CONCURRENCY = 16       # simultaneous API calls (matches step05)
MAX_NAMES = 6          # family names per person bundle
FLUSH_EVERY = 2000     # batches per processing wave (then insert + sync side table)

VALID = {"Protestant", "Katholiek", "Onbekend"}

SYSTEM_PROMPT = (
    "You are a helpful assistant classifying Dutch historical persons by religion. "
    "For each record you are given the full name of one FOCAL person together with "
    "a few of their close relatives' names (children, father, grandfather) for "
    "extra signal. Decide the most likely religion OF THE FOCAL PERSON: "
    "'Protestant', 'Katholiek', or 'Onbekend' (unknown). The first name in each "
    "record's name list is always the focal person; the others are relatives that "
    "help disambiguate. Base your judgment on Dutch historical naming conventions:\n"
    "- PROTESTANT families typically used vernacular Dutch or Germanic first names "
    "(Jan, Piet/Pieter, Klaas/Nicolaas, Hendrik, Gerrit, Dirk, Kees, Grietje, "
    "Trijntje, Neeltje, Aaltje, Antje) and biblical names in their Dutch form. "
    "Last names are often plain Dutch patronymics or occupational names.\n"
    "- CATHOLIC families typically used Latinised or saints' names, often with "
    "Latin endings: Josephus, Franciscus, Adrianus, Petrus, Wilhelmus, Antonius, "
    "Henricus, Gerardus, Joannes/Johannes (when alongside other Latin forms), "
    "Petronella, Catharina, Johanna, Hubertus, Lambertus, Bernardus, Theodorus, "
    "Leonardus. The consistent use of -us/-a Latin suffixes across the focal person "
    "and relatives is a strong Catholic indicator.\n"
    "- Use 'Onbekend' only when the names are genuinely ambiguous or too incomplete "
    "to classify confidently.\n"
    "Each input record has an 'idx' field. Return a JSON object with key "
    "'classifications' whose value is an array of objects, each with 'idx' "
    "(matching the input) and 'label' (one of 'Protestant', 'Katholiek', "
    "'Onbekend'). The array must contain exactly one entry per input record."
)


# ---------------------------------------------------------------------------
# Schema / context setup
# ---------------------------------------------------------------------------

def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Add the two persons columns and the append-only side table (idempotent)."""
    con.execute("ALTER TABLE persons ADD COLUMN IF NOT EXISTS religion VARCHAR")
    con.execute("ALTER TABLE persons ADD COLUMN IF NOT EXISTS religion_status VARCHAR")
    con.execute("""
        CREATE TABLE IF NOT EXISTS religion_classifications (
            url           VARCHAR PRIMARY KEY,
            religion      VARCHAR,
            status        VARCHAR NOT NULL,
            classified_at TIMESTAMP DEFAULT current_timestamp
        )
    """)


def build_context(con: duckdb.DuckDBPyConnection, rebuild: bool = False) -> None:
    """
    Materialise `person_name_context(url, names)` once. `names` is the ordered,
    deduplicated, <=MAX_NAMES list: [self] + children + [father, grandfather].
    Descendants (children) precede ancestors (father, grandfather) per spec.
    """
    exists = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name='person_name_context'"
    ).fetchone()
    if exists and not rebuild:
        return
    if exists:
        con.execute("DROP TABLE person_name_context")

    print("Building person_name_context (one-time graph assembly) ...", flush=True)
    con.execute(f"""
        CREATE TABLE person_name_context AS
        WITH self_n AS (
            SELECT
                url,
                COALESCE(person_name_full, person_name) AS self_name,
                father_url,
                father_name
            FROM persons
        ),
        kids AS (
            SELECT parent_url AS url, list(child_name) AS child_names
            FROM person_children
            WHERE child_name IS NOT NULL AND TRIM(child_name) <> ''
            GROUP BY parent_url
        ),
        gf AS (   -- grandfather = the father's own father_name
            SELECT s.url, fp.father_name AS grandfather_name
            FROM self_n s
            JOIN persons fp ON fp.url = s.father_url
        )
        SELECT
            s.url,
            list_slice(
                list_filter(
                    list_distinct(
                        list_concat(
                            [s.self_name],
                            COALESCE(k.child_names, []),
                            [s.father_name],
                            [gf.grandfather_name]
                        )
                    ),
                    x -> x IS NOT NULL AND length(TRIM(x)) > 0
                ),
                1, {MAX_NAMES}
            ) AS names
        FROM self_n s
        LEFT JOIN kids k ON k.url = s.url
        LEFT JOIN gf   ON gf.url = s.url
    """)
    n = con.execute("SELECT COUNT(*) FROM person_name_context").fetchone()[0]
    print(f"  person_name_context: {n:,} rows", flush=True)


def seed_nameless(con: duckdb.DuckDBPyConnection) -> int:
    """
    Persons whose bundle is empty have nothing to classify: write them as
    'Onbekend'/'ok' directly (no API call) so they never sit in 'pending'.
    """
    con.execute("""
        INSERT OR IGNORE INTO religion_classifications (url, religion, status)
        SELECT c.url, 'Onbekend', 'ok'
        FROM person_name_context c
        WHERE len(c.names) = 0
    """)
    return con.execute("SELECT COUNT(*) FROM person_name_context WHERE len(names) = 0").fetchone()[0]


def load_pending(con: duckdb.DuckDBPyConnection, limit: int | None) -> list[tuple[str, list[str]]]:
    """(url, names) for persons not yet 'ok' (absent from side table OR status='failed')."""
    lim = f"LIMIT {int(limit)}" if limit else ""
    rows = con.execute(f"""
        SELECT c.url, c.names
        FROM person_name_context c
        LEFT JOIN religion_classifications rc ON rc.url = c.url
        WHERE len(c.names) > 0
          AND (rc.url IS NULL OR rc.status = 'failed')
        ORDER BY c.url
        {lim}
    """).fetchall()
    return [(u, list(names)) for u, names in rows]


# ---------------------------------------------------------------------------
# Side-table writes + final sync to persons columns
# ---------------------------------------------------------------------------

def upsert(con: duckdb.DuckDBPyConnection, buffer: list[tuple]) -> None:
    if not buffer:
        return
    df = pd.DataFrame(buffer, columns=["url", "religion", "status", "classified_at"])
    con.execute("""
        INSERT OR REPLACE INTO religion_classifications
            SELECT url, religion, status, classified_at FROM df
    """)
    buffer.clear()


def sync_persons(con: duckdb.DuckDBPyConnection) -> None:
    """One full UPDATE of persons.religion/religion_status from the side table."""
    print("Syncing labels onto persons.religion / persons.religion_status ...", flush=True)
    con.execute("""
        UPDATE persons
        SET religion = rc.religion, religion_status = rc.status
        FROM religion_classifications rc
        WHERE persons.url = rc.url
    """)


# ---------------------------------------------------------------------------
# DeepSeek batch classification
# ---------------------------------------------------------------------------

def _parse_json(content: str) -> dict:
    """Defensive JSON extraction: direct, fenced, then first {...} block."""
    try:
        return json.loads(content)
    except Exception:
        pass
    stripped = re.sub(r"^```(?:json)?|```$", "", content.strip(), flags=re.MULTILINE).strip()
    try:
        return json.loads(stripped)
    except Exception:
        pass
    m = re.search(r"\{.*\}", content, re.DOTALL)
    if m:
        return json.loads(m.group(0))
    raise ValueError("no JSON object found in response")


async def classify_batch(
    chat,
    sem: asyncio.Semaphore,
    batch: list[tuple[str, list[str]]],
    show_raw: bool = False,
) -> list[tuple]:
    """Classify one batch. Returns [(url, religion, status, ts), ...]."""
    urls = [b[0] for b in batch]
    records = [{"idx": i, "names": b[1]} for i, b in enumerate(batch)]
    user_content = (
        f"Classify the religion of the focal person in each of the following "
        f"{len(batch)} Dutch records. Return a JSON object with key "
        f"'classifications' containing an array of exactly {len(batch)} objects, "
        "each with 'idx' (integer, matching the input) and 'label' (one of "
        "'Protestant', 'Katholiek', 'Onbekend').\n\nRecords:\n"
        + json.dumps(records, ensure_ascii=False)
    )
    messages = [SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=user_content)]

    async with sem:
        for attempt in range(5):
            try:
                resp = await chat.ainvoke(messages)
                content = resp.content if isinstance(resp.content, str) else str(resp.content)
                if show_raw:
                    print(f"\n--- raw response (batch of {len(batch)}) ---\n{content}\n", flush=True)
                data = _parse_json(content)
                raw = data.get("classifications", [])

                idx_to_label: dict[int, str] = {}
                for entry in raw:
                    if isinstance(entry, dict):
                        idx = entry.get("idx")
                        label = entry.get("label", "Onbekend")
                        if isinstance(idx, int) and 0 <= idx < len(batch):
                            idx_to_label[idx] = label if label in VALID else "Onbekend"

                missing = [i for i in range(len(batch)) if i not in idx_to_label]
                if missing:
                    print(
                        f"  [warn] {len(missing)} idx(es) missing "
                        f"{missing[:5]}{'...' if len(missing) > 5 else ''}; filling Onbekend",
                        flush=True,
                    )
                now = datetime.now(timezone.utc)
                return [(urls[i], idx_to_label.get(i, "Onbekend"), "ok", now) for i in range(len(batch))]

            except Exception as exc:
                wait = 2 ** attempt
                print(f"  [warn] API error (attempt {attempt + 1}/5): {exc}; retry in {wait}s", flush=True)
                await asyncio.sleep(wait)

    # All retries exhausted -> mark this batch 'failed' (re-runnable), religion NULL.
    print(f"  [error] batch failed after 5 attempts; marking {len(batch)} as 'failed'", flush=True)
    now = datetime.now(timezone.utc)
    return [(u, None, "failed", now) for u in urls]


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

async def run(con, chat, batch_size, concurrency, limit, show_raw) -> None:
    pending = load_pending(con, limit)
    total = len(pending)
    done_ok = con.execute("SELECT COUNT(*) FROM religion_classifications WHERE status='ok'").fetchone()[0]
    print(f"Already classified (ok): {done_ok:,}")
    print(f"Pending                : {total:,}")
    if total == 0:
        print("Nothing to do.")
        return

    batches = [pending[i:i + batch_size] for i in range(0, total, batch_size)]
    print(f"Batches: {len(batches):,}  (~{batch_size}/call, {concurrency} concurrent)", flush=True)

    sem = asyncio.Semaphore(concurrency)
    buffer: list[tuple] = []
    n_done = 0
    t0 = time.monotonic()

    # Process in waves so memory stays bounded and we checkpoint regularly.
    for w in range(0, len(batches), FLUSH_EVERY):
        wave = batches[w:w + FLUSH_EVERY]
        coros = [classify_batch(chat, sem, b, show_raw) for b in wave]
        for fut in asyncio.as_completed(coros):
            results = await fut
            buffer.extend(results)
            n_done += len(results)
        upsert(con, buffer)
        elapsed = time.monotonic() - t0
        rate = n_done / elapsed if elapsed else float("inf")
        eta = (total - n_done) / rate / 60 if rate else float("inf")
        print(
            f"  {n_done:,}/{total:,} ({100 * n_done / total:.1f}%)  "
            f"{rate:.1f} rec/s  ETA {eta:.0f} min",
            flush=True,
        )

    upsert(con, buffer)
    print(f"Done. Classified {n_done:,} persons in {(time.monotonic()-t0)/60:.1f} min.", flush=True)


def report(con: duckdb.DuckDBPyConnection) -> None:
    dist = con.execute("""
        SELECT COALESCE(religion_status,'(not attempted)') AS status,
               COALESCE(religion,'-') AS religion,
               COUNT(*) AS n
        FROM persons
        GROUP BY 1, 2 ORDER BY n DESC
    """).df()
    print("\npersons by religion_status / religion:")
    print(dist.to_string(index=False))


def main() -> None:
    p = argparse.ArgumentParser(description="Classify genealogie persons by religion via DeepSeek.")
    p.add_argument("--db", default=str(DB_PATH))
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    p.add_argument("--concurrency", type=int, default=CONCURRENCY)
    p.add_argument("--limit", type=int, default=None, help="Classify at most N pending persons (validation gate)")
    p.add_argument("--show-raw", action="store_true", help="Print raw model responses (use with a small --limit)")
    p.add_argument("--rebuild-context", action="store_true", help="Rebuild person_name_context from the graph")
    args = p.parse_args()

    load_dotenv(ROOT / ".env")
    if not os.getenv("DEEPSEEK_API_KEY"):
        raise SystemExit("DEEPSEEK_API_KEY missing from environment / .env")

    print(f"Connecting to DuckDB at {args.db} ...", flush=True)
    con = duckdb.connect(args.db)
    ensure_schema(con)
    build_context(con, rebuild=args.rebuild_context)
    n_nameless = seed_nameless(con)
    print(f"Nameless persons pre-marked 'Onbekend'/'ok': {n_nameless:,}", flush=True)

    chat = ChatDeepSeek(model=MODEL, temperature=0.0, max_retries=5).bind(
        response_format={"type": "json_object"}
    )

    asyncio.run(run(con, chat, args.batch_size, args.concurrency, args.limit, args.show_raw))

    sync_persons(con)
    report(con)
    con.close()


if __name__ == "__main__":
    main()
