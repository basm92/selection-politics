# =============================================================================
# openarch_step3_classify_religion.py  [OPENARCHIEF PIPELINE - STEP 3]
# Input:  data/openarchive/marriages.duckdb  (marriages_raw table)
# Output: data/openarchive/marriages.duckdb  (religion_classifications table)
#         data/openarchive/parquet/archive_code=xxx/part-N.parquet
#
# For every unique marriage in marriages_raw where at least one person has a
# recorded profession, classifies the marriage as 'Protestant', 'Katholiek',
# or 'Onbekend' using the full names of all persons in the record.
#
# Uses gpt-5-nano in batch mode (BATCH_SIZE marriages per API call) with
# async concurrency (CONCURRENCY simultaneous calls) and automatic retry on
# rate-limit errors.  Fully resumable: already-classified identifiers are
# skipped on re-run.
#
# Run:
#   uv run python code/data_wrangling/openarch/openarch_step3_classify_religion.py
# =============================================================================
import argparse
import asyncio
import json
import os
import time
from datetime import datetime, timezone

import duckdb
import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH     = "./data/openarchive/marriages.duckdb"
PARQUET_DIR = "./data/openarchive/parquet"
MODEL       = "gpt-5-nano"
BATCH_SIZE  = 30    # marriages per API call
CONCURRENCY = 20    # simultaneous API calls
FLUSH_EVERY = 500   # rows to buffer before writing to DuckDB

SYSTEM_PROMPT = (
    "You are a helpful assistant classifying Dutch historical marriage records "
    "by religion. Given the full names of all persons in a marriage record "
    "(groom, bride, their fathers, mothers, and witnesses), decide whether the "
    "marriage is most likely 'Protestant', 'Katholiek', or 'Onbekend' (unknown). "
    "Base your judgment on Dutch historical naming conventions:\n"
    "- PROTESTANT families typically used vernacular Dutch or Germanic first names "
    "(Jan, Piet/Pieter, Klaas/Nicolaas, Hendrik, Gerrit, Dirk, Kees, "
    "Grietje, Trijntje, Neeltje, Aaltje, Antje) and biblical names in their "
    "Dutch form. Last names are often "
    "plain Dutch patronymics or occupational names.\n"
    "- CATHOLIC families typically used Latinised or saints' names, often with "
    "Latin endings: Josephus, Franciscus, Adrianus, Petrus, Wilhelmus, Antonius, "
    "Henricus, Gerardus, Joannes/Johannes (when appearing alongside other Latin "
    "forms), Petronella, Catharina, Johanna, Hubertus, Lambertus, Bernardus, "
    "Theodorus, Leonardus. The consistent use of -us/-a Latin suffixes across "
    "multiple persons in the same record is a strong Catholic indicator.\n"
    "- Use 'Onbekend' only when names are genuinely ambiguous or too incomplete "
    "to classify confidently.\n"
    "Each input record has an 'idx' field. Return a JSON object with key "
    "'classifications' whose value is an array of objects, each with 'idx' "
    "(matching the input) and 'label' (one of 'Protestant', 'Katholiek', "
    "'Onbekend'). The array must contain exactly one entry per input record."
)

# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------

DDL_CLASSIFICATIONS = """
CREATE TABLE IF NOT EXISTS religion_classifications (
    identifier    VARCHAR PRIMARY KEY,
    religion      VARCHAR NOT NULL,
    classified_at TIMESTAMP DEFAULT current_timestamp
);
"""


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL_CLASSIFICATIONS)


def load_pending(
    con: duckdb.DuckDBPyConnection,
) -> list[tuple[str, list[str]]]:
    """
    Return (identifier, all_names) for every marriage that:
      - has at least one person with a non-null, non-empty profession
      - has NOT yet been classified
    Names are the full names of ALL persons in the record, joined and cleaned.
    """
    rows = con.execute("""
        SELECT
            m.identifier,
            list(
                TRIM(
                    COALESCE(m.first_name, '')
                    || CASE WHEN m.prefix_last_name IS NOT NULL
                            THEN ' ' || m.prefix_last_name
                            ELSE '' END
                    || CASE WHEN m.last_name IS NOT NULL
                            THEN ' ' || m.last_name
                            ELSE '' END
                )
            ) AS all_names
        FROM marriages_raw m
        WHERE m.identifier NOT IN (
            SELECT identifier FROM religion_classifications
        )
        GROUP BY m.identifier
        HAVING bool_or(
            m.profession IS NOT NULL AND TRIM(m.profession) != ''
        )
        ORDER BY m.identifier
    """).fetchall()

    # Strip empty strings from name lists and deduplicate
    cleaned = []
    for identifier, names in rows:
        names_clean = list(dict.fromkeys(
            n for n in names if n.strip()
        ))
        cleaned.append((identifier, names_clean))
    return cleaned


def load_unknown(
    con: duckdb.DuckDBPyConnection,
) -> list[tuple[str, list[str]]]:
    """
    Return (identifier, all_names) for every marriage already classified as
    'Onbekend', so it can be re-run through the classifier.
    """
    rows = con.execute("""
        SELECT
            m.identifier,
            list(
                TRIM(
                    COALESCE(m.first_name, '')
                    || CASE WHEN m.prefix_last_name IS NOT NULL
                            THEN ' ' || m.prefix_last_name
                            ELSE '' END
                    || CASE WHEN m.last_name IS NOT NULL
                            THEN ' ' || m.last_name
                            ELSE '' END
                )
            ) AS all_names
        FROM marriages_raw m
        INNER JOIN religion_classifications rc
            ON m.identifier = rc.identifier AND rc.religion = 'Onbekend'
        GROUP BY m.identifier
        ORDER BY m.identifier
    """).fetchall()

    cleaned = []
    for identifier, names in rows:
        names_clean = list(dict.fromkeys(n for n in names if n.strip()))
        cleaned.append((identifier, names_clean))
    return cleaned


def flush(
    con: duckdb.DuckDBPyConnection,
    buffer: list[tuple],
    upsert: bool = False,
) -> None:
    if not buffer:
        return
    df = pd.DataFrame(buffer, columns=["identifier", "religion", "classified_at"])
    if upsert:
        con.execute("""
            INSERT OR REPLACE INTO religion_classifications
                SELECT identifier, religion, classified_at FROM df
        """)
    else:
        con.execute("""
            INSERT OR IGNORE INTO religion_classifications
                SELECT identifier, religion, classified_at FROM df
        """)
    buffer.clear()


# ---------------------------------------------------------------------------
# OpenAI batch classification
# ---------------------------------------------------------------------------

VALID = {"Protestant", "Katholiek", "Onbekend"}


async def classify_batch(
    client: AsyncOpenAI,
    sem: asyncio.Semaphore,
    batch: list[tuple[str, list[str]]],
) -> list[tuple[str, str]]:
    """
    Classify a batch of marriages in a single API call.
    Returns list of (identifier, religion, classified_at).

    Records are sent as {idx, names} objects so the model echoes back each idx
    explicitly — this eliminates positional misalignment even if the model
    reorders or skips entries, since we map by idx rather than by position.
    Falls back to 'Onbekend' for any missing or malformed entry.
    """
    identifiers = [b[0] for b in batch]
    records = [
        {"idx": i, "names": b[1]}
        for i, b in enumerate(batch)
    ]

    user_content = (
        f"Classify the following {len(batch)} Dutch marriage records. "
        "Return a JSON object with key 'classifications' containing an array "
        f"of exactly {len(batch)} objects, each with 'idx' (integer, matching "
        "the input) and 'label' (one of 'Protestant', 'Katholiek', 'Onbekend').\n\n"
        "Records:\n"
        + json.dumps(records, ensure_ascii=False)
    )

    async with sem:
        for attempt in range(5):
            try:
                response = await client.chat.completions.create(
                    model=MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user",   "content": user_content},
                    ],
                    response_format={"type": "json_object"},
                    temperature=1.0,
                )
                data = json.loads(response.choices[0].message.content)
                raw = data.get("classifications", [])

                # Build an idx→label map; any missing idx falls back to Onbekend
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
                        f"  [warn] {len(missing)} idx(es) missing from response "
                        f"{missing[:5]}{'...' if len(missing) > 5 else ''}; "
                        "filling with Onbekend",
                        flush=True,
                    )

                now = datetime.now(timezone.utc)
                return [
                    (identifiers[i], idx_to_label.get(i, "Onbekend"), now)
                    for i in range(len(batch))
                ]

            except Exception as exc:
                wait = 2 ** attempt
                print(
                    f"  [warn] API error (attempt {attempt + 1}/5): {exc}; "
                    f"retrying in {wait}s",
                    flush=True,
                )
                await asyncio.sleep(wait)

    # All retries exhausted — fall back to Onbekend for every record
    print(
        f"  [error] batch failed after 5 attempts; "
        f"marking {len(batch)} records as Onbekend",
        flush=True,
    )
    now = datetime.now(timezone.utc)
    return [(ident, "Onbekend", now) for ident in identifiers]


# ---------------------------------------------------------------------------
# Main async pipeline
# ---------------------------------------------------------------------------

async def run(
    con: duckdb.DuckDBPyConnection,
    client: AsyncOpenAI,
    batch_size: int,
    concurrency: int,
    limit: int | None = None,
    recode_unknown: bool = False,
) -> None:
    if recode_unknown:
        pending = load_unknown(con)
        n_unknown = len(pending)
        print(f"Recode-unknown mode: {n_unknown:,} 'Onbekend' records to reclassify")
    else:
        pending = load_pending(con)

    if limit is not None:
        pending = pending[:limit]
    total = len(pending)

    already_done = con.execute(
        "SELECT count(*) FROM religion_classifications"
    ).fetchone()[0]

    if not recode_unknown:
        print(f"Already classified : {already_done:,}")
    print(f"Pending            : {total:,}")

    if total == 0:
        print("Nothing to do.")
        return

    # Split into batches
    batches = [
        pending[i : i + batch_size]
        for i in range(0, total, batch_size)
    ]
    print(
        f"Batches            : {len(batches):,}  "
        f"(~{batch_size} marriages/call, {concurrency} concurrent)",
        flush=True,
    )

    sem = asyncio.Semaphore(concurrency)
    buffer: list[tuple] = []
    n_done = 0
    t0 = time.monotonic()

    coros = [classify_batch(client, sem, b) for b in batches]

    for fut in asyncio.as_completed(coros):
        results = await fut
        buffer.extend(results)
        n_done += len(results)

        if len(buffer) >= FLUSH_EVERY:
            flush(con, buffer, upsert=recode_unknown)
            elapsed = time.monotonic() - t0
            rate = n_done / elapsed if elapsed > 0 else float("inf")
            eta = (total - n_done) / rate / 60 if rate > 0 else float("inf")
            print(
                f"  {n_done:,}/{total:,} ({100 * n_done / total:.1f}%)  "
                f"{rate:.1f} rec/s  ETA {eta:.0f} min",
                flush=True,
            )

    flush(con, buffer, upsert=recode_unknown)
    elapsed = time.monotonic() - t0
    print(
        f"Done. Classified {n_done:,} marriages in {elapsed/60:.1f} min.",
        flush=True,
    )
    _export_parquet(con)


# ---------------------------------------------------------------------------
# Parquet export
# ---------------------------------------------------------------------------

def _export_parquet(con: duckdb.DuckDBPyConnection) -> None:
    """Export marriages_raw joined with religion_classifications to partitioned parquet."""
    os.makedirs(PARQUET_DIR, exist_ok=True)
    n = con.execute("SELECT COUNT(*) FROM marriages_raw").fetchone()[0]
    if n == 0:
        print("marriages_raw is empty; skipping parquet export.")
        return
    print(f"Exporting {n:,} rows to {PARQUET_DIR}/ (partitioned by archive_code)...")
    con.execute(f"""
        COPY (
            SELECT
                m.identifier, m.archive_code, m.amco,
                m.eventdate_year, m.eventdate_month, m.eventdate_day,
                m.eventplace_raw, m.relation_type, m.person_pid,
                m.first_name, m.prefix_last_name, m.last_name,
                m.profession,
                m.birth_year, m.birth_month, m.birth_day,
                m.age_literal, m.age_years, m.source_date,
                m.fetched_at,
                rc.religion,
                rc.classified_at
            FROM marriages_raw m
            LEFT JOIN religion_classifications rc ON m.identifier = rc.identifier
        ) TO '{PARQUET_DIR}'
        (FORMAT PARQUET, PARTITION_BY (archive_code),
         OVERWRITE_OR_IGNORE TRUE, COMPRESSION ZSTD)
    """)
    print(f"Parquet export complete → {PARQUET_DIR}/archive_code=*/")

    summary = con.execute("""
        SELECT archive_code, COUNT(DISTINCT identifier) AS marriages, COUNT(*) AS person_rows
        FROM marriages_raw
        GROUP BY archive_code
        ORDER BY marriages DESC
        LIMIT 30
    """).df()
    print("\nTop archives by marriage count:")
    print(summary.to_string(index=False))

    occ_rate = con.execute("""
        SELECT
            relation_type,
            COUNT(*) AS n,
            ROUND(100.0 * SUM(CASE WHEN profession IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_with_profession
        FROM marriages_raw
        GROUP BY relation_type
        ORDER BY n DESC
    """).df()
    print("\nOccupation fill rate by role:")
    print(occ_rate.to_string(index=False))

    rel_dist = con.execute("""
        SELECT religion, COUNT(*) AS n,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM religion_classifications
        GROUP BY religion
        ORDER BY n DESC
    """).df()
    print("\nReligion classification distribution:")
    print(rel_dist.to_string(index=False))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Classify marriages in marriages.duckdb by religion using GPT-5.1-nano."
    )
    p.add_argument("--db",          default=DB_PATH,     help="Path to DuckDB file")
    p.add_argument("--batch-size",  type=int, default=BATCH_SIZE,  help="Marriages per API call")
    p.add_argument("--concurrency", type=int, default=CONCURRENCY, help="Simultaneous API calls")
    p.add_argument("--limit",          type=int, default=None,        help="Process at most N marriages (for testing)")
    p.add_argument("--recode-unknown", action="store_true",           help="Reclassify all marriages currently marked 'Onbekend'")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Connecting to DuckDB at {args.db} ...")
    con = duckdb.connect(args.db)
    ensure_table(con)

    client = AsyncOpenAI()  # reads OPENAI_API_KEY from env

    asyncio.run(run(con, client, args.batch_size, args.concurrency, args.limit, args.recode_unknown))

    con.close()


if __name__ == "__main__":
    main()
