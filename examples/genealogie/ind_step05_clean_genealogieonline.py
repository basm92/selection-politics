"""
ind_step05_clean_genealogieonline.py

Clean Dutch historical profession strings on the merged genealogie database by
sending each unique string through DeepSeek (v4-flash) via LangChain. Results are
cached in a local SQLite cache so the script is fully resumable: re-running picks
up exactly where a previous run left off and re-processes nothing.

Reads the canonical `pairs` table in genealogie.duckdb, which spans BOTH eras
(source='socmob_1750_1900', 1750-1900; source='births_1500_1800', 1500-1800).
The output parquet therefore covers both eras; the `source` column lets
downstream analyses subset to the original socmob sample. Because the per-string
SQLite cache and the "string not already in lookup" guard are preserved, the raw
-> clean values for the socmob era are byte-stable across this migration.

Inputs:
    data/genealogieonline/genealogie.duckdb  (pairs table; built by the merge +
                                              lineage steps)

Outputs:
    data/genealogieonline/profession_lookup.parquet     (raw -> cleaned map, both eras)
    data/genealogieonline/socmob_pairs_cleaned.parquet  (widened, source-flagged pairs)
    data/genealogieonline/.langchain_cache.sqlite       (LLM response cache)

Usage:
    uv run python code/data_wrangling/genealogie/ind_step05_clean_genealogieonline.py
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import duckdb
import polars as pl
from dotenv import load_dotenv
from langchain_community.cache import SQLiteCache
from langchain_core.globals import set_llm_cache
from langchain_core.messages import HumanMessage
from langchain_deepseek import ChatDeepSeek
from tqdm.asyncio import tqdm_asyncio

ROOT = Path(__file__).resolve().parents[3]   # project root (file is in code/data_wrangling/genealogie/)
DATA_DIR = ROOT / "data" / "genealogieonline"
DUCKDB_PATH = DATA_DIR / "genealogie.duckdb"
LOOKUP_PARQUET = DATA_DIR / "profession_lookup.parquet"
PAIRS_OUT = DATA_DIR / "socmob_pairs_cleaned.parquet"
CACHE_PATH = DATA_DIR / ".langchain_cache.sqlite"

MODEL = "deepseek-v4-flash"
MAX_CONCURRENCY = 16
CHECKPOINT_EVERY = 500


def build_prompt(raw: str) -> str:
    return (
        f'Clean this Dutch historical profession string: "{raw}". '
        "Return ONLY a single lowercase profession name in Dutch. "
        "If multiple professions are mentioned, return only the first one. "
        "Remove digits, dates, place names, and anything in parentheses. "
        "Output nothing else - just the occupation word(s)."
    )


def load_unique_strings() -> list[str]:
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    df = con.execute(
        """
        SELECT son_beroep AS s FROM pairs WHERE son_beroep IS NOT NULL
        UNION
        SELECT father_beroep AS s FROM pairs WHERE father_beroep IS NOT NULL
        """
    ).pl()
    con.close()
    strings = (
        df["s"].str.strip_chars().filter(df["s"].str.strip_chars().str.len_chars() > 0).unique().to_list()
    )
    return sorted(strings)


def load_existing_lookup() -> dict[str, str]:
    if not LOOKUP_PARQUET.exists():
        return {}
    df = pl.read_parquet(LOOKUP_PARQUET)
    return dict(zip(df["raw"].to_list(), df["cleaned_profession"].to_list()))


def save_lookup(lookup: dict[str, str]) -> None:
    df = pl.DataFrame(
        {"raw": list(lookup.keys()), "cleaned_profession": list(lookup.values())}
    )
    tmp = LOOKUP_PARQUET.with_suffix(".parquet.tmp")
    df.write_parquet(tmp)
    tmp.replace(LOOKUP_PARQUET)


async def clean_one(
    chat: ChatDeepSeek, raw: str, sem: asyncio.Semaphore
) -> tuple[str, str | None]:
    prompt = build_prompt(raw)
    async with sem:
        try:
            resp = await chat.ainvoke([HumanMessage(content=prompt)])
            return raw, resp.content.strip().lower()
        except Exception as exc:
            print(f"  ! failed on {raw!r}: {exc}")
            return raw, None


async def run() -> None:
    load_dotenv(ROOT / ".env")
    if not os.getenv("DEEPSEEK_API_KEY"):
        raise RuntimeError("DEEPSEEK_API_KEY missing from environment / .env")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    set_llm_cache(SQLiteCache(database_path=str(CACHE_PATH)))

    unique_strings = load_unique_strings()
    print(f"Total unique profession strings: {len(unique_strings)}")

    lookup = load_existing_lookup()
    todo = [s for s in unique_strings if s not in lookup]
    print(f"  already cleaned (parquet checkpoint): {len(lookup)}")
    print(f"  to process this run:                 {len(todo)}")

    if not todo:
        print("Nothing to do; building joined output.")
    else:
        chat = ChatDeepSeek(model=MODEL, temperature=0.0, max_retries=5)
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        tasks = [clean_one(chat, s, sem) for s in todo]

        processed_since_checkpoint = 0
        for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="cleaning"):
            raw, cleaned = await coro
            if cleaned is not None:
                lookup[raw] = cleaned
            processed_since_checkpoint += 1
            if processed_since_checkpoint >= CHECKPOINT_EVERY:
                save_lookup(lookup)
                processed_since_checkpoint = 0

        save_lookup(lookup)

    missing = [s for s in unique_strings if s not in lookup]
    if missing:
        print(f"WARNING: {len(missing)} strings still unresolved after run.")
        print("Re-run the script to retry; the SQLite cache will skip already-cleaned items.")
    else:
        print("All unique strings cleaned.")

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    # Widened, source-flagged pairs. The original five columns (amco, son_beroep,
    # father_beroep, + the two *_clean joins below) are preserved exactly for the
    # socmob era; everything else is additive lineage/metadata (NA where absent)
    # so no variable is dropped.
    pairs = con.execute(
        """
        SELECT
            p.amco, p.son_beroep, p.father_beroep,
            p.source,
            p.son_url, p.father_url,
            p.son_name, p.father_name,
            p.son_birth_year, p.father_birth_year,
            p.son_birth_place, p.father_birth_place,
            ps.source_tree AS son_source_tree,
            p.dynasty_id, p.generation_depth
        FROM pairs p
        LEFT JOIN persons ps ON ps.url = p.son_url
        WHERE p.son_beroep IS NOT NULL AND p.father_beroep IS NOT NULL
        """
    ).pl()
    con.close()

    lookup_df = pl.DataFrame(
        {"raw": list(lookup.keys()), "cleaned_profession": list(lookup.values())}
    )

    pairs_cleaned = (
        pairs.join(
            lookup_df.rename({"raw": "son_beroep", "cleaned_profession": "son_beroep_clean"}),
            on="son_beroep",
            how="left",
        )
        .join(
            lookup_df.rename(
                {"raw": "father_beroep", "cleaned_profession": "father_beroep_clean"}
            ),
            on="father_beroep",
            how="left",
        )
        # Keep the original five columns first for readability / stable diffs.
        .select(
            "amco", "son_beroep", "father_beroep",
            "son_beroep_clean", "father_beroep_clean",
            "source",
            "son_url", "father_url", "son_name", "father_name",
            "son_birth_year", "father_birth_year",
            "son_birth_place", "father_birth_place",
            "son_source_tree", "dynasty_id", "generation_depth",
        )
    )

    pairs_cleaned.write_parquet(PAIRS_OUT)
    n_socmob = pairs_cleaned.filter(pl.col("source") == "socmob_1750_1900").height
    print(f"Written {pairs_cleaned.height} rows to {PAIRS_OUT.relative_to(ROOT)} "
          f"({n_socmob} socmob_1750_1900, {pairs_cleaned.height - n_socmob} births_1500_1800)")


if __name__ == "__main__":
    asyncio.run(run())
