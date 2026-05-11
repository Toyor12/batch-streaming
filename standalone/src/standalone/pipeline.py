"""Day 19 -- Batch vs Streaming Architecture Tradeoffs (plain Python).

Demonstrates batch and streaming processing paths for the same dataset,
a serving layer that merges results, and an architecture recommendation
function.  Streaming is simulated with file-based events (no Kafka).
"""

import csv
import json
import logging
from collections import defaultdict
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
LAKE_DIR = Path(__file__).parent / "lake"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_lake_dirs() -> tuple[Path, Path]:
    """Create lake/batch/ and lake/streaming/ directories."""
    raise NotImplementedError("TODO: implement")


def _clean_orders(rows: list[dict]) -> list[dict]:
    """Shared cleaning logic for both batch and streaming paths.

    1. Drop rows missing order_id, customer_id, or product_id
    2. Keep only rows with quantity > 0 and price > 0
    3. Add revenue = quantity * price
    4. Normalise status to lowercase/stripped
    """
    raise NotImplementedError("TODO: implement")


def _aggregate_by_category(rows: list[dict]) -> list[dict]:
    """Aggregate cleaned rows by category -> total_revenue, order_count."""
    raise NotImplementedError("TODO: implement")


def _get_sample_events() -> list[dict]:
    """Simulate streaming events (provided plumbing -- first 5 CSV rows as dicts)."""
    with open(DATA_DIR / "orders.csv", newline="") as f:
        return list(csv.DictReader(f))[:5]


# ---------------------------------------------------------------------------
# Architecture recommendation
# ---------------------------------------------------------------------------

def recommend_architecture(
    latency_requirement: str,
    budget: str,
    data_volume: str,
    reprocessing_needed: bool,
) -> str:
    """Recommend batch, kappa, or lambda based on requirements.

    Decision logic:
    1. hours latency OR low budget -> "batch"
    2. no reprocessing AND not large data -> "kappa"
    3. otherwise -> "lambda"
    """
    raise NotImplementedError("TODO: implement")


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def batch_orders() -> list[dict]:
    """Batch path: read full CSV, clean, aggregate by category."""
    raise NotImplementedError("TODO: implement")


def streaming_orders() -> list[dict]:
    """Streaming simulation path: process file-based events, clean, aggregate."""
    raise NotImplementedError("TODO: implement")


def serving_layer(batch: list[dict], streaming: list[dict]) -> list[dict]:
    """Merge batch and streaming results; batch wins on duplicates."""
    raise NotImplementedError("TODO: implement")


def consistency_check(batch: list[dict], streaming: list[dict]) -> dict:
    """Verify batch and streaming produce consistent results for shared data."""
    raise NotImplementedError("TODO: implement")


def write_results(results: list[dict], name: str) -> Path:
    """Write aggregation results to lake/ as JSON."""
    raise NotImplementedError("TODO: implement")


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    """Execute the full pipeline and print each stage so you can see what changed.

    Provided as plumbing -- you do not need to edit this. As you implement the
    tasks in order, re-run `uv run python -m standalone` and watch more stages
    light up. Stages that depend on unimplemented tasks will raise
    NotImplementedError; that is your cue for what to build next.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("\n=== BATCH PATH ===")
    batch = batch_orders()
    for row in batch:
        print(f"  {row}")

    print("\n=== STREAMING PATH ===")
    streaming = streaming_orders()
    for row in streaming:
        print(f"  {row}")

    print("\n=== SERVING LAYER (batch wins on duplicates) ===")
    served = serving_layer(batch, streaming)
    for row in served:
        print(f"  {row}")

    print("\n=== CONSISTENCY CHECK ===")
    print(f"  {consistency_check(batch, streaming)}")

    print("\n=== ARCHITECTURE RECOMMENDATIONS ===")
    examples = [
        ("hours", "low", "small", False),
        ("seconds", "high", "small", False),
        ("minutes", "high", "large", True),
    ]
    for args in examples:
        print(f"  {args} -> {recommend_architecture(*args)}")

    print("\n=== WRITING RESULTS TO LAKE ===")
    _ensure_lake_dirs()
    print(f"  batch     -> {write_results(batch, 'batch/results')}")
    print(f"  streaming -> {write_results(streaming, 'streaming/results')}")
    print(f"  serving   -> {write_results(served, 'serving')}")
