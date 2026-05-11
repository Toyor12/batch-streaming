"""Day 19 -- Batch vs Streaming Architecture Tradeoffs (plain Python).

Implements batch and streaming processing paths over the same orders dataset,
a serving layer that merges results with batch as source of truth, and an
architecture recommendation function. Streaming is simulated with file-based
events (no Kafka).
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
    """Create lake/batch/ and lake/streaming/ directories if they do not exist.

    Returns the batch and streaming directory paths as a tuple.
    """
    batch_dir = LAKE_DIR / "batch"
    streaming_dir = LAKE_DIR / "streaming"
    batch_dir.mkdir(parents=True, exist_ok=True)
    streaming_dir.mkdir(parents=True, exist_ok=True)
    return batch_dir, streaming_dir


def _clean_orders(rows: list[dict]) -> list[dict]:
    """Drop invalid rows, add revenue = quantity * price, normalise status to lowercase/stripped.

    Shared by both batch and streaming paths to guarantee identical cleaning logic.
    """
    cleaned = []
    for row in rows:
        quantity = float(row.get("quantity", 0))
        price = float(row.get("price", 0))
        if not row.get("order_id") or not row.get("customer_id") or not row.get("product_id"):
            continue
        if quantity <= 0 or price <= 0:
            continue
        row["revenue"] = quantity * price
        row["status"] = row.get("status", "").lower().strip()
        cleaned.append(row)
    return cleaned


def _aggregate_by_category(rows: list[dict]) -> list[dict]:
    """Return a list of {category, total_revenue, order_count} dicts, one per category."""
    agg = defaultdict(lambda: {"total_revenue": 0.0, "order_count": 0})
    for row in rows:
        cat = row["category"]
        agg[cat]["total_revenue"] += row["revenue"]
        agg[cat]["order_count"] += 1
    result = []
    for cat, values in agg.items():
        result.append({"category": cat, "total_revenue": values["total_revenue"], "order_count": values["order_count"]})
    return result


def _get_sample_events() -> list[dict]:
    """Simulate streaming events (provided plumbing -- first 5 CSV rows as dicts)."""
    with open(DATA_DIR / "orders.csv", newline="") as f:
        return list(csv.DictReader(f))[:5]


# ---------------------------------------------------------------------------
# Architecture recommendation
# ---------------------------------------------------------------------------

def recommend_architecture(latency_requirement, budget, data_volume, reprocessing_needed) -> str:
    """Return 'batch', 'kappa', or 'lambda' based on the four input requirements.

    Rules: hours latency or low budget -> batch. No reprocessing and not large data -> kappa. Otherwise -> lambda.
    Note: a single string output hides tensions between inputs -- see SCENARIOS.md for critique.
    """
    if latency_requirement == "hours" or budget == "low":
        return "batch"
    elif reprocessing_needed == False and data_volume != "large":
        return "kappa"
    else:
        return "lambda"


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def batch_orders() -> list[dict]:
    """Read the full orders CSV, clean it, and return aggregated revenue and order count per category."""
    with open(DATA_DIR / "orders.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    cleaned = _clean_orders(rows)
    return _aggregate_by_category(cleaned)


def streaming_orders() -> list[dict]:
    """Process simulated stream events (first 5 CSV rows), clean, and return aggregated revenue and order count per category."""
    events = _get_sample_events()
    cleaned = _clean_orders(events)
    return _aggregate_by_category(cleaned)


def serving_layer(batch: list[dict], streaming: list[dict]) -> list[dict]:
    """Merge batch and streaming aggregates into one list sorted by category.

    Batch is source of truth -- if a category appears in both, the batch numbers win.
    This works because batch is iterated first, so streaming duplicates are skipped.
    """
    seen = set()
    merged = []
    for row in batch + streaming:
        if row["category"] not in seen:
            seen.add(row["category"])
            merged.append(row)
    return sorted(merged, key=lambda r: r["category"])


def consistency_check(batch: list[dict], streaming: list[dict]) -> dict:
    """Compare batch and streaming category sets.

    Returns overlapping_categories (count), batch_only (list), and stream_only (list).
    """
    batch_cats = {r["category"] for r in batch}
    streaming_cats = {r["category"] for r in streaming}
    return {
        "overlapping_categories": len(batch_cats & streaming_cats),
        "batch_only": sorted(batch_cats - streaming_cats),
        "stream_only": sorted(streaming_cats - batch_cats),
    }


def write_results(results: list[dict], name: str) -> Path:
    """Write results as JSON to LAKE_DIR/<name>.json and return the file path."""
    path = LAKE_DIR / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(results, f)
    return path


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    """Execute the full pipeline and print each stage.

    Re-run after implementing each task to see the next stage light up.
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