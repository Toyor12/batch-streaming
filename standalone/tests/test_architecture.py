"""Tests for Day 19 Lab -- Batch vs Streaming Architecture Tradeoffs (standalone)."""

import csv
import json
import shutil
from collections import defaultdict
from pathlib import Path

import pytest

from standalone.pipeline import (
    LAKE_DIR,
    _aggregate_by_category,
    _clean_orders,
    _get_sample_events,
    batch_orders,
    consistency_check,
    recommend_architecture,
    serving_layer,
    streaming_orders,
    write_results,
)


@pytest.fixture
def orders_csv_path():
    return Path(__file__).parent.parent / "src" / "standalone" / "data" / "orders.csv"


@pytest.fixture
def sample_events():
    return _get_sample_events()


@pytest.fixture(autouse=True)
def clean_lake():
    """Remove lake directory before and after each test."""
    if LAKE_DIR.exists():
        shutil.rmtree(LAKE_DIR)
    yield
    if LAKE_DIR.exists():
        shutil.rmtree(LAKE_DIR)


# -- _clean_orders ----------------------------------------------------------


def test_clean_orders_adds_revenue(sample_events):
    """Cleaning adds a revenue column (quantity * price)."""
    cleaned = _clean_orders(sample_events)
    assert all("revenue" in r for r in cleaned)
    assert cleaned[0]["revenue"] == 2 * 129.99


def test_clean_orders_drops_missing_ids():
    """Rows with missing order_id / customer_id / product_id are dropped."""
    rows = [
        {"order_id": "", "customer_id": 1, "product_id": 1, "quantity": 1, "price": 10, "status": "ok", "category": "A"},
        {"order_id": 1, "customer_id": "", "product_id": 1, "quantity": 1, "price": 10, "status": "ok", "category": "A"},
        {"order_id": 1, "customer_id": 1, "product_id": 1, "quantity": 1, "price": 10, "status": "ok", "category": "A"},
    ]
    cleaned = _clean_orders(rows)
    assert len(cleaned) == 1


def test_clean_orders_drops_zero_quantity():
    """Rows with quantity <= 0 are dropped."""
    rows = [
        {"order_id": 1, "customer_id": 1, "product_id": 1, "quantity": 0, "price": 10, "status": "ok", "category": "A"},
        {"order_id": 2, "customer_id": 1, "product_id": 1, "quantity": 2, "price": 10, "status": "ok", "category": "A"},
    ]
    assert len(_clean_orders(rows)) == 1


def test_clean_orders_drops_zero_price():
    """Rows with price <= 0 are dropped."""
    rows = [
        {"order_id": 1, "customer_id": 1, "product_id": 1, "quantity": 1, "price": 0, "status": "ok", "category": "A"},
        {"order_id": 2, "customer_id": 1, "product_id": 1, "quantity": 1, "price": 5, "status": "ok", "category": "A"},
    ]
    assert len(_clean_orders(rows)) == 1


def test_clean_orders_normalises_status():
    """Status is lowercased and stripped."""
    rows = [
        {"order_id": 1, "customer_id": 1, "product_id": 1, "quantity": 1, "price": 10, "status": "  PENDING  ", "category": "A"},
    ]
    cleaned = _clean_orders(rows)
    assert cleaned[0]["status"] == "pending"


# -- _aggregate_by_category -------------------------------------------------


def test_aggregate_groups_correctly():
    """Aggregation sums revenue and counts orders per category."""
    rows = [
        {"category": "A", "revenue": 100.0},
        {"category": "A", "revenue": 50.0},
        {"category": "B", "revenue": 200.0},
    ]
    agg = _aggregate_by_category(rows)
    assert len(agg) == 2
    a = next(r for r in agg if r["category"] == "A")
    assert a["total_revenue"] == 150.0
    assert a["order_count"] == 2


# -- batch_orders -----------------------------------------------------------


def test_batch_path(orders_csv_path):
    """Batch path reads CSV, cleans, and aggregates by category."""
    with open(orders_csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    cleaned = _clean_orders(rows)
    agg: dict[str, dict] = defaultdict(lambda: {"total_revenue": 0.0, "order_count": 0})
    for r in cleaned:
        agg[r["category"]]["total_revenue"] += r["revenue"]
        agg[r["category"]]["order_count"] += 1
    result = list(agg.values())
    assert len(result) > 0
    assert all("total_revenue" in r for r in result)
    assert sum(r["total_revenue"] for r in result) > 0


def test_batch_orders_returns_categories():
    """batch_orders() returns aggregated results with expected keys."""
    result = batch_orders()
    assert len(result) > 0
    assert all("category" in r and "total_revenue" in r and "order_count" in r for r in result)


def test_batch_orders_three_categories():
    """The CSV has 3 categories: Electronics, Home & Garden, Sports."""
    result = batch_orders()
    cats = {r["category"] for r in result}
    assert cats == {"Electronics", "Home & Garden", "Sports"}


# -- streaming_orders -------------------------------------------------------


def test_streaming_path(sample_events):
    """Streaming path processes events, cleans, and aggregates."""
    cleaned = _clean_orders(sample_events)
    agg: dict[str, dict] = defaultdict(lambda: {"total_revenue": 0.0, "order_count": 0})
    for r in cleaned:
        agg[r["category"]]["total_revenue"] += r["revenue"]
        agg[r["category"]]["order_count"] += 1
    result = list(agg.values())
    assert len(result) > 0
    assert sum(v["total_revenue"] for v in result) > 0


def test_streaming_orders_returns_categories():
    """streaming_orders() returns aggregated results with expected keys."""
    result = streaming_orders()
    assert len(result) > 0
    assert all("category" in r for r in result)


# -- serving_layer ----------------------------------------------------------


def test_serving_layer_merges():
    """Serving layer merges batch and streaming, deduplicates by category."""
    batch = [
        {"category": "Electronics", "total_revenue": 1000.0, "order_count": 10},
        {"category": "Sports", "total_revenue": 500.0, "order_count": 5},
    ]
    streaming = [
        {"category": "Electronics", "total_revenue": 200.0, "order_count": 2},
        {"category": "Home & Garden", "total_revenue": 300.0, "order_count": 3},
    ]
    result = serving_layer(batch, streaming)
    assert len(result) == 3
    elec = next(r for r in result if r["category"] == "Electronics")
    assert elec["total_revenue"] == 1000.0  # batch wins


def test_serving_layer_deduplicates():
    """Serving layer removes duplicate categories, keeping batch (first)."""
    batch = [{"category": "Electronics", "total_revenue": 1000.0, "order_count": 10}]
    streaming = [{"category": "Electronics", "total_revenue": 200.0, "order_count": 2}]
    result = serving_layer(batch, streaming)
    assert len(result) == 1
    assert result[0]["total_revenue"] == 1000.0


def test_serving_layer_sorted():
    """Serving layer output is sorted by category."""
    batch = [{"category": "Zebra", "total_revenue": 1.0, "order_count": 1}]
    streaming = [{"category": "Alpha", "total_revenue": 2.0, "order_count": 1}]
    result = serving_layer(batch, streaming)
    assert result[0]["category"] == "Alpha"
    assert result[1]["category"] == "Zebra"


# -- consistency_check ------------------------------------------------------


def test_consistency_same_input():
    """Both paths produce identical output when given identical input."""
    data = [
        {"order_id": 1, "customer_id": 42, "product_id": 7, "quantity": 2,
         "price": 129.99, "status": "delivered", "category": "Electronics"},
        {"order_id": 2, "customer_id": 17, "product_id": 3, "quantity": 1,
         "price": 49.99, "status": "delivered", "category": "Home & Garden"},
    ]
    batch_result = _clean_orders([dict(r) for r in data])
    stream_result = _clean_orders([dict(r) for r in data])
    assert batch_result == stream_result


def test_consistency_check_overlap():
    """consistency_check reports overlapping and exclusive categories."""
    batch = [{"category": "A"}, {"category": "B"}]
    streaming = [{"category": "B"}, {"category": "C"}]
    result = consistency_check(batch, streaming)
    assert result["overlapping_categories"] == 1
    assert result["batch_only"] == ["A"]
    assert result["stream_only"] == ["C"]


# -- recommend_architecture -------------------------------------------------


def test_recommend_batch_low_budget():
    """Low budget or high latency tolerance recommends batch."""
    assert recommend_architecture("hours", "low", "small", False) == "batch"
    assert recommend_architecture("hours", "medium", "large", True) == "batch"


def test_recommend_kappa():
    """Small data, no reprocessing, low latency recommends Kappa."""
    assert recommend_architecture("seconds", "high", "small", False) == "kappa"
    assert recommend_architecture("minutes", "medium", "medium", False) == "kappa"


def test_recommend_lambda():
    """Large data with reprocessing and low latency recommends Lambda."""
    assert recommend_architecture("minutes", "high", "large", True) == "lambda"
    assert recommend_architecture("seconds", "high", "large", True) == "lambda"


# -- write_results ----------------------------------------------------------


def test_write_results_creates_file():
    """write_results writes JSON to the lake directory."""
    data = [{"category": "Test", "total_revenue": 42.0, "order_count": 1}]
    path = write_results(data, "test_output")
    assert path.exists()
    with open(path) as f:
        loaded = json.load(f)
    assert loaded == data
