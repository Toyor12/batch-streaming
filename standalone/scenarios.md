# Task 7b -- Architecture Recommender Critique

## Scenario 1: ("seconds", "low", "small", False) -> "batch"

**Input the rule ignored:** the `"seconds"` latency requirement was silently dropped because `budget == "low"` fired first.

**Tradeoff the output suppresses:** batch runs on a schedule and can never deliver results in seconds. Low budget and seconds latency are incompatible — the single string hides this conflict entirely.

**Recommendation:** tell the team they must either increase budget to afford a streaming architecture like Kappa, or explicitly relax their latency requirement. Forcing a choice makes the constraint visible.

## Scenario 2: ("seconds", "high", "large", False) -> "lambda"

**Input the rule ignored:** `data_volume == "large"` ruled out Kappa, but "large" collapses two separate questions: stream retention and reprocessing scope.

**Tradeoff the output suppresses:** if stream retention covers the required window (e.g. 90 days), Kappa works fine and avoids Lambda's dual-codepath maintenance cost. If retention is bounded to 7 days but history goes back 3 years, Kappa fails and Lambda is correct.

**Recommendation:** make stream retention an explicit input. With sufficient retention -> Kappa. With bounded retention and multi-year history -> Lambda.

---

# Synthesis Task -- Corrected Batch Override

## REPL transcript

```python
corrected_batch = [
    {"category": "Electronics", "total_revenue": 2000.0, "order_count": 10},
    {"category": "Sports", "total_revenue": 600.0, "order_count": 5},
]
streaming = [
    {"category": "Electronics", "total_revenue": 389.97, "order_count": 2},
    {"category": "Sports", "total_revenue": 159.98, "order_count": 1},
    {"category": "Outdoor", "total_revenue": 250.0, "order_count": 3},
]

serving_layer(corrected_batch, streaming)
# -> Electronics and Sports carry batch numbers, Outdoor survives with streaming numbers

consistency_check(corrected_batch, streaming)
# -> Outdoor appears as stream_only
```

## Why Lambda works here and Kappa would fail

Lambda stores a separate batch layer on durable storage. When a corrected batch run arrives, it overwrites the batch layer output for Electronics and Sports, while Outdoor survives because it came from the streaming layer which Lambda keeps separate. The serving layer then merges both. Kappa has no batch layer — it relies entirely on reprocessing the stream. If stream retention is bounded (e.g. 7 days) and the buggy window is older than that, Kappa cannot reprocess it at all, and Outdoor would be lost permanently. Lambda's explicit separation of batch and streaming layers is exactly what makes the corrected override safe.