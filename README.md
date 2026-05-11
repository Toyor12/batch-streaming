# Day 19 Lab -- Batch vs Streaming Architecture Tradeoffs

> :de: [Deutsche Version](README_DE.md)

## Objective

Build both a batch and a streaming path over the same orders dataset, merge them in a serving layer, and encode the architecture decision as a recommendation function. You will not set up Kafka -- "streaming" here reads events from a file to keep the lab grounded in logic rather than infrastructure.

**Bloom's level: Analyze -> Evaluate.** Tasks 1-4 are Apply-tier setup (you implement shared primitives). Tasks 5-7 plus the synthesis task are Analyze/Evaluate-tier — you reason about consistency, justify an architecture, and verify the serving layer's invariants under reprocessing.

## Learning Objectives

By the end of this lab you will be able to:

1. **Implement** a shared cleaning function so batch and streaming paths stay consistent (Apply)
2. **Implement** a batch path that reads a CSV and aggregates by category (Apply)
3. **Implement** a streaming simulation path that processes events from a file and aggregates by category (Apply)
4. **Justify** the serving layer's "batch wins on duplicates" rule by tracing what happens when streaming and batch disagree on the same category (Evaluate)
5. **Analyze** the consistency of batch vs streaming output by classifying categories as overlapping, batch-only, or stream-only (Analyze)
6. **Critique** a rule-based architecture recommender by finding inputs where its single-string output hides a violated requirement (Evaluate)
7. **Critique** a Kappa proposal that requires multi-year reprocessing, and argue why Lambda's batch layer is the safer choice when stream retention is bounded (Evaluate)

## What is provided

- `src/standalone/pipeline.py` -- function stubs with `raise NotImplementedError` for each task
- `src/standalone/data/orders.csv` -- 20-row ShopFlow orders dataset
- `_get_sample_events()` and `run_pipeline()` -- plumbing (already implemented). `run_pipeline()` prints each stage so you can see what each function returns as you fill them in.

> Dagster and Airflow variants of this pipeline are covered in Day 21.

## Prerequisites

Plain Python lab -- **no Spark, no Java, no Docker required**. `uv` handles the rest.

## Setup and run

```bash
cd standalone
uv sync
uv run python -m standalone
```

The first run will fail with `NotImplementedError` from Task 1. That is the loop: implement a function, re-run, see the next stage's output, repeat.

---

## Tasks

### Task 1 -- `_clean_orders(rows)`

Implement a cleaning step used by both batch and streaming paths:

- Drop rows missing `order_id`, `customer_id`, or `product_id`
- Keep only `quantity > 0` and `price > 0`
- Add a computed `revenue = quantity * price`
- Normalise `status` (lowercase, strip whitespace)

### Task 2 -- `_aggregate_by_category(rows)`

Given cleaned rows, return a list of dicts with `category`, `total_revenue`, `order_count` per category.

### Task 3 -- `batch_orders()`

Read `orders.csv`, clean it, aggregate by category, return the result.

### Task 4 -- `streaming_orders()`

Call `_get_sample_events()` (already implemented) to simulate a stream, clean, aggregate by category, return the result. The point is that the cleaning + aggregation is identical to the batch path -- only the source differs.

### Task 5 -- `serving_layer(batch, streaming)`

Merge both outputs: concatenate, deduplicate on `category` (batch is source of truth), and sort by `category`.

### Task 6 -- `consistency_check(batch, streaming)`

Return a dict with:

- `overlapping_categories` -- categories present in both
- `batch_only` -- categories only in the batch path
- `streaming_only` -- categories only in the streaming path

### Task 7a -- `recommend_architecture(latency, budget, data_volume, reprocessing_needed)` (Apply, warm-up)

Encode the rule as written so the tests pass:

- If `latency` is `"hours"` or `budget` is `"low"` -> `"batch"`
- Else if `reprocessing_needed` is `False` and `data_volume != "large"` -> `"kappa"`
- Else -> `"lambda"`

This is a warm-up. The interesting work is in 7b, where you push back on the rule.

### Task 7b -- Critique the recommender (Evaluate)

A single-string output can only carry one verdict. That hides cases where two inputs are in tension and the rule silently picks a winner. Your job: find inputs where the function's answer suppresses a violated requirement, then write what you would actually recommend (and why).

Two boundary scenarios. For each, run the function, then write your answer in `SCENARIOS.md` (create it in the lab root) or in a docstring block at the bottom of `pipeline.py`:

1. `("seconds", "low", "small", False)` -> the function says `"batch"`. Which input did the rule ignore? What does the `"seconds"` latency requirement imply that the output never reflects? What would you actually recommend, and what does that recommendation force the user to change?

2. `("seconds", "high", "large", False)` -> the function says `"lambda"`. Kappa is ruled out solely because `data_volume == "large"`, but that one signal collapses two questions: (a) can your stream retain enough history? (b) is Lambda's operational overhead worth it? Pick a stream-retention assumption that flips your recommendation to Kappa, and one that keeps it at Lambda.

For each scenario, record ~3-5 sentences answering:

- **Input the rule ignored:** which of the four parameters was silently dropped?
- **Tradeoff the binary output suppresses:** what cost or risk is the single string hiding?
- **Your recommendation:** what would you tell the team, and what assumption are you forcing them to make explicit?

Acceptance: `SCENARIOS.md` (or the in-file critique block) addresses both scenarios, and each answer references at least one concept beyond the four input parameters (stream retention, operator headcount, recovery objective, dual-codepath maintenance cost, etc.).

### Task 8 -- `write_results(results, name)` (and `_ensure_lake_dirs()`)

Persist aggregation output to disk so a downstream serving layer can pick it up.

- `_ensure_lake_dirs()` creates `LAKE_DIR/batch/` and `LAKE_DIR/streaming/` and returns both paths.
- `write_results(results, name)` writes `results` as JSON to `LAKE_DIR/<name>.json` and returns the path.

This mirrors how a real Lambda pipeline materialises each layer's output to its own location before the serving layer reads them.

---

## Success Criteria

- [ ] `uv run python -m standalone` runs end-to-end and prints batch, streaming, serving-layer, consistency-check, recommendation, and write-results output
- [ ] You can explain why `_clean_orders` is shared between the two paths
- [ ] You can name a workload where Kappa is the wrong choice and explain why
- [ ] You can argue for Lambda when `reprocessing_needed = True` on a large dataset
- [ ] `SCENARIOS.md` (or the in-file critique block) addresses both Task 7b boundary cases

---

## Synthesis Task -- Trace the corrected-batch override (Evaluate)

This task recombines two primitives -- `serving_layer` (Task 5) and `consistency_check` (Task 6) -- in a scenario the lecture does not walk through: **a corrected batch run must override stream-only output without erasing stream-only categories that the batch did not see**. No new code -- you use the functions you already implemented and write your reasoning down.

Scenario: streaming has been serving aggregates for categories `{Electronics, Sports, Outdoor}` from a buggy window. A corrected batch run produces aggregates for `{Electronics, Sports}` only -- the batch source has no `Outdoor` data for that window.

Your job (~10 minutes):

1. In a Python REPL (or in `SCENARIOS.md`), write two literal lists:
   - `corrected_batch` -- with `Electronics` and `Sports`, but with **different** revenue/order_count numbers than streaming.
   - `streaming` -- with all three categories, including `Outdoor`.
2. Call `serving_layer(corrected_batch, streaming)` and inspect the result. Confirm that `Electronics` and `Sports` carry the batch numbers and `Outdoor` survives with its streaming numbers.
3. Call `consistency_check(corrected_batch, streaming)` and confirm `Outdoor` shows up as `stream_only`.
4. Write a short answer (3-5 sentences) in `SCENARIOS.md`: **Why does this work for Lambda, and why would Kappa with bounded stream retention fail this exact scenario?**

Acceptance: `SCENARIOS.md` contains the prose answer to step 4 and a short note (or copy-pasted REPL transcript) showing the override behaviour from steps 2-3.

---

## Stretch Goals (optional, no new code required)

1. **Score a real scenario** -- pick a company you know (e.g., a food delivery app, an internal analytics dashboard at work). Walk through `recommend_architecture`'s inputs for that company, then write 3-4 sentences in `SCENARIOS.md` arguing whether the function's verdict is the one you would actually defend in a design review.
2. **Find a third boundary case** -- come up with one more input tuple (beyond the two in Task 7b) where the function's verdict masks a real tradeoff. Add it to `SCENARIOS.md` with the same three-bullet structure.
