# Phase 1 Evaluations

## Current Workspace Status

Complete and locally validated.

## Objective

Verify that review ingestion is deterministic, auditable, and reusable.

## Checks

| ID | Check | Method |
| --- | --- | --- |
| P1-E01 | App Store fixture replay produces a deterministic review snapshot | Fixture test |
| P1-E02 | Play Store fixture replay produces a deterministic review snapshot | Fixture test |
| P1-E03 | Unified `RawReview` model maps both sources into one schema | Model validation test |
| P1-E04 | Raw JSON snapshot is written to `data/raw/{product}/{run_id}.jsonl` | File existence check |
| P1-E05 | Dedup-upsert writes stable rows into `reviews` table | DB assertion |
| P1-E06 | Live Groww smoke run returns a non-trivial review set | Live-network smoke test |
| P1-E07 | Rerunning the same ingest command within a minute is a no-op in inserts | Idempotency test |
| P1-E08 | CLI `pulse ingest --product groww --weeks 10` works end to end | CLI smoke test |

## Acceptance

- canned responses produce deterministic snapshots
- at least one real product can be ingested
- reruns avoid duplicate inserts
