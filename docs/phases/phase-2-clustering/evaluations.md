# Phase 2 Evaluations

## Current Workspace Status

Complete and locally validated.

## Objective

Verify that embeddings and clustering produce coherent, reproducible clusters with cached reuse.

## Checks

| ID | Check | Method |
| --- | --- | --- |
| P2-E01 | Language filter keeps only supported language rows | Unit test |
| P2-E02 | Length filter removes too-short reviews | Unit test |
| P2-E03 | Embedding provider interface works for OpenAI and local BGE implementation | Provider contract test |
| P2-E04 | Embedding cache keyed by `sha1(text)` is used on rerun | Cache hit assertion |
| P2-E05 | Golden fixture around 400 reviews yields `4..12` HDBSCAN clusters | Fixture test |
| P2-E06 | Noise ratio remains below the configured threshold, for example `<35%` | Fixture assertion |
| P2-E07 | Fixed random seeds produce stable cluster assignments on identical runs | Determinism test |
| P2-E08 | `review_embeddings` and `clusters` tables are persisted correctly | DB assertion |

## Acceptance

- cluster outputs are reproducible enough for downstream use
- embedding cache gives full reuse on rerun
- persisted artifacts are ready for summarization
