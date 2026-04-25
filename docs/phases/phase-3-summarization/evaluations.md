# Phase 3 Evaluations

## Current Workspace Status

Complete and locally validated.

## Objective

Verify that the LLM produces grounded summaries, quotes, and actions from cluster evidence.

## Checks

| ID | Scenario | Expected Result |
| --- | --- | --- |
| P3-E01 | Structured prompt run | Output matches the required schema |
| P3-E02 | Theme naming | Cluster names are coherent and concise |
| P3-E03 | Quote selection | Quote candidates come from real evidence packets |
| P3-E04 | Quote validation | Every accepted quote maps to stored review text |
| P3-E05 | Action ideas | Suggestions tie back to actual themes |
| P3-E06 | Malformed model output | Invalid responses are rejected or retried |

## Acceptance

- no hallucinated quotes are published
- summaries are grounded in cluster evidence
- model output is schema-validated
