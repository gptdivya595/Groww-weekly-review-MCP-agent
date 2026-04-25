# Phase 7 Evaluations

## Current Workspace Status

Code complete, but still pending a live end-to-end Docs plus Gmail MCP run in
this workspace.

## Objective

Verify that the complete weekly pipeline can run safely across products and recover from partial failures.

## Checks

### 1. Scheduling Evaluations

| ID | Check | Method |
| --- | --- | --- |
| P7-E01 | Full run for one product completes phases 1 through 6 in order | End-to-end smoke test |
| P7-E02 | Weekly scheduler triggers the same orchestration path as manual CLI | Scheduler smoke test |
| P7-E03 | Duplicate trigger does not create duplicate Docs or emails | Concurrency test |
| P7-E04 | Docs success and Gmail failure rerun retries only Gmail | Recovery test |
| P7-E05 | Reconciliation repairs local state from external delivery state | Reconciliation test |
| P7-E06 | Multi-product weekly run isolates status by product | Batch run test |
| P7-E07 | Audit lookup returns run metadata and external IDs | Audit query test |

### 2. Observability Evaluations

| ID | Check | Method |
| --- | --- | --- |
| P7-E11 | OTel spans cover each ingestion source, each LLM call, each MCP call, and each orchestrator stage | Exporter test |
| P7-E12 | Every span has `run_id`, `product`, and `iso_week` attributes | Exporter assertion |
| P7-E13 | OTLP exporter is reachable from the container and logs fall back to stdout on exporter failure | Chaos test |
| P7-E14 | Metrics are registered for reviews ingested, clusters formed, themes generated, LLM tokens, MCP latency, and publish status | Prometheus scrape |
| P7-E15 | Dashboards are committed as code under `infra/grafana/` and alerts under `infra/alerts/` | File existence check |

### 3. Alerting Evaluations

| ID | Check | Method |
| --- | --- | --- |
| P7-E16 | Ingestion drop alert triggers when `reviews_ingested < 0.5 * avg(last 4 weeks)` | Alert expression unit test |
| P7-E17 | Rating shift alert triggers when `avg_rating_delta_abs > 1.0` as an informational alert | Alert config test |
| P7-E18 | LLM schema failure alert triggers when `llm_schema_fail_rate > 2% over 1h` | Alert config test |
| P7-E19 | MCP error rate alert triggers when `mcp_error_rate > 1% over 30m` | Alert config test |
| P7-E20 | Cost spike alert triggers when `llm_cost_usd > 2x rolling median` | Alert config test |

## Acceptance

- weekly orchestration is safe and observable
- duplicate visible output is prevented
- partial failures are recoverable without manual cleanup
