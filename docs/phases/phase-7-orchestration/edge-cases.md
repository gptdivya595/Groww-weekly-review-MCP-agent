# Phase 7 Edge Cases

| ID | Edge Case | Expected Handling |
| --- | --- | --- |
| P7-C01 | Scheduler fires the same product/week twice | Lock by product and ISO week |
| P7-C02 | Docs publish succeeded but local state was not written | Reconcile using persisted Doc identifiers |
| P7-C03 | Gmail send succeeded but local state says failed | Reconcile before retrying send |
| P7-C04 | Backfill run overlaps with scheduled run | Isolate runs and prevent cross-run publish collisions |
| P7-C05 | Observability backend is partially down | Delivery still works; logs degrade gracefully |
| P7-C06 | OTLP exporter is unavailable from the container | Fall back to stdout logs and surface telemetry health |
| P7-C07 | MCP error rate spikes above threshold | Alert and preserve enough context for triage |
| P7-C08 | Weekly ingestion volume drops sharply | Alert without blocking the run automatically |
| P7-C09 | LLM schema failure rate rises during production | Alert and hold broken summaries from publish |
| P7-C10 | LLM cost spikes unusually | Alert and enforce cost guardrails |
