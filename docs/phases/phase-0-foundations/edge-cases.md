# Phase 0 Edge Cases

| ID | Edge Case | Expected Handling |
| --- | --- | --- |
| P0-C01 | `.env.example` contains a real API key | Block merge and replace with a placeholder |
| P0-C02 | `products.yaml` is missing required fields | Fail validation before runtime |
| P0-C03 | CLI subcommands drift from the documented contract | Treat as a contract failure |
| P0-C04 | SQLite schema is missing one of the core tables | Block later phases until fixed |
| P0-C05 | Mock MCP endpoints are set as default production delivery targets | Separate mock, staging, and production config explicitly |
| P0-C06 | Logs do not carry `run_id` context | Fail the logging contract early |
