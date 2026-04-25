# Phase 5 Evaluations

## Current Workspace Status

Code complete, but still pending live Docs MCP validation in this workspace.

## Objective

Verify idempotent Google Docs append through MCP only.

## Checks

| ID | Scenario | Expected Result |
| --- | --- | --- |
| P5-E01 | First publish to Docs | A new dated section is appended |
| P5-E02 | Second publish of same run | No duplicate section is created |
| P5-E03 | Document resolution | Existing product doc is found or created correctly |
| P5-E04 | Real render fidelity | Headings, bullets, quotes, and footer render correctly |
| P5-E05 | Heading ID persistence | `gdoc_heading_id` and `gdoc_id` are stored |
| P5-E06 | Deep link validation | Stored deep link opens the correct section |
| P5-E07 | Mock MCP integration test | JSON-RPC requests are correct and complete |
| P5-E08 | Real MCP staging test | The phase works against a real Docs MCP server |

## Acceptance

- first run creates a section
- second run is a no-op for the same anchor
- deep link and heading ID are persisted and reusable
