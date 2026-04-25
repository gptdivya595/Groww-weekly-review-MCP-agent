# Phase 6 Evaluations

## Current Workspace Status

Code complete, but still pending live Gmail MCP validation in this workspace.

## Objective

Verify idempotent Gmail delivery through MCP, with draft as the safe default.

## Checks

| ID | Scenario | Expected Result |
| --- | --- | --- |
| P6-E01 | Draft publish | One canonical draft is created |
| P6-E02 | Draft rerun | Same run updates or reuses the existing draft |
| P6-E03 | Send gating | Send does not happen unless explicitly enabled |
| P6-E04 | Send publish | A single stakeholder email is sent when gated on |
| P6-E05 | Docs link in email | Email contains a valid Docs deep link |
| P6-E06 | Metadata persistence | Draft/message/thread IDs are stored |
| P6-E07 | Mock Gmail MCP test | JSON-RPC request flow is correct |
| P6-E08 | Real Gmail MCP staging test | Draft/send behavior works against a real Gmail MCP server |

## Acceptance

- draft mode is safe by default
- send mode is explicit
- reruns do not create duplicate drafts or sends
