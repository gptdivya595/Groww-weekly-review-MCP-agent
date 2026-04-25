# Weekly Product Review Pulse - Problem Statement

## Summary

We are building an automated weekly pulse that turns public App Store and
Google Play reviews for selected fintech products into a one-page insight
report and delivers it through Google Workspace.

Google delivery must use MCP:

- Google Docs MCP appends the report to the product's running Google Doc
- Gmail MCP drafts or sends the stakeholder email that links back to that Doc

The system also includes an internal operator surface:

- a backend control API for status and triggers
- a frontend operator dashboard for readiness, recent runs, and manual flow
  execution

The operator dashboard is for internal operations. The stakeholder artifact is
still the Google Doc plus the Gmail link to that Doc section.

Supported products in the initial scope:

- INDMoney
- Groww
- PowerUp Money
- Wealth Monitor
- Kuvera

## Objective

Give product, support, and leadership teams a repeatable weekly snapshot of
what customers are saying in app store reviews, without manual copy-paste,
spreadsheet stitching, or ad hoc report writing.

## What The System Does

1. Ingest public reviews from the last 8 to 12 weeks, using a configurable
   window, from Apple App Store and Google Play for each configured product.
2. Cluster and rank feedback using embeddings and density-based clustering,
   then use an LLM to name themes, select grounded quotes, and propose action
   ideas.
3. Render a concise one-page report plus a short email teaser.
4. Append the report to the product's Google Doc through Docs MCP and draft or
   send the stakeholder email through Gmail MCP.
5. Expose an internal operator console so the team can:
   - inspect phase completion
   - check readiness gaps
   - view recent runs and audit payloads
   - trigger a single-product run or weekly batch

## Architecture Boundary

| Concern | Where it lives |
| --- | --- |
| Data retrieval | Local ingestion modules for App Store and Google Play |
| Reasoning | Local clustering and summarization modules |
| Output generation | Local rendering for Docs payloads and Gmail teaser content |
| Google Workspace delivery | MCP clients only, through Docs MCP and Gmail MCP |
| Internal operations | FastAPI control API and Next.js operator console |

The agent is an MCP client or host for delivery. It does not embed Google
credentials or call Google Docs or Gmail REST APIs directly for stakeholder
delivery.

## Key Requirements

- MCP-only Google delivery for Docs append and Gmail draft or send
- weekly cadence with backfill support by ISO week
- idempotent re-runs for the same product plus ISO week
- auditable delivery identifiers and run metadata
- PII scrubbing before LLM use and before publish
- reviews treated as data, not as trusted instructions
- internal operator visibility into readiness, status, and audit state

## Non-Goals

- a stakeholder BI dashboard or analytics portal
- a generic Google Workspace suite beyond Docs append and Gmail send or draft
- social source ingestion in the initial scope
- storing Google OAuth secrets in the agent codebase

Clarification:

- an internal operator dashboard is in scope
- a stakeholder-facing analytics dashboard is not

## Who This Helps

| Audience | Value |
| --- | --- |
| Product | Prioritize roadmap from recurring review themes |
| Support | Spot repeating complaints and quality issues |
| Leadership | Get a concise weekly health snapshot tied to customer voice |
| Operators | Monitor readiness, trigger flows, and inspect audit trails |

## Sample Output

**Groww - Weekly Review Pulse**

**Period:** last 8 to 12 weeks

**Top themes**

1. App performance and bugs: lag, crashes during trading hours, login and
   session timeouts
2. Customer support friction: slow responses and unresolved tickets
3. UX and feature gaps: confusing portfolio navigation and missing advanced
   analytics

**Real user quotes**

- "The app freezes exactly when the market opens, very frustrating."
- "Support takes days to reply and does not solve the issue."
- "Good for beginners but lacks detailed analysis tools."

**Action ideas**

1. Stabilize peak-time performance and improve crash visibility
2. Improve support SLA visibility and ticket tracking
3. Enhance power-user portfolio analytics and navigation clarity

## Delivery Expectations

- each run appends one clearly labeled weekly section to the product Doc
- the email is a short teaser with a link to the new Doc section
- development and staging should default to draft-only Gmail behavior until
  explicitly approved

## Success Criteria

- an end-to-end run produces grounded themes, validated quotes, and action
  ideas for a configured product and time window
- the Google Doc and Gmail outcomes are idempotent per product plus ISO week
- operators can see status and trigger a flow without editing code
- phases 5 to 7 are not considered complete in any environment until at least
  one live Docs delivery and one live Gmail delivery are recorded through MCP

## Current Implementation Note

In this workspace, the codebase implements the full phase set, including the
operator console, but live Docs and Gmail delivery still depends on real MCP
configuration and the first successful live publish run.
