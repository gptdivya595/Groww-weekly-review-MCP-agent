# Phase 4 Edge Cases

| ID | Edge Case | Expected Handling |
| --- | --- | --- |
| P4-C01 | Rendered report exceeds one-page intent | Enforce section and item caps |
| P4-C02 | Anchor is not unique enough | Include product and ISO week in the run key |
| P4-C03 | Docs and email payloads diverge semantically | Render both from one canonical artifact |
| P4-C04 | Markdown or request tree contains invalid formatting | Fail render validation before publish |
| P4-C05 | Quotes contain unreadable redaction artifacts | Drop or replace them before final render |
