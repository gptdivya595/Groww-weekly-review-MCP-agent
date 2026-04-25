# Phase 6 Edge Cases

| ID | Edge Case | Expected Handling |
| --- | --- | --- |
| P6-C01 | Gmail MCP is reachable but send fails after Docs publish | Preserve Docs result and retry Gmail only |
| P6-C02 | Existing draft is found with changed teaser content | Update or replace deterministically |
| P6-C03 | Same run is sent twice from two workers | Use run lock plus stored message ID |
| P6-C04 | Recipient list is empty or invalid | Block email publish and surface config error |
| P6-C05 | Local mock server hides real Gmail behavior | Keep mocks test-only and require real staging validation |
