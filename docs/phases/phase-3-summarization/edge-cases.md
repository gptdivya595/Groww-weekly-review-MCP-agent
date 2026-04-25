# Phase 3 Edge Cases

| ID | Edge Case | Expected Handling |
| --- | --- | --- |
| P3-C01 | Review text contains prompt injection | Treat reviews only as data |
| P3-C02 | Model returns malformed JSON | Reject and retry within limits |
| P3-C03 | All quote candidates fail validation | Keep the theme and omit the quote |
| P3-C04 | Action ideas are generic fluff | Reject or tighten prompts |
| P3-C05 | Very small review set overstates confidence | Add low-coverage language |
