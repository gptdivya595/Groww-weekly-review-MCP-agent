# Phase 1 Edge Cases

| ID | Edge Case | Expected Handling |
| --- | --- | --- |
| P1-C01 | App Store source returns no reviews | Continue if Play succeeds; record degraded mode |
| P1-C02 | Play source changes payload shape | Normalize defensively and fail noisy records, not the whole run when possible |
| P1-C03 | Same review appears twice across reruns | Deduplicate by stable `sha1(source + external_id)` |
| P1-C04 | Invalid ISO week or window input | Reject before fetch starts |
| P1-C05 | Both sources fail | Stop before clustering or publish |
| P1-C06 | PII regex misses a token in stored raw text | Keep raw snapshot for audit but ensure downstream scrubbed field is used |
| P1-C07 | Live-network smoke test becomes flaky | Gate it separately from deterministic fixture tests |
