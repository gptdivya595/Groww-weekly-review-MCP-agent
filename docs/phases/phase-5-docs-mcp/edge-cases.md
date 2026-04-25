# Phase 5 Edge Cases

| ID | Edge Case | Expected Handling |
| --- | --- | --- |
| P5-C01 | Two publishes race on the same run | Use run locking plus anchor detection |
| P5-C02 | `docs.get_document` misses a just-written update | Re-read with retry before declaring failure |
| P5-C03 | Anchor check is substring-based and ambiguous | Use an exact machine-readable run key |
| P5-C04 | Document exists but heading was manually edited | Use stored run key and persisted IDs for reconciliation |
| P5-C05 | Mock MCP passes but real Docs MCP differs | Require staging verification before sign-off |
