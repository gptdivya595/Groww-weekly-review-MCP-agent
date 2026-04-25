# Phase 2 Edge Cases

| ID | Edge Case | Expected Handling |
| --- | --- | --- |
| P2-C01 | PII scrubber removes too much context | Preserve meaning while redacting sensitive tokens |
| P2-C02 | PII scrubber misses a token | Fail publication path later; improve scrub rules |
| P2-C03 | No dominant clusters emerge | Emit a low-signal result instead of forcing themes |
| P2-C04 | Duplicate reviews skew one cluster | Down-weight or deduplicate before final ranking |
| P2-C05 | Embedding cache path is corrupted | Rebuild cache safely without changing run semantics |
| P2-C06 | HDBSCAN marks too many reviews as noise | Tune thresholds and log the drift |
| P2-C07 | Local BGE and OpenAI embeddings produce materially different clusters | Treat provider as an explicit run parameter |
