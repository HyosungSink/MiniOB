# MiniOB 2025 Local Evaluation Cases

This directory contains local MiniOB case inputs for the 2025 problem set. Cases are intentionally split by topic so an unfinished kernel can run focused checks with `--test-cases=<case>`.

## Result Generation

Most 2025 cases can use the competition-provided MySQL version under `mysql-server/` to generate standard output. Full text index is the exception because MiniOB uses cppjieba tokenization and BM25 scoring rules from the competition statement, not MySQL fulltext semantics.

Recommended workflow after starting the competition MySQL server:

```bash
python3 test/case/generate_mysql_result.py --socket /tmp/miniob2026-mysql.sock
```

This generates `.result` files for all `miniob2025-*.test` cases that can be checked against the competition MySQL oracle. It intentionally skips `miniob2025-full-text-index-boundary.test`.

For MySQL-generated expected output, keep the MiniOB formatting rules:

- Echo SQL text before each SQL result.
- DDL/DML success is `SUCCESS`; failure is `FAILURE`.
- Query output uses ` | ` between fields.
- Sort cases should sort every returned line, matching the existing `-- sort` behavior in `miniob_test.py`.
- The current runner uppercases written output, so generated `.result` files should be compared with that in mind.

## Coverage Map

| Problem | Existing or New Local Case |
| --- | --- |
| 1. basic | `basic.test` |
| 2. update | `primary-update.test`, `miniob2025-cross-topic-boundary.test` |
| 3. drop-table | `primary-drop-table.test`, `miniob2025-cross-topic-boundary.test` |
| 4. date | `primary-date.test`, `miniob2025-cross-topic-boundary.test` |
| 5. join-tables | `primary-join-tables.test`, `miniob2025-cross-topic-boundary.test` |
| 6. expression | `primary-expression.test`, `miniob2025-cross-topic-boundary.test` |
| 7. function | `primary-aggregation-func.test`, `miniob2025-cross-topic-boundary.test` |
| 8. multi-index | `primary-multi-index.test`, `miniob2025-cross-topic-boundary.test` |
| 9. unique | `primary-unique.test`, `miniob2025-cross-topic-boundary.test` |
| 10. group-by | `primary-group-by.test`, `vectorized-aggregation-and-group-by.test` |
| 11. simple-sub-query | `primary-simple-sub-query.test` |
| 12. alias | `miniob2025-alias-boundary.test` |
| 13. null | `primary-null.test`, `miniob2025-cross-topic-boundary.test` |
| 14. union | `miniob2025-union-boundary.test` |
| 15. order-by | `primary-order-by.test`, `vectorized-order-by-limit.test` |
| 16. vector-basic | `vectorized-basic.test`, `miniob2025-vector-search-boundary.test` |
| 17. text | `primary-text.test`, `miniob2025-full-text-index-boundary.test` |
| 18. vector-search | `miniob2025-vector-search-boundary.test` |
| 19. alter | `miniob2025-alter-boundary.test` |
| 20. update-mvcc | `miniob2025-update-mvcc-boundary.test` |
| 21. complex-sub-query | `primary-complex-sub-query.test` |
| 22. create-view | `miniob2025-create-view-boundary.test` |
| 23. full-text-index | `miniob2025-full-text-index-boundary.test` |
| 24. big-order-by | `miniob2025-big-order-by-boundary.test` |

## Notes

- `miniob2025-update-mvcc-boundary.test` requires running `observer` with `-t mvcc`; the default `test/case/miniob_test.py` server startup does not currently pass that flag.
- `miniob2025-full-text-index-boundary.test` should not use MySQL as the oracle. Use the competition jieba/BM25 implementation described in `miniob-2025-problem-list.md`.
- Vector cases assume the competition MySQL build supports `VECTOR`, `STRING_TO_VECTOR`, `VECTOR_TO_STRING`, and `DISTANCE`.
