# MiniOB 2025 Local Evaluation Cases

This directory contains local MiniOB case inputs for the 2025 problem set. Cases are split by competition problem so an unfinished kernel can run focused checks with `--test-cases=<case>`.

## Result Generation

Most 2025 cases can use the competition-provided MySQL version under `mysql-server/` to generate standard output. Full text index is the exception because MiniOB uses cppjieba tokenization and BM25 scoring rules from the competition statement, not MySQL fulltext semantics.

Recommended workflow after starting the competition MySQL server:

```bash
python3 test/case/generate_mysql_result.py --socket /tmp/miniob2026-mysql.sock
```

This generates `.result` files for all `miniob2025-*.test` cases that can be checked against the competition MySQL oracle. It intentionally skips `miniob2025-23-full-text-index.test`.

For MySQL-generated expected output, keep the MiniOB formatting rules:

- Echo SQL text before each SQL result.
- DDL/DML success is `SUCCESS`; failure is `FAILURE`.
- Query output uses ` | ` between fields.
- Sort cases should sort every returned line, matching the existing `-- sort` behavior in `miniob_test.py`.
- The current runner uppercases written output, so generated `.result` files should be compared with that in mind.

## Notes

- `miniob2025-20-update-mvcc.test` requires running `observer` with `-t mvcc`; the default `test/case/miniob_test.py` server startup does not currently pass that flag.
- `miniob2025-23-full-text-index.test` should not use MySQL as the oracle. Use the competition jieba/BM25 implementation described in `miniob-2025-problem-list.md`.
- Vector cases assume the competition MySQL build supports `VECTOR`, `STRING_TO_VECTOR`, `VECTOR_TO_STRING`, and `DISTANCE`.
