# MiniOB 2025 SQL Cases

MiniOB 2025 inputs are normal `test/case` SQL cases. They use the same `.test` input format, `.result` oracle files, runner commands, sorting directive, multi-connection directives, and report-only flow as every other SQL case.

Cases remain named by competition problem so an unfinished kernel can run focused checks with `--test-cases=<case>`, but there is no separate MiniOB2025 runner. Running `python3 test/case/miniob_test.py` executes all SQL cases that have result files, including MiniOB2025 cases.

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

- Cases that need non-default observer startup flags declare them inside the `.test` file with `-- observer-args ...`. For example, `miniob2025-20-update-mvcc.test` declares `-- observer-args -t mvcc`, so it can run together with the rest of the SQL case suite.
- `miniob2025-23-full-text-index.test` should not use MySQL as the oracle. Use the competition jieba/BM25 implementation described in `miniob-2025-problem-list.md`.
- Vector cases assume the competition MySQL build supports `VECTOR`, `STRING_TO_VECTOR`, `VECTOR_TO_STRING`, and `DISTANCE`.
