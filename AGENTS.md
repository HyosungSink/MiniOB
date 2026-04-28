# MiniOB Competition Development Guide

## Project Structure

- `src/observer/main.cpp`: MiniOB observer server entry point.
- `src/observer/net/`: network services and protocol handling, including plain/MySQL/CLI connection paths.
- `src/observer/event/`: SQL request events and execution-stage plumbing.
- `src/observer/session/`: session state, transaction binding, and thread-local session helpers.
- `src/observer/catalog/`: catalog-related interfaces and schema lookup support.
- `src/observer/common/`: observer-local common utilities and SQL/runtime type helpers.
- `src/observer/sql/parser/`: SQL lexer/parser and generated parser artifacts. Edit grammar sources first, then regenerate generated files when needed.
- `src/observer/sql/stmt/`: statement objects built from parsed SQL, including DDL/DML/query statement validation.
- `src/observer/sql/expr/`: scalar, aggregate, comparison, arithmetic, and expression evaluation code.
- `src/observer/sql/optimizer/`: rewrite and optimization rules that transform logical plans.
- `src/observer/sql/operator/`: logical and physical operators for scans, joins, predicates, projection, aggregation, sort, insert, delete, explain, and vectorized execution.
- `src/observer/sql/executor/`: SQL command executors and execution dispatch.
- `src/observer/sql/plan_cache/` and `src/observer/sql/query_cache/`: experimental plan/query cache code.
- `src/observer/storage/buffer/`: page, frame, buffer pool, double-write buffer, and buffer-pool WAL support.
- `src/observer/storage/clog/`: commit log infrastructure, log files, log buffers, disk log handler, and integrated replay.
- `src/observer/storage/db/`: database lifecycle, table discovery, log initialization, and recovery entry points.
- `src/observer/storage/table/`: table abstraction, heap/LSM table engines, table metadata, and table-level operations.
- `src/observer/storage/record/`: record pages, record file handlers, scanners, record WAL, row/PAX storage helpers.
- `src/observer/storage/index/`: B+ tree index implementation, latch memo, index scans, and B+ tree WAL/replay.
- `src/observer/storage/trx/`: transaction framework, vacuous/MVCC transactions, and MVCC transaction WAL.
- `src/observer/storage/persist/`, `field/`, `common/`, `default/`, `tokenizer/`: metadata persistence, field abstractions, shared storage helpers, default handlers, and tokenizer support.
- `src/common/`: shared utilities used across the project, including logging, config, IO, memory, threading, time, math, queues, and language wrappers.
- `src/obclient/`: command-line client for connecting to MiniOB.
- `src/oblsm/`: standalone LSM-tree components, client utilities, benchmarks, memtable/table/compaction/WAL code.
- `src/memtracer/`: memory tracing support and related runtime utilities.
- `src/cpplings/`: C++ learning/exercise code; usually not needed for kernel work.
- `unittest/`: GoogleTest unit tests grouped by `common`, `observer`, `oblsm`, and `memtracer`.
- `test/case/`: local MiniOB SQL case runner, case files, and expected results.
- `test/integration_test/`: larger integration test framework and MiniOB test harness.
- `test/sysbench/`: sysbench-related test scripts/configuration.
- `benchmark/`: benchmark programs and benchmark helpers.
- `tools/`: auxiliary tools such as clog dump utilities.
- `etc/`: runtime configuration, including `observer.ini`.
- `docs/`: documentation site content, design notes, lab/dev environment docs, images, and course material.
- `deps/3rd/`: vendored third-party dependencies such as libevent, googletest, benchmark, jsoncpp, replxx, and cppjieba. Do not modify unless explicitly required.
- `mysql-server/`: optional submodule for the competition-specified MySQL server used to generate standard output for local evaluation cases; a normal clone records only the gitlink until the submodule is initialized.
- `.github/workflows/`: CI workflows for build/test, formatting, and documentation/page automation.
- `miniob-2025-problem-list.md`: local MiniOB 2025 problem summary plus per-problem local evaluation notes with paired SQL case snippets and expected results.

## Restrictions

- Keep the code compatible with the official MiniOB training/evaluation environment: Ubuntu 22.04, GCC 11.4, Intel x86_64. Avoid macOS-only behavior, absolute local paths, local symlinks, or assumptions from the current workstation.
- Do not commit secrets or private access material. Training submission may use a private GitHub repository, token URL, or the official tester account, but tokens and invitation links must never be stored in the repo.
- Do not change the plain-text protocol, MySQL protocol behavior, client/server message terminator semantics, or output formatting casually. The backend black-box tests compare exact client-visible output.
- Follow MiniOB output conventions strictly: parser errors return only `FAILURE`; successful DDL/DML returns `SUCCESS`; failed DDL/DML/query returns `FAILURE`; query headers, separators, date, string, float, and empty-result formatting must match the official rules.
- Do not print ad hoc debug text to client-visible output. Use the existing `sql_debug` path only for temporary training-platform diagnosis, keep it bounded, and restore the default disabled state before final commits.
- Do not hard-code answers, branch on hidden case names, special-case official tests, weaken assertions only to pass tests, alter official expected output, or silently skip failing tests. Implement real SQL/kernel behavior.
- Do not copy or borrow code from previous teams, online submissions, external DBMS projects, or incompatible licenses. For vector search tasks, third-party libraries are allowed only when their license is compatible and the dependency can be built in the official environment.
- Do not change root build options, `CMakeLists.txt`, dependency layout, or `deps/3rd/` unless the feature explicitly requires it. Keep new dependencies minimal, reproducible, and documented.
- Do not reintroduce Docker/container workflow files or require Docker for local build, CI, or training submission.
- Do not commit generated build artifacts, `build*` directories, database data directories, logs, core files, benchmark outputs, generated binaries, or local test scratch data.
- Do not hand-edit generated parser files as the primary source of truth. Edit grammar/lexer sources first, then regenerate generated parser artifacts consistently.
- Do not bypass normal memory allocation in MiniOB code with `mmap`, `brk`, `sbrk`, raw allocation syscalls, or similar tricks. MemTracer relies on intercepting normal `malloc/free` and `new/delete` paths and is not compatible with ASAN.
- Preserve existing public interfaces used by tests: `observer` command-line flags, `etc/observer.ini`, `test/case/miniob_test.py`, sysbench MySQL-socket mode, and integration-test entry points.

## Development Principles

- Implement MiniOB competition tasks as small, verifiable slices. Keep parser, statement, optimizer, physical operator, storage, and transaction changes separated unless one feature genuinely crosses those boundaries.
- Prefer completing one official problem-list item or one clearly scoped subtask at a time. Record known gaps instead of mixing partial implementations from several problems.
- Fix root causes rather than symptoms. Do not mask incomplete kernel behavior by weakening assertions, skipping paths, changing output, or special-casing tests.
- Keep module boundaries clear: parser builds SQL AST, `stmt` validates and binds schema, optimizer rewrites plans, operators execute plans, storage persists data, and transaction/log modules protect consistency.
- Maintain invariants after every write: table metadata, record pages, indexes, buffer-pool pin/unpin, frame latches, WAL LSNs, transaction state, and recovery replay order.
- Treat concurrency and recovery failures as kernel bugs. The current `bplus_tree_log_test` and `mvcc_trx_log_test` failures should not be dismissed as local environment issues.
- Optimize only after correctness is defensible. Prefer simple, testable implementations before adding vectorization, caching, parallelism, or specialized index paths.
- Keep behavior compatible with both plain protocol SQL cases and MySQL-socket sysbench paths when touching server, executor, transaction, or storage code.

## Git Workflow

### Commit Discipline

- Always start with `git status --short --branch` and confirm it only lists intended files. Untracked local notes, generated data, build outputs, and scratch directories must not be staged accidentally.
- Commit discipline is mandatory: make small commits, each focused on one logical MiniOB change such as one parser rule, one statement binder, one operator behavior, one storage fix, or one test/documentation update.
- Hard guardrail: do not let a single logical batch grow beyond roughly `3-5` tracked files or cross parser/optimizer/operator/storage/transaction boundaries without making an intermediate commit first.
- Hard guardrail: once a small checkpoint has been verified, stop and commit before continuing. Good checkpoints include one target builds, one SQL case turns green, one unit test regression is fixed, or one AGENT/docs section is updated.
- If many files have already accumulated without a commit, pause feature work, inspect the diff, split the batch, and commit the smallest verified subset first.
- Keep documentation-only commits separate from kernel behavior changes unless the documentation explains that exact change.
- Stage only related files with `git add <path>` and use short English commit messages under 20 words.

### Pre-Commit Checks

- Relevant builds should pass before committing. For most kernel changes, use `bash build.sh debug -DWITH_CPPLINGS=OFF -DWITH_UNIT_TESTS=ON --make -j4`.
- Relevant tests should be run, and any failures must be understood. If a known failure remains, mention it explicitly instead of presenting the tree as fully green.
- New SQL features should include or update `test/case` coverage when result comparison is practical. At minimum, run the nearest SQL case and document missing expected-result coverage.
- Storage, transaction, recovery, and concurrency changes should run the closest `unittest/observer` binary or CTest filter before broader SQL tests.
- Before pushing, recheck `git status --short --branch` and confirm no logs, database folders, binaries, temporary files, local symlinks, or unrelated changes are staged.

## Build

```bash
bash build.sh debug -DWITH_CPPLINGS=OFF -DWITH_UNIT_TESTS=ON --make -j4
cmake --build build_debug --target observer
cmake --build build_debug --target obclient
```

If parser grammar changes:

```bash
cd src/observer/sql/parser
flex --header-file=lex_sql.h -o lex_sql.cpp lex_sql.l
bison --defines=yacc_sql.hpp -o yacc_sql.cpp yacc_sql.y
```

## Testing

Run checks from narrow to broad: build the touched target, run the nearest CTest/GoogleTest binary, run the relevant MiniOB SQL case, then expand to sysbench or integration tests if the change touches networking, persistence, transactions, recovery, or MySQL protocol behavior.

### Unit Tests

Build with unit tests enabled before running CTest:

```bash
bash build.sh debug -DWITH_CPPLINGS=OFF -DWITH_UNIT_TESTS=ON --make -j4
ctest --test-dir build_debug --output-on-failure
```

For ASAN debug builds on macOS, keep the container-overflow workaround used during local verification:

```bash
export ASAN_OPTIONS=detect_container_overflow=0
ctest --test-dir build_debug -E memtracer_test --output-on-failure
```

Run a single failing or nearby unit test while iterating:

```bash
ctest --test-dir build_debug -R parser_test --output-on-failure
./build_debug/unittest/parser_test
./build_debug/unittest/record_manager_test
```

Unit tests live under `unittest/common`, `unittest/observer`, `unittest/oblsm`, and `unittest/memtracer`. The current local kernel state has known failures in `bplus_tree_log_test` and `mvcc_trx_log_test`; treat them as real WAL/concurrency issues, not environment failures.

### SQL Case Tests

The lightweight MiniOB SQL case runner is under `test/case` and starts `observer` automatically:

```bash
python3 test/case/miniob_test.py --test-cases=basic
python3 test/case/miniob_test.py --test-cases=vectorized-basic
python3 test/case/miniob_test.py --test-cases=basic,vectorized-basic
```

Only cases with expected result files can be checked normally. Current result files include `basic`, `vectorized-basic`, `vectorized-aggregation-and-group-by`, and MiniOB 2025 per-problem cases `miniob2025-01` through `miniob2025-22` plus `miniob2025-24`. The full-text case `miniob2025-23-full-text-index.test` intentionally has no MySQL-generated `.result`; inspect it with `--report-only` or a feature-specific oracle. For other cases without result files, use `--report-only` when generating or inspecting behavior:

```bash
python3 test/case/miniob_test.py --test-cases=primary-update --report-only
```

The competition-specified MySQL server is an optional submodule. Initialize it only when regenerating MySQL-backed standard output:

```bash
git submodule update --init mysql-server
```

Be aware that `bash build.sh init` currently runs `git submodule update --init`, which initializes all submodules and may also fetch `mysql-server`.

When the repository path contains spaces, prefer an explicit temporary work directory and reuse an existing build through a symlink:

```bash
rm -rf /tmp/miniob-functional
mkdir -p /tmp/miniob-functional
ln -s "$PWD/build_debug" /tmp/miniob-functional/build
python3 test/case/miniob_test.py --project-dir "$PWD" --work-dir /tmp/miniob-functional --test-cases=basic
```

### Sysbench Tests

Sysbench tests exercise the MySQL protocol path, thread models, MVCC, and disk durability. They require `sysbench` and a MySQL/MariaDB client:

```bash
./build_release/bin/observer -T one-thread-per-connection -s /tmp/miniob.sock -f etc/observer.ini -P mysql -t mvcc -d disk
cd test/sysbench
sysbench --mysql-socket=/tmp/miniob.sock --mysql-ignore-errors=41 --threads=10 miniob_insert prepare
sysbench --mysql-socket=/tmp/miniob.sock --mysql-ignore-errors=41 --threads=10 miniob_insert run
```

Available sysbench Lua cases include `miniob_insert`, `miniob_delete`, and `miniob_select`.

### Integration Tests

The larger harness is under `test/integration_test`. It is closer to the hosted MiniOB evaluation environment and uses `conf.ini` plus `libminiob_test.py`:

```bash
cd test/integration_test
python3 ./libminiob_test.py -c conf.ini
```

Use it after focused CTest and SQL case coverage, especially for cross-feature changes involving server startup, recovery, compatibility, or official problem-suite behavior.

### CI Parity

The main build workflow is `.github/workflows/build-test.yml` and is currently manual-only via `workflow_dispatch`. It builds debug and release variants, runs CTest, checks the `basic` SQL case, runs sysbench, and runs selected benchmark/memtracer checks. For local CI-like coverage:

```bash
bash build.sh debug -DCONCURRENCY=ON -DENABLE_COVERAGE=ON -DWITH_BENCHMARK=ON -DWITH_MEMTRACER=ON -DWITH_UNIT_TESTS=ON --make -j4
ctest --test-dir build_debug -E memtracer_test --output-on-failure
bash build.sh release -DCONCURRENCY=ON -DWITH_UNIT_TESTS=ON -DWITH_BENCHMARK=ON -DENABLE_ASAN=OFF -DWITH_MEMTRACER=ON --make -j4
python3 test/case/miniob_test.py --test-cases=basic
```
