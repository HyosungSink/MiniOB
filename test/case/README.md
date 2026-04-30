# miniob-test
miniob自动化功能测试

运行所有带结果文件的 SQL 测试用例：
```bash
python3 miniob_test.py
```

`test/case/test` 下的所有 `.test` 都是同一种 SQL case，MiniOB2025 题目 case 也直接纳入这个套件。默认非 `--report-only` 模式只运行存在同名 `.result` 的 case，避免没有 oracle 的草稿 case 误报失败。

运行指定测试用例
```bash
python3 miniob_test.py --test-cases=`case-name` --report-only
```

> 把`case-name` 换成你想测试的用例名称。
> 如果要运行多个测试用例，则在 --test-cases 参数中使用 ',' 分隔写多个即可

你可以设计自己的测试用例，并填充 result 文件后，删除 `--report-only` 参数进行测试。你可以用这种方法进行日常回归测试，防止在实现新功能时破坏了之前编写的功能。

运行 basic 测试用例
```bash
python3 miniob_test.py --test-cases=basic
```

注意：MiniOB 的题目要求是运行结果与 MySQL8.0 的结果一致，因此删除了过时的结果文件。如果不清楚某个测试用例的预期结果，参考 MySQL8.0 的运行结果即可。

需要特殊 observer 参数的 case 应在 `.test` 文件顶部声明，例如：
```sql
-- observer-args -t mvcc
```
runner 会在该 case 启动 observer 时自动追加参数，因此 MVCC 类 MiniOB2025 case 可以和普通 SQL case 一起运行。

暂不纳入默认回归的历史 case 可以声明：
```sql
-- case-state inactive
```
这类 case 不会被默认全量 SQL suite 选中，但仍可用 `--test-cases=<case-name>` 显式运行。

更多运行方法和参数可以参考 miniob_test.py
