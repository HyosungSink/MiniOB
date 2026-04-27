# MiniOB-2025 题目列表整理

- 来源页面：https://open.oceanbase.com/train/detail/17?questionId=600060
- 抓取时间：2026-04-28 00:07:08 CST
- 总题数：24
- 总分：460
- 难度分布：简单 7 题 / 中等 12 题 / 困难 5 题

## 题目完整信息

### 1. basic

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800445&subQuestionName=basic
- 难度：简单
- 题目总分：10
- 通过率：62.6%
- 通过人数：1982
- 未通过人数：1185
- 题解数：0
- 题目 ID：800445

MiniOB本身具有的一些基本功能。比如创建表、创建索引、查询数据、查看表结构等。也就是说本题可以理解为送分题。

在开发其它功能时，需要留意不要破坏这些基础功能。

### 2. update

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800448&subQuestionName=update
- 难度：简单
- 题目总分：10
- 通过率：71.4%
- 通过人数：1455
- 未通过人数：583
- 题解数：0
- 题目 ID：800448

1\. 实现更新行数据的功能。

2\. 当前实现 update 单个字段即可。现在 MiniOB 具有 insert 和 delete 功能，在此基础上实现更新功能。可以参考 insert\_record 和 delete\_record 的实现。目前仅能支持单字段update的语法解析，但是不能执行。需要考虑带条件查询的更新，和不带条件的更新，同时需要考虑带索引时的更新。

示例 SQL 语句：

```
UPDATE t SET id = 1 WHERE age = 1;
```

### 3. drop-table

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800447&subQuestionName=drop-table
- 难度：简单
- 题目总分：10
- 通过率：78.2%
- 通过人数：1593
- 未通过人数：445
- 题解数：0
- 题目 ID：800447

1.实现删除表 (drop table)，清除表相关的资源。

2.当前 MiniOB 支持建表与创建索引，但是没有删除表的功能。

3.在实现此功能时，除了要删除所有与表关联的数据，不仅包括磁盘中的文件，还包括内存中的索引等数据。

示例 SQL 语句：

```
DROP TABLE table_name;
```

### 4. date

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800446&subQuestionName=date
- 难度：简单
- 题目总分：10
- 通过率：55.1%
- 通过人数：1123
- 未通过人数：915
- 题解数：0
- 题目 ID：800446

在现有功能上实现日期类型字段。

当前已经支持了 int、char、float类型，在此基础上实现date类型的字段。date 测试可能超过 2038 年 2 月，也可能小于 1970 年 1 月 1 号。注意处理非法的 date 输入（考虑 date 类型的值的合法性，如考虑闰年的情况），需要返回 FAILURE。

这道题目需要考虑语法解析，类型相关操作，还需要考虑 DATE 类型数据的存储。

示例 SQL 语句：

```
CREATE TABLE t(id INT, birthday DATE);
INSERT INTO t VALUES(1, '2022-10-10');
```

有关基本类型转换可以参考：[22年赛题-基本类型转换](https://github.com/oceanbase/miniob/wiki/OceanBase--数据库大赛-2022-初赛赛题#4-基本类型转换)

参考资料：

Date 类型解析视频：<https://open.oceanbase.com/course/detail/13252>

### 5. join-tables

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800454&subQuestionName=join-tables
- 难度：简单
- 题目总分：10
- 通过率：39.1%
- 通过人数：797
- 未通过人数：1241
- 题解数：0
- 题目 ID：800454

1\. 实现 INNER JOIN 功能，需要支持 join 多张表。

2\. 当前已经支持多表查询的功能，这里主要工作是语法扩展，并考虑数据量比较大时如何处理。

3\. 注意带有多条 on 条件的 join 操作。

4\. 注意隐式内连接和 <span style="color: rgb(0, 0, 0);">INNER JOIN 混合的情况。</span>

示例 SQL 语句：

```
SELECT * FROM t INNER JOIN t1 ON t.id = t1.id, t2 WHERE t2.id = t.id;
```

<span style="color: rgb(0, 0, 0);">有关基本类型转换可以参考：</span>[22年赛题-基本类型转换](https://github.com/oceanbase/miniob/wiki/OceanBase--%E6%95%B0%E6%8D%AE%E5%BA%93%E5%A4%A7%E8%B5%9B-2022-%E5%88%9D%E8%B5%9B%E8%B5%9B%E9%A2%98#4-%E5%9F%BA%E6%9C%AC%E7%B1%BB%E5%9E%8B%E8%BD%AC%E6%8D%A2)

### 6. expression

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800451&subQuestionName=expression
- 难度：简单
- 题目总分：10
- 通过率：22.3%
- 通过人数：454
- 未通过人数：1584
- 题解数：0
- 题目 ID：800451

实现表达式功能。

各种表达式运算是 SQL 的基础，有了表达式，才能使用 SQL 描述丰富的应用场景。

这里的表达式仅考虑算数表达式，可以参考现有实现的 calc 语句，可以参考 [表达式解析](https://oceanbase.github.io/miniob/design/miniob-sql-expression) ，在 SELECT 语句中实现。

如果有些表达式运算结果有疑问，可以在 MySQL 中执行相应的 SQL，然后参考 MySQL 的执行即可。比如一个数字除以 0，应该按照NULL 类型的数字来处理。

当然为了简化，这里只有数字类型的运算。

示例 SQL 语句：

```
SELECT col3 * 4 FROM exp_table WHERE 5 + col2 < col1 + 6;
```

### 7. function

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800449&subQuestionName=function
- 难度：简单
- 题目总分：10
- 通过率：17.5%
- 通过人数：357
- 未通过人数：1681
- 题解数：0
- 题目 ID：800449

实现一些常见的函数，包括 length、round 和 date\_format。

函数是 SQL 中常见功能之一。这些函数除了用在查询字段上，还可能会出现在条件语句中，作为查询数据过滤条件之一。

为了简化，仅考虑上述三种函数即可，其中 length 只考虑 char 类型，round 只考虑 float 类型，date\_format 只考虑 date 类型，遇到其他的数据类型返回 FAILURE 即可。

示例 SQL 语句：

```
SELECT id, LENGTH(name), ROUND(score), DATE_FORMAT(u_date, '%D,%M,%Y') FROM function_table;
```

<span style="color: rgb(0, 0, 0);">可以参考：</span>[22年赛题说明-函数](https://github.com/oceanbase/miniob/wiki/OceanBase--%E6%95%B0%E6%8D%AE%E5%BA%93%E5%A4%A7%E8%B5%9B-2022-%E5%88%9D%E8%B5%9B%E8%B5%9B%E9%A2%98#10-%E5%87%BD%E6%95%B0)

<span style="color: rgb(0, 0, 0);">DATE\_FORMAT 函数补充说明（ </span>[MySQL文档](https://dev.mysql.com/doc/refman/9.4/en/date-and-time-functions.html#function_date-format) ）

只需实现如下格式符即可：

|        |                    |            |
|--------|--------------------|------------|
| 格式符 | 含义               | 示例       |
| `%Y`   | 四位年份           | `2024`     |
| `%y`   | 两位年份           | `24`       |
| `%m`   | 两位月份           | `05`       |
| `%d`   | 两位日期           | `20`       |
| `%D`   | 带英文后缀的日期   | `20th`     |
| `%M`   | 完整月份名（英文） | `December` |

对于 %z 和 %n 这类非法格式符<span style="color: rgb(0, 0, 0);">不进行替换，原样输出 </span>

```
SELECT DATE_FORMAT('2024-05-20', 'abc %z def %n ghi');
abc z def n ghi
```

### 8. multi-index

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800456&subQuestionName=multi-index
- 难度：中等
- 题目总分：20
- 通过率：39.8%
- 通过人数：812
- 未通过人数：1226
- 题解数：0
- 题目 ID：800456

多字段索引功能。即一个索引中同时关联了多个字段。

此功能除了需要修改语法分析，还需要调整 B+ 树相关的实现，帮助同学们增加 B+ 树数据存储知识的理解。

示例 SQL 语句：

```
CREATE INDEX i_1_12 ON multi_index(col1, col2);
```

### 9. unique

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800457&subQuestionName=unique
- 难度：中等
- 题目总分：20
- 通过率：5.6%
- 通过人数：114
- 未通过人数：1924
- 题解数：0
- 题目 ID：800457

实现唯一索引功能。

唯一索引是指一个索引上的数据都不是重复的。支持使用简单的 SQL 创建索引。

注意：需要支持多列的唯一索引。为了简化场景，不考虑在已有重复数据的列上建立唯一索引的情况。

需要考虑数据插入、数据更新等场景。此功能主要涉及到 B+ 树与语法解析方面的模块。

注意：本题目需要实现 drop index 功能。

示例 SQL 语句：

```
CREATE UNIQUE INDEX t_i ON t(id);
DROP INDEX t_i ON t;
```

### 10. group-by

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800453&subQuestionName=group-by
- 难度：中等
- 题目总分：20
- 通过率：9.4%
- 通过人数：191
- 未通过人数：1847
- 题解数：0
- 题目 ID：800453

**本题目要求实现聚合函数功能和数据分组（group by）功能。**

**聚合函数功能要求：**

1\. 实现聚合函数 max/min/count/avg/sum。

2\. 聚合函数会遍历所有相关的行数据做相关的统计，并输出结果。

3\. 对于如下这样的聚合和单个字段混合的测试语句，返回FAILURE。

```
SELECT id, COUNT(age) FROM t;
SELECT COUNT(id) FROM t1 GROUP BY name HAVING COUNT(id) > 2;
```

4\. 测试用例中不会包含一些比较复杂的处理，比如表达式。但是有些数据类型会有隐式转换，比如avg计算整数类型时，结果会是浮点数。

5\. 注意处理语义处理时的异常场景，比如:

-   查询不存在的字段；
-   <span style="color: rgb(0, 0, 0);">查询空字段；</span>

**group by 功能要求：**

分组功能也是数据库的基本功能之一，目的是为了方便用户查询数据结果，按照一定条件进行分组，方便分析数据。

按照一个或多个字段对查询结果分组，group by中的聚合函数不要求支持表达式。

需要支持having子句，因为聚合函数不能出现在where后面，所以增加having子句用于筛选分组后的数据。不过having只和聚合函数一起出现。

注意需要考虑分组字段为null的情况。

示例：

```
SELECT t.id, t.name, AVG(t.score), AVG(t2.age) FROM t, t2 WHERE t.id = t2.id GROUP BY t.id, t.name;
SELECT COUNT(id) FROM t1 GROUP BY name HAVING COUNT(id) > 2;
```

### 11. simple-sub-query

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800455&subQuestionName=simple-sub-query
- 难度：中等
- 题目总分：20
- 通过率：17.9%
- 通过人数：365
- 未通过人数：1673
- 题解数：0
- 题目 ID：800455

简单子查询。此功能是对基础查询功能的一个扩充，使数据库 SQL 的能力更加丰富。

这里需要支持的功能包括但不限于：

  - 支持简单的 IN (NOT IN) 语句，不涉及基本类型转换。注意 NOT IN 语句面对 NULL 时的特殊性。

  - 支持与子查询结果做比较运算。 注意子查询结果为多行的情况。

  - 支持子查询中带聚合函数。

  - 子查询中不会与主查询做关联。这也是简单子查询区分于复杂子查询的地方。

  - 表达式中可能存在不同类型值比较。

示例 SQL 语句：

```
SELECT * FROM ssq_1 WHERE id IN (SELECT ssq_2.id FROM ssq_2);
```

### 12. alias

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800459&subQuestionName=alias
- 难度：中等
- 题目总分：20
- 通过率：10.1%
- 通过人数：206
- 未通过人数：1832
- 题解数：0
- 题目 ID：800459

实现字段、表别名的功能。

别名功能看起来不是数据库的必备功能，但是它可以极大地方便我们使用。比如美化或简化数据结构的输出、优化查询语句的编写。

表和列可以临时取别名，在打印结果时表和字段都打印别名（如果有）。在查询时能够使用表的别名访问表的字段。两个表的别名在同一层查询中不能重复，子查询里面和外面的表的别名可以重复。列的别名只需要支持查询结果显示，不需要考虑使用列别名进行运算和比较，也不考虑列的别名重复。需要考虑表别名对运算和比较的影响。

<span style="color: rgb(0, 0, 0);">注意：本题前置依赖 simple\_sub\_query中的 in 和子查询。</span>

示例：

```
SELECT column_name AS col FROM table_name;
SELECT t.column_name FROM table_name AS t;
```

### 13. null

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800460&subQuestionName=null
- 难度：中等
- 题目总分：20
- 通过率：10.6%
- 通过人数：216
- 未通过人数：1822
- 题解数：0
- 题目 ID：800460

NULL 是数据库的一个基本功能。

表字段可以有NULL属性，表示此字段是否允许为 NULL 值。NULL 在做数值运算、逻辑比较时，都有特殊的含义，同时在做聚合运算(count/avg 等）都需要做不同的处理。使用 NULL 关键字，不区分大小写。

注意：

  - NULL 与任何数值比较，结果都是 false。

  - NULL 用例非常基础，它出现在许多其它用例中。

示例：

```
CREATE TABLE t(id INT NULL, name CHAR NOT NULL);
```

<span style="color: rgb(0, 0, 0);">其中字段 id 可以为 NULL 值，而 name 字段不允许为 NULL 值。</span>

### 14. union

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800452&subQuestionName=union
- 难度：中等
- 题目总分：20
- 通过率：6.4%
- 通过人数：130
- 未通过人数：1908
- 题解数：0
- 题目 ID：800452

UNION 操作符用于连接两个以上的 SELECT 语句的结果组合到一个结果集合，并去除重复的行，而 <span style="color: rgb(0, 0, 0);">UNION ALL 不去除重复行。</span>

UNION (<span style="color: rgb(0, 0, 0);">UNION ALL</span>) 操作符必须由两个或多个 SELECT 语句组成，每个 SELECT 语句的列数和对应位置的数据类型必须相同。

<span style="color: rgb(51, 51, 51); background-color: rgb(250, 252, 253);">为了简化，本题不会出现SELECT之间</span><span style="color: rgb(0, 0, 0);">对应位置的数据类型不同的情况。</span>

<span style="color: rgb(0, 0, 0);">注意：</span>

<span style="color: rgb(0, 0, 0);">UNION 的操作是合并两个查询结果，并自动去除所有重复的行（基于整行完全相同），所以单个表中的重复数据也会被去重。</span>

<span style="color: rgb(0, 0, 0);">UNION (UNION ALL) 执行顺序从左到右。为了简化，本题不存在使用括号改变其执行顺序。</span>

示例 SQL 语句：

```
SELECT * FROM t UNION SELECT * FROM t1 UNION ALL SELECT * FROM t2;
```

### 15. order-by

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800462&subQuestionName=order-by
- 难度：中等
- 题目总分：20
- 通过率：23.8%
- 通过人数：486
- 未通过人数：1552
- 题解数：0
- 题目 ID：800462

实现排序功能。

排序也是数据库的一个基本功能，就是将查询的结果按照指定的字段和顺序进行排序。

示例：

```
SELECT * from t, t1 WHERE t.id = t1.id ORDER BY t.id ASC, t1.score DESC;
```

示例中就是将结果按照 t.id 升序、t1.score 降序的方式排序。

其中 asc 表示升序排序，desc 表示降序。如果不指定排序顺序，就是升序，即 asc。

在MySQL中，可以使用 order by 1,2 的方式，指定排序字段，我们为了简化，不实现这种功能。

### 16. vector-basic

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800450&subQuestionName=vector-basic
- 难度：中等
- 题目总分：20
- 通过率：12.3%
- 通过人数：250
- 未通过人数：1788
- 题解数：0
- 题目 ID：800450

### 向量数据库题目一：向量类型基础功能

<span style="color: rgb(77, 77, 77); background-color: rgb(255, 255, 255);">实现向量类型：</span>

<span style="color: rgb(77, 77, 77); background-color: rgb(255, 255, 255);">1. 支持创建包含向量类型的表。</span>

<span style="color: rgb(77, 77, 77); background-color: rgb(255, 255, 255);">2. 支持插入向量类型的记录。</span>

-   <span style="color: rgb(77, 77, 77); background-color: rgb(255, 255, 255);">实现距离表达式计算：</span>

<span style="color: rgb(0, 0, 0);">    DISTANCE(v1, v2, COSINE/EUCLIDEAN/DOT)</span>

-   <span style="color: rgb(0, 0, 0);">实现 VECTOR\_TO\_STRING，STRING\_TO\_VECTOR，参考MySQL。</span>

<span style="color: rgb(0, 0, 0);">    所有输入的向量都由 STRING\_TO\_VECTOR 包裹</span>

<span style="color: rgb(0, 0, 0);">    查询向量列时，对应列由 VECTOR\_TO\_STRING 包裹</span>

SQL 示例：

```
CREATE TABLE TEST (id INT, C1 VECTOR(3));
INSERT INTO TEST VALUES(1, STRING_TO_VECTOR('[1, 2, 3]'));
SELECT DISTANCE(STRING_TO_VECTOR('[1, 2, 3]'), STRING_TO_VECTOR('[2, 3, 4]'), 'COSINE') AS DIST_COSINE;

SELECT ID, VECTOR_TO_STRING(C1) AS VEC_STR, DISTANCE(C1, STRING_TO_VECTOR('[1, 2, 3]'), 'EUCLIDEAN') AS DIST_EUC FROM TEST;
1, [3.07000e+00,-1.24000e+00], 2.31
```

对于 MySQL 向量的科学计数法显示<span style="color: rgb(0, 0, 0);">（如：\[-4.94000e+00, 9.23000e+00\] ）</span>，可以参考如下代码：

```
std::string formatFloatToString(float value) const {
    std::ostringstream oss;    // 设置格式：
    oss << std::scientific    // 科学计数法
        << std::setprecision(5)  // 保留 5 位小数，例如 3.61000
        << value;
    return oss.str();
}
```

<span style="color: rgb(77, 77, 77); background-color: rgb(255, 255, 255);">详情请参考文档：</span>

[https://oceanbase.github.io/miniob/game/miniob-vectordb-2025](https://oceanbase.github.io/miniob/game/miniob-vectordb-2025/)

### 17. text

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800461&subQuestionName=text
- 难度：中等
- 题目总分：20
- 通过率：15.8%
- 通过人数：321
- 未通过人数：1717
- 题解数：0
- 题目 ID：800461

实现文本字段。

在数据库中，我们会有存储超大数据的需求，比如在数据库中存放网页。这里考虑使用 text 字段，存放超大数据。参考 MySQL 的实现，text 字段的最大长度为 65535 个字节，插入超出这个长度的数据就报错。

这里除了需要实现语法解析，还需要考虑如何在存储引擎中存放超长字段，扩展 record\_manager，以支持超过一页的数据。

示例：

```
CREATE TABLE t(id INT, article TEXT);
```

### 18. vector-search

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800463&subQuestionName=vector-search
- 难度：中等
- 题目总分：20
- 通过率：11.6%
- 通过人数：237
- 未通过人数：1801
- 题解数：0
- 题目 ID：800463

### 向量数据库题目二：向量检索

-   需要在没有索引的场景下，支持向量检索功能（即精确检索）。

向量检索示例：

```
SELECT ID FROM TAB_VEC ORDER BY DISTANCE(B, STRING_TO_VECTOR('[10, 0.0, 5.0]'), 'EUCLIDEAN') LIMIT 1;
```

<span style="color: rgb(77, 77, 77); background-color: rgb(255, 255, 255);">详情请参考文档：</span>

[https://oceanbase.github.io/miniob/game/miniob-vectordb-2025](https://oceanbase.github.io/miniob/game/miniob-vectordb-2025/)

### 19. alter

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800458&subQuestionName=alter
- 难度：中等
- 题目总分：20
- 通过率：4.7%
- 通过人数：95
- 未通过人数：1943
- 题解数：0
- 题目 ID：800458

当我们需要修改数据表名或者修改数据表字段时，就需要使用到 alter 命令。

<span style="color: rgb(0, 0, 0);">ALTER</span> 命令用于修改数据库、表和索引等对象的结构。

<span style="color: rgb(0, 0, 0);">ALTER</span> 命令允许你添加、修改或删除数据库对象，并且可以用于更改表的列定义、添加约束、创建和删除索引等操作。

ALTER 命令非常强大，可以在数据库结构发生变化时进行灵活的修改和调整。

为了简化，本题不涉及对字段数据类型的修改，且所有操作只基于表。

本题单次操作最多只会修改一个列，且表上只会构建单列索引，注意更改表结构后相应索引的变化。

只需实现如下四种 SQL 语句：

```
ALTER TABLE alter_table_1 ADD COLUMN col INT;
ALTER TABLE alter_table_1 DROP COLUMN col;
ALTER TABLE alter_table_1 CHANGE COLUMN col id INT; // 将 col 列改名为 id，不涉及类型修改
ALTER TABLE alter_table_1 RENAME TO alter_table_2;
```

### 20. update-mvcc

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800464&subQuestionName=update-mvcc
- 难度：困难
- 题目总分：30
- 通过率：2.9%
- 通过人数：60
- 未通过人数：1978
- 题解数：0
- 题目 ID：800464

实现MVCC中的更新功能。

事务是数据库的基本功能，此功能希望同学们补充 MVCC（多版本并发控制）的 update 功能。这里主要考察不同连接同时操作数据库表时的问题。

事务管理在MiniOB并没有完善的实现，比如原子性提交、持久化、垃圾回收等。如果有兴趣的同学，可以给 MiniOB 提交 PR。

注意：

  - 测试多连接，但不会测试多并发（即程序是串行执行的）；

  - 启动 observer 程序时，需要增加 -t mvcc 参数 ，比如 ./bin/observer -f ../etc/observer.ini -s miniob.sock -t mvcc

  - 测试过程中如果遇到官方代码自有的BUG，请修复它，也欢迎提PR。

### 21. complex-sub-query

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800466&subQuestionName=complex-sub-query
- 难度：困难
- 题目总分：30
- 通过率：3.2%
- 通过人数：66
- 未通过人数：1972
- 题解数：0
- 题目 ID：800466

实现复杂子查询功能。

复杂子查询是简单子查询的升级。与其最大的不同就在于子查询中会跟复查询联动。注意需要考虑查询条件中带有聚合函数的情况。

<span style="color: rgb(0, 0, 0);">本题会考察对 EXISTS (NOT EXISTS) 语句的支持。</span>

示例：

```
SELECT * FROM t1 WHERE age IN (SELECT id FROM t2 WHERE t2.name IN (SELECT name FROM t3));
```

备注：查询条件只会使用 and 或 or，没有包含 and 和 or 混合的情况。

### 22. create-view

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800467&subQuestionName=create-view
- 难度：困难
- 题目总分：30
- 通过率：0.5%
- 通过人数：10
- 未通过人数：2028
- 题解数：0
- 题目 ID：800467

实现视图功能。

视图是数据库的基本功能之一。视图可以极大地方便数据库的使用。

视图，顾名思义，就是一个能够自动执行查询语句的虚拟表。

不过视图功能也非常复杂，需要考虑视图更新时如果更新实体表。如果视图对应了单张表，并且没有虚拟字段，更新视图，即更新了实体表。如果实体表中某些字段不在视图中，那此字段的结果应该是 NULL 或默认值。如果视图中包含虚拟字段，比如通过聚合查询的结果，或者视图关联了多张表，他的更新规则就变得复杂起来。在这些场景中，同学们可以参考MySQL的实现方案。

示例：

```
CREATE VIEW create_view_v4 AS select t1.id AS id, t1.age AS age, t2.name AS name FROM create_view_t1 t1, create_view_t2 t2 WHERE t1.id = t2.id;
INSERT INTO create_view_v4(id, age) VALUES(1, 1);
UPDATE create_view_v4 SET id=1, age=1;
```

<span style="color: rgb(77, 77, 77); background-color: rgb(255, 255, 255);">详情请参考文档：</span>

[https://oceanbase.github.io/miniob/game/create-view-2025](https://oceanbase.github.io/miniob/game/create-view-2025/)

MySQL官方文档：

<https://dev.mysql.com/doc/refman/8.0/en/view-updatability.html>

### 23. full-text-index

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800468&subQuestionName=full-text-index
- 难度：困难
- 题目总分：30
- 通过率：0.6%
- 通过人数：13
- 未通过人数：2025
- 题解数：0
- 题目 ID：800468

全文索引（Full-Text Search）是一种用于在大量文本数据中进行高效搜索的技术。它通过基于相似度的查询，而不是精确数值比较，来查找文本中的相关信息。相比于使用 LIKE + % 的模糊匹配，全文索引在处理大量数据时速度更快。

简化流程如下：

#### 1.1 **文本预处理**

-   **分词（Tokenization）**：将文本数据拆分为单个的词语或短语，这些词语成为索引的基本单位。例如，“全文索引的原理”可能会被拆分为“全文”、“索引”、“原理”等词条。
-   **去除停用词（Stop Words Removal）**：停用词是指在搜索中不太有意义的常用词汇，如“的”、“是”等。去除这些词可以减少索引的规模，并提高搜索效率。

#### 1.2 **倒排索引（Inverted Index）**

倒排索引是全文索引的核心数据结构。它通过记录每个词条在哪些文档中出现来实现快速查询。

-   **词典（Dictionary）**：保存所有出现过的词条，以及这些词条的文档频率。
-   **倒排列表（Posting List）**：对于每个词条，倒排列表保存了包含该词条的文档ID，甚至可能包含词条在文档中出现的位置和频率等信息。

#### 1.3 **查询处理**

-   **排名和排序**：全文索引系统通常会根据词频、文档长度、词条的逆文档频率（IDF）等因素对查询结果进行评分和排序，返回最相关的文档。

#### **功能要求：**

1\. 支持使用 jieba 进行中文分词

<span style="color: rgb(0, 0, 0);">2. 支持创建全文索引，支持全文索引查询</span>

<span style="color: rgb(0, 0, 0);">3. 支持使用 BM25 评分</span>

SQL示例：

```
SELECT TOKENIZE('information_schema.SCHEMATA表的主要功能是什么？', 'jieba') as text_tokens;
["information", "schema", "SCHEMATA", "表", "主要", "功能"] // 结果之间存在空格

ALTER TABLE texts ADD FULLTEXT INDEX idx_texts_jieba (content) WITH PARSER jieba;
SELECT id, content, MATCH(content) AGAINST('你好') AS score FROM texts WHERE MATCH(content) AGAINST('你好') > 0 ORDER BY MATCH(content) AGAINST('你好') DESC, id ASC;
```

注意：

1\. 本题只需实现使用 [cppjieba](https://github.com/yanyiwu/cppjieba) 库进行 jieba 分词。

初始代码已经实现 cppjieba 的对接，提测能正常通过编译。cppjieba 所需词库位于 /usr/local/dict/ 下。

2\. 本题只需使用 BM25 评分进行排序，<span style="color: rgb(0, 0, 0);">MATCH(content) AGAINST('xxx') 返回结果为该文档的 BM25 评分</span>

BM25 分数计算公式如下：

![1760683225](https://obcommunityprod.oss-cn-shanghai.aliyuncs.com/prod/competiondetail/2025-10/9569c708-0bf6-4e42-8d14-06bbab0af2f4.png)

<span style="color: rgb(0, 0, 0);">其中 k1 = 1.5，b = 0.75</span>

3\. 本题只会对单列建全文索引，不考虑多列索引。

测评端代码标准：

-   测评端 jieba 分词： <https://github.com/HuXin0817/cppjieba/>
-   BM25：  [https://github.com/dorianbrown/rank\_bm25/tree/0.2.2/](https://github.com/dorianbrown/rank_bm25/tree/0.2.2)

### 24. big-order-by

- 详情链接：https://open.oceanbase.com/train/TopicDetails?questionId=600060&subQesitonId=800465&subQuestionName=big-order-by
- 难度：困难
- 题目总分：30
- 通过率：9.0%
- 通过人数：184
- 未通过人数：1854
- 题解数：0
- 题目 ID：800465

大数据量的排序功能。

在内存有限的情况下，实现大数据量的排序，需要优化内存使用。注意，测试数据具备一定的随机性。

有四张表，每张表有 20 个字段，数据量在 20 左右，所有表在一起做笛卡尔积查询，并且对每个字段都会做 order by 排序。通常这么大的数据量不能在纯内存中排序完成，所以需要考虑使用外部排序。

注意：本题内存限制在 350MB，返回结果中的调用栈出现 <span style="color: rgb(51, 51, 51); background-color: rgb(255, 255, 255); font-size: 13px;">memtracer::MemTracer::alloc(unsigned long) (下图 \#5) 时说明超出内存限制</span><span style="color: rgb(0, 0, 0);">。</span>

```
#0  __pthread_kill_implementation (no_tid=0, signo=6, threadid=) at ./nptl/pthread_kill.c:44
#1  __pthread_kill_internal (signo=6, threadid=) at ./nptl/pthread_kill.c:78
#2  __GI___pthread_kill (threadid=, signo=signo@entry=6) at ./nptl/pthread_kill.c:89
#3  0x00007fdc46c7527e in __GI_raise (sig=sig@entry=6) at ../sysdeps/posix/raise.c:26
#4  0x00007fdc46c588ff in __GI_abort () at ./stdlib/abort.c:79
#5  0x00007fdc471df84b in memtracer::MemTracer::alloc(unsigned long) () from /usr/lib/libmemtracer.so
#6  0x00007fdc471de6fe in malloc () from /usr/lib/libmemtracer.so
```

本题预估数据量为 (20)^4 = 1.6e5 行，<span style="color: rgb(0, 0, 0); background-color: rgb(255, 255, 255);">常数较高 (O(80)) 。中间/最终结果预估达到 51.2 MB。</span>

<span style="color: rgb(0, 0, 0);">关于内存限制可以参考：</span>

<span style="color: rgb(0, 0, 0);"> </span>[https://oceanbase.github.io/miniob/game/miniob-memtracer](https://oceanbase.github.io/miniob/game/miniob-memtracer/)

或者MiniOB目录下 docs/docs/game/miniob-memtracer.md

## 本地评测用例补充说明

`test/case/test` 下新增的 `miniob2025-XX-*.test` 用例按 2025 比赛题目粒度划分，每道题对应一个独立 case，便于针对未完成内核逐题运行和定位。除全文索引题外，标准输出使用仓库本地 `mysql-server/` 中的比赛指定 MySQL 版本生成。

全文索引题 `miniob2025-23-full-text-index.test` 不使用 MySQL 作为标准输出来源，因为比赛语义依赖 cppjieba 分词和 BM25 评分，和 MySQL fulltext 行为不同。

| 题号 | 题目 | 本地 case | 标准输出 |
| --- | --- | --- | --- |
| 1 | basic | `miniob2025-01-basic.test` | `miniob2025-01-basic.result` |
| 2 | update | `miniob2025-02-update.test` | `miniob2025-02-update.result` |
| 3 | drop-table | `miniob2025-03-drop-table.test` | `miniob2025-03-drop-table.result` |
| 4 | date | `miniob2025-04-date.test` | `miniob2025-04-date.result` |
| 5 | join-tables | `miniob2025-05-join-tables.test` | `miniob2025-05-join-tables.result` |
| 6 | expression | `miniob2025-06-expression.test` | `miniob2025-06-expression.result` |
| 7 | function | `miniob2025-07-function.test` | `miniob2025-07-function.result` |
| 8 | multi-index | `miniob2025-08-multi-index.test` | `miniob2025-08-multi-index.result` |
| 9 | unique | `miniob2025-09-unique.test` | `miniob2025-09-unique.result` |
| 10 | group-by | `miniob2025-10-group-by.test` | `miniob2025-10-group-by.result` |
| 11 | simple-sub-query | `miniob2025-11-simple-sub-query.test` | `miniob2025-11-simple-sub-query.result` |
| 12 | alias | `miniob2025-12-alias.test` | `miniob2025-12-alias.result` |
| 13 | null | `miniob2025-13-null.test` | `miniob2025-13-null.result` |
| 14 | union | `miniob2025-14-union.test` | `miniob2025-14-union.result` |
| 15 | order-by | `miniob2025-15-order-by.test` | `miniob2025-15-order-by.result` |
| 16 | vector-basic | `miniob2025-16-vector-basic.test` | `miniob2025-16-vector-basic.result` |
| 17 | text | `miniob2025-17-text.test` | `miniob2025-17-text.result` |
| 18 | vector-search | `miniob2025-18-vector-search.test` | `miniob2025-18-vector-search.result` |
| 19 | alter | `miniob2025-19-alter.test` | `miniob2025-19-alter.result` |
| 20 | update-mvcc | `miniob2025-20-update-mvcc.test` | `miniob2025-20-update-mvcc.result` |
| 21 | complex-sub-query | `miniob2025-21-complex-sub-query.test` | `miniob2025-21-complex-sub-query.result` |
| 22 | create-view | `miniob2025-22-create-view.test` | `miniob2025-22-create-view.result` |
| 23 | full-text-index | `miniob2025-23-full-text-index.test` | 不使用 MySQL 生成 |
| 24 | big-order-by | `miniob2025-24-big-order-by.test` | `miniob2025-24-big-order-by.result` |
