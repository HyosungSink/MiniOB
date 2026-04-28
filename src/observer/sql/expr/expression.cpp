/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "common/defs.h"
#include "sql/expr/tuple.h"
#include "sql/expr/arithmetic_operator.hpp"
#include "session/session.h"
#include "sql/optimizer/logical_plan_generator.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "sql/parser/expression_binder.h"
#include "sql/parser/parse.h"
#include "sql/stmt/stmt.h"
#include "storage/db/db.h"
#include "storage/trx/trx.h"

#include <cmath>
#include <cstdio>
#include <cctype>
#include <cstring>
#include <strings.h>

using namespace std;

static thread_local const Tuple *current_subquery_outer_tuple = nullptr;

class ScopedSubqueryOuterTuple
{
public:
  explicit ScopedSubqueryOuterTuple(const Tuple *outer_tuple)
      : previous_(current_subquery_outer_tuple)
  {
    if (outer_tuple != nullptr) {
      current_subquery_outer_tuple = outer_tuple;
    }
  }

  ~ScopedSubqueryOuterTuple() { current_subquery_outer_tuple = previous_; }

private:
  const Tuple *previous_ = nullptr;
};

RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  TupleCellSpec spec(table_name(), field_name());
  RC rc = tuple.find_cell(spec, value);
  if (rc == RC::NOTFOUND && current_subquery_outer_tuple != nullptr && current_subquery_outer_tuple != &tuple) {
    rc = current_subquery_outer_tuple->find_cell(spec, value);
  }
  return rc;
}

bool FieldExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::FIELD) {
    return false;
  }
  const auto &other_field_expr = static_cast<const FieldExpr &>(other);
  return table_name() == other_field_expr.table_name() && field_name() == other_field_expr.field_name();
}

// TODO: 在进行表达式计算时，`chunk` 包含了所有列，因此可以通过 `field_id` 获取到对应列。
// 后续可以优化成在 `FieldExpr` 中存储 `chunk` 中某列的位置信息。
RC FieldExpr::get_column(Chunk &chunk, Column &column)
{
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    column.reference(chunk.column(field().meta()->field_id()));
  }
  return RC::SUCCESS;
}

bool ValueExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::VALUE) {
    return false;
  }
  const auto &other_value_expr = static_cast<const ValueExpr &>(other);
  return value_.compare(other_value_expr.get_value()) == 0;
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  value = value_;
  return RC::SUCCESS;
}

RC ValueExpr::get_column(Chunk &chunk, Column &column)
{
  column.init(value_, chunk.rows());
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type, bool loose_numeric)
    : child_(std::move(child)), cast_type_(cast_type), loose_numeric_(loose_numeric)
{}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }
  if (loose_numeric_ && value.attr_type() == AttrType::CHARS) {
    if (cast_type_ == AttrType::INTS) {
      cast_value.set_int(value.get_int());
      return rc;
    }
    if (cast_type_ == AttrType::FLOATS) {
      cast_value.set_float(value.get_float());
      return rc;
    }
  }
  rc = Value::cast_to(value, cast_type_, cast_value);
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &result) const
{
  Value value;
  RC rc = child_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

RC CastExpr::get_column(Chunk &chunk, Column &column)
{
  Column child_column;
  RC rc = child_->get_column(chunk, child_column);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  column.init(cast_type_, child_column.attr_len());
  for (int i = 0; i < child_column.count(); ++i) {
    Value value = child_column.get_value(i);
    Value cast_value;
    rc = cast(value, cast_value);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    column.append_value(cast_value);
  }
  return rc;
}

RC CastExpr::try_get_value(Value &result) const
{
  Value value;
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{
}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  RC  rc         = RC::SUCCESS;
  result         = false;
  if (left.is_null() || right.is_null()) {
    return rc;
  }

  int cmp_result = 0;
  if (left.attr_type() == AttrType::BOOLEANS && right.attr_type() == AttrType::BOOLEANS) {
    cmp_result = static_cast<int>(left.get_boolean()) - static_cast<int>(right.get_boolean());
  } else {
    cmp_result = left.compare(right);
  }
  switch (comp_) {
    case EQUAL_TO: {
      result = (0 == cmp_result);
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);
    } break;
    case NOT_EQUAL: {
      result = (cmp_result != 0);
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);
    } break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);
      rc = RC::INTERNAL;
    } break;
  }

  return rc;
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  Value left_cell;
  Value right_cell;

  RC rc = left_->try_get_value(left_cell);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = right_->try_get_value(right_cell);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  if (left_cell.is_null() || right_cell.is_null()) {
    cell.set_null();
    return RC::SUCCESS;
  }

  bool value = false;
  rc         = compare_value(left_cell, right_cell, value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
  } else {
    cell.set_boolean(value);
  }
  return rc;
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value left_value;
  Value right_value;

  RC rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }

  if (left_value.is_null() || right_value.is_null()) {
    value.set_null();
    return RC::SUCCESS;
  }

  bool bool_value = false;

  rc = compare_value(left_value, right_value, bool_value);
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }
  return rc;
}

RC ComparisonExpr::eval(Chunk &chunk, vector<uint8_t> &select)
{
  RC     rc = RC::SUCCESS;
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_column.attr_type() != right_column.attr_type()) {
    LOG_WARN("cannot compare columns with different types");
    return RC::INTERNAL;
  }
  if (left_column.attr_type() == AttrType::INTS) {
    rc = compare_column<int>(left_column, right_column, select);
  } else if (left_column.attr_type() == AttrType::FLOATS) {
    rc = compare_column<float>(left_column, right_column, select);
  } else if (left_column.attr_type() == AttrType::CHARS) {
    int rows = 0;
    if (left_column.column_type() == Column::Type::CONSTANT_COLUMN) {
      rows = right_column.count();
    } else {
      rows = left_column.count();
    }
    for (int i = 0; i < rows; ++i) {
      Value left_val = left_column.get_value(i);
      Value right_val = right_column.get_value(i);
      bool        result   = false;
      rc                   = compare_value(left_val, right_val, result);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
        return rc;
      }
      select[i] &= result ? 1 : 0;
    }

  } else {
    LOG_WARN("unsupported data type %d", left_column.attr_type());
    return RC::INTERNAL;
  }
  return rc;
}

template <typename T>
RC ComparisonExpr::compare_column(const Column &left, const Column &right, vector<uint8_t> &result) const
{
  RC rc = RC::SUCCESS;

  bool left_const  = left.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    compare_result<T, true, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else if (left_const && !right_const) {
    compare_result<T, true, false>((T *)left.data(), (T *)right.data(), right.count(), result, comp_);
  } else if (!left_const && right_const) {
    compare_result<T, false, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else {
    compare_result<T, false, false>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> &children)
    : conjunction_type_(type), children_(std::move(children))
{}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
      return rc;
    }
    bool bool_value = tmp_value.get_boolean();
    if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
      value.set_boolean(bool_value);
      return rc;
    }
  }

  bool default_value = (conjunction_type_ == Type::AND);
  value.set_boolean(default_value);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

InExpr::InExpr(unique_ptr<Expression> left, vector<unique_ptr<Expression>> values, bool not_in)
    : left_(std::move(left)), values_(std::move(values)), not_in_(not_in)
{}

RC InExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value left_value;
  RC rc = left_->get_value(tuple, left_value);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get value of IN left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (left_value.is_null()) {
    value.set_null();
    return RC::SUCCESS;
  }

  bool has_null = false;
  bool matched  = false;
  for (const unique_ptr<Expression> &expr : values_) {
    Value right_value;
    rc = expr->get_value(tuple, right_value);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to get value of IN list expression. rc=%s", strrc(rc));
      return rc;
    }

    if (right_value.is_null()) {
      has_null = true;
      continue;
    }

    if (left_value.compare(right_value) == 0) {
      matched = true;
      break;
    }
  }

  if (matched) {
    value.set_boolean(!not_in_);
  } else if (has_null) {
    value.set_null();
  } else {
    value.set_boolean(not_in_);
  }

  return RC::SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////

SubqueryExpr::SubqueryExpr(string sql) : sql_(std::move(sql)) {}

SubqueryExpr::SubqueryExpr(string sql, AttrType value_type, int value_length, AttrType cast_type)
    : sql_(std::move(sql)), value_type_(value_type), value_length_(value_length), cast_type_(cast_type)
{}

static string trim_copy(const string &text)
{
  size_t begin = 0;
  while (begin < text.size() && isspace(static_cast<unsigned char>(text[begin]))) {
    begin++;
  }
  size_t end = text.size();
  while (end > begin && isspace(static_cast<unsigned char>(text[end - 1]))) {
    end--;
  }
  return text.substr(begin, end - begin);
}

static bool word_equals_at(const string &text, size_t pos, const char *word)
{
  const size_t len = strlen(word);
  if (pos + len > text.size()) {
    return false;
  }
  for (size_t i = 0; i < len; i++) {
    if (toupper(static_cast<unsigned char>(text[pos + i])) != toupper(static_cast<unsigned char>(word[i]))) {
      return false;
    }
  }

  bool left_ok = pos == 0 ||
                 (!isalnum(static_cast<unsigned char>(text[pos - 1])) && text[pos - 1] != '_');
  bool right_ok = pos + len == text.size() ||
                  (!isalnum(static_cast<unsigned char>(text[pos + len])) && text[pos + len] != '_');
  return left_ok && right_ok;
}

static size_t find_top_level_word(const string &text, const char *word, size_t start = 0)
{
  int  depth = 0;
  char quote = 0;
  for (size_t i = start; i < text.size(); i++) {
    char ch = text[i];
    if (quote != 0) {
      if (ch == quote) {
        quote = 0;
      }
      continue;
    }
    if (ch == '\'' || ch == '"') {
      quote = ch;
      continue;
    }
    if (ch == '(') {
      depth++;
      continue;
    }
    if (ch == ')') {
      depth--;
      continue;
    }
    if (depth == 0 && word_equals_at(text, i, word)) {
      return i;
    }
  }
  return string::npos;
}

static size_t find_matching_paren(const string &text, size_t open_pos)
{
  int  depth = 0;
  char quote = 0;
  for (size_t i = open_pos; i < text.size(); i++) {
    char ch = text[i];
    if (quote != 0) {
      if (ch == quote) {
        quote = 0;
      }
      continue;
    }
    if (ch == '\'' || ch == '"') {
      quote = ch;
      continue;
    }
    if (ch == '(') {
      depth++;
    } else if (ch == ')') {
      depth--;
      if (depth == 0) {
        return i;
      }
    }
  }
  return string::npos;
}

static vector<string> split_select_items(const string &select_list)
{
  vector<string> items;
  size_t start = 0;
  int    depth = 0;
  char   quote = 0;
  for (size_t i = 0; i < select_list.size(); i++) {
    char ch = select_list[i];
    if (quote != 0) {
      if (ch == quote) {
        quote = 0;
      }
      continue;
    }
    if (ch == '\'' || ch == '"') {
      quote = ch;
      continue;
    }
    if (ch == '(') {
      depth++;
    } else if (ch == ')') {
      depth--;
    } else if (ch == ',' && depth == 0) {
      items.push_back(trim_copy(select_list.substr(start, i - start)));
      start = i + 1;
    }
  }
  items.push_back(trim_copy(select_list.substr(start)));
  return items;
}

static RC execute_select_sql(const string &sql, vector<TupleCellSpec> &specs, vector<vector<Value>> &rows)
{
  Session *session = Session::current_session();
  if (session == nullptr) {
    return RC::INTERNAL;
  }
  Db *db = session->get_current_db();
  if (db == nullptr) {
    return RC::SCHEMA_DB_NOT_EXIST;
  }

  ParsedSqlResult parsed_sql_result;
  RC rc = parse(sql.c_str(), &parsed_sql_result);
  if (OB_FAIL(rc)) {
    return rc;
  }
  if (parsed_sql_result.sql_nodes().size() != 1 || parsed_sql_result.sql_nodes().front()->flag != SCF_SELECT) {
    return RC::SQL_SYNTAX;
  }

  Stmt *raw_stmt = nullptr;
  rc = Stmt::create_stmt(db, *parsed_sql_result.sql_nodes().front(), raw_stmt);
  unique_ptr<Stmt> stmt(raw_stmt);
  if (OB_FAIL(rc)) {
    return rc;
  }

  LogicalPlanGenerator logical_plan_generator;
  unique_ptr<LogicalOperator> logical_operator;
  rc = logical_plan_generator.create(stmt.get(), logical_operator);
  if (OB_FAIL(rc)) {
    return rc;
  }

  PhysicalPlanGenerator physical_plan_generator;
  unique_ptr<PhysicalOperator> physical_operator;
  rc = physical_plan_generator.create(*logical_operator, physical_operator, session);
  if (OB_FAIL(rc)) {
    return rc;
  }

  TupleSchema schema;
  physical_operator->tuple_schema(schema);
  specs.clear();
  for (int i = 0; i < schema.cell_num(); i++) {
    specs.push_back(schema.cell_at(i));
  }

  Trx *trx = session->current_trx();
  trx->start_if_need();
  rc = physical_operator->open(trx);
  if (OB_FAIL(rc)) {
    return rc;
  }

  while ((rc = physical_operator->next()) == RC::SUCCESS) {
    Tuple *tuple = physical_operator->current_tuple();
    if (tuple == nullptr) {
      physical_operator->close();
      return RC::INTERNAL;
    }

    vector<Value> row;
    for (int i = 0; i < tuple->cell_num(); i++) {
      Value cell;
      rc = tuple->cell_at(i, cell);
      if (OB_FAIL(rc)) {
        physical_operator->close();
        return rc;
      }
      row.push_back(std::move(cell));
    }
    rows.push_back(std::move(row));
  }

  RC close_rc = physical_operator->close();
  if (rc == RC::RECORD_EOF) {
    rc = close_rc;
  }
  return rc;
}

static string unqualify_column_name(string item)
{
  item = trim_copy(item);
  size_t dot_pos = item.rfind('.');
  if (dot_pos != string::npos) {
    item = item.substr(dot_pos + 1);
  }
  return trim_copy(item);
}

static RC materialize_derived_select(const string &sql, vector<Value> &values, AttrType cast_type)
{
  const string trimmed = trim_copy(sql);
  if (!word_equals_at(trimmed, 0, "SELECT")) {
    return RC::SQL_SYNTAX;
  }

  size_t from_pos = find_top_level_word(trimmed, "FROM", strlen("SELECT"));
  if (from_pos == string::npos) {
    return RC::SQL_SYNTAX;
  }

  string select_list = trimmed.substr(strlen("SELECT"), from_pos - strlen("SELECT"));
  size_t derived_open = trimmed.find('(', from_pos + strlen("FROM"));
  if (derived_open == string::npos) {
    return RC::SQL_SYNTAX;
  }
  size_t derived_close = find_matching_paren(trimmed, derived_open);
  if (derived_close == string::npos) {
    return RC::SQL_SYNTAX;
  }

  const string inner_sql = trimmed.substr(derived_open + 1, derived_close - derived_open - 1);
  vector<TupleCellSpec> inner_specs;
  vector<vector<Value>> inner_rows;
  RC rc = execute_select_sql(inner_sql, inner_specs, inner_rows);
  if (OB_FAIL(rc)) {
    return rc;
  }

  vector<int> projection_indexes;
  for (const string &raw_item : split_select_items(select_list)) {
    string item = unqualify_column_name(raw_item);
    if (item == "*") {
      for (size_t i = 0; i < inner_specs.size(); i++) {
        projection_indexes.push_back(static_cast<int>(i));
      }
      continue;
    }

    int found_index = -1;
    for (size_t i = 0; i < inner_specs.size(); i++) {
      const TupleCellSpec &spec = inner_specs[i];
      if (0 == strcasecmp(item.c_str(), spec.alias()) || 0 == strcasecmp(item.c_str(), spec.field_name())) {
        found_index = static_cast<int>(i);
        break;
      }
    }
    if (found_index < 0) {
      return RC::SCHEMA_FIELD_MISSING;
    }
    projection_indexes.push_back(found_index);
  }

  if (projection_indexes.size() != 1) {
    return RC::INVALID_ARGUMENT;
  }

  values.clear();
  for (const vector<Value> &row : inner_rows) {
    Value value = row[projection_indexes[0]];
    if (cast_type != AttrType::UNDEFINED && !value.is_null() && value.attr_type() != cast_type) {
      Value cast_value;
      rc = Value::cast_to(value, cast_type, cast_value);
      if (OB_FAIL(rc)) {
        return rc;
      }
      value = std::move(cast_value);
    }
    values.push_back(std::move(value));
  }
  return RC::SUCCESS;
}

RC SubqueryExpr::materialize(const Tuple *outer_tuple) const
{
  if (!correlated_ && materialized_) {
    return materialize_rc_;
  }

  if (!correlated_) {
    materialized_ = true;
  }
  ScopedSubqueryOuterTuple scoped_outer_tuple(correlated_ ? outer_tuple : nullptr);
  values_.clear();

  Session *session = Session::current_session();
  if (session == nullptr) {
    materialize_rc_ = RC::INTERNAL;
    return materialize_rc_;
  }

  Db *db = session->get_current_db();
  if (db == nullptr) {
    materialize_rc_ = RC::SCHEMA_DB_NOT_EXIST;
    return materialize_rc_;
  }

  ParsedSqlResult parsed_sql_result;
  RC rc = parse(sql_.c_str(), &parsed_sql_result);
  if (OB_FAIL(rc)) {
    materialize_rc_ = materialize_derived_select(sql_, values_, cast_type_);
    return materialize_rc_;
  }
  if (parsed_sql_result.sql_nodes().size() != 1 || parsed_sql_result.sql_nodes().front()->flag != SCF_SELECT) {
    materialize_rc_ = materialize_derived_select(sql_, values_, cast_type_);
    return materialize_rc_;
  }

  Stmt *raw_stmt = nullptr;
  BinderContext parent_context;
  BinderContext *parent_context_ptr = nullptr;
  if (!parent_tables_.empty()) {
    parent_context.set_db(db);
    for (const ParentTableRef &parent_table : parent_tables_) {
      parent_context.add_table(parent_table.table, parent_table.alias);
    }
    parent_context_ptr = &parent_context;
  }
  rc = Stmt::create_stmt(db, *parsed_sql_result.sql_nodes().front(), raw_stmt, parent_context_ptr);
  unique_ptr<Stmt> stmt(raw_stmt);
  if (OB_FAIL(rc)) {
    materialize_rc_ = materialize_derived_select(sql_, values_, cast_type_);
    return materialize_rc_;
  }

  LogicalPlanGenerator logical_plan_generator;
  unique_ptr<LogicalOperator> logical_operator;
  rc = logical_plan_generator.create(stmt.get(), logical_operator);
  if (OB_FAIL(rc)) {
    materialize_rc_ = rc;
    return materialize_rc_;
  }

  PhysicalPlanGenerator physical_plan_generator;
  unique_ptr<PhysicalOperator> physical_operator;
  rc = physical_plan_generator.create(*logical_operator, physical_operator, session);
  if (OB_FAIL(rc)) {
    materialize_rc_ = rc;
    return materialize_rc_;
  }

  Trx *trx = session->current_trx();
  trx->start_if_need();
  rc = physical_operator->open(trx);
  if (OB_FAIL(rc)) {
    materialize_rc_ = rc;
    return materialize_rc_;
  }

  while ((rc = physical_operator->next()) == RC::SUCCESS) {
    Tuple *tuple = physical_operator->current_tuple();
    if (tuple == nullptr || tuple->cell_num() != 1) {
      physical_operator->close();
      materialize_rc_ = RC::INVALID_ARGUMENT;
      return materialize_rc_;
    }

    Value value;
    rc = tuple->cell_at(0, value);
    if (OB_FAIL(rc)) {
      physical_operator->close();
      materialize_rc_ = rc;
      return materialize_rc_;
    }

    if (cast_type_ != AttrType::UNDEFINED && !value.is_null() && value.attr_type() != cast_type_) {
      Value cast_value;
      rc = Value::cast_to(value, cast_type_, cast_value);
      if (OB_FAIL(rc)) {
        physical_operator->close();
        materialize_rc_ = rc;
        return materialize_rc_;
      }
      value = std::move(cast_value);
    }

    values_.push_back(std::move(value));
  }

  RC close_rc = physical_operator->close();
  if (rc == RC::RECORD_EOF) {
    rc = close_rc;
  }

  materialize_rc_ = rc;
  return materialize_rc_;
}

RC SubqueryExpr::materialized_values(const vector<Value> *&values) const
{
  RC rc = materialize();
  if (OB_FAIL(rc)) {
    return rc;
  }

  values = &values_;
  return RC::SUCCESS;
}

RC SubqueryExpr::materialized_values(const Tuple &outer_tuple, const vector<Value> *&values) const
{
  RC rc = materialize(&outer_tuple);
  if (OB_FAIL(rc)) {
    return rc;
  }

  values = &values_;
  return RC::SUCCESS;
}

RC SubqueryExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = materialize(&tuple);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (values_.size() != 1) {
    LOG_WARN("scalar subquery should return one row. actual rows=%d", values_.size());
    return RC::INVALID_ARGUMENT;
  }

  value = values_[0];
  return RC::SUCCESS;
}

RC SubqueryExpr::prepare() const
{
  if (correlated_) {
    return RC::SUCCESS;
  }

  RC rc = materialize();
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (values_.size() != 1) {
    LOG_WARN("scalar subquery should return one row. actual rows=%d", values_.size());
    return RC::INVALID_ARGUMENT;
  }

  return RC::SUCCESS;
}

RC SubqueryExpr::try_get_value(Value &value) const
{
  if (correlated_) {
    return RC::UNIMPLEMENTED;
  }

  RC rc = prepare();
  if (OB_FAIL(rc)) {
    return rc;
  }

  value = values_[0];
  return RC::SUCCESS;
}

InSubqueryExpr::InSubqueryExpr(unique_ptr<Expression> left, unique_ptr<SubqueryExpr> subquery, bool not_in)
    : left_(std::move(left)), subquery_(std::move(subquery)), not_in_(not_in)
{}

RC InSubqueryExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value left_value;
  RC rc = left_->get_value(tuple, left_value);
  if (OB_FAIL(rc)) {
    return rc;
  }
  if (left_value.is_null()) {
    value.set_null();
    return RC::SUCCESS;
  }

  const vector<Value> *subquery_values = nullptr;
  rc = subquery_->materialized_values(tuple, subquery_values);
  if (OB_FAIL(rc)) {
    return rc;
  }

  bool has_null = false;
  bool matched  = false;
  for (const Value &subquery_value : *subquery_values) {
    if (subquery_value.is_null()) {
      has_null = true;
      continue;
    }

    if (left_value.compare(subquery_value) == 0) {
      matched = true;
      break;
    }
  }

  if (matched) {
    value.set_boolean(!not_in_);
  } else if (has_null) {
    value.set_null();
  } else {
    value.set_boolean(not_in_);
  }

  return RC::SUCCESS;
}

RC InSubqueryExpr::prepare() const
{
  if (subquery_->correlated()) {
    return RC::SUCCESS;
  }

  const vector<Value> *subquery_values = nullptr;
  return subquery_->materialized_values(subquery_values);
}

////////////////////////////////////////////////////////////////////////////////

IsNullExpr::IsNullExpr(unique_ptr<Expression> child, bool not_null)
    : child_(std::move(child)), not_null_(not_null)
{}

RC IsNullExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value child_value;
  RC rc = child_->get_value(tuple, child_value);
  if (OB_FAIL(rc)) {
    return rc;
  }

  value.set_boolean(not_null_ ? !child_value.is_null() : child_value.is_null());
  return RC::SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////

static RC compare_with_op(CompOp comp, const Value &left, const Value &right, bool &result)
{
  result = false;
  if (left.is_null() || right.is_null()) {
    return RC::SUCCESS;
  }

  int cmp_result = 0;
  if (left.attr_type() == AttrType::BOOLEANS && right.attr_type() == AttrType::BOOLEANS) {
    cmp_result = static_cast<int>(left.get_boolean()) - static_cast<int>(right.get_boolean());
  } else {
    cmp_result = left.compare(right);
  }

  switch (comp) {
    case EQUAL_TO: result = (0 == cmp_result); break;
    case LESS_EQUAL: result = (cmp_result <= 0); break;
    case NOT_EQUAL: result = (cmp_result != 0); break;
    case LESS_THAN: result = (cmp_result < 0); break;
    case GREAT_EQUAL: result = (cmp_result >= 0); break;
    case GREAT_THAN: result = (cmp_result > 0); break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp);
      return RC::INTERNAL;
    }
  }

  return RC::SUCCESS;
}

QuantifiedComparisonExpr::QuantifiedComparisonExpr(
    unique_ptr<Expression> left, CompOp comp, unique_ptr<SubqueryExpr> subquery, Quantifier quantifier)
    : left_(std::move(left)), comp_(comp), subquery_(std::move(subquery)), quantifier_(quantifier)
{}

RC QuantifiedComparisonExpr::prepare() const
{
  if (subquery_->correlated()) {
    return RC::SUCCESS;
  }

  const vector<Value> *subquery_values = nullptr;
  return subquery_->materialized_values(subquery_values);
}

RC QuantifiedComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value left_value;
  RC rc = left_->get_value(tuple, left_value);
  if (OB_FAIL(rc)) {
    return rc;
  }
  if (left_value.is_null()) {
    value.set_null();
    return RC::SUCCESS;
  }

  const vector<Value> *subquery_values = nullptr;
  rc = subquery_->materialized_values(tuple, subquery_values);
  if (OB_FAIL(rc)) {
    return rc;
  }

  bool has_null = false;
  bool result   = (quantifier_ == Quantifier::ALL);
  for (const Value &subquery_value : *subquery_values) {
    if (subquery_value.is_null()) {
      has_null = true;
      continue;
    }

    bool cmp_result = false;
    rc = compare_with_op(comp_, left_value, subquery_value, cmp_result);
    if (OB_FAIL(rc)) {
      return rc;
    }

    if (quantifier_ == Quantifier::ANY && cmp_result) {
      value.set_boolean(true);
      return RC::SUCCESS;
    }
    if (quantifier_ == Quantifier::ALL && !cmp_result) {
      value.set_boolean(false);
      return RC::SUCCESS;
    }
  }

  if (has_null && ((quantifier_ == Quantifier::ANY && !result) || (quantifier_ == Quantifier::ALL && result))) {
    value.set_null();
  } else {
    value.set_boolean(result);
  }
  return RC::SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

bool ArithmeticExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }
  auto &other_arith_expr = static_cast<const ArithmeticExpr &>(other);
  return arithmetic_type_ == other_arith_expr.arithmetic_type() && left_->equal(*other_arith_expr.left_) &&
         right_->equal(*other_arith_expr.right_);
}
AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if ((left_->value_type() == AttrType::INTS) &&
   (right_->value_type() == AttrType::INTS) &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  if (left_value.is_null() || (right_ && right_value.is_null())) {
    value.set_null();
    return rc;
  }

  const AttrType target_type = value_type();
  value.set_type(target_type);

  switch (arithmetic_type_) {
    case Type::ADD: {
      Value::add(left_value, right_value, value);
    } break;

    case Type::SUB: {
      Value::subtract(left_value, right_value, value);
    } break;

    case Type::MUL: {
      Value::multiply(left_value, right_value, value);
    } break;

    case Type::DIV: {
      Value::divide(left_value, right_value, value);
    } break;

    case Type::NEGATIVE: {
      Value::negative(left_value, value);
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
RC ArithmeticExpr::execute_calc(
    const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const
{
  RC rc = RC::SUCCESS;
  switch (type) {
    case Type::ADD: {
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, AddOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, AddOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
    } break;
    case Type::SUB:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, SubtractOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, SubtractOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::MUL:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, MultiplyOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, MultiplyOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::DIV:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, DivideOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, DivideOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::NEGATIVE:
      if (attr_type == AttrType::INTS) {
        unary_operator<LEFT_CONSTANT, int, NegateOperator>((int *)left.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        unary_operator<LEFT_CONSTANT, float, NegateOperator>(
            (float *)left.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    default: rc = RC::UNIMPLEMENTED; break;
  }
  if (rc == RC::SUCCESS) {
    result.set_count(result.capacity());
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  if (right_) {
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }
  rc = calc_value(left_value, right_value, value);
  return rc;
}

RC ArithmeticExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
    return rc;
  }
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of left expression. rc=%s", strrc(rc));
    return rc;
  }
  if (right_) {
    rc = right_->get_column(chunk, right_column);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get column of right expression. rc=%s", strrc(rc));
      return rc;
    }
  } else {
    right_column.reference(left_column);
  }
  return calc_column(left_column, right_column, column);
}

RC ArithmeticExpr::calc_column(const Column &left_column, const Column &right_column, Column &column) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  column.init(target_type, left_column.attr_len(), max(left_column.count(), right_column.count()));
  bool left_const  = left_column.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right_column.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    column.set_column_type(Column::Type::CONSTANT_COLUMN);
    rc = execute_calc<true, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (left_const && !right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<true, false>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (!left_const && right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, false>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  return rc;
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

////////////////////////////////////////////////////////////////////////////////

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, Expression *child)
    : aggregate_name_(aggregate_name), child_(child)
{}

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, unique_ptr<Expression> child)
    : aggregate_name_(aggregate_name), child_(std::move(child))
{}

UnboundFunctionExpr::UnboundFunctionExpr(const char *function_name, vector<unique_ptr<Expression>> arguments)
    : function_name_(function_name), arguments_(std::move(arguments))
{}

unique_ptr<Expression> UnboundFunctionExpr::copy() const
{
  vector<unique_ptr<Expression>> arguments;
  for (const unique_ptr<Expression> &argument : arguments_) {
    arguments.emplace_back(argument->copy());
  }
  return make_unique<UnboundFunctionExpr>(function_name_.c_str(), std::move(arguments));
}

////////////////////////////////////////////////////////////////////////////////
FunctionExpr::FunctionExpr(Type type, vector<unique_ptr<Expression>> arguments)
    : function_type_(type), arguments_(std::move(arguments))
{}

unique_ptr<Expression> FunctionExpr::copy() const
{
  vector<unique_ptr<Expression>> arguments;
  for (const unique_ptr<Expression> &argument : arguments_) {
    arguments.emplace_back(argument->copy());
  }
  auto expr = make_unique<FunctionExpr>(function_type_, std::move(arguments));
  expr->set_name(name());
  return expr;
}

AttrType FunctionExpr::value_type() const
{
  switch (function_type_) {
    case Type::LENGTH: return AttrType::INTS;
    case Type::ROUND: return arguments_.size() == 1 ? AttrType::INTS : AttrType::FLOATS;
    case Type::DATE_FORMAT: return AttrType::CHARS;
  }
  return AttrType::UNDEFINED;
}

int FunctionExpr::value_length() const
{
  switch (value_type()) {
    case AttrType::INTS: return sizeof(int);
    case AttrType::FLOATS: return sizeof(float);
    case AttrType::CHARS: return 64;
    default: return -1;
  }
}

static string ordinal_day(int day)
{
  const char *suffix = "TH";
  if (day % 100 < 11 || day % 100 > 13) {
    switch (day % 10) {
      case 1: suffix = "ST"; break;
      case 2: suffix = "ND"; break;
      case 3: suffix = "RD"; break;
      default: break;
    }
  }

  char buffer[16];
  snprintf(buffer, sizeof(buffer), "%d%s", day, suffix);
  return string(buffer);
}

static string format_date_value(int date, const string &format)
{
  static const char *MONTH_NAMES[] = {"", "January", "February", "March", "April", "May", "June",
      "July", "August", "September", "October", "November", "December"};

  int year  = date / 10000;
  int month = (date / 100) % 100;
  int day   = date % 100;

  string result;
  char   buffer[32];
  for (size_t i = 0; i < format.size(); i++) {
    if (format[i] != '%' || i + 1 >= format.size()) {
      result.push_back(format[i]);
      continue;
    }

    char spec = format[++i];
    switch (spec) {
      case 'Y':
        snprintf(buffer, sizeof(buffer), "%04d", year);
        result.append(buffer);
        break;
      case 'y':
        snprintf(buffer, sizeof(buffer), "%02d", year % 100);
        result.append(buffer);
        break;
      case 'm':
        snprintf(buffer, sizeof(buffer), "%02d", month);
        result.append(buffer);
        break;
      case 'd':
        snprintf(buffer, sizeof(buffer), "%02d", day);
        result.append(buffer);
        break;
      case 'D': result.append(ordinal_day(day)); break;
      case 'M':
        if (month >= 1 && month <= 12) {
          result.append(MONTH_NAMES[month]);
        }
        break;
      default: result.push_back(spec); break;
    }
  }
  return result;
}

RC FunctionExpr::eval_arguments(const vector<Value> &arguments, Value &value) const
{
  for (const Value &argument : arguments) {
    if (argument.is_null()) {
      value.set_null();
      return RC::SUCCESS;
    }
  }

  switch (function_type_) {
    case Type::LENGTH: {
      value.set_int(static_cast<int>(arguments[0].get_string_t().size()));
    } break;
    case Type::ROUND: {
      float input = arguments[0].get_float();
      if (arguments.size() == 1) {
        value.set_int(static_cast<int>(std::nearbyint(input)));
      } else {
        int   precision = arguments[1].get_int();
        float scale     = std::pow(10.0f, static_cast<float>(precision));
        value.set_float(std::nearbyint(input * scale) / scale);
      }
    } break;
    case Type::DATE_FORMAT: {
      Value date_value;
      if (arguments[0].attr_type() == AttrType::DATES) {
        date_value = arguments[0];
      } else {
        RC rc = Value::cast_to(arguments[0], AttrType::DATES, date_value);
        if (OB_FAIL(rc)) {
          value.set_null();
          return RC::SUCCESS;
        }
      }
      value.set_string(format_date_value(date_value.get_date(), arguments[1].get_string()).c_str());
    } break;
  }

  return RC::SUCCESS;
}

RC FunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  vector<Value> arguments;
  arguments.reserve(arguments_.size());
  for (const unique_ptr<Expression> &argument_expr : arguments_) {
    Value argument;
    RC rc = argument_expr->get_value(tuple, argument);
    if (OB_FAIL(rc)) {
      return rc;
    }
    arguments.emplace_back(std::move(argument));
  }
  return eval_arguments(arguments, value);
}

RC FunctionExpr::try_get_value(Value &value) const
{
  vector<Value> arguments;
  arguments.reserve(arguments_.size());
  for (const unique_ptr<Expression> &argument_expr : arguments_) {
    Value argument;
    RC rc = argument_expr->try_get_value(argument);
    if (OB_FAIL(rc)) {
      return rc;
    }
    arguments.emplace_back(std::move(argument));
  }
  return eval_arguments(arguments, value);
}

RC FunctionExpr::type_from_string(const char *type_str, FunctionExpr::Type &type)
{
  if (0 == strcasecmp(type_str, "length")) {
    type = Type::LENGTH;
    return RC::SUCCESS;
  }
  if (0 == strcasecmp(type_str, "round")) {
    type = Type::ROUND;
    return RC::SUCCESS;
  }
  if (0 == strcasecmp(type_str, "date_format")) {
    type = Type::DATE_FORMAT;
    return RC::SUCCESS;
  }
  return RC::INVALID_ARGUMENT;
}

////////////////////////////////////////////////////////////////////////////////
AggregateExpr::AggregateExpr(Type type, Expression *child) : aggregate_type_(type), child_(child) {}

AggregateExpr::AggregateExpr(Type type, unique_ptr<Expression> child) : aggregate_type_(type), child_(std::move(child))
{}

RC AggregateExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    rc = RC::INTERNAL;
  }
  return rc;
}

bool AggregateExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != type()) {
    return false;
  }
  const AggregateExpr &other_aggr_expr = static_cast<const AggregateExpr &>(other);
  return aggregate_type_ == other_aggr_expr.aggregate_type() && child_->equal(*other_aggr_expr.child());
}

unique_ptr<Aggregator> AggregateExpr::create_aggregator() const
{
  unique_ptr<Aggregator> aggregator;
  switch (aggregate_type_) {
    case Type::COUNT: {
      aggregator = make_unique<CountAggregator>();
      break;
    }
    case Type::SUM: {
      aggregator = make_unique<SumAggregator>();
      break;
    }
    case Type::AVG: {
      aggregator = make_unique<AvgAggregator>(child_->value_type());
      break;
    }
    case Type::MIN: {
      aggregator = make_unique<MinAggregator>();
      break;
    }
    case Type::MAX: {
      aggregator = make_unique<MaxAggregator>();
      break;
    }
    default: {
      ASSERT(false, "unsupported aggregate type");
      break;
    }
  }
  return aggregator;
}

RC AggregateExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(name()), value);
}

RC AggregateExpr::type_from_string(const char *type_str, AggregateExpr::Type &type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "count")) {
    type = Type::COUNT;
  } else if (0 == strcasecmp(type_str, "sum")) {
    type = Type::SUM;
  } else if (0 == strcasecmp(type_str, "avg")) {
    type = Type::AVG;
  } else if (0 == strcasecmp(type_str, "max")) {
    type = Type::MAX;
  } else if (0 == strcasecmp(type_str, "min")) {
    type = Type::MIN;
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}
