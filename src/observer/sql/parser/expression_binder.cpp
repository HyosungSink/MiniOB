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
// Created by Wangyunlai on 2024/05/29.
//

#include "common/log/log.h"
#include "common/lang/string.h"
#include "common/lang/ranges.h"
#include "sql/parser/expression_binder.h"
#include "sql/parser/parse.h"
#include "sql/expr/expression_iterator.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/stmt.h"
#include "storage/db/db.h"

#include <cctype>
#include <cstring>

using namespace common;

void BinderContext::add_table(Table *table, const string &alias)
{
  auto table_iter = ranges::find(query_tables_, table);
  if (table_iter == query_tables_.end()) {
    query_tables_.push_back(table);
  }
  if (!is_blank(alias.c_str())) {
    table_aliases_.push_back({alias, table});
    aliased_tables_.push_back(table);
  }
}

Table *BinderContext::find_table(const char *table_name) const
{
  auto alias_pred = [table_name](const TableAlias &table_alias) {
    return 0 == strcasecmp(table_name, table_alias.alias.c_str());
  };
  auto alias_iter = ranges::find_if(table_aliases_, alias_pred);
  if (alias_iter != table_aliases_.end()) {
    return alias_iter->table;
  }

  auto pred = [this, table_name](Table *table) {
    if (ranges::find(aliased_tables_, table) != aliased_tables_.end()) {
      return false;
    }
    return 0 == strcasecmp(table_name, table->name());
  };
  auto iter = ranges::find_if(query_tables_, pred);
  if (iter != query_tables_.end()) {
    return *iter;
  }

  if (parent_ != nullptr) {
    Table *parent_table = parent_->find_table(table_name);
    if (parent_table != nullptr) {
      has_outer_reference_ = true;
    }
    return parent_table;
  }
  return nullptr;
}

RC BinderContext::find_table_by_field(const char *field_name, Table *&table) const
{
  table = nullptr;
  for (Table *candidate_table : query_tables_) {
    const FieldMeta *field_meta = candidate_table->table_meta().field(field_name);
    if (field_meta == nullptr) {
      continue;
    }

    if (table != nullptr) {
      LOG_INFO("ambiguous field: %s", field_name);
      return RC::SCHEMA_FIELD_MISSING;
    }
    table = candidate_table;
  }

  if (table != nullptr) {
    return RC::SUCCESS;
  }

  if (parent_ != nullptr) {
    RC rc = parent_->find_table_by_field(field_name, table);
    if (OB_SUCC(rc) && table != nullptr) {
      has_outer_reference_ = true;
    }
    return rc;
  }

  LOG_INFO("no such field in table list: %s", field_name);
  return RC::SCHEMA_FIELD_MISSING;
}

void BinderContext::collect_table_refs(vector<SubqueryExpr::ParentTableRef> &table_refs) const
{
  for (const TableAlias &table_alias : table_aliases_) {
    table_refs.push_back({table_alias.table, table_alias.alias});
  }
  for (Table *table : query_tables_) {
    if (ranges::find(aliased_tables_, table) == aliased_tables_.end()) {
      table_refs.push_back({table, ""});
    }
  }
  if (parent_ != nullptr) {
    parent_->collect_table_refs(table_refs);
  }
}

////////////////////////////////////////////////////////////////////////////////
static void wildcard_fields(Table *table, vector<unique_ptr<Expression>> &expressions)
{
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    Field      field(table, table_meta.field(i));
    FieldExpr *field_expr = new FieldExpr(field);
    field_expr->set_name(field.field_name());
    expressions.emplace_back(field_expr);
  }
}

RC ExpressionBinder::bind_expression(unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  switch (expr->type()) {
    case ExprType::STAR: {
      return bind_star_expression(expr, bound_expressions);
    } break;

    case ExprType::UNBOUND_FIELD: {
      return bind_unbound_field_expression(expr, bound_expressions);
    } break;

    case ExprType::UNBOUND_AGGREGATION: {
      return bind_aggregate_expression(expr, bound_expressions);
    } break;

    case ExprType::UNBOUND_FUNCTION: {
      return bind_function_expression(expr, bound_expressions);
    } break;

    case ExprType::FIELD: {
      return bind_field_expression(expr, bound_expressions);
    } break;

    case ExprType::VALUE: {
      return bind_value_expression(expr, bound_expressions);
    } break;

    case ExprType::CAST: {
      return bind_cast_expression(expr, bound_expressions);
    } break;

    case ExprType::COMPARISON: {
      return bind_comparison_expression(expr, bound_expressions);
    } break;

    case ExprType::CONJUNCTION: {
      return bind_conjunction_expression(expr, bound_expressions);
    } break;

    case ExprType::IN_LIST: {
      return bind_in_expression(expr, bound_expressions);
    } break;

    case ExprType::SUBQUERY: {
      return bind_subquery_expression(expr, bound_expressions);
    } break;

    case ExprType::IN_SUBQUERY: {
      return bind_in_subquery_expression(expr, bound_expressions);
    } break;

    case ExprType::IS_NULL: {
      return bind_is_null_expression(expr, bound_expressions);
    } break;

    case ExprType::COMP_SUBQUERY: {
      return bind_quantified_comparison_expression(expr, bound_expressions);
    } break;

    case ExprType::ARITHMETIC: {
      return bind_arithmetic_expression(expr, bound_expressions);
    } break;

    case ExprType::AGGREGATION: {
      ASSERT(false, "shouldn't be here");
    } break;

    default: {
      LOG_WARN("unknown expression type: %d", static_cast<int>(expr->type()));
      return RC::INTERNAL;
    }
  }
  return RC::INTERNAL;
}

RC ExpressionBinder::bind_star_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto star_expr = static_cast<StarExpr *>(expr.get());

  vector<Table *> tables_to_wildcard;

  const char *table_name = star_expr->table_name();
  if (!is_blank(table_name) && 0 != strcmp(table_name, "*")) {
    Table *table = context_.find_table(table_name);
    if (nullptr == table) {
      LOG_INFO("no such table in from list: %s", table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    tables_to_wildcard.push_back(table);
  } else {
    const vector<Table *> &all_tables = context_.query_tables();
    tables_to_wildcard.insert(tables_to_wildcard.end(), all_tables.begin(), all_tables.end());
  }

  for (Table *table : tables_to_wildcard) {
    wildcard_fields(table, bound_expressions);
  }

  return RC::SUCCESS;
}

RC ExpressionBinder::bind_unbound_field_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto unbound_field_expr = static_cast<UnboundFieldExpr *>(expr.get());

  const char *table_name = unbound_field_expr->table_name();
  const char *field_name = unbound_field_expr->field_name();

  Table *table = nullptr;
  if (is_blank(table_name)) {
    RC rc = context_.find_table_by_field(field_name, table);
    if (OB_FAIL(rc)) {
      return rc;
    }
  } else {
    table = context_.find_table(table_name);
    if (nullptr == table) {
      LOG_INFO("no such table in from list: %s", table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
  }

  if (0 == strcmp(field_name, "*")) {
    wildcard_fields(table, bound_expressions);
  } else {
    const FieldMeta *field_meta = table->table_meta().field(field_name);
    if (nullptr == field_meta) {
      LOG_INFO("no such field in table: %s.%s", table_name, field_name);
      return RC::SCHEMA_FIELD_MISSING;
    }

    Field      field(table, field_meta);
    FieldExpr *field_expr = new FieldExpr(field);
    const char *expression_name = unbound_field_expr->name();
    if (!is_blank(expression_name) && strchr(expression_name, '.') == nullptr &&
        0 != strcasecmp(expression_name, field_name)) {
      field_expr->set_name(expression_name);
    } else {
      field_expr->set_name(field_name);
    }
    bound_expressions.emplace_back(field_expr);
  }

  return RC::SUCCESS;
}

RC ExpressionBinder::bind_field_expression(
    unique_ptr<Expression> &field_expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  bound_expressions.emplace_back(std::move(field_expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_value_expression(
    unique_ptr<Expression> &value_expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  bound_expressions.emplace_back(std::move(value_expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_cast_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto cast_expr = static_cast<CastExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &child_expr = cast_expr->child();

  RC rc = bind_expression(child_expr, child_bound_expressions);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid children number of cast expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &child = child_bound_expressions[0];
  if (child.get() == child_expr.get()) {
    return RC::SUCCESS;
  }

  child_expr.reset(child.release());
  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_comparison_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto comparison_expr = static_cast<ComparisonExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &left_expr  = comparison_expr->left();
  unique_ptr<Expression>        &right_expr = comparison_expr->right();

  RC rc = bind_expression(left_expr, child_bound_expressions);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());
  }

  child_bound_expressions.clear();
  rc = bind_expression(right_expr, child_bound_expressions);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid right children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &right = child_bound_expressions[0];
  if (right.get() != right_expr.get()) {
    right_expr.reset(right.release());
  }

  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_conjunction_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());

  vector<unique_ptr<Expression>>  child_bound_expressions;
  vector<unique_ptr<Expression>> &children = conjunction_expr->children();

  for (unique_ptr<Expression> &child_expr : children) {
    child_bound_expressions.clear();

    RC rc = bind_expression(child_expr, child_bound_expressions);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    if (child_bound_expressions.size() != 1) {
      LOG_WARN("invalid children number of conjunction expression: %d", child_bound_expressions.size());
      return RC::INVALID_ARGUMENT;
    }

    unique_ptr<Expression> &child = child_bound_expressions[0];
    if (child.get() != child_expr.get()) {
      child_expr.reset(child.release());
    }
  }

  bound_expressions.emplace_back(std::move(expr));

  return RC::SUCCESS;
}

RC ExpressionBinder::bind_in_expression(unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto in_expr = static_cast<InExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &left_expr = in_expr->left();
  RC rc = bind_expression(left_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }
  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of IN expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());
  }

  vector<unique_ptr<Expression>> &values = in_expr->values();
  for (unique_ptr<Expression> &value_expr : values) {
    child_bound_expressions.clear();
    rc = bind_expression(value_expr, child_bound_expressions);
    if (OB_FAIL(rc)) {
      return rc;
    }
    if (child_bound_expressions.size() != 1) {
      LOG_WARN("invalid value children number of IN expression: %d", child_bound_expressions.size());
      return RC::INVALID_ARGUMENT;
    }

    unique_ptr<Expression> &value = child_bound_expressions[0];
    if (value.get() != value_expr.get()) {
      value_expr.reset(value.release());
    }

    if (value_expr->value_type() == AttrType::UNDEFINED || left_expr->value_type() == AttrType::UNDEFINED ||
        value_expr->value_type() == left_expr->value_type()) {
      continue;
    }

    int cast_cost = DataType::type_instance(value_expr->value_type())->cast_cost(left_expr->value_type());
    if (cast_cost == INT32_MAX) {
      LOG_WARN("unsupported IN list cast from %s to %s",
          attr_type_to_string(value_expr->value_type()),
          attr_type_to_string(left_expr->value_type()));
      return RC::UNSUPPORTED;
    }

    auto cast_expr = make_unique<CastExpr>(std::move(value_expr), left_expr->value_type());
    if (cast_expr->child()->type() == ExprType::VALUE) {
      Value cast_value;
      rc = cast_expr->try_get_value(cast_value);
      if (OB_FAIL(rc)) {
        return rc;
      }
      value_expr = make_unique<ValueExpr>(cast_value);
    } else {
      value_expr = std::move(cast_expr);
    }
  }

  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

static RC infer_subquery_value_info(Db *db, SubqueryExpr &subquery_expr, const BinderContext *parent_context)
{
  if (db == nullptr) {
    return RC::SCHEMA_DB_NOT_EXIST;
  }
  if (parent_context != nullptr) {
    vector<SubqueryExpr::ParentTableRef> parent_tables;
    parent_context->collect_table_refs(parent_tables);
    subquery_expr.set_parent_tables(parent_tables);
  }

  auto maybe_derived_select = [](const string &sql) -> bool {
    auto word_equals_at = [&sql](size_t pos, const char *word) -> bool {
      size_t len = strlen(word);
      if (pos + len > sql.size()) {
        return false;
      }
      for (size_t i = 0; i < len; i++) {
        if (toupper(static_cast<unsigned char>(sql[pos + i])) !=
            toupper(static_cast<unsigned char>(word[i]))) {
          return false;
        }
      }
      bool left_ok = pos == 0 ||
                     (!isalnum(static_cast<unsigned char>(sql[pos - 1])) && sql[pos - 1] != '_');
      bool right_ok = pos + len == sql.size() ||
                      (!isalnum(static_cast<unsigned char>(sql[pos + len])) && sql[pos + len] != '_');
      return left_ok && right_ok;
    };

    bool has_select = false;
    for (size_t i = 0; i < sql.size(); i++) {
      if (word_equals_at(i, "SELECT")) {
        has_select = true;
      }
      if (word_equals_at(i, "FROM")) {
        size_t next = i + strlen("FROM");
        while (next < sql.size() && isspace(static_cast<unsigned char>(sql[next]))) {
          next++;
        }
        if (has_select && next < sql.size() && sql[next] == '(') {
          return true;
        }
      }
    }
    return false;
  };

  ParsedSqlResult parsed_sql_result;
  RC rc = parse(subquery_expr.sql().c_str(), &parsed_sql_result);
  if (OB_FAIL(rc)) {
    if (maybe_derived_select(subquery_expr.sql())) {
      subquery_expr.set_value_info(AttrType::UNDEFINED, -1);
      return RC::SUCCESS;
    }
    return rc;
  }
  if (parsed_sql_result.sql_nodes().size() != 1 || parsed_sql_result.sql_nodes().front()->flag != SCF_SELECT) {
    if (maybe_derived_select(subquery_expr.sql())) {
      subquery_expr.set_value_info(AttrType::UNDEFINED, -1);
      return RC::SUCCESS;
    }
    return RC::SQL_SYNTAX;
  }

  Stmt *raw_stmt = nullptr;
  rc = Stmt::create_stmt(db, *parsed_sql_result.sql_nodes().front(), raw_stmt, parent_context);
  unique_ptr<Stmt> stmt(raw_stmt);
  if (OB_FAIL(rc)) {
    if (maybe_derived_select(subquery_expr.sql())) {
      subquery_expr.set_value_info(AttrType::UNDEFINED, -1);
      return RC::SUCCESS;
    }
    return rc;
  }
  if (stmt->type() != StmtType::SELECT) {
    return RC::INVALID_ARGUMENT;
  }

  auto select_stmt = static_cast<SelectStmt *>(stmt.get());
  vector<unique_ptr<Expression>> &expressions = select_stmt->query_expressions();
  if (expressions.size() != 1) {
    return RC::INVALID_ARGUMENT;
  }

  subquery_expr.set_value_info(expressions[0]->value_type(), expressions[0]->value_length());
  subquery_expr.set_correlated(select_stmt->has_outer_reference());
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_subquery_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto subquery_expr = static_cast<SubqueryExpr *>(expr.get());
  RC rc = infer_subquery_value_info(context_.db(), *subquery_expr, &context_);
  if (OB_FAIL(rc)) {
    return rc;
  }

  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_in_subquery_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto in_subquery_expr = static_cast<InSubqueryExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &left_expr = in_subquery_expr->left();
  RC rc = bind_expression(left_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }
  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of IN subquery expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());
  }

  SubqueryExpr &subquery = in_subquery_expr->subquery();
  rc = infer_subquery_value_info(context_.db(), subquery, &context_);
  if (OB_FAIL(rc)) {
    return rc;
  }

  AttrType left_type     = left_expr->value_type();
  AttrType subquery_type = subquery.value_type();
  if (left_type != AttrType::UNDEFINED && subquery_type != AttrType::UNDEFINED && left_type != subquery_type &&
      !(is_numerical_type(left_type) && is_numerical_type(subquery_type))) {
    int cast_cost = DataType::type_instance(subquery_type)->cast_cost(left_type);
    if (cast_cost == INT32_MAX) {
      LOG_WARN("unsupported IN subquery cast from %s to %s",
          attr_type_to_string(subquery_type),
          attr_type_to_string(left_type));
      return RC::UNSUPPORTED;
    }
    subquery.set_cast_type(left_type);
  }

  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_is_null_expression(unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto is_null_expr = static_cast<IsNullExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &child_expr = is_null_expr->child();
  RC rc = bind_expression(child_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }
  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid child number of IS NULL expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &child = child_bound_expressions[0];
  if (child.get() != child_expr.get()) {
    child_expr.reset(child.release());
  }

  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_quantified_comparison_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto comp_subquery_expr = static_cast<QuantifiedComparisonExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &left_expr = comp_subquery_expr->left();
  RC rc = bind_expression(left_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }
  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of quantified subquery expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());
  }

  SubqueryExpr &subquery = comp_subquery_expr->subquery();
  rc = infer_subquery_value_info(context_.db(), subquery, &context_);
  if (OB_FAIL(rc)) {
    return rc;
  }

  AttrType left_type     = left_expr->value_type();
  AttrType subquery_type = subquery.value_type();
  if (left_type != AttrType::UNDEFINED && subquery_type != AttrType::UNDEFINED && left_type != subquery_type &&
      !(is_numerical_type(left_type) && is_numerical_type(subquery_type))) {
    int cast_cost = DataType::type_instance(subquery_type)->cast_cost(left_type);
    if (cast_cost == INT32_MAX) {
      LOG_WARN("unsupported quantified subquery cast from %s to %s",
          attr_type_to_string(subquery_type),
          attr_type_to_string(left_type));
      return RC::UNSUPPORTED;
    }
    subquery.set_cast_type(left_type);
  }

  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC ExpressionBinder::bind_arithmetic_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto arithmetic_expr = static_cast<ArithmeticExpr *>(expr.get());

  vector<unique_ptr<Expression>> child_bound_expressions;
  unique_ptr<Expression>        &left_expr  = arithmetic_expr->left();
  unique_ptr<Expression>        &right_expr = arithmetic_expr->right();

  RC rc = bind_expression(left_expr, child_bound_expressions);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (child_bound_expressions.size() != 1) {
    LOG_WARN("invalid left children number of comparison expression: %d", child_bound_expressions.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression> &left = child_bound_expressions[0];
  if (left.get() != left_expr.get()) {
    left_expr.reset(left.release());
  }

  if (right_expr != nullptr) {
    child_bound_expressions.clear();
    rc = bind_expression(right_expr, child_bound_expressions);
    if (OB_FAIL(rc)) {
      return rc;
    }

    if (child_bound_expressions.size() != 1) {
      LOG_WARN("invalid right children number of comparison expression: %d", child_bound_expressions.size());
      return RC::INVALID_ARGUMENT;
    }

    unique_ptr<Expression> &right = child_bound_expressions[0];
    if (right.get() != right_expr.get()) {
      right_expr.reset(right.release());
    }
  }

  bound_expressions.emplace_back(std::move(expr));
  return RC::SUCCESS;
}

RC check_aggregate_expression(AggregateExpr &expression)
{
  // 必须有一个子表达式
  Expression *child_expression = expression.child().get();
  if (nullptr == child_expression) {
    LOG_WARN("child expression of aggregate expression is null");
    return RC::INVALID_ARGUMENT;
  }

  // 校验数据类型与聚合类型是否匹配
  AggregateExpr::Type aggregate_type   = expression.aggregate_type();
  AttrType            child_value_type = child_expression->value_type();
  switch (aggregate_type) {
    case AggregateExpr::Type::SUM:
    case AggregateExpr::Type::AVG: {
      // 仅支持数值类型
      if (!is_numerical_type(child_value_type)) {
        LOG_WARN("invalid child value type for aggregate expression: %d", static_cast<int>(child_value_type));
        return RC::INVALID_ARGUMENT;
      }
    } break;

    case AggregateExpr::Type::COUNT:
    case AggregateExpr::Type::MAX:
    case AggregateExpr::Type::MIN: {
      // 任何类型都支持
    } break;
  }

  // 子表达式中不能再包含聚合表达式
  function<RC(unique_ptr<Expression>&)> check_aggregate_expr = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {
      LOG_WARN("aggregate expression cannot be nested");
      return RC::INVALID_ARGUMENT;
    }
    rc = ExpressionIterator::iterate_child_expr(*expr, check_aggregate_expr);
    return rc;
  };

  RC rc = ExpressionIterator::iterate_child_expr(expression, check_aggregate_expr);

  return rc;
}

RC ExpressionBinder::bind_aggregate_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto unbound_aggregate_expr = static_cast<UnboundAggregateExpr *>(expr.get());
  const char *aggregate_name = unbound_aggregate_expr->aggregate_name();
  vector<unique_ptr<Expression>> arguments;
  arguments.emplace_back(std::move(unbound_aggregate_expr->child()));
  return bind_aggregate_function(aggregate_name, arguments, unbound_aggregate_expr->name(), bound_expressions);
}

RC ExpressionBinder::bind_function_expression(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions)
{
  if (nullptr == expr) {
    return RC::SUCCESS;
  }

  auto unbound_function_expr = static_cast<UnboundFunctionExpr *>(expr.get());
  const char *function_name = unbound_function_expr->function_name();

  AggregateExpr::Type aggregate_type;
  RC rc = AggregateExpr::type_from_string(function_name, aggregate_type);
  if (OB_SUCC(rc)) {
    return bind_aggregate_function(
        function_name, unbound_function_expr->arguments(), unbound_function_expr->name(), bound_expressions);
  }

  return bind_scalar_function(
      function_name, unbound_function_expr->arguments(), unbound_function_expr->name(), bound_expressions);
}

RC ExpressionBinder::bind_aggregate_function(const char *aggregate_name,
    vector<unique_ptr<Expression>> &arguments,
    const char *expression_name,
    vector<unique_ptr<Expression>> &bound_expressions)
{
  AggregateExpr::Type aggregate_type;
  RC rc = AggregateExpr::type_from_string(aggregate_name, aggregate_type);
  if (OB_FAIL(rc)) {
    LOG_WARN("invalid aggregate name: %s", aggregate_name);
    return rc;
  }

  if (arguments.size() != 1) {
    LOG_WARN("invalid arguments number of aggregate expression: %d", arguments.size());
    return RC::INVALID_ARGUMENT;
  }

  unique_ptr<Expression>        &child_expr = arguments[0];
  vector<unique_ptr<Expression>> child_bound_expressions;

  if (child_expr->type() == ExprType::STAR && aggregate_type == AggregateExpr::Type::COUNT) {
    ValueExpr *value_expr = new ValueExpr(Value(1));
    child_expr.reset(value_expr);
  } else {
    rc = bind_expression(child_expr, child_bound_expressions);
    if (OB_FAIL(rc)) {
      return rc;
    }

    if (child_bound_expressions.size() != 1) {
      LOG_WARN("invalid children number of aggregate expression: %d", child_bound_expressions.size());
      return RC::INVALID_ARGUMENT;
    }

    if (child_bound_expressions[0].get() != child_expr.get()) {
      child_expr.reset(child_bound_expressions[0].release());
    }
  }

  auto aggregate_expr = make_unique<AggregateExpr>(aggregate_type, std::move(child_expr));
  aggregate_expr->set_name(expression_name);
  rc = check_aggregate_expression(*aggregate_expr);
  if (OB_FAIL(rc)) {
    return rc;
  }

  bound_expressions.emplace_back(std::move(aggregate_expr));
  return RC::SUCCESS;
}

static bool is_null_literal_type(AttrType type) { return type == AttrType::UNDEFINED; }

RC ExpressionBinder::bind_scalar_function(const char *function_name,
    vector<unique_ptr<Expression>> &arguments,
    const char *expression_name,
    vector<unique_ptr<Expression>> &bound_expressions)
{
  FunctionExpr::Type function_type;
  RC rc = FunctionExpr::type_from_string(function_name, function_type);
  if (OB_FAIL(rc)) {
    LOG_WARN("invalid function name: %s", function_name);
    return rc;
  }

  vector<unique_ptr<Expression>> bound_arguments;
  for (unique_ptr<Expression> &argument_expr : arguments) {
    vector<unique_ptr<Expression>> child_bound_expressions;
    rc = bind_expression(argument_expr, child_bound_expressions);
    if (OB_FAIL(rc)) {
      return rc;
    }

    if (child_bound_expressions.size() != 1) {
      LOG_WARN("invalid argument children number of function expression: %d", child_bound_expressions.size());
      return RC::INVALID_ARGUMENT;
    }

    bound_arguments.emplace_back(std::move(child_bound_expressions[0]));
  }

  auto type_at = [&](size_t index) { return bound_arguments[index]->value_type(); };

  switch (function_type) {
    case FunctionExpr::Type::LENGTH: {
      if (bound_arguments.size() != 1) {
        return RC::INVALID_ARGUMENT;
      }
      AttrType type = type_at(0);
      if (type != AttrType::CHARS && !is_null_literal_type(type)) {
        return RC::INVALID_ARGUMENT;
      }
    } break;
    case FunctionExpr::Type::ROUND: {
      if (bound_arguments.size() != 1 && bound_arguments.size() != 2) {
        return RC::INVALID_ARGUMENT;
      }
      AttrType value_type = type_at(0);
      if (!is_numerical_type(value_type) && !is_null_literal_type(value_type)) {
        return RC::INVALID_ARGUMENT;
      }
      if (bound_arguments.size() == 2) {
        AttrType precision_type = type_at(1);
        if (precision_type != AttrType::INTS && !is_null_literal_type(precision_type)) {
          return RC::INVALID_ARGUMENT;
        }
      }
    } break;
    case FunctionExpr::Type::DATE_FORMAT: {
      if (bound_arguments.size() != 2) {
        return RC::INVALID_ARGUMENT;
      }
      AttrType date_type = type_at(0);
      AttrType format_type = type_at(1);
      if (date_type != AttrType::DATES && date_type != AttrType::CHARS && !is_null_literal_type(date_type)) {
        return RC::INVALID_ARGUMENT;
      }
      if (format_type != AttrType::CHARS && !is_null_literal_type(format_type)) {
        return RC::INVALID_ARGUMENT;
      }
    } break;
  }

  auto function_expr = make_unique<FunctionExpr>(function_type, std::move(bound_arguments));
  function_expr->set_name(expression_name);
  bound_expressions.emplace_back(std::move(function_expr));
  return RC::SUCCESS;
}
