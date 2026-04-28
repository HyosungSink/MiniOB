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

#pragma once

#include "sql/expr/expression.h"

class Db;

class BinderContext
{
public:
  BinderContext()          = default;
  virtual ~BinderContext() = default;

  void set_db(Db *db) { db_ = db; }
  Db  *db() const { return db_; }
  void set_parent(const BinderContext *parent) { parent_ = parent; }

  void add_table(Table *table) { add_table(table, ""); }
  void add_table(Table *table, const string &alias);

  Table *find_table(const char *table_name) const;
  RC find_table_by_field(const char *field_name, Table *&table) const;
  bool has_outer_reference() const { return has_outer_reference_; }
  void collect_table_refs(vector<SubqueryExpr::ParentTableRef> &table_refs) const;

  const vector<Table *> &query_tables() const { return query_tables_; }

private:
  Db *db_ = nullptr;
  const BinderContext *parent_ = nullptr;
  mutable bool has_outer_reference_ = false;

  struct TableAlias
  {
    string alias;
    Table *table = nullptr;
  };

  vector<Table *> query_tables_;
  vector<TableAlias> table_aliases_;
  vector<Table *> aliased_tables_;
};

/**
 * @brief 绑定表达式
 * @details 绑定表达式，就是在SQL解析后，得到文本描述的表达式，将表达式解析为具体的数据库对象
 */
class ExpressionBinder
{
public:
  ExpressionBinder(BinderContext &context) : context_(context) {}
  virtual ~ExpressionBinder() = default;

  RC bind_expression(unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &bound_expressions);

private:
  RC bind_star_expression(unique_ptr<Expression> &star_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_unbound_field_expression(
      unique_ptr<Expression> &unbound_field_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_field_expression(unique_ptr<Expression> &field_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_value_expression(unique_ptr<Expression> &value_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_cast_expression(unique_ptr<Expression> &cast_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_comparison_expression(
      unique_ptr<Expression> &comparison_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_conjunction_expression(
      unique_ptr<Expression> &conjunction_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_in_expression(unique_ptr<Expression> &in_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_subquery_expression(
      unique_ptr<Expression> &subquery_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_in_subquery_expression(
      unique_ptr<Expression> &in_subquery_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_is_null_expression(
      unique_ptr<Expression> &is_null_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_quantified_comparison_expression(
      unique_ptr<Expression> &comp_subquery_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_arithmetic_expression(
      unique_ptr<Expression> &arithmetic_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_function_expression(
      unique_ptr<Expression> &function_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_scalar_function(const char *function_name,
      vector<unique_ptr<Expression>> &arguments,
      const char *expression_name,
      vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_aggregate_expression(
      unique_ptr<Expression> &aggregate_expr, vector<unique_ptr<Expression>> &bound_expressions);
  RC bind_aggregate_function(const char *aggregate_name,
      vector<unique_ptr<Expression>> &arguments,
      const char *expression_name,
      vector<unique_ptr<Expression>> &bound_expressions);

private:
  BinderContext &context_;
};
