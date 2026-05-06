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
// Created by Wangyunlai on 2022/5/22.
//

#pragma once

#include "common/sys/rc.h"
#include "sql/stmt/stmt.h"

class Table;
class FieldMeta;
class FilterStmt;
class Expression;

/**
 * @brief 更新语句
 * @ingroup Statement
 */
class UpdateStmt : public Stmt
{
public:
  UpdateStmt() = default;
  UpdateStmt(
      Table *table,
      const vector<const FieldMeta *> &field_metas,
      vector<unique_ptr<Expression>> &&expressions,
      FilterStmt *filter_stmt);
  ~UpdateStmt() override;

public:
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);

public:
  StmtType type() const override { return StmtType::UPDATE; }

  Table                         *table() const { return table_; }
  const vector<const FieldMeta *> &field_metas() const { return field_metas_; }
  vector<unique_ptr<Expression>> &expressions() { return expressions_; }
  const vector<unique_ptr<Expression>> &expressions() const { return expressions_; }
  const FieldMeta               *field_meta() const { return field_metas_.empty() ? nullptr : field_metas_.front(); }
  FilterStmt                    *filter_stmt() const { return filter_stmt_; }
  vector<const FieldMeta *>      take_field_metas() { return std::move(field_metas_); }
  vector<unique_ptr<Expression>> take_expressions() { return std::move(expressions_); }
  FilterStmt                    *release_filter_stmt()
  {
    FilterStmt *filter_stmt = filter_stmt_;
    filter_stmt_ = nullptr;
    return filter_stmt;
  }
  void set_mirror_update(
      Table *table, vector<const FieldMeta *> &&field_metas, vector<unique_ptr<Expression>> &&expressions, FilterStmt *filter_stmt);
  void set_base_update_match_fields(
      vector<const FieldMeta *> &&base_field_metas, vector<const FieldMeta *> &&mirror_field_metas);
  Table                         *mirror_table() const { return mirror_table_; }
  const vector<const FieldMeta *> &mirror_field_metas() const { return mirror_field_metas_; }
  vector<unique_ptr<Expression>> &mirror_expressions() { return mirror_expressions_; }
  const vector<unique_ptr<Expression>> &mirror_expressions() const { return mirror_expressions_; }
  FilterStmt                    *mirror_filter_stmt() const { return mirror_filter_stmt_; }
  const vector<const FieldMeta *> &base_match_field_metas() const { return base_match_field_metas_; }
  const vector<const FieldMeta *> &mirror_match_field_metas() const { return mirror_match_field_metas_; }

private:
  Table                        *table_       = nullptr;
  vector<const FieldMeta *>     field_metas_;
  vector<unique_ptr<Expression>> expressions_;
  FilterStmt                   *filter_stmt_ = nullptr;
  Table                        *mirror_table_       = nullptr;
  vector<const FieldMeta *>     mirror_field_metas_;
  vector<unique_ptr<Expression>> mirror_expressions_;
  FilterStmt                   *mirror_filter_stmt_ = nullptr;
  vector<const FieldMeta *>     base_match_field_metas_;
  vector<const FieldMeta *>     mirror_match_field_metas_;
};
