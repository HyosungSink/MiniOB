/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the
Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/value.h"
#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "storage/field/field_meta.h"

class Table;

/**
 * @brief 逻辑算子，用于执行 update 语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(
      Table *table, const vector<const FieldMeta *> &field_metas, vector<unique_ptr<Expression>> &&expressions);
  ~UpdateLogicalOperator() override = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  OpType              get_op_type() const override { return OpType::LOGICALUPDATE; }

  Table                         *table() const { return table_; }
  const vector<const FieldMeta *> &field_metas() const { return field_metas_; }
  vector<unique_ptr<Expression>> &expressions() { return expressions_; }
  const vector<unique_ptr<Expression>> &expressions() const { return expressions_; }
  const FieldMeta               *field_meta() const { return field_metas_.empty() ? nullptr : field_metas_.front(); }
  void set_mirror_update(
      Table *table, vector<const FieldMeta *> &&field_metas, vector<unique_ptr<Expression>> &&expressions);
  Table                         *mirror_table() const { return mirror_table_; }
  const vector<const FieldMeta *> &mirror_field_metas() const { return mirror_field_metas_; }
  vector<unique_ptr<Expression>> &mirror_expressions() { return mirror_expressions_; }
  const vector<unique_ptr<Expression>> &mirror_expressions() const { return mirror_expressions_; }

private:
  Table                    *table_ = nullptr;
  vector<const FieldMeta *> field_metas_;
  vector<unique_ptr<Expression>> expressions_;
  Table                    *mirror_table_ = nullptr;
  vector<const FieldMeta *> mirror_field_metas_;
  vector<unique_ptr<Expression>> mirror_expressions_;
};
