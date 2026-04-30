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

#include "sql/expr/expression.h"
#include "sql/operator/physical_operator.h"
#include "storage/field/field_meta.h"
#include "storage/record/record.h"

class Table;
class Trx;

/**
 * @brief 物理算子，用于执行 update 语句
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(
      Table *table, const vector<const FieldMeta *> &field_metas, vector<unique_ptr<Expression>> &&expressions);
  UpdatePhysicalOperator(Table *table,
      const vector<const FieldMeta *> &field_metas,
      vector<unique_ptr<Expression>> &&expressions,
      Table *mirror_table,
      const vector<const FieldMeta *> &mirror_field_metas,
      vector<unique_ptr<Expression>> &&mirror_expressions);
  ~UpdatePhysicalOperator() override = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }
  OpType               get_op_type() const override { return OpType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

private:
  RC make_updated_record(Table *table,
      const vector<const FieldMeta *> &field_metas,
      const vector<unique_ptr<Expression>> &expressions,
      const Record &old_record,
      Record &new_record) const;
  RC update_records(Table *table,
      const vector<const FieldMeta *> &field_metas,
      const vector<unique_ptr<Expression>> &expressions,
      PhysicalOperator &child,
      Trx *trx) const;

private:
  Table                    *table_ = nullptr;
  vector<const FieldMeta *> field_metas_;
  vector<unique_ptr<Expression>> expressions_;
  Table                    *mirror_table_ = nullptr;
  vector<const FieldMeta *> mirror_field_metas_;
  vector<unique_ptr<Expression>> mirror_expressions_;
};
