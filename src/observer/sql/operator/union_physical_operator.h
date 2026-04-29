/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/operator/physical_operator.h"

class UnionPhysicalOperator : public PhysicalOperator
{
public:
  explicit UnionPhysicalOperator(vector<bool> &&union_all) : union_all_(std::move(union_all)) {}
  ~UnionPhysicalOperator() override = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UNION; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;
  RC     tuple_schema(TupleSchema &schema) const override;

private:
  static bool same_row(const vector<Value> &left, const vector<Value> &right);
  void        deduplicate_rows();

private:
  vector<bool>           union_all_;
  vector<ValueListTuple> rows_;
  vector<TupleCellSpec>  specs_;
  size_t                 position_ = 0;
};
