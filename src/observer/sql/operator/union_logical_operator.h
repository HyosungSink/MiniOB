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

#include "sql/operator/logical_operator.h"

class UnionLogicalOperator : public LogicalOperator
{
public:
  explicit UnionLogicalOperator(vector<bool> &&union_all) : union_all_(std::move(union_all)) {}
  ~UnionLogicalOperator() override = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UNION; }
  OpType              get_op_type() const override { return OpType::LOGICALPHYSICALDELIMITER; }

  vector<bool> &union_all() { return union_all_; }

private:
  vector<bool> union_all_;
};
