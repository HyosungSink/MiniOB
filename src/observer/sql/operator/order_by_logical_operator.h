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

class OrderByLogicalOperator : public LogicalOperator
{
public:
  OrderByLogicalOperator(vector<unique_ptr<Expression>> &&expressions, vector<bool> &&asc, int limit)
      : asc_(std::move(asc)), limit_(limit)
  {
    expressions_ = std::move(expressions);
  }

  ~OrderByLogicalOperator() override = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::ORDER_BY; }
  OpType              get_op_type() const override { return OpType::LOGICALLIMIT; }

  vector<unique_ptr<Expression>> &order_expressions() { return expressions_; }
  vector<bool>                   &asc() { return asc_; }
  int                             limit() const { return limit_; }

private:
  vector<bool> asc_;
  int          limit_ = -1;
};
