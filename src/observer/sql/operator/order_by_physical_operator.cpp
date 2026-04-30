/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/order_by_physical_operator.h"

#include <algorithm>

#include "common/log/log.h"

using namespace std;

OrderByPhysicalOperator::OrderByPhysicalOperator(vector<unique_ptr<Expression>> &&expressions, vector<bool> &&asc, int limit)
    : expressions_(std::move(expressions)), asc_(std::move(asc)), limit_(limit)
{}

RC OrderByPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 1) {
    return RC::INTERNAL;
  }

  rows_.clear();
  position_ = 0;

  PhysicalOperator *child = children_[0].get();
  RC rc = child->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open order by child. rc=%s", strrc(rc));
    return rc;
  }

  while ((rc = child->next()) == RC::SUCCESS) {
    Tuple *tuple = child->current_tuple();
    OrderedTuple ordered_tuple;
    ordered_tuple.keys.reserve(expressions_.size());
    for (const unique_ptr<Expression> &expression : expressions_) {
      Value value;
      rc = expression->get_value(*tuple, value);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to evaluate order by expression. rc=%s", strrc(rc));
        child->close();
        return rc;
      }
      ordered_tuple.keys.emplace_back(value);
    }

    rc = ValueListTuple::make(*tuple, ordered_tuple.tuple);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to materialize tuple for order by. rc=%s", strrc(rc));
      child->close();
      return rc;
    }

    rows_.emplace_back(std::move(ordered_tuple));
  }

  if (rc != RC::RECORD_EOF) {
    child->close();
    return rc;
  }

  if (!expressions_.empty()) {
    stable_sort(rows_.begin(), rows_.end(), [this](const OrderedTuple &left, const OrderedTuple &right) {
      for (size_t i = 0; i < expressions_.size(); i++) {
        int cmp = left.keys[i].compare(right.keys[i]);
        if (cmp != 0) {
          return asc_[i] ? cmp < 0 : cmp > 0;
        }
      }
      return false;
    });
  }

  if (limit_ >= 0 && rows_.size() > static_cast<size_t>(limit_)) {
    rows_.resize(static_cast<size_t>(limit_));
  }

  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::next()
{
  if (position_ >= rows_.size()) {
    return RC::RECORD_EOF;
  }

  position_++;
  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  rows_.clear();
  position_ = 0;
  return RC::SUCCESS;
}

Tuple *OrderByPhysicalOperator::current_tuple()
{
  if (position_ == 0 || position_ > rows_.size()) {
    return nullptr;
  }

  return &rows_[position_ - 1].tuple;
}

RC OrderByPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  if (children_.empty()) {
    return RC::INTERNAL;
  }
  return children_[0]->tuple_schema(schema);
}
