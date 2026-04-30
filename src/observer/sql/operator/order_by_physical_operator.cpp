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
#include <functional>

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
  output_specs_.clear();
  sort_key_refs_.clear();
  position_ = 0;

  PhysicalOperator *child = children_[0].get();
  RC rc = child->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open order by child. rc=%s", strrc(rc));
    return rc;
  }

  auto row_key = [this](const OrderedTuple &row, size_t key_index) -> const Value & {
    const SortKeyRef &key_ref = sort_key_refs_[key_index];
    return key_ref.cell_index >= 0 ? row.tuple.cell_ref(key_ref.cell_index) : row.keys[key_ref.key_index];
  };

  auto comes_before = [&row_key, this](const OrderedTuple &left, const OrderedTuple &right) {
    for (size_t i = 0; i < expressions_.size(); i++) {
      int cmp = row_key(left, i).compare(row_key(right, i));
      if (cmp != 0) {
        return asc_[i] ? cmp < 0 : cmp > 0;
      }
    }
    return false;
  };

  auto keys_come_before = [&row_key, this](const vector<Value> &left_keys, const OrderedTuple &right) {
    for (size_t i = 0; i < expressions_.size(); i++) {
      int cmp = left_keys[i].compare(row_key(right, i));
      if (cmp != 0) {
        return asc_[i] ? cmp < 0 : cmp > 0;
      }
    }
    return false;
  };

  const bool use_topn = limit_ >= 0 && !expressions_.empty();
  auto       heap_cmp = [&comes_before](const OrderedTuple &left, const OrderedTuple &right) {
    return comes_before(left, right);
  };

  while ((rc = child->next()) == RC::SUCCESS) {
    Tuple *tuple = child->current_tuple();

    rc = init_output_specs(*tuple);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to init order by output specs. rc=%s", strrc(rc));
      child->close();
      return rc;
    }

    rc = init_sort_key_refs();
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to init order by sort key refs. rc=%s", strrc(rc));
      child->close();
      return rc;
    }

    vector<Value> evaluated_keys;
    evaluated_keys.reserve(expressions_.size());
    for (const unique_ptr<Expression> &expression : expressions_) {
      Value value;
      rc = expression->get_value(*tuple, value);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to evaluate order by expression. rc=%s", strrc(rc));
        child->close();
        return rc;
      }
      evaluated_keys.emplace_back(std::move(value));
    }

    if (use_topn && limit_ == 0) {
      continue;
    }

    if (use_topn && rows_.size() >= static_cast<size_t>(limit_) && !keys_come_before(evaluated_keys, rows_.front())) {
      continue;
    }

    OrderedTuple ordered_tuple;
    rc = materialize_tuple_cells(*tuple, ordered_tuple.tuple);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to materialize tuple for order by. rc=%s", strrc(rc));
      child->close();
      return rc;
    }
    ordered_tuple.keys.reserve(expressions_.size());
    for (size_t i = 0; i < expressions_.size(); i++) {
      if (sort_key_refs_[i].cell_index < 0) {
        ordered_tuple.keys.emplace_back(std::move(evaluated_keys[i]));
      }
    }

    if (use_topn && rows_.size() >= static_cast<size_t>(limit_)) {
      pop_heap(rows_.begin(), rows_.end(), heap_cmp);
      rows_.back() = std::move(ordered_tuple);
      push_heap(rows_.begin(), rows_.end(), heap_cmp);
    } else {
      rows_.emplace_back(std::move(ordered_tuple));
      if (use_topn) {
        push_heap(rows_.begin(), rows_.end(), heap_cmp);
      }
    }
  }

  if (rc != RC::RECORD_EOF) {
    child->close();
    return rc;
  }

  if (!expressions_.empty()) {
    stable_sort(rows_.begin(), rows_.end(), comes_before);
  }

  if (limit_ >= 0 && rows_.size() > static_cast<size_t>(limit_)) {
    rows_.resize(static_cast<size_t>(limit_));
  }

  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::init_sort_key_refs()
{
  if (!sort_key_refs_.empty() || expressions_.empty()) {
    return RC::SUCCESS;
  }

  sort_key_refs_.resize(expressions_.size());
  int key_index = 0;
  for (size_t i = 0; i < expressions_.size(); i++) {
    if (expressions_[i]->type() == ExprType::FIELD) {
      const FieldExpr &field_expr = static_cast<const FieldExpr &>(*expressions_[i]);
      TupleCellSpec    spec(field_expr.table_name(), field_expr.field_name());
      for (size_t j = 0; j < output_specs_.size(); j++) {
        if (output_specs_[j].equals(spec)) {
          sort_key_refs_[i].cell_index = static_cast<int>(j);
          break;
        }
      }
    }

    if (sort_key_refs_[i].cell_index < 0) {
      sort_key_refs_[i].key_index = key_index++;
    }
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
  output_specs_.clear();
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

RC OrderByPhysicalOperator::init_output_specs(const Tuple &tuple)
{
  if (!output_specs_.empty()) {
    return RC::SUCCESS;
  }

  const int cell_num = tuple.cell_num();
  output_specs_.reserve(cell_num);
  for (int i = 0; i < cell_num; i++) {
    TupleCellSpec spec;
    RC rc = tuple.spec_at(i, spec);
    if (OB_FAIL(rc)) {
      return rc;
    }
    output_specs_.push_back(std::move(spec));
  }
  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::materialize_tuple_cells(const Tuple &tuple, ValueListTuple &value_list) const
{
  vector<Value> cells;
  const int cell_num = tuple.cell_num();
  cells.reserve(cell_num);
  for (int i = 0; i < cell_num; i++) {
    Value cell;
    RC rc = tuple.cell_at(i, cell);
    if (OB_FAIL(rc)) {
      return rc;
    }
    cells.push_back(std::move(cell));
  }

  value_list.set_names_ref(&output_specs_);
  value_list.set_cells(cells);
  return RC::SUCCESS;
}
