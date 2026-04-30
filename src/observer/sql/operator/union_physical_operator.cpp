/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/union_physical_operator.h"

#include "common/log/log.h"

using namespace std;

RC UnionPhysicalOperator::open(Trx *trx)
{
  if (children_.empty() || union_all_.size() + 1 != children_.size()) {
    return RC::INTERNAL;
  }

  rows_.clear();
  specs_.clear();
  position_ = 0;

  for (size_t child_index = 0; child_index < children_.size(); child_index++) {
    PhysicalOperator *child = children_[child_index].get();
    RC rc = child->open(trx);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to open union child. rc=%s", strrc(rc));
      return rc;
    }

    while ((rc = child->next()) == RC::SUCCESS) {
      Tuple *tuple = child->current_tuple();
      if (specs_.empty()) {
        for (int i = 0; i < tuple->cell_num(); i++) {
          TupleCellSpec spec;
          rc = tuple->spec_at(i, spec);
          if (OB_FAIL(rc)) {
            child->close();
            return rc;
          }
          specs_.push_back(spec);
        }
      }

      vector<Value> cells;
      cells.reserve(tuple->cell_num());
      for (int i = 0; i < tuple->cell_num(); i++) {
        Value cell;
        rc = tuple->cell_at(i, cell);
        if (OB_FAIL(rc)) {
          child->close();
          return rc;
        }
        cells.emplace_back(cell);
      }

      ValueListTuple value_tuple;
      value_tuple.set_cells(cells);
      value_tuple.set_names(specs_);
      rows_.emplace_back(std::move(value_tuple));
    }

    child->close();
    if (rc != RC::RECORD_EOF) {
      return rc;
    }

    if (child_index > 0 && !union_all_[child_index - 1]) {
      deduplicate_rows();
    }
  }

  return RC::SUCCESS;
}

RC UnionPhysicalOperator::next()
{
  if (position_ >= rows_.size()) {
    return RC::RECORD_EOF;
  }
  position_++;
  return RC::SUCCESS;
}

RC UnionPhysicalOperator::close()
{
  for (unique_ptr<PhysicalOperator> &child : children_) {
    child->close();
  }
  rows_.clear();
  specs_.clear();
  position_ = 0;
  return RC::SUCCESS;
}

Tuple *UnionPhysicalOperator::current_tuple()
{
  if (position_ == 0 || position_ > rows_.size()) {
    return nullptr;
  }
  return &rows_[position_ - 1];
}

RC UnionPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  if (specs_.empty() && !children_.empty()) {
    return children_[0]->tuple_schema(schema);
  }
  for (const TupleCellSpec &spec : specs_) {
    schema.append_cell(spec);
  }
  return RC::SUCCESS;
}

bool UnionPhysicalOperator::same_row(const vector<Value> &left, const vector<Value> &right)
{
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); i++) {
    if (left[i].compare(right[i]) != 0) {
      return false;
    }
  }
  return true;
}

void UnionPhysicalOperator::deduplicate_rows()
{
  vector<ValueListTuple> unique_rows;
  vector<vector<Value>>  unique_cells;
  for (ValueListTuple &row : rows_) {
    vector<Value> cells;
    cells.reserve(row.cell_num());
    for (int i = 0; i < row.cell_num(); i++) {
      Value cell;
      row.cell_at(i, cell);
      cells.emplace_back(cell);
    }

    bool exists = false;
    for (const vector<Value> &existing : unique_cells) {
      if (same_row(existing, cells)) {
        exists = true;
        break;
      }
    }

    if (!exists) {
      unique_cells.emplace_back(std::move(cells));
      unique_rows.emplace_back(std::move(row));
    }
  }
  rows_.swap(unique_rows);
}
