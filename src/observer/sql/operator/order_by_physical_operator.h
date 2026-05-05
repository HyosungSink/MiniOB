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

class OrderByPhysicalOperator : public PhysicalOperator
{
public:
  OrderByPhysicalOperator(vector<unique_ptr<Expression>> &&expressions, vector<bool> &&asc, int limit);
  ~OrderByPhysicalOperator() override = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;
  RC     tuple_schema(TupleSchema &schema) const override;

private:
  struct CellInfo
  {
    AttrType type   = AttrType::UNDEFINED;
    int      offset = 0;
    int      length = 0;
  };

  struct OrderedTuple
  {
    vector<Value> cells;
    vector<Value> keys;
    size_t        packed_block        = 0;
    size_t        packed_block_offset = 0;
    bool          packed              = false;
  };

  struct SortKeyRef
  {
    int cell_index = -1;
    int key_index  = -1;
  };

  class PackedCellStorage;

  class MaterializedTuple : public Tuple
  {
  public:
    void set_context(const OrderedTuple *row, const vector<TupleCellSpec> *specs, const vector<CellInfo> *cell_infos,
        const PackedCellStorage *packed_cells);

    int cell_num() const override;
    RC  cell_at(int index, Value &cell) const override;
    RC  spec_at(int index, TupleCellSpec &spec) const override;
    RC  find_cell(const TupleCellSpec &spec, Value &cell) const override;

  private:
    const OrderedTuple          *row_        = nullptr;
    const vector<TupleCellSpec> *specs_      = nullptr;
    const vector<CellInfo>      *cell_infos_ = nullptr;
    const PackedCellStorage     *packed_cells_ = nullptr;
  };

  class PackedCellStorage
  {
  public:
    void clear();
    char *append_row(size_t length, size_t &block_index, size_t &block_offset);
    const char *data(size_t block_index, size_t block_offset) const;

  private:
    static constexpr size_t BLOCK_SIZE = 1024 * 1024;

  private:
    vector<vector<char>> blocks_;
    size_t               write_offset_ = 0;
  };

  RC init_output_specs(const Tuple &tuple);
  RC init_sort_key_refs();
  RC init_cell_infos(const Tuple &tuple);
  RC materialize_tuple_cells(const Tuple &tuple, OrderedTuple &ordered_tuple);
  void read_cell_value(const OrderedTuple &row, int cell_index, Value &cell) const;
  int  compare_sort_key(const OrderedTuple &left, const OrderedTuple &right, size_t key_index) const;
  int  compare_evaluated_key(const vector<Value> &left_keys, const OrderedTuple &right, size_t key_index) const;

private:
  vector<unique_ptr<Expression>> expressions_;
  vector<bool>                   asc_;
  int                            limit_ = -1;
  vector<OrderedTuple>           rows_;
  vector<TupleCellSpec>           output_specs_;
  vector<CellInfo>                cell_infos_;
  vector<SortKeyRef>              sort_key_refs_;
  PackedCellStorage                packed_cells_;
  MaterializedTuple               current_tuple_;
  size_t                         position_ = 0;
  int                            packed_cell_size_ = 0;
};
