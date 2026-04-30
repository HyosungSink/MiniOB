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

#include "common/lang/unordered_map.h"
#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"

/**
 * @brief Hash Join 算子
 * @ingroup PhysicalOperator
 */
class HashJoinPhysicalOperator
    : public PhysicalOperator
{
public:
  HashJoinPhysicalOperator(vector<unique_ptr<Expression>> left_keys,
      vector<unique_ptr<Expression>> right_keys, vector<unique_ptr<Expression>> predicates);
  virtual ~HashJoinPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::HASH_JOIN; }
  OpType               get_op_type() const override { return OpType::INNERHASHJOIN; }

  double calculate_cost(LogicalProperty *prop, const vector<LogicalProperty *> &child_log_props, CostModel *cm) override
  {
    if (child_log_props.size() != 2) {
      return 0.0;
    }
    int left_card  = child_log_props[0]->get_card();
    int right_card = child_log_props[1]->get_card();
    if (left_card * right_card <= 1) {
      return cm->cpu_op() * 2;
    }
    return (left_card + right_card) * cm->hash_probe() * 0.1;
  }

  RC     open(Trx *trx) override;
  RC     next() override;
  RC     close() override;
  Tuple *current_tuple() override { return &joined_tuple_; }

private:
  RC make_key(const Tuple &tuple, const vector<unique_ptr<Expression>> &keys, string &key, bool &has_null);
  RC filter(bool &result);

private:
  Trx              *trx_   = nullptr;
  PhysicalOperator *left_  = nullptr;
  PhysicalOperator *right_ = nullptr;

  vector<unique_ptr<Expression>> left_keys_;
  vector<unique_ptr<Expression>> right_keys_;
  vector<unique_ptr<Expression>> predicates_;

  vector<ValueListTuple>                  right_rows_;
  unordered_map<string, vector<size_t>>   hash_table_;
  const vector<size_t>                   *current_matches_ = nullptr;
  size_t                                  current_match_pos_ = 0;
  Tuple                                  *left_tuple_ = nullptr;
  ValueListTuple                         *right_tuple_ = nullptr;
  JoinedTuple                             joined_tuple_;
};
