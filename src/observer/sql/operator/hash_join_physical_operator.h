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
#include "sql/parser/parse.h"
#include "sql/expr/expression.h"
#include <vector>
#include <unordered_map>

/**
 * @brief Hash Join 算子
 * @ingroup PhysicalOperator
 * @details 使用哈希表实现等值连接。左表构建哈希表，右表进行探测。
 */
class HashJoinPhysicalOperator : public PhysicalOperator
{
public:
  HashJoinPhysicalOperator() = default;
  virtual ~HashJoinPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::HASH_JOIN; }

  OpType get_op_type() const override { return OpType::INNERHASHJOIN; }

  virtual double calculate_cost(
      LogicalProperty *prop, const vector<LogicalProperty *> &child_log_props, CostModel *cm) override
  {
    if (child_log_props.size() >= 2) {
      double left_card  = child_log_props[0]->get_card();
      double right_card = child_log_props[1]->get_card();
      return left_card + right_card;
    }
    return 0.0;
  }

  RC     open(Trx *trx) override;
  RC     next() override;
  RC     close() override;
  Tuple *current_tuple() override;

  void set_join_condition(unique_ptr<Expression> condition) { join_condition_ = std::move(condition); }

private:
  RC build_hash_table();
  RC probe_next();

  struct HashEntry {
    vector<Value> key_values;
    vector<unique_ptr<Tuple>> tuples;
  };

  struct ValueHash {
    size_t operator()(const vector<Value> &vals) const;
  };

  struct ValueEqual {
    bool operator()(const vector<Value> &a, const vector<Value> &b) const;
  };

  Trx *trx_ = nullptr;
  PhysicalOperator *left_  = nullptr;
  PhysicalOperator *right_ = nullptr;

  unique_ptr<Expression> join_condition_;

  // Hash table: key_values -> list of tuples
  unordered_map<vector<Value>, vector<unique_ptr<Tuple>>, ValueHash, ValueEqual> hash_table_;

  // Current state
  vector<unique_ptr<Tuple>> *current_matches_ = nullptr;
  size_t                      current_match_idx_ = 0;
  Tuple                      *right_tuple_ = nullptr;
  JoinedTuple                 joined_tuple_;
  bool                        built_ = false;
  bool                        right_exhausted_ = true;
};
