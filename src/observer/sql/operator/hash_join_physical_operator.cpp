/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/hash_join_physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"

using namespace std;

size_t HashJoinPhysicalOperator::ValueHash::operator()(const vector<Value> &vals) const
{
  size_t h = 0;
  for (const Value &v : vals) {
    int int_val = v.get_int();
    h ^= std::hash<int>{}(int_val) + 0x9e3779b9 + (h << 6) + (h >> 2);
  }
  return h;
}

bool HashJoinPhysicalOperator::ValueEqual::operator()(const vector<Value> &a, const vector<Value> &b) const
{
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); i++) {
    if (a[i].compare(b[i]) != 0) return false;
  }
  return true;
}

static unique_ptr<ValueListTuple> clone_tuple(Tuple *tuple)
{
  auto stored = make_unique<ValueListTuple>();
  RC rc = ValueListTuple::make(*tuple, *stored);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to clone tuple. rc=%s", strrc(rc));
    return nullptr;
  }
  return stored;
}

RC HashJoinPhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  trx_ = trx;

  if (children_.size() != 2) {
    LOG_WARN("hash join should have 2 children, but have %d", children_.size());
    return RC::INTERNAL;
  }

  left_  = children_[0].get();
  right_ = children_[1].get();

  rc = left_->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open left child. rc=%s", strrc(rc));
    return rc;
  }

  rc = right_->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open right child. rc=%s", strrc(rc));
    left_->close();
    return rc;
  }

  // Build hash table from left child
  rc = build_hash_table();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to build hash table. rc=%s", strrc(rc));
    return rc;
  }

  built_ = true;
  right_exhausted_ = true;
  current_matches_ = nullptr;
  current_match_idx_ = 0;

  return rc;
}

RC HashJoinPhysicalOperator::build_hash_table()
{
  RC rc = RC::SUCCESS;

  // Extract the left key expression from the join condition
  ComparisonExpr *cmp_expr = nullptr;
  if (join_condition_ && join_condition_->type() == ExprType::COMPARISON) {
    cmp_expr = static_cast<ComparisonExpr *>(join_condition_.get());
  }
  if (!cmp_expr || cmp_expr->comp() != EQUAL_TO) {
    LOG_WARN("hash join requires equality condition");
    return RC::INVALID_ARGUMENT;
  }

  Expression *left_key_expr = cmp_expr->left().get();

  while (RC::SUCCESS == (rc = left_->next())) {
    Tuple *tuple = left_->current_tuple();
    if (!tuple) continue;

    Value key_val;
    rc = left_key_expr->get_value(*tuple, key_val);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get left key value. rc=%s", strrc(rc));
      return rc;
    }

    vector<Value> key{key_val};
    hash_table_[key].push_back(clone_tuple(tuple));
  }
  left_->close();

  return RC::SUCCESS;
}

RC HashJoinPhysicalOperator::next()
{
  RC rc = RC::SUCCESS;

  if (!built_) {
    return RC::INTERNAL;
  }

  // Try to get next match from current right tuple's matches
  if (current_matches_ && current_match_idx_ < current_matches_->size()) {
    current_match_idx_++;
    if (current_match_idx_ < current_matches_->size()) {
      return RC::SUCCESS;
    }
    current_matches_ = nullptr;
    right_exhausted_ = true;
  }

  // Probe with next right tuple
  while (true) {
    if (right_exhausted_) {
      rc = right_->next();
      if (rc != RC::SUCCESS) {
        return rc; // EOF or error
      }
      right_tuple_ = right_->current_tuple();
      right_exhausted_ = false;
    }

    if (!right_tuple_) {
      right_exhausted_ = true;
      continue;
    }

    // Get right key value
    ComparisonExpr *cmp_expr = static_cast<ComparisonExpr *>(join_condition_.get());
    Expression *right_key_expr = cmp_expr->right().get();

    Value key_val;
    rc = right_key_expr->get_value(*right_tuple_, key_val);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get right key value. rc=%s", strrc(rc));
      right_exhausted_ = true;
      continue;
    }

    vector<Value> key{key_val};
    auto iter = hash_table_.find(key);
    if (iter != hash_table_.end() && !iter->second.empty()) {
      current_matches_ = &iter->second;
      current_match_idx_ = 0;
      return RC::SUCCESS;
    }

    // No match, try next right tuple
    right_exhausted_ = true;
  }
}

RC HashJoinPhysicalOperator::close()
{
  hash_table_.clear();
  current_matches_ = nullptr;
  current_match_idx_ = 0;
  built_ = false;

  if (left_) {
    left_->close();
  }
  if (right_) {
    right_->close();
  }

  return RC::SUCCESS;
}

Tuple *HashJoinPhysicalOperator::current_tuple()
{
  if (!current_matches_ || current_match_idx_ >= current_matches_->size()) {
    return nullptr;
  }

  Tuple *left_tuple = (*current_matches_)[current_match_idx_].get();
  joined_tuple_.set_left(left_tuple);
  joined_tuple_.set_right(right_tuple_);
  return &joined_tuple_;
}
