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

HashJoinPhysicalOperator::HashJoinPhysicalOperator(vector<unique_ptr<Expression>> left_keys,
    vector<unique_ptr<Expression>> right_keys, vector<unique_ptr<Expression>> predicates)
    : left_keys_(std::move(left_keys)), right_keys_(std::move(right_keys)), predicates_(std::move(predicates))
{}

RC HashJoinPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 2) {
    LOG_WARN("hash join operator should have 2 children");
    return RC::INTERNAL;
  }

  trx_   = trx;
  left_  = children_[0].get();
  right_ = children_[1].get();
  right_rows_.clear();
  hash_table_.clear();
  current_matches_   = nullptr;
  current_match_pos_ = 0;
  left_tuple_        = nullptr;
  right_tuple_       = nullptr;

  RC rc = right_->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  while ((rc = right_->next()) == RC::SUCCESS) {
    Tuple *tuple = right_->current_tuple();
    string key;
    bool   has_null = false;
    rc = make_key(*tuple, right_keys_, key, has_null);
    if (rc != RC::SUCCESS) {
      right_->close();
      return rc;
    }
    if (has_null) {
      continue;
    }

    ValueListTuple value_tuple;
    rc = ValueListTuple::make(*tuple, value_tuple);
    if (rc != RC::SUCCESS) {
      right_->close();
      return rc;
    }
    right_rows_.emplace_back(std::move(value_tuple));
    hash_table_[key].push_back(right_rows_.size() - 1);
  }

  RC close_rc = right_->close();
  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }
  if (rc != RC::SUCCESS) {
    return rc;
  }
  if (close_rc != RC::SUCCESS) {
    return close_rc;
  }

  return left_->open(trx);
}

RC HashJoinPhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  while (true) {
    while (current_matches_ != nullptr && current_match_pos_ < current_matches_->size()) {
      const size_t row_index = (*current_matches_)[current_match_pos_++];
      right_tuple_ = &right_rows_[row_index];
      joined_tuple_.set_left(left_tuple_);
      joined_tuple_.set_right(right_tuple_);

      bool filter_result = true;
      rc = filter(filter_result);
      if (rc != RC::SUCCESS) {
        return rc;
      }
      if (filter_result) {
        return RC::SUCCESS;
      }
    }

    rc = left_->next();
    if (rc != RC::SUCCESS) {
      return rc;
    }
    left_tuple_ = left_->current_tuple();

    string key;
    bool   has_null = false;
    rc = make_key(*left_tuple_, left_keys_, key, has_null);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    if (has_null) {
      current_matches_ = nullptr;
      continue;
    }

    auto iter = hash_table_.find(key);
    if (iter == hash_table_.end()) {
      current_matches_ = nullptr;
      continue;
    }

    current_matches_   = &iter->second;
    current_match_pos_ = 0;
  }
}

RC HashJoinPhysicalOperator::close()
{
  RC rc = RC::SUCCESS;
  if (left_ != nullptr) {
    rc = left_->close();
  }
  right_rows_.clear();
  hash_table_.clear();
  current_matches_ = nullptr;
  return rc;
}

RC HashJoinPhysicalOperator::make_key(
    const Tuple &tuple, const vector<unique_ptr<Expression>> &keys, string &key, bool &has_null)
{
  RC rc = RC::SUCCESS;
  has_null = false;
  key.clear();
  for (const unique_ptr<Expression> &expr : keys) {
    Value value;
    rc = expr->get_value(tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    if (value.is_null()) {
      has_null = true;
      return RC::SUCCESS;
    }
    key.append(attr_type_to_string(value.attr_type()));
    key.push_back(':');
    key.append(value.to_string());
    key.push_back('|');
  }
  return RC::SUCCESS;
}

RC HashJoinPhysicalOperator::filter(bool &result)
{
  RC    rc = RC::SUCCESS;
  Value value;
  for (unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(joined_tuple_, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    if (!value.get_boolean()) {
      result = false;
      return RC::SUCCESS;
    }
  }
  result = true;
  return RC::SUCCESS;
}
