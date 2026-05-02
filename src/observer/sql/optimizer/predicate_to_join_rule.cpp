/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/optimizer/predicate_to_join_rule.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/join_logical_operator.h"

using namespace std;

static bool is_cross_table_comparison(ComparisonExpr *cmp_expr)
{
  Expression *left  = cmp_expr->left().get();
  Expression *right = cmp_expr->right().get();

  if (left->type() != ExprType::FIELD || right->type() != ExprType::FIELD) {
    return false;
  }

  auto *left_field  = static_cast<FieldExpr *>(left);
  auto *right_field = static_cast<FieldExpr *>(right);

  return strcmp(left_field->table_name(), right_field->table_name()) != 0;
}

static bool is_equality_comparison(Expression *expr)
{
  if (expr->type() != ExprType::COMPARISON) {
    return false;
  }
  auto *cmp = static_cast<ComparisonExpr *>(expr);
  return cmp->comp() == EQUAL_TO && is_cross_table_comparison(cmp);
}

static bool is_equality_comparison(unique_ptr<Expression> &expr)
{
  return is_equality_comparison(expr.get());
}

RC PredicateToJoinRewriter::rewrite(unique_ptr<LogicalOperator> &oper, bool &change_made)
{
  RC rc = RC::SUCCESS;

  if (oper->type() != LogicalOperatorType::PREDICATE) {
    return rc;
  }

  if (oper->children().size() != 1) {
    return rc;
  }

  unique_ptr<LogicalOperator> &child = oper->children().front();
  if (child->type() != LogicalOperatorType::JOIN) {
    return rc;
  }

  auto *join_oper = static_cast<JoinLogicalOperator *>(child.get());

  vector<unique_ptr<Expression>> &pred_exprs = oper->expressions();
  if (pred_exprs.empty()) {
    return rc;
  }

  // Process each expression in the predicate
  vector<unique_ptr<Expression>> remaining_exprs;
  for (auto &expr : pred_exprs) {
    if (expr->type() == ExprType::CONJUNCTION) {
      auto *conj = static_cast<ConjunctionExpr *>(expr.get());
      if (conj->conjunction_type() == ConjunctionExpr::Type::AND) {
        // Split AND conjunction into join predicates and remaining
        vector<unique_ptr<Expression>> remaining_children;
        for (auto &child_expr : conj->children()) {
          if (is_equality_comparison(child_expr)) {
            join_oper->add_join_predicate(std::move(child_expr));
            change_made = true;
          } else {
            remaining_children.push_back(std::move(child_expr));
          }
        }
        if (!remaining_children.empty()) {
          if (remaining_children.size() == 1) {
            remaining_exprs.push_back(std::move(remaining_children[0]));
          } else {
            remaining_exprs.push_back(
                make_unique<ConjunctionExpr>(ConjunctionExpr::Type::AND, remaining_children));
          }
        }
      } else {
        // OR conjunction - keep as is
        remaining_exprs.push_back(std::move(expr));
      }
    } else if (is_equality_comparison(expr)) {
      join_oper->add_join_predicate(std::move(expr));
      change_made = true;
    } else {
      remaining_exprs.push_back(std::move(expr));
    }
  }

  // Update predicate expressions
  pred_exprs = std::move(remaining_exprs);

  if (pred_exprs.empty()) {
    // All expressions were moved to join - replace with TRUE
    Value value((bool)true);
    pred_exprs.push_back(make_unique<ValueExpr>(value));
  }

  return rc;
}
