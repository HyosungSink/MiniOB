/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/12/30.
//

#include "sql/optimizer/predicate_pushdown_rewriter.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include <unordered_set>

RC PredicatePushdownRewriter::rewrite(unique_ptr<LogicalOperator> &oper, bool &change_made)
{
  RC rc = RC::SUCCESS;
  if (oper->type() != LogicalOperatorType::PREDICATE) {
    return rc;
  }

  if (oper->children().size() != 1) {
    return rc;
  }

  unique_ptr<LogicalOperator> &child_oper = oper->children().front();

  // Case 1: PREDICATE → TABLE_GET (existing logic)
  if (child_oper->type() == LogicalOperatorType::TABLE_GET) {
    auto table_get_oper = static_cast<TableGetLogicalOperator *>(child_oper.get());

    vector<unique_ptr<Expression>> &predicate_oper_exprs = oper->expressions();
    if (predicate_oper_exprs.size() != 1) {
      return rc;
    }

    unique_ptr<Expression>             &predicate_expr = predicate_oper_exprs.front();
    vector<unique_ptr<Expression>> pushdown_exprs;
    rc = get_exprs_can_pushdown(predicate_expr, pushdown_exprs);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get exprs can pushdown. rc=%s", strrc(rc));
      return rc;
    }

    if (!predicate_expr || is_empty_predicate(predicate_expr)) {
      LOG_TRACE("all expressions of predicate operator were pushdown to table get operator, then make a fake one");
      Value value((bool)true);
      predicate_expr = unique_ptr<Expression>(new ValueExpr(value));
    }

    if (!pushdown_exprs.empty()) {
      change_made = true;
      table_get_oper->set_predicates(std::move(pushdown_exprs));
    }
    return rc;
  }

  // Case 2: PREDICATE → JOIN — push table-specific predicates through the join
  if (child_oper->type() == LogicalOperatorType::JOIN) {
    vector<unique_ptr<Expression>> &pred_exprs = oper->expressions();
    if (pred_exprs.empty()) {
      return rc;
    }

    // Split top-level AND-connected expressions
    vector<unique_ptr<Expression>> all_exprs;
    for (auto &expr : pred_exprs) {
      if (expr && expr->type() == ExprType::CONJUNCTION) {
        auto *conj = static_cast<ConjunctionExpr *>(expr.get());
        if (conj->conjunction_type() == ConjunctionExpr::Type::AND) {
          for (auto &child : conj->children()) {
            all_exprs.push_back(std::move(child));
          }
          conj->children().clear();
          continue;
        }
      }
      all_exprs.push_back(std::move(expr));
    }
    pred_exprs.clear();

    // Collect tables from each child of the join
    vector<unique_ptr<LogicalOperator>> &join_children = child_oper->children();
    if (join_children.size() != 2) {
      // Can't split, put everything back
      pred_exprs = std::move(all_exprs);
      return rc;
    }

    unordered_set<string> left_tables, right_tables;
    collect_tables(join_children[0].get(), left_tables);
    collect_tables(join_children[1].get(), right_tables);

    vector<unique_ptr<Expression>> left_exprs, right_exprs, remaining;
    split_exprs_by_table_side(all_exprs, left_tables, right_tables, left_exprs, right_exprs, remaining);

    // Push left predicates to left child
    if (!left_exprs.empty()) {
      change_made = true;
      unique_ptr<Expression> left_expr;
      if (left_exprs.size() == 1) {
        left_expr = std::move(left_exprs[0]);
      } else {
        left_expr = make_unique<ConjunctionExpr>(ConjunctionExpr::Type::AND, left_exprs);
      }
      auto left_pred = make_unique<PredicateLogicalOperator>(std::move(left_expr));
      left_pred->add_child(std::move(join_children[0]));
      join_children[0] = std::move(left_pred);
    }

    // Push right predicates to right child
    if (!right_exprs.empty()) {
      change_made = true;
      unique_ptr<Expression> right_expr;
      if (right_exprs.size() == 1) {
        right_expr = std::move(right_exprs[0]);
      } else {
        right_expr = make_unique<ConjunctionExpr>(ConjunctionExpr::Type::AND, right_exprs);
      }
      auto right_pred = make_unique<PredicateLogicalOperator>(std::move(right_expr));
      right_pred->add_child(std::move(join_children[1]));
      join_children[1] = std::move(right_pred);
    }

    // Keep remaining (cross-child) predicates at this level
    if (!remaining.empty()) {
      pred_exprs = std::move(remaining);
    } else {
      Value value((bool)true);
      pred_exprs.push_back(unique_ptr<Expression>(new ValueExpr(value)));
    }
    return rc;
  }

  return rc;
}

bool PredicatePushdownRewriter::is_empty_predicate(unique_ptr<Expression> &expr)
{
  bool bool_ret = false;
  if (!expr) {
    return true;
  }

  if (expr->type() == ExprType::CONJUNCTION) {
    ConjunctionExpr *conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());
    if (conjunction_expr->children().empty()) {
      bool_ret = true;
    }
  }

  return bool_ret;
}

/**
 * 查看表达式是否可以直接下放到table get算子的filter
 * @param expr 是当前的表达式。如果可以下放给table get 算子，执行完成后expr就失效了
 * @param pushdown_exprs 当前所有要下放给table get 算子的filter。此函数执行多次，
 *                       pushdown_exprs 只会增加，不要做清理操作
 */
RC PredicatePushdownRewriter::get_exprs_can_pushdown(
    unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &pushdown_exprs)
{
  RC rc = RC::SUCCESS;
  if (expr->type() == ExprType::CONJUNCTION) {
    ConjunctionExpr *conjunction_expr = static_cast<ConjunctionExpr *>(expr.get());
    // OR expressions cannot be pushed down to table scan; keep them in the predicate operator
    if (conjunction_expr->conjunction_type() == ConjunctionExpr::Type::OR) {
      return rc;
    }

    vector<unique_ptr<Expression>> &child_exprs = conjunction_expr->children();
    for (auto iter = child_exprs.begin(); iter != child_exprs.end();) {
      // 对每个子表达式，判断是否可以下放到table get 算子
      // 如果可以的话，就从当前孩子节点中删除他
      rc = get_exprs_can_pushdown(*iter, pushdown_exprs);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to get pushdown expressions. rc=%s", strrc(rc));
        return rc;
      }

      if (!*iter) {
        iter = child_exprs.erase(iter);
      } else {
        ++iter;
      }
    }
  } else if (expr->type() == ExprType::COMPARISON) {
    // 如果是比较操作，并且比较的左边或右边是表某个列值，那么就下推下去

    pushdown_exprs.emplace_back(std::move(expr));
  }
  return rc;
}

void PredicatePushdownRewriter::collect_tables(LogicalOperator *oper, unordered_set<string> &tables)
{
  if (oper->type() == LogicalOperatorType::TABLE_GET) {
    auto *table_get = static_cast<TableGetLogicalOperator *>(oper);
    tables.insert(table_get->table()->name());
  } else if (oper->type() == LogicalOperatorType::JOIN) {
    for (auto &child : oper->children()) {
      collect_tables(child.get(), tables);
    }
  }
}

bool PredicatePushdownRewriter::expr_refs_only_tables(Expression *expr, const unordered_set<string> &tables)
{
  if (expr->type() == ExprType::FIELD) {
    auto *field_expr = static_cast<FieldExpr *>(expr);
    return tables.count(field_expr->table_name()) > 0;
  }

  // Recurse into child expressions
  vector<Expression *> children;
  switch (expr->type()) {
    case ExprType::COMPARISON: {
      auto *cmp = static_cast<ComparisonExpr *>(expr);
      children.push_back(cmp->left().get());
      children.push_back(cmp->right().get());
    } break;
    case ExprType::CONJUNCTION: {
      auto *conj = static_cast<ConjunctionExpr *>(expr);
      for (auto &child : conj->children()) {
        children.push_back(child.get());
      }
    } break;
    case ExprType::ARITHMETIC: {
      auto *arith = static_cast<ArithmeticExpr *>(expr);
      children.push_back(arith->left().get());
      children.push_back(arith->right().get());
    } break;
    case ExprType::CAST: {
      auto *cast_expr = static_cast<CastExpr *>(expr);
      children.push_back(cast_expr->child().get());
    } break;
    case ExprType::VALUE:
    case ExprType::STAR:
      // No field references, so it's fine
      return true;
    default:
      break;
  }

  for (Expression *child : children) {
    if (!expr_refs_only_tables(child, tables)) {
      return false;
    }
  }
  return true;
}

void PredicatePushdownRewriter::split_exprs_by_table_side(
    vector<unique_ptr<Expression>> &exprs,
    const unordered_set<string> &left_tables,
    const unordered_set<string> &right_tables,
    vector<unique_ptr<Expression>> &left_exprs,
    vector<unique_ptr<Expression>> &right_exprs,
    vector<unique_ptr<Expression>> &remaining)
{
  for (auto &expr : exprs) {
    bool left_only  = expr_refs_only_tables(expr.get(), left_tables);
    bool right_only = expr_refs_only_tables(expr.get(), right_tables);

    // Only push to a side if it exclusively references that side's tables.
    // If both are true, the expression references no tables (e.g. 1=1) — keep it.
    if (left_only && !right_only) {
      left_exprs.push_back(std::move(expr));
    } else if (right_only && !left_only) {
      right_exprs.push_back(std::move(expr));
    } else {
      remaining.push_back(std::move(expr));
    }
  }
}
