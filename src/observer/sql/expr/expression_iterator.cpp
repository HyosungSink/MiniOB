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
// Created by Wangyunlai on 2024/05/30.
//

#include "sql/expr/expression_iterator.h"
#include "sql/expr/expression.h"
#include "common/log/log.h"

using namespace std;

RC ExpressionIterator::iterate_child_expr(Expression &expr, function<RC(unique_ptr<Expression> &)> callback)
{
  RC rc = RC::SUCCESS;

  switch (expr.type()) {
    case ExprType::CAST: {
      auto &cast_expr = static_cast<CastExpr &>(expr);
      rc = callback(cast_expr.child());
    } break;

    case ExprType::COMPARISON: {

      auto &comparison_expr = static_cast<ComparisonExpr &>(expr);
      rc = callback(comparison_expr.left());

      if (OB_SUCC(rc)) {
        rc = callback(comparison_expr.right());
      }

    } break;

    case ExprType::CONJUNCTION: {
      auto &conjunction_expr = static_cast<ConjunctionExpr &>(expr);
      for (auto &child : conjunction_expr.children()) {
        rc = callback(child);
        if (OB_FAIL(rc)) {
          break;
        }
      }
    } break;

    case ExprType::IN_LIST: {
      auto &in_expr = static_cast<InExpr &>(expr);
      rc = callback(in_expr.left());
      if (OB_FAIL(rc)) {
        break;
      }
      for (auto &value : in_expr.values()) {
        rc = callback(value);
        if (OB_FAIL(rc)) {
          break;
        }
      }
    } break;

    case ExprType::IN_SUBQUERY: {
      auto &in_subquery_expr = static_cast<InSubqueryExpr &>(expr);
      rc = callback(in_subquery_expr.left());
    } break;

    case ExprType::EXISTS_SUBQUERY: {
      // EXISTS has no scalar child expression; the subquery itself is bound separately.
    } break;

    case ExprType::IS_NULL: {
      auto &is_null_expr = static_cast<IsNullExpr &>(expr);
      rc = callback(is_null_expr.child());
    } break;

    case ExprType::COMP_SUBQUERY: {
      auto &comp_subquery_expr = static_cast<QuantifiedComparisonExpr &>(expr);
      rc = callback(comp_subquery_expr.left());
    } break;

    case ExprType::ARITHMETIC: {

      auto &arithmetic_expr = static_cast<ArithmeticExpr &>(expr);
      rc = callback(arithmetic_expr.left());
      unique_ptr<Expression> &right_expr = arithmetic_expr.right();
      if (OB_SUCC(rc) && right_expr != nullptr) {
        rc = callback(right_expr);
      }
    } break;

    case ExprType::FUNCTION: {
      auto &function_expr = static_cast<FunctionExpr &>(expr);
      for (auto &argument : function_expr.arguments()) {
        rc = callback(argument);
        if (OB_FAIL(rc)) {
          break;
        }
      }
    } break;

    case ExprType::AGGREGATION: {
      auto &aggregate_expr = static_cast<AggregateExpr &>(expr);
      rc = callback(aggregate_expr.child());
    } break;

    case ExprType::NONE:
    case ExprType::STAR:
    case ExprType::UNBOUND_FIELD:
    case ExprType::UNBOUND_FUNCTION:
    case ExprType::FIELD:
    case ExprType::VALUE:
    case ExprType::SUBQUERY: {
      // Do nothing
    } break;

    default: {
      ASSERT(false, "Unknown expression type");
    }
  }

  return rc;
}
