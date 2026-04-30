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

#include "common/lang/unordered_set.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"

using namespace std;

static void collect_tables(Expression &expr, unordered_set<const Table *> &tables)
{
  switch (expr.type()) {
    case ExprType::FIELD: {
      auto &field_expr = static_cast<FieldExpr &>(expr);
      tables.insert(field_expr.field().table());
    } break;
    case ExprType::CAST: {
      collect_tables(*static_cast<CastExpr &>(expr).child(), tables);
    } break;
    case ExprType::COMPARISON: {
      auto &comparison_expr = static_cast<ComparisonExpr &>(expr);
      collect_tables(*comparison_expr.left(), tables);
      collect_tables(*comparison_expr.right(), tables);
    } break;
    case ExprType::CONJUNCTION: {
      auto &conjunction_expr = static_cast<ConjunctionExpr &>(expr);
      for (auto &child : conjunction_expr.children()) {
        collect_tables(*child, tables);
      }
    } break;
    case ExprType::ARITHMETIC: {
      auto &arithmetic_expr = static_cast<ArithmeticExpr &>(expr);
      collect_tables(*arithmetic_expr.left(), tables);
      if (arithmetic_expr.right()) {
        collect_tables(*arithmetic_expr.right(), tables);
      }
    } break;
    case ExprType::FUNCTION: {
      auto &function_expr = static_cast<FunctionExpr &>(expr);
      for (auto &arg : function_expr.arguments()) {
        collect_tables(*arg, tables);
      }
    } break;
    case ExprType::IS_NULL: {
      collect_tables(*static_cast<IsNullExpr &>(expr).child(), tables);
    } break;
    case ExprType::IN_LIST: {
      auto &in_expr = static_cast<InExpr &>(expr);
      collect_tables(*in_expr.left(), tables);
      for (auto &value : in_expr.values()) {
        collect_tables(*value, tables);
      }
    } break;
    default:
      break;
  }
}

static bool contains_all(const unordered_set<const Table *> &haystack, const unordered_set<const Table *> &needles)
{
  for (const Table *table : needles) {
    if (haystack.find(table) == haystack.end()) {
      return false;
    }
  }
  return true;
}

static unordered_set<const Table *> subtree_tables(LogicalOperator &oper)
{
  unordered_set<const Table *> tables;
  if (oper.type() == LogicalOperatorType::TABLE_GET) {
    auto &table_get = static_cast<TableGetLogicalOperator &>(oper);
    tables.insert(table_get.table());
    return tables;
  }

  for (auto &child : oper.children()) {
    auto child_tables = subtree_tables(*child);
    tables.insert(child_tables.begin(), child_tables.end());
  }
  return tables;
}

static bool attach_predicate(LogicalOperator &oper, unique_ptr<Expression> predicate)
{
  unordered_set<const Table *> predicate_tables;
  collect_tables(*predicate, predicate_tables);
  if (predicate_tables.empty()) {
    return false;
  }

  if (oper.type() == LogicalOperatorType::TABLE_GET) {
    if (predicate_tables.size() != 1) {
      return false;
    }
    auto &table_get = static_cast<TableGetLogicalOperator &>(oper);
    if (*predicate_tables.begin() != table_get.table()) {
      return false;
    }
    table_get.predicates().emplace_back(std::move(predicate));
    return true;
  }

  if (oper.type() != LogicalOperatorType::JOIN) {
    return false;
  }

  for (auto &child : oper.children()) {
    auto child_tables = subtree_tables(*child);
    if (contains_all(child_tables, predicate_tables)) {
      return attach_predicate(*child, std::move(predicate));
    }
  }

  auto join_tables = subtree_tables(oper);
  if (!contains_all(join_tables, predicate_tables)) {
    return false;
  }

  auto &join = static_cast<JoinLogicalOperator &>(oper);
  join.add_join_predicate(std::move(predicate));
  return true;
}

static bool is_empty_conjunction(unique_ptr<Expression> &expr)
{
  if (!expr || expr->type() != ExprType::CONJUNCTION) {
    return false;
  }
  auto *conjunction = static_cast<ConjunctionExpr *>(expr.get());
  return conjunction->children().empty();
}

RC PredicateToJoinRewriter::rewrite(unique_ptr<LogicalOperator> &oper, bool &change_made)
{
  if (oper->type() != LogicalOperatorType::PREDICATE || oper->children().size() != 1 ||
      oper->children().front()->type() != LogicalOperatorType::JOIN || oper->expressions().size() != 1) {
    return RC::SUCCESS;
  }

  unique_ptr<Expression> &predicate = oper->expressions().front();
  LogicalOperator        &child     = *oper->children().front();

  if (predicate->type() == ExprType::CONJUNCTION &&
      static_cast<ConjunctionExpr *>(predicate.get())->conjunction_type() == ConjunctionExpr::Type::AND) {
    auto &children = static_cast<ConjunctionExpr *>(predicate.get())->children();
    for (auto iter = children.begin(); iter != children.end();) {
      if (attach_predicate(child, std::move(*iter))) {
        iter        = children.erase(iter);
        change_made = true;
      } else {
        ++iter;
      }
    }
  } else if (attach_predicate(child, std::move(predicate))) {
    change_made = true;
  }

  if (!predicate || is_empty_conjunction(predicate)) {
    Value value(true);
    predicate = make_unique<ValueExpr>(value);
  }
  return RC::SUCCESS;
}
