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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/expr/expression_iterator.h"
#include "sql/parser/expression_binder.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

static unique_ptr<Expression> copy_rewrite_view_expr(const Expression &expr, const ViewDefinition &view)
{
  if (expr.type() == ExprType::UNBOUND_FIELD) {
    const UnboundFieldExpr &field_expr = static_cast<const UnboundFieldExpr &>(expr);
    const bool              from_view  = common::is_blank(field_expr.table_name()) ||
                            0 == strcasecmp(field_expr.table_name(), view.view_name.c_str());
    if (from_view) {
      const string *base_column = view.base_column_for(field_expr.field_name());
      if (base_column != nullptr) {
        auto rewritten = make_unique<UnboundFieldExpr>("", *base_column);
        rewritten->set_name(*base_column);
        return rewritten;
      }
    }
  }

  return expr.copy();
}

static RC create_table_update_stmt(Db *db, const UpdateSqlNode &update, Table *table, Stmt *&stmt);

static RC create_view_update_stmt(Db *db, const UpdateSqlNode &update, const ViewDefinition &view, Stmt *&stmt)
{
  if (!view.updatable) {
    if (!view.materialized_insertable) {
      return RC::INVALID_ARGUMENT;
    }

    Table *view_table = db->find_table(view.view_name.c_str());
    if (view_table == nullptr) {
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    for (const UpdateAssignmentSqlNode &assignment : update.assignments) {
      if (view.base_column_for(assignment.attribute_name) == nullptr) {
        return RC::SCHEMA_FIELD_NOT_EXIST;
      }
    }
    return create_table_update_stmt(db, update, view_table, stmt);
  }

  Table *view_table = db->find_table(view.view_name.c_str());
  if (view_table == nullptr) {
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  UpdateSqlNode base_update;
  base_update.relation_name = view.base_table_name;

  for (const UpdateAssignmentSqlNode &assignment : update.assignments) {
    const string *base_column = view.base_column_for(assignment.attribute_name);
    if (base_column == nullptr) {
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }

    UpdateAssignmentSqlNode base_assignment;
    base_assignment.attribute_name = *base_column;
    base_assignment.value          = assignment.value;
    if (assignment.expression != nullptr) {
      base_assignment.expression = copy_rewrite_view_expr(*assignment.expression, view);
    }
    base_update.assignments.emplace_back(std::move(base_assignment));
  }

  for (const ConditionSqlNode &condition : update.conditions) {
    ConditionSqlNode base_condition;
    base_condition.conjunction = condition.conjunction;
    base_condition.comp        = condition.comp;
    if (condition.left_expr != nullptr) {
      base_condition.left_expr = copy_rewrite_view_expr(*condition.left_expr, view);
    }
    if (condition.right_expr != nullptr) {
      base_condition.right_expr = copy_rewrite_view_expr(*condition.right_expr, view);
    }
    base_update.conditions.emplace_back(std::move(base_condition));
  }

  Stmt *base_stmt = nullptr;
  RC rc = create_table_update_stmt(db, base_update, db->find_table(view.base_table_name.c_str()), base_stmt);
  if (OB_FAIL(rc)) {
    return rc;
  }

  Stmt *mirror_stmt = nullptr;
  rc = create_table_update_stmt(db, update, view_table, mirror_stmt);
  if (OB_FAIL(rc)) {
    delete base_stmt;
    return rc;
  }

  auto *base_update_stmt   = static_cast<UpdateStmt *>(base_stmt);
  auto *mirror_update_stmt = static_cast<UpdateStmt *>(mirror_stmt);
  base_update_stmt->set_mirror_update(mirror_update_stmt->table(),
      mirror_update_stmt->take_field_metas(),
      mirror_update_stmt->take_expressions(),
      mirror_update_stmt->release_filter_stmt());
  delete mirror_update_stmt;

  stmt = base_update_stmt;
  return RC::SUCCESS;
}

UpdateStmt::UpdateStmt(
    Table *table,
    const vector<const FieldMeta *> &field_metas,
    vector<unique_ptr<Expression>> &&expressions,
    FilterStmt *filter_stmt)
    : table_(table), field_metas_(field_metas), expressions_(std::move(expressions)), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (filter_stmt_ != nullptr) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
  if (mirror_filter_stmt_ != nullptr) {
    delete mirror_filter_stmt_;
    mirror_filter_stmt_ = nullptr;
  }
}

void UpdateStmt::set_mirror_update(
    Table *table, vector<const FieldMeta *> &&field_metas, vector<unique_ptr<Expression>> &&expressions, FilterStmt *filter_stmt)
{
  mirror_table_       = table;
  mirror_field_metas_ = std::move(field_metas);
  mirror_expressions_ = std::move(expressions);
  mirror_filter_stmt_ = filter_stmt;
}

static RC create_table_update_stmt(Db *db, const UpdateSqlNode &update, Table *table, Stmt *&stmt)
{
  stmt = nullptr;

  const char *table_name = update.relation_name.c_str();
  if (db == nullptr || common::is_blank(table_name) || update.assignments.empty()) {
    LOG_WARN("invalid update statement. db=%p, table=%s, assignment_num=%d",
        db, table_name, static_cast<int>(update.assignments.size()));
    return RC::INVALID_ARGUMENT;
  }

  if (table == nullptr) {
    LOG_WARN("no such table. db=%s, table=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  vector<const FieldMeta *> field_metas;
  vector<unique_ptr<Expression>> expressions;
  field_metas.reserve(update.assignments.size());
  expressions.reserve(update.assignments.size());

  RC rc = RC::SUCCESS;
  BinderContext binder_context;
  binder_context.set_db(db);
  binder_context.add_table(table);
  ExpressionBinder expression_binder(binder_context);

  auto reject_aggregate_expression = [](Expression &expr) -> RC {
    if (expr.type() == ExprType::AGGREGATION) {
      LOG_WARN("aggregate expression is not allowed in update assignment");
      return RC::INVALID_ARGUMENT;
    }

    function<RC(unique_ptr<Expression> &)> check_child = [&](unique_ptr<Expression> &child) -> RC {
      if (child->type() == ExprType::AGGREGATION) {
        LOG_WARN("aggregate expression is not allowed in update assignment");
        return RC::INVALID_ARGUMENT;
      }
      return ExpressionIterator::iterate_child_expr(*child, check_child);
    };

    return ExpressionIterator::iterate_child_expr(expr, check_child);
  };

  function<RC(Expression &)> allow_assignment_subquery_rows;
  allow_assignment_subquery_rows = [&](Expression &expr) -> RC {
    if (expr.type() == ExprType::SUBQUERY) {
      static_cast<SubqueryExpr &>(expr).set_allow_multi_row_scalar(true);
      return RC::SUCCESS;
    }

    function<RC(unique_ptr<Expression> &)> mark_child = [&](unique_ptr<Expression> &child) -> RC {
      return allow_assignment_subquery_rows(*child);
    };
    return ExpressionIterator::iterate_child_expr(expr, mark_child);
  };

  for (const UpdateAssignmentSqlNode &assignment : update.assignments) {
    const FieldMeta *field_meta = table->table_meta().field(assignment.attribute_name.c_str());
    if (field_meta == nullptr) {
      LOG_WARN("no such field. table=%s, field=%s", table_name, assignment.attribute_name.c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }

    if (assignment.expression == nullptr) {
      LOG_WARN("update assignment expression is null. table=%s, field=%s", table_name, field_meta->name());
      return RC::INVALID_ARGUMENT;
    }

    unique_ptr<Expression> expression = assignment.expression->copy();
    vector<unique_ptr<Expression>> bound_expressions;
    rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to bind update expression. table=%s, field=%s, rc=%s", table_name, field_meta->name(), strrc(rc));
      return rc;
    }
    if (bound_expressions.size() != 1) {
      LOG_WARN("invalid update assignment expression count. table=%s, field=%s, count=%d",
          table_name, field_meta->name(), static_cast<int>(bound_expressions.size()));
      return RC::INVALID_ARGUMENT;
    }

    rc = reject_aggregate_expression(*bound_expressions[0]);
    if (OB_FAIL(rc)) {
      return rc;
    }
    rc = allow_assignment_subquery_rows(*bound_expressions[0]);
    if (OB_FAIL(rc)) {
      return rc;
    }

    field_metas.push_back(field_meta);
    expressions.emplace_back(std::move(bound_expressions[0]));
  }

  unordered_map<string, Table *> table_map;
  table_map.insert(pair<string, Table *>(string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  rc = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create update filter. rc=%s", strrc(rc));
    return rc;
  }

  stmt = new UpdateStmt(table, field_metas, std::move(expressions), filter_stmt);
  return RC::SUCCESS;
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  stmt = nullptr;

  const char *table_name = update.relation_name.c_str();
  if (db == nullptr || common::is_blank(table_name) || update.assignments.empty()) {
    LOG_WARN("invalid update statement. db=%p, table=%s, assignment_num=%d",
        db, table_name, static_cast<int>(update.assignments.size()));
    return RC::INVALID_ARGUMENT;
  }

  const ViewDefinition *view = db->find_view(table_name);
  if (view != nullptr) {
    return create_view_update_stmt(db, update, *view, stmt);
  }

  RC rc = create_table_update_stmt(db, update, db->find_table(table_name), stmt);
  if (OB_FAIL(rc)) {
    return rc;
  }

  const ViewDefinition *mirror_view = db->find_base_table_mirror_view(table_name);
  if (mirror_view == nullptr) {
    return RC::SUCCESS;
  }

  Table *view_table = db->find_table(mirror_view->view_name.c_str());
  if (view_table == nullptr) {
    delete stmt;
    stmt = nullptr;
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  Stmt *mirror_stmt = nullptr;
  rc = create_table_update_stmt(db, update, view_table, mirror_stmt);
  if (OB_FAIL(rc)) {
    delete stmt;
    stmt = nullptr;
    return rc;
  }

  auto *base_update_stmt   = static_cast<UpdateStmt *>(stmt);
  auto *mirror_update_stmt = static_cast<UpdateStmt *>(mirror_stmt);
  base_update_stmt->set_mirror_update(mirror_update_stmt->table(),
      mirror_update_stmt->take_field_metas(),
      mirror_update_stmt->take_expressions(),
      mirror_update_stmt->release_filter_stmt());
  delete mirror_update_stmt;
  return RC::SUCCESS;
}
