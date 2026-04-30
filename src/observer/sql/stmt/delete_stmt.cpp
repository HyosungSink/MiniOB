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

#include "sql/stmt/delete_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

static unique_ptr<Expression> copy_rewrite_view_delete_expr(const Expression &expr, const ViewDefinition &view)
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

static RC create_table_delete_stmt(Db *db, const DeleteSqlNode &delete_sql, Table *table, Stmt *&stmt);

static RC create_view_delete_stmt(Db *db, const DeleteSqlNode &delete_sql, const ViewDefinition &view, Stmt *&stmt)
{
  if (!view.updatable) {
    return RC::INVALID_ARGUMENT;
  }

  Table *view_table = db->find_table(view.view_name.c_str());
  if (view_table == nullptr) {
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  DeleteSqlNode base_delete;
  base_delete.relation_name = view.base_table_name;
  for (const ConditionSqlNode &condition : delete_sql.conditions) {
    ConditionSqlNode base_condition;
    base_condition.conjunction = condition.conjunction;
    base_condition.comp        = condition.comp;
    if (condition.left_expr != nullptr) {
      base_condition.left_expr = copy_rewrite_view_delete_expr(*condition.left_expr, view);
    }
    if (condition.right_expr != nullptr) {
      base_condition.right_expr = copy_rewrite_view_delete_expr(*condition.right_expr, view);
    }
    base_delete.conditions.emplace_back(std::move(base_condition));
  }

  Stmt *base_stmt = nullptr;
  RC rc = create_table_delete_stmt(db, base_delete, db->find_table(view.base_table_name.c_str()), base_stmt);
  if (OB_FAIL(rc)) {
    return rc;
  }

  Stmt *mirror_stmt = nullptr;
  rc = create_table_delete_stmt(db, delete_sql, view_table, mirror_stmt);
  if (OB_FAIL(rc)) {
    delete base_stmt;
    return rc;
  }

  auto *base_delete_stmt   = static_cast<DeleteStmt *>(base_stmt);
  auto *mirror_delete_stmt = static_cast<DeleteStmt *>(mirror_stmt);
  base_delete_stmt->set_mirror_delete(mirror_delete_stmt->table(), mirror_delete_stmt->release_filter_stmt());
  delete mirror_delete_stmt;

  stmt = base_delete_stmt;
  return RC::SUCCESS;
}

DeleteStmt::DeleteStmt(Table *table, FilterStmt *filter_stmt) : table_(table), filter_stmt_(filter_stmt) {}

DeleteStmt::~DeleteStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
  if (nullptr != mirror_filter_stmt_) {
    delete mirror_filter_stmt_;
    mirror_filter_stmt_ = nullptr;
  }
}

void DeleteStmt::set_mirror_delete(Table *table, FilterStmt *filter_stmt)
{
  mirror_table_       = table;
  mirror_filter_stmt_ = filter_stmt;
}

static RC create_table_delete_stmt(Db *db, const DeleteSqlNode &delete_sql, Table *table, Stmt *&stmt)
{
  const char *table_name = delete_sql.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  unordered_map<string, Table *> table_map;
  table_map.insert(pair<string, Table *>(string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(
      db, table, &table_map, delete_sql.conditions.data(), static_cast<int>(delete_sql.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  stmt = new DeleteStmt(table, filter_stmt);
  return rc;
}

RC DeleteStmt::create(Db *db, const DeleteSqlNode &delete_sql, Stmt *&stmt)
{
  const char *table_name = delete_sql.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  const ViewDefinition *view = db->find_view(table_name);
  if (view != nullptr) {
    return create_view_delete_stmt(db, delete_sql, *view, stmt);
  }

  RC rc = create_table_delete_stmt(db, delete_sql, db->find_table(table_name), stmt);
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
  rc = create_table_delete_stmt(db, delete_sql, view_table, mirror_stmt);
  if (OB_FAIL(rc)) {
    delete stmt;
    stmt = nullptr;
    return rc;
  }

  auto *base_delete_stmt   = static_cast<DeleteStmt *>(stmt);
  auto *mirror_delete_stmt = static_cast<DeleteStmt *>(mirror_stmt);
  base_delete_stmt->set_mirror_delete(mirror_delete_stmt->table(), mirror_delete_stmt->release_filter_stmt());
  delete mirror_delete_stmt;
  return RC::SUCCESS;
}
