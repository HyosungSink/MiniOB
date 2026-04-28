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
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

UpdateStmt::UpdateStmt(
    Table *table, const vector<const FieldMeta *> &field_metas, const vector<Value> &values, FilterStmt *filter_stmt)
    : table_(table), field_metas_(field_metas), values_(values), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (filter_stmt_ != nullptr) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
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

  Table *table = db->find_table(table_name);
  if (table == nullptr) {
    LOG_WARN("no such table. db=%s, table=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  vector<const FieldMeta *> field_metas;
  vector<Value>             values;
  field_metas.reserve(update.assignments.size());
  values.reserve(update.assignments.size());

  RC rc = RC::SUCCESS;
  for (const UpdateAssignmentSqlNode &assignment : update.assignments) {
    const FieldMeta *field_meta = table->table_meta().field(assignment.attribute_name.c_str());
    if (field_meta == nullptr) {
      LOG_WARN("no such field. table=%s, field=%s", table_name, assignment.attribute_name.c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }

    Value value;
    if (assignment.value.attr_type() == field_meta->type()) {
      value = assignment.value;
    } else {
      rc = Value::cast_to(assignment.value, field_meta->type(), value);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to cast update value. table=%s, field=%s, rc=%s", table_name, field_meta->name(), strrc(rc));
        return rc;
      }
    }

    field_metas.push_back(field_meta);
    values.push_back(value);
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

  stmt = new UpdateStmt(table, field_metas, values, filter_stmt);
  return RC::SUCCESS;
}
