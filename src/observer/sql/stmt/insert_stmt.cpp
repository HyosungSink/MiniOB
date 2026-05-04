/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
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

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

static RC field_index(const TableMeta &table_meta, const char *field_name, int &index)
{
  for (int i = table_meta.sys_field_num(); i < table_meta.field_num(); i++) {
    const FieldMeta *field = table_meta.field(i);
    if (0 == strcmp(field->name(), field_name)) {
      index = i - table_meta.sys_field_num();
      return RC::SUCCESS;
    }
  }
  return RC::SCHEMA_FIELD_NOT_EXIST;
}

static RC view_row_matches(Table *base_table, const ViewDefinition &view, const vector<Value> &base_row, bool &matches)
{
  matches = true;
  if (view.predicates.empty()) {
    return RC::SUCCESS;
  }

  Record record;
  RC rc = base_table->make_record(static_cast<int>(base_row.size()), base_row.data(), record);
  if (OB_FAIL(rc)) {
    return rc;
  }

  RowTuple tuple;
  tuple.set_schema(base_table, base_table->table_meta().field_metas());
  tuple.set_record(&record);

  bool initialized = false;
  bool result      = true;
  for (const ViewPredicate &predicate : view.predicates) {
    Value value;
    rc = predicate.expression->get_value(tuple, value);
    if (OB_FAIL(rc)) {
      return rc;
    }

    const bool current = !value.is_null() && value.get_boolean();
    if (!initialized) {
      result = current;
      initialized = true;
    } else if (predicate.conjunction == ConditionConjunction::OR) {
      result = result || current;
    } else {
      result = result && current;
    }
  }

  matches = initialized ? result : true;
  return RC::SUCCESS;
}

static RC project_base_row_to_view(
    Table *base_table, Table *view_table, const ViewDefinition &view, const vector<Value> &base_row, vector<Value> &view_row)
{
  const TableMeta &base_meta = base_table->table_meta();
  const TableMeta &view_meta = view_table->table_meta();
  const int        view_field_num = view_meta.field_num() - view_meta.sys_field_num();

  view_row.assign(view_field_num, Value());
  for (Value &value : view_row) {
    value.set_null();
  }

  for (const ViewColumnMapping &column : view.columns) {
    int base_index = -1;
    RC rc = field_index(base_meta, column.base_column.c_str(), base_index);
    if (OB_FAIL(rc)) {
      return rc;
    }

    int view_index = -1;
    rc = field_index(view_meta, column.view_column.c_str(), view_index);
    if (OB_FAIL(rc)) {
      return rc;
    }
    if (base_index < 0 || base_index >= static_cast<int>(base_row.size())) {
      return RC::SCHEMA_FIELD_MISSING;
    }
    view_row[view_index] = base_row[base_index];
  }
  return RC::SUCCESS;
}

static RC create_view_insert_stmt(Db *db, const InsertSqlNode &inserts, const ViewDefinition &view, Stmt *&stmt)
{
  if (!view.updatable) {
    if (!view.materialized_insertable) {
      return RC::INVALID_ARGUMENT;
    }

    Table *view_table = db->find_table(view.view_name.c_str());
    if (view_table == nullptr) {
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    vector<vector<Value>> single_row;
    const vector<vector<Value>> *value_rows = &inserts.value_rows;
    if (value_rows->empty() && !inserts.values.empty()) {
      single_row.emplace_back(inserts.values);
      value_rows = &single_row;
    }

    const TableMeta &view_table_meta = view_table->table_meta();
    const int        view_field_num  = view_table_meta.field_num() - view_table_meta.sys_field_num();
    vector<vector<Value>> view_rows;
    view_rows.reserve(value_rows->size());
    for (const vector<Value> &row : *value_rows) {
      if (inserts.attribute_names.empty()) {
        if (view_field_num != static_cast<int>(row.size())) {
          return RC::SCHEMA_FIELD_MISSING;
        }
        view_rows.emplace_back(row);
        continue;
      }

      if (inserts.attribute_names.size() != row.size()) {
        return RC::SCHEMA_FIELD_MISSING;
      }

      vector<Value> view_row(view_field_num);
      for (Value &value : view_row) {
        value.set_null();
      }
      for (size_t i = 0; i < row.size(); i++) {
        int view_index = -1;
        RC rc = field_index(view_table_meta, inserts.attribute_names[i].c_str(), view_index);
        if (OB_FAIL(rc)) {
          return rc;
        }
        view_row[view_index] = row[i];
      }
      view_rows.emplace_back(std::move(view_row));
    }

    stmt = new InsertStmt(view_table, view_rows);
    return RC::SUCCESS;
  }

  Table *base_table = db->find_table(view.base_table_name.c_str());
  if (base_table == nullptr) {
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  Table *view_table = db->find_table(view.view_name.c_str());
  if (view_table == nullptr) {
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  vector<vector<Value>> single_row;
  const vector<vector<Value>> *value_rows = &inserts.value_rows;
  if (value_rows->empty() && !inserts.values.empty()) {
    single_row.emplace_back(inserts.values);
    value_rows = &single_row;
  }

  const TableMeta &table_meta = base_table->table_meta();
  const int        field_num  = table_meta.field_num() - table_meta.sys_field_num();
  const TableMeta &view_table_meta = view_table->table_meta();
  const int        view_field_num  = view_table_meta.field_num() - view_table_meta.sys_field_num();
  vector<string>   insert_columns = inserts.attribute_names;
  if (insert_columns.empty()) {
    for (const ViewColumnMapping &column : view.columns) {
      insert_columns.push_back(column.view_column);
    }
  }

  vector<vector<Value>> base_rows;
  vector<vector<Value>> view_rows;
  base_rows.reserve(value_rows->size());
  view_rows.reserve(value_rows->size());
  for (const vector<Value> &row : *value_rows) {
    if (insert_columns.size() != row.size()) {
      return RC::SCHEMA_FIELD_MISSING;
    }

    vector<Value> base_row(field_num);
    for (Value &value : base_row) {
      value.set_null();
    }
    vector<Value> view_row(view_field_num);
    for (Value &value : view_row) {
      value.set_null();
    }

    for (size_t i = 0; i < row.size(); i++) {
      const string *base_column = view.base_column_for(insert_columns[i]);
      if (base_column == nullptr) {
        return RC::SCHEMA_FIELD_NOT_EXIST;
      }

      int index = -1;
      RC rc = field_index(table_meta, base_column->c_str(), index);
      if (OB_FAIL(rc)) {
        return rc;
      }
      base_row[index] = row[i];

      int view_index = -1;
      rc = field_index(view_table_meta, insert_columns[i].c_str(), view_index);
      if (OB_FAIL(rc)) {
        return rc;
      }
      view_row[view_index] = row[i];
    }
    base_rows.emplace_back(std::move(base_row));

    bool matches = true;
    RC rc = view_row_matches(base_table, view, base_rows.back(), matches);
    if (OB_FAIL(rc)) {
      return rc;
    }
    if (matches) {
      view_rows.emplace_back(std::move(view_row));
    }
  }

  auto insert_stmt = new InsertStmt(base_table, base_rows);
  insert_stmt->set_mirror_insert(view_table, std::move(view_rows));
  stmt = insert_stmt;
  return RC::SUCCESS;
}

InsertStmt::InsertStmt(Table *table, const vector<vector<Value>> &value_rows)
    : table_(table), value_rows_(value_rows)
{
  if (!value_rows_.empty()) {
    values_       = value_rows_.front().data();
    value_amount_ = static_cast<int>(value_rows_.front().size());
  }
}

void InsertStmt::set_mirror_insert(Table *table, vector<vector<Value>> &&value_rows)
{
  mirror_table_      = table;
  mirror_value_rows_ = std::move(value_rows);
}

RC InsertStmt::create(Db *db, const InsertSqlNode &inserts, Stmt *&stmt)
{
  const char *table_name = inserts.relation_name.c_str();
  vector<vector<Value>> single_row;
  const vector<vector<Value>> *value_rows = &inserts.value_rows;
  if (value_rows->empty() && !inserts.values.empty()) {
    single_row.emplace_back(inserts.values);
    value_rows = &single_row;
  }

  if (nullptr == db || nullptr == table_name || value_rows->empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, inserts.values.empty() ? 0 : static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  const ViewDefinition *view = db->find_view(table_name);
  if (view != nullptr) {
    return create_view_insert_stmt(db, inserts, *view, stmt);
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check the fields number
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num() - table_meta.sys_field_num();
  for (const vector<Value> &row : *value_rows) {
    if (field_num != static_cast<int>(row.size())) {
      LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", static_cast<int>(row.size()), field_num);
      return RC::SCHEMA_FIELD_MISSING;
    }
  }

  // everything alright
  auto insert_stmt = new InsertStmt(table, *value_rows);
  const ViewDefinition *mirror_view = db->find_base_table_mirror_view(table_name);
  if (mirror_view != nullptr) {
    Table *view_table = db->find_table(mirror_view->view_name.c_str());
    if (view_table == nullptr) {
      delete insert_stmt;
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    vector<vector<Value>> mirror_rows;
    mirror_rows.reserve(value_rows->size());
    for (const vector<Value> &row : *value_rows) {
      bool matches = true;
      RC rc = view_row_matches(table, *mirror_view, row, matches);
      if (OB_FAIL(rc)) {
        delete insert_stmt;
        return rc;
      }
      if (!matches) {
        continue;
      }

      vector<Value> mirror_row;
      rc = project_base_row_to_view(table, view_table, *mirror_view, row, mirror_row);
      if (OB_FAIL(rc)) {
        delete insert_stmt;
        return rc;
      }
      mirror_rows.emplace_back(std::move(mirror_row));
    }
    insert_stmt->set_mirror_insert(view_table, std::move(mirror_rows));
  }
  stmt = insert_stmt;
  return RC::SUCCESS;
}
