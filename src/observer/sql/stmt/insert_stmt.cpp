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
#include "storage/db/db.h"
#include "storage/table/table.h"

InsertStmt::InsertStmt(Table *table, const vector<vector<Value>> &value_rows)
    : table_(table), value_rows_(value_rows)
{
  if (!value_rows_.empty()) {
    values_       = value_rows_.front().data();
    value_amount_ = static_cast<int>(value_rows_.front().size());
  }
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
  stmt = new InsertStmt(table, *value_rows);
  return RC::SUCCESS;
}
