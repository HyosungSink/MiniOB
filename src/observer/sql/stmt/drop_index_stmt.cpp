/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved. */

#include "sql/stmt/drop_index_stmt.h"

#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

RC DropIndexStmt::create(Db *db, const DropIndexSqlNode &drop_index, Stmt *&stmt)
{
  stmt = nullptr;

  const char *table_name = drop_index.relation_name.c_str();
  const char *index_name = drop_index.index_name.c_str();
  if (db == nullptr || common::is_blank(table_name) || common::is_blank(index_name)) {
    return RC::INVALID_ARGUMENT;
  }

  Table *table = db->find_table(table_name);
  if (table == nullptr) {
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  if (table->find_index(index_name) == nullptr) {
    return RC::SCHEMA_INDEX_NAME_REPEAT;
  }

  stmt = new DropIndexStmt(table, drop_index.index_name);
  return RC::SUCCESS;
}
