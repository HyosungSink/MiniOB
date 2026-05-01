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
// Created for MiniOB competition
//

#include "sql/stmt/drop_table_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"

RC DropTableStmt::create(Db *db, const DropTableSqlNode &drop_table_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db=%p", db);
    return RC::INVALID_ARGUMENT;
  }

  stmt = new DropTableStmt(db, drop_table_sql.relation_name);
  return RC::SUCCESS;
}
