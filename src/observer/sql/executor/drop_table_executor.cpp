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

#include "sql/executor/drop_table_executor.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/stmt/drop_table_stmt.h"
#include "storage/db/db.h"

RC DropTableExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt = sql_event->stmt();
  Db      *db   = nullptr;

  DropTableStmt *drop_table_stmt = static_cast<DropTableStmt *>(stmt);
  db = drop_table_stmt->db();

  RC rc = db->drop_table(drop_table_stmt->table_name().c_str());
  return rc;
}
