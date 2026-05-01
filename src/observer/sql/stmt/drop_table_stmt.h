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

#pragma once

#include "sql/stmt/stmt.h"

class Db;

class DropTableStmt : public Stmt
{
public:
  DropTableStmt(Db *db, const string &table_name) : db_(db), table_name_(table_name) {}
  virtual ~DropTableStmt() = default;

  StmtType type() const override { return StmtType::DROP_TABLE; }

  Db           *db() const { return db_; }
  const string &table_name() const { return table_name_; }

  static RC create(Db *db, const DropTableSqlNode &drop_table_sql, Stmt *&stmt);

private:
  Db    *db_;
  string table_name_;
};
