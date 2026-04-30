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
// Created by Wangyunlai on 2022/5/27.
//

#pragma once

#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"

class Table;
class FilterStmt;

/**
 * @brief Delete 语句
 * @ingroup Statement
 */
class DeleteStmt : public Stmt
{
public:
  DeleteStmt(Table *table, FilterStmt *filter_stmt);
  ~DeleteStmt() override;

  Table      *table() const { return table_; }
  FilterStmt *filter_stmt() const { return filter_stmt_; }
  void        set_mirror_delete(Table *table, FilterStmt *filter_stmt);
  Table      *mirror_table() const { return mirror_table_; }
  FilterStmt *mirror_filter_stmt() const { return mirror_filter_stmt_; }
  FilterStmt *release_filter_stmt()
  {
    FilterStmt *filter_stmt = filter_stmt_;
    filter_stmt_ = nullptr;
    return filter_stmt;
  }

  StmtType type() const override { return StmtType::DELETE; }

public:
  static RC create(Db *db, const DeleteSqlNode &delete_sql, Stmt *&stmt);

private:
  Table      *table_       = nullptr;
  FilterStmt *filter_stmt_ = nullptr;
  Table      *mirror_table_       = nullptr;
  FilterStmt *mirror_filter_stmt_ = nullptr;
};
