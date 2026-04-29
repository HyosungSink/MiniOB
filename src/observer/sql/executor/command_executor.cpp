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
// Created by Wangyunlai on 2023/4/25.
//

#include "sql/executor/command_executor.h"
#include "common/log/log.h"
#include "event/sql_event.h"
#include "sql/executor/analyze_table_executor.h"
#include "sql/executor/create_index_executor.h"
#include "sql/executor/create_table_executor.h"
#include "sql/executor/desc_table_executor.h"
#include "sql/executor/drop_index_executor.h"
#include "sql/executor/drop_table_executor.h"
#include "sql/executor/help_executor.h"
#include "sql/executor/load_data_executor.h"
#include "sql/executor/set_variable_executor.h"
#include "sql/executor/show_tables_executor.h"
#include "sql/expr/expression.h"
#include "sql/optimizer/logical_plan_generator.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "sql/parser/parse.h"
#include "sql/stmt/select_stmt.h"
#include "sql/executor/trx_begin_executor.h"
#include "sql/executor/trx_end_executor.h"
#include "sql/stmt/stmt.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

static size_t view_attr_length(const Expression &expression)
{
  int length = expression.value_length();
  if (expression.value_type() == AttrType::CHARS && length <= 0) {
    return 4096;
  }
  if (expression.value_type() == AttrType::VECTORS && length > 0) {
    return static_cast<size_t>(length) / sizeof(float);
  }
  return length > 0 ? static_cast<size_t>(length) : 4;
}

static RC parse_view_select(const string &select_sql, ParsedSqlResult &sql_result, ParsedSqlNode *&select_node)
{
  RC rc = parse(select_sql.c_str(), &sql_result);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (sql_result.sql_nodes().size() != 1 || sql_result.sql_nodes()[0]->flag != SCF_SELECT) {
    return RC::SQL_SYNTAX;
  }

  select_node = sql_result.sql_nodes()[0].get();
  return RC::SUCCESS;
}

static RC create_materialized_view(SQLStageEvent *sql_event, const CreateViewStmt &create_view_stmt)
{
  Session *session = sql_event->session_event()->session();
  Db      *db      = session->get_current_db();

  ParsedSqlResult sql_result;
  ParsedSqlNode  *select_node = nullptr;
  RC rc = parse_view_select(create_view_stmt.select_sql(), sql_result, select_node);
  if (OB_FAIL(rc)) {
    return rc;
  }

  Stmt *stmt = nullptr;
  rc = SelectStmt::create(db, select_node->selection, stmt);
  if (OB_FAIL(rc)) {
    return rc;
  }
  unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt *>(stmt));

  vector<AttrInfoSqlNode> attrs;
  attrs.reserve(select_stmt->query_expressions().size());
  for (const unique_ptr<Expression> &expression : select_stmt->query_expressions()) {
    AttrInfoSqlNode attr;
    attr.name     = expression->name();
    attr.type     = expression->value_type();
    attr.length   = view_attr_length(*expression);
    attr.nullable = true;
    attrs.emplace_back(std::move(attr));
  }

  rc = db->create_table(create_view_stmt.relation_name().c_str(), span<const AttrInfoSqlNode>(attrs.data(), attrs.size()), {});
  if (OB_FAIL(rc)) {
    return rc;
  }

  LogicalPlanGenerator logical_plan_generator;
  unique_ptr<LogicalOperator> logical_operator;
  rc = logical_plan_generator.create(select_stmt.get(), logical_operator);
  if (OB_FAIL(rc)) {
    db->drop_table(create_view_stmt.relation_name().c_str());
    return rc;
  }

  PhysicalPlanGenerator physical_plan_generator;
  unique_ptr<PhysicalOperator> physical_operator;
  rc = physical_plan_generator.create(*logical_operator, physical_operator, session);
  if (OB_FAIL(rc)) {
    db->drop_table(create_view_stmt.relation_name().c_str());
    return rc;
  }

  Trx *trx = session->current_trx();
  trx->start_if_need();
  rc = physical_operator->open(trx);
  if (OB_FAIL(rc)) {
    db->drop_table(create_view_stmt.relation_name().c_str());
    return rc;
  }

  Table *view_table = db->find_table(create_view_stmt.relation_name().c_str());
  while ((rc = physical_operator->next()) == RC::SUCCESS) {
    Tuple *tuple = physical_operator->current_tuple();
    vector<Value> row;
    row.reserve(tuple->cell_num());
    for (int i = 0; i < tuple->cell_num(); i++) {
      Value value;
      RC cell_rc = tuple->cell_at(i, value);
      if (OB_FAIL(cell_rc)) {
        physical_operator->close();
        db->drop_table(create_view_stmt.relation_name().c_str());
        return cell_rc;
      }
      row.emplace_back(std::move(value));
    }

    Record record;
    rc = view_table->make_record(static_cast<int>(row.size()), row.data(), record);
    if (OB_FAIL(rc)) {
      physical_operator->close();
      db->drop_table(create_view_stmt.relation_name().c_str());
      return rc;
    }
    rc = view_table->insert_record(record);
    if (OB_FAIL(rc)) {
      physical_operator->close();
      db->drop_table(create_view_stmt.relation_name().c_str());
      return rc;
    }
  }

  RC close_rc = physical_operator->close();
  if (rc == RC::RECORD_EOF) {
    rc = close_rc;
  }
  if (OB_FAIL(rc)) {
    db->drop_table(create_view_stmt.relation_name().c_str());
  }
  return rc;
}

RC CommandExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();

  RC rc = RC::SUCCESS;
  switch (stmt->type()) {
    case StmtType::CREATE_INDEX: {
      CreateIndexExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::CREATE_TABLE: {
      CreateTableExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::CREATE_VIEW: {
      CreateViewStmt *create_view_stmt = static_cast<CreateViewStmt *>(stmt);
      rc = create_materialized_view(sql_event, *create_view_stmt);
    } break;

    case StmtType::DROP_TABLE: {
      DropTableExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::ALTER_TABLE: {
      AlterTableStmt *alter_table_stmt = static_cast<AlterTableStmt *>(stmt);
      rc = sql_event->session_event()->session()->get_current_db()->alter_table(alter_table_stmt->alter_table());
    } break;

    case StmtType::DROP_INDEX: {
      DropIndexExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::DESC_TABLE: {
      DescTableExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::ANALYZE_TABLE: {
      AnalyzeTableExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::HELP: {
      HelpExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::SHOW_TABLES: {
      ShowTablesExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::BEGIN: {
      TrxBeginExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::COMMIT:
    case StmtType::ROLLBACK: {
      TrxEndExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::SET_VARIABLE: {
      SetVariableExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::LOAD_DATA: {
      LoadDataExecutor executor;
      rc = executor.execute(sql_event);
    } break;

    case StmtType::EXIT: {
      rc = RC::SUCCESS;
    } break;

    default: {
      LOG_ERROR("unknown command: %d", static_cast<int>(stmt->type()));
      rc = RC::UNIMPLEMENTED;
    } break;
  }

  if (OB_SUCC(rc) && stmt_type_ddl(stmt->type())) {
    // 每次做完DDL之后，做一次sync，保证元数据与日志保持一致
    rc = sql_event->session_event()->session()->get_current_db()->sync();
    LOG_INFO("sync db after ddl. rc=%d", rc);
  }

  return rc;
}
