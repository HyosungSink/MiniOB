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
// Created by Wangyunlai on 2023/6/14.
//

#include "sql/executor/desc_table_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/operator/string_list_physical_operator.h"
#include "sql/stmt/desc_table_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

using namespace std;

static string mysql_desc_type(const FieldMeta &field_meta)
{
  switch (field_meta.type()) {
    case AttrType::INTS: return "INT";
    case AttrType::FLOATS: return "FLOAT";
    case AttrType::DATES: return "DATE";
    case AttrType::CHARS: return string("CHAR(") + to_string(field_meta.len()) + ")";
    case AttrType::VECTORS: return "VECTOR";
    case AttrType::BOOLEANS: return "BOOLEAN";
    default: return attr_type_to_string(field_meta.type());
  }
}

static string mysql_desc_key(const TableMeta &table_meta, const FieldMeta &field_meta)
{
  for (int i = 0; i < table_meta.index_num(); i++) {
    const IndexMeta *index_meta = table_meta.index(i);
    for (const string &field : index_meta->fields()) {
      if (field == field_meta.name()) {
        return index_meta->is_unique() ? "UNI" : "MUL";
      }
    }
  }
  return "";
}

RC DescTableExecutor::execute(SQLStageEvent *sql_event)
{
  RC            rc            = RC::SUCCESS;
  Stmt         *stmt          = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();
  Session      *session       = session_event->session();
  ASSERT(stmt->type() == StmtType::DESC_TABLE,
      "desc table executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  DescTableStmt *desc_table_stmt = static_cast<DescTableStmt *>(stmt);
  SqlResult     *sql_result      = session_event->sql_result();
  const char    *table_name      = desc_table_stmt->table_name().c_str();

  Db    *db    = session->get_current_db();
  Table *table = db->find_table(table_name);
  if (table != nullptr) {
    TupleSchema tuple_schema;
    tuple_schema.append_cell(TupleCellSpec("", "Field", "Field"));
    tuple_schema.append_cell(TupleCellSpec("", "Type", "Type"));
    tuple_schema.append_cell(TupleCellSpec("", "Null", "Null"));
    tuple_schema.append_cell(TupleCellSpec("", "Key", "Key"));
    tuple_schema.append_cell(TupleCellSpec("", "Default", "Default"));
    tuple_schema.append_cell(TupleCellSpec("", "Extra", "Extra"));

    sql_result->set_tuple_schema(tuple_schema);

    auto             oper       = new StringListPhysicalOperator;
    const TableMeta &table_meta = table->table_meta();
    for (int i = table_meta.sys_field_num(); i < table_meta.field_num(); i++) {
      const FieldMeta *field_meta = table_meta.field(i);
      oper->append({field_meta->name(),
          mysql_desc_type(*field_meta),
          field_meta->nullable() ? "YES" : "NO",
          mysql_desc_key(table_meta, *field_meta),
          "NULL",
          ""});
    }

    sql_result->set_operator(unique_ptr<PhysicalOperator>(oper));
  } else {

    sql_result->set_return_code(RC::SCHEMA_TABLE_NOT_EXIST);
    sql_result->set_state_string("Table not exists");
  }
  return rc;
}
