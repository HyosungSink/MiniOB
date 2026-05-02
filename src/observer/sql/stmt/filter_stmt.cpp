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

#include "sql/stmt/filter_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/sys/rc.h"
#include "sql/expr/expression.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

FilterStmt::~FilterStmt()
{
  for (FilterUnit *unit : filter_units_) {
    delete unit;
  }
  filter_units_.clear();
}

RC FilterStmt::create(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
    const ConditionSqlNode *conditions, int condition_num, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt  = nullptr;

  FilterStmt *tmp_stmt = new FilterStmt();
  for (int i = 0; i < condition_num; i++) {
    FilterUnit *filter_unit = nullptr;

    rc = create_filter_unit(db, default_table, tables, conditions[i], filter_unit);
    if (rc != RC::SUCCESS) {
      delete tmp_stmt;
      LOG_WARN("failed to create filter unit. condition index=%d", i);
      return rc;
    }
    tmp_stmt->filter_units_.push_back(filter_unit);
  }

  stmt = tmp_stmt;
  return rc;
}

RC get_table_and_field(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
    const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field)
{
  if (common::is_blank(attr.relation_name.c_str())) {
    table = default_table;
  } else if (nullptr != tables) {
    auto iter = tables->find(attr.relation_name);
    if (iter != tables->end()) {
      table = iter->second;
    }
  } else {
    table = db->find_table(attr.relation_name.c_str());
  }
  if (nullptr == table) {
    LOG_WARN("No such table: attr.relation_name: %s", attr.relation_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  field = table->table_meta().field(attr.attribute_name.c_str());
  if (nullptr == field) {
    LOG_WARN("no such field in table: table %s, field %s", table->name(), attr.attribute_name.c_str());
    table = nullptr;
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  return RC::SUCCESS;
}

RC FilterStmt::create_filter_unit(Db *db, Table *default_table, unordered_map<string, Table *> *tables,
    const ConditionSqlNode &condition, FilterUnit *&filter_unit)
{
  RC rc = RC::SUCCESS;

  CompOp comp = condition.comp;
  if (comp < EQUAL_TO || comp >= NO_OP) {
    LOG_WARN("invalid compare operator : %d", comp);
    return RC::INVALID_ARGUMENT;
  }

  filter_unit = new FilterUnit;

  if (condition.has_expressions) {
    // Expression-based condition (arithmetic in WHERE)
    // Try to convert simple expressions to old format for backward compatibility
    auto try_convert_simple = [&](Expression *expr, FilterObj &obj) -> RC {
      if (expr == nullptr) {
        obj.init_value(Value());
        return RC::SUCCESS;
      }
      if (expr->type() == ExprType::UNBOUND_FIELD) {
        auto *uf = static_cast<UnboundFieldExpr *>(expr);
        RelAttrSqlNode attr;
        attr.relation_name = uf->table_name() ? uf->table_name() : "";
        attr.attribute_name = uf->field_name();
        Table           *table = nullptr;
        const FieldMeta *field = nullptr;
        RC r = get_table_and_field(db, default_table, tables, attr, table, field);
        if (r == RC::SUCCESS) {
          obj.init_attr(Field(table, field));
          delete expr;
          return RC::SUCCESS;
        }
      } else if (expr->type() == ExprType::VALUE) {
        auto *ve = static_cast<ValueExpr *>(expr);
        Value val;
        ve->try_get_value(val);
        obj.init_value(val);
        delete expr;
        return RC::SUCCESS;
      } else if (expr->type() == ExprType::FIELD) {
        auto *fe = static_cast<FieldExpr *>(expr);
        obj.init_attr(fe->field());
        delete expr;
        return RC::SUCCESS;
      }
      // Complex expression - keep as expression
      obj.init_expression(unique_ptr<Expression>(expr));
      return RC::SUCCESS;
    };

    FilterObj left_obj;
    rc = try_convert_simple(condition.left_expr, left_obj);
    if (rc != RC::SUCCESS) return rc;
    const_cast<ConditionSqlNode &>(condition).left_expr = nullptr;
    filter_unit->set_left(left_obj);

    FilterObj right_obj;
    rc = try_convert_simple(condition.right_expr, right_obj);
    if (rc != RC::SUCCESS) return rc;
    const_cast<ConditionSqlNode &>(condition).right_expr = nullptr;
    filter_unit->set_right(right_obj);
  } else {
    if (condition.left_is_attr) {
      Table           *table = nullptr;
      const FieldMeta *field = nullptr;
      rc                     = get_table_and_field(db, default_table, tables, condition.left_attr, table, field);
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot find attr");
        return rc;
      }
      FilterObj filter_obj;
      filter_obj.init_attr(Field(table, field));
      filter_unit->set_left(filter_obj);
    } else {
      FilterObj filter_obj;
      filter_obj.init_value(condition.left_value);
      filter_unit->set_left(filter_obj);
    }

    if (condition.right_is_attr) {
      Table           *table = nullptr;
      const FieldMeta *field = nullptr;
      rc                     = get_table_and_field(db, default_table, tables, condition.right_attr, table, field);
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot find attr");
        return rc;
      }
      FilterObj filter_obj;
      filter_obj.init_attr(Field(table, field));
      filter_unit->set_right(filter_obj);
    } else {
      FilterObj filter_obj;
      filter_obj.init_value(condition.right_value);
      filter_unit->set_right(filter_obj);
    }
  }

  filter_unit->set_comp(comp);
  filter_unit->set_is_or(condition.is_or);

  return rc;
}
