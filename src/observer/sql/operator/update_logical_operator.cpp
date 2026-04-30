/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the
Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/update_logical_operator.h"

UpdateLogicalOperator::UpdateLogicalOperator(
    Table *table, const vector<const FieldMeta *> &field_metas, vector<unique_ptr<Expression>> &&expressions)
    : table_(table), field_metas_(field_metas), expressions_(std::move(expressions))
{}

void UpdateLogicalOperator::set_mirror_update(
    Table *table, vector<const FieldMeta *> &&field_metas, vector<unique_ptr<Expression>> &&expressions)
{
  mirror_table_       = table;
  mirror_field_metas_ = std::move(field_metas);
  mirror_expressions_ = std::move(expressions);
}
