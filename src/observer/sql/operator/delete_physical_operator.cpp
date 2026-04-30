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
// Created by WangYunlai on 2022/6/27.
//

#include "sql/operator/delete_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC DeletePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  unique_ptr<PhysicalOperator> &child = children_[0];
  trx_ = trx;
  RC rc = delete_records(table_, *child, trx);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (mirror_table_ != nullptr) {
    if (children_.size() < 2) {
      LOG_WARN("mirror delete is missing scan child");
      return RC::INTERNAL;
    }
    rc = delete_records(mirror_table_, *children_[1], trx);
    if (OB_FAIL(rc)) {
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC DeletePhysicalOperator::delete_records(Table *table, PhysicalOperator &child, Trx *trx)
{
  vector<Record> records;

  RC rc = child.open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = child.next())) {
    Tuple *tuple = child.current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    records.emplace_back(std::move(record));
  }

  child.close();

  // 先收集记录再删除
  // 记录的有效性由事务来保证，如果事务不保证删除的有效性，那说明此事务类型不支持并发控制，比如VacuousTrx
  for (Record &record : records) {
    rc = trx->delete_record(table, record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC DeletePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC DeletePhysicalOperator::close()
{
  return RC::SUCCESS;
}
