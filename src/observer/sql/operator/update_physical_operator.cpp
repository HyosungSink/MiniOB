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

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/table/table_meta.h"
#include "storage/field/field_meta.h"
#include "storage/trx/trx.h"
#include "storage/record/record.h"
#include "sql/expr/tuple.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const FieldMeta *field_meta, const Value &value)
    : table_(table), field_meta_(field_meta), value_(value)
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  unique_ptr<PhysicalOperator> &child = children_[0];

  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  // Collect all records first, then close child scan before modifying
  // (same pattern as DeletePhysicalOperator to avoid B+ tree latch ordering issues)
  vector<Record> old_records;
  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record    old_record = row_tuple->record();
    old_records.emplace_back(std::move(old_record));
  }

  child->close();

  // Now perform updates after releasing scan latches
  int record_size = table_->table_meta().record_size();
  for (Record &old_record : old_records) {
    char *record_data = (char *)malloc(record_size);
    memcpy(record_data, old_record.data(), record_size);

    size_t copy_len = field_meta_->len();
    size_t data_len = value_.length();
    if (field_meta_->type() == AttrType::CHARS) {
      if (copy_len > data_len) {
        copy_len = data_len + 1;
      }
    }
    memcpy(record_data + field_meta_->offset(), value_.data(), copy_len);

    Record new_record;
    new_record.set_data_owner(record_data, record_size);

    rc = trx_->update_record(table_, old_record, new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  return RC::SUCCESS;
}
