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

#include "sql/operator/update_physical_operator.h"
#include "common/lang/algorithm.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

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
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (tuple == nullptr) {
      LOG_WARN("failed to get current tuple");
      child->close();
      return RC::INTERNAL;
    }

    RowTuple *row_tuple  = static_cast<RowTuple *>(tuple);
    Record   &old_record = row_tuple->record();

    Record copied_record;
    copied_record.set_rid(old_record.rid());
    rc = copied_record.copy_data(old_record.data(), old_record.len());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to copy record before update. rc=%s", strrc(rc));
      child->close();
      return rc;
    }
    records_.emplace_back(std::move(copied_record));
  }

  RC close_rc = child->close();
  if (rc == RC::RECORD_EOF) {
    rc = close_rc;
  }
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to scan records for update. rc=%s", strrc(rc));
    return rc;
  }

  for (Record &old_record : records_) {
    Record new_record;
    rc = make_updated_record(old_record, new_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to make updated record. rc=%s", strrc(rc));
      return rc;
    }

    rc = trx->delete_record(table_, old_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to delete old record while updating. rc=%s", strrc(rc));
      return rc;
    }

    rc = trx->insert_record(table_, new_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to insert updated record. rc=%s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::make_updated_record(const Record &old_record, Record &new_record) const
{
  RC rc = new_record.copy_data(old_record.data(), old_record.len());
  if (OB_FAIL(rc)) {
    return rc;
  }
  new_record.set_rid(old_record.rid());

  rc = new_record.reset_filed(field_meta_->offset(), field_meta_->len());
  if (OB_FAIL(rc)) {
    return rc;
  }

  int copy_len = value_.length();
  if (field_meta_->type() == AttrType::CHARS) {
    copy_len = min(field_meta_->len(), value_.length() + 1);
  }
  copy_len = min(copy_len, field_meta_->len());

  return new_record.set_field(field_meta_->offset(), copy_len, value_.data());
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  records_.clear();
  return RC::SUCCESS;
}
