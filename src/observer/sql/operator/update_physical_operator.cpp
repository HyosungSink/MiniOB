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
#include "sql/expr/tuple.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(
    Table *table, const vector<const FieldMeta *> &field_metas, vector<unique_ptr<Expression>> &&expressions)
    : table_(table), field_metas_(field_metas), expressions_(std::move(expressions))
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  for (const unique_ptr<Expression> &expression : expressions_) {
    RC rc = expression->prepare();
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to prepare update expression. rc=%s", strrc(rc));
      return rc;
    }
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

  vector<Record> new_records;
  new_records.reserve(records_.size());
  for (Record &old_record : records_) {
    Record new_record;
    rc = make_updated_record(old_record, new_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to make updated record. rc=%s", strrc(rc));
      return rc;
    }

    rc = table_->validate_unique_constraints(new_record, &old_record.rid());
    if (OB_FAIL(rc)) {
      LOG_WARN("unique constraint check failed while updating. rc=%s", strrc(rc));
      return rc;
    }
    new_records.emplace_back(std::move(new_record));
  }

  for (size_t i = 0; i < records_.size(); i++) {
    Record &old_record = records_[i];
    Record &new_record = new_records[i];

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

  RowTuple row_tuple;
  row_tuple.set_record(const_cast<Record *>(&old_record));
  row_tuple.set_schema(table_, table_->table_meta().field_metas());

  for (size_t i = 0; i < field_metas_.size(); i++) {
    const FieldMeta *field_meta = field_metas_[i];
    const Expression *expression = expressions_[i].get();

    Value value;
    rc = expression->get_value(row_tuple, value);
    if (OB_FAIL(rc)) {
      return rc;
    }

    rc = new_record.reset_filed(field_meta->offset(), field_meta->len());
    if (OB_FAIL(rc)) {
      return rc;
    }

    if (value.is_null()) {
      if (!field_meta->nullable()) {
        LOG_WARN("field can not be null. table name:%s, field name:%s", table_->name(), field_meta->name());
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      Value::set_null_data(new_record.data() + field_meta->offset(), field_meta->len(), field_meta->type());
      continue;
    }

    Value stored_value;
    const Value *value_to_store = &value;
    if (field_meta->type() != value.attr_type()) {
      rc = Value::cast_to(value, field_meta->type(), stored_value);
      if (OB_FAIL(rc)) {
        return rc;
      }
      value_to_store = &stored_value;
    }

    if (field_meta->type() == AttrType::VECTORS && value_to_store->length() != field_meta->len()) {
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    if (field_meta->type() == AttrType::CHARS && value_to_store->length() > field_meta->len()) {
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    if (field_meta->type() == AttrType::TEXTS && value_to_store->length() > TEXT_MAX_LENGTH) {
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }

    if (field_meta->type() == AttrType::TEXTS) {
      rc = table->set_value_to_record(new_record.data(), *value_to_store, field_meta);
      if (OB_FAIL(rc)) {
        return rc;
      }
      continue;
    }

    int copy_len = value_to_store->length();
    if (field_meta->type() == AttrType::CHARS) {
      copy_len = min(field_meta->len(), value_to_store->length() + 1);
    }
    copy_len = min(copy_len, field_meta->len());

    rc = new_record.set_field(field_meta->offset(), copy_len, value_to_store->data());
    if (OB_FAIL(rc)) {
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
  records_.clear();
  return RC::SUCCESS;
}
