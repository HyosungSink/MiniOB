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

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table,
    const vector<const FieldMeta *> &field_metas,
    vector<unique_ptr<Expression>> &&expressions,
    Table *mirror_table,
    const vector<const FieldMeta *> &mirror_field_metas,
    vector<unique_ptr<Expression>> &&mirror_expressions,
    const vector<const FieldMeta *> &base_match_field_metas,
    const vector<const FieldMeta *> &mirror_match_field_metas)
    : table_(table),
      field_metas_(field_metas),
      expressions_(std::move(expressions)),
      mirror_table_(mirror_table),
      mirror_field_metas_(mirror_field_metas),
      mirror_expressions_(std::move(mirror_expressions)),
      base_match_field_metas_(base_match_field_metas),
      mirror_match_field_metas_(mirror_match_field_metas)
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
  RC rc = RC::SUCCESS;
  vector<vector<Value>> allowed_keys;
  const bool restrict_base_by_mirror = mirror_table_ != nullptr && !base_match_field_metas_.empty();
  if (restrict_base_by_mirror) {
    if (children_.size() < 2 || base_match_field_metas_.size() != mirror_match_field_metas_.size()) {
      LOG_WARN("invalid mirror-driven update match fields");
      return RC::INTERNAL;
    }
    rc = collect_match_keys(mirror_table_, mirror_match_field_metas_, *children_[1], trx, allowed_keys);
    if (OB_FAIL(rc)) {
      return rc;
    }
  }

  rc = update_records(table_,
      field_metas_,
      expressions_,
      *child,
      trx,
      restrict_base_by_mirror ? &allowed_keys : nullptr,
      restrict_base_by_mirror ? &base_match_field_metas_ : nullptr);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (mirror_table_ != nullptr) {
    for (const unique_ptr<Expression> &expression : mirror_expressions_) {
      rc = expression->prepare();
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to prepare mirror update expression. rc=%s", strrc(rc));
        return rc;
      }
    }

    if (children_.size() < 2) {
      LOG_WARN("mirror update is missing scan child");
      return RC::INTERNAL;
    }
    rc = update_records(mirror_table_, mirror_field_metas_, mirror_expressions_, *children_[1], trx);
    if (OB_FAIL(rc)) {
      return rc;
    }
  }

  return RC::SUCCESS;
}

static RC read_record_field(Table *table, const Record &record, const FieldMeta *field_meta, Value &value)
{
  const char *field_data = record.data() + field_meta->offset();
  value.reset();
  if (field_meta->type() == AttrType::TEXTS) {
    return table->get_text_value(record.data(), field_meta, value);
  }
  if (Value::is_null_data(field_data, field_meta->len(), field_meta->type())) {
    value.set_null();
    return RC::SUCCESS;
  }
  value.set_type(field_meta->type());
  value.set_data(field_data, field_meta->len());
  return RC::SUCCESS;
}

static RC make_match_key(Table *table, const Record &record, const vector<const FieldMeta *> &field_metas, vector<Value> &key)
{
  key.clear();
  key.reserve(field_metas.size());
  for (const FieldMeta *field_meta : field_metas) {
    Value value;
    RC rc = read_record_field(table, record, field_meta, value);
    if (OB_FAIL(rc)) {
      return rc;
    }
    key.emplace_back(std::move(value));
  }
  return RC::SUCCESS;
}

static bool match_key_equals(const vector<Value> &left, const vector<Value> &right)
{
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); i++) {
    if (left[i].compare(right[i]) != 0) {
      return false;
    }
  }
  return true;
}

static bool match_key_exists(const vector<Value> &key, const vector<vector<Value>> &allowed_keys)
{
  for (const vector<Value> &allowed_key : allowed_keys) {
    if (match_key_equals(key, allowed_key)) {
      return true;
    }
  }
  return false;
}

RC UpdatePhysicalOperator::collect_match_keys(Table *table,
    const vector<const FieldMeta *> &field_metas,
    PhysicalOperator &child,
    Trx *trx,
    vector<vector<Value>> &keys) const
{
  RC rc = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open mirror child operator. rc=%s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = child.next())) {
    Tuple *tuple = child.current_tuple();
    if (tuple == nullptr) {
      child.close();
      return RC::INTERNAL;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    vector<Value> key;
    rc = make_match_key(table, row_tuple->record(), field_metas, key);
    if (OB_FAIL(rc)) {
      child.close();
      return rc;
    }
    if (!match_key_exists(key, keys)) {
      keys.emplace_back(std::move(key));
    }
  }

  RC close_rc = child.close();
  if (rc == RC::RECORD_EOF) {
    rc = close_rc;
  }
  return rc;
}

RC UpdatePhysicalOperator::update_records(Table *table,
    const vector<const FieldMeta *> &field_metas,
    const vector<unique_ptr<Expression>> &expressions,
    PhysicalOperator &child,
    Trx *trx,
    const vector<vector<Value>> *allowed_keys,
    const vector<const FieldMeta *> *match_field_metas) const
{
  vector<Record> records;

  RC rc = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = child.next())) {
    Tuple *tuple = child.current_tuple();
    if (tuple == nullptr) {
      LOG_WARN("failed to get current tuple");
      child.close();
      return RC::INTERNAL;
    }

    RowTuple *row_tuple  = static_cast<RowTuple *>(tuple);
    Record   &old_record = row_tuple->record();
    if (allowed_keys != nullptr && match_field_metas != nullptr) {
      vector<Value> key;
      rc = make_match_key(table, old_record, *match_field_metas, key);
      if (OB_FAIL(rc)) {
        child.close();
        return rc;
      }
      if (!match_key_exists(key, *allowed_keys)) {
        continue;
      }
    }

    Record copied_record;
    copied_record.set_rid(old_record.rid());
    rc = copied_record.copy_data(old_record.data(), old_record.len());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to copy record before update. rc=%s", strrc(rc));
      child.close();
      return rc;
    }
    records.emplace_back(std::move(copied_record));
  }

  RC close_rc = child.close();
  if (rc == RC::RECORD_EOF) {
    rc = close_rc;
  }
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to scan records for update. rc=%s", strrc(rc));
    return rc;
  }

  vector<Record> new_records;
  new_records.reserve(records.size());
  for (Record &old_record : records) {
    Record new_record;
    rc = make_updated_record(table, field_metas, expressions, old_record, new_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to make updated record. rc=%s", strrc(rc));
      return rc;
    }

    rc = table->validate_unique_constraints(new_record, &old_record.rid(), trx);
    if (OB_FAIL(rc)) {
      LOG_WARN("unique constraint check failed while updating. rc=%s", strrc(rc));
      return rc;
    }
    new_records.emplace_back(std::move(new_record));
  }

  for (size_t i = 0; i < records.size(); i++) {
    Record &old_record = records[i];
    Record &new_record = new_records[i];

    rc = trx->delete_record(table, old_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to delete old record while updating. rc=%s", strrc(rc));
      return rc;
    }

    rc = trx->insert_record(table, new_record);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to insert updated record. rc=%s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::make_updated_record(Table *table,
    const vector<const FieldMeta *> &field_metas,
    const vector<unique_ptr<Expression>> &expressions,
    const Record &old_record,
    Record &new_record) const
{
  RC rc = new_record.copy_data(old_record.data(), old_record.len());
  if (OB_FAIL(rc)) {
    return rc;
  }
  new_record.set_rid(old_record.rid());

  RowTuple row_tuple;
  row_tuple.set_record(const_cast<Record *>(&old_record));
  row_tuple.set_schema(table, table->table_meta().field_metas());

  for (size_t i = 0; i < field_metas.size(); i++) {
    const FieldMeta *field_meta = field_metas[i];
    const Expression *expression = expressions[i].get();

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
        LOG_WARN("field can not be null. table name:%s, field name:%s", table->name(), field_meta->name());
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
  return RC::SUCCESS;
}
