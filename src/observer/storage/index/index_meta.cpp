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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
const static Json::StaticString FIELD_FIELD_NAMES("field_names");
const static Json::StaticString FIELD_UNIQUE("unique");

RC IndexMeta::init(const char *name, const FieldMeta &field)
{
  vector<const FieldMeta *> fields;
  fields.push_back(&field);
  return init(name, fields, false);
}

RC IndexMeta::init(const char *name, const vector<const FieldMeta *> &fields, bool unique)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }
  if (fields.empty() || fields[0] == nullptr) {
    LOG_ERROR("Failed to init index, fields is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_ = name;
  field_ = fields[0]->name();
  fields_.clear();
  fields_.reserve(fields.size());
  for (const FieldMeta *field : fields) {
    if (field == nullptr) {
      return RC::INVALID_ARGUMENT;
    }
    fields_.push_back(field->name());
  }
  unique_ = unique;
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME]       = name_;
  json_value[FIELD_FIELD_NAME] = field_;
  Json::Value field_names_value;
  for (const string &field : fields_) {
    field_names_value.append(field);
  }
  json_value[FIELD_FIELD_NAMES] = std::move(field_names_value);
  json_value[FIELD_UNIQUE] = unique_;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value  = json_value[FIELD_NAME];
  const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if (!field_value.isString() && !json_value[FIELD_FIELD_NAMES].isArray()) {
    LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
        name_value.asCString(), field_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  vector<const FieldMeta *> fields;
  const Json::Value &field_names_value = json_value[FIELD_FIELD_NAMES];
  if (field_names_value.isArray()) {
    for (const Json::Value &field_name_value : field_names_value) {
      if (!field_name_value.isString()) {
        return RC::INTERNAL;
      }
      const FieldMeta *field = table.field(field_name_value.asCString());
      if (nullptr == field) {
        LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_name_value.asCString());
        return RC::SCHEMA_FIELD_MISSING;
      }
      fields.push_back(field);
    }
  } else {
    const FieldMeta *field = table.field(field_value.asCString());
    if (nullptr == field) {
      LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
      return RC::SCHEMA_FIELD_MISSING;
    }
    fields.push_back(field);
  }

  bool unique = json_value[FIELD_UNIQUE].isBool() ? json_value[FIELD_UNIQUE].asBool() : false;
  return index.init(name_value.asCString(), fields, unique);
}

const char *IndexMeta::name() const { return name_.c_str(); }

const char *IndexMeta::field() const { return field_.c_str(); }

void IndexMeta::desc(ostream &os) const { os << "index name=" << name_ << ", field=" << field_; }
