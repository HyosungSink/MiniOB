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

RC IndexMeta::init(const char *name, const FieldMeta &field)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_  = name;
  field_ = field.name();
  fields_.clear();
  fields_.push_back(field.name());
  return RC::SUCCESS;
}

RC IndexMeta::init(const char *name, const vector<const FieldMeta *> &field_metas)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }
  if (field_metas.empty()) {
    LOG_ERROR("Failed to init index, no fields provided.");
    return RC::INVALID_ARGUMENT;
  }

  name_  = name;
  field_ = field_metas[0]->name();
  fields_.clear();
  for (const FieldMeta *fm : field_metas) {
    fields_.push_back(fm->name());
  }
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME]       = name_;
  json_value[FIELD_FIELD_NAME] = field_;
  if (fields_.size() > 1) {
    Json::Value arr(Json::arrayValue);
    for (const string &f : fields_) {
      arr.append(f);
    }
    json_value[FIELD_FIELD_NAMES] = arr;
  }
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value  = json_value[FIELD_NAME];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  // Check for composite index (field_names array) first
  const Json::Value &fields_value = json_value[FIELD_FIELD_NAMES];
  if (fields_value.isArray() && fields_value.size() > 0) {
    vector<const FieldMeta *> field_metas;
    for (Json::ArrayIndex i = 0; i < fields_value.size(); i++) {
      if (!fields_value[i].isString()) {
        LOG_ERROR("Field name at index %d of composite index [%s] is not a string.", i, name_value.asCString());
        return RC::INTERNAL;
      }
      const FieldMeta *field = table.field(fields_value[i].asCString());
      if (nullptr == field) {
        LOG_ERROR("Deserialize composite index [%s]: no such field: %s", name_value.asCString(), fields_value[i].asCString());
        return RC::SCHEMA_FIELD_MISSING;
      }
      field_metas.push_back(field);
    }
    return index.init(name_value.asCString(), field_metas);
  }

  // Fall back to single field_name
  const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
  if (!field_value.isString()) {
    LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
        name_value.asCString(), field_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  const FieldMeta *field = table.field(field_value.asCString());
  if (nullptr == field) {
    LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
    return RC::SCHEMA_FIELD_MISSING;
  }

  return index.init(name_value.asCString(), *field);
}

const char *IndexMeta::name() const { return name_.c_str(); }

const char *IndexMeta::field() const { return field_.c_str(); }

void IndexMeta::desc(ostream &os) const
{
  os << "index name=" << name_ << ", field=";
  for (size_t i = 0; i < fields_.size(); i++) {
    if (i > 0) os << ",";
    os << fields_[i];
  }
}