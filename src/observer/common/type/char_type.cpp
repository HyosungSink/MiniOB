/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/type/date_type.h"
#include "common/value.h"

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(is_string_type(left.attr_type()) && is_string_type(right.attr_type()), "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::INTS: {
      stringstream deserialize_stream;
      deserialize_stream.clear();
      deserialize_stream.str(val.get_string());

      int int_value = 0;
      deserialize_stream >> int_value;
      if (!deserialize_stream || !deserialize_stream.eof()) {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      result.set_int(int_value);
      return RC::SUCCESS;
    }
    case AttrType::FLOATS: {
      stringstream deserialize_stream;
      deserialize_stream.clear();
      deserialize_stream.str(val.get_string());

      float float_value = 0;
      deserialize_stream >> float_value;
      if (!deserialize_stream || !deserialize_stream.eof()) {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      result.set_float(float_value);
      return RC::SUCCESS;
    }
    case AttrType::DATES: {
      int date = 0;
      RC rc = DateType::parse_date(val.get_string(), date);
      if (OB_FAIL(rc)) {
        return rc;
      }
      result.set_date(date);
      return RC::SUCCESS;
    }
    case AttrType::TEXTS:
    case AttrType::CHARS: {
      result.set_string(val.get_string().c_str());
      return RC::SUCCESS;
    }
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (is_string_type(type)) {
    return 0;
  } else if (type == AttrType::INTS || type == AttrType::FLOATS) {
    return 1;
  } else if (type == AttrType::DATES) {
    return 1;
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}
