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

#include "common/type/date_type.h"

#include <cstdio>

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/value.h"

int DateType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::DATES && right.attr_type() == AttrType::DATES, "invalid date type");
  return common::compare_int((void *)&left.value_.int_value_, (void *)&right.value_.int_value_);
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::CHARS: {
      result.set_string(format_date(val.value_.int_value_).c_str());
      return RC::SUCCESS;
    }
    default: return RC::UNIMPLEMENTED;
  }
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  int date = 0;
  RC rc = parse_date(data, date);
  if (OB_FAIL(rc)) {
    return rc;
  }
  val.set_date(date);
  return RC::SUCCESS;
}

RC DateType::to_string(const Value &val, string &result) const
{
  result = format_date(val.value_.int_value_);
  return RC::SUCCESS;
}

RC DateType::parse_date(const string &data, int &date)
{
  if (data.size() != 10 || data[4] != '-' || data[7] != '-') {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  for (int i : {0, 1, 2, 3, 5, 6, 8, 9}) {
    if (data[i] < '0' || data[i] > '9') {
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  int year  = stoi(data.substr(0, 4));
  int month = stoi(data.substr(5, 2));
  int day   = stoi(data.substr(8, 2));

  if (!is_valid_date(year, month, day)) {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  date = year * 10000 + month * 100 + day;
  return RC::SUCCESS;
}

string DateType::format_date(int date)
{
  int year  = date / 10000;
  int month = (date / 100) % 100;
  int day   = date % 100;

  char buffer[16];
  snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", year, month, day);
  return string(buffer);
}

bool DateType::is_valid_date(int year, int month, int day)
{
  if (year <= 0 || month < 1 || month > 12 || day < 1) {
    return false;
  }

  static const int days_of_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int max_day = days_of_month[month - 1];
  bool leap_year = (year % 400 == 0) || (year % 4 == 0 && year % 100 != 0);
  if (month == 2 && leap_year) {
    max_day = 29;
  }

  return day <= max_day;
}
