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

#include "common/type/date_type.h"
#include "common/value.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>

static bool is_leap_year(int year)
{
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

static int days_in_month(int year, int month)
{
  static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month == 2 && is_leap_year(year)) {
    return 29;
  }
  return days[month - 1];
}

// Convert a date to days since 1970-01-01 (can be negative for dates before epoch)
static int32_t date_to_days(int year, int month, int day)
{
  int32_t days = 0;

  if (year >= 1970) {
    for (int y = 1970; y < year; y++) {
      days += is_leap_year(y) ? 366 : 365;
    }
  } else {
    for (int y = 1969; y >= year; y--) {
      days -= is_leap_year(y) ? 366 : 365;
    }
  }

  for (int m = 1; m < month; m++) {
    days += days_in_month(year, m);
  }

  days += day - 1;
  return days;
}

bool DateType::is_valid_date(int year, int month, int day)
{
  if (month < 1 || month > 12) {
    return false;
  }
  if (day < 1 || day > days_in_month(year, month)) {
    return false;
  }
  return true;
}

bool DateType::parse_date(const char *str, int32_t &days)
{
  if (str == nullptr) {
    return false;
  }

  int year = 0, month = 0, day = 0;
  if (sscanf(str, "%d-%d-%d", &year, &month, &day) != 3) {
    return false;
  }

  if (!is_valid_date(year, month, day)) {
    return false;
  }

  days = date_to_days(year, month, day);
  return true;
}

void DateType::days_to_date(int32_t days, int &year, int &month, int &day)
{
  year = 1970;

  if (days >= 0) {
    while (true) {
      int days_in_year = is_leap_year(year) ? 366 : 365;
      if (days < days_in_year) {
        break;
      }
      days -= days_in_year;
      year++;
    }
  } else {
    while (days < 0) {
      year--;
      days += is_leap_year(year) ? 366 : 365;
    }
  }

  month = 1;
  while (true) {
    int dim = days_in_month(year, month);
    if (days < dim) {
      break;
    }
    days -= dim;
    month++;
  }

  day = days + 1;
}

int DateType::compare(const Value &left, const Value &right) const
{
  int32_t l = left.get_int();
  int32_t r = right.get_int();
  if (l < r) {
    return -1;
  } else if (l > r) {
    return 1;
  }
  return 0;
}

RC DateType::to_string(const Value &val, string &result) const
{
  int32_t days = val.get_int();
  int year, month, day;
  days_to_date(days, year, month, day);

  char buf[16];
  snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
  result = buf;
  return RC::SUCCESS;
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  if (type == AttrType::DATES) {
    result = val;
    return RC::SUCCESS;
  }
  if (type == AttrType::CHARS) {
    string str;
    RC rc = to_string(val, str);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    result.set_string(str.c_str());
    return RC::SUCCESS;
  }
  return RC::UNIMPLEMENTED;
}
