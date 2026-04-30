/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/type/data_type.h"
#include "common/lang/vector.h"

/**
 * @brief 向量类型
 * @ingroup DataType
 */
class VectorType : public DataType
{
public:
  static constexpr int MAX_DIMENSION = 16383;

  VectorType() : DataType(AttrType::VECTORS) {}
  virtual ~VectorType() {}

  int compare(const Value &left, const Value &right) const override;

  RC add(const Value &left, const Value &right, Value &result) const override { return RC::UNIMPLEMENTED; }
  RC subtract(const Value &left, const Value &right, Value &result) const override { return RC::UNIMPLEMENTED; }
  RC multiply(const Value &left, const Value &right, Value &result) const override { return RC::UNIMPLEMENTED; }

  RC cast_to(const Value &val, AttrType type, Value &result) const override;
  RC to_string(const Value &val, string &result) const override;

  static RC parse_vector_literal(const string &text, vector<float> &elements);
  static RC parse_vector_literal(const string &text, Value &value);
  static RC distance(const Value &left, const Value &right, const string &metric, Value &result);
};
