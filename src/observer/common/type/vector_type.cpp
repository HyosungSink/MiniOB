/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/type/vector_type.h"

#include <cerrno>
#include <cmath>

#include "common/lang/cmath.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/value.h"

using common::strip;

static bool parse_float_token(const string &token, float &value)
{
  errno           = 0;
  char *end       = nullptr;
  value           = strtof(token.c_str(), &end);
  const bool done = end != nullptr && *end == '\0';
  return errno == 0 && done;
}

static string format_vector_component(float value)
{
  char buffer[32];
  snprintf(buffer, sizeof(buffer), "%.5E", value);
  return string(buffer);
}

int VectorType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::VECTORS && right.attr_type() == AttrType::VECTORS, "invalid type");

  const int left_dim  = left.get_vector_dimension();
  const int right_dim = right.get_vector_dimension();
  if (left_dim != right_dim) {
    return left_dim < right_dim ? -1 : 1;
  }

  const float *left_data  = left.get_vector();
  const float *right_data = right.get_vector();
  for (int i = 0; i < left_dim; i++) {
    if (fabsf(left_data[i] - right_data[i]) <= EPSILON) {
      continue;
    }
    return left_data[i] < right_data[i] ? -1 : 1;
  }
  return 0;
}

RC VectorType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::CHARS: {
      string text;
      RC rc = to_string(val, text);
      if (OB_FAIL(rc)) {
        return rc;
      }
      result.set_string(text.c_str());
      return RC::SUCCESS;
    }
    default: return RC::UNIMPLEMENTED;
  }
}

RC VectorType::to_string(const Value &val, string &result) const
{
  const int    dim  = val.get_vector_dimension();
  const float *data = val.get_vector();

  result.clear();
  result.push_back('[');
  for (int i = 0; i < dim; i++) {
    if (i != 0) {
      result.push_back(',');
    }
    result.append(format_vector_component(data[i]));
  }
  result.push_back(']');
  return RC::SUCCESS;
}

RC VectorType::parse_vector_literal(const string &text, vector<float> &elements)
{
  string trimmed = text;
  strip(trimmed);
  if (trimmed.size() < 2 || trimmed.front() != '[' || trimmed.back() != ']') {
    return RC::INVALID_ARGUMENT;
  }

  string body = trimmed.substr(1, trimmed.size() - 2);
  strip(body);
  if (body.empty()) {
    return RC::INVALID_ARGUMENT;
  }

  vector<string> tokens;
  common::split_string(body, ",", tokens);
  if (tokens.empty()) {
    return RC::INVALID_ARGUMENT;
  }

  elements.clear();
  elements.reserve(tokens.size());
  for (string &token : tokens) {
    strip(token);
    if (token.empty()) {
      return RC::INVALID_ARGUMENT;
    }

    float value = 0;
    if (!parse_float_token(token, value)) {
      return RC::INVALID_ARGUMENT;
    }
    elements.push_back(value);
  }

  return RC::SUCCESS;
}

RC VectorType::parse_vector_literal(const string &text, Value &value)
{
  vector<float> elements;
  RC rc = parse_vector_literal(text, elements);
  if (OB_FAIL(rc)) {
    return rc;
  }

  value.set_vector(elements.data(), static_cast<int>(elements.size()));
  return RC::SUCCESS;
}

RC VectorType::distance(const Value &left, const Value &right, const string &metric, Value &result)
{
  if (left.attr_type() != AttrType::VECTORS || right.attr_type() != AttrType::VECTORS) {
    return RC::INVALID_ARGUMENT;
  }

  const int left_dim  = left.get_vector_dimension();
  const int right_dim = right.get_vector_dimension();
  if (left_dim != right_dim) {
    return RC::INVALID_ARGUMENT;
  }

  const float *left_data  = left.get_vector();
  const float *right_data = right.get_vector();

  if (0 == strcasecmp(metric.c_str(), "EUCLIDEAN")) {
    float distance = 0;
    for (int i = 0; i < left_dim; i++) {
      const float diff = left_data[i] - right_data[i];
      distance += diff * diff;
    }
    result.set_float(sqrtf(distance), 16);
    return RC::SUCCESS;
  }

  if (0 == strcasecmp(metric.c_str(), "DOT")) {
    float distance = 0;
    for (int i = 0; i < left_dim; i++) {
      distance += left_data[i] * right_data[i];
    }
    result.set_float(distance, 16);
    return RC::SUCCESS;
  }

  if (0 == strcasecmp(metric.c_str(), "COSINE")) {
    float dot        = 0;
    float left_norm  = 0;
    float right_norm = 0;
    for (int i = 0; i < left_dim; i++) {
      dot += left_data[i] * right_data[i];
      left_norm += left_data[i] * left_data[i];
      right_norm += right_data[i] * right_data[i];
    }

    if (left_norm <= EPSILON || right_norm <= EPSILON) {
      return RC::INVALID_ARGUMENT;
    }

    const float cosine = dot / (sqrtf(left_norm) * sqrtf(right_norm));
    result.set_float(1.0f - cosine, 16);
    return RC::SUCCESS;
  }

  return RC::INVALID_ARGUMENT;
}
