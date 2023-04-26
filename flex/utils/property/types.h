/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPHSCOPE_TYPES_H_
#define GRAPHSCOPE_TYPES_H_

#include <assert.h>

#include <istream>
#include <ostream>
#include <vector>

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

enum class StorageStrategy {
  kNone,
  kMem,
};

enum class PropertyType {
  kInt32,
  kDate,
  kString,
  kEmpty,
  kInt64,
};

struct Date {
  Date() = default;
  ~Date() = default;
  Date(int64_t x);
  Date(const char* str);

  void reset(const char* str);
  std::string to_string() const;

  int64_t milli_second;
};

union AnyValue {
  AnyValue() {}
  ~AnyValue() {}

  int i;
  int64_t l;
  Date d;
  std::string_view s;
};

template <typename T>
struct AnyConverter;

struct Any {
  Any() : type(PropertyType::kEmpty) {}
  ~Any() {}

  int64_t get_long() const {
    assert(type == PropertyType::kInt64);
    return value.l;
  }

  void set_integer(int v) {
    type = PropertyType::kInt32;
    value.i = v;
  }

  void set_long(int64_t v) {
    type = PropertyType::kInt64;
    value.l = v;
  }

  void set_date(int64_t v) {
    type = PropertyType::kDate;
    value.d.milli_second = v;
  }
  void set_date(Date v) {
    type = PropertyType::kDate;
    value.d = v;
  }

  void set_string(std::string_view v) {
    type = PropertyType::kString;
    value.s = v;
  }

  std::string to_string() const {
    if (type == PropertyType::kInt32) {
      return std::to_string(value.i);
    } else if (type == PropertyType::kInt64) {
      return std::to_string(value.l);
    } else if (type == PropertyType::kString) {
      return std::string(value.s.data(), value.s.size());
      //      return value.s.to_string();
    } else if (type == PropertyType::kDate) {
      return value.d.to_string();
    } else if (type == PropertyType::kEmpty) {
      return "NULL";
    } else {
      LOG(FATAL) << "Unexpected property type: " << static_cast<int>(type);
      return "";
    }
  }

  std::string AsString() const {
    assert(type == PropertyType::kString);
    return std::string(value.s);
  }

  int64_t AsInt64() const {
    assert(type == PropertyType::kInt64);
    return value.l;
  }

  const std::string_view& AsStringView() const {
    assert(type == PropertyType::kString);
    return value.s;
  }

  const Date& AsDate() const {
    assert(type == PropertyType::kDate);
    return value.d;
  }

  template <typename T>
  static Any From(const T& value) {
    return AnyConverter<T>::to_any(value);
  }

  PropertyType type;
  AnyValue value;
};

template <typename T>
struct ConvertAny {
  static void to(const Any& value, T& out) {
    LOG(FATAL) << "Unexpected convert type...";
  }
};

template <>
struct ConvertAny<int> {
  static void to(const Any& value, int& out) {
    CHECK(value.type == PropertyType::kInt32);
    out = value.value.i;
  }
};

template <>
struct ConvertAny<int64_t> {
  static void to(const Any& value, int64_t& out) {
    CHECK(value.type == PropertyType::kInt64);
    out = value.value.l;
  }
};

template <>
struct ConvertAny<Date> {
  static void to(const Any& value, Date& out) {
    CHECK(value.type == PropertyType::kDate);
    out = value.value.d;
  }
};

template <>
struct ConvertAny<grape::EmptyType> {
  static void to(const Any& value, grape::EmptyType& out) {
    CHECK(value.type == PropertyType::kEmpty);
  }
};

template <>
struct ConvertAny<std::string> {
  static void to(const Any& value, std::string& out) {
    CHECK(value.type == PropertyType::kString);
    out = std::string(value.value.s);
  }
};

template <typename T>
struct AnyConverter {};

template <>
struct AnyConverter<int> {
  static constexpr PropertyType type = PropertyType::kInt32;

  static Any to_any(const int& value) {
    Any ret;
    ret.set_integer(value);
    return ret;
  }

  static AnyValue to_any_value(const int& value) {
    AnyValue ret;
    ret.i = value;
    return ret;
  }

  static const int& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kInt32);
    return value.value.i;
  }

  static const int& from_any_value(const AnyValue& value) { return value.i; }
};

template <>
struct AnyConverter<int64_t> {
  static constexpr PropertyType type = PropertyType::kInt64;

  static Any to_any(const int64_t& value) {
    Any ret;
    ret.set_long(value);
    return ret;
  }

  static AnyValue to_any_value(const int64_t& value) {
    AnyValue ret;
    ret.l = value;
    return ret;
  }

  static const int64_t& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kInt64);
    return value.value.l;
  }

  static const int64_t& from_any_value(const AnyValue& value) {
    return value.l;
  }
};

template <>
struct AnyConverter<Date> {
  static constexpr PropertyType type = PropertyType::kDate;

  static Any to_any(const Date& value) {
    Any ret;
    ret.set_date(value);
    return ret;
  }

  static AnyValue to_any_value(const Date& value) {
    AnyValue ret;
    ret.d = value;
    return ret;
  }

  static const Date& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kDate);
    return value.value.d;
  }

  static const Date& from_any_value(const AnyValue& value) { return value.d; }
};

template <>
struct AnyConverter<std::string_view> {
  static constexpr PropertyType type = PropertyType::kString;

  static Any to_any(const std::string_view& value) {
    Any ret;
    ret.set_string(value);
    return ret;
  }

  static AnyValue to_any_value(const std::string_view& value) {
    AnyValue ret;
    ret.s = value;
    return ret;
  }

  static const std::string_view& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kString);
    return value.value.s;
  }

  static const std::string_view& from_any_value(const AnyValue& value) {
    return value.s;
  }
};

template <>
struct AnyConverter<std::string> {
  static constexpr PropertyType type = PropertyType::kString;

  static Any to_any(const std::string& value) {
    Any ret;
    ret.set_string(value);
    return ret;
  }

  static AnyValue to_any_value(const std::string& value) {
    AnyValue ret;
    ret.s = value;
    return ret;
  }

  static std::string from_any(const Any& value) {
    CHECK(value.type == PropertyType::kString);
    return std::string(value.value.s);
  }

  static std::string from_any_value(const AnyValue& value) {
    return std::string(value.s);
  }
};

template <>
struct AnyConverter<grape::EmptyType> {
  static constexpr PropertyType type = PropertyType::kEmpty;

  static Any to_any(const grape::EmptyType& value) {
    Any ret;
    return ret;
  }

  static AnyValue to_any_value(const grape::EmptyType& value) {
    AnyValue ret;
    return ret;
  }

  static grape::EmptyType from_any(const Any& value) {
    CHECK(value.type == PropertyType::kEmpty);
    return grape::EmptyType();
  }

  static grape::EmptyType from_any_value(const AnyValue& value) {
    return grape::EmptyType();
  }
};

void ParseRecord(const char* line, std::vector<Any>& rec);

void ParseRecord(const char* line, int64_t& id, std::vector<Any>& rec);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, Date& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  grape::EmptyType& prop);

grape::InArchive& operator<<(grape::InArchive& in_archive, const Any& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value);

}  // namespace gs

namespace std {

inline ostream& operator<<(ostream& os, const gs::Date& dt) {
  os << dt.to_string();
  return os;
}

}  // namespace std

#endif  // GRAPHSCOPE_TYPES_H_
