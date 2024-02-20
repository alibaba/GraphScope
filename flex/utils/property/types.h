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

#include <chrono>
#include <istream>
#include <ostream>
#include <vector>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace grape {

inline bool operator<(const EmptyType& lhs, const EmptyType& rhs) {
  return false;
}

}  // namespace grape

namespace gs {

enum class StorageStrategy {
  kNone,
  kMem,
  kDisk,
};

namespace impl {

enum class PropertyTypeImpl {
  kInt32,
  kDate,
  kDay,
  kString,
  kEmpty,
  kInt64,
  kDouble,
  kUInt32,
  kUInt64,
  kBool,
  kFloat,
  kUInt8,
  kUInt16,
  kStringMap,
  kVarChar,
};

// Stores additional type information for PropertyTypeImpl
union AdditionalTypeInfo {
  uint16_t max_length;  // for varchar
};
}  // namespace impl

struct PropertyType {
  static constexpr const uint16_t STRING_DEFAULT_MAX_LENGTH = 256;
  impl::PropertyTypeImpl type_enum;
  impl::AdditionalTypeInfo additional_type_info;

  constexpr PropertyType()
      : type_enum(impl::PropertyTypeImpl::kEmpty), additional_type_info() {}
  constexpr PropertyType(impl::PropertyTypeImpl type)
      : type_enum(type), additional_type_info() {}
  constexpr PropertyType(impl::PropertyTypeImpl type, uint16_t max_length)
      : type_enum(type), additional_type_info({.max_length = max_length}) {
    assert(type == impl::PropertyTypeImpl::kVarChar);
  }

  bool IsVarchar() const;

  static PropertyType Empty();
  static PropertyType Bool();
  static PropertyType UInt8();
  static PropertyType UInt16();
  static PropertyType Int32();
  static PropertyType UInt32();
  static PropertyType Float();
  static PropertyType Int64();
  static PropertyType UInt64();
  static PropertyType Double();
  static PropertyType Date();
  static PropertyType Day();
  static PropertyType String();
  static PropertyType StringMap();
  static PropertyType Varchar(uint16_t max_length);

  static const PropertyType kEmpty;
  static const PropertyType kBool;
  static const PropertyType kUInt8;
  static const PropertyType kUInt16;
  static const PropertyType kInt32;
  static const PropertyType kUInt32;
  static const PropertyType kFloat;
  static const PropertyType kInt64;
  static const PropertyType kUInt64;
  static const PropertyType kDouble;
  static const PropertyType kDate;
  static const PropertyType kDay;
  static const PropertyType kString;
  static const PropertyType kStringMap;

  bool operator==(const PropertyType& other) const;
  bool operator!=(const PropertyType& other) const;
};

struct __attribute__((packed)) Date {
  Date() = default;
  ~Date() = default;
  Date(int64_t x);

  std::string to_string() const;

  bool operator<(const Date& rhs) const {
    return milli_second < rhs.milli_second;
  }

  int64_t milli_second;
};

struct DayValue {
  uint32_t year : 18;
  uint32_t month : 4;
  uint32_t day : 5;
  uint32_t hour : 5;
};

struct Day {
  Day() = default;
  ~Day() = default;

  Day(int64_t ts);

  std::string to_string() const;

  uint32_t to_u32() const;
  void from_u32(uint32_t val);

  int64_t to_timestamp() const {
    const boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));

    boost::gregorian::date new_date(year(), month(), day());
    boost::posix_time::ptime new_time_point(
        new_date, boost::posix_time::time_duration(hour(), 0, 0));
    boost::posix_time::time_duration diff = new_time_point - epoch;
    int64_t new_timestamp_sec = diff.total_seconds();

    return new_timestamp_sec * 1000;
  }

  void from_timestamp(int64_t ts) {
    const boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
    int64_t ts_sec = ts / 1000;
    boost::posix_time::ptime time_point =
        epoch + boost::posix_time::seconds(ts_sec);
    boost::posix_time::ptime::date_type date = time_point.date();
    boost::posix_time::time_duration td = time_point.time_of_day();
    this->value.internal.year = date.year();
    this->value.internal.month = date.month().as_number();
    this->value.internal.day = date.day();
    this->value.internal.hour = td.hours();

    int64_t ts_back = to_timestamp();
    CHECK_EQ(ts, ts_back);
  }

  bool operator<(const Day& rhs) const { return this->to_u32() < rhs.to_u32(); }
  bool operator==(const Day& rhs) const {
    return this->to_u32() == rhs.to_u32();
  }

  int year() const;
  int month() const;
  int day() const;
  int hour() const;

  union {
    DayValue internal;
    uint32_t integer;
  } value;
};

struct LabelKey {
  using label_data_type = uint8_t;
  int32_t label_id;
  LabelKey() = default;
  LabelKey(label_data_type id) : label_id(id) {}
  LabelKey(LabelKey&&) = default;
  LabelKey(const LabelKey&) = default;

  // operator=
  LabelKey& operator=(const LabelKey& other) {
    label_id = other.label_id;
    return *this;
  }
};

union AnyValue {
  AnyValue() {}
  ~AnyValue() {}

  bool b;
  int32_t i;
  uint32_t ui;
  float f;
  int64_t l;
  uint64_t ul;

  Date d;
  Day day;
  std::string_view s;
  double db;
  uint8_t u8;
  uint16_t u16;
};

template <typename T>
struct AnyConverter;

struct Any {
  Any() : type(PropertyType::kEmpty) {}

  Any(const Any& other) : type(other.type), value(other.value) {}

  template <typename T>
  Any(const T& val) {
    Any a = Any::From(val);
    type = a.type;
    value = a.value;
  }

  ~Any() {}

  int64_t get_long() const {
    assert(type == PropertyType::kInt64);
    return value.l;
  }
  void set_bool(bool v) {
    type = PropertyType::kBool;
    value.b = v;
  }

  void set_i32(int32_t v) {
    type = PropertyType::kInt32;
    value.i = v;
  }

  void set_u32(uint32_t v) {
    type = PropertyType::kUInt32;
    value.ui = v;
  }

  void set_i64(int64_t v) {
    type = PropertyType::kInt64;
    value.l = v;
  }

  void set_u64(uint64_t v) {
    type = PropertyType::kUInt64;
    value.ul = v;
  }

  void set_date(int64_t v) {
    type = PropertyType::kDate;
    value.d.milli_second = v;
  }

  void set_date(Date v) {
    type = PropertyType::kDate;
    value.d = v;
  }

  void set_day(Day v) {
    type = PropertyType::kDay;
    value.day = v;
  }

  void set_string(std::string_view v) {
    type = PropertyType::kString;
    value.s = v;
  }

  void set_float(float v) {
    type = PropertyType::kFloat;
    value.f = v;
  }

  void set_double(double db) {
    type = PropertyType::kDouble;
    value.db = db;
  }

  void set_u8(uint8_t v) {
    type = PropertyType::kUInt8;
    value.u8 = v;
  }

  void set_u16(uint16_t v) {
    type = PropertyType::kUInt16;
    value.u16 = v;
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
    } else if (type == PropertyType::kDay) {
      return value.day.to_string();
    } else if (type == PropertyType::kEmpty) {
      return "NULL";
    } else if (type == PropertyType::kDouble) {
      return std::to_string(value.db);
    } else if (type == PropertyType::kUInt8) {
      return std::to_string(value.u8);
    } else if (type == PropertyType::kUInt16) {
      return std::to_string(value.u16);
    } else if (type == PropertyType::kUInt32) {
      return std::to_string(value.ui);
    } else if (type == PropertyType::kUInt64) {
      return std::to_string(value.ul);
    } else if (type == PropertyType::kBool) {
      return value.b ? "true" : "false";
    } else if (type == PropertyType::kFloat) {
      return std::to_string(value.f);
    } else {
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(type.type_enum);
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

  uint64_t AsUInt64() const {
    assert(type == PropertyType::kUInt64);
    return value.ul;
  }

  int32_t AsInt32() const {
    assert(type == PropertyType::kInt32);
    return value.i;
  }

  uint32_t AsUInt32() const {
    assert(type == PropertyType::kUInt32);
    return value.ui;
  }

  bool AsBool() const {
    assert(type == PropertyType::kBool);
    return value.b;
  }

  double AsDouble() const {
    assert(type == PropertyType::kDouble);
    return value.db;
  }

  float AsFloat() const {
    assert(type == PropertyType::kFloat);
    return value.f;
  }

  const std::string_view& AsStringView() const {
    assert(type == PropertyType::kString);
    return value.s;
  }

  const Date& AsDate() const {
    assert(type == PropertyType::kDate);
    return value.d;
  }

  const Day& AsDay() const {
    assert(type == PropertyType::kDay);
    return value.day;
  }

  template <typename T>
  static Any From(const T& value) {
    return AnyConverter<T>::to_any(value);
  }

  bool operator==(const Any& other) const {
    if (type == other.type) {
      if (type == PropertyType::kInt32) {
        return value.i == other.value.i;
      } else if (type == PropertyType::kInt64) {
        return value.l == other.value.l;
      } else if (type == PropertyType::kDate) {
        return value.d.milli_second == other.value.d.milli_second;
      } else if (type == PropertyType::kDay) {
        return value.day == other.value.day;
      } else if (type == PropertyType::kString) {
        return value.s == other.value.s;
      } else if (type == PropertyType::kEmpty) {
        return true;
      } else if (type == PropertyType::kDouble) {
        return value.db == other.value.db;
      } else if (type == PropertyType::kUInt32) {
        return value.ui == other.value.ui;
      } else if (type == PropertyType::kUInt64) {
        return value.ul == other.value.ul;
      } else if (type == PropertyType::kBool) {
        return value.b == other.value.b;
      } else if (type == PropertyType::kFloat) {
        return value.f == other.value.f;
      } else if (type.type_enum == impl::PropertyTypeImpl::kVarChar) {
        if (other.type.type_enum != impl::PropertyTypeImpl::kVarChar) {
          return false;
        }
        return value.s == other.value.s;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  bool operator<(const Any& other) const {
    if (type == other.type) {
      if (type == PropertyType::kInt32) {
        return value.i < other.value.i;
      } else if (type == PropertyType::kInt64) {
        return value.l < other.value.l;
      } else if (type == PropertyType::kDate) {
        return value.d.milli_second < other.value.d.milli_second;
      } else if (type == PropertyType::kDay) {
        return value.day < other.value.day;
      } else if (type == PropertyType::kString) {
        return value.s < other.value.s;
      } else if (type == PropertyType::kEmpty) {
        return false;
      } else if (type == PropertyType::kDouble) {
        return value.db < other.value.db;
      } else if (type == PropertyType::kUInt32) {
        return value.ui < other.value.ui;
      } else if (type == PropertyType::kUInt64) {
        return value.ul < other.value.ul;
      } else if (type == PropertyType::kBool) {
        return value.b < other.value.b;
      } else if (type == PropertyType::kFloat) {
        return value.f < other.value.f;
      } else {
        return false;
      }
    } else {
      LOG(FATAL) << "Type [" << static_cast<int>(type.type_enum) << "] and ["
                 << static_cast<int>(other.type.type_enum)
                 << "] cannot be compared..";
    }
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
struct ConvertAny<bool> {
  static void to(const Any& value, bool& out) {
    CHECK(value.type == PropertyType::kBool);
    out = value.value.b;
  }
};

template <>
struct ConvertAny<int32_t> {
  static void to(const Any& value, int32_t& out) {
    CHECK(value.type == PropertyType::kInt32);
    out = value.value.i;
  }
};

template <>
struct ConvertAny<uint32_t> {
  static void to(const Any& value, uint32_t& out) {
    CHECK(value.type == PropertyType::kUInt32);
    out = value.value.ui;
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
struct ConvertAny<uint64_t> {
  static void to(const Any& value, uint64_t& out) {
    CHECK(value.type == PropertyType::kUInt64);
    out = value.value.ul;
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
struct ConvertAny<Day> {
  static void to(const Any& value, Day& out) {
    CHECK(value.type == PropertyType::kDay);
    out = value.value.day;
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

template <>
struct ConvertAny<std::string_view> {
  static void to(const Any& value, std::string_view& out) {
    CHECK(value.type == PropertyType::kString);
    out = value.value.s;
  }
};

template <>
struct ConvertAny<float> {
  static void to(const Any& value, float& out) {
    CHECK(value.type == PropertyType::kFloat);
    out = value.value.f;
  }
};

template <>
struct ConvertAny<double> {
  static void to(const Any& value, double& out) {
    CHECK(value.type == PropertyType::kDouble);
    out = value.value.db;
  }
};

template <typename T>
struct AnyConverter {};

// specialization for bool
template <>
struct AnyConverter<bool> {
  static PropertyType type() { return PropertyType::kBool; }

  static Any to_any(const bool& value) {
    Any ret;
    ret.set_bool(value);
    return ret;
  }

  static AnyValue to_any_value(const bool& value) {
    AnyValue ret;
    ret.b = value;
    return ret;
  }

  static const bool& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kBool);
    return value.value.b;
  }

  static const bool& from_any_value(const AnyValue& value) { return value.b; }
};

template <>
struct AnyConverter<uint8_t> {
  static PropertyType type() { return PropertyType::kUInt8; }
  static Any to_any(const uint8_t& value) {
    Any ret;
    ret.set_u8(value);
    return ret;
  }
  static const uint8_t& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kUInt8);
    return value.value.u8;
  }
};

template <>
struct AnyConverter<uint16_t> {
  static PropertyType type() { return PropertyType::kUInt16; }
  static Any to_any(const uint16_t& value) {
    Any ret;
    ret.set_u16(value);
    return ret;
  }
  static const uint16_t& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kUInt8);
    return value.value.u16;
  }
};

template <>
struct AnyConverter<int32_t> {
  static PropertyType type() { return PropertyType::kInt32; }

  static Any to_any(const int32_t& value) {
    Any ret;
    ret.set_i32(value);
    return ret;
  }

  static AnyValue to_any_value(const int32_t& value) {
    AnyValue ret;
    ret.i = value;
    return ret;
  }

  static const int32_t& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kInt32);
    return value.value.i;
  }

  static const int32_t& from_any_value(const AnyValue& value) {
    return value.i;
  }
};

template <>
struct AnyConverter<uint32_t> {
  static PropertyType type() { return PropertyType::kUInt32; }

  static Any to_any(const uint32_t& value) {
    Any ret;
    ret.set_u32(value);
    return ret;
  }

  static AnyValue to_any_value(const uint32_t& value) {
    AnyValue ret;
    ret.ui = value;
    return ret;
  }

  static const uint32_t& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kUInt32);
    return value.value.ui;
  }

  static const uint32_t& from_any_value(const AnyValue& value) {
    return value.ui;
  }
};
template <>
struct AnyConverter<int64_t> {
  static PropertyType type() { return PropertyType::kInt64; }

  static Any to_any(const int64_t& value) {
    Any ret;
    ret.set_i64(value);
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
struct AnyConverter<uint64_t> {
  static PropertyType type() { return PropertyType::kUInt64; }

  static Any to_any(const uint64_t& value) {
    Any ret;
    ret.set_u64(value);
    return ret;
  }

  static AnyValue to_any_value(const uint64_t& value) {
    AnyValue ret;
    ret.ul = value;
    return ret;
  }

  static const uint64_t& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kUInt64);
    return value.value.ul;
  }

  static const uint64_t& from_any_value(const AnyValue& value) {
    return value.ul;
  }
};

template <>
struct AnyConverter<Date> {
  static PropertyType type() { return PropertyType::kDate; }

  static Any to_any(const Date& value) {
    Any ret;
    ret.set_date(value);
    return ret;
  }

  static Any to_any(int64_t value) {
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
struct AnyConverter<Day> {
  static PropertyType type() { return PropertyType::kDay; }

  static Any to_any(const Day& value) {
    Any ret;
    ret.set_day(value);
    return ret;
  }

  static Any to_any(int64_t value) {
    Day dval(value);
    Any ret;
    ret.set_day(dval);
    return ret;
  }

  static AnyValue to_any_value(const Day& value) {
    AnyValue ret;
    ret.day = value;
    return ret;
  }

  static const Day& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kDay);
    return value.value.day;
  }

  static const Day& from_any_value(const AnyValue& value) { return value.day; }
};

template <>
struct AnyConverter<std::string_view> {
  static PropertyType type() { return PropertyType::kString; }

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
  static PropertyType type() { return PropertyType::kString; }

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
  static PropertyType type() { return PropertyType::kEmpty; }

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

template <>
struct AnyConverter<double> {
  static PropertyType type() { return PropertyType::kDouble; }

  static Any to_any(const double& value) {
    Any ret;
    ret.set_double(value);
    return ret;
  }

  static AnyValue to_any_value(const double& value) {
    AnyValue ret;
    ret.db = value;
    return ret;
  }

  static const double& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kDouble);
    return value.value.db;
  }

  static const double& from_any_value(const AnyValue& value) {
    return value.db;
  }
};

// specialization for float
template <>
struct AnyConverter<float> {
  static PropertyType type() { return PropertyType::kFloat; }

  static Any to_any(const float& value) {
    Any ret;
    ret.set_float(value);
    return ret;
  }

  static AnyValue to_any_value(const float& value) {
    AnyValue ret;
    ret.f = value;
    return ret;
  }

  static const float& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kFloat);
    return value.value.f;
  }

  static const float& from_any_value(const AnyValue& value) { return value.f; }
};

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const PropertyType& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              PropertyType& value);

grape::InArchive& operator<<(grape::InArchive& in_archive, const Any& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value);

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const std::string_view& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              std::string_view& value);

}  // namespace gs

namespace boost {
// override boost hash function for EmptyType
inline std::size_t hash_value(const grape::EmptyType& value) { return 0; }
}  // namespace boost

namespace std {
inline bool operator==(const grape::EmptyType& a, const grape::EmptyType& b) {
  return true;
}

inline bool operator!=(const grape::EmptyType& a, const grape::EmptyType& b) {
  return false;
}

inline ostream& operator<<(ostream& os, const gs::Date& dt) {
  os << dt.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, const gs::Day& dt) {
  os << dt.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, gs::PropertyType pt) {
  if (pt == gs::PropertyType::Bool()) {
    os << "bool";
  } else if (pt == gs::PropertyType::Empty()) {
    os << "empty";
  } else if (pt == gs::PropertyType::UInt8()) {
    os << "uint8";
  } else if (pt == gs::PropertyType::UInt16()) {
    os << "uint16";
  } else if (pt == gs::PropertyType::Int32()) {
    os << "int32";
  } else if (pt == gs::PropertyType::UInt32()) {
    os << "uint32";
  } else if (pt == gs::PropertyType::Float()) {
    os << "float";
  } else if (pt == gs::PropertyType::Int64()) {
    os << "int64";
  } else if (pt == gs::PropertyType::UInt64()) {
    os << "uint64";
  } else if (pt == gs::PropertyType::Double()) {
    os << "double";
  } else if (pt == gs::PropertyType::Date()) {
    os << "date";
  } else if (pt == gs::PropertyType::Day()) {
    os << "day";
  } else if (pt == gs::PropertyType::String()) {
    os << "string";
  } else if (pt == gs::PropertyType::StringMap()) {
    os << "string_map";
  } else if (pt.type_enum == gs::impl::PropertyTypeImpl::kVarChar) {
    os << "varchar(" << pt.additional_type_info.max_length << ")";
  } else {
    os << "unknown";
  }
  return os;
}

}  // namespace std

#endif  // GRAPHSCOPE_TYPES_H_
