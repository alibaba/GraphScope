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

#include <any>
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
  kEmpty,
  kInt8,
  kUInt8,
  kInt16,
  kUInt16,
  kInt32,
  kUInt32,
  kInt64,
  kUInt64,
  kDate,
  kFloat,
  kDouble,
  kString,
  kStringView,
  kList,
};

template <typename T>
struct AnyConverter {};

template <>
struct AnyConverter<grape::EmptyType> {
  static constexpr PropertyType type = PropertyType::kEmpty;
};

template <>
struct AnyConverter<int8_t> {
  static constexpr PropertyType type = PropertyType::kInt8;
};

template <>
struct AnyConverter<uint8_t> {
  static constexpr PropertyType type = PropertyType::kUInt8;
};

template <>
struct AnyConverter<int16_t> {
  static constexpr PropertyType type = PropertyType::kInt16;
};

template <>
struct AnyConverter<uint16_t> {
  static constexpr PropertyType type = PropertyType::kUInt16;
};

template <>
struct AnyConverter<int32_t> {
  static constexpr PropertyType type = PropertyType::kInt32;
};

template <>
struct AnyConverter<uint32_t> {
  static constexpr PropertyType type = PropertyType::kUInt32;
};

template <>
struct AnyConverter<int64_t> {
  static constexpr PropertyType type = PropertyType::kInt64;
};

template <>
struct AnyConverter<uint64_t> {
  static constexpr PropertyType type = PropertyType::kUInt64;
};

struct Date {
  Date() = default;
  ~Date() = default;
  Date(const Date& rhs);
  Date(int64_t x);
  Date(const char* str);

  void reset(const char* str);
  std::string to_string() const;

  int64_t milli_second;
};

grape::InArchive& operator<<(grape::InArchive& arc, const Date& v);
grape::OutArchive& operator>>(grape::OutArchive& arc, Date& v);

template <>
struct AnyConverter<Date> {
  static constexpr PropertyType type = PropertyType::kDate;
};

template <>
struct AnyConverter<float> {
  static constexpr PropertyType type = PropertyType::kFloat;
};

template <>
struct AnyConverter<double> {
  static constexpr PropertyType type = PropertyType::kDouble;
};

template <>
struct AnyConverter<std::string> {
  static constexpr PropertyType type = PropertyType::kString;
};

template <>
struct AnyConverter<std::string_view> {
  static constexpr PropertyType type = PropertyType::kStringView;
};

class Property;

template <>
struct AnyConverter<std::vector<Property>> {
  static constexpr PropertyType type = PropertyType::kList;
};

class Property {
 public:
  Property() : type_(PropertyType::kEmpty) {}
  ~Property() {}

  Property(const Property& rhs) : type_(rhs.type_), value_(rhs.value_) {}

  Property(Property&& rhs) noexcept
      : type_(rhs.type_), value_(std::move(rhs.value_)) {}

  Property& operator=(const Property& rhs) {
    type_ = rhs.type_;
    value_ = rhs.value_;
    return *this;
  }

  Property& operator=(Property&& rhs) noexcept {
    type_ = rhs.type_;
    value_ = std::move(rhs.value_);
    return *this;
  }

  template <typename T>
  Property& operator=(const T& val) {
    set_value(val);
    return *this;
  }

  template <typename T>
  Property& operator=(T&& val) {
    set_value(std::move(val));
    return *this;
  }

  template <typename T, std::enable_if_t<!std::is_same_v<Property, T>, int> = 0>
  void set_value(const T& val) {
    type_ = AnyConverter<T>::type;
    value_ = val;
  }

  template <typename T, std::enable_if_t<std::is_same_v<Property, T>, int> = 0>
  void set_value(const T& val) {
    type_ = val.type_;
    value_ = val.value_;
  }

  template <typename T, std::enable_if_t<!std::is_same_v<Property, T>, int> = 0>
  void set_value(T&& val) {
    type_ = AnyConverter<T>::type;
    value_ = std::move(val);
  }

  template <typename T, std::enable_if_t<std::is_same_v<Property, T>, int> = 0>
  void set_value(T&& val) {
    type_ = val.type_;
    value_ = std::move(val.value_);
  }

  template <typename T>
  static Property From(const T& val) {
    Property ret;
    ret.set_value(val);
    return ret;
  }

  template <typename T>
  T get_value() const {
    return std::any_cast<T>(value_);
  }

  PropertyType type() const { return type_; }

  void set_type(PropertyType type) { type_ = type; }

  bool empty() const { return type_ == PropertyType::kEmpty; }

  void clear() {
    type_ = PropertyType::kEmpty;
    value_ = std::any();
  }

  void swap(Property& rhs) {
    std::swap(type_, rhs.type_);
    std::swap(value_, rhs.value_);
  }

  bool operator==(const Property& rhs) const {
    if (type_ != rhs.type_) {
      return false;
    }
    switch (type_) {
    case PropertyType::kEmpty:
      return true;
    case PropertyType::kUInt8:
      return get_value<uint8_t>() == rhs.get_value<uint8_t>();
    case PropertyType::kInt8:
      return get_value<int8_t>() == rhs.get_value<int8_t>();
    case PropertyType::kUInt16:
      return get_value<uint16_t>() == rhs.get_value<uint16_t>();
    case PropertyType::kInt16:
      return get_value<int16_t>() == rhs.get_value<int16_t>();
    case PropertyType::kUInt32:
      return get_value<uint32_t>() == rhs.get_value<uint32_t>();
    case PropertyType::kInt32:
      return get_value<int32_t>() == rhs.get_value<int32_t>();
    case PropertyType::kUInt64:
      return get_value<uint64_t>() == rhs.get_value<uint64_t>();
    case PropertyType::kInt64:
      return get_value<int64_t>() == rhs.get_value<int64_t>();
    case PropertyType::kDate:
      return get_value<Date>().milli_second ==
             rhs.get_value<Date>().milli_second;
    case PropertyType::kFloat:
      return get_value<float>() == rhs.get_value<float>();
    case PropertyType::kDouble:
      return get_value<double>() == rhs.get_value<double>();
    case PropertyType::kString:
      return get_value<std::string>() == rhs.get_value<std::string>();
    case PropertyType::kStringView:
      return get_value<std::string_view>() == rhs.get_value<std::string_view>();
    case PropertyType::kList:
      return get_value<std::vector<Property>>() ==
             rhs.get_value<std::vector<Property>>();
    default:
      return false;
    }
  }

  bool operator!=(const Property& rhs) const { return !(*this == rhs); }

 private:
  PropertyType type_;
  std::any value_;
};

void ParseRecord(const char* line, std::vector<Property>& rec);

void ParseRecord(const char* line, int64_t& id, std::vector<Property>& rec);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, int64_t& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst, Date& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  grape::EmptyType& prop);

void ParseRecordX(const char* line, int64_t& src, int64_t& dst,
                  std::vector<Property>& rec);

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const Property& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive, Property& value);

}  // namespace gs

namespace std {

inline ostream& operator<<(ostream& os, const gs::Date& dt) {
  os << dt.to_string();
  return os;
}

inline ostream& operator<<(ostream& os, gs::PropertyType pt) {
  switch (pt) {
  case gs::PropertyType::kEmpty:
    os << "empty";
    break;
  case gs::PropertyType::kUInt8:
    os << "uint8";
    break;
  case gs::PropertyType::kInt8:
    os << "int8";
    break;
  case gs::PropertyType::kUInt16:
    os << "uint16";
    break;
  case gs::PropertyType::kInt16:
    os << "int16";
    break;
  case gs::PropertyType::kUInt32:
    os << "uint32";
    break;
  case gs::PropertyType::kInt32:
    os << "int32";
    break;
  case gs::PropertyType::kUInt64:
    os << "uint64";
    break;
  case gs::PropertyType::kInt64:
    os << "int64";
    break;
  case gs::PropertyType::kDate:
    os << "Date";
    break;
  case gs::PropertyType::kFloat:
    os << "float";
    break;
  case gs::PropertyType::kDouble:
    os << "double";
    break;
  case gs::PropertyType::kString:
    os << "string";
    break;
  case gs::PropertyType::kStringView:
    os << "string_view";
    break;
  case gs::PropertyType::kList:
    os << "list";
    break;
  default:
    os << "Unrecoginzed type";
    break;
  }
  return os;
}

}  // namespace std

#endif  // GRAPHSCOPE_TYPES_H_
