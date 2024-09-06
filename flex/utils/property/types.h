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

#include <yaml-cpp/yaml.h>

namespace grape {

inline bool operator<(const EmptyType& lhs, const EmptyType& rhs) {
  return false;
}

}  // namespace grape

namespace gs {

// primitive types
static constexpr const char* DT_UNSIGNED_INT8 = "DT_UNSIGNED_INT8";
static constexpr const char* DT_UNSIGNED_INT16 = "DT_UNSIGNED_INT16";
static constexpr const char* DT_SIGNED_INT32 = "DT_SIGNED_INT32";
static constexpr const char* DT_UNSIGNED_INT32 = "DT_UNSIGNED_INT32";
static constexpr const char* DT_SIGNED_INT64 = "DT_SIGNED_INT64";
static constexpr const char* DT_UNSIGNED_INT64 = "DT_UNSIGNED_INT64";
static constexpr const char* DT_BOOL = "DT_BOOL";
static constexpr const char* DT_FLOAT = "DT_FLOAT";
static constexpr const char* DT_DOUBLE = "DT_DOUBLE";
static constexpr const char* DT_STRING = "DT_STRING";
static constexpr const char* DT_STRINGMAP = "DT_STRINGMAP";
static constexpr const char* DT_DATE = "DT_DATE32";
static constexpr const char* DT_DAY = "DT_DAY32";

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
  kStringView,
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
  kVertexGlobalId,
  kLabel,
  kRecordView,
  kRecord,
  kString,
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
  std::string ToString() const;

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
  static PropertyType StringView();
  static PropertyType StringMap();
  static PropertyType Varchar(uint16_t max_length);
  static PropertyType VertexGlobalId();
  static PropertyType Label();
  static PropertyType RecordView();
  static PropertyType Record();
  static PropertyType String();

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
  static const PropertyType kStringView;
  static const PropertyType kStringMap;
  static const PropertyType kVertexGlobalId;
  static const PropertyType kLabel;
  static const PropertyType kRecordView;
  static const PropertyType kRecord;
  static const PropertyType kString;

  bool operator==(const PropertyType& other) const;
  bool operator!=(const PropertyType& other) const;
};

namespace config_parsing {
std::string PrimitivePropertyTypeToString(PropertyType type);
PropertyType StringToPrimitivePropertyType(const std::string& str);
}  // namespace config_parsing

// encoded with label_id and vid_t.
struct GlobalId {
  using label_id_t = uint8_t;
  using vid_t = uint32_t;
  using gid_t = uint64_t;
  static constexpr int32_t label_id_offset = 64 - sizeof(label_id_t) * 8;
  static constexpr uint64_t vid_mask = (1ULL << label_id_offset) - 1;

  static label_id_t get_label_id(gid_t gid);
  static vid_t get_vid(gid_t gid);

  uint64_t global_id;

  GlobalId();
  GlobalId(label_id_t label_id, vid_t vid);
  GlobalId(gid_t gid);

  label_id_t label_id() const;
  vid_t vid() const;

  std::string to_string() const;
};

inline bool operator==(const GlobalId& lhs, const GlobalId& rhs) {
  return lhs.global_id == rhs.global_id;
}

inline bool operator!=(const GlobalId& lhs, const GlobalId& rhs) {
  return lhs.global_id != rhs.global_id;
}
inline bool operator<(const GlobalId& lhs, const GlobalId& rhs) {
  return lhs.global_id < rhs.global_id;
}

inline bool operator>(const GlobalId& lhs, const GlobalId& rhs) {
  return lhs.global_id > rhs.global_id;
}

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
};

class Table;
struct Any;
struct RecordView {
  RecordView() : offset(0), table(nullptr) {}
  RecordView(size_t offset, const Table* table)
      : offset(offset), table(table) {}
  size_t size() const;
  Any operator[](size_t idx) const;

  template <typename T>
  T get_field(int col_id) const;

  size_t offset;
  const Table* table;
};

struct Any;
struct Record {
  Record() : len(0), props(nullptr) {}
  Record(size_t len);
  Record(const Record& other);
  Record(Record&& other);
  Record& operator=(const Record& other);
  Record(const std::vector<Any>& vec);
  Record(const std::initializer_list<Any>& list);
  ~Record();
  size_t size() const { return len; }
  Any operator[](size_t idx) const;
  Any* begin() const;
  Any* end() const;

  size_t len;
  Any* props;
};

struct StringPtr {
  StringPtr() : ptr(nullptr) {}
  StringPtr(const std::string& str) : ptr(new std::string(str)) {}
  StringPtr(const StringPtr& other) {
    if (other.ptr) {
      ptr = new std::string(*other.ptr);
    } else {
      ptr = nullptr;
    }
  }
  StringPtr(StringPtr&& other) : ptr(other.ptr) { other.ptr = nullptr; }
  StringPtr& operator=(const StringPtr& other) {
    if (this == &other) {
      return *this;
    }
    if (ptr) {
      delete ptr;
    }
    if (other.ptr) {
      ptr = new std::string(*other.ptr);
    } else {
      ptr = nullptr;
    }
    return *this;
  }
  ~StringPtr() {
    if (ptr) {
      delete ptr;
    }
  }
  // return string_view
  std::string_view operator*() const {
    return std::string_view((*ptr).data(), (*ptr).size());
  }
  std::string* ptr;
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
  GlobalId vertex_gid;
  LabelKey label_key;

  Date d;
  Day day;
  std::string_view s;
  double db;
  uint8_t u8;
  uint16_t u16;
  RecordView record_view;

  // Non-trivial types
  Record record;
  StringPtr s_ptr;
};

template <typename T>
struct AnyConverter;

struct Any {
  Any() : type(PropertyType::kEmpty) {}

  Any(const Any& other) : type(other.type) {
    if (type == PropertyType::kRecord) {
      new (&value.record) Record(other.value.record);
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value.s_ptr) StringPtr(other.value.s_ptr);
    } else {
      memcpy(static_cast<void*>(&value), static_cast<const void*>(&other.value),
             sizeof(AnyValue));
    }
  }

  Any(Any&& other) : type(other.type) {
    if (type == PropertyType::kRecord) {
      new (&value.record) Record(std::move(other.value.record));
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value.s_ptr) StringPtr(std::move(other.value.s_ptr));
    } else {
      memcpy(static_cast<void*>(&value), static_cast<const void*>(&other.value),
             sizeof(AnyValue));
    }
  }

  Any(const std::initializer_list<Any>& list) {
    type = PropertyType::kRecord;
    new (&value.record) Record(list);
  }
  Any(const std::vector<Any>& vec) {
    type = PropertyType::kRecord;
    new (&value.record) Record(vec);
  }

  Any(const std::string& str) {
    type = PropertyType::kString;
    new (&value.s_ptr) StringPtr(str);
  }

  template <typename T>
  Any(const T& val) {
    Any a = Any::From(val);
    type = a.type;
    if (type == PropertyType::kRecord) {
      new (&value.record) Record(a.value.record);
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value.s_ptr) StringPtr(a.value.s_ptr);
    } else {
      memcpy(static_cast<void*>(&value), static_cast<const void*>(&a.value),
             sizeof(AnyValue));
    }
  }

  Any& operator=(const Any& other) {
    if (this == &other) {
      return *this;
    }
    if (type == PropertyType::kRecord) {
      value.record.~Record();
    }
    type = other.type;
    if (type == PropertyType::kRecord) {
      new (&value.record) Record(other.value.record);
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      new (&value.s_ptr) StringPtr(other.value.s_ptr);
    } else {
      memcpy(static_cast<void*>(&value), static_cast<const void*>(&other.value),
             sizeof(AnyValue));
    }
    return *this;
  }

  ~Any() {
    if (type == PropertyType::kRecord) {
      value.record.~Record();
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      value.s_ptr.~StringPtr();
    }
  }

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

  void set_vertex_gid(GlobalId v) {
    type = PropertyType::kVertexGlobalId;
    value.vertex_gid = v;
  }

  void set_label_key(LabelKey v) {
    type = PropertyType::kLabel;
    value.label_key = v;
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

  void set_string_view(std::string_view v) {
    type = PropertyType::kStringView;
    value.s = v;
  }

  void set_string(const std::string& v) {
    type = PropertyType::kString;
    new (&value.s_ptr) StringPtr(v);
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

  void set_record_view(RecordView v) {
    type = PropertyType::kRecordView;
    value.record_view = v;
  }

  void set_record(Record v) {
    if (type == PropertyType::kRecord) {
      value.record.~Record();
    }
    type = PropertyType::kRecord;
    new (&(value.record)) Record(v);
  }

  std::string to_string() const {
    if (type == PropertyType::kInt32) {
      return std::to_string(value.i);
    } else if (type == PropertyType::kInt64) {
      return std::to_string(value.l);
    } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
      return *value.s_ptr.ptr;
    } else if (type == PropertyType::kStringView) {
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
    } else if (type == PropertyType::kVertexGlobalId) {
      return value.vertex_gid.to_string();
    } else if (type == PropertyType::kLabel) {
      return std::to_string(value.label_key.label_id);
    } else {
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(type.type_enum);
      return "";
    }
  }

  const std::string& AsString() const {
    assert(type.type_enum == impl::PropertyTypeImpl::kString);
    return *value.s_ptr.ptr;
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

  std::string_view AsStringView() const {
    assert(type == PropertyType::kStringView);
    if (type.type_enum != impl::PropertyTypeImpl::kString) {
      return value.s;
    } else {
      return *value.s_ptr.ptr;
    }
  }

  const Date& AsDate() const {
    assert(type == PropertyType::kDate);
    return value.d;
  }

  const Day& AsDay() const {
    assert(type == PropertyType::kDay);
    return value.day;
  }

  const GlobalId& AsGlobalId() const {
    assert(type == PropertyType::kVertexGlobalId);
    return value.vertex_gid;
  }

  const LabelKey& AsLabelKey() const {
    assert(type == PropertyType::kLabel);
    return value.label_key;
  }

  const RecordView& AsRecordView() const {
    assert(type == PropertyType::kRecordView);
    return value.record_view;
  }

  const Record& AsRecord() const {
    assert(type == PropertyType::kRecord);
    return value.record;
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
      } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
        return *value.s_ptr == other.AsStringView();
      } else if (type == PropertyType::kStringView) {
        return value.s == other.AsStringView();
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
      } else if (type == PropertyType::kVertexGlobalId) {
        return value.vertex_gid == other.value.vertex_gid;
      } else if (type == PropertyType::kLabel) {
        return value.label_key.label_id == other.value.label_key.label_id;
      } else if (type.type_enum == impl::PropertyTypeImpl::kVarChar) {
        if (other.type.type_enum != impl::PropertyTypeImpl::kVarChar) {
          return false;
        }
        return value.s == other.value.s;
      } else {
        return false;
      }
    } else if (type == PropertyType::kRecordView) {
      return value.record_view.offset == other.value.record_view.offset &&
             value.record_view.table == other.value.record_view.table;
    } else if (type == PropertyType::kRecord) {
      if (value.record.len != other.value.record.len) {
        return false;
      }
      for (size_t i = 0; i < value.record.len; ++i) {
        if (!(value.record.props[i] == other.value.record.props[i])) {
          return false;
        }
      }
      return true;
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
      } else if (type.type_enum == impl::PropertyTypeImpl::kString) {
        return *value.s_ptr < other.AsStringView();
      } else if (type == PropertyType::kStringView) {
        return value.s < other.AsStringView();
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
      } else if (type == PropertyType::kVertexGlobalId) {
        return value.vertex_gid < other.value.vertex_gid;
      } else if (type == PropertyType::kLabel) {
        return value.label_key.label_id < other.value.label_key.label_id;
      } else if (type == PropertyType::kRecord) {
        for (size_t i = 0; i < value.record.len; ++i) {
          if (i >= other.value.record.len) {
            return false;
          }
          if (value.record.props[i] < other.value.record.props[i]) {
            return true;
          } else if (other.value.record.props[i] < value.record.props[i]) {
            return false;
          }
        }
        return false;
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
struct ConvertAny<GlobalId> {
  static void to(const Any& value, GlobalId& out) {
    CHECK(value.type == PropertyType::kVertexGlobalId);
    out = value.value.vertex_gid;
  }
};

template <>
struct ConvertAny<LabelKey> {
  static void to(const Any& value, LabelKey& out) {
    CHECK(value.type == PropertyType::kLabel);
    out = value.value.label_key;
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
    CHECK(value.type.type_enum == impl::PropertyTypeImpl::kString);
    out = *value.value.s_ptr.ptr;
  }
};

template <>
struct ConvertAny<std::string_view> {
  static void to(const Any& value, std::string_view& out) {
    CHECK(value.type == PropertyType::kStringView);
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

template <>
struct ConvertAny<RecordView> {
  static void to(const Any& value, RecordView& out) {
    CHECK(value.type == PropertyType::kRecordView);
    out.offset = value.value.record_view.offset;
    out.table = value.value.record_view.table;
  }
};

template <>
struct ConvertAny<Record> {
  static void to(const Any& value, Record& out) {
    CHECK(value.type == PropertyType::kRecord);
    out = value.value.record;
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

  static const uint64_t& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kUInt64);
    return value.value.ul;
  }

  static const uint64_t& from_any_value(const AnyValue& value) {
    return value.ul;
  }
};

template <>
struct AnyConverter<GlobalId> {
  static PropertyType type() { return PropertyType::kVertexGlobalId; }

  static Any to_any(const GlobalId& value) {
    Any ret;
    ret.set_vertex_gid(value);
    return ret;
  }

  static const GlobalId& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kVertexGlobalId);
    return value.value.vertex_gid;
  }

  static const GlobalId& from_any_value(const AnyValue& value) {
    return value.vertex_gid;
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

  static const Day& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kDay);
    return value.value.day;
  }

  static const Day& from_any_value(const AnyValue& value) { return value.day; }
};

template <>
struct AnyConverter<std::string_view> {
  static PropertyType type() { return PropertyType::kStringView; }

  static Any to_any(const std::string_view& value) {
    Any ret;
    ret.set_string_view(value);
    return ret;
  }

  static const std::string_view& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kStringView &&
          value.type.type_enum != impl::PropertyTypeImpl::kString);
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

  static std::string& from_any(const Any& value) {
    CHECK(value.type.type_enum == impl::PropertyTypeImpl::kString);
    return *value.value.s_ptr.ptr;
  }

  static std::string& from_any_value(const AnyValue& value) {
    return *value.s_ptr.ptr;
  }
};

template <>
struct AnyConverter<grape::EmptyType> {
  static PropertyType type() { return PropertyType::kEmpty; }

  static Any to_any(const grape::EmptyType& value) {
    Any ret;
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

  static const float& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kFloat);
    return value.value.f;
  }

  static const float& from_any_value(const AnyValue& value) { return value.f; }
};

template <>
struct AnyConverter<LabelKey> {
  static PropertyType type() { return PropertyType::kLabel; }

  static Any to_any(const LabelKey& value) {
    Any ret;
    ret.set_label_key(value);
    return ret;
  }

  static const LabelKey& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kLabel);
    return value.value.label_key;
  }

  static const LabelKey& from_any_value(const AnyValue& value) {
    return value.label_key;
  }
};
Any ConvertStringToAny(const std::string& value, const gs::PropertyType& type);

template <>
struct AnyConverter<RecordView> {
  static PropertyType type() { return PropertyType::kRecordView; }

  static Any to_any(const RecordView& value) {
    Any ret;
    ret.set_record_view(value);
    return ret;
  }

  static const RecordView& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kRecordView);
    return value.value.record_view;
  }

  static const RecordView& from_any_value(const AnyValue& value) {
    return value.record_view;
  }
};

template <>
struct AnyConverter<Record> {
  static PropertyType type() { return PropertyType::kRecord; }

  static Any to_any(const Record& value) {
    Any ret;
    ret.set_record(value);
    return ret;
  }

  static const Record& from_any(const Any& value) {
    CHECK(value.type == PropertyType::kRecord);
    return value.value.record;
  }

  static const Record& from_any_value(const AnyValue& value) {
    return value.record;
  }
};

template <typename T>
T RecordView::get_field(int col_id) const {
  auto val = operator[](col_id);
  T ret{};
  ConvertAny<T>::to(val, ret);
  return ret;
}

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

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const GlobalId& value);
grape::OutArchive& operator>>(grape::OutArchive& out_archive, GlobalId& value);

}  // namespace gs

namespace boost {
// override boost hash function for EmptyType
inline std::size_t hash_value(const grape::EmptyType& value) { return 0; }
inline std::size_t hash_value(const gs::GlobalId& value) {
  return std::hash<uint64_t>()(value.global_id);
}
// overload hash_value for LabelKey
inline std::size_t hash_value(const gs::LabelKey& key) {
  return std::hash<int32_t>()(key.label_id);
}

}  // namespace boost

namespace std {

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
  } else if (pt == gs::PropertyType::StringView()) {
    os << "string";
  } else if (pt == gs::PropertyType::StringMap()) {
    os << "string_map";
  } else if (pt.type_enum == gs::impl::PropertyTypeImpl::kVarChar) {
    os << "varchar(" << pt.additional_type_info.max_length << ")";
  } else if (pt == gs::PropertyType::VertexGlobalId()) {
    os << "vertex_global_id";
  } else {
    os << "unknown";
  }
  return os;
}

template <>
struct hash<gs::GlobalId> {
  size_t operator()(const gs::GlobalId& value) const {
    return std::hash<uint64_t>()(value.global_id);
  }
};

}  // namespace std

namespace grape {
inline bool operator==(const EmptyType& a, const EmptyType& b) { return true; }

inline bool operator!=(const EmptyType& a, const EmptyType& b) { return false; }
}  // namespace grape

namespace YAML {
template <>
struct convert<gs::PropertyType> {
  // concurrently preseve backwards compatibility with old config files
  static bool decode(const Node& config, gs::PropertyType& property_type) {
    if (config["primitive_type"]) {
      property_type = gs::config_parsing::StringToPrimitivePropertyType(
          config["primitive_type"].as<std::string>());
    } else if (config["string"]) {
      if (config["string"].IsMap()) {
        if (config["string"]["long_text"]) {
          property_type = gs::PropertyType::StringView();
        } else if (config["string"]["var_char"]) {
          if (config["string"]["var_char"]["max_length"]) {
            property_type = gs::PropertyType::Varchar(
                config["string"]["var_char"]["max_length"].as<int32_t>());
          }
          property_type = gs::PropertyType::Varchar(
              gs::PropertyType::STRING_DEFAULT_MAX_LENGTH);
        } else {
          LOG(ERROR) << "Unrecognized string type";
        }
      } else {
        LOG(ERROR) << "string should be a map";
      }
    } else if (config["temporal"]) {
      if (config["temporal"]["date32"]) {
        property_type = gs::PropertyType::Day();
      } else if (config["temporal"]["timestamp"]) {
        property_type = gs::PropertyType::Date();
      } else {
        LOG(ERROR) << "Unrecognized temporal type";
      }
    }
    // compatibility with old config files
    else if (config["day"]) {
      property_type = gs::config_parsing::StringToPrimitivePropertyType(
          config["day"].as<std::string>());
    } else if (config["varchar"]) {
      if (config["varchar"]["max_length"]) {
        property_type = gs::PropertyType::Varchar(
            config["varchar"]["max_length"].as<int32_t>());
      } else {
        property_type = gs::PropertyType::Varchar(
            gs::PropertyType::STRING_DEFAULT_MAX_LENGTH);
      }
    } else if (config["date"]) {
      property_type = gs::PropertyType::Date();
    } else {
      LOG(ERROR) << "Unrecognized property type: " << config;
      return false;
    }
    return true;
  }

  static Node encode(const gs::PropertyType& type) {
    YAML::Node node;
    if (type == gs::PropertyType::Bool() || type == gs::PropertyType::Int32() ||
        type == gs::PropertyType::UInt32() ||
        type == gs::PropertyType::Float() ||
        type == gs::PropertyType::Int64() ||
        type == gs::PropertyType::UInt64() ||
        type == gs::PropertyType::Double()) {
      node["primitive_type"] =
          gs::config_parsing::PrimitivePropertyTypeToString(type);
    } else if (type == gs::PropertyType::StringView() ||
               type == gs::PropertyType::StringMap()) {
      node["string"]["long_text"] = "";
    } else if (type.IsVarchar()) {
      node["string"]["var_char"]["max_length"] =
          type.additional_type_info.max_length;
    } else if (type == gs::PropertyType::Date()) {
      node["temporal"]["timestamp"] = "";
    } else if (type == gs::PropertyType::Day()) {
      node["temporal"]["date32"] = "";
    } else {
      LOG(ERROR) << "Unrecognized property type: " << type;
    }
    return node;
  }
};
}  // namespace YAML

#endif  // GRAPHSCOPE_TYPES_H_
