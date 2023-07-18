/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_CORE_OBJECT_DYNAMIC_H_
#define ANALYTICAL_ENGINE_CORE_OBJECT_DYNAMIC_H_
#ifdef NETWORKX

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <utility>

// IWYU pragma: begin_exports
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
// IWYU pragma: end_exports

#include "grape/serialization/in_archive.h"
#include "proto/graph_def.pb.h"

namespace gs {

namespace dynamic {

using AllocatorT = rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator>;

// More supported types than rapidjson::Type.
enum Type {
  kNullType = 0,
  kBoolType = 1,
  kObjectType = 3,
  kArrayType = 4,
  kStringType = 5,
  kInt64Type = 6,
  kDoubleType = 7,
  kInt32Type = 8,
};

// An inherit class of rapidjson::Value to support more features.
class Value : public rapidjson::Value {
  using Base = rapidjson::Value;

 public:
  // Default constructor to create a null value.
  Value() noexcept : Base() {}
  // Copy constructor
  Value(const Value& rhs) { Base::CopyFrom(rhs, allocator_); }
  explicit Value(const rapidjson::Value& rhs) {
    Base::CopyFrom(rhs, allocator_);
  }
  // Constructor with move semantics.
  Value(Value& rhs) { Base::CopyFrom(rhs, allocator_); }
  explicit Value(rapidjson::Value& rhs) : Base(std::move(rhs)) {}
  // Move constructor
  Value(Value&& rhs) noexcept : Base(std::move(rhs)) {}
  explicit Value(rapidjson::Value&& rhs) : Base(std::move(rhs)) {}
  // Constructor with value type
  explicit Value(rapidjson::Type type) noexcept : Base(type) {}
  // Constructor for common type
  explicit Value(bool b) noexcept : Base(b) {}
  explicit Value(int i) noexcept : Base(i) {}
  explicit Value(unsigned u) noexcept : Base(u) {}
  explicit Value(int64_t i64) noexcept : Base(i64) {}
  explicit Value(uint64_t u64) noexcept : Base(u64) {}
  explicit Value(double d) noexcept : Base(d) {}
  explicit Value(float f) noexcept : Base(f) {}

  // Destructor
  ~Value() = default;

  // Copy assignment operator.
  Value& operator=(const Value& rhs) {
    if (this != &rhs) {
      Base::CopyFrom(rhs, allocator_);
    }
    return *this;
  }
  Value& operator=(const rapidjson::Value& rhs) {
    Base::CopyFrom(rhs, allocator_);
    return *this;
  }

  // Move assignment.
  Value& operator=(Value&& rhs) noexcept {
    if (this != &rhs) {
      Base::operator=(rhs.Move());
    }
    return *this;
  }
  Value& operator=(rapidjson::Value&& rhs) noexcept {
    Base::operator=(rhs);
    return *this;
  }
  Value& operator=(rapidjson::Value& rhs) noexcept {
    Base::operator=(rhs);
    return *this;
  }

  bool operator<(const Value& rhs) const {
    if (Base::GetType() != rhs.GetType()) {
      return Base::GetType() < rhs.GetType();
    }
    if (Base::IsInt64()) {
      return Base::GetInt64() < rhs.GetInt64();
    } else if (Base::IsDouble()) {
      return Base::GetDouble() < rhs.GetDouble();
    } else if (Base::IsString()) {
      return Base::GetString() < rhs.GetString();
    }
    return false;
  }
  bool operator>=(const Value& rhs) const {
    if (Base::GetType() != rhs.GetType()) {
      return Base::GetType() >= rhs.GetType();
    }
    if (Base::IsInt64()) {
      return Base::GetInt64() >= rhs.GetInt64();
    } else if (Base::IsDouble()) {
      return Base::GetDouble() >= rhs.GetDouble();
    } else if (Base::IsString()) {
      return Base::GetString() >= rhs.GetString();
    }
    return false;
  }

  // Constructor for copy-string from a string object (i.e. do make a copy of
  // string)
  explicit Value(const std::string& s) : Base(s.c_str(), allocator_) {}
  explicit Value(const char* s) : Base(s, allocator_) {}

  void CopyFrom(const Value& rhs, AllocatorT& allocator = allocator_) {
    if (this != &rhs) {
      Base::CopyFrom(rhs, allocator);
    }
  }

  // Insert for object
  template <typename T>
  void Insert(const std::string& key, T&& value) {
    Value v_(value);
    Base::AddMember(rapidjson::Value(key, allocator_).Move(), v_, allocator_);
  }

  void Insert(const std::string& key, Value& value) {
    Base::AddMember(rapidjson::Value(key, allocator_).Move(), value,
                    allocator_);
  }

  void Insert(const std::string& key, rapidjson::Value& value) {
    Base::AddMember(rapidjson::Value(key, allocator_).Move(), value,
                    allocator_);
  }

  // Update for object
  void Update(rapidjson::Value& value) {
    if (!value.IsObject() || value.ObjectEmpty()) {
      return;
    }
    for (auto member = value.MemberBegin(); member != value.MemberEnd();
         ++member) {
      auto dst_member = Base::FindMember(member->name);
      if (dst_member == Base::MemberEnd()) {
        Base::AddMember(member->name, member->value, allocator_);
      } else {
        Base::operator[](member->name) = member->value;
      }
    }
  }

  // Update with move semantics
  void Update(Value&& rhs) {
    if (!rhs.IsObject() || rhs.ObjectEmpty()) {
      return;
    }
    for (auto member = rhs.MemberBegin(); member != rhs.MemberEnd(); ++member) {
      auto dst_member = Base::FindMember(member->name);
      if (dst_member == Base::MemberEnd()) {
        Base::AddMember(member->name, member->value, allocator_);
      } else {
        Base::operator[](member->name) = member->value;
      }
    }
  }

  // Update with copy semantics
  void Update(const Value& rhs) {
    if (!rhs.IsObject() || rhs.ObjectEmpty()) {
      return;
    }
    Value value(rhs);
    Update(std::move(value));
  }

  // PushBack for array
  template <typename T>
  Value& PushBack(T value) {
    Base::PushBack(value, allocator_);
    return *this;
  }

  Value& PushBack(const std::string& str) {
    Base::PushBack(Value(str).Move(), allocator_);
    return *this;
  }

  Value& SetString(const std::string& str) {
    Base::SetString(str, allocator_);
    return *this;
  }

  // hash
  std::size_t hash() const;

  const rapidjson::Value* begin() const { return Base::GetArray().begin(); }
  const rapidjson::Value* end() const { return Base::GetArray().end(); }

  friend std::ostream& operator<<(std::ostream&, Value const&);

 public:
  static AllocatorT allocator_;
};

// Stringify Value to json.
static inline const char* Stringify(const rapidjson::Value& value) {
  static rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  buffer.Clear();
  value.Accept(writer);
  return buffer.GetString();
}

// Parse json to Value.
static inline void Parse(const std::string& str, rapidjson::Value& val) {
  // the document d must use the same allocator with other values
  rapidjson::Document d(&Value::allocator_);
  d.Parse(str.c_str());
  val.Swap(d);  // constant time
}

// Get Value's type.
static inline Type GetType(const rapidjson::Value& val) {
  switch (val.GetType()) {
  case rapidjson::kTrueType:
  case rapidjson::kFalseType:
    return Type::kBoolType;
  case rapidjson::kNumberType: {
    return val.IsDouble() ? Type::kDoubleType : Type::kInt64Type;
  }
  default:
    return static_cast<Type>(val.GetType());
  }
}

// Type string to Type
static inline rpc::graph::DataTypePb Str2RpcType(const std::string& s) {
  static const std::map<std::string, rpc::graph::DataTypePb> str2type = {
      {"NULL", rpc::graph::DataTypePb::NULLVALUE},
      {"null", rpc::graph::DataTypePb::NULLVALUE},
      {"BOOL", rpc::graph::DataTypePb::BOOL},
      {"bool", rpc::graph::DataTypePb::BOOL},
      {"boolean", rpc::graph::DataTypePb::BOOL},
      {"INT", rpc::graph::DataTypePb::INT},
      {"int", rpc::graph::DataTypePb::INT},
      {"int32", rpc::graph::DataTypePb::INT},
      {"int32_t", rpc::graph::DataTypePb::INT},
      {"LONG", rpc::graph::DataTypePb::LONG},
      {"long", rpc::graph::DataTypePb::LONG},
      {"int64", rpc::graph::DataTypePb::LONG},
      {"int64_t", rpc::graph::DataTypePb::LONG},
      {"FLOAT", rpc::graph::DataTypePb::DOUBLE},
      {"float", rpc::graph::DataTypePb::DOUBLE},
      {"float32", rpc::graph::DataTypePb::DOUBLE},
      {"DOUBLE", rpc::graph::DataTypePb::DOUBLE},
      {"double", rpc::graph::DataTypePb::DOUBLE},
      {"float64", rpc::graph::DataTypePb::DOUBLE},
      {"STRING", rpc::graph::DataTypePb::STRING},
      {"str", rpc::graph::DataTypePb::STRING},
      {"string", rpc::graph::DataTypePb::STRING},
  };
  return str2type.at(s);
}

static inline rpc::graph::DataTypePb DynamicType2RpcType(const Type& t) {
  static const std::map<Type, rpc::graph::DataTypePb> type2type = {
      {Type::kNullType, rpc::graph::DataTypePb::NULLVALUE},
      {Type::kBoolType, rpc::graph::DataTypePb::BOOL},
      {Type::kInt32Type, rpc::graph::DataTypePb::INT},
      {Type::kInt64Type, rpc::graph::DataTypePb::LONG},
      {Type::kDoubleType, rpc::graph::DataTypePb::DOUBLE},
      {Type::kStringType, rpc::graph::DataTypePb::STRING},
      {Type::kArrayType, rpc::graph::DataTypePb::INT_LIST},
      {Type::kObjectType, rpc::graph::DataTypePb::DYNAMIC}};
  return type2type.at(t);
}

inline std::ostream& operator<<(std::ostream& out, Value const& val) {
  out << Stringify(val);
  return out;
}

}  // namespace dynamic
}  // namespace gs

namespace std {

template <>
struct hash<::gs::dynamic::Value> {
  std::size_t operator()(::gs::dynamic::Value const& d) const {
    return d.hash();
  }
};

}  // namespace std

namespace grape {
inline grape::InArchive& operator<<(grape::InArchive& archive,
                                    const rapidjson::Value& value) {
  if (value.IsInt64()) {
    archive << value.GetInt64();
  } else if (value.IsDouble()) {
    archive << value.GetDouble();
  } else if (value.IsString()) {
    std::size_t size = value.GetStringLength();
    archive << size;
    archive.AddBytes(value.GetString(), size);
  } else {
    std::string json = gs::dynamic::Stringify(value);
    archive << json;
  }
  return archive;
}

inline grape::InArchive& operator<<(grape::InArchive& archive,
                                    const gs::dynamic::Value& value) {
  if (value.IsInt64()) {
    archive << value.GetInt64();
  } else if (value.IsDouble()) {
    archive << value.GetDouble();
  } else if (value.IsString()) {
    std::size_t size = value.GetStringLength();
    archive << size;
    archive.AddBytes(value.GetString(), size);
  } else {
    std::string json = gs::dynamic::Stringify(value);
    archive << json;
  }
  return archive;
}
}  // namespace grape

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_CORE_OBJECT_DYNAMIC_H_
