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

#ifndef ANALYTICAL_ENGINE_CORE_UTILS_MSGPACK_UTILS_H_
#define ANALYTICAL_ENGINE_CORE_UTILS_MSGPACK_UTILS_H_

#ifdef NETWORKX

#include "msgpack.hpp"  // IWYU pragma: export

#include "grape/serialization/in_archive.h"

#include "core/object/dynamic.h"

namespace grape {
inline InArchive& operator<<(InArchive& in_archive,
                             const msgpack::sbuffer& buf) {
  in_archive << buf.size();
  in_archive.AddBytes(buf.data(), buf.size());
  return in_archive;
}
}  // namespace grape

// clang-format off

// The implementation references code from xpol/xchange:
// https://github.com/xpol/xchange/blob/master/src/msgpack/type/rapidjson.hpp
namespace msgpack {

MSGPACK_API_VERSION_NAMESPACE(v1) {
namespace adaptor {

// Serialize rapidjson::Value to bytes with msgpack
template <typename Encoding, typename Allocator>
struct pack<rapidjson::GenericValue<Encoding, Allocator>> {
  template <typename Stream>
  msgpack::packer<Stream>& operator()(
      msgpack::packer<Stream>& o,
      rapidjson::GenericValue<Encoding, Allocator> const& v) const {
    switch (v.GetType()) {
    case rapidjson::kNullType:
      return o.pack_nil();
    case rapidjson::kFalseType:
      return o.pack_false();
    case rapidjson::kTrueType:
      return o.pack_true();
    case rapidjson::kObjectType: {
      o.pack_map(v.MemberCount());
      typename rapidjson::GenericValue<
          Encoding, Allocator>::ConstMemberIterator i = v.MemberBegin(),
                                                    END = v.MemberEnd();
      for (; i != END; ++i) {
        o.pack_str(i->name.GetStringLength())
            .pack_str_body(i->name.GetString(), i->name.GetStringLength());
        o.pack(i->value);
      }
      return o;
    }
    case rapidjson::kArrayType: {
      o.pack_array(v.Size());
      for (const auto& val : v.GetArray()) {
        o.pack(val);
      }
      return o;
    }
    case rapidjson::kStringType:
      return o.pack_str(v.GetStringLength())
          .pack_str_body(v.GetString(), v.GetStringLength());
    case rapidjson::kNumberType:
      if (v.IsInt()) {
        return o.pack_int(v.GetInt());
      }
      if (v.IsUint()) {
        return o.pack_unsigned_int(v.GetUint());
      }
      if (v.IsInt64()) {
        return o.pack_int64(v.GetInt64());
      }
      if (v.IsUint64()) {
        return o.pack_uint64(v.GetUint64());
      }
      if (v.IsDouble() || v.IsNumber())
        return o.pack_double(v.GetDouble());
    default:
      return o;
    }
  }
};

// Serialize dynamic::Value to bytes with msgpack
template <>
struct pack<gs::dynamic::Value> {
  template <typename Stream>
  msgpack::packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                      gs::dynamic::Value const& v) const {
    switch (v.GetType()) {
    case rapidjson::kNullType:
      return o.pack_nil();
    case rapidjson::kFalseType:
      return o.pack_false();
    case rapidjson::kTrueType:
      return o.pack_true();
    case rapidjson::kObjectType: {
      o.pack_map(v.MemberCount());
      for (auto i = v.MemberBegin(); i != v.MemberEnd(); ++i) {
        o.pack_str(i->name.GetStringLength())
            .pack_str_body(i->name.GetString(), i->name.GetStringLength());
        o.pack(i->value);
      }
      return o;
    }
    case rapidjson::kArrayType: {
      o.pack_array(v.Size());
      for (const auto& val : v.GetArray()) {
        o.pack(val);
      }
      return o;
    }
    case rapidjson::kStringType:
      return o.pack_str(v.GetStringLength())
          .pack_str_body(v.GetString(), v.GetStringLength());
    case rapidjson::kNumberType:
      if (v.IsInt()) {
        return o.pack_int(v.GetInt());
      }
      if (v.IsUint()) {
        return o.pack_unsigned_int(v.GetUint());
      }
      if (v.IsInt64()) {
        return o.pack_int64(v.GetInt64());
      }
      if (v.IsUint64()) {
        return o.pack_uint64(v.GetUint64());
      }
      if (v.IsDouble() || v.IsNumber())
        return o.pack_double(v.GetDouble());
    default:
      return o;
    }
  }
};
}  // namespace adaptor
}  // MSGPACK_API_VERSION_NAMESPACE

}  // namespace msgpack
// clang-format on

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_CORE_UTILS_MSGPACK_UTILS_H_
