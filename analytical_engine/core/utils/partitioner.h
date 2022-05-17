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

#ifndef ANALYTICAL_ENGINE_CORE_UTILS_PARTITIONER_H_
#define ANALYTICAL_ENGINE_CORE_UTILS_PARTITIONER_H_

#include <glog/logging.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>

#include "core/config.h"
#include "core/object/dynamic.h"

namespace grape {

template <typename OID_T>
class HashPartitioner;

#if defined(NETWORKX)
template <>
class HashPartitioner<::gs::dynamic::Value> {
 public:
  using oid_t = ::gs::dynamic::Value;

  HashPartitioner() : fnum_(1) {}
  explicit HashPartitioner(size_t frag_num) : fnum_(frag_num) {}

  inline fid_t GetPartitionId(const oid_t& oid) const {
    size_t hash_value;
    if (oid.IsArray() && oid.Size() == 2 && oid[0].IsString() &&
        (oid[1].IsInt64() || oid[1].IsString())) {
      hash_value = oid[1].IsInt64()
                       ? std::hash<int64_t>()(oid[1].GetInt64())
                       : std::hash<std::string>()(oid[1].GetString());
    } else {
      hash_value = std::hash<oid_t>()(oid);
    }
    return static_cast<fid_t>(static_cast<uint64_t>(hash_value) % fnum_);
  }

  void SetPartitionId(const oid_t& oid, fid_t fid) {
    LOG(FATAL) << "not support";
  }

  HashPartitioner& operator=(const HashPartitioner& other) {
    if (this == &other) {
      return *this;
    }
    fnum_ = other.fnum_;
    return *this;
  }

  HashPartitioner& operator=(HashPartitioner&& other) {
    if (this == &other) {
      return *this;
    }
    fnum_ = other.fnum_;
    return *this;
  }

  template <typename IOADAPTOR_T>
  void serialize(std::unique_ptr<IOADAPTOR_T>& writer) {
    CHECK(writer->Write(&fnum_, sizeof(fid_t)));
  }

  template <typename IOADAPTOR_T>
  void deserialize(std::unique_ptr<IOADAPTOR_T>& reader) {
    CHECK(reader->Read(&fnum_, sizeof(fid_t)));
  }

 private:
  fid_t fnum_;
};
#endif  // NETWORKX

}  // namespace grape
#endif  // ANALYTICAL_ENGINE_CORE_UTILS_PARTITIONER_H_
