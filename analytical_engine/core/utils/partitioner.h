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

#include "core/object/dynamic.h"
#include "vineyard/graph/utils/partitioner.h"

namespace vineyard {

#if defined(NETWORKX)
template <>
class HashPartitioner<::gs::dynamic::Value> {
 public:
  using oid_t = ::gs::dynamic::Value;

  HashPartitioner() : fnum_(1) {}

  void Init(fid_t fnum) { fnum_ = fnum; }

  inline fid_t GetPartitionId(const oid_t& oid) const {
    size_t hash_value = std::hash<oid_t>()(oid);
    return static_cast<fid_t>(static_cast<uint64_t>(hash_value) % fnum_);
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

 private:
  fid_t fnum_;
};
#endif  // NETWORKX

}  // namespace vineyard
#endif  // ANALYTICAL_ENGINE_CORE_UTILS_PARTITIONER_H_
