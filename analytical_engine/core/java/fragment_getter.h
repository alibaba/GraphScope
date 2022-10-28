
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_FRAGMENT_GETTER_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_FRAGMENT_GETTER_H_

#define WITH_PROFILING

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"

#include "grape/grape.h"
#include "grape/util.h"
#include "grape/worker/comm_spec.h"

#include "core/error.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "vineyard/graph/fragment/arrow_fragment_group.h"

/**
 * @brief This make us easier when obtain fragment from object id in java via
 * FFI.
 *
 */
namespace gs {

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
class ArrowProjectedFragmentGetter {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using edata_t = ED_T;

 public:
  ArrowProjectedFragmentGetter() {}
  ~ArrowProjectedFragmentGetter() {}
  std::shared_ptr<ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t>> Get(
      vineyard::Client& client, vineyard::ObjectID fragmentID) {
    auto fragment = std::dynamic_pointer_cast<
        ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t>>(
        client.GetObject(fragmentID));
    return fragment;
  }
};

class ArrowFragmentGroupGetter {
 public:
  ArrowFragmentGroupGetter() {}
  ~ArrowFragmentGroupGetter() {}

  std::shared_ptr<vineyard::ArrowFragmentGroup> Get(
      vineyard::Client& client, vineyard::ObjectID groupId) {
    auto fragment = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
        client.GetObject(groupId));
    return fragment;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_FRAGMENT_GETTER_H_
