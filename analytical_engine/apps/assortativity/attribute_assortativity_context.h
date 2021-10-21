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
 *
 * Author: Ning Xin
 */

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_ASSORTATIVITY_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_ASSORTATIVITY_CONTEXT_H_

#include <limits>
#include <string>
#include <unordered_map>
#include <utility>

#include "grape/grape.h"

#include "core/app/app_base.h"
#include "core/context/tensor_context.h"

namespace gs {
template <typename FRAG_T>
class AttributeAssortativityContext : public TensorContext<FRAG_T, double> {
 public:
  using vdata_t = typename FRAG_T::vdata_t;
  explicit AttributeAssortativityContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, double>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, bool numeric) {
    merge_stage = false;
    this->numeric = numeric;
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    if (frag.fid() == 0) {
      os << attribute_assortativity << std::endl;
    }
  }
  std::unordered_map<vdata_t, std::unordered_map<vdata_t, int>>
      attribute_mixing_map;
  double attribute_assortativity;
  bool merge_stage;
  // if true, it is numeric assortativity app else attribute assortativity app
  bool numeric;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_ASSORTATIVITY_CONTEXT_H_
