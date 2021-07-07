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

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_DEGREE_DEGREE_ASSORTATIVITY_COEFFICIENT_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_DEGREE_DEGREE_ASSORTATIVITY_COEFFICIENT_CONTEXT_H_

#include <limits>
#include <string>
#include <unordered_map>

#include "grape/grape.h"

#include "core/app/app_base.h"
#include "core/context/tensor_context.h"

namespace gs {
enum class DegreeType { IN, OUT };
template <typename FRAG_T>
class DegreeAssortativityContext : public TensorContext<FRAG_T, double> {
 public:
  // using vid_t = typename FRAG_T::vid_t;
  explicit DegreeAssortativityContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, double>(fragment) {}

  void Init(grape::DefaultMessageManager& messages,
            std::string source_degree_type = "out",
            std::string target_degree_type = "in", bool directed = false) {
    merge_stage = false;
    max_degree = 0;
    this->directed = directed;
    if (source_degree_type == "in") {
      source_degree_type_ = DegreeType::IN;
    } else if (source_degree_type == "out") {
      source_degree_type_ = DegreeType::OUT;
    } else {
      LOG(FATAL) << "invalid parameter source_degree_type: "
                 << source_degree_type;
    }
    if (target_degree_type == "in") {
      target_degree_type_ = DegreeType::IN;
    } else if (target_degree_type == "out") {
      target_degree_type_ = DegreeType::OUT;
    } else {
      LOG(FATAL) << "invalid parameter target_degree_type: "
                 << target_degree_type;
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    if(frag.fid() == 0){
      os << degree_assortativity << std::endl;
    }
  }
  std::unordered_map<std::pair<int, int>, int> degree_mixing_map;
  bool merge_stage;
  int max_degree;
  bool directed;
  DegreeType source_degree_type_;
  DegreeType target_degree_type_;
  double degree_assortativity;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_DEGREE_DEGREE_ASSORTATIVITY_COEFFICIENT_CONTEXT_H_
