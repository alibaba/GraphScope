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

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_CONTEXT_H_

#include <string>
#include <unordered_map>
#include <utility>

#include "grape/grape.h"

#include "apps/assortativity/utils.h"
#include "core/app/app_base.h"
#include "core/context/tensor_context.h"

namespace gs {

template <typename FRAG_T>
class AverageDegreeConnectivityContext : public TensorContext<FRAG_T, double> {
 public:
  explicit AverageDegreeConnectivityContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, double>(fragment) {}

  using edata_t = typename FRAG_T::edata_t;
  void Init(grape::DefaultMessageManager& messages,
            std::string source_degree_type = "in+out",
            std::string target_degree_type = "in+out") {
    merge_stage = false;
    this->directed = this->fragment().directed();
    if (source_degree_type == "in") {
      source_degree_type_ = DegreeType::IN;
    } else if (source_degree_type == "out") {
      source_degree_type_ = DegreeType::OUT;
    } else if (source_degree_type == "in+out") {
      source_degree_type_ = DegreeType::INANDOUT;
    } else {
      LOG(FATAL) << "Invalid parameter source_degree_type: "
                 << source_degree_type;
    }
    if (target_degree_type == "in") {
      target_degree_type_ = DegreeType::IN;
    } else if (target_degree_type == "out") {
      target_degree_type_ = DegreeType::OUT;
    } else if (target_degree_type == "in+out") {
      target_degree_type_ = DegreeType::INANDOUT;
    } else {
      LOG(FATAL) << "Invalid parameter target_degree_type: "
                 << target_degree_type;
    }
    if (std::is_same<edata_t, grape::EmptyType>::value) {
      weighted = false;
    } else {
      weighted = true;
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    if (frag.fid() == 0) {
      for (auto& a : degree_connectivity_map) {
        os << a.first << ": " << a.second.first << std::endl;
      }
    }
  }
  bool merge_stage;
  bool directed;
  bool weighted;
  DegreeType source_degree_type_;
  DegreeType target_degree_type_;
  // <degree, <sum of neighbor's, weighted degree> pair>
  std::unordered_map<int, std::pair<double, double>> degree_connectivity_map;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_AVERAGE_DEGREE_CONNECTIVITY_CONTEXT_H_
