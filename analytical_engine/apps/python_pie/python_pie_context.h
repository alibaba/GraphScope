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

#ifndef ANALYTICAL_ENGINE_APPS_PYTHON_PIE_PYTHON_PIE_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_PYTHON_PIE_PYTHON_PIE_CONTEXT_H_

#include <string>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

#include "grape/grape.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/context/vertex_data_context.h"

namespace gs {
template <typename FRAG_T>
class PropertyAutoMessageManager;

template <typename FRAG_T, typename COMPUTE_CONTEXT_T>
class PIEContext
    : public LabeledVertexDataContext<FRAG_T,
                                      typename COMPUTE_CONTEXT_T::vd_t> {
  using vid_t = typename FRAG_T::vid_t;
  using vd_t = typename COMPUTE_CONTEXT_T::vd_t;
  using fragment_t = FRAG_T;
  using label_id_t = typename fragment_t::label_id_t;

 public:
  explicit PIEContext(const fragment_t& fragment)
      : LabeledVertexDataContext<FRAG_T, typename COMPUTE_CONTEXT_T::vd_t>(
            fragment),
        compute_context_(this->data()) {}

  void Init(PropertyAutoMessageManager<FRAG_T>& messages,
            const std::string& args) {
    auto& frag = this->fragment();

    compute_context_.init(frag);
    compute_context_.set_fragment(&frag);
    compute_context_.set_message_manager(&messages);

    // The app params are passed via serialized json string.
    if (!args.empty()) {
      boost::property_tree::ptree pt;
      std::stringstream ss;
      ss << args;
      boost::property_tree::read_json(ss, pt);
      for (const auto& x : pt) {
        compute_context_.set_config(x.first, x.second.get_value<std::string>());
      }
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    for (label_id_t i = 0; i < frag.vertex_label_num(); ++i) {
      auto& result = compute_context_.partial_result(i);
      auto iv = frag.InnerVertices(i);
      for (auto v : iv) {
        auto d = result[v];
        os << frag.GetId(v) << "\t" << d << std::endl;
      }
    }
  }

  COMPUTE_CONTEXT_T compute_context_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PYTHON_PIE_PYTHON_PIE_CONTEXT_H_
