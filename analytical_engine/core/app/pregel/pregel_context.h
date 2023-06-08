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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_CONTEXT_H_

#include <string>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

#include "grape/grape.h"

#include "core/context/vertex_data_context.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

namespace gs {

/**
 * @brief PregelContext holds the computation result with
 * grape::VertexDataContext.
 * @tparam FRAG_T
 * @tparam COMPUTE_CONTEXT_T
 */
template <typename FRAG_T, typename COMPUTE_CONTEXT_T>
class PregelContext
    : public grape::VertexDataContext<FRAG_T,
                                      typename COMPUTE_CONTEXT_T::vd_t> {
  using vid_t = typename FRAG_T::vid_t;
  using vd_t = typename COMPUTE_CONTEXT_T::vd_t;
  using fragment_t = FRAG_T;

 public:
  explicit PregelContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, typename COMPUTE_CONTEXT_T::vd_t>(
            fragment),
        compute_context_(this->data()) {}

  void Init(grape::DefaultMessageManager& messages, const std::string& args) {
    auto& frag = this->fragment();

    compute_context_.init(frag);
    compute_context_.set_fragment(&frag);
    compute_context_.set_message_manager(&messages);

    if (!args.empty()) {
      // The app params are passed via serialized json string.
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
    auto& result = compute_context_.vertex_data();
    auto iv = frag.InnerVertices();
    for (auto v : iv) {
      auto d = result[v];
      os << frag.GetId(v) << " " << d << std::endl;
    }
  }

  COMPUTE_CONTEXT_T compute_context_;
};

/**
 * @brief This is a specialized PregelContext for the labeled graph. The data
 * attached to the vertices are stored in gs::LabeledVertexDataContext.
 *
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 * @tparam COMPUTE_CONTEXT_T
 */
template <typename OID_T, typename VID_T, typename VERTEX_MAP_T,
          /* bool COMPACT, */  // TODO(tao): support compact CSR
          typename COMPUTE_CONTEXT_T>
class PregelContext<vineyard::ArrowFragment<OID_T, VID_T, VERTEX_MAP_T>,
                    COMPUTE_CONTEXT_T>
    : public LabeledVertexDataContext<
          vineyard::ArrowFragment<OID_T, VID_T, VERTEX_MAP_T>,
          typename COMPUTE_CONTEXT_T::vd_t> {
  using fragment_t = vineyard::ArrowFragment<OID_T, VID_T, VERTEX_MAP_T>;
  using vid_t = typename fragment_t::vid_t;
  using vd_t = typename COMPUTE_CONTEXT_T::vd_t;
  using label_id_t = typename fragment_t::label_id_t;

 public:
  explicit PregelContext(const fragment_t& fragment)
      : LabeledVertexDataContext<fragment_t, vd_t>(fragment),
        compute_context_(this->data(), fragment.schema()) {}

  void Init(grape::DefaultMessageManager& messages, const std::string& args) {
    auto& frag = this->fragment();
    compute_context_.init(frag);
    compute_context_.set_fragment(&frag);
    compute_context_.set_message_manager(&messages);

    if (!args.empty()) {
      // The app params are passed via serialized json string.
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
      auto& result = compute_context_.vertex_data(i);
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

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_CONTEXT_H_
