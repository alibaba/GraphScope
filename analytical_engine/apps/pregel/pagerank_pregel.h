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

#ifndef ANALYTICAL_ENGINE_APPS_PREGEL_PAGERANK_PREGEL_H_
#define ANALYTICAL_ENGINE_APPS_PREGEL_PAGERANK_PREGEL_H_

#include <limits>

#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/app/pregel/i_vertex_program.h"
#include "core/app/pregel/pregel_compute_context.h"
#include "core/app/pregel/pregel_property_app_base.h"

namespace gs {
class PregelPagerank
    : public IPregelProgram<
          PregelPropertyVertex<
              vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                                      vineyard::property_graph_types::VID_TYPE>,
              double, double>,
          PregelPropertyComputeContext<
              vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                                      vineyard::property_graph_types::VID_TYPE>,
              double, double>> {
  using fragment_t =
      vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;

 public:
  void Init(PregelPropertyVertex<fragment_t, double, double>& v,
            PregelPropertyComputeContext<fragment_t, double, double>& context)
      override {
    v.set_value(1.0 / context.get_total_vertices_num());
  }

  void Compute(grape::IteratorPair<double*> messages,
               PregelPropertyVertex<fragment_t, double, double>& v,
               PregelPropertyComputeContext<fragment_t, double, double>&
                   context) override {
    double delta = std::stod(context.get_config("delta"));
    int max_round = std::stoi(context.get_config("max_round"));

    if (context.superstep() >= 1) {
      double sum = 0.0;
      for (auto msg : messages) {
        sum += msg;
      }
      v.set_value(delta * sum +
                  ((1 - delta) / context.get_total_vertices_num()));
    }

    if (context.superstep() < max_round) {
      size_t od_num = 0;
      for (int label_id = 0; label_id < context.edge_label_num(); label_id++) {
        od_num += v.outgoing_edges(label_id).size();
      }
      if (od_num != 0) {
        double msg = v.value() / od_num;
        for (int label_id = 0; label_id < context.edge_label_num();
             label_id++) {
          for (auto& e : v.outgoing_edges(label_id)) {
            v.send(e.vertex(), msg);
          }
        }
      }
    } else {
      v.vote_to_halt();
    }
  }
};

class PregelPagerankCombinator : public ICombinator<double> {
 public:
  double CombineMessages(MessageIterator<double> messages) {
    double ret = 0.0;
    for (auto msg : messages) {
      ret += msg;
    }
    return ret;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PREGEL_PAGERANK_PREGEL_H_
