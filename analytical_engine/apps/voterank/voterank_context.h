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

#ifndef EXAMPLES_ANALYTICAL_APPS_VOTERANK_VOTERANK_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_VOTERANK_VOTERANK_CONTEXT_H_

#include <grape/grape.h>

#include <iomanip>

namespace grape {
/**
 * @brief Context for of VoteRank.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class VoteRankContext : public VertexDataContext<FRAG_T, int> {
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

 public:
  explicit VoteRankContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, int32_t>(fragment, true),
        rank(this->data()) {}

  void Init(ParallelMessageManager& messages, int max_round) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    this->max_round = max_round;
    weight.SetValue(0);
    weight.Init(vertices);
    scores.Init(vertices);
    step = 0;
    avg_degree = 0;
    
#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      if(rank[v])
         os << frag.GetId(v) << " "  << rank[v] << std::endl;
    }
#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  typename FRAG_T::template vertex_array_t<double> weight;
  typename FRAG_T::template vertex_array_t<double> scores;
  typename FRAG_T::template vertex_array_t<int>& rank;
#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif
  std::pair<double,oid_t> max_score;
  int step = 0;
  int max_round = 0;
  double avg_degree = 0;
};
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_VOTERANK_VOTERANK_CONTEXT_H_
