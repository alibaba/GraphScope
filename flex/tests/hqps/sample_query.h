
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
#ifndef TESTS_HQPS_SAMPLE_QUERY_H_
#define TESTS_HQPS_SAMPLE_QUERY_H_

#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/utils/app_utils.h"

namespace gs {

struct Expression1 {
 public:
  using result_t = bool;
  Expression1(int64_t oid) : oid_(oid) {}

  inline bool operator()(int64_t data) const { return oid_ == data; }

 private:
  int64_t oid_;
};

class SampleQuery : public ReadAppBase {
 public:
  using GRAPH_INTERFACE = gs::MutableCSRInterface;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

 public:
  bool Query(const GraphDBSession& graph, Decoder& input,
             Encoder& output) override {
    int64_t id = input.get_long();
    int64_t maxDate = input.get_long();
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface, id, maxDate);
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    output.put_string(res_str);
    return true;
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph, int64_t id,
                                   int64_t maxDate) const {
    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId("PERSON");
    label_id_t knows_label_id = graph.GetEdgeLabelId("KNOWS");
    label_id_t post_label_id = graph.GetVertexLabelId("POST");
    label_id_t comment_label_id = graph.GetVertexLabelId("COMMENT");
    label_id_t has_creator_label_id = graph.GetEdgeLabelId("HASCREATOR");

    using Engine = SyncEngine<GRAPH_INTERFACE>;

    auto filter =
        gs::make_filter(Expression1(id), gs::PropertySelector<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<AppendOpt::Temp>(
        graph, person_label_id, std::move(filter));

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto ctx1 = Engine::template EdgeExpandV<AppendOpt::Persist, LAST_COL>(
        graph, std::move(ctx0), std::move(edge_expand_opt));

    std::array<label_id_t, 2> labels{post_label_id, comment_label_id};
    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id, std::move(labels));
    auto ctx3 = Engine::template EdgeExpandV<AppendOpt::Temp, LAST_COL>(
        graph, std::move(ctx1), std::move(edge_expand_opt2));

    gs::OrderingPropPair<gs::SortOrder::DESC, INPUT_COL_ID(-1), int64_t> pair0(
        "creationDate");  // creationDate.
    gs::OrderingPropPair<gs::SortOrder::ASC, INPUT_COL_ID(-1), int64_t> pair1(
        "id");
    auto ctx4 = Engine::Sort(graph, std::move(ctx3), gs::Range(0, 20),
                             std::tuple{pair0, pair1});

    // project
    // double t3 = -grape::GetCurrentTime();
    auto mapper1 = gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
        PropertySelector<int64_t>("id"));
    auto mapper2 = gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
        PropertySelector<std::string_view>("firstName"));
    auto mapper3 = gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
        PropertySelector<std::string_view>("lastName"));
    auto mapper4 = gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
        PropertySelector<int64_t>("id"));
    auto mapper5 = gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
        PropertySelector<std::string_view>("content"));
    auto mapper6 = gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
        PropertySelector<std::string_view>("imageFile"));
    auto mapper7 = gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
        PropertySelector<int64_t>("creationDate"));
    auto ctx5 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx4),
        std::tuple{std::move(mapper1), std::move(mapper2), std::move(mapper3),
                   std::move(mapper4), std::move(mapper5), std::move(mapper6),
                   std::move(mapper7)});
    return Engine::Sink(graph, ctx5);
  }
};
}  // namespace gs
#endif  // TESTS_HQPS_SAMPLE_QUERY_H_
