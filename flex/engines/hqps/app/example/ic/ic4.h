#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC4_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC4_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

// These expression should be generated.
// One generated expression can be reused with different params.

class IC4Expression1 {
 public:
  using result_t = bool;
  IC4Expression1(int64_t minDate, int64_t maxDate)
      : minDate_(minDate), maxDate_(maxDate) {}

  bool operator()(int64_t date) const {
    return date < maxDate_ && date >= minDate_;
  }

 private:
  int64_t minDate_, maxDate_;
};

class IC4Expression01 {
 public:
  using result_t = bool;
  IC4Expression01(int64_t minDate) : minDate_(minDate) {}

  bool operator()(int64_t date) const { return date < minDate_; }

 private:
  int64_t minDate_;
};

template <typename GRAPH_INTERFACE>
class QueryIC4 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "PERSON";
  std::string knows_label = "KNOWS";
  std::string post_label = "POST";
  std::string comment_label = "COMMENT";
  std::string has_creator_label = "HASCREATOR";
  std::string has_tag_label = "HASTAG";
  std::string tag_label = "TAG";
  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ4");
    int64_t start_date = input.get<int64_t>("startDate");
    int32_t durationDays = input.get<int32_t>("durationDays");
    //    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_long(start_date);
    input_encoder.put_int(durationDays);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("tagName", output_decoder.get_string());  // id
      node.put("postCount", output_decoder.get_int());   // dist

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t post_label_id = graph.GetVertexLabelId(post_label);
    label_id_t comment_label_id = graph.GetVertexLabelId(comment_label);
    label_id_t has_creator_label_id = graph.GetEdgeLabelId(has_creator_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(has_tag_label);
    label_id_t tag_label_id = graph.GetVertexLabelId(tag_label);

    int64_t id = input.get_long();
    int64_t start_date = input.get_long();
    // convert to int64_t
    int64_t duration_days = input.get_int();
    int64_t end_date = start_date + duration_days * 86400000;
    int32_t limit = 10;

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, id);

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto ctx1 = Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx0), std::move(edge_expand_opt));

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id, post_label_id);
    auto ctx_post_left =
        Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt2));

    auto ctx_post_right(ctx_post_left);
    // posts

    auto filter1 =
        gs::make_filter(IC4Expression1(start_date, end_date),
                        gs::PropertySelector<int64_t>("creationDate"));
    auto get_v_opt = gs::make_getv_opt(gs::VOpt::Itself,
                                       std::array<label_id_t, 1>{post_label_id},
                                       std::move(filter1));
    auto ctx_post_filter_right =
        Engine::template GetV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx_post_right), std::move(get_v_opt));
    // posts satisfy right condition

    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx_tag_right =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx_post_filter_right),
            std::move(edge_expand_opt4));

    for (auto iter : ctx_tag_right) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << " tag_right: " << gs::to_string(eles);
    }

    auto ctx_tag_group_right = Engine::GroupBy(
        graph, std::move(ctx_tag_right),
        std::tuple{GroupKey<1, grape::EmptyType>(
            PropertySelector<grape::EmptyType>())},
        std::tuple{gs::make_aggregate_prop<AggFunc::COUNT>(
            std::tuple{gs::PropertySelector<grape::EmptyType>()},
            std::integer_sequence<int32_t, 0>{})}

    );
    // tag, num_post

    // get left part
    auto filter =
        gs::make_filter(IC4Expression01(start_date),
                        gs::PropertySelector<int64_t>("creationDate"));
    auto get_v_opt2 = gs::make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{post_label_id},
        std::move(filter));
    auto ctx_post_filter_left =
        Engine::template GetV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
            graph, std::move(ctx_post_right), std::move(get_v_opt2));

    auto edge_expand_left = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx_tag_left =
        Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
            graph, std::move(ctx_post_filter_left),
            std::move(edge_expand_left));

    for (auto iter : ctx_tag_left) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << " tag_right: " << gs::to_string(eles);
    }

    // after antijoin we only keep the record in ctx_tag_right with tag_id not
    // appeared in ctx_tag_left;
    auto ctx_anti_joined = Engine::template Join<0, 0, JoinKind::AntiJoin>(
        std::move(ctx_tag_group_right), std::move(ctx_tag_left));
    // after antijoin, we have: tag, num_post

    for (auto iter : ctx_anti_joined) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << " tag_right: " << gs::to_string(eles);
    }
    LOG(INFO) << "demagles: " << demangle(ctx_anti_joined);

    auto ctx8 = Engine::Sort(
        graph, std::move(ctx_anti_joined), gs::Range(0, limit),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, size_t>(
                "None"),  // indicate the set's element itself.
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view>(
                "name")  // name
        });

    // gs::TagProp<-1, std::string_view> name_prop({"name"});
    auto ctx9 = Engine::template Project<PROJ_TO_APPEND>(
        graph, std::move(ctx8),
        std::tuple{gs::make_mapper_with_variable<0>(
            gs::PropertySelector<std::string_view>("name"))});

    for (auto iter : ctx9) {
      auto data_tuple = iter.GetAllElement();
      output.put_string_view(std::get<2>(data_tuple));
      output.put_int(std::get<1>(data_tuple));
    }
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC4_H_
