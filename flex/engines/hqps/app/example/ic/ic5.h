#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC5_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC5_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP0, typename TAG_PROP1>
class IC5Express0 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP0, TAG_PROP1>;
  IC5Express0(int64_t oid, TAG_PROP0&& prop0, TAG_PROP1&& prop1)
      : oid_(oid), prop0_(std::move(prop0)), prop1_(std::move(prop1)) {}

  template <typename label_id_t>
  inline bool operator()(label_id_t label_id, int64_t oid) const {
    // auto& cur_date = std::get<0>(data_tuple);
    return label_id == 1 && oid == oid_;
  }

  tag_prop_t Properties() const { return std::make_tuple(prop0_, prop1_); }

 private:
  int64_t oid_;
  TAG_PROP0 prop0_;
  TAG_PROP1 prop1_;
};

template <typename TAG_PROP>
class IC5Expression1 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC5Expression1(int64_t min_date, TAG_PROP&& props)
      : min_join_date_(min_date), props_(std::move(props)) {}

  // This contains the logic parsed from expression.
  // @.creationDate >= start_date && @.creationDate < end_date
  //   template <typename ELE_TUPLE>
  bool operator()(int64_t join_date) {
    // VLOG(10) << "cur: " << std::get<0>(tuple) << ", min : " <<
    // min_join_date_;
    return join_date > min_join_date_;
  }

  tag_prop_t Properties() { return std::make_tuple(props_); }

 private:
  int64_t min_join_date_;
  TAG_PROP props_;
};

template <typename GRAPH_INTERFACE>
class QueryIC5 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "person";
  std::string knows_label = "knows";
  std::string post_label = "post";
  std::string comment_label = "comment";
  std::string has_creator_label = "hasCreator";
  std::string forum_label = "forum";
  std::string has_member_label = "hasMember";
  std::string container_of_label = "containerOf";
  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ5");
    int64_t min_date = input.get<int64_t>("minDate");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_long(min_date);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("forumTitle", output_decoder.get_string());  // id
      node.put("postCount", output_decoder.get_int());      // post cnt

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    int64_t min_join_date = input.get_long();
    int32_t limit = 20;
    using Engine = SyncEngine<GRAPH_INTERFACE>;

    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t forum_label_id = graph.GetVertexLabelId(forum_label);
    label_id_t has_member_label_id = graph.GetEdgeLabelId(has_member_label);
    label_id_t container_of_label_id = graph.GetEdgeLabelId(container_of_label);
    label_id_t post_label_id = graph.GetVertexLabelId(post_label);
    label_id_t has_creator_label_id = graph.GetEdgeLabelId(has_creator_label);

    double t0 = -grape::GetCurrentTime();

    IC5Express0 expr_scan(id, gs::LabelKeyProperty<label_id_t>("label"),
                          gs::NamedProperty<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<-1>(
        time_stamp, graph, person_label_id, std::move(expr_scan));
    // uncomment this line to use fast scan.
    // auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
    //                                                  person_label_id, id);

    auto edge_expand_opt = gs::make_edge_expand_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 3));
    auto ctx1 = Engine::template PathExpandV<0, -1>(
        time_stamp, graph, std::move(ctx0), std::move(path_expand_opt));

    // copy a right ctx.
    auto right_ctx_1(ctx1);
    t0 += grape::GetCurrentTime();

    double t1 = -grape::GetCurrentTime();
    IC5Expression1 left_expr1(min_join_date,
                              gs::NamedProperty<int64_t>("joinDate"));
    auto left_edge_expand_opt3 =
        gs::make_edge_expand_opt(gs::Direction::In, has_member_label_id,
                                 forum_label_id, std::move(left_expr1));
    auto left_ctx3 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx1), std::move(left_edge_expand_opt3));

    // auto left_ctx4 = Engine::template Dedup<1>(std::move(left_ctx3));
    // t1 += grape::GetCurrentTime();

    double t2 = -grape::GetCurrentTime();
    // person hasCreator -> post
    auto right_edge_expand_opt5 = gs::make_edge_expand_opt(
        gs::Direction::In, has_creator_label_id, post_label_id);
    auto right_ctx_2 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(right_ctx_1),
        std::move(right_edge_expand_opt5));

    auto right_edge_expand_opt6 = gs::make_edge_expand_opt(
        gs::Direction::In, container_of_label_id, forum_label_id);
    auto right_ctx_3 = Engine::template EdgeExpandV<2, 1>(
        time_stamp, graph, std::move(right_ctx_2),
        std::move(right_edge_expand_opt6));
    t2 += grape::GetCurrentTime();
    // group by forum

    // join on pair.
    double t3 = -grape::GetCurrentTime();
    auto ctx_joined =
        Engine::template Join<0, 1, 0, 2, JoinKind::LeftOuterJoin>(
            std::move(left_ctx3), std::move(right_ctx_3));
    t3 += grape::GetCurrentTime();
    // after join is 0,1

    LOG(INFO) << "start group by";
    gs::AliasTagProp<1, 0, grape::EmptyType> group_keyx({"None"});
    auto aggx =
        gs::make_aggregate_prop<1, gs::AggFunc::COUNT, grape::EmptyType>(
            gs::PropNameArray<grape::EmptyType>{"None"},
            std::integer_sequence<int32_t, 2>{});
    auto group_by_forum_opt =
        gs::make_group_opt(std::move(group_keyx), std::move(aggx));
    auto right_ctx_4 = Engine::GroupBy(time_stamp, graph, std::move(ctx_joined),
                                       std::move(group_by_forum_opt));

    double t4 = -grape::GetCurrentTime();
    gs::OrderingPropPair<gs::SortOrder::DESC, 1, size_t> pair0("None");
    gs::OrderingPropPair<gs::SortOrder::ASC, 0,
                         gs::oid_t> pair1("id");  // id
    auto pairs =
        gs::make_sort_opt(gs::Range(0, 20), std::move(pair0), std::move(pair1));
    auto ctx8 = Engine::Sort(time_stamp, graph, std::move(right_ctx_4),
                             std::move(pairs));
    t4 += grape::GetCurrentTime();

    gs::AliasTagProp<0, 2, std::string_view> prop_col0({"title"});
    auto proj_opt = gs::make_project_opt(std::move(prop_col0));
    auto ctx9 = Engine::template Project<true>(
        time_stamp, graph, std::move(ctx8), std::move(proj_opt));

    for (auto iter : ctx9) {
      auto data_tuple = iter.GetAllElement();
      LOG(INFO) << gs::to_string(data_tuple);
      output.put_string_view(std::get<2>(data_tuple));
      output.put_int(std::get<1>(data_tuple));
    }

    LOG(INFO) << "End of IC5, left path expand takes: " << t0
              << ", left edge expande: " << t1 << ", right get forum : " << t2
              << ", join cost: " << t3 << ", sort cost: " << t4;

    // size_t null_post_cnt = 0;
    // size_t act_post_cnt = 0;
    // for (auto iter : ctx_joined) {
    //   auto eles = iter.GetAllElement();
    //   LOG(INFO) << "eles size: " << gs::to_string(eles);
    //   if (std::get<2>(eles) == std::numeric_limits<int64_t>::max()) {
    //     null_post_cnt++;
    //   } else {
    //     act_post_cnt++;
    //   }
    // }
    // LOG(INFO) << "null_post_cnt: " << null_post_cnt;
    // LOG(INFO) << "act_post_cnt: " << act_post_cnt;
    // LOG(FATAL) << "exit";
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC5_H_
