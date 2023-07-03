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

template <typename TAG_PROP>
class IC4Expression1 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC4Expression1(int64_t minDate, int64_t maxDate, TAG_PROP&& props)
      : minDate_(minDate), maxDate_(maxDate), props_(props) {}

  bool operator()(int64_t date) const {
    return date < maxDate_ && date >= minDate_;
  }

  tag_prop_t Properties() const { return std::make_tuple(props_); }

 private:
  int64_t minDate_, maxDate_;
  TAG_PROP props_;
};

template <typename TAG_PROP>
class IC4Expression01 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC4Expression01(int64_t minDate, TAG_PROP&& props)
      : minDate_(minDate), props_(props) {}

  bool operator()(int64_t date) const { return date < minDate_; }

  tag_prop_t Properties() const { return std::make_tuple(props_); }

 private:
  int64_t minDate_;
  TAG_PROP props_;
};

template <typename GRAPH_INTERFACE>
class IC4 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "person";
  std::string knows_label = "knows";
  std::string post_label = "post";
  std::string comment_label = "comment";
  std::string has_creator_label = "hasCreator";
  std::string has_tag_label = "hasTag";
  std::string tag_label = "tag";
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
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
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
    // LOG(INFO) << "start: " << start_date << ", " << (duration_days *
    // 86400000)
    // << ", end: " << end_date;
    int32_t limit = 10;

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
                                                       person_label_id, id);

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto ctx1 = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt));

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id, post_label_id);
    auto ctx_post_left = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt2));

    auto ctx_post_right(ctx_post_left);
    // posts

    IC4Expression1 expr1(start_date, end_date,
                         gs::NamedProperty<int64_t>("creationDate"));
    auto get_v_opt = gs::make_getv_opt(gs::VOpt::Itself,
                                       std::array<label_id_t, 1>{post_label_id},
                                       std::move(expr1));
    auto ctx_post_filter_right = Engine::template GetV<0, -1>(
        time_stamp, graph, std::move(ctx_post_right), std::move(get_v_opt));
    // posts satisfy right condition

    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx_tag_right = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx_post_filter_right),
        std::move(edge_expand_opt4));

    for (auto iter : ctx_tag_right) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << " tag_right: " << gs::to_string(eles);
    }

    gs::AliasTagProp<1, 0, grape::EmptyType> group_key(
        gs::PropNameArray<grape::EmptyType>{"None"});
    auto agg0 =
        gs::make_aggregate_prop<1, gs::AggFunc::COUNT, grape::EmptyType>(
            gs::PropNameArray<grape::EmptyType>{"None"},
            std::integer_sequence<int32_t, 0>{});
    auto group_opt = gs::make_group_opt(std::move(group_key), std::move(agg0));
    auto ctx_tag_group_right = Engine::GroupBy(
        time_stamp, graph, std::move(ctx_tag_right), std::move(group_opt));
    // tag, num_post

    // get left part
    gs::NamedProperty<int64_t> tag_prop_left("creationDate");
    IC4Expression01 expr2(start_date, std::move(tag_prop_left));
    auto get_v_opt2 = gs::make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{post_label_id},
        std::move(expr2));
    auto ctx_post_filter_left = Engine::template GetV<-1, -1>(
        time_stamp, graph, std::move(ctx_post_right), std::move(get_v_opt2));

    auto edge_expand_left = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx_tag_left = Engine::template EdgeExpandV<0, -1>(
        time_stamp, graph, std::move(ctx_post_filter_left),
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

    // gs::OrderingPair<gs::SortOrder::DESC, 2, 0> pair0;  // count
    // gs::OrderingPair<gs::SortOrder::ASC, 4, 0> pair1;   // name
    gs::OrderingPropPair<gs::SortOrder::DESC, 0, size_t> pair0(
        "None");  // indicate the set's element itself.
    gs::OrderingPropPair<gs::SortOrder::ASC, 1, std::string_view> pair1(
        "name");  // name
    // IC4SortComparator sorter;
    auto pairs = gs::make_sort_opt(gs::Range(0, limit), std::move(pair0),
                                   std::move(pair1));
    auto ctx8 = Engine::Sort(time_stamp, graph, std::move(ctx_anti_joined),
                             std::move(pairs));

    // gs::TagProp<-1, std::string_view> name_prop({"name"});
    gs::AliasTagProp<1, 2, std::string_view> prop_col0({"name"});
    auto proj_opt = gs::make_project_opt(std::move(prop_col0));
    auto ctx9 = Engine::template Project<true>(
        time_stamp, graph, std::move(ctx8), std::move(proj_opt));

    for (auto iter : ctx9) {
      // base_tag: 2
      // VLOG(10) << gs::to_string(iter.GetAllData());
      auto data_tuple = iter.GetAllData();
      output.put_string_view(std::get<2>(data_tuple));
      output.put_int(std::get<0>(data_tuple));
    }
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC4_H_
