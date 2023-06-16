#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC6_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC6_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

// tagName == ''
template <typename TAG_PROP>
class IC6Expression2 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC6Expression2(std::string_view param1, TAG_PROP&& props)
      : param1_(param1), props_(props) {}

  //   template <typename ELE_TUPLE>
  bool operator()(std::string_view name) const { return name == param1_; }

  tag_prop_t Properties() { return std::make_tuple(props_); }

 private:
  std::string_view param1_;
  TAG_PROP props_;
};

template <typename TAG_PROP>
class IC6Expression3 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC6Expression3(std::string_view param1, TAG_PROP&& props)
      : param1_(param1), props_(props) {}

  bool operator()(std::string_view name) { return name != param1_; }

  tag_prop_t Properties() { return std::make_tuple(props_); }

 private:
  std::string_view param1_;
  TAG_PROP props_;
};

template <typename GRAPH_INTERFACE>
class IC6 {
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
  std::string tag_label = "tag";
  std::string has_tag_label = "hasTag";
  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ6");
    std::string tagName = input.get<std::string>("tagName");
    int32_t limit = 10;

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(tagName);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("tagName", output_decoder.get_string());  // id
      node.put("postCount", output_decoder.get_int());   // post cnt

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    std::string_view tag_name = input.get_string();
    int32_t limit = 10;

    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t post_label_id = graph.GetVertexLabelId(post_label);
    label_id_t comment_label_id = graph.GetVertexLabelId(comment_label);
    label_id_t has_creator_label_id = graph.GetEdgeLabelId(has_creator_label);
    label_id_t forum_label_id = graph.GetVertexLabelId(forum_label);
    label_id_t has_member_label_id = graph.GetEdgeLabelId(has_member_label);
    label_id_t container_of_label_id = graph.GetEdgeLabelId(container_of_label);
    label_id_t tag_label_id = graph.GetVertexLabelId(tag_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(has_tag_label);

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
                                                       person_label_id, id);

    auto edge_expand_opt = gs::make_edge_expand_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 3));
    auto ctx1 = Engine::template PathExpandV<-1, -1>(
        time_stamp, graph, std::move(ctx0), std::move(path_expand_opt));
    LOG(INFO) << "Got " << ctx1.GetHead().Size()
              << " vertices after path expand";

    auto edge_expand_opt4 = gs::make_edge_expand_opt(
        gs::Direction::In, has_creator_label_id, post_label_id);
    auto ctx4 = Engine::template EdgeExpandV<0, -1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt4));

    auto edge_expand_opt5 = gs::make_edge_expand_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx5 = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(ctx4), std::move(edge_expand_opt5));

    gs::NamedProperty<std::string_view> tag_prop("name");
    IC6Expression2 expr(tag_name, std::move(tag_prop));
    auto get_v_opt6 = gs::make_getv_opt(gs::VOpt::Itself,
                                        std::array<label_id_t, 1>{tag_label_id},
                                        std::move(expr));
    auto ctx6 = Engine::template GetV<-1, -1>(
        time_stamp, graph, std::move(ctx5), std::move(get_v_opt6));

    LOG(INFO) << "after get v, head size: " << ctx6.GetHead().Size();

    gs::ProjectSelf<0, 1> proj_col;  // projecting the vertex set itself
    auto proj_opt = gs::make_project_opt(std::move(proj_col));
    auto ctx7 = Engine::template Project<true>(
        time_stamp, graph, std::move(ctx6), std::move(proj_opt));

    auto edge_expand_opt8 = gs::make_edge_expand_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx8 = Engine::template EdgeExpandV<-1, 1>(
        time_stamp, graph, std::move(ctx7), std::move(edge_expand_opt8));

    gs::NamedProperty<std::string_view> tag_prop3("name");
    IC6Expression3 expr3(tag_name, std::move(tag_prop3));
    auto get_v_opt9 = gs::make_getv_opt(gs::VOpt::Itself,
                                        std::array<label_id_t, 1>{tag_label_id},
                                        std::move(expr3));
    auto ctx9 = Engine::template GetV<2, -1>(time_stamp, graph, std::move(ctx8),
                                             std::move(get_v_opt9));
    LOG(INFO) << "after filter with name neq" << tag_name
              << ", head size: " << ctx9.GetHead().Size();

    // group count.
    // gs::GroupKeyAlias<2, 4, -1> group_key;
    gs::AliasTagProp<2, 0, grape::EmptyType> group_key(
        gs::PropNameArray<grape::EmptyType>{"None"});
    // gs::Aggregate<1, 3, gs::AggFunc::COUNT, -1> agg;
    auto agg = gs::make_aggregate_prop<1, gs::AggFunc::COUNT, grape::EmptyType>(
        gs::PropNameArray<grape::EmptyType>{"None"},
        std::integer_sequence<int32_t, 1>{});
    auto group_opt = gs::make_group_opt(std::move(group_key), std::move(agg));
    auto ctx10 = Engine::GroupBy(time_stamp, graph, std::move(ctx9),
                                 std::move(group_opt));

    // sort by
    // TODO: sort by none.none, means using inner id as sort key.
    gs::OrderingPropPair<gs::SortOrder::DESC, 1, vertex_id_t> pair0(
        "None");  // indicate the set's element itself.
    gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view> pair1(
        "name");  // id
    auto pairs = gs::make_sort_opt(gs::Range(0, limit), std::move(pair0),
                                   std::move(pair1));
    auto ctx10_2 =
        Engine::Sort(time_stamp, graph, std::move(ctx10), std::move(pairs));

    gs::AliasTagProp<0, 2, std::string_view> prop_col0({"name"});
    // gs::AliasTagProp<4, 6, gs::oid_t> prop_col1({"id"});
    auto proj_opt11 = gs::make_project_opt(std::move(prop_col0));
    auto ctx11 = Engine::template Project<true>(
        time_stamp, graph, std::move(ctx10_2), std::move(proj_opt11));

    for (auto iter : ctx11) {
      // base_tag: 2
      auto data_tuple = iter.GetAllData();
      output.put_string_view(std::get<2>(data_tuple));
      output.put_int(std::get<1>(data_tuple));
    }
    LOG(INFO) << "End";
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC6_H_
