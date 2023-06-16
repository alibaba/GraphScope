#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC12_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC12_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP>
class IC12Expression2 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC12Expression2(std::string_view tag_class_name, TAG_PROP&& props)
      : tag_class_name_(tag_class_name), props_(props) {}

  bool operator()(const std::string_view& data) const {
    return data == tag_class_name_;
  }

  tag_prop_t Properties() const { return std::make_tuple(props_); }

 private:
  std::string_view tag_class_name_;
  TAG_PROP props_;
};

template <typename GRAPH_INTERFACE>
class IC12 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "person";
  std::string knows_label = "knows";
  std::string post_label = "post";
  std::string comment_label = "comment";
  std::string has_creator_label = "hasCreator";
  std::string reply_of_label = "replyOf";
  std::string forum_label = "forum";
  std::string likes_label = "likes";
  std::string has_member_label = "hasMember";
  std::string container_of_label = "containerOf";
  std::string work_at_label = "workAt";
  std::string tag_label = "tag";
  std::string has_tag_label = "hasTag";
  std::string has_type_label = "hasType";
  std::string tag_class_label = "tagClass";
  std::string is_sub_class_of_label = "isSubclassOf";
  std::string place_label = "place";
  std::string org_label = "organisation";
  std::string is_locatedIn_label = "isLocatedIn";
  std::string replyOf_label = "replyOf";
  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ12");
    std::string tag_class_name = input.get<std::string>("tagClassName");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(tag_class_name);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("personId", output_decoder.get_long());           // id
      node.put("personFirstName", output_decoder.get_string());  // post cnt
      node.put("personLastName", output_decoder.get_string());
      // new node for list.
      boost::property_tree::ptree tag_names;
      int32_t size = output_decoder.get_int();
      for (auto i = 0; i < size; ++i) {
        std::string str{output_decoder.get_string()};
        tag_names.push_back(
            std::make_pair("", boost::property_tree::ptree(str)));
      }
      node.add_child("tagNames", tag_names);
      node.put("replyCount", output_decoder.get_int());
      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    std::string_view tag_class_name = input.get_string();
    int32_t limit = 20;

    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t post_label_id = graph.GetVertexLabelId(post_label);
    label_id_t comment_label_id = graph.GetVertexLabelId(comment_label);
    label_id_t has_creator_label_id = graph.GetEdgeLabelId(has_creator_label);
    label_id_t reply_of_label_id = graph.GetEdgeLabelId(reply_of_label);
    label_id_t forum_label_id = graph.GetVertexLabelId(forum_label);
    label_id_t likes_label_id = graph.GetEdgeLabelId(likes_label);
    label_id_t has_member_label_id = graph.GetEdgeLabelId(has_member_label);
    label_id_t container_of_label_id = graph.GetEdgeLabelId(container_of_label);
    label_id_t work_at_label_id = graph.GetEdgeLabelId(work_at_label);
    label_id_t tag_label_id = graph.GetVertexLabelId(tag_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(has_tag_label);
    label_id_t has_type_label_id = graph.GetEdgeLabelId(has_type_label);
    label_id_t tag_class_label_id = graph.GetVertexLabelId(tag_class_label);
    label_id_t is_sub_class_of_label_id =
        graph.GetEdgeLabelId(is_sub_class_of_label);

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
                                                       person_label_id, id);
    // message

    auto edge_expand_opt1 = gs::make_edge_expand_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto ctx1 = Engine::template EdgeExpandV<0, -1>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt1));

    auto edge_expand_opt2 = gs::make_edge_expand_opt(
        gs::Direction::In, has_creator_label_id, comment_label_id);
    // message
    auto ctx2 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt2));

    auto edge_expand_opt3 = gs::make_edge_expand_opt(
        gs::Direction::Out, reply_of_label_id, post_label_id);
    // post
    auto ctx3 = Engine::template EdgeExpandV<2, 1>(
        time_stamp, graph, std::move(ctx2), std::move(edge_expand_opt3));

    // tags
    auto edge_expand_opt4 = gs::make_edge_expand_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx4 = Engine::template EdgeExpandV<3, 2>(
        time_stamp, graph, std::move(ctx3), std::move(edge_expand_opt4));

    // tag classes
    auto edge_expand_opt5 = gs::make_edge_expand_opt(
        gs::Direction::Out, has_type_label_id, tag_class_label_id);
    auto ctx5 = Engine::template EdgeExpandV<-1, 3>(
        time_stamp, graph, std::move(ctx4), std::move(edge_expand_opt5));

    // PathExpand to find all valid tag classes.

    auto edge_expand_opt6 = gs::make_edge_expand_opt(
        gs::Direction::Out, is_sub_class_of_label_id, tag_class_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{tag_class_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt6), std::move(get_v_opt), gs::Range(0, 10));
    auto ctx6 = Engine::template PathExpandV<4, -1>(
        time_stamp, graph, std::move(ctx5), std::move(path_expand_opt));
    // auto ctx7 = Engine::template Dedup<-1>(std::move(ctx6));

    gs::NamedProperty<std::string_view> tag_prop7("name");
    IC12Expression2 expr2(tag_class_name, std::move(tag_prop7));
    auto ctx7 =
        Engine::Select(time_stamp, graph, std::move(ctx6), std::move(expr2));

    gs::AliasTagProp<0, 7, grape::EmptyType> group_key(
        gs::PropNameArray<grape::EmptyType>{"None"});
    auto agg1 =
        gs::make_aggregate_prop<5, gs::AggFunc::TO_SET, std::string_view>(
            gs::PropNameArray<std::string_view>{"name"},
            std::integer_sequence<int32_t, 3>{});
    auto agg2 =
        gs::make_aggregate_prop<6, gs::AggFunc::COUNT, grape::EmptyType>(
            gs::PropNameArray<grape::EmptyType>{"None"},
            std::integer_sequence<int32_t, 1>{});

    auto group_opt = gs::make_group_opt(std::move(group_key), std::move(agg1),
                                        std::move(agg2));
    auto ctx8 = Engine::GroupBy(time_stamp, graph, std::move(ctx7),
                                std::move(group_opt));

    gs::OrderingPropPair<gs::SortOrder::DESC, 6, size_t> pair0(
        "none");  // the element itself.
    gs::OrderingPropPair<gs::SortOrder::ASC, 7, gs::oid_t> pair1("id");  // id
    // std::move(pair0),
    auto pairs = gs::make_sort_opt(gs::Range(0, limit), std::move(pair0),
                                   std::move(pair1));
    auto ctx9 =
        Engine::Sort(time_stamp, graph, std::move(ctx8), std::move(pairs));

    gs::AliasTagProp<7, 8, gs::oid_t, std::string_view, std::string_view>
        prop_col0({"id", "firstName", "lastName"});  // other person
    gs::ProjectSelf<5, 9> prop_col1;                 // company name
    gs::ProjectSelf<6, 10> prop_col2;                // company name
    auto proj_opt4 = gs::make_project_opt(
        std::move(prop_col0), std::move(prop_col1), std::move(prop_col2));
    auto ctx10 = Engine::template Project<true>(
        time_stamp, graph, std::move(ctx9), std::move(proj_opt4));

    for (auto iter : ctx10) {
      auto element = iter.GetAllElement();
      auto& person = std::get<3>(element);
      auto& tag_names = std::get<4>(element);
      auto& reply_cnt = std::get<5>(element);
      output.put_long(std::get<0>(person));         // person id
      output.put_string_view(std::get<1>(person));  // person first name
      output.put_string_view(std::get<2>(person));  // person last name
      output.put_int(tag_names.size());
      for (auto tag_name : tag_names) {
        output.put_string_view(tag_name);  // tag_name
      }
      output.put_int(reply_cnt);  // reply cnt
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC12_H_
