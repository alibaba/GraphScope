#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC11_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC11_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP>
class IC11Expression2 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC11Expression2(int32_t work_year, TAG_PROP&& props)
      : work_year_(work_year), props_(props) {}

  bool operator()(const int32_t work_year) const {
    // VLOG(10) << "Compare: " << std::get<0>(data_tuple) << ", " << work_year_;
    return work_year < work_year_;
  }

  tag_prop_t Properties() const { return std::make_tuple(props_); }

 private:
  int32_t work_year_;
  TAG_PROP props_;
};

template <typename TAG_PROP>
class IC11Expression3 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC11Expression3(std::string_view name, TAG_PROP&& props)
      : name_(name), props_(props) {}

  //   template <typename TUPLE_T>
  //   bool operator()(const TUPLE_T& data_tuple) const {
  bool operator()(std::string_view name) const { return name == name_; }

  tag_prop_t Properties() { return std::make_tuple(props_); }

 private:
  std::string_view name_;
  TAG_PROP props_;
};

template <typename GRAPH_INTERFACE>
class QueryIC11 {
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
  std::string place_label = "place";
  std::string org_label = "organisation";
  std::string is_locatedIn_label = "isLocatedIn";
  // static std::string_view firstName = "Jack";
  static double path_expand_time;
  static double edge_expand_time;
  static double get_company_time;
  static double expand_country_time;
  static double filter_country_time;

 public:
  ~QueryIC11() {
    LOG(INFO) << "path_expand_time: " << path_expand_time;
    LOG(INFO) << "edge_expand_time: " << edge_expand_time;
    LOG(INFO) << "get_company_time: " << get_company_time;
    LOG(INFO) << "expand_country_time: " << expand_country_time;
    LOG(INFO) << "filter_country_time: " << filter_country_time;
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ11");
    std::string country_name = input.get<std::string>("countryName");
    int32_t work_from_year = input.get<int32_t>("workFromYear");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(country_name);
    input_encoder.put_int(work_from_year);
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
      node.put("organizationName", output_decoder.get_string());
      node.put("organizationWorkFromYear", output_decoder.get_int());

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    std::string_view country_name = input.get_string();
    int32_t work_year = input.get_int();
    int32_t limit = 10;
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
    label_id_t place_label_id = graph.GetVertexLabelId(place_label);
    label_id_t org_label_id = graph.GetVertexLabelId(org_label);
    label_id_t is_locatedIn_label_id = graph.GetEdgeLabelId(is_locatedIn_label);

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
                                                       person_label_id, id);

    double t0 = grape::GetCurrentTime();
    auto edge_expand_opt = gs::make_edge_expand_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 3));
    auto ctx1 = Engine::template PathExpandV<0, -1>(
        time_stamp, graph, std::move(ctx0), std::move(path_expand_opt));
    t0 = grape::GetCurrentTime() - t0;
    path_expand_time += t0;

    double t1 = grape::GetCurrentTime();
    IC11Expression2 expr1(work_year, gs::NamedProperty<int32_t>("workFrom"));
    auto edge_expand_opt3 = gs::make_edge_expande_opt<int32_t>(
        gs::PropNameArray<int32_t>{"workFrom"}, gs::Direction::Out,
        work_at_label_id, org_label_id, std::move(expr1));
    auto ctx3_0 = Engine::template EdgeExpandE<1, 0>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt3));
    t1 = grape::GetCurrentTime() - t1;
    edge_expand_time += t1;

    double t2 = grape::GetCurrentTime();
    auto get_v_opt3 = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{org_label_id});
    auto ctx3 = Engine::template GetV<2, 1>(
        time_stamp, graph, std::move(ctx3_0), std::move(get_v_opt3));
    t2 = grape::GetCurrentTime() - t2;
    get_company_time += t2;

    double t3 = grape::GetCurrentTime();
    auto edge_expand_opt4 = gs::make_edge_expand_opt(
        gs::Direction::Out, is_locatedIn_label_id, place_label_id);
    auto ctx4 = Engine::template EdgeExpandV<-1, 2>(
        time_stamp, graph, std::move(ctx3), std::move(edge_expand_opt4));
    t3 = grape::GetCurrentTime() - t3;
    expand_country_time += t3;

    double t4 = grape::GetCurrentTime();
    gs::NamedProperty<std::string_view> tag_prop5("name");
    IC11Expression3 expr5(country_name, std::move(tag_prop5));
    auto get_v_opt5 = gs::make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{place_label_id},
        std::move(expr5));
    auto ctx5 = Engine::template GetV<-1, -1>(
        time_stamp, graph, std::move(ctx4), std::move(get_v_opt5));
    t4 = grape::GetCurrentTime() - t4;
    filter_country_time += t4;
    // project to new columns
    auto project_opt10 = gs::make_project_opt(
        gs::ProjectSelf<0, 0>(), gs::AliasTagProp<1, 1, int32_t>({"workFrom"}),
        gs::ProjectSelf<2, 2>());

    auto ctx5_1 = Engine::template Project<0>(
        time_stamp, graph, std::move(ctx5), std::move(project_opt10));

    // sort
    auto sort_opt11 = gs::make_sort_opt(
        gs::Range(0, 10),
        gs::OrderingPropPair<gs::SortOrder::ASC, 1, int32_t>("none"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("id"),
        gs::OrderingPropPair<gs::SortOrder::DESC, 2, std::string_view>("name"));

    auto ctx6 = Engine::Sort(time_stamp, graph, std::move(ctx5_1),
                             std::move(sort_opt11));

    auto project_opt12 = gs::make_project_opt(
        gs::AliasTagProp<0, 0, int64_t>({"id"}),
        gs::AliasTagProp<0, 1, std::string_view>({"firstName"}),
        gs::AliasTagProp<0, 2, std::string_view>({"lastName"}),
        gs::ProjectSelf<1, 3>(),
        gs::AliasTagProp<2, 4, std::string_view>({"name"}));
    auto ctx7 = Engine::template Project<0>(time_stamp, graph, std::move(ctx6),
                                            std::move(project_opt12));

    for (auto iter : ctx7) {
      auto element = iter.GetAllElement();
      //   auto& person = std::get<3>(element);
      //   auto& cmy = std::get<4>(element);
      //   auto& work_at = std::get<5>(element);
      output.put_long(std::get<0>(element));         // person id
      output.put_string_view(std::get<1>(element));  // person first name
      output.put_string_view(std::get<2>(element));  // person last name
      output.put_string_view(std::get<4>(element));  // company name
      output.put_int(std::get<3>(element));          // workAt year
    }
  }
};

template <typename GRAPH>
double QueryIC11<GRAPH>::path_expand_time = 0.0;
template <typename GRAPH>
double QueryIC11<GRAPH>::edge_expand_time = 0.0;
template <typename GRAPH>
double QueryIC11<GRAPH>::get_company_time = 0.0;
template <typename GRAPH>
double QueryIC11<GRAPH>::expand_country_time = 0.0;
template <typename GRAPH>
double QueryIC11<GRAPH>::filter_country_time = 0.0;

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC11_H_
