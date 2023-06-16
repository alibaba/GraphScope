#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC3_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC3_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_COL>
class IC3Expre0 {
 public:
  using tag_prop_t = std::tuple<TAG_COL>;
  IC3Expre0(std::string_view ctr_y_name, TAG_COL&& props)
      : ctr_y_name_(ctr_y_name), props_(props) {}

  auto operator()(std::string_view country_name) const {
    return country_name == ctr_y_name_;
  }

  tag_prop_t Properties() { return std::make_tuple(props_); }

 private:
  std::string_view ctr_y_name_;
  TAG_COL props_;
};

template <typename TAG_COL>
class IC3Expression2 {
 public:
  using tag_prop_t = std::tuple<TAG_COL>;
  IC3Expression2(std::string_view param1, std::string_view param2,
                 TAG_COL&& props)
      : param1_(param1), param2_(param2), props_(props) {}

  //   template <typename TUPLE_T>
  //   bool operator()(TUPLE_T& props) {
  bool operator()(std::string_view country_name) const {
    return param1_ == country_name || param2_ == country_name;
  }

  tag_prop_t Properties() { return std::make_tuple(props_); }

 private:
  std::string_view param1_, param2_;
  TAG_COL props_;
};

template <typename TAG_COL>
class IC3Expression3 {
 public:
  using tag_prop_t = std::tuple<TAG_COL>;
  IC3Expression3(int64_t start_date, int64_t end_date, TAG_COL&& props)
      : start_date_(start_date), end_date_(end_date), props_(props) {}

  //   template <typename TUPLE_T>
  bool operator()(const int64_t date) const {
    // VLOG(10) << "1: " << start_date_ << ", " << end_date_ << " "
    //          << std::get<0>(props);
    return date >= start_date_ && date < end_date_;
  }

  tag_prop_t Properties() { return std::make_tuple(props_); }

 private:
  int64_t start_date_;
  int64_t end_date_;
  TAG_COL props_;
};

template <typename TAG_COL0, typename TAG_COL1>
class IC3Expression4 {
 public:
  using tag_prop_t = std::tuple<TAG_COL0, TAG_COL1>;
  using result_t = uint64_t;
  IC3Expression4(TAG_COL0&& prop0, TAG_COL1&& prop1)
      : prop0_(std::move(prop0)), prop1_(std::move(prop1)) {}

  auto operator()(uint64_t a, uint64_t b) const { return a + b; }

  tag_prop_t Properties() { return std::make_tuple(prop0_, prop1_); }

 private:
  TAG_COL0 prop0_;
  TAG_COL1 prop1_;
};

template <typename GRAPH_INTERFACE>
class IC3 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "person";
  std::string knows_label = "knows";
  std::string isLocatedIn_label = "isLocatedIn";
  std::string place_label = "place";
  std::string workAt_label = "workAt";
  std::string studyAt_label = "studyAt";
  std::string is_locatedIn_label = "isLocatedIn";
  std::string org_label = "organisation";
  std::string isPartOf_label = "isPartOf";
  std::string post_label = "post";
  std::string comment_label = "comment";
  std::string has_creator_label = "hasCreator";
  // static std::string_view firstName = "Jack";

  using Engine = SyncEngine<GRAPH_INTERFACE>;

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ3");

    int64_t start_date = input.get<int64_t>("startDate");
    int32_t duration_days = input.get<int32_t>("durationDays");
    std::string country_x = input.get<std::string>("countryXName");
    std::string country_y = input.get<std::string>("countryYName");
    int32_t limit = 20;

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(country_x);
    input_encoder.put_string(country_y);
    input_encoder.put_long(start_date);
    input_encoder.put_int(duration_days);

    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("personId", output_decoder.get_long());           // id
      node.put("personFirstName", output_decoder.get_string());  // dist
      node.put("personLastName", output_decoder.get_string());   // lastName"
      node.put("xCount", output_decoder.get_long());             // xcount
      node.put("yCount",
               output_decoder.get_long());  // ycount
      node.put("count",
               output_decoder.get_long());  // gender

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    using Engine = SyncEngine<GRAPH_INTERFACE>;
    int64_t id = input.get_long();

    std::string_view country_x_name = input.get_string();
    std::string_view country_y_name = input.get_string();
    int64_t start_date = input.get_long();
    int64_t duration_days = input.get_int();
    int64_t end_date = start_date + duration_days * 86400000;
    int32_t limit = 20;

    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t is_locatedIn_label_id = graph.GetEdgeLabelId(is_locatedIn_label);
    label_id_t place_label_id = graph.GetVertexLabelId(place_label);
    label_id_t post_label_id = graph.GetVertexLabelId(post_label);
    label_id_t comment_label_id = graph.GetVertexLabelId(comment_label);
    label_id_t isPartOf_label_id = graph.GetEdgeLabelId(isPartOf_label);
    label_id_t has_creator_label_id = graph.GetEdgeLabelId(has_creator_label);

    auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
                                                       person_label_id, id);

    //
    auto edge_expand_opt = gs::make_edge_expand_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 3));
    auto ctx1 = Engine::template PathExpandV<0, -1>(
        time_stamp, graph, std::move(ctx0), std::move(path_expand_opt));

    auto edge_expand_opt3 = gs::make_edge_expand_opt(
        gs::Direction::Out, is_locatedIn_label_id, place_label_id);
    auto ctx2 = Engine::template EdgeExpandV<-1, 0>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt3));

    auto edge_expand_opt4 = gs::make_edge_expand_opt(
        gs::Direction::Out, isPartOf_label_id, place_label_id);
    auto ctx3 = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(ctx2), std::move(edge_expand_opt4));

    gs::NamedProperty<std::string_view> tag_prop("name");
    IC3Expression2 expr2(country_x_name, country_y_name, std::move(tag_prop));
    auto get_v_opt3 = gs::make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{place_label_id},
        std::move(expr2));
    auto ctx4 = Engine::template GetV<1, -1>(time_stamp, graph, std::move(ctx3),
                                             std::move(get_v_opt3));

    // ctx_x countryx
    IC3Expre0 expr_scan0(country_x_name,
                         gs::NamedProperty<std::string_view>("name"));
    auto ctx_x0 = Engine::template ScanVertex<-1>(
        time_stamp, graph, place_label_id, std::move(expr_scan0));

    auto edge_expand_opt_x0 = gs::make_edge_expand_opt(
        gs::Direction::In, is_locatedIn_label_id,
        std::array<label_id_t, 2>{post_label_id, comment_label_id});
    // comment and post
    auto ctx_x1 = Engine::template EdgeExpandVMultiLabel<-1, -1>(
        time_stamp, graph, std::move(ctx_x0), std::move(edge_expand_opt_x0));
    // filter
    gs::NamedProperty<int64_t> creation_date_propx("creationDate");
    IC3Expression3 expr3x(start_date, end_date, std::move(creation_date_propx));
    auto get_v_optx = gs::make_getv_opt(
        gs::VOpt::Itself,
        std::array<label_id_t, 2>{post_label_id, comment_label_id},
        std::move(expr3x));
    auto ctx_x2 = Engine::template GetV<0, -1>(
        time_stamp, graph, std::move(ctx_x1), std::move(get_v_optx));

    auto edge_expand_opt_x1 = gs::make_edge_expand_opt(
        gs::Direction::Out, has_creator_label_id, person_label_id);
    // person
    auto ctx_x3 = Engine::template EdgeExpandV<-1, 0>(
        time_stamp, graph, std::move(ctx_x2), std::move(edge_expand_opt_x1));

    // group by person
    gs::AliasTagProp<-1, 0, grape::EmptyType> group_keyx(
        gs::PropNameArray<grape::EmptyType>{"None"});
    auto aggx =
        gs::make_aggregate_prop<1, gs::AggFunc::COUNT, grape::EmptyType>(
            gs::PropNameArray<grape::EmptyType>{"None"},
            std::integer_sequence<int32_t, 0>{});
    auto group_optx =
        gs::make_group_opt(std::move(group_keyx), std::move(aggx));
    auto ctx_x4 = Engine::GroupBy(time_stamp, graph, std::move(ctx_x3),
                                  std::move(group_optx));
    // ctx_x4: person, counts

    IC3Expre0 expr_scan1(country_y_name,
                         gs::NamedProperty<std::string_view>("name"));
    auto ctx_y0 = Engine::template ScanVertex<-1>(
        time_stamp, graph, place_label_id, std::move(expr_scan1));

    auto edge_expand_opt_y0 = gs::make_edge_expand_opt(
        gs::Direction::In, is_locatedIn_label_id,
        std::array<label_id_t, 2>{post_label_id, comment_label_id});
    // comment and post
    auto ctx_y1 = Engine::template EdgeExpandVMultiLabel<-1, -1>(
        time_stamp, graph, std::move(ctx_y0), std::move(edge_expand_opt_y0));
    // filter
    gs::NamedProperty<int64_t> creation_date_propy("creationDate");
    IC3Expression3 expr3y(start_date, end_date, std::move(creation_date_propy));
    auto get_v_opty = gs::make_getv_opt(
        gs::VOpt::Itself,
        std::array<label_id_t, 2>{post_label_id, comment_label_id},
        std::move(expr3y));
    auto ctx_y2 = Engine::template GetV<0, -1>(
        time_stamp, graph, std::move(ctx_y1), std::move(get_v_opty));

    auto edge_expand_opt_y1 = gs::make_edge_expand_opt(
        gs::Direction::Out, has_creator_label_id, person_label_id);
    // person
    auto ctx_y3 = Engine::template EdgeExpandV<-1, 0>(
        time_stamp, graph, std::move(ctx_y2), std::move(edge_expand_opt_y1));

    // group by person
    gs::AliasTagProp<-1, 0, grape::EmptyType> group_keyy(
        gs::PropNameArray<grape::EmptyType>{"None"});
    auto aggy =
        gs::make_aggregate_prop<1, gs::AggFunc::COUNT, grape::EmptyType>(
            gs::PropNameArray<grape::EmptyType>{"None"},
            std::integer_sequence<int32_t, 0>{});
    auto group_opty =
        gs::make_group_opt(std::move(group_keyy), std::move(aggy));
    auto ctx_y4 = Engine::GroupBy(time_stamp, graph, std::move(ctx_y3),
                                  std::move(group_opty));

    auto ctx_joined = Engine::template Join<0, 0, JoinKind::InnerJoin>(
        std::move(ctx_x4), std::move(ctx_y4));
    VLOG(10) << "countryx 's msg: " << ctx_x4.GetHead().Size()
             << ", country y's msg: " << ctx_y4.GetHead().Size();
    auto ctx_joined2 = Engine::template Join<0, 0, JoinKind::InnerJoin>(
        std::move(ctx_joined), std::move(ctx4));
    // person, xcount, ycount, city

    for (auto iter : ctx_joined2) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << "eles: " << gs::to_string(eles);
    }

    gs::AliasTagProp<0, 0, gs::oid_t> prop_col0({"id"});
    gs::AliasTagProp<0, 1, std::string_view> prop_col1({"firstName"});
    gs::AliasTagProp<0, 2, std::string_view> prop_col2({"lastName"});
    gs::ProjectSelf<1, 3> prop_col3;
    gs::ProjectSelf<2, 4> prop_col4;
    gs::InnerIdProperty<1> x_count;
    gs::InnerIdProperty<2> y_count;

    IC3Expression4 expr4(std::move(x_count), std::move(y_count));
    auto prop_col5 = make_project_expr<5, size_t>(std::move(expr4));

    auto proj_opt10 = gs::make_project_opt(
        std::move(prop_col0), std::move(prop_col1), std::move(prop_col2),
        std::move(prop_col3), std::move(prop_col4), std::move(prop_col5));
    auto ctx10 = Engine::template Project<false>(
        time_stamp, graph, std::move(ctx_joined2), std::move(proj_opt10));

    auto sort_opt5 = gs::make_sort_opt(
        gs::Range(0, 20),
        gs::OrderingPropPair<gs::SortOrder::DESC, 5, size_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 3, oid_t>("None"));

    auto ctx6 =
        Engine::Sort(time_stamp, graph, std::move(ctx10), std::move(sort_opt5));
    for (auto iter : ctx6) {
      auto eles = iter.GetAllElement();
      output.put_long(std::get<0>(eles));
      output.put_string_view(std::get<1>(eles));
      output.put_string_view(std::get<2>(eles));
      output.put_long(std::get<3>(eles));
      output.put_long(std::get<4>(eles));
      output.put_long(std::get<5>(eles));
    }
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC3_H_
