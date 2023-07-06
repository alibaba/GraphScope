#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC3_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC3_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

class IC3Expre0 {
 public:
  using result_t = bool;
  IC3Expre0(std::string_view ctr_y_name) : ctr_y_name_(ctr_y_name) {}

  auto operator()(std::string_view country_name) const {
    return country_name == ctr_y_name_;
  }

 private:
  std::string_view ctr_y_name_;
};

class IC3Expression2 {
 public:
  using result_t = bool;
  IC3Expression2(std::string_view param1, std::string_view param2)
      : param1_(param1), param2_(param2) {}

  bool operator()(std::string_view country_name) const {
    return param1_ == country_name || param2_ == country_name;
  }

 private:
  std::string_view param1_, param2_;
};

class IC3Expression3 {
 public:
  using result_t = bool;
  IC3Expression3(int64_t start_date, int64_t end_date)
      : start_date_(start_date), end_date_(end_date) {}

  //   template <typename TUPLE_T>
  bool operator()(const int64_t date) const {
    return date >= start_date_ && date < end_date_;
  }

 private:
  int64_t start_date_;
  int64_t end_date_;
};

class IC3Expression4 {
 public:
  using result_t = uint64_t;
  IC3Expression4() {}

  auto operator()(uint64_t a, uint64_t b) const { return a + b; }

 private:
};

template <typename GRAPH_INTERFACE>
class QueryIC3 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "PERSON";
  std::string knows_label = "KNOWS";
  std::string isLocatedIn_label = "ISLOCATEDIN";
  std::string place_label = "PLACE";
  std::string workAt_label = "WORKAT";
  std::string studyAt_label = "STUDYAT";
  std::string is_locatedIn_label = "ISLOCATEDIN";
  std::string org_label = "ORGANISATION";
  std::string isPartOf_label = "ISPARTOF";
  std::string post_label = "POST";
  std::string comment_label = "COMMENT";
  std::string has_creator_label = "HASCREATOR";
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
  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
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

    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, id);

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 3));
    auto ctx1 =
        Engine::template PathExpandV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx0), std::move(path_expand_opt));

    auto edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::Out, is_locatedIn_label_id, place_label_id);
    auto ctx2 = Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(edge_expand_opt3));

    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::Out, isPartOf_label_id, place_label_id);
    auto ctx3 = Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx2), std::move(edge_expand_opt4));

    auto filter =
        gs::make_filter(IC3Expression2(country_x_name, country_y_name),
                        gs::PropertySelector<std::string_view>("name"));
    auto get_v_opt3 = gs::make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{place_label_id},
        std::move(filter));
    auto ctx4 = Engine::template GetV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx3), std::move(get_v_opt3));

    // ctx_x countryx
    auto filter2 =
        gs::make_filter(IC3Expre0(country_x_name),
                        gs::PropertySelector<std::string_view>("name"));
    auto ctx_x0 = Engine::template ScanVertex<AppendOpt::Temp>(
        graph, place_label_id, std::move(filter2));

    auto edge_expand_opt_x0 = gs::make_edge_expandv_opt(
        gs::Direction::In, is_locatedIn_label_id,
        std::array<label_id_t, 2>{post_label_id, comment_label_id});
    // comment and post
    auto ctx_x1 = Engine::template EdgeExpandVMultiLabel<AppendOpt::Temp,
                                                         INPUT_COL_ID(-1)>(
        graph, std::move(ctx_x0), std::move(edge_expand_opt_x0));
    // filter

    auto filter3 =
        gs::make_filter(IC3Expression3(start_date, end_date),
                        gs::PropertySelector<int64_t>("creationDate"));
    auto get_v_optx = gs::make_getv_opt(
        gs::VOpt::Itself,
        std::array<label_id_t, 2>{post_label_id, comment_label_id},
        std::move(filter3));
    auto ctx_x2 = Engine::template GetV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx_x1), std::move(get_v_optx));

    auto edge_expand_opt_x1 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_creator_label_id, person_label_id);
    // person
    auto ctx_x3 =
        Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(0)>(
            graph, std::move(ctx_x2), std::move(edge_expand_opt_x1));

    // group by person

    auto ctx_x4 = Engine::GroupBy(
        graph, std::move(ctx_x3),
        std::tuple{GroupKey<INPUT_COL_ID(-1), grape::EmptyType>()},
        std::tuple{gs::make_aggregate_prop<gs::AggFunc::COUNT>(
            std::tuple{gs::PropertySelector<grape::EmptyType>()},
            std::integer_sequence<int32_t, 0>{})});
    // ctx_x4: person, counts

    auto filter4 =
        gs::make_filter(IC3Expre0(country_y_name),
                        gs::PropertySelector<std::string_view>("name"));
    auto ctx_y0 = Engine::template ScanVertex<AppendOpt::Temp>(
        graph, place_label_id, std::move(filter4));

    auto edge_expand_opt_y0 = gs::make_edge_expandv_opt(
        gs::Direction::In, is_locatedIn_label_id,
        std::array<label_id_t, 2>{post_label_id, comment_label_id});
    // comment and post

    auto ctx_y1 = Engine::template EdgeExpandVMultiLabel<AppendOpt::Temp,
                                                         INPUT_COL_ID(-1)>(
        graph, std::move(ctx_y0), std::move(edge_expand_opt_y0));

    auto filter5 =
        gs::make_filter(IC3Expression3(start_date, end_date),
                        gs::PropertySelector<int64_t>("creationDate"));
    auto get_v_opty = gs::make_getv_opt(
        gs::VOpt::Itself,
        std::array<label_id_t, 2>{post_label_id, comment_label_id},
        std::move(filter5));
    auto ctx_y2 = Engine::template GetV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx_y1), std::move(get_v_opty));

    auto edge_expand_opt_y1 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_creator_label_id, person_label_id);
    // person
    auto ctx_y3 =
        Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(0)>(
            graph, std::move(ctx_y2), std::move(edge_expand_opt_y1));

    // group by person

    auto ctx_y4 = Engine::GroupBy(
        graph, std::move(ctx_y3),
        std::tuple{GroupKey<-1, grape::EmptyType>(
            PropertySelector<grape::EmptyType>())},
        std::tuple{gs::make_aggregate_prop<gs::AggFunc::COUNT>(
            std::tuple{gs::PropertySelector<grape::EmptyType>()},
            std::integer_sequence<int32_t, 0>{})});

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

    auto ctx10 = Engine::template Project<false>(
        graph, std::move(ctx_joined2),
        std::tuple{
            gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                gs::PropertySelector<gs::oid_t>("id")),
            gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                gs::PropertySelector<std::string_view>("firstName")),
            gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                gs::PropertySelector<std::string_view>("lastName")),
            gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                gs::PropertySelector<grape::EmptyType>("")),
            gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                gs::PropertySelector<grape::EmptyType>("")),
            gs::make_mapper_with_expr<INPUT_COL_ID(1), INPUT_COL_ID(2)>(
                IC3Expression4(), gs::PropertySelector<grape::EmptyType>(""),
                gs::PropertySelector<grape::EmptyType>(""))});

    auto ctx6 = Engine::Sort(
        graph, std::move(ctx10), gs::Range(0, 20),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, INPUT_COL_ID(5), size_t>(
                "None"),
            gs::OrderingPropPair<gs::SortOrder::ASC, INPUT_COL_ID(3), oid_t>(
                "None")});
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
