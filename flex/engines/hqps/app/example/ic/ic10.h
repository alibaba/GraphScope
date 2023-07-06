#ifndef GRAPHSCOPE_APPS_IC10_H_
#define GRAPHSCOPE_APPS_IC10_H_

#include <time.h>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

class IC10Expression2 {
 public:
  using result_t = bool;
  IC10Expression2(int32_t month) : month_(month) {}

  bool operator()(int64_t time_stamp) const {
    auto millon_second = time_stamp / 1000;
    auto tm = gmtime((time_t*) (&millon_second));

    return (tm->tm_mon + 1 == month_ && tm->tm_mday >= 21) ||
           ((month_ <= 11 && tm->tm_mon == month_ && tm->tm_mday < 22) ||
            (tm->tm_mon == 0 && month_ == 12 && tm->tm_mday < 22));
  }

 private:
  int32_t month_;
};

class IC10Expression3 {
 public:
  using result_t = bool;
  IC10Expression3(int64_t id) : id_(id) {}

  bool operator()(int64_t a) const { return a == id_; }

 private:
  int64_t id_;
};

class IC10Expression4 {
 public:
  using result_t = int32_t;
  int32_t operator()(const int32_t a, int32_t b) const { return a - b; }
};

template <typename GRAPH_INTERFACE>
class QueryIC10 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "PERSON";
  std::string knows_label = "KNOWS";
  std::string post_label = "POST";
  std::string comment_label = "COMMENT";
  std::string has_creator_label = "HASCREATOR";
  std::string reply_of_label = "REPLYOF";
  std::string forum_label = "FORUM";
  std::string likes_label = "LIKES";
  std::string has_member_label = "HASMEMBER";
  std::string container_of_label = "CONTAINEROF";
  std::string tag_label = "TAG";
  std::string has_tag_label = "HASTAG";
  std::string has_interest_in_label = "HASINTEREST";
  std::string is_located_in_label = "ISLOCATEDIN";
  std::string place_label = "PLACE";

  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ10");
    int32_t month = input.get<int32_t>("month");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_int(month);
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
      node.put("commonInterestScore", output_decoder.get_int());
      node.put("personGender", output_decoder.get_string());
      node.put("personCityName", output_decoder.get_string());

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    int32_t month = input.get_int();
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
    label_id_t tag_label_id = graph.GetVertexLabelId(tag_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(has_tag_label);
    label_id_t has_interest_in_label_id =
        graph.GetEdgeLabelId(has_interest_in_label);
    label_id_t is_located_in_label_id =
        graph.GetEdgeLabelId(is_located_in_label);
    label_id_t place_label_id = graph.GetVertexLabelId(place_label);

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, id);
    // message

    // foaf
    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(2, 3));
    auto ctx1 =
        Engine::template PathExpandV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx0), std::move(path_expand_opt));

    // filter person with birthday.
    auto filter = gs::make_filter(IC10Expression2(month),
                                  PropertySelector<int64_t>("birthday"));
    auto foaf_right = Engine::template Select<INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(filter));

    // copied person
    auto foaf_left(foaf_right);
    LOG(INFO) << "num person : " << foaf_left.GetHead().Size();
    for (auto iter : foaf_left) {
      auto eles = iter.GetAllElement();
      LOG(INFO) << "person: " << gs::to_string(eles);
    }

    // left
    auto lambda_x = [&](auto&& left_inner_ctx_0) {
      auto edge_expand_opt1 = gs::make_edge_expandv_opt(
          gs::Direction::In, has_creator_label_id, post_label_id);
      //  post
      auto left_inner_ctx_1 =
          Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
              graph, std::move(left_inner_ctx_0), std::move(edge_expand_opt1));

      auto edge_expand_opt2 = gs::make_edge_expandv_opt(
          gs::Direction::Out, has_tag_label_id, tag_label_id);
      //  tag
      auto left_inner_ctx_2 =
          Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(1)>(
              graph, std::move(left_inner_ctx_1), std::move(edge_expand_opt2));

      auto edge_expand_opt3 = gs::make_edge_expandv_opt(
          gs::Direction::In, has_interest_in_label_id, person_label_id);
      //  person
      auto left_inner_ctx_3 =
          Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
              graph, std::move(left_inner_ctx_2), std::move(edge_expand_opt3));

      auto filter3 = gs::make_filter(IC10Expression3(id),
                                     gs::PropertySelector<oid_t>("id"));
      auto left_inner_ctx_4 = Engine::template Select<INPUT_COL_ID(-1)>(
          graph, std::move(left_inner_ctx_3), std::move(filter3));

      // dedup left on post
      auto left_inner_ctx_5 =
          Engine::template Dedup<INPUT_COL_ID(1)>(std::move(left_inner_ctx_4));

      // TODO: debug, remove this
      auto left_inner_ctx_6 = Engine::template Project<true>(
          graph, std::move(left_inner_ctx_5),
          std::tuple{gs::make_mapper_with_variable<1>(
              gs::PropertySelector<oid_t>("id"))});
      for (auto iter : left_inner_ctx_6) {
        auto eles = iter.GetAllElement();
        LOG(INFO) << "left lambda proj post id: " << gs::to_string(eles);
      }

      return Engine::GroupByWithoutKey(
          graph, std::move(left_inner_ctx_6),
          std::tuple{gs::make_aggregate_prop<AggFunc::COUNT>(
              std::tuple{gs::PropertySelector<grape::EmptyType>()},
              std::integer_sequence<int32_t, 1>{})});
    };

    auto foaf_left_ctx2 =
        Engine::template Apply<AppendOpt::Persist, JoinKind::InnerJoin>(
            std::move(foaf_left), std::move(lambda_x));

    // right

    auto lambda_y = [&](auto&& in_ctx0) {
      auto edge_expand_opt7 = gs::make_edge_expandv_opt(
          gs::Direction::In, has_creator_label_id, post_label_id);
      //  post
      auto ctx7 =
          Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
              graph, std::move(in_ctx0), std::move(edge_expand_opt7));

      auto lambda_yy = [&](auto&& iin_ctx0) {
        auto edge_expand_opt8 = gs::make_edge_expandv_opt(
            gs::Direction::Out, has_tag_label_id, tag_label_id);
        //  tag
        auto ctx8 =
            Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(1)>(
                graph, std::move(iin_ctx0), std::move(edge_expand_opt8));

        auto edge_expand_opt9 = gs::make_edge_expandv_opt(
            gs::Direction::In, has_interest_in_label_id, person_label_id);

        //  person
        auto ctx9 =
            Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
                graph, std::move(ctx8), std::move(edge_expand_opt9));

        auto filter4 = gs::make_filter(IC10Expression3(id),
                                       gs::PropertySelector<oid_t>("id"));
        return Engine::template Select<INPUT_COL_ID(-1)>(graph, std::move(ctx9),
                                                         std::move(filter4));
      };
      auto ctx8 = Engine::template Apply<JoinKind::AntiJoin>(
          std::move(ctx7), std::move(lambda_yy));

      auto ctx9 = Engine::template Dedup<1>(std::move(ctx8));

      auto res = Engine::GroupByWithoutKey(
          graph, std::move(ctx9),
          std::tuple{gs::make_aggregate_prop<gs::AggFunc::COUNT>(
              std::tuple{gs::PropertySelector<grape::EmptyType>()},
              std::integer_sequence<int32_t, -1>{})});
      return res;
    };
    auto foaf_right_ctx2 =
        Engine::template Apply<AppendOpt::Persist, JoinKind::InnerJoin>(
            std::move(foaf_right), std::move(lambda_y));

    auto joined = Engine::template Join<0, 0, JoinKind::InnerJoin>(
        std::move(foaf_left_ctx2), std::move(foaf_right_ctx2));

    // project with expression.
    // 1: foaf
    // 2: common score
    // 3. uncommon score

    auto ctx8 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(joined),
        std::tuple{
            gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                gs::PropertySelector<grape::EmptyType>()),
            gs::make_mapper_with_expr<INPUT_COL_ID(1), INPUT_COL_ID(2)>(
                IC10Expression4(), gs::PropertySelector<grape::EmptyType>(),
                gs::PropertySelector<grape::EmptyType>())});

    auto ctx9 = Engine::Sort(
        graph, std::move(ctx8), gs::Range(0, 10),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, int32_t>(
                "none"),  // the element itself.
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, gs::oid_t>("id")});

    // get city
    auto edge_expand_opt10 = gs::make_edge_expandv_opt(
        gs::Direction::Out, is_located_in_label_id, place_label_id);
    auto ctx10 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx9), std::move(edge_expand_opt10));

    auto ctx11 = Engine::template Project<false>(
        graph, std::move(ctx10),
        std::tuple{
            gs::make_mapper_with_variable<0>(gs::PropertySelector<oid_t>("id")),
            gs::make_mapper_with_variable<0>(
                gs::PropertySelector<std::string_view>("firstName")),
            gs::make_mapper_with_variable<0>(
                gs::PropertySelector<std::string_view>("lastName")),
            gs::make_mapper_with_variable<0>(
                gs::PropertySelector<std::string_view>("gender")),
            gs::make_mapper_with_variable<1>(
                gs::PropertySelector<grape::EmptyType>()),
            gs::make_mapper_with_variable<2>(
                gs::PropertySelector<std::string_view>("name"))});

    {
      for (auto iter : ctx11) {
        auto tuple = iter.GetAllElement();
        LOG(INFO) << "tuple: " << gs::to_string(tuple);
        output.put_long(std::get<0>(tuple));
        output.put_string_view(std::get<1>(tuple));
        output.put_string_view(std::get<2>(tuple));
        output.put_int(std::get<4>(tuple));
        output.put_string_view(std::get<3>(tuple));
        output.put_string_view(std::get<5>(tuple));
      }
    }
  }
};  // namespace gs
}  // namespace gs

#endif  // GRAPHSCOPE_APPS_IC10_H_