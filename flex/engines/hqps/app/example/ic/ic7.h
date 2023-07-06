#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC7_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC7_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

class IC7Expr0 {
 public:
  using result_t = bool;
  IC7Expr0() {}

  template <typename TUPLE_T>
  inline auto operator()(const TUPLE_T& tuple) const {
    if (tuple == NONE) {
      return true;
    } else {
      return false;
    }
  }

 private:
};

class IC7Expr1 {
 public:
  using result_t = int64_t;
  IC7Expr1() {}

  inline auto operator()(int64_t like_time, int64_t creation_time) const {
    return (like_time - creation_time) / (1000 * 60);
  }

 private:
};

template <typename GRAPH_INTERFACE>
class QueryIC7 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "PERSON";
  std::string knows_label = "KNOWS";
  std::string post_label = "POST";
  std::string comment_label = "COMMENT";
  std::string has_creator_label = "HASCREATOR";
  std::string forum_label = "FORUM";
  std::string likes_label = "LIKES";
  std::string has_member_label = "HASMEMBER";
  std::string container_of_label = "CONTAINEROF";
  std::string tag_label = "TAG";
  std::string has_tag_label = "HASTAG";
  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ7");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
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
      node.put("likeCreationDate", output_decoder.get_long());
      node.put("messageId", output_decoder.get_long());
      node.put("messageContent", output_decoder.get_string());
      node.put("minutesLatency", output_decoder.get_int());
      node.put("isNew", (bool) output_decoder.get_byte());

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    int32_t limit = 20;

    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t post_label_id = graph.GetVertexLabelId(post_label);
    label_id_t comment_label_id = graph.GetVertexLabelId(comment_label);
    label_id_t has_creator_label_id = graph.GetEdgeLabelId(has_creator_label);
    label_id_t forum_label_id = graph.GetVertexLabelId(forum_label);
    label_id_t likes_label_id = graph.GetEdgeLabelId(likes_label);
    label_id_t has_member_label_id = graph.GetEdgeLabelId(has_member_label);
    label_id_t container_of_label_id = graph.GetEdgeLabelId(container_of_label);
    label_id_t tag_label_id = graph.GetVertexLabelId(tag_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(has_tag_label);

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    LOG(INFO) << "person id: " << id;
    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Persist>(
        graph, person_label_id, id);
    auto ctx_person(ctx0);  // create a copy

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id,
        std::array<label_id_t, 2>{post_label_id, comment_label_id});
    // message
    auto ctx1 = Engine::template EdgeExpandVMultiLabel<AppendOpt::Persist,
                                                       INPUT_COL_ID(-1)>(
        graph, std::move(ctx0), std::move(edge_expand_opt));

    auto edge_expand_opt2 = gs::make_edge_expande_opt<int64_t>(
        gs::PropNameArray<int64_t>{"creationDate"}, gs::Direction::In,
        likes_label_id, person_label_id);

    // liked person
    auto ctx2 =
        Engine::template EdgeExpandE<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt2));

    // get out v.
    auto get_v_opt2 = gs::make_getv_opt(
        gs::VOpt::Start, std::array<label_id_t, 1>{person_label_id});
    auto ctx3 = Engine::template GetV<AppendOpt::Persist, INPUT_COL_ID(2)>(
        graph, std::move(ctx2), std::move(get_v_opt2));

    // from person expand to friend
    auto edge_expand_opt3 = gs::make_edge_expande_opt<int64_t>(
        gs::PropNameArray<int64_t>{"creationDate"}, gs::Direction::Both,
        knows_label_id, person_label_id);
    auto ctx4 =
        Engine::template EdgeExpandE<AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx_person), std::move(edge_expand_opt3));

    // get person
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::Other, std::array<label_id_t, 1>{person_label_id});
    auto ctx5 = Engine::template GetV<AppendOpt::Persist, INPUT_COL_ID(1)>(
        graph, std::move(ctx4), std::move(get_v_opt));

    // outer join
    auto ctx_join = Engine::template Join<0, 3, 0, 2, JoinKind::LeftOuterJoin>(
        std::move(ctx3), std::move(ctx5));
    // after join, should be personId, message, likes, friend, knows

    // project the knows with case when.

    auto ctx6 = Engine::template Project<0>(
        graph, std::move(ctx_join),
        std::tuple{gs::make_mapper_with_variable<1>(
                       PropertySelector<grape::EmptyType>()),
                   gs::make_mapper_with_variable<2>(
                       PropertySelector<int64_t>("creationDate")),
                   gs::make_mapper_with_variable<3>(
                       PropertySelector<grape::EmptyType>()),
                   gs::make_mapper_with_expr<4>(
                       IC7Expr0(), gs::PropertySelector<grape::EmptyType>())});
    // message, likeDate, liker, knows

    auto ctx7 = Engine::Sort(
        graph, std::move(ctx6), gs::Range(0, INT_MAX),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>("None"),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, gs::oid_t>("id")});

    // group by (liker,knows) collection first (msg, likeDate) pair;
    auto ctx8 = Engine::GroupBy(
        graph, std::move(ctx7), std::tuple{GroupKey<2, grape::EmptyType>()},
        std::tuple{gs::make_aggregate_prop<gs::AggFunc::FIRST>(
                       std::tuple{gs::PropertySelector<grape::EmptyType>()},
                       std::integer_sequence<int32_t, 0>()),
                   gs::make_aggregate_prop<gs::AggFunc::FIRST>(
                       std::tuple{gs::PropertySelector<grape::EmptyType>()},
                       std::integer_sequence<int32_t, 1>()),
                   gs::make_aggregate_prop<gs::AggFunc::FIRST>(
                       std::tuple{gs::PropertySelector<grape::EmptyType>()},
                       std::integer_sequence<int32_t, 3>())});
    // liker, message,  likedata, knowsOrNot

    auto ctx9 = Engine::Sort(
        graph, std::move(ctx8), gs::Range(0, 20),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 2, int64_t>("None"),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, gs::oid_t>("id")});

    auto ctx10 = Engine::template Project<0>(
        graph, std::move(ctx9),
        std::tuple{
            gs::make_mapper_with_variable<0>(PropertySelector<gs::oid_t>("id")),
            gs::make_mapper_with_variable<0>(
                PropertySelector<std::string_view>("firstName")),
            gs::make_mapper_with_variable<0>(
                PropertySelector<std::string_view>("lastName")),
            gs::make_mapper_with_variable<2>(
                PropertySelector<grape::EmptyType>()),
            gs::make_mapper_with_variable<1>(PropertySelector<gs::oid_t>("id")),
            gs::make_mapper_with_variable<1>(
                PropertySelector<std::string_view>("content")),
            gs::make_mapper_with_variable<1>(
                PropertySelector<std::string_view>("imageFile")),
            gs::make_mapper_with_expr<2, 1>(
                IC7Expr1(), gs::PropertySelector<grape::EmptyType>(),
                gs::PropertySelector<int64_t>("creationDate")),
            gs::make_mapper_with_variable<3>(
                PropertySelector<grape::EmptyType>())});
    for (auto iter : ctx10) {
      auto ele = iter.GetAllElement();
      //   LOG(INFO) << gs::to_string(ele);
      output.put_long(std::get<0>(ele));
      output.put_string_view(std::get<1>(ele));
      output.put_string_view(std::get<2>(ele));
      output.put_long(std::get<3>(ele));  // likeDate
      output.put_long(std::get<4>(ele));  // message id
      if (std::get<5>(ele).empty()) {
        output.put_string_view(std::get<6>(ele));
      } else {
        output.put_string_view(std::get<5>(ele));
      }
      output.put_int(std::get<7>(ele));   // minus latency
      output.put_byte(std::get<8>(ele));  // knowsOrNot
    }

    LOG(INFO) << "Finish running ic7";
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC7_H_
