#ifndef ENGINES_HPQS_APP_EXAMPLE_IS_IS7_H_
#define ENGINES_HPQS_APP_EXAMPLE_IS_IS7_H_

#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP_0>
struct IS7Expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  IS7Expr0(int64_t messageId, TAG_PROP_0&& prop_0)
      : messageId_(messageId), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == messageId_);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t messageId_;
  TAG_PROP_0 prop_0_;
};

template <typename TAG_PROP_0>
struct IS7Expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  IS7Expr1(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}

  template <typename ELE_t>
  inline auto operator()(const ELE_t& edge_ele_tuple) const {
    if (edge_ele_tuple == NONE) {
      return false;
    } else {
      return true;
    }
  }

  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};

template <typename GRAPH_INTERFACE>
class IS7 {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    LOG(INFO) << "start";
    oid_t id = input.get<oid_t>("messageRepliesId");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("commentId", output_decoder.get_long());         // id
      node.put("commentContent", output_decoder.get_string());  // dist

      output.push_back(std::make_pair("", node));
    }
  }

  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    auto messageId = input.get_long();

    auto expr0 = IS7Expr0(messageId, gs::NamedProperty<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<0>(
        time_stamp, graph, std::array<label_id_t, 2>{2, 3}, std::move(expr0));
    auto right_ctx(ctx0);

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 2, (label_id_t) 2);

    auto ctx1 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);

    auto ctx2 = Engine::template EdgeExpandV<2, 0>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt1));
    for (auto iter : ctx2) {
      auto ele = iter.GetAllElement();
      LOG(INFO) << "ctx2:" << gs::to_string(ele);
    }

    // right side

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);
    auto right_ctx1 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(right_ctx), std::move(edge_expand_opt2));

    auto edge_expand_opt3 = gs::make_edge_expande_opt<int64_t>(
        {"creationDate"}, gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);
    auto right_ctx2 = Engine::template EdgeExpandE<2, 1>(
        time_stamp, graph, std::move(right_ctx1), std::move(edge_expand_opt3));

    auto get_v_opt0 =
        make_getv_opt(gs::VOpt::Other, std::array<label_id_t, 1>{1});
    auto right_ctx3 = Engine::template GetV<3, 2>(
        time_stamp, graph, std::move(right_ctx2), std::move(get_v_opt0));

    for (auto iter : right_ctx3) {
      auto ele = iter.GetAllElement();
      LOG(INFO) << "ctx2:" << gs::to_string(ele);
    }

    auto ctx3 = Engine::template Join<0, 2, 0, 3, JoinKind::LeftOuterJoin>(
        std::move(ctx2), std::move(right_ctx3));
    // message, cmt, replyAuthor, msgAuthor, knows.

    auto proj_opt = gs::make_project_opt(
        gs::AliasTagProp<1, 0, int64_t>({"id"}),
        gs::AliasTagProp<1, 1, std::string_view>({"content"}),
        gs::AliasTagProp<1, 2, int64_t>({"creationDate"}),
        gs::AliasTagProp<2, 3, int64_t>({"id"}),
        gs::AliasTagProp<2, 4, std::string_view>({"firstName"}),
        gs::AliasTagProp<2, 5, std::string_view>({"lastName"}),
        gs::make_project_expr<6, bool>(IS7Expr1(gs::InnerIdProperty<4>())));

    auto ctx4 = Engine::template Project<0>(time_stamp, graph, std::move(ctx3),
                                            std::move(proj_opt));

    for (auto iter : ctx4) {
      auto ele = iter.GetAllElement();
      LOG(INFO) << gs::to_string(ele);
      output.put_long(std::get<0>(ele));
      output.put_string_view(std::get<1>(ele));
      output.put_long(std::get<2>(ele));
      output.put_long(std::get<3>(ele));
      output.put_string_view(std::get<4>(ele));
      output.put_string_view(std::get<5>(ele));
      output.put_byte(std::get<6>(ele));
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IS_IS7_H_
