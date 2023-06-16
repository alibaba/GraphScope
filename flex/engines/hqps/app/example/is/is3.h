#ifndef ENGINES_HPQS_APP_EXAMPLE_IS_IS3_H_
#define ENGINES_HPQS_APP_EXAMPLE_IS_IS3_H_

#include "flex/engines/hqps/engine/sync_engine.h"
#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/storages/mutable_csr/grape_graph_interface.h"

namespace gs {

template <typename TAG_PROP_0>
struct IS3expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  IS3expr0(int64_t personId, TAG_PROP_0&& prop_0)
      : personId_(personId), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == personId_);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t personId_;
  TAG_PROP_0 prop_0_;
};

template <typename GRAPH_INTERFACE>
class IS3 {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ2");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("messageId", output_decoder.get_string());       // id
      node.put("messageContent", output_decoder.get_string());  // dist
      LOG(FATAL) << "impl";

      output.push_back(std::make_pair("", node));
    }
  }

  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    auto personId = input.get_long();

    auto expr0 = IS3expr0(personId, gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expande_opt<int64_t>(
        {"creationDate"}, gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);
    auto ctx1 = Engine::template EdgeExpandE<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));
    auto get_v_opt1 =
        make_getv_opt(gs::VOpt::Other, std::array<label_id_t, 1>{1});
    auto ctx2 = Engine::template GetV<2, -1>(time_stamp, graph, std::move(ctx1),
                                             std::move(get_v_opt1));
    auto project_opt2 = gs::make_project_opt(
        gs::AliasTagProp<2, 0, int64_t>({"id"}),
        gs::AliasTagProp<2, 1, std::string_view>({"firstName"}),
        gs::AliasTagProp<2, 2, std::string_view>({"lastName"}),
        gs::AliasTagProp<1, 3, int64_t>({"creationDate"}));

    auto ctx3 = Engine::template Project<0>(time_stamp, graph, std::move(ctx2),
                                            std::move(project_opt2));

    auto sort_opt3 = gs::make_sort_opt(
        gs::Range(0, 0),
        gs::OrderingPropPair<gs::SortOrder::DESC, 3, int64_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("None"));

    auto ctx4 =
        Engine::Sort(time_stamp, graph, std::move(ctx3), std::move(sort_opt3));

    for (auto iter : ctx4) {
      auto eles = iter.GetAllElement();
      output.put_long(std::get<0>(eles));
      output.put_string_view(std::get<1>(eles));
      output.put_string_view(std::get<2>(eles));
      output.put_long(std::get<3>(eles));
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IS_IS3_H_
