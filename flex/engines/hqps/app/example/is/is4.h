#ifndef ENGINES_HPQS_APP_EXAMPLE_IS_IS4_H_
#define ENGINES_HPQS_APP_EXAMPLE_IS_IS4_H_

#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP_0>
struct IS4expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  IS4expr0(int64_t messageId, TAG_PROP_0&& prop_0)
      : messageId_(messageId), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return var0 == messageId_; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t messageId_;
  TAG_PROP_0 prop_0_;
};

template <typename GRAPH_INTERFACE>
class IS4 {
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
    auto messageId = input.get_long();

    auto expr0 = IS4expr0(messageId, gs::NamedProperty<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<0>(
        time_stamp, graph, std::array<label_id_t, 2>{2, 3}, std::move(expr0));
    auto project_opt0 = gs::make_project_opt(
        gs::AliasTagProp<0, 0, int64_t>({"creationDate"}),
        gs::AliasTagProp<0, 1, std::string_view>({"content"}),
        gs::AliasTagProp<0, 2, std::string_view>({"imageFile"}));

    auto ctx1 = Engine::template Project<0>(time_stamp, graph, std::move(ctx0),
                                            std::move(project_opt0));

    for (auto iter : ctx1) {
      auto eles = iter.GetAllElement();
      output.put_long(std::get<0>(eles));
      if (std::get<1>(eles).empty()) {
        output.put_string_view(std::get<2>(eles));
      } else {
        output.put_string_view(std::get<1>(eles));
      }
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IS_IS4_H_
