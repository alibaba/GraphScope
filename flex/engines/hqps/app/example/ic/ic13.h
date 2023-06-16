#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC13_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC13_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP>
class IC13Expression0 {
 public:
  IC13Expression0(oid_t oid, TAG_PROP&& props) : oid_(oid), props_(props) {}

  template <typename TUPLE_T>
  bool operator()(const TUPLE_T& data_tuple) const {
    return std::get<0>(data_tuple) == oid_;
  }

  TAG_PROP Properties() const { return props_; }

 private:
  oid_t oid_;
  TAG_PROP props_;
};

template <typename GRAPH_INTERFACE>
class IC13 {
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
    oid_t src_id = input.get<oid_t>("person1IdQ13StartNode");
    oid_t dst_id = input.get<oid_t>("person2IdQ13EndNode");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(src_id);
    input_encoder.put_long(dst_id);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      output.put("shortestPathLength", output_decoder.get_int());  // id
      // output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t src_id = input.get_long();
    int64_t dst_id = input.get_long();

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
                                                       person_label_id, src_id);
    // message

    auto edge_expand_opt6 = gs::make_edge_expand_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});

    gs::NamedProperty<oid_t> id_prop("id");
    IC13Expression0 expr(dst_id, std::move(id_prop));

    auto shortest_path_opt = gs::make_shortest_path_opt(
        std::move(edge_expand_opt6), std::move(get_v_opt),
        gs::Range(0, INT_MAX), std::move(expr), PathOpt::Simple,
        ResultOpt::AllV);

    auto ctx1 = Engine::template ShortestPath<0, -1>(
        time_stamp, graph, std::move(ctx0), std::move(shortest_path_opt));

    size_t len = 0;
    for (auto iter : ctx1) {
      auto ele = std::get<0>(iter.GetAllElement());
      if (len != 0) {
        auto l = ele.length();
        CHECK(len == l);
      } else {
        len = ele.length();
      }
      // VLOG(10) << gs::to_string(iter.GetAllElement());
    }
    output.put_int(len);
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC13_H_
