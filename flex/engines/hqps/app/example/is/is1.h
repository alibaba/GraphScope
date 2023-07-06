#ifndef ENGINES_HPQS_APP_EXAMPLE_IS_IS1_H_
#define ENGINES_HPQS_APP_EXAMPLE_IS_IS1_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP_0>
struct IS1Expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  IS1Expr0(int64_t personId, TAG_PROP_0&& prop_0)
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
class IS1 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "PERSON";
  std::string knows_label = "KNOWS";
  std::string isLocatedIn_label = "ISLOCATEDIN";
  std::string place_label = "PLACE";
  std::string workAt_label = "WORKAT";
  std::string studyAt_label = "STUDYAT";

  std::string org_label = "ORGANISATION";
  // static std::string_view firstName = "Jack";

  using Engine = SyncEngine<GRAPH_INTERFACE>;

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ1");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("friendFirstName", output_decoder.get_string());   // id
      node.put("friendLastName", output_decoder.get_string());    // dist
      node.put("friendBirthday", output_decoder.get_long());      // lastName"
      node.put("friendLocationIP", output_decoder.get_string());  // birthday
      node.put("friendBrowserUsed",
               output_decoder.get_string());  // creationDate
      node.put("friendId",
               output_decoder.get_long());  // gender
      node.put("friendGender",
               output_decoder.get_string());  // browserUsed
      node.put("friendCreationDate",
               output_decoder.get_long());  // locationIP

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    using Engine = SyncEngine<GRAPH_INTERFACE>;
    using label_id_t = typename GRAPH_INTERFACE::label_id_t;

    int64_t personId = input.get_long();

    auto expr0 = IS1Expr0(personId, gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr0));
    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);

    auto ctx1 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto project_opt1 = gs::make_project_opt(
        gs::AliasTagProp<0, 0, std::string_view>({"firstName"}),
        gs::AliasTagProp<0, 1, std::string_view>({"lastName"}),
        gs::AliasTagProp<0, 2, int64_t>({"birthday"}),
        gs::AliasTagProp<0, 3, std::string_view>({"locationIP"}),
        gs::AliasTagProp<0, 4, std::string_view>({"browserUsed"}),
        gs::AliasTagProp<1, 5, int64_t>({"id"}),
        gs::AliasTagProp<0, 6, std::string_view>({"gender"}),
        gs::AliasTagProp<0, 7, int64_t>({"creationDate"}));

    auto ctx2 = Engine::template Project<0>(time_stamp, graph, std::move(ctx1),
                                            std::move(project_opt1));
    // no limit
    for (auto iter : ctx2) {
      auto ele = iter.GetAllElement();
      output.put_string_view(std::get<0>(ele));
      output.put_string_view(std::get<1>(ele));
      output.put_long(std::get<2>(ele));
      output.put_string_view(std::get<3>(ele));
      output.put_string_view(std::get<4>(ele));
      output.put_long(std::get<5>(ele));
      output.put_string_view(std::get<6>(ele));
      output.put_long(std::get<7>(ele));
    }
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IS_IS1_H_
