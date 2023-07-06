#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC9_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC9_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

class IC9Expression2 {
 public:
  using result_t = bool;
  IC9Expression2(int64_t maxDate) : max_date(maxDate) {}

  bool operator()(const int64_t data_tuple) const {
    return data_tuple < max_date;
  }

 private:
  int64_t max_date;
};

template <typename GRAPH_INTERFACE>
class QueryIC9 {
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
  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ9");
    int64_t date = input.get<int64_t>("maxDate");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_long(date);
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
      node.put("messageId", output_decoder.get_long());
      node.put("messageContent", output_decoder.get_string());
      node.put("messageCreationDate", output_decoder.get_long());

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t ts, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    int64_t max_date = input.get_long();
    int32_t limit = 20;

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

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, id);
    // message

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 3));
    auto ctx1_0 =
        Engine::template PathExpandV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx0), std::move(path_expand_opt));

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id,
        std::array<label_id_t, 2>{post_label_id, comment_label_id});

    // message
    auto ctx3 = Engine::template EdgeExpandVMultiLabel<AppendOpt::Persist,
                                                       INPUT_COL_ID(0)>(
        graph, std::move(ctx1_0), std::move(edge_expand_opt2));

    auto filter =
        gs::make_filter(IC9Expression2(max_date),
                        gs::PropertySelector<int64_t>("creationDate"));
    auto ctx4 = Engine::template Select<INPUT_COL_ID(-1)>(
        graph, std::move(ctx3), std::move(filter));

    auto ctx5 = Engine::Sort(
        graph, std::move(ctx4), gs::Range(0, limit),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>(
                "creationDate"),  // indicate the set's element itself.
            gs::OrderingPropPair<gs::SortOrder::ASC, 1, gs::oid_t>("id")  // id
        });

    auto ctx6 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx5),
        std::tuple{
            gs::make_mapper_with_variable<0>(gs::PropertySelector<oid_t>("id")),
            gs::make_mapper_with_variable<0>(
                gs::PropertySelector<std::string_view>("firstName")),
            gs::make_mapper_with_variable<0>(
                gs::PropertySelector<std::string_view>("lastName")),
            gs::make_mapper_with_variable<1>(gs::PropertySelector<oid_t>("id")),
            gs::make_mapper_with_variable<1>(
                gs::PropertySelector<std::string_view>("content")),
            gs::make_mapper_with_variable<1>(
                gs::PropertySelector<std::string_view>("imageFile")),
            gs::make_mapper_with_variable<1>(
                gs::PropertySelector<int64_t>("creationDate"))});

    for (auto iter : ctx6) {
      auto element = iter.GetAllElement();
      output.put_long(std::get<0>(element));         // person id
      output.put_string_view(std::get<1>(element));  // person first name
      output.put_string_view(std::get<2>(element));  // person last name
      output.put_long(std::get<3>(element));         // cmt id.
      if (std::get<4>(element).empty()) {
        output.put_string_view(std::get<5>(element));  // imageFile
      } else {
        output.put_string_view(std::get<4>(element));  // content
      }
      output.put_long(std::get<6>(element));  // cmt creation date
    }
  }
};
}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC9_H_
