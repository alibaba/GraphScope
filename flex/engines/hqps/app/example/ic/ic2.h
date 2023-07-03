#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC2_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC2_H_

#include "flex/engines/hqps/engine/sync_engine.h"
#include "flex/utils/app_utils.h"

namespace gs {

struct IC2Expression1 {
 public:
  IC2Expression1(oid_t oid) : oid_(oid) {}

  inline bool operator()(oid_t data) const { return oid_ == data; }

 private:
  oid_t oid_;
};

class IC2Expression2 {
 public:
  IC2Expression2(int64_t maxDate) : maxDate_(maxDate) {}

  inline bool operator()(int64_t data) const {
    // auto& cur_date = std::get<0>(data_tuple);
    return data < maxDate_;
  }

 private:
  int64_t maxDate_;
};

template <typename GRAPH_INTERFACE>
class QueryIC2 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "PERSON";
  std::string knows_label = "KNOWS";
  std::string post_label = "POST";
  std::string comment_label = "COMMENT";
  std::string has_creator_label = "HASCREATOR";
  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ2");
    int64_t maxDate = input.get<int64_t>("maxDate");
    int32_t limit = input.get<int32_t>("limit");

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_long(maxDate);
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
      node.put("messageId", output_decoder.get_long());          // birthday
      node.put("messageContent",
               output_decoder.get_string());  // creationDate
      node.put("messageCreationDate",
               output_decoder.get_long());  // gender

      output.push_back(std::make_pair("", node));
    }
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    LOG(INFO) << "ic2 start";
    int64_t id = input.get_long();
    int64_t maxDate = input.get_long();

    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t post_label_id = graph.GetVertexLabelId(post_label);
    label_id_t comment_label_id = graph.GetVertexLabelId(comment_label);
    label_id_t has_creator_label_id = graph.GetEdgeLabelId(has_creator_label);

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    // auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
    //                                                    person_label_id, id);
    auto filter =
        gs::make_filter(IC2Expression1(id), gs::PropertySelector<oid_t>("id"));
    auto ctx0 = Engine::template ScanVertex<AppendOpt::Temp>(
        graph, person_label_id, std::move(filter));

    auto edge_expand_opt = gs::make_edge_expand_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto ctx1 = Engine::template EdgeExpandV<AppendOpt::Persist, LAST_COL>(
        graph, std::move(ctx0), std::move(edge_expand_opt));

    std::array<label_id_t, 2> labels{post_label_id, comment_label_id};
    auto edge_expand_opt2 = gs::make_edge_expand_opt(
        gs::Direction::In, has_creator_label_id, std::move(labels));
    auto ctx3 =
        Engine::template EdgeExpandVMultiLabel<AppendOpt::Temp, LAST_COL>(
            graph, std::move(ctx1), std::move(edge_expand_opt2));

    auto filter2 = gs::make_filter(
        IC2Expression2(maxDate), gs::PropertySelector<int64_t>("creationDate"));
    auto ctx5 = Engine::template Select<INPUT_COL_ID(-1)>(
        graph, std::move(ctx3), std::move(filter2));

    gs::OrderingPropPair<gs::SortOrder::DESC, -1, int64_t> pair0(
        "creationDate");  // creationDate.
    gs::OrderingPropPair<gs::SortOrder::ASC, -1, oid_t> pair1("id");
    auto ctx6 = Engine::Sort(graph, std::move(ctx5), gs::Range(0, 20),
                             std::tuple{pair0, pair1});

    // project
    // double t3 = -grape::GetCurrentTime();
    auto mapper1 = gs::make_identity_mapper<0>(PropertySelector<oid_t>("id"));
    auto mapper2 = gs::make_identity_mapper<0>(
        PropertySelector<std::string_view>("firstName"));
    auto mapper3 = gs::make_identity_mapper<0>(
        PropertySelector<std::string_view>("lastName"));
    auto mapper4 = gs::make_identity_mapper<1>(PropertySelector<oid_t>("id"));
    auto mapper5 = gs::make_identity_mapper<1>(
        PropertySelector<std::string_view>("content"));
    auto mapper6 = gs::make_identity_mapper<1>(
        PropertySelector<std::string_view>("imageFile"));
    auto mapper7 =
        gs::make_identity_mapper<1>(PropertySelector<int64_t>("creationDate"));
    auto ctx7 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx6),
        std::tuple{std::move(mapper1), std::move(mapper2), std::move(mapper3),
                   std::move(mapper4), std::move(mapper5), std::move(mapper6),
                   std::move(mapper7)});

    for (auto iter : ctx7) {
      auto ele = iter.GetAllElement();
      LOG(INFO) << gs::to_string(ele);
    }

    {
      size_t cnt = 0;
      for (auto iter : ctx7) {
        cnt += 1;
        auto ele = iter.GetAllElement();
        output.put_long(std::get<0>(ele));
        output.put_string_view(std::get<1>(ele));
        output.put_string_view(std::get<2>(ele));
        output.put_long(std::get<3>(ele));
        if (std::get<4>(ele).empty()) {
          output.put_string_view(std::get<5>(ele));
          // VLOG(10) << "found imagefile";
        } else {
          output.put_string_view(std::get<4>(ele));
        }
        output.put_long(std::get<6>(ele));
      }
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_IC2_IC2_H_
