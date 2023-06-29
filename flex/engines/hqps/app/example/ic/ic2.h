#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC2_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC2_H_

#include "flex/engines/hqps/engine/sync_engine.h"
#include "flex/utils/app_utils.h"

namespace gs {

template <typename TAG_PROP>
class IC2Expression2 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC2Expression2(int64_t maxDate, TAG_PROP&& props)
      : maxDate_(maxDate), props_(std::move(props)) {}

  inline bool operator()(int64_t data) const {
    // auto& cur_date = std::get<0>(data_tuple);
    return data < maxDate_;
  }

  tag_prop_t Properties() const { return std::make_tuple(props_); }

 private:
  int64_t maxDate_;
  TAG_PROP props_;
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
    auto ctx0 = Engine::template ScanVertexWithOid<-1>(time_stamp, graph,
                                                       person_label_id, id);

    double t0 = -grape::GetCurrentTime();
    auto edge_expand_opt = gs::make_edge_expand_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto ctx1 = Engine::template EdgeExpandV<0, -1>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt));

    std::array<label_id_t, 2> labels{post_label_id, comment_label_id};
    auto edge_expand_opt2 = gs::make_edge_expand_opt(
        gs::Direction::In, has_creator_label_id, std::move(labels));

    auto ctx3 = Engine::template EdgeExpandVMultiLabel<1, -1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt2));

    t0 += grape::GetCurrentTime();

    double t1 = -grape::GetCurrentTime();
    IC2Expression2 expr2(maxDate, gs::NamedProperty<int64_t>("creationDate"));
    // IC2Expression2 expr2(maxDate, std::move(expr_tag_prop));
    auto ctx5 =
        Engine::Select(time_stamp, graph, std::move(ctx3), std::move(expr2));
    t1 += grape::GetCurrentTime();

    double t2 = -grape::GetCurrentTime();
    //     gs::OrderingPair<gs::SortOrder::DESC, -1, 0, int64_t> pair0;
    gs::OrderingPropPair<gs::SortOrder::DESC, -1, int64_t> pair0(
        "creationDate");  // creationDate.
    // gs::OrderingPair<gs::SortOrder::ASC, -1, 1, oid_t> pair1;  // id
    gs::OrderingPropPair<gs::SortOrder::ASC, -1, oid_t> pair1("id");  // id
    // IC2SortComparator comparator;
    // std::move(comparator),
    auto pairs =
        gs::make_sort_opt(gs::Range(0, 20), std::move(pair0), std::move(pair1));
    auto ctx6 =
        Engine::Sort(time_stamp, graph, std::move(ctx5), std::move(pairs));

    // auto ctx6 = Engine::template GetVAndSort<1, -1>(
    // time_stamp, graph, std::move(ctx3), std::move(get_v_opt),
    // std::move(pairs));

    t2 += grape::GetCurrentTime();

    // project
    //@0.id, firstName, lastName,
    //@1.id, content, imageFile, creationDate.
    double t3 = -grape::GetCurrentTime();
    gs::AliasTagProp<0, 0, oid_t, std::string_view, std::string_view> prop_col0(
        {"id", "firstName", "lastName"});
    gs::AliasTagProp<1, 1, oid_t, std::string_view, std::string_view, int64_t>
        prop_col1({"id", "content", "imageFile", "creationDate"});
    auto proj_opt =
        gs::make_project_opt(std::move(prop_col0), std::move(prop_col1));
    auto ctx7 = Engine::template Project<false>(
        time_stamp, graph, std::move(ctx6), std::move(proj_opt));
    t3 += grape::GetCurrentTime();

    double t4 = -grape::GetCurrentTime();
    {
      size_t cnt = 0;
      for (auto iter : ctx7) {
        cnt += 1;
        // if (cnt < 10) {
        //    VLOG(10) << gs::to_string(iter.GetAllElement());
        auto data_tuple = iter.GetAllData();
        auto col0 = std::get<0>(data_tuple);
        auto col1 = std::get<1>(data_tuple);
        output.put_long(std::get<0>(col0));
        output.put_string_view(std::get<1>(col0));
        output.put_string_view(std::get<2>(col0));
        output.put_long(std::get<0>(col1));
        if (std::get<2>(col1).empty()) {
          output.put_string_view(std::get<1>(col1));
          // VLOG(10) << "found imagefile";
        } else {
          output.put_string_view(std::get<2>(col1));
        }
        output.put_long(std::get<3>(col1));
      }
    }
    t4 += grape::GetCurrentTime();
    LOG(INFO) << "edge expand cost: " << t0 << ", filter cost: " << t1
              << ", osrt cost: " << t2 << ", project times: " << t3
              << ", output time: " << t4;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_IC2_IC2_H_
