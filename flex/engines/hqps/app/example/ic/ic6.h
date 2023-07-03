#ifndef ENGINES_HPQS_APP_EXAMPLE_IC_IC6_H_
#define ENGINES_HPQS_APP_EXAMPLE_IC_IC6_H_

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "flex/utils/app_utils.h"

#include "flex/engines/hqps/engine/context.h"
#include "flex/engines/hqps/engine/hqps_utils.h"

#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

// tagName == ''
class IC6Expression2 {
 public:
  IC6Expression2(std::string_view param1) : param1_(param1) {}

  bool operator()(std::string_view name) const { return name == param1_; }

 private:
  std::string_view param1_;
};

class IC6Expression3 {
 public:
  IC6Expression3(std::string_view param1) : param1_(param1) {}

  bool operator()(std::string_view name) { return name != param1_; }

 private:
  std::string_view param1_;
};

template <typename GRAPH_INTERFACE>
class IC6 {
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  // static gs::oid_t oid_param = 6597069767117;
  std::string person_label = "PERSON";
  std::string knows_label = "KNOWS";
  std::string post_label = "POST";
  std::string comment_label = "COMMENT";
  std::string has_creator_label = "HASCREATOR";
  std::string forum_label = "FORUM";
  std::string has_member_label = "HASMEMBER";
  std::string container_of_label = "CONTAINEROF";
  std::string tag_label = "TAG";
  std::string has_tag_label = "HASTAG";
  // static std::string_view firstName = "Jack";

 public:
  void Query(const GRAPH_INTERFACE& graph, int64_t ts,
             const boost::property_tree::ptree& input,
             boost::property_tree::ptree& output) const {
    oid_t id = input.get<oid_t>("personIdQ6");
    std::string tagName = input.get<std::string>("tagName");
    int32_t limit = 10;

    std::vector<char> input_buffer, output_buffer;
    Encoder input_encoder(input_buffer);
    input_encoder.put_long(id);
    input_encoder.put_string(tagName);
    input_encoder.put_int(limit);
    Decoder input_decoder(input_buffer.data(), input_buffer.size());

    Encoder output_encoder(output_buffer);
    Query(graph, ts, input_decoder, output_encoder);

    Decoder output_decoder(output_buffer.data(), output_buffer.size());
    while (!output_decoder.empty()) {
      boost::property_tree::ptree node;
      node.put("tagName", output_decoder.get_string());  // id
      node.put("postCount", output_decoder.get_int());   // post cnt

      output.push_back(std::make_pair("", node));
    }
    LOG(INFO) << "Finsih put result into ptree";
  }
  void Query(const GRAPH_INTERFACE& graph, int64_t time_stamp, Decoder& input,
             Encoder& output) const {
    int64_t id = input.get_long();
    std::string_view tag_name = input.get_string();
    int32_t limit = 10;

    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId(person_label);
    label_id_t knows_label_id = graph.GetEdgeLabelId(knows_label);
    label_id_t post_label_id = graph.GetVertexLabelId(post_label);
    label_id_t comment_label_id = graph.GetVertexLabelId(comment_label);
    label_id_t has_creator_label_id = graph.GetEdgeLabelId(has_creator_label);
    label_id_t forum_label_id = graph.GetVertexLabelId(forum_label);
    label_id_t has_member_label_id = graph.GetEdgeLabelId(has_member_label);
    label_id_t container_of_label_id = graph.GetEdgeLabelId(container_of_label);
    label_id_t tag_label_id = graph.GetVertexLabelId(tag_label);
    label_id_t has_tag_label_id = graph.GetEdgeLabelId(has_tag_label);

    using Engine = SyncEngine<GRAPH_INTERFACE>;
    auto ctx0 = Engine::template ScanVertexWithOid<AppendOpt::Temp>(
        graph, person_label_id, id);

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto get_v_opt = gs::make_getv_opt(
        gs::VOpt::End, std::array<label_id_t, 1>{person_label_id});
    auto path_expand_opt = gs::make_path_expand_opt(
        std::move(edge_expand_opt), std::move(get_v_opt), gs::Range(1, 3));
    auto ctx1 = Engine::template PathExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx0), std::move(path_expand_opt));
    LOG(INFO) << "Got " << ctx1.GetHead().Size()
              << " vertices after path expand";

    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::In, has_creator_label_id, post_label_id);
    auto ctx4 =
        Engine::template EdgeExpandV<AppendOpt::Persist, INPUT_COL_ID(-1)>(
            graph, std::move(ctx1), std::move(edge_expand_opt4));

    auto edge_expand_opt5 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx5 = Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx4), std::move(edge_expand_opt5));
    {
      size_t cnt = 0;
      for (auto iter : ctx5) {
        cnt++;
      }
      LOG(INFO) << "Before filter with eq tag name" << cnt;
    }

    auto filter =
        gs::make_filter(IC6Expression2(tag_name),
                        gs::PropertySelector<std::string_view>("name"));
    auto get_v_opt6 = gs::make_getv_opt(gs::VOpt::Itself,
                                        std::array<label_id_t, 1>{tag_label_id},
                                        std::move(filter));
    auto ctx6 = Engine::template GetV<AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx5), std::move(get_v_opt6));

    {
      size_t cnt = 0;
      for (auto iter : ctx6) {
        auto ele = iter.GetAllElement();
        LOG(INFO) << gs::to_string(ele);
        cnt += 1;
      }
      LOG(INFO) << "After filter with eq tag name" << cnt;
    }

    LOG(INFO) << "after get v, head size: " << ctx6.GetHead().Size();

    auto ctx7 = Engine::template Project<false>(
        graph, std::move(ctx6),
        std::tuple{gs::IdentityMapper<0, InternalIdSelector>()});
    for (auto iter : ctx7) {
      auto ele = iter.GetAllElement();
      LOG(INFO) << gs::to_string(ele);
    }

    auto edge_expand_opt8 = gs::make_edge_expandv_opt(
        gs::Direction::Out, has_tag_label_id, tag_label_id);
    auto ctx8 = Engine::template EdgeExpandV<AppendOpt::Temp, INPUT_COL_ID(0)>(
        graph, std::move(ctx7), std::move(edge_expand_opt8));

    {
      size_t cnt = 0;
      for (auto iter : ctx8) {
        auto ele = iter.GetAllElement();
        LOG(INFO) << "before filter with tagName" << gs::to_string(ele);
        cnt++;
      }
      LOG(INFO) << "Before filter cost: " << cnt;
    }

    auto filter2 =
        gs::make_filter(IC6Expression3(tag_name),
                        gs::PropertySelector<std::string_view>("name"));
    auto get_v_opt9 = gs::make_getv_opt(gs::VOpt::Itself,
                                        std::array<label_id_t, 1>{tag_label_id},
                                        std::move(filter2));
    auto ctx9 = Engine::template GetV<AppendOpt::Persist, LAST_COL>(
        graph, std::move(ctx8), std::move(get_v_opt9));
    LOG(INFO) << "after filter with name neq" << tag_name
              << ", head size: " << ctx9.GetHead().Size();

    GroupKey<1, grape::EmptyType> group_key(
        gs::PropertySelector<grape::EmptyType>{});
    auto agg = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>{}},
        std::integer_sequence<int32_t, 0>{});
    auto ctx10 = Engine::GroupBy(graph, std::move(ctx9),
                                 std::tuple{std::move(group_key)},
                                 std::tuple{std::move(agg)});

    // // sort by
    // TODO: sort by none.none, means using inner id as sort key.
    gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t> pair0(
        "None");  // indicate the set's element itself.
    gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view> pair1(
        "name");  // id
    auto ctx11 = Engine::Sort(graph, std::move(ctx10), gs::Range(0, 10),
                              std::tuple{pair0, pair1});

    // gs::AliasTagProp<0, 2, std::string_view> prop_col0({"name"});
    // gs::AliasTagProp<4, 6, gs::oid_t> prop_col1({"id"});
    auto mapper1 = gs::make_mapper_with_variable<0>(
        PropertySelector<std::string_view>("name"));
    auto mapper2 =
        gs::make_mapper_with_variable<1>(PropertySelector<grape::EmptyType>());
    auto ctx12 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx11), std::tuple{mapper1, mapper2});

    for (auto iter : ctx12) {
      auto ele = iter.GetAllElement();
      output.put_string_view(std::get<0>(ele));
      output.put_int(std::get<1>(ele));
      LOG(INFO) << gs::to_string(ele);
    }
    LOG(INFO) << "End";
  }
};

}  // namespace gs

#endif  // ENGINES_HPQS_APP_EXAMPLE_IC_IC6_H_
