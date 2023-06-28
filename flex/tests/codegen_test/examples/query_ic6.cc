#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

using Engine = SyncEngine<GrapeGraphInterface>;
using label_id_t = typename GrapeGraphInterface::label_id_t;
using vertex_id_t = typename GrapeGraphInterface::vertex_id_t;
template <typename TAG_PROP_0>
struct Query0expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr0(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(std::string_view var0) const {
    return var0 == "Shakira";
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query0expr2 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr2(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == 30786325579101);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0, typename TAG_PROP_1>
struct Query0expr3 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query0expr3(TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
      : prop_0_(std::move(prop_0)), prop_1_(std::move(prop_1)) {}
  inline auto operator()(vertex_id_t var0, vertex_id_t var1) const {
    return var0 != var1;
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};

class Query0 : public HqpsAppBase<GrapeGraphInterface> {
 public:
  results::CollectiveResults Query(const GrapeGraphInterface& graph,
                                   int64_t time_stamp) const override {
    auto expr0 = Query0expr0(gs::NamedProperty<std::string_view>("name"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 7, std::move(expr0));
    auto edge_expand_opt0 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 1, (label_id_t) 3);

    auto ctx1 = Engine::template EdgeExpandV<-1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto get_v_opt1 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{3});
    auto ctx2 = Engine::template GetV<1, -1>(time_stamp, graph, std::move(ctx1),
                                             std::move(get_v_opt1));
    auto edge_expand_opt2 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);

    auto ctx3 = Engine::template EdgeExpandV<2, 1>(
        time_stamp, graph, std::move(ctx2), std::move(edge_expand_opt2));

    auto edge_expand_opt4 = gs::make_edge_expand_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt3 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{1});
    auto path_opt5 = gs::make_path_expand_opt(
        std::move(edge_expand_opt4), std::move(get_v_opt3), gs::Range(1, 3));
    auto ctx4 = Engine::template PathExpandV<-1, 2>(
        time_stamp, graph, std::move(ctx3), std::move(path_opt5));

    Query0expr2 expr1(gs::NamedProperty<int64_t>("id"));

    auto get_v_opt6 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr1));
    auto ctx5 = Engine::template GetV<3, -1>(time_stamp, graph, std::move(ctx4),
                                             std::move(get_v_opt6));
    auto edge_expand_opt7 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 1, (label_id_t) 7);

    auto ctx6 = Engine::template EdgeExpandV<4, 1>(
        time_stamp, graph, std::move(ctx5), std::move(edge_expand_opt7));

    Query0expr3 expr2(InnerIdProperty<4>{}, InnerIdProperty<0>{});

    auto ctx7 =
        Engine::Select(time_stamp, graph, std::move(ctx6), std::move(expr2));

    gs::AliasTagProp<4, 0, std::string_view> group_key10({"name"});

    auto agg_func11 = gs::make_aggregate_prop<1, gs::AggFunc::COUNT_DISTINCT,
                                              grape::EmptyType>(
        {"None"}, std::integer_sequence<int32_t, 1>{});

    auto group_opt12 =
        gs::make_group_opt(std::move(group_key10), std::move(agg_func11));

    auto ctx8 = Engine::GroupBy(time_stamp, graph, std::move(ctx7),
                                std::move(group_opt12));

    auto sort_opt13 = gs::make_sort_opt(
        gs::Range(0, 10),
        gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view>("None"));

    auto ctx9 =
        Engine::Sort(time_stamp, graph, std::move(ctx8), std::move(sort_opt13));

    return Engine::Sink(ctx9, std::array<int32_t, 2>{5, 6});
  }
};
}  // namespace gs

extern "C" {
void* CreateApp() {
  gs::Query0* app = new gs::Query0();
  return static_cast<void*>(app);
}
void DeleteApp(void* app) {
  gs::Query0* casted = static_cast<gs::Query0*>(app);
  delete casted;
}
}