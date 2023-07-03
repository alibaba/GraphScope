#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP_0>
struct Query0expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr0(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == 8796093037034);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query0expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr1(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(std::string_view var0) const {
    return var0 == "MartialArtist";
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};

template <typename GRAPH_INTERFACE>
class Query0 : public HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp) const {
    auto expr0 = Query0expr0(gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr0));
    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto ctx1 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0, (label_id_t) 2);

    auto ctx2 = Engine::template EdgeExpandV<2, 1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt1));

    auto edge_expand_opt2 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 2, (label_id_t) 3);

    auto ctx3 = Engine::template EdgeExpandV<3, 2>(
        time_stamp, graph, std::move(ctx2), std::move(edge_expand_opt2));

    auto edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 1, (label_id_t) 7);

    auto ctx4 = Engine::template EdgeExpandV<4, 3>(
        time_stamp, graph, std::move(ctx3), std::move(edge_expand_opt3));

    auto edge_expand_opt4 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 12, (label_id_t) 6);

    auto ctx5 = Engine::template EdgeExpandV<5, 4>(
        time_stamp, graph, std::move(ctx4), std::move(edge_expand_opt4));

    auto edge_expand_opt6 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 13, (label_id_t) 6);

    auto get_v_opt5 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{6});
    auto path_opt7 = gs::make_path_expand_opt(
        std::move(edge_expand_opt6), std::move(get_v_opt5), gs::Range(0, 10));
    auto ctx6 = Engine::template PathExpandV<-1, 5>(
        time_stamp, graph, std::move(ctx5), std::move(path_opt7));

    Query0expr1 expr1(gs::NamedProperty<std::string_view>("name"));

    auto get_v_opt8 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{6}, std::move(expr1));
    auto ctx7 = Engine::template GetV<6, -1>(time_stamp, graph, std::move(ctx6),
                                             std::move(get_v_opt8));
    auto project_opt9 =
        gs::make_project_opt(gs::ProjectSelf<0, 0>(), gs::ProjectSelf<1, 1>(),
                             gs::ProjectSelf<2, 2>(), gs::ProjectSelf<4, 3>());

    auto ctx8 = Engine::template Project<0>(time_stamp, graph, std::move(ctx7),
                                            std::move(project_opt9));

    gs::AliasTagProp<1, 0, grape::EmptyType> group_key10({"None"});

    auto agg_func11 =
        gs::make_aggregate_prop<1, gs::AggFunc::TO_SET, std::string_view>(
            {"name"}, std::integer_sequence<int32_t, 3>{});

    auto agg_func12 = gs::make_aggregate_prop<2, gs::AggFunc::COUNT_DISTINCT,
                                              grape::EmptyType>(
        {"None"}, std::integer_sequence<int32_t, 2>{});

    auto group_opt13 = gs::make_group_opt(
        std::move(group_key10), std::move(agg_func11), std::move(agg_func12));

    auto ctx9 = Engine::GroupBy(time_stamp, graph, std::move(ctx8),
                                std::move(group_opt13));

    auto sort_opt14 = gs::make_sort_opt(
        gs::Range(0, 20),
        gs::OrderingPropPair<gs::SortOrder::DESC, 2, int64_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("id"));

    auto ctx10 =
        Engine::Sort(time_stamp, graph, std::move(ctx9), std::move(sort_opt14));

    auto project_opt15 = gs::make_project_opt(
        gs::AliasTagProp<0, 0, int64_t>({"id"}),
        gs::AliasTagProp<0, 1, std::string_view>({"firstName"}),
        gs::AliasTagProp<0, 2, std::string_view>({"lastName"}),
        gs::ProjectSelf<1, 3>(), gs::ProjectSelf<2, 4>());

    auto ctx11 = Engine::template Project<0>(
        time_stamp, graph, std::move(ctx10), std::move(project_opt15));

    return Engine::Sink(ctx11, std::array<int32_t, 5>{6, 7, 8, 4, 5});
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& decoder) const override {
    return Query(graph, time_stamp);
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query0<gs::GrapeGraphInterface>* app =
        new gs::Query0<gs::GrapeGraphInterface>();
    return static_cast<void*>(app);
  } else {
    gs::Query0<gs::GrockGraphInterface>* app =
        new gs::Query0<gs::GrockGraphInterface>();
    return static_cast<void*>(app);
  }
}
void DeleteApp(void* app, gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query0<gs::GrapeGraphInterface>* casted =
        static_cast<gs::Query0<gs::GrapeGraphInterface>*>(app);
    delete casted;
  } else {
    gs::Query0<gs::GrockGraphInterface>* casted =
        static_cast<gs::Query0<gs::GrockGraphInterface>*>(app);
    delete casted;
  }
}
}