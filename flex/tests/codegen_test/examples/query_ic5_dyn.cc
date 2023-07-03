#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP_0>
struct Query5expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query5expr0(int64_t id, TAG_PROP_0&& prop_0)
      : id_(id), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return (true) && (var0 == id_); }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t id_;
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query5expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query5expr1(int64_t min_date, TAG_PROP_0&& prop_0)
      : min_date_(min_date), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return var0 > min_date_; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t min_date_;
  TAG_PROP_0 prop_0_;
};

template <typename GRAPH_INTERFACE>
class Query5 : HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& input) const override {
    int64_t id = input.get_long();
    int64_t min_date = input.get_long();
    return Query(graph, time_stamp, id, min_date);
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp, int64_t id,
                                   int64_t min_join_date) const {
    // auto expr0 = Query5expr0(id, gs::NamedProperty<int64_t>("id"));
    // auto ctx0 =
    // Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr0));
    auto ctx0 = Engine::template ScanVertexWithOid<0>(time_stamp, graph, 1, id);
    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt0 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{1});
    auto path_opt2 = gs::make_path_expand_opt(
        std::move(edge_expand_opt1), std::move(get_v_opt0), gs::Range(1, 3));
    auto ctx1 = Engine::template PathExpandV<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(path_opt2));

    auto expr1 =
        Query5expr1(min_join_date, gs::NamedProperty<int64_t>("joinDate"));
    auto edge_expand_opt3 = gs::make_edge_expande_opt<int64_t>(
        {"joinDate"}, gs::Direction::In, (label_id_t) 4, (label_id_t) 4,
        std::move(expr1));

    auto ctx2 = Engine::template EdgeExpandE<2, 1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt3));

    auto get_v_opt4 =
        make_getv_opt(gs::VOpt::Start, std::array<label_id_t, 1>{4});
    auto ctx3 = Engine::template GetV<3, -1>(time_stamp, graph, std::move(ctx2),
                                             std::move(get_v_opt4));
    auto right_ctx3(ctx3);

    auto edge_expand_opt5 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 3, (label_id_t) 3);

    auto ctx4 = Engine::template EdgeExpandV<4, 3>(
        time_stamp, graph, std::move(ctx3), std::move(edge_expand_opt5));

    // auto right_edge_expand_opt5 = gs::make_edge_expandv_opt(
    //     gs::Direction::In, (label_id_t) 0, std::array<label_id_t, 2>{3, 2});
    // auto right_ctx4 = Engine::template EdgeExpandVMultiLabel<4, 1>(
    //     time_stamp, graph, std::move(right_ctx3),
    //     std::move(right_edge_expand_opt5));

    auto right_edge_expand_opt5 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0, (label_id_t) 3);
    auto right_ctx4 = Engine::template EdgeExpandV<4, 1>(
        time_stamp, graph, std::move(right_ctx3),
        std::move(right_edge_expand_opt5));

    double t = -grape::GetCurrentTime();
    auto ctx5 = Engine::template Intersect<4, 4>(std::move(ctx4),
                                                 std::move(right_ctx4));
    t += grape::GetCurrentTime();
    LOG(INFO) << "intersect cost: " << t;
    auto get_v_opt6 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{3});
    auto ctx6 = Engine::template GetV<4, 4>(time_stamp, graph, std::move(ctx5),
                                            std::move(get_v_opt6));
    gs::AliasTagProp<3, 0, grape::EmptyType> group_key7({"None"});

    auto agg_func8 = gs::make_aggregate_prop<1, gs::AggFunc::COUNT_DISTINCT,
                                             grape::EmptyType>(
        {"None"}, std::integer_sequence<int32_t, 4>{});

    auto group_opt9 =
        gs::make_group_opt(std::move(group_key7), std::move(agg_func8));

    auto ctx7 = Engine::GroupBy(time_stamp, graph, std::move(ctx6),
                                std::move(group_opt9));

    auto sort_opt10 = gs::make_sort_opt(
        gs::Range(0, 20),
        gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("id"));

    auto ctx8 =
        Engine::Sort(time_stamp, graph, std::move(ctx7), std::move(sort_opt10));

    auto project_opt11 = gs::make_project_opt(
        gs::AliasTagProp<0, 0, std::string_view>({"title"}),
        gs::ProjectSelf<1, 1>());

    auto ctx9 = Engine::template Project<0>(time_stamp, graph, std::move(ctx8),
                                            std::move(project_opt11));

    return Engine::Sink(ctx9, std::array<int32_t, 2>{7, 6});
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query5<gs::GrapeGraphInterface>* app =
        new gs::Query5<gs::GrapeGraphInterface>();
    return static_cast<void*>(app);
  } else {
    gs::Query5<gs::GrockGraphInterface>* app =
        new gs::Query5<gs::GrockGraphInterface>();
    return static_cast<void*>(app);
  }
}
void DeleteApp(void* app, gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query5<gs::GrapeGraphInterface>* casted =
        static_cast<gs::Query5<gs::GrapeGraphInterface>*>(app);
    delete casted;
  } else {
    gs::Query5<gs::GrockGraphInterface>* casted =
        static_cast<gs::Query5<gs::GrockGraphInterface>*>(app);
    delete casted;
  }
}
}