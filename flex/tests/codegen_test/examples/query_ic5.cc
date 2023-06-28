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
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == 15393162790207);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query0expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr1(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return var0 > 1344643200000; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};

class Query0 : public HqpsAppBase<GrapeGraphInterface> {
 public:
  results::CollectiveResults Query(const GrapeGraphInterface& graph,
                                   int64_t time_stamp) const override {
    auto expr0 = Query0expr0(gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr0));
    auto edge_expand_opt1 = gs::make_edge_expand_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt0 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{1});
    auto path_opt2 = gs::make_path_expand_opt(
        std::move(edge_expand_opt1), std::move(get_v_opt0), gs::Range(1, 3));
    auto ctx1 = Engine::template PathExpandV<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(path_opt2));

    auto expr1 = Query0expr1(gs::NamedProperty<int64_t>("joinDate"));
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

    auto edge_expand_opt5 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 3, (label_id_t) 3);

    auto ctx4 = Engine::template EdgeExpandV<4, 3>(
        time_stamp, graph, std::move(ctx3), std::move(edge_expand_opt5));

    auto right_edge_expand_opt5 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 0, std::array<label_id_t, 2>{3, 2});

    auto right_ctx4 = Engine::template EdgeExpandVMultiLabel<4, 1>(
        time_stamp, graph, std::move(right_ctx3),
        std::move(right_edge_expand_opt5));

    auto ctx5 = Engine::template Intersect<4, 4>(std::move(ctx4),
                                                 std::move(right_ctx4));
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
void* CreateApp() {
  gs::Query0* app = new gs::Query0();
  return static_cast<void*>(app);
}
void DeleteApp(void* app) {
  gs::Query0* casted = static_cast<gs::Query0*>(app);
  delete casted;
}
}