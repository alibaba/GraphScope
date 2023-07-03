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
  inline auto operator()(int64_t var0) const { return var0 == 10995116278874; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0, typename TAG_PROP_1>
struct Query0expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query0expr1(TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
      : prop_0_(std::move(prop_0)), prop_1_(std::move(prop_1)) {}
  inline auto operator()(int64_t var0, int64_t var1) const {
    return var0 >= 1338508800000 && var1 < 1340928000000;
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};
template <typename TAG_PROP_0>
struct Query0right_expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0right_expr0(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return var0 == 10995116278874; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query0right_expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0right_expr1(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return var0 < 1338508800000; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};

class Query0 : public HqpsAppBase<GrapeGraphInterface> {
 public:
  std::vector<results::Results> Query(const GrapeGraphInterface& graph,
                                      int64_t time_stamp) const override {
    auto expr0 = Query0expr0(gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr0));
    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto ctx1 = Engine::template EdgeExpandV<1, -1>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0, (label_id_t) 3);

    auto ctx2 = Engine::template EdgeExpandV<-1, 1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt1));

    Query0expr1 expr1(gs::NamedProperty<int64_t>("creationDate"),
                      gs::NamedProperty<int64_t>("creationDate"));

    auto get_v_opt2 = make_getv_opt(
        gs::VOpt::Start, std::array<label_id_t, 1>{3}, std::move(expr1));
    auto ctx3 = Engine::template GetV<2, -1>(time_stamp, graph, std::move(ctx2),
                                             std::move(get_v_opt2));
    auto edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 1, (label_id_t) 7);

    auto ctx4 = Engine::template EdgeExpandV<3, 2>(
        time_stamp, graph, std::move(ctx3), std::move(edge_expand_opt3));

    auto right_expr0 = Query0right_expr0(gs::NamedProperty<int64_t>("id"));
    auto right_ctx0 = Engine::template ScanVertex<0>(time_stamp, graph, 1,
                                                     std::move(right_expr0));
    auto right_edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto right_ctx1 = Engine::template EdgeExpandV<1, -1>(
        time_stamp, graph, std::move(right_ctx0),
        std::move(right_edge_expand_opt0));

    auto right_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::In, (label_id_t) 0, (label_id_t) 3);

    auto right_ctx2 = Engine::template EdgeExpandV<-1, 1>(
        time_stamp, graph, std::move(right_ctx1),
        std::move(right_edge_expand_opt1));

    Query0right_expr1 right_expr1(gs::NamedProperty<int64_t>("creationDate"));

    auto right_get_v_opt2 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{3}, std::move(right_expr1));
    auto right_ctx3 = Engine::template GetV<2, -1>(
        time_stamp, graph, std::move(right_ctx2), std::move(right_get_v_opt2));
    auto right_edge_expand_opt3 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 1, (label_id_t) 7);

    auto right_ctx4 = Engine::template EdgeExpandV<3, 2>(
        time_stamp, graph, std::move(right_ctx3),
        std::move(right_edge_expand_opt3));

    auto ctx5 = Engine::template Join<3, 3, gs::JoinKind::AntiJoin>(
        std::move(ctx4), std::move(right_ctx4));
    gs::AliasTagProp<3, 0, std::string_view> group_key4({"name"});

    auto agg_func5 = gs::make_aggregate_prop<1, gs::AggFunc::COUNT_DISTINCT,
                                             grape::EmptyType>(
        {"None"}, std::integer_sequence<int32_t, 2>{});

    auto group_opt6 =
        gs::make_group_opt(std::move(group_key4), std::move(agg_func5));

    auto ctx6 = Engine::GroupBy(time_stamp, graph, std::move(ctx5),
                                std::move(group_opt6));

    auto sort_opt7 = gs::make_sort_opt(
        gs::Range(0, 10),
        gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view>("None"));

    auto ctx7 =
        Engine::Sort(time_stamp, graph, std::move(ctx6), std::move(sort_opt7));

    // as the plan is not generate, we fake two tag_ids for ic4.
    return Engine::Sink(ctx7, std::array<int32_t, 2>{0, 1});
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
