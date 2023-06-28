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
    return (true) && (var0 == 32985348834013);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query0expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr1(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return var0 < 1346112000000; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0, typename TAG_PROP_1>
struct Query0expr2 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query0expr2(TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
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

    auto edge_expand_opt3 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 0, std::array<label_id_t, 2>{3, 2});

    auto ctx2 = Engine::template EdgeExpandVMultiLabel<-1, 1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt3));

    Query0expr1 expr1(gs::NamedProperty<int64_t>("creationDate"));

    auto get_v_opt4 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 2>{2, 3}, std::move(expr1));
    auto ctx3 = Engine::template GetV<2, -1>(time_stamp, graph, std::move(ctx2),
                                             std::move(get_v_opt4));
    Query0expr2 expr2(InnerIdProperty<1>{}, InnerIdProperty<0>{});

    auto ctx4 =
        Engine::Select(time_stamp, graph, std::move(ctx3), std::move(expr2));

    auto project_opt7 = gs::make_project_opt(
        gs::AliasTagProp<1, 0, int64_t>({"id"}),
        gs::AliasTagProp<1, 1, std::string_view>({"firstName"}),
        gs::AliasTagProp<1, 2, std::string_view>({"lastName"}),
        gs::AliasTagProp<2, 3, int64_t>({"id"}),
        gs::AliasTagProp<2, 4, std::string_view>({"content"}),
        gs::AliasTagProp<2, 5, std::string_view>({"imageFile"}),
        gs::AliasTagProp<2, 6, int64_t>({"creationDate"}));

    auto ctx5 = Engine::template Project<0>(time_stamp, graph, std::move(ctx4),
                                            std::move(project_opt7));

    auto sort_opt8 = gs::make_sort_opt(
        gs::Range(0, 20),
        gs::OrderingPropPair<gs::SortOrder::DESC, 6, int64_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 3, int64_t>("None"));

    auto ctx6 =
        Engine::Sort(time_stamp, graph, std::move(ctx5), std::move(sort_opt8));

    return Engine::Sink(ctx6, std::array<int32_t, 7>{3, 4, 5, 6, 7, 8, 9});
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