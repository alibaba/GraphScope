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
    return (true) && (var0 == 2199023256816);
  }
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
    auto edge_expand_opt0 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 0, std::array<label_id_t, 2>{3, 2});

    auto ctx1 = Engine::template EdgeExpandVMultiLabel<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 2, (label_id_t) 2);

    auto ctx2 = Engine::template EdgeExpandV<2, 1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt1));

    auto edge_expand_opt2 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);

    auto ctx3 = Engine::template EdgeExpandV<3, 2>(
        time_stamp, graph, std::move(ctx2), std::move(edge_expand_opt2));

    auto project_opt3 =
        gs::make_project_opt(gs::ProjectSelf<0, 0>(), gs::ProjectSelf<1, 1>(),
                             gs::ProjectSelf<2, 2>(), gs::ProjectSelf<3, 3>());

    auto ctx4 = Engine::template Project<0>(time_stamp, graph, std::move(ctx3),
                                            std::move(project_opt3));

    auto sort_opt4 = gs::make_sort_opt(
        gs::Range(0, 20),
        gs::OrderingPropPair<gs::SortOrder::DESC, 2, int64_t>("creationDate"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 2, int64_t>("id"));

    auto ctx5 =
        Engine::Sort(time_stamp, graph, std::move(ctx4), std::move(sort_opt4));

    auto project_opt5 = gs::make_project_opt(
        gs::AliasTagProp<3, 0, int64_t>({"id"}),
        gs::AliasTagProp<3, 1, std::string_view>({"firstName"}),
        gs::AliasTagProp<3, 2, std::string_view>({"lastName"}),
        gs::AliasTagProp<2, 3, int64_t>({"creationDate"}),
        gs::AliasTagProp<2, 4, int64_t>({"id"}),
        gs::AliasTagProp<2, 5, std::string_view>({"content"}));

    auto ctx6 = Engine::template Project<0>(time_stamp, graph, std::move(ctx5),
                                            std::move(project_opt5));

    return Engine::Sink(ctx6, std::array<int32_t, 6>{4, 5, 6, 7, 8, 9});
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