#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

using Engine = SyncEngine<GrapeGraphInterface>;
using label_id_t = typename GrapeGraphInterface::label_id_t;
using vertex_id_t = typename GrapeGraphInterface::vertex_id_t;
template <typename TAG_PROP_0>
struct Query0expr23 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr23(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == 30786325579101);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};

class Query0 : public HqpsAppBase<GrapeGraphInterface> {
 public:
  std::vector<results::Results> Query(const GrapeGraphInterface& graph,
                                      int64_t time_stamp) const override {
    auto expr24 = Query0expr23(gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr24));
    auto edge_expand_opt0 = gs::make_edge_expande_opt<int64_t>(
        {"creationDate"}, gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto ctx1 = Engine::template EdgeExpandE<-1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto get_v_opt1 =
        make_getv_opt(gs::VOpt::Other, std::array<label_id_t, 1>{1});
    auto ctx2 = Engine::template GetV<1, -1>(time_stamp, graph, std::move(ctx1),
                                             std::move(get_v_opt1));
    auto project_opt2 = gs::make_project_opt(gs::ProjectSelf<1, 0>());

    auto ctx3 = Engine::template Project<0>(time_stamp, graph, std::move(ctx2),
                                            std::move(project_opt2));

    return Engine::Sink(ctx3);
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