#include "flex/engines/hqps/engine/sync_engine.h"
#include "flex/storages/mutable_csr/grape_graph_interface.h"

namespace gs {

template <typename TAG_PROP_0>
struct Query0expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr1(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == 8780);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};

class Query0 {
 public:
  using Engine = SyncEngine<GrapeGraphInterface>;
  using label_id_t = typename GrapeGraphInterface::label_id_t;
  using vertex_id_t = typename GrapeGraphInterface::vertex_id_t;
  auto Query(const GrapeGraphInterface& graph, int64_t time_stamp) {
    auto expr2 = Query0expr1(gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr2));
    auto edge_expand_opt0 =
        gs::make_edge_expande_opt(gs::Direction::Out, 12, 1);

    auto ctx1 = Engine::template EdgeExpandE<-1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto get_v_opt1 =
        make_getv_opt(gs::VOpt::End, std::array<label_id_t, 1>{1});
    auto ctx2 = Engine::template GetV<1, -1>(time_stamp, graph, std::move(ctx1),
                                             std::move(get_v_opt1));
    auto project_opt2 = gs::make_project_opt(gs::ProjectSelf<1, 0>());

    auto ctx3 = Engine::template Project<0>(time_stamp, graph, std::move(ctx2),
                                            std::move(project_opt2));

    auto ctx4 = Engine::template Dedup<0>(std::move(ctx3));

    return Engine::Sink(ctx4);
  }
};
}  // namespace gs