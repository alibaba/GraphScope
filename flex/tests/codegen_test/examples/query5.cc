#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

using Engine = SyncEngine<GrapeGraphInterface>;
using label_id_t = typename GrapeGraphInterface::label_id_t;
using vertex_id_t = typename GrapeGraphInterface::vertex_id_t;
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

class Query0 {
 public:
  auto Query(const GrapeGraphInterface& graph, int64_t time_stamp) {
    auto expr2 = Query0expr1(gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr2));
    auto edge_expand_opt1 = gs::make_edge_expand_opt(
        gs::Direction::Both, (label_id_t) 12, (label_id_t) 1);

    auto get_v_opt0 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{1});
    auto path_opt2 = gs::make_path_expand_opt(
        std::move(edge_expand_opt1), std::move(get_v_opt0), gs::Range(1, 2));
    auto ctx1 = Engine::template PathExpandV<-1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(path_opt2));

    auto get_v_opt3 =
        make_getv_opt(gs::VOpt::End, std::array<label_id_t, 1>{1});
    auto ctx2 = Engine::template GetV<1, -1>(time_stamp, graph, std::move(ctx1),
                                             std::move(get_v_opt3));
    Query0expr3 expr4(InnerIdProperty<vertex_id_t>{},
                      InnerIdProperty<vertex_id_t>{});

    auto ctx3 =
        Engine::Select(time_stamp, graph, std::move(ctx2), std::move(expr4));

    auto project_opt6 =
        gs::make_project_opt(gs::ProjectSelf<0, 0>(), gs::ProjectSelf<1, 1>());

    auto ctx4 = Engine::template Project<0>(time_stamp, graph, std::move(ctx3),
                                            std::move(project_opt6));

    return Engine::Sink(ctx4);
  }
};
}  // namespace gs