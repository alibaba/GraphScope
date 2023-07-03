#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP_0>
struct Query0expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr0(int64_t personIdQ2, TAG_PROP_0&& prop_0)
      : personIdQ2_(personIdQ2), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return var0 == personIdQ2_; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t personIdQ2_;
  TAG_PROP_0 prop_0_;
};

template <typename GRAPH_INTERFACE>
class Query0 : public HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   int64_t personIdQ2) const {
    auto expr0 = Query0expr0(personIdQ2, gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr0));
    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto ctx1 = Engine::template EdgeExpandV<1, -1>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto right_ctx1(ctx1);

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);

    auto ctx2 = Engine::template EdgeExpandV<2, -1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt1));

    auto right_edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 10, (label_id_t) 5);

    auto right_ctx2 = Engine::template EdgeExpandV<2, -1>(
        time_stamp, graph, std::move(right_ctx1),
        std::move(right_edge_expand_opt1));

    auto ctx3 = Engine::template Join<0, 1, 0, 1, gs::JoinKind::LeftOuterJoin>(
        std::move(ctx2), std::move(right_ctx2));
    return Engine::Sink(ctx3, std::array<int, 4>{0, 1, 2, 3});
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& decoder) const override {
    int64_t var0 = decoder.get_long();
    return Query(graph, time_stamp, var0);
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