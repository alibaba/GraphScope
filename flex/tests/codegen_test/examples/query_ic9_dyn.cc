#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP_0>
struct Query9expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query9expr0(int64_t id, TAG_PROP_0&& prop_0)
      : id_(id), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return (true) && (var0 == id_); }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t id_;
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query9expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query9expr1(int64_t max_date, TAG_PROP_0&& prop_0)
      : max_date_(max_date), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return var0 < max_date_; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t max_date_;
  TAG_PROP_0 prop_0_;
};
template <typename vertex_id_t, typename TAG_PROP_0, typename TAG_PROP_1>
struct Query9expr2 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query9expr2(TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
      : prop_0_(std::move(prop_0)), prop_1_(std::move(prop_1)) {}
  inline auto operator()(vertex_id_t var0, vertex_id_t var1) const {
    return var0 != var1;
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};

template <typename GRAPH_INTERFACE>
class Query9 : public HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& input) const override {
    int64_t id = input.get_long();
    int64_t max_date = input.get_long();
    return Query(graph, time_stamp, id, max_date);
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp, int64_t id,
                                   int64_t max_date) const {
    // auto expr0 = Query9expr0(id, gs::NamedProperty<int64_t>("id"));
    // auto ctx0 =
    // Engine::template ScanVertex<0>(time_stamp, graph, 1,
    // std::move(expr0));
    auto ctx0 = Engine::template ScanVertexWithOid<0>(time_stamp, graph, 1, id);
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

    Query9expr1 expr1(max_date, gs::NamedProperty<int64_t>("creationDate"));

    auto get_v_opt4 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 2>{2, 3}, std::move(expr1));
    auto ctx3 = Engine::template GetV<2, -1>(time_stamp, graph, std::move(ctx2),
                                             std::move(get_v_opt4));

    Query9expr2<vertex_id_t, InnerIdProperty<1>, InnerIdProperty<0>> expr2(
        InnerIdProperty<1>{}, InnerIdProperty<0>{});
    auto ctx4 =
        Engine::Select(time_stamp, graph, std::move(ctx3), std::move(expr2));

    auto project_opt7 =
        gs::make_project_opt(gs::ProjectSelf<1, 0>(), gs::ProjectSelf<2, 1>());

    auto ctx5 = Engine::template Project<0>(time_stamp, graph, std::move(ctx4),
                                            std::move(project_opt7));

    auto sort_opt8 = gs::make_sort_opt(
        gs::Range(0, 20),
        gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>("creationDate"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 1, int64_t>("id"));

    auto ctx6 =
        Engine::Sort(time_stamp, graph, std::move(ctx5), std::move(sort_opt8));

    auto project_opt9 = gs::make_project_opt(
        gs::AliasTagProp<0, 0, int64_t>({"id"}),
        gs::AliasTagProp<0, 1, std::string_view>({"firstName"}),
        gs::AliasTagProp<0, 2, std::string_view>({"lastName"}),
        gs::AliasTagProp<1, 3, int64_t>({"id"}),
        gs::AliasTagProp<1, 4, std::string_view>({"content"}),
        gs::AliasTagProp<1, 5, std::string_view>({"imageFile"}),
        gs::AliasTagProp<1, 6, int64_t>({"creationDate"}));

    auto ctx7 = Engine::template Project<0>(time_stamp, graph, std::move(ctx6),
                                            std::move(project_opt9));
    return Engine::Sink(ctx7, std::array<int32_t, 7>{3, 4, 5, 6, 7, 8, 9});
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query9<gs::GrapeGraphInterface>* app =
        new gs::Query9<gs::GrapeGraphInterface>();
    return static_cast<void*>(app);
  } else {
    gs::Query9<gs::GrockGraphInterface>* app =
        new gs::Query9<gs::GrockGraphInterface>();
    return static_cast<void*>(app);
  }
}
void DeleteApp(void* app, gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query9<gs::GrapeGraphInterface>* casted =
        static_cast<gs::Query9<gs::GrapeGraphInterface>*>(app);
    delete casted;
  } else {
    gs::Query9<gs::GrockGraphInterface>* casted =
        static_cast<gs::Query9<gs::GrockGraphInterface>*>(app);
    delete casted;
  }
}
}
