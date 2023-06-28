
#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP_0>
struct Query11expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query11expr0(std::string_view country_name, TAG_PROP_0&& prop_0)
      : name_(country_name), prop_0_(std::move(prop_0)) {}
  inline auto operator()(std::string_view var0) const { return var0 == name_; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  std::string_view name_;
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query11expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query11expr1(int32_t work_year, TAG_PROP_0&& prop_0)
      : work_year_(work_year), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int32_t var0) const { return var0 < work_year_; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int32_t work_year_;
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query11expr2 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query11expr2(int64_t id, TAG_PROP_0&& prop_0)
      : id_(id), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return (true) && (var0 == id_); }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t id_;
  TAG_PROP_0 prop_0_;
};
template <typename vertex_id_t, typename TAG_PROP_0, typename TAG_PROP_1>
struct Query11expr3 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query11expr3(TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
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
class Query11 : HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& input) const override {
    int64_t id = input.get_long();
    std::string_view country_name = input.get_string();
    int32_t work_year = input.get_int();
    return Query(graph, time_stamp, id, country_name, work_year);
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp, int64_t id,
                                   std::string_view country_name,
                                   int32_t work_year) const {
    auto expr0 =
        Query11expr0(country_name, gs::NamedProperty<std::string_view>("name"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 0, std::move(expr0));
    auto edge_expand_opt0 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 7, (label_id_t) 5);

    auto ctx1 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto expr1 =
        Query11expr1(work_year, gs::NamedProperty<int32_t>("workFrom"));
    auto edge_expand_opt1 = gs::make_edge_expande_opt<int32_t>(
        {"workFrom"}, gs::Direction::In, (label_id_t) 10, (label_id_t) 1,
        std::move(expr1));

    auto ctx2 = Engine::template EdgeExpandE<2, 1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt1));

    auto get_v_opt2 =
        make_getv_opt(gs::VOpt::Start, std::array<label_id_t, 1>{1});
    auto ctx3 = Engine::template GetV<3, -1>(time_stamp, graph, std::move(ctx2),
                                             std::move(get_v_opt2));
    auto edge_expand_opt4 = gs::make_edge_expand_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt3 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{1});
    auto path_opt5 = gs::make_path_expand_opt(
        std::move(edge_expand_opt4), std::move(get_v_opt3), gs::Range(1, 3));
    auto ctx4 = Engine::template PathExpandV<-1, 3>(
        time_stamp, graph, std::move(ctx3), std::move(path_opt5));

    Query11expr2 expr2(id, gs::NamedProperty<int64_t>("id"));

    auto get_v_opt6 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr2));
    auto ctx5 = Engine::template GetV<4, -1>(time_stamp, graph, std::move(ctx4),
                                             std::move(get_v_opt6));
    auto project_opt7 =
        gs::make_project_opt(gs::ProjectSelf<4, 0>(), gs::ProjectSelf<3, 1>(),
                             gs::ProjectSelf<2, 2>(), gs::ProjectSelf<1, 3>());

    auto ctx6 = Engine::template Project<0>(time_stamp, graph, std::move(ctx5),
                                            std::move(project_opt7));

    Query11expr3<vertex_id_t, InnerIdProperty<0>, InnerIdProperty<1>> expr3(
        InnerIdProperty<0>{}, InnerIdProperty<1>{});

    auto ctx7 =
        Engine::Select(time_stamp, graph, std::move(ctx6), std::move(expr3));

    auto project_opt10 =
        gs::make_project_opt(gs::ProjectSelf<1, 0>(), gs::ProjectSelf<3, 1>(),
                             gs::AliasTagProp<2, 2, int32_t>({"workFrom"}));

    auto ctx8 = Engine::template Project<0>(time_stamp, graph, std::move(ctx7),
                                            std::move(project_opt10));

    auto ctx9 = Engine::template Dedup<0, 1, 2>(std::move(ctx8));

    auto sort_opt11 = gs::make_sort_opt(
        gs::Range(0, 10),
        gs::OrderingPropPair<gs::SortOrder::ASC, 2, int32_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("id"),
        gs::OrderingPropPair<gs::SortOrder::DESC, 1, std::string_view>("name"));

    auto ctx10 =
        Engine::Sort(time_stamp, graph, std::move(ctx9), std::move(sort_opt11));

    auto project_opt12 = gs::make_project_opt(
        gs::AliasTagProp<0, 0, int64_t>({"id"}),
        gs::AliasTagProp<0, 1, std::string_view>({"firstName"}),
        gs::AliasTagProp<0, 2, std::string_view>({"lastName"}),
        gs::AliasTagProp<1, 3, std::string_view>({"name"}),
        gs::ProjectSelf<2, 4>());

    auto ctx11 = Engine::template Project<0>(
        time_stamp, graph, std::move(ctx10), std::move(project_opt12));
    return Engine::Sink(ctx11, std::array<int32_t, 5>{5, 6, 7, 8, 4});
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query11<gs::GrapeGraphInterface>* app =
        new gs::Query11<gs::GrapeGraphInterface>();
    return static_cast<void*>(app);
  } else {
    gs::Query11<gs::GrockGraphInterface>* app =
        new gs::Query11<gs::GrockGraphInterface>();
    return static_cast<void*>(app);
  }
}
void DeleteApp(void* app, gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query11<gs::GrapeGraphInterface>* casted =
        static_cast<gs::Query11<gs::GrapeGraphInterface>*>(app);
    delete casted;
  } else {
    gs::Query11<gs::GrockGraphInterface>* casted =
        static_cast<gs::Query11<gs::GrockGraphInterface>*>(app);
    delete casted;
  }
}
}