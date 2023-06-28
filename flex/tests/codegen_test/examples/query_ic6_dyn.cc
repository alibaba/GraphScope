

#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

template <typename TAG_PROP_0>
struct Query6expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query6expr0(std::string_view tag_name, TAG_PROP_0&& prop_0)
      : tag_name_(tag_name), prop_0_(std::move(prop_0)) {}
  inline auto operator()(std::string_view var0) const {
    return var0 == tag_name_;
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  std::string_view tag_name_;
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query6expr2 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query6expr2(int64_t id, TAG_PROP_0&& prop_0)
      : id_(id), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return (true) && (var0 == id_); }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t id_;
  TAG_PROP_0 prop_0_;
};
template <typename vertex_id_t, typename TAG_PROP_0, typename TAG_PROP_1>
struct Query6expr3 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query6expr3(TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
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
class Query6 : public HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& input) const override {
    int64_t id = input.get_long();
    std::string_view tag_name = input.get_string();
    return Query(graph, time_stamp, id, tag_name);
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp, int64_t id,
                                   std::string_view tag_name) const {
    auto expr0 =
        Query6expr0(tag_name, gs::NamedProperty<std::string_view>("name"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 7, std::move(expr0));
    auto edge_expand_opt0 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 1, (label_id_t) 3);

    auto ctx1 = Engine::template EdgeExpandV<-1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto get_v_opt1 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{3});
    auto ctx2 = Engine::template GetV<1, -1>(time_stamp, graph, std::move(ctx1),
                                             std::move(get_v_opt1));
    auto edge_expand_opt2 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);

    auto ctx3 = Engine::template EdgeExpandV<2, 1>(
        time_stamp, graph, std::move(ctx2), std::move(edge_expand_opt2));

    auto edge_expand_opt4 = gs::make_edge_expand_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt3 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{1});
    auto path_opt5 = gs::make_path_expand_opt(
        std::move(edge_expand_opt4), std::move(get_v_opt3), gs::Range(1, 3));
    auto ctx4 = Engine::template PathExpandV<-1, 2>(
        time_stamp, graph, std::move(ctx3), std::move(path_opt5));

    Query6expr2 expr1(id, gs::NamedProperty<int64_t>("id"));

    auto get_v_opt6 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr1));
    auto ctx5 = Engine::template GetV<3, -1>(time_stamp, graph, std::move(ctx4),
                                             std::move(get_v_opt6));
    auto edge_expand_opt7 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 1, (label_id_t) 7);

    auto ctx6 = Engine::template EdgeExpandV<4, 1>(
        time_stamp, graph, std::move(ctx5), std::move(edge_expand_opt7));

    Query6expr3<vertex_id_t, InnerIdProperty<4>, InnerIdProperty<0>> expr2(
        InnerIdProperty<4>{}, InnerIdProperty<0>{});

    auto ctx7 =
        Engine::Select(time_stamp, graph, std::move(ctx6), std::move(expr2));

    gs::AliasTagProp<4, 0, std::string_view> group_key10({"name"});

    auto agg_func11 = gs::make_aggregate_prop<1, gs::AggFunc::COUNT_DISTINCT,
                                              grape::EmptyType>(
        {"None"}, std::integer_sequence<int32_t, 1>{});

    auto group_opt12 =
        gs::make_group_opt(std::move(group_key10), std::move(agg_func11));

    auto ctx8 = Engine::GroupBy(time_stamp, graph, std::move(ctx7),
                                std::move(group_opt12));

    auto sort_opt13 = gs::make_sort_opt(
        gs::Range(0, 10),
        gs::OrderingPropPair<gs::SortOrder::DESC, 1, int64_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view>("None"));

    auto ctx9 =
        Engine::Sort(time_stamp, graph, std::move(ctx8), std::move(sort_opt13));

    return Engine::Sink(ctx9, std::array<int32_t, 2>{5, 6});
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query6<gs::GrapeGraphInterface>* app =
        new gs::Query6<gs::GrapeGraphInterface>();
    return static_cast<void*>(app);
  } else {
    gs::Query6<gs::GrockGraphInterface>* app =
        new gs::Query6<gs::GrockGraphInterface>();
    return static_cast<void*>(app);
  }
}
void DeleteApp(void* app, gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query6<gs::GrapeGraphInterface>* casted =
        static_cast<gs::Query6<gs::GrapeGraphInterface>*>(app);
    delete casted;
  } else {
    gs::Query6<gs::GrockGraphInterface>* casted =
        static_cast<gs::Query6<gs::GrockGraphInterface>*>(app);
    delete casted;
  }
}
}