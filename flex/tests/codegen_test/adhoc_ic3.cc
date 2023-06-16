#include "flex/engines/hqps/engine/sync_engine.h"
#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/storages/mutable_csr/grape_graph_interface.h"

namespace gs {

template <typename TAG_PROP_0>
struct Query0expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr0(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(std::string_view var0) const {
    return (true) && (var0 == "Switzerland");
  }
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
    return var0 >= 1298937600000 && var1 < 1301702400000;
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};
template <typename TAG_PROP_0, typename TAG_PROP_1>
struct Query0expr3 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query0expr3(TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
      : prop_0_(std::move(prop_0)), prop_1_(std::move(prop_1)) {}
  inline auto operator()(int64_t var0, int64_t var1) const {
    return var0 >= 1298937600000 && var1 < 1301702400000;
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};
template <typename TAG_PROP_0>
struct Query0expr4 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr4(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(std::string_view var0) const {
    return (true) && (var0 == "Papua_New_Guinea");
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0>
struct Query0expr5 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr5(TAG_PROP_0&& prop_0) : prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == 27493);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0, typename TAG_PROP_1>
struct Query0expr6 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query0expr6(TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
      : prop_0_(std::move(prop_0)), prop_1_(std::move(prop_1)) {}
  inline auto operator()(std::string_view var0, std::string_view var1) const {
    return var0 != "Papua_New_Guinea" && var1 != "Switzerland";
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};
template <typename TAG_PROP_0, typename TAG_PROP_1>
struct Query0expr7 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query0expr7(TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
      : prop_0_(std::move(prop_0)), prop_1_(std::move(prop_1)) {}
  template <typename vertex_id_t>
  inline auto operator()(vertex_id_t var0, vertex_id_t var1) const {
    return var0 + var1;
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};

template <typename GRAPH_INTERFACE>
class Query0 : public HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp) const {
    auto expr0 = Query0expr0(gs::NamedProperty<std::string_view>("name"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 0, std::move(expr0));
    auto edge_expand_opt0 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 7, std::array<label_id_t, 2>{3, 2});

    auto ctx1 = Engine::template EdgeExpandVMultiLabel<-1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    Query0expr1 expr1(gs::NamedProperty<int64_t>("creationDate"),
                      gs::NamedProperty<int64_t>("creationDate"));

    auto get_v_opt1 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 2>{2, 3}, std::move(expr1));
    auto ctx2 = Engine::template GetV<1, -1>(time_stamp, graph, std::move(ctx1),
                                             std::move(get_v_opt1));
    auto edge_expand_opt2 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);

    auto ctx3 = Engine::template EdgeExpandV<-1, 1>(
        time_stamp, graph, std::move(ctx2), std::move(edge_expand_opt2));

    auto get_v_opt3 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{1});
    auto ctx4 = Engine::template GetV<2, -1>(time_stamp, graph, std::move(ctx3),
                                             std::move(get_v_opt3));
    auto edge_expand_opt4 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 0, std::array<label_id_t, 2>{3, 2});

    auto ctx5 = Engine::template EdgeExpandVMultiLabel<-1, 2>(
        time_stamp, graph, std::move(ctx4), std::move(edge_expand_opt4));

    Query0expr3 expr2(gs::NamedProperty<int64_t>("creationDate"),
                      gs::NamedProperty<int64_t>("creationDate"));

    auto get_v_opt5 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 2>{2, 3}, std::move(expr2));
    auto ctx6 = Engine::template GetV<3, -1>(time_stamp, graph, std::move(ctx5),
                                             std::move(get_v_opt5));
    auto edge_expand_opt6 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);

    auto ctx7 = Engine::template EdgeExpandV<-1, 3>(
        time_stamp, graph, std::move(ctx6), std::move(edge_expand_opt6));

    Query0expr4 expr3(gs::NamedProperty<std::string_view>("name"));

    auto get_v_opt7 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr3));
    auto ctx8 = Engine::template GetV<4, -1>(time_stamp, graph, std::move(ctx7),
                                             std::move(get_v_opt7));
    auto edge_expand_opt9 = gs::make_edge_expand_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto get_v_opt8 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 1>{1});
    auto path_opt10 = gs::make_path_expand_opt(
        std::move(edge_expand_opt9), std::move(get_v_opt8), gs::Range(1, 3));
    auto ctx9 = Engine::template PathExpandV<-1, 2>(
        time_stamp, graph, std::move(ctx8), std::move(path_opt10));

    Query0expr5 expr4(gs::NamedProperty<int64_t>("id"));

    auto get_v_opt11 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr4));
    auto ctx10 = Engine::template GetV<5, -1>(
        time_stamp, graph, std::move(ctx9), std::move(get_v_opt11));
    auto edge_expand_opt12 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);

    auto ctx11 = Engine::template EdgeExpandV<6, 2>(
        time_stamp, graph, std::move(ctx10), std::move(edge_expand_opt12));

    auto edge_expand_opt13 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 11, (label_id_t) 0);

    auto ctx12 = Engine::template EdgeExpandV<-1, 6>(
        time_stamp, graph, std::move(ctx11), std::move(edge_expand_opt13));

    Query0expr6 expr5(gs::NamedProperty<std::string_view>("name"),
                      gs::NamedProperty<std::string_view>("name"));

    auto get_v_opt14 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{0}, std::move(expr5));
    auto ctx13 = Engine::template GetV<7, -1>(
        time_stamp, graph, std::move(ctx12), std::move(get_v_opt14));
    gs::AliasTagProp<2, 0, grape::EmptyType> group_key15({"None"});

    auto agg_func16 =
        gs::make_aggregate_prop<1, gs::AggFunc::COUNT, grape::EmptyType>(
            {"None"}, std::integer_sequence<int32_t, 3>{});

    auto agg_func17 =
        gs::make_aggregate_prop<2, gs::AggFunc::COUNT, grape::EmptyType>(
            {"None"}, std::integer_sequence<int32_t, 1>{});

    auto group_opt18 = gs::make_group_opt(
        std::move(group_key15), std::move(agg_func16), std::move(agg_func17));

    auto ctx14 = Engine::GroupBy(time_stamp, graph, std::move(ctx13),
                                 std::move(group_opt18));

    auto project_opt19 = gs::make_project_opt(
        gs::AliasTagProp<0, 0, int64_t>({"id"}),
        gs::AliasTagProp<0, 1, std::string_view>({"firstName"}),
        gs::AliasTagProp<0, 2, std::string_view>({"lastName"}),
        gs::ProjectSelf<1, 3>(), gs::ProjectSelf<2, 4>(),
        make_project_expr<5, int64_t>(
            Query0expr7(InnerIdProperty<1>{}, InnerIdProperty<2>{})));

    auto ctx15 = Engine::template Project<0>(
        time_stamp, graph, std::move(ctx14), std::move(project_opt19));

    auto sort_opt22 = gs::make_sort_opt(
        gs::Range(0, 20),
        gs::OrderingPropPair<gs::SortOrder::DESC, 5, int64_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("None"));

    auto ctx16 = Engine::Sort(time_stamp, graph, std::move(ctx15),
                              std::move(sort_opt22));

    return Engine::Sink(ctx16, std::array<int32_t, 6>{10, 11, 12, 8, 9, 13});
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& decoder) const override {
    return Query(graph, time_stamp);
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