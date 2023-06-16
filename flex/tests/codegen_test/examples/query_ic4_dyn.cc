
#include "flex/engines/hqps/engine/sync_engine.h"
#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/storages/mutable_csr/grape_graph_interface.h"

namespace gs {

template <typename TAG_PROP>
class IC4Expression0 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC4Expression0(int64_t minDate, TAG_PROP&& props)
      : minDate_(minDate), props_(props) {}

  bool operator()(int64_t date) const { return date < minDate_; }

  tag_prop_t Properties() const { return std::make_tuple(props_); }

 private:
  int64_t minDate_;
  TAG_PROP props_;
};

template <typename TAG_PROP>
class IC4Expression1 {
 public:
  using tag_prop_t = std::tuple<TAG_PROP>;
  IC4Expression1(int64_t minDate, int64_t maxDate, TAG_PROP&& props)
      : minDate_(minDate), maxDate_(maxDate), props_(props) {}

  bool operator()(int64_t date) const {
    return date < maxDate_ && date >= minDate_;
  }

  tag_prop_t Properties() const { return std::make_tuple(props_); }

 private:
  int64_t minDate_, maxDate_;
  TAG_PROP props_;
};

template <typename GRAPH_INTERFACE>
class Query4 : HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& input) const override {
    int64_t id = input.get_long();
    int64_t start_date = input.get_long();
    int64_t duration_days = input.get_int();
    // NOT GENERATABLE.
    int64_t end_date = start_date + 1l * 86400000l * duration_days;
    return Query(graph, time_stamp, id, start_date, end_date);
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp, int64_t id,
                                   int64_t start_date, int64_t end_date) const {
    auto ctx0 =
        Engine::template ScanVertexWithOid<-1>(time_stamp, graph, 1, id);

    auto edge_expand_opt = gs::make_edge_expand_opt(gs::Direction::Both, 8, 1);
    auto ctx1 = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt));

    auto edge_expand_opt2 = gs::make_edge_expand_opt(gs::Direction::In, 0, 3);
    auto ctx2 = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt2));

    IC4Expression0 expr2(start_date,
                         gs::NamedProperty<int64_t>("creationDate"));
    auto get_v_opt2 = gs::make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{3}, std::move(expr2));
    auto ctx3 = Engine::template GetV<-1, -1>(
        time_stamp, graph, std::move(ctx2), std::move(get_v_opt2));

    auto edge_expand_opt3 =
        gs::make_edge_expand_opt(gs::Direction::Out, 1, 7);  // has tag, tag
    auto ctx4 = Engine::template EdgeExpandV<0, -1>(
        time_stamp, graph, std::move(ctx3), std::move(edge_expand_opt3));

    /// right side
    auto ctx_right0 =
        Engine::template ScanVertexWithOid<-1>(time_stamp, graph, 1, id);

    auto edge_expand_opt4 = gs::make_edge_expand_opt(gs::Direction::Both, 8, 1);
    auto ctx_right1 = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(ctx_right0), std::move(edge_expand_opt4));

    auto edge_expand_opt5 = gs::make_edge_expand_opt(gs::Direction::In, 0, 3);
    auto ctx_right3 = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(ctx_right1), std::move(edge_expand_opt5));

    IC4Expression1 expr3(start_date, end_date,
                         gs::NamedProperty<int64_t>("creationDate"));
    auto get_v_opt3 = gs::make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{3}, std::move(expr3));
    auto ctx_right4 = Engine::template GetV<0, -1>(
        time_stamp, graph, std::move(ctx_right3), std::move(get_v_opt3));

    auto edge_expand_opt6 =
        gs::make_edge_expand_opt(gs::Direction::Out, 1, 7);  // has tag, tag
    auto ctx_right5 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx_right4), std::move(edge_expand_opt6));

    // group count
    gs::AliasTagProp<1, 0, grape::EmptyType> group_key(
        gs::PropNameArray<grape::EmptyType>{"None"});
    auto agg0 =
        gs::make_aggregate_prop<1, gs::AggFunc::COUNT, grape::EmptyType>(
            gs::PropNameArray<grape::EmptyType>{"None"},
            std::integer_sequence<int32_t, 0>{});
    auto group_opt = gs::make_group_opt(std::move(group_key), std::move(agg0));
    auto ctx_right6 = Engine::GroupBy(time_stamp, graph, std::move(ctx_right5),
                                      std::move(group_opt));

    auto ctx_anti_joined = Engine::template Join<0, 0, JoinKind::AntiJoin>(
        std::move(ctx_right6), std::move(ctx4));

    gs::OrderingPropPair<gs::SortOrder::DESC, 1, size_t> pair0(
        "None");  // indicate the set's element itself.
    gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view> pair1(
        "name");  // name
    // // IC4SortComparator sorter;
    auto pairs =
        gs::make_sort_opt(gs::Range(0, 10), std::move(pair0), std::move(pair1));
    auto ctx8 = Engine::Sort(time_stamp, graph, std::move(ctx_anti_joined),
                             std::move(pairs));

    gs::AliasTagProp<0, 0, std::string_view> prop_col0({"name"});
    gs::ProjectSelf<1, 1> prop_col1;
    auto proj_opt =
        gs::make_project_opt(std::move(prop_col0), std::move(prop_col1));
    auto ctx9 = Engine::template Project<false>(
        time_stamp, graph, std::move(ctx8), std::move(proj_opt));

    return Engine::Sink(ctx9, std::array<int32_t, 2>{0, 1});
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query4<gs::GrapeGraphInterface>* app =
        new gs::Query4<gs::GrapeGraphInterface>();
    return static_cast<void*>(app);
  } else {
    gs::Query4<gs::GrockGraphInterface>* app =
        new gs::Query4<gs::GrockGraphInterface>();
    return static_cast<void*>(app);
  }
}
void DeleteApp(void* app, gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query4<gs::GrapeGraphInterface>* casted =
        static_cast<gs::Query4<gs::GrapeGraphInterface>*>(app);
    delete casted;
  } else {
    gs::Query4<gs::GrockGraphInterface>* casted =
        static_cast<gs::Query4<gs::GrockGraphInterface>*>(app);
    delete casted;
  }
}
}
