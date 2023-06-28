#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/ds/expression.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

struct Query2Expr0 : public Predicate<int64_t> {
 public:
  Query2Expr0(const int64_t& person_id) : expect_person_id_(person_id) {}

  inline bool evaluate(const int64_t& person_id) const override {
    return person_id == expect_person_id_;
  }

 private:
  int64_t expect_person_id_;
};

struct Query2Expr1 : public Predicate<int64_t> {
 public:
  Query2Expr1(int64_t maxDate) : maxDate_(maxDate) {}
  inline bool evaluate(const int64_t& var0) const override {
    return var0 < maxDate_;
  }

 private:
  int64_t maxDate_;
};

template <typename GRAPH_INTERFACE>
class Query2 : public HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp, int64_t personIdQ2,
                                   int64_t maxDate) const {
    Query2Expr0 expr0(personIdQ2);
    auto ctx0 = Engine::template ScanVertexV2(
        time_stamp,        // time_stamp
        graph,             // input_graph
        1,                 // label_id, i.e. person_label_id
        std::move(expr0),  // expression
        std::make_tuple(
            gs::PropertySelector<int64_t>("id"))  // property selector
    );

    auto edge_expand_opt0 = gs::make_edge_expand_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto ctx1 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 0, std::array<label_id_t, 2>{3, 2});

    auto ctx2 = Engine::template EdgeExpandVMultiLabel<-1, 1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt1));

    // another example
    Query2Expr1 expr1(maxDate);
    auto get_v_opt2 = make_getv_opt(
        gs::VOpt::Itself,                 // GetVOpt
        std::array<label_id_t, 2>{2, 3},  // post_label_id , comment_label_id
        std::move(expr1),                 // expression
        std::make_tuple(gs::PropertySelector<int64_t, 1>("creationDate"),
                        gs::PropertySelector<int64_t, 0>(
                            "creationDate"))  // property selector
    );
    auto ctx3 = Engine::template GetV<2, -1>(time_stamp, graph, std::move(ctx2),
                                             std::move(get_v_opt2));

    auto project_opt3 = gs::make_project_opt(
        gs::AliasTagProp<1, 0, int64_t>({"id"}),
        gs::AliasTagProp<1, 1, std::string_view>({"firstName"}),
        gs::AliasTagProp<1, 2, std::string_view>({"lastName"}),
        gs::AliasTagProp<2, 3, int64_t>({"id"}),
        gs::AliasTagProp<2, 4, std::string_view>({"content"}),
        gs::AliasTagProp<2, 5, std::string_view>({"imageFile"}),
        gs::AliasTagProp<2, 6, int64_t>({"creationDate"}));

    auto ctx4 = Engine::template Project<0>(time_stamp, graph, std::move(ctx3),
                                            std::move(project_opt3));

    auto sort_opt4 = gs::make_sort_opt(
        gs::Range(0, 20),
        gs::OrderingPropPair<gs::SortOrder::DESC, 6, int64_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 3, int64_t>("None"));

    auto ctx5 =
        Engine::Sort(time_stamp, graph, std::move(ctx4), std::move(sort_opt4));

    return Engine::Sink(ctx5, std::array<int32_t, 7>{3, 4, 5, 6, 7, 8, 9});
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& decoder) const override {
    int64_t personIdQ2 = decoder.get_long();
    int64_t maxDate = decoder.get_long();
    return Query(graph, time_stamp, personIdQ2, maxDate);
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query2<gs::GrapeGraphInterface>* app =
        new gs::Query2<gs::GrapeGraphInterface>();
    return static_cast<void*>(app);
  } else {
    return nullptr;
  }
}
void DeleteApp(void* app, gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query2<gs::GrapeGraphInterface>* casted =
        static_cast<gs::Query2<gs::GrapeGraphInterface>*>(app);
    delete casted;
  } else {
    return;
  }
}
}
