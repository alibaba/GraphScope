
#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/engines/hqps/engine/sync_engine.h"

namespace gs {

using Engine = SyncEngine<GrapeGraphInterface>;
using label_id_t = typename GrapeGraphInterface::label_id_t;
using vertex_id_t = typename GrapeGraphInterface::vertex_id_t;
template <typename TAG_PROP_0>
struct Query0expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr0(int64_t person_id, TAG_PROP_0&& prop_0)
      : person_id_(person_id), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == person_id_);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
  int64_t person_id_;
};
template <typename TAG_PROP_0>
struct Query0expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0expr1(int64_t max_date, TAG_PROP_0&& prop_0)
      : max_date_(max_date), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const {
    return var0 < max_date_;
  }  // 1354060800000
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  TAG_PROP_0 prop_0_;
  int64_t max_date_;
};

class Query0 : public HqpsAppBase<GrapeGraphInterface> {
 public:
  results::CollectiveResults Query(const GrapeGraphInterface& graph,
                                   int64_t time_stamp,
                                   Decoder& decoder) const override {
    CHECK(decoder.size() == 16);
    int64_t person_id = decoder.get_long();
    int64_t max_date = decoder.get_long();
    return Query(graph, time_stamp, person_id, max_date);
  }

  results::CollectiveResults Query(const GrapeGraphInterface& graph,
                                   int64_t time_stamp, int64_t person_id,
                                   int64_t max_date) const {
    auto expr0 = Query0expr0(person_id, gs::NamedProperty<int64_t>("id"));
    auto ctx0 =
        Engine::template ScanVertex<0>(time_stamp, graph, 1, std::move(expr0));
    auto edge_expand_opt0 = gs::make_edge_expand_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto ctx1 = Engine::template EdgeExpandV<1, 0>(
        time_stamp, graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto edge_expand_opt1 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 0, std::array<label_id_t, 2>{3, 2});

    auto ctx2 = Engine::template EdgeExpandVMultiLabel<-1, 1>(
        time_stamp, graph, std::move(ctx1), std::move(edge_expand_opt1));

    Query0expr1 expr1(max_date, gs::NamedProperty<int64_t>("creationDate"));

    auto get_v_opt2 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 2>{2, 3}, std::move(expr1));
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