

#include "flex/engines/hqps/engine/sync_engine.h"
#include "flex/engines/hqps/app/cypher_app_base.h"
#include "flex/storages/mutable_csr/grape_graph_interface.h"

namespace gs {

template <typename TAG_COL0, typename TAG_COL1>
class IC3Expression4 {
 public:
  using tag_prop_t = std::tuple<TAG_COL0, TAG_COL1>;
  using result_t = uint64_t;
  IC3Expression4(TAG_COL0&& prop0, TAG_COL1&& prop1)
      : prop0_(std::move(prop0)), prop1_(std::move(prop1)) {}

  uint64_t operator()(uint64_t a, uint64_t b) const { return a + b; }

  tag_prop_t Properties() { return std::make_tuple(prop0_, prop1_); }

 private:
  TAG_COL0 prop0_;
  TAG_COL1 prop1_;
};

template <typename TAG_PROP_0>
struct Query0left_left_expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0left_left_expr0(std::string_view countryX, TAG_PROP_0&& prop_0)
      : countryX_(countryX), prop_0_(std::move(prop_0)) {}
  inline auto operator()(std::string_view var0) const {
    return var0 == countryX_;
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  std::string_view countryX_;
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0, typename TAG_PROP_1>
struct Query0left_left_expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query0left_left_expr1(int64_t startDate, int64_t endDate, TAG_PROP_0&& prop_0,
                        TAG_PROP_1&& prop_1)
      : startDate_(startDate),
        endDate_(endDate),
        prop_0_(std::move(prop_0)),
        prop_1_(std::move(prop_1)) {}
  inline auto operator()(int64_t var0, int64_t var1) const {
    return (var0 >= startDate_) && (var1 < endDate_);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  int64_t startDate_;
  int64_t endDate_;
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};
template <typename TAG_PROP_0>
struct Query0left_right_expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0left_right_expr0(std::string_view countryY, TAG_PROP_0&& prop_0)
      : countryY_(countryY), prop_0_(std::move(prop_0)) {}
  inline auto operator()(std::string_view var0) const {
    return var0 == countryY_;
  }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  std::string_view countryY_;
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0, typename TAG_PROP_1>
struct Query0left_right_expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query0left_right_expr1(int64_t startDate, int64_t endDate,
                         TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
      : startDate_(startDate),
        endDate_(endDate),
        prop_0_(std::move(prop_0)),
        prop_1_(std::move(prop_1)) {}
  inline auto operator()(int64_t var0, int64_t var1) const {
    return (var0 >= startDate_) && (var1 < endDate_);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  int64_t startDate_;
  int64_t endDate_;
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};
template <typename TAG_PROP_0>
struct Query0right_expr0 {
  using tag_prop_t = std::tuple<TAG_PROP_0>;
  Query0right_expr0(int64_t personId, TAG_PROP_0&& prop_0)
      : personId_(personId), prop_0_(std::move(prop_0)) {}
  inline auto operator()(int64_t var0) const { return var0 == personId_; }
  inline auto Properties() const { return std::make_tuple(prop_0_); }

 private:
  int64_t personId_;
  TAG_PROP_0 prop_0_;
};
template <typename TAG_PROP_0, typename TAG_PROP_1>
struct Query0right_expr1 {
  using tag_prop_t = std::tuple<TAG_PROP_0, TAG_PROP_1>;
  Query0right_expr1(std::string_view countryX, std::string_view countryY,
                    TAG_PROP_0&& prop_0, TAG_PROP_1&& prop_1)
      : countryX_(countryX),
        countryY_(countryY),
        prop_0_(std::move(prop_0)),
        prop_1_(std::move(prop_1)) {}
  inline auto operator()(std::string_view var0, std::string_view var1) const {
    return (var0 != countryX_) && (var1 != countryY_);
  }
  inline auto Properties() const { return std::make_tuple(prop_0_, prop_1_); }

 private:
  std::string_view countryX_;
  std::string_view countryY_;
  TAG_PROP_0 prop_0_;
  TAG_PROP_1 prop_1_;
};

template <typename GRAPH_INTERFACE>
class Query3 : public HqpsAppBase<GRAPH_INTERFACE> {
 public:
  using Engine = SyncEngine<GRAPH_INTERFACE>;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;
  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp,
                                   Decoder& decoder) const override {
    int64_t personId = decoder.get_long();
    std::string_view countryX = decoder.get_string();
    std::string_view countryY = decoder.get_string();
    int64_t startDate = decoder.get_long();
    int64_t duration_days = decoder.get_int();
    int64_t endDate = startDate + duration_days * 86400000;

    return Query(graph, time_stamp, personId, countryX, countryY, startDate,
                 endDate);
  }
  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   int64_t time_stamp, int64_t personId,
                                   std::string_view countryX,
                                   std::string_view countryY, int64_t startDate,
                                   int64_t endDate) const {
    auto left_left_expr0 = Query0left_left_expr0(
        countryX, gs::NamedProperty<std::string_view>("name"));
    auto left_left_ctx0 = Engine::template ScanVertex<0>(
        time_stamp, graph, 0, std::move(left_left_expr0));
    auto left_left_edge_expand_opt0 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 7, std::array<label_id_t, 2>{3, 2});

    auto left_left_ctx1 = Engine::template EdgeExpandVMultiLabel<-1, -1>(
        time_stamp, graph, std::move(left_left_ctx0),
        std::move(left_left_edge_expand_opt0));

    Query0left_left_expr1 left_left_expr1(
        startDate, endDate, gs::NamedProperty<int64_t>("creationDate"),
        gs::NamedProperty<int64_t>("creationDate"));

    auto left_left_get_v_opt1 =
        make_getv_opt(gs::VOpt::Start, std::array<label_id_t, 2>{2, 3},
                      std::move(left_left_expr1));
    auto left_left_ctx2 = Engine::template GetV<1, -1>(
        time_stamp, graph, std::move(left_left_ctx1),
        std::move(left_left_get_v_opt1));
    auto left_left_edge_expand_opt2 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);

    auto left_left_ctx3 = Engine::template EdgeExpandV<2, -1>(
        time_stamp, graph, std::move(left_left_ctx2),
        std::move(left_left_edge_expand_opt2));
    // group count
    gs::AliasTagProp<2, 0, grape::EmptyType> group_key0(
        gs::PropNameArray<grape::EmptyType>{"None"});
    auto agg0 =
        gs::make_aggregate_prop<1, gs::AggFunc::COUNT, grape::EmptyType>(
            gs::PropNameArray<grape::EmptyType>{"None"},
            std::integer_sequence<int32_t, 1>{});
    auto group_opt0 =
        gs::make_group_opt(std::move(group_key0), std::move(agg0));
    auto left_left_ctx4 = Engine::GroupBy(
        time_stamp, graph, std::move(left_left_ctx3), std::move(group_opt0));

    auto left_right_expr0 = Query0left_right_expr0(
        countryY, gs::NamedProperty<std::string_view>("name"));
    auto left_right_ctx0 = Engine::template ScanVertex<0>(
        time_stamp, graph, 0, std::move(left_right_expr0));
    auto left_right_edge_expand_opt0 = gs::make_edge_expand_opt(
        gs::Direction::In, (label_id_t) 7, std::array<label_id_t, 2>{3, 2});

    auto left_right_ctx1 = Engine::template EdgeExpandVMultiLabel<-1, -1>(
        time_stamp, graph, std::move(left_right_ctx0),
        std::move(left_right_edge_expand_opt0));

    Query0left_right_expr1 left_right_expr1(
        startDate, endDate, gs::NamedProperty<int64_t>("creationDate"),
        gs::NamedProperty<int64_t>("creationDate"));

    auto left_right_get_v_opt1 =
        make_getv_opt(gs::VOpt::Start, std::array<label_id_t, 2>{2, 3},
                      std::move(left_right_expr1));
    auto left_right_ctx2 = Engine::template GetV<1, -1>(
        time_stamp, graph, std::move(left_right_ctx1),
        std::move(left_right_get_v_opt1));
    auto left_right_edge_expand_opt2 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 0, (label_id_t) 1);

    auto left_right_ctx3 = Engine::template EdgeExpandV<2, -1>(
        time_stamp, graph, std::move(left_right_ctx2),
        std::move(left_right_edge_expand_opt2));
    // group count
    gs::AliasTagProp<2, 0, grape::EmptyType> group_key1(
        gs::PropNameArray<grape::EmptyType>{"None"});
    auto agg1 =
        gs::make_aggregate_prop<1, gs::AggFunc::COUNT, grape::EmptyType>(
            gs::PropNameArray<grape::EmptyType>{"None"},
            std::integer_sequence<int32_t, 1>{});
    auto group_opt1 =
        gs::make_group_opt(std::move(group_key1), std::move(agg1));
    auto left_right_ctx4 = Engine::GroupBy(
        time_stamp, graph, std::move(left_right_ctx3), std::move(group_opt1));

    auto left_left_ctx5 = Engine::template Join<0, 0, gs::JoinKind::InnerJoin>(
        std::move(left_left_ctx4), std::move(left_right_ctx4));

    auto right_expr0 =
        Query0right_expr0(personId, gs::NamedProperty<int64_t>("id"));
    auto right_ctx0 = Engine::template ScanVertex<-1>(time_stamp, graph, 1,
                                                      std::move(right_expr0));
    auto right_edge_expand_opt1 = gs::make_edge_expand_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);

    auto right_get_v_opt0 =
        make_getv_opt(gs::VOpt::End, std::array<label_id_t, 1>{1});
    auto right_path_opt2 =
        gs::make_path_expand_opt(std::move(right_edge_expand_opt1),
                                 std::move(right_get_v_opt0), gs::Range(1, 3));
    auto right_ctx1 = Engine::template PathExpandV<0, -1>(
        time_stamp, graph, std::move(right_ctx0), std::move(right_path_opt2));

    auto right_edge_expand_opt3 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 7, (label_id_t) 0);

    auto right_ctx2 = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(right_ctx1),
        std::move(right_edge_expand_opt3));

    auto right_edge_expand_opt4 = gs::make_edge_expand_opt(
        gs::Direction::Out, (label_id_t) 11, (label_id_t) 0);

    auto right_ctx3 = Engine::template EdgeExpandV<-1, -1>(
        time_stamp, graph, std::move(right_ctx2),
        std::move(right_edge_expand_opt4));

    Query0right_expr1 right_expr1(countryX, countryY,
                                  gs::NamedProperty<std::string_view>("name"),
                                  gs::NamedProperty<std::string_view>("name"));

    auto right_get_v_opt5 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 1>{0}, std::move(right_expr1));
    auto right_ctx4 = Engine::template GetV<-1, -1>(
        time_stamp, graph, std::move(right_ctx3), std::move(right_get_v_opt5));

    auto left_left_ctx6 = Engine::template Join<0, 0, gs::JoinKind::InnerJoin>(
        std::move(left_left_ctx5), std::move(right_ctx4));
    // return Engine::Sink(left_left_ctx6, std::array<int32_t, 3>{2, 4, 1});

    gs::AliasTagProp<0, 0, gs::oid_t> prop_col0({"id"});
    gs::AliasTagProp<0, 1, std::string_view> prop_col1({"firstName"});
    gs::AliasTagProp<0, 2, std::string_view> prop_col2({"lastName"});
    gs::ProjectSelf<1, 3> prop_col3;
    gs::ProjectSelf<2, 4> prop_col4;
    gs::InnerIdProperty<1> x_count;
    gs::InnerIdProperty<2> y_count;

    IC3Expression4 expr4(std::move(x_count), std::move(y_count));
    auto prop_col5 = make_project_expr<5, size_t>(std::move(expr4));

    auto proj_opt10 = gs::make_project_opt(
        std::move(prop_col0), std::move(prop_col1), std::move(prop_col2),
        std::move(prop_col3), std::move(prop_col4), std::move(prop_col5));
    auto ctx10 = Engine::template Project<false>(
        time_stamp, graph, std::move(left_left_ctx6), std::move(proj_opt10));

    auto sort_opt5 = gs::make_sort_opt(
        gs::Range(0, 20),
        gs::OrderingPropPair<gs::SortOrder::DESC, 5, size_t>("None"),
        gs::OrderingPropPair<gs::SortOrder::ASC, 0, oid_t>("None"));

    auto ctx6 =
        Engine::Sort(time_stamp, graph, std::move(ctx10), std::move(sort_opt5));
    return Engine::Sink(ctx6, std::array<int32_t, 6>{10, 11, 12, 8, 9, 13});
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query3<gs::GrapeGraphInterface>* app =
        new gs::Query3<gs::GrapeGraphInterface>();
    return static_cast<void*>(app);
  } else {
    gs::Query3<gs::GrockGraphInterface>* app =
        new gs::Query3<gs::GrockGraphInterface>();
    return static_cast<void*>(app);
  }
}
void DeleteApp(void* app, gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::Query3<gs::GrapeGraphInterface>* casted =
        static_cast<gs::Query3<gs::GrapeGraphInterface>*>(app);
    delete casted;
  } else {
    gs::Query3<gs::GrockGraphInterface>* casted =
        static_cast<gs::Query3<gs::GrockGraphInterface>*>(app);
    delete casted;
  }
}
}
