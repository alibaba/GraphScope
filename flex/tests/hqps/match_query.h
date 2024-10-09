
/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef TESTS_HQPS_MATCH_QUERY_H_
#define TESTS_HQPS_MATCH_QUERY_H_

#include "flex/engines/hqps_db/app/interactive_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/utils/app_utils.h"

namespace gs {

struct Query0expr0 {
 public:
  using result_t = bool;
  Query0expr0() {}

  inline auto operator()() const { return true; }

 private:
};

struct Query5expr0 {
 public:
  using result_t = bool;
  Query5expr0() {}

  inline auto operator()(int64_t var0) const { return var0 == 6597069767117; }

 private:
};

struct Query5expr1 {
 public:
  using result_t = bool;
  Query5expr1() {}

  inline auto operator()() const { return true; }

 private:
};

class MatchQuery : public ReadAppBase {
 public:
  using GRAPH_INTERFACE = gs::MutableCSRInterface;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

 public:
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func

    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }

  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    using Engine = SyncEngine<GRAPH_INTERFACE>;

    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, std::array<label_id_t, 1>{1},
        Filter<TruePredicate>());  // person

    // auto ctx0_1 = Engine::template Limit(std::move(ctx0), 0, 100);

    // Both means we need both in and out.
    auto opt = gs::make_edge_expand_multie_opt<
        label_id_t, std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>>(
        Direction::Out,
        std::array<std::array<label_id_t, 3>, 2>{
            std::array<label_id_t, 3>{4, 3, 3},
            std::array<label_id_t, 3>{1, 1, 8}},
        std::tuple{
            PropTupleArrayT<std::tuple<grape::EmptyType>>{},
            PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
        });
    auto ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(opt));

    auto get_v_opt = gs::make_getv_opt(VOpt::End, std::array<label_id_t, 1>{1},
                                       Filter<TruePredicate>());

    auto ctx2 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
        graph, std::move(ctx1), std::move(get_v_opt));
    LOG(INFO) << ctx2.GetHead().Size();

    auto ctx3 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx2),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       PropertySelector<int64_t>("creationDate"))});
    for (auto iter : ctx3) {
      auto ele = iter.GetAllElement();
      VLOG(10) << gs::to_string(ele);
    }

    return Engine::Sink(graph, ctx3, std::array<int32_t, 2>{0, 1});
  }
};

class MatchQuery1 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery1() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, std::array<label_id_t, 8>{0, 1, 2, 3, 4, 5, 6, 7},
        Filter<TruePredicate>());

    auto ctx1 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx0),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
            gs::PropertySelector<grape::EmptyType>(""))});

    auto ctx2 = Engine::Sort(
        graph, std::move(ctx1), gs::Range(0, 10),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("id")});

    for (auto iter : ctx2) {
      VLOG(10) << "ctx0" << gs::to_string(iter.GetAllElement());
    }

    auto ctx3 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx2),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
            gs::PropertySelector<int64_t>("id"))});
    for (auto iter : ctx3) {
      VLOG(10) << gs::to_string(iter.GetAllElement());
    }

    return Engine::Sink(graph, ctx3, std::array<int32_t, 1>{0});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func

    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

class MatchQuery2 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery2() {}

  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto expr0 = gs::make_filter(Query0expr0());
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, std::array<label_id_t, 7>{1, 2, 3, 4, 5, 6, 7},
        std::move(expr0));

    auto ctx0_1 = Engine::template Limit(std::move(ctx0), 0, 100);

    auto edge_expand_opt0 = gs::make_edge_expand_multie_opt<
        label_id_t, std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>>(
        gs::Direction::Out,
        std::array<std::array<label_id_t, 3>, 2>{
            std::array<label_id_t, 3>{4, 3, 3},
            std::array<label_id_t, 3>{1, 1, 8}},
        std::tuple{
            PropTupleArrayT<std::tuple<grape::EmptyType>>{},
            PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
        });
    auto ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0_1), std::move(edge_expand_opt0));

    auto get_v_opt1 = make_getv_opt(
        gs::VOpt::End,
        std::array<label_id_t, 8>{
            (label_id_t) 0, (label_id_t) 1, (label_id_t) 2, (label_id_t) 3,
            (label_id_t) 4, (label_id_t) 5, (label_id_t) 6, (label_id_t) 7});
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));
    auto agg_func2 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 1>{});

    auto ctx3 = Engine::GroupByWithoutKey(graph, std::move(ctx2),
                                          std::tuple{std::move(agg_func2)});
    for (auto iter : ctx3) {
      auto ele = iter.GetAllElement();
      LOG(INFO) << gs::to_string(ele);
    }
    return Engine::Sink(graph, ctx3, std::array<int32_t, 1>{3});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func

    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

class MatchQuery3 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;

  MatchQuery3() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, std::array<label_id_t, 7>{1, 2, 3, 4, 5, 6, 7},
        Filter<TruePredicate>());

    auto ctx0_1 = Engine::template Limit(std::move(ctx0), 0, 100);

    auto edge_expand_opt0 = gs::make_edge_expand_multie_opt<
        label_id_t, std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>>(
        gs::Direction::Out,
        std::array<std::array<label_id_t, 3>, 2>{
            std::array<label_id_t, 3>{4, 3, 3},
            std::array<label_id_t, 3>{1, 1, 8}},
        std::tuple{
            PropTupleArrayT<std::tuple<grape::EmptyType>>{},
            PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
        });
    auto ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0_1), std::move(edge_expand_opt0));

    auto ctx1_1 = Engine::template Limit(std::move(ctx1), 0, 100);

    auto edge_expand_opt1 = gs::make_edge_expand_multie_opt<
        label_id_t, std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>>(
        gs::Direction::Out,
        std::array<std::array<label_id_t, 3>, 2>{
            std::array<label_id_t, 3>{4, 3, 3},
            std::array<label_id_t, 3>{1, 1, 8}},
        std::tuple{
            PropTupleArrayT<std::tuple<grape::EmptyType>>{},
            PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
        });
    auto ctx2 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(1)>(
            graph, std::move(ctx1_1), std::move(edge_expand_opt1));

    auto get_v_opt2 =
        make_getv_opt(gs::VOpt::Other, std::array<label_id_t, 0>{});
    auto ctx3 = Engine::template GetV<gs::AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx2), std::move(get_v_opt2));
    auto get_v_opt3 =
        make_getv_opt(gs::VOpt::Itself, std::array<label_id_t, 0>{});
    auto ctx4 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx3), std::move(get_v_opt3));
    auto agg_func4 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 2>{});

    auto ctx6 = Engine::GroupByWithoutKey(graph, std::move(ctx4),
                                          std::tuple{std::move(agg_func4)});
    return Engine::Sink(graph, ctx6, std::array<int32_t, 1>{2});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

// Query4
class MatchQuery4 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery4() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto expr0 = gs::make_filter(Query0expr0());
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 0, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expand_multie_opt<
        label_id_t, std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<int64_t>, std::tuple<int64_t>,
        std::tuple<int64_t>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<int32_t>, std::tuple<int64_t>,
        std::tuple<int32_t>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>>(
        gs::Direction::Out,
        std::array<std::array<label_id_t, 3>, 21>{
            std::array<label_id_t, 3>{2, 2, 2},
            std::array<label_id_t, 3>{2, 3, 2},
            std::array<label_id_t, 3>{1, 7, 6},
            std::array<label_id_t, 3>{6, 6, 13},
            std::array<label_id_t, 3>{4, 3, 3},
            std::array<label_id_t, 3>{2, 0, 7},
            std::array<label_id_t, 3>{1, 0, 7},
            std::array<label_id_t, 3>{3, 0, 7},
            std::array<label_id_t, 3>{5, 0, 7},
            std::array<label_id_t, 3>{1, 1, 8},
            std::array<label_id_t, 3>{1, 2, 9},
            std::array<label_id_t, 3>{1, 3, 9},
            std::array<label_id_t, 3>{0, 0, 11},
            std::array<label_id_t, 3>{7, 6, 12},
            std::array<label_id_t, 3>{2, 1, 0},
            std::array<label_id_t, 3>{3, 1, 0},
            std::array<label_id_t, 3>{1, 5, 10},
            std::array<label_id_t, 3>{4, 1, 4},
            std::array<label_id_t, 3>{1, 5, 14},
            std::array<label_id_t, 3>{3, 7, 1},
            std::array<label_id_t, 3>{4, 1, 5}},
        std::tuple{PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
                   PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
                   PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<int32_t>>{"workFrom"},
                   PropTupleArrayT<std::tuple<int64_t>>{"joinDate"},
                   PropTupleArrayT<std::tuple<int32_t>>{"classYear"},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{}});
    auto ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto get_v_opt1 = make_getv_opt(
        gs::VOpt::End,
        std::array<label_id_t, 8>{
            (label_id_t) 0, (label_id_t) 1, (label_id_t) 2, (label_id_t) 3,
            (label_id_t) 4, (label_id_t) 5, (label_id_t) 6, (label_id_t) 7});
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));
    auto agg_func2 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 1>{});

    auto ctx3 = Engine::GroupByWithoutKey(graph, std::move(ctx2),
                                          std::tuple{std::move(agg_func2)});
    return Engine::Sink(graph, ctx3, std::array<int32_t, 1>{3});
  }

  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};
class MatchQuery5 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery5() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto expr0 =
        gs::make_filter(Query5expr0(), gs::PropertySelector<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);
    auto ctx1 =
        Engine::template EdgeExpandV<gs::AppendOpt::Temp, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));
    for (auto iter : ctx1) {
      VLOG(10) << "ctx1: " << gs::to_string(iter.GetAllElement());
    }

    auto expr2 = gs::make_filter(Query5expr1());
    auto get_v_opt1 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr2));
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));

    for (auto iter : ctx2) {
      VLOG(10) << "ctx2: " << gs::to_string(iter.GetAllElement());
    }

    auto agg_func2 = gs::make_aggregate_prop<gs::AggFunc::COUNT>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 0>{});

    auto ctx3 = Engine::GroupByWithoutKey(graph, std::move(ctx2),
                                          std::tuple{std::move(agg_func2)});
    for (auto iter : ctx3) {
      VLOG(10) << "ctx3: " << gs::to_string(iter.GetAllElement());
    }
    return Engine::Sink(graph, ctx3, std::array<int32_t, 1>{2});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

class MatchQuery7 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery7() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, Filter<TruePredicate>());

    auto edge_expand_opt0 = gs::make_edge_expande_opt<int32_t>(
        gs::PropNameArray<int32_t>{"classYear"}, gs::Direction::Out,
        (label_id_t) 14, (label_id_t) 5);
    auto ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto get_v_opt1 =
        make_getv_opt(gs::VOpt::Other, std::array<label_id_t, 0>{});
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));
    auto expr1 = gs::make_filter(Query0expr0());
    auto get_v_opt2 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr1));
    auto ctx3 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx2), std::move(get_v_opt2));
    auto ctx4 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx3),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
            gs::PropertySelector<int32_t>("classYear"))});
    auto ctx5 = Engine::Sort(
        graph, std::move(ctx4), gs::Range(0, 10),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::DESC, 0, int32_t>("")});
    return Engine::Sink(graph, ctx5, std::array<int32_t, 1>{3});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

class MatchQuery9 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery9() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto expr0 = gs::make_filter(Query0expr0());
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 4, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expande_opt<grape::EmptyType>(
        gs::PropNameArray<grape::EmptyType>{""}, gs::Direction::Out,
        (label_id_t) 3, (label_id_t) 3);
    auto ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto get_v_opt1 = make_getv_opt(
        gs::VOpt::End,
        std::array<label_id_t, 8>{
            (label_id_t) 0, (label_id_t) 1, (label_id_t) 2, (label_id_t) 3,
            (label_id_t) 4, (label_id_t) 5, (label_id_t) 6, (label_id_t) 7});
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));
    auto ctx3 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx2),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
            gs::PropertySelector<int64_t>("id"))});
    auto ctx4 = Engine::Sort(
        graph, std::move(ctx3), gs::Range(0, 10),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("")});
    return Engine::Sink(graph, ctx4, std::array<int32_t, 1>{3});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

// test path expand

struct Query10expr0 {
 public:
  using result_t = bool;
  Query10expr0() {}

  inline auto operator()(std::string_view var0, int64_t var1) const {
    return var0 == "Ian" && var1 != 30786325579101;
  }

 private:
};

struct Query10expr1 {
 public:
  using result_t = bool;
  Query10expr1() {}

  inline auto operator()(int64_t var0) const {
    return (true) && (var0 == 30786325579101);
  }

 private:
};
class MatchQuery10 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery10() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto expr0 = gs::make_filter(
        Query10expr0(), gs::PropertySelector<std::string_view>("firstName"),
        gs::PropertySelector<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, std::move(expr0));

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Both, (label_id_t) 8, (label_id_t) 1);
    auto get_v_opt0 = make_getv_opt(gs::VOpt::Itself,
                                    std::array<label_id_t, 1>{(label_id_t) 1});
    auto path_opt2 = gs::make_path_expandv_opt(
        std::move(edge_expand_opt1), std::move(get_v_opt0), gs::Range(1, 4));
    auto ctx1 = Engine::PathExpandP<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
        graph, std::move(ctx0), std::move(path_opt2));

    auto get_v_opt3 = make_getv_opt(gs::VOpt::End, std::array<label_id_t, 0>{});
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt3));

    auto expr2 =
        gs::make_filter(Query10expr1(), gs::PropertySelector<int64_t>("id"));
    auto get_v_opt4 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr2));
    auto ctx3 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx2), std::move(get_v_opt4));
    auto ctx4 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx3),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<LengthKey>("length"))});

    GroupKey<0, grape::EmptyType> group_key5(
        gs::PropertySelector<grape::EmptyType>("None"));

    auto agg_func6 = gs::make_aggregate_prop<gs::AggFunc::MIN>(
        std::tuple{gs::PropertySelector<grape::EmptyType>("None")},
        std::integer_sequence<int32_t, 1>{});

    auto ctx5 = Engine::GroupBy(graph, std::move(ctx4), std::tuple{group_key5},
                                std::tuple{agg_func6});
    for (auto iter : ctx5) {
      VLOG(10) << "after groupby: " << gs::to_string(iter.GetAllElement());
    }
    auto ctx6 = Engine::Sort(
        graph, std::move(ctx5), gs::Range(0, 20),
        std::tuple{
            gs::OrderingPropPair<gs::SortOrder::ASC, 1, int32_t>(""),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, std::string_view>(
                "lastName"),
            gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>("id")});
    auto ctx7 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx6),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("firstName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<std::string_view>("lastName")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id"))});
    return Engine::Sink(graph, ctx7, std::array<int32_t, 4>{4, 5, 6, 7});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

struct MatchQuery11Expr0 {
 public:
  using result_t = bool;
  MatchQuery11Expr0() {}

  inline auto operator()(int64_t id) const { return (true) && (id == 933); }

 private:
};
struct MatchQuery11Expr1 {
 public:
  using result_t = bool;
  MatchQuery11Expr1() {}

  inline auto operator()(int64_t id) const {
    return (true) && (id == 2199023256077);
  }

 private:
};

class MatchQuery11 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery11() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto expr0 = gs::make_filter(MatchQuery11Expr0(),
                                 gs::PropertySelector<int64_t>("id"));
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, std::array<label_id_t, 8>{0, 1, 2, 3, 4, 5, 6, 7},
        std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expand_multie_opt<
        label_id_t, std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<int64_t>, std::tuple<int64_t>,
        std::tuple<int64_t>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<int32_t>, std::tuple<int64_t>,
        std::tuple<int32_t>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>>(
        gs::Direction::Both,
        std::array<std::array<label_id_t, 3>, 21>{
            std::array<label_id_t, 3>{2, 2, 2},
            std::array<label_id_t, 3>{2, 3, 2},
            std::array<label_id_t, 3>{1, 7, 6},
            std::array<label_id_t, 3>{6, 6, 13},
            std::array<label_id_t, 3>{4, 3, 3},
            std::array<label_id_t, 3>{2, 0, 7},
            std::array<label_id_t, 3>{1, 0, 7},
            std::array<label_id_t, 3>{3, 0, 7},
            std::array<label_id_t, 3>{5, 0, 7},
            std::array<label_id_t, 3>{1, 1, 8},
            std::array<label_id_t, 3>{1, 2, 9},
            std::array<label_id_t, 3>{1, 3, 9},
            std::array<label_id_t, 3>{0, 0, 11},
            std::array<label_id_t, 3>{7, 6, 12},
            std::array<label_id_t, 3>{2, 1, 0},
            std::array<label_id_t, 3>{3, 1, 0},
            std::array<label_id_t, 3>{1, 5, 10},
            std::array<label_id_t, 3>{4, 1, 4},
            std::array<label_id_t, 3>{1, 5, 14},
            std::array<label_id_t, 3>{3, 7, 1},
            std::array<label_id_t, 3>{4, 1, 5}},
        std::tuple{PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
                   PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
                   PropTupleArrayT<std::tuple<int64_t>>{"creationDate"},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<int32_t>>{"workFrom"},
                   PropTupleArrayT<std::tuple<int64_t>>{"joinDate"},
                   PropTupleArrayT<std::tuple<int32_t>>{"classYear"},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{}});
    auto ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto get_v_opt1 =
        make_getv_opt(gs::VOpt::Other, std::array<label_id_t, 0>{});
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Temp, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));
    auto expr1 = gs::make_filter(MatchQuery11Expr1(),
                                 gs::PropertySelector<int64_t>("id"));
    auto get_v_opt2 = make_getv_opt(
        gs::VOpt::Itself, std::array<label_id_t, 0>{}, std::move(expr1));
    auto ctx3 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx2), std::move(get_v_opt2));
    auto ctx4 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx3),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<LabelKey>("label")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<LabelKey>("label"))});
    return Engine::Sink(graph, ctx4, std::array<int32_t, 2>{0, 1});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

class MatchQuery12 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery12() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, Filter<TruePredicate>());

    auto ctx1 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx0),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
            gs::PropertySelector<grape::EmptyType>(""))});
    auto ctx2 = Engine::Limit(std::move(ctx1), 0, 5);
    auto res = Engine::Sink(graph, ctx2, std::array<int32_t, 1>{0});
    LOG(INFO) << "res: " << res.DebugString();
    return res;
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

struct MatchQuery13Expr0 {
 public:
  using result_t = bool;
  MatchQuery13Expr0() {}

  inline auto operator()(Date var0) const {
    return gs::DateTimeExtractor<Interval::MONTH>::extract(var0) == 7;
  }

 private:
};

struct MatchQuery13Expr1 {
 public:
  using result_t = int64_t;
  MatchQuery13Expr1() {}

  inline auto operator()(Date var1) const {
    return gs::DateTimeExtractor<Interval::MONTH>::extract(var1);
  }

 private:
};

class MatchQuery13 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery13() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, Filter<TruePredicate>());

    auto ctx1 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx0),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
            gs::PropertySelector<Date>("birthday"))});
    auto expr0 = gs::make_filter(MatchQuery13Expr0(),
                                 gs::PropertySelector<Date>("None"));
    auto ctx2 = Engine::template Select<INPUT_COL_ID(0)>(graph, std::move(ctx1),
                                                         std::move(expr0));

    auto ctx3 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx2),
        std::tuple{gs::make_mapper_with_expr<0>(
            MatchQuery13Expr1(), gs::PropertySelector<Date>("None"))});
    return Engine::Sink(graph, ctx3, std::array<int32_t, 1>{2});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

// Auto generated query class definition
class MatchQuery14 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery14() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto expr0 = gs::make_filter(Query0expr0());
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 2, std::move(expr0));

    auto edge_expand_opt1 = gs::make_edge_expandv_opt(
        gs::Direction::Out, (label_id_t) 2,
        std::array<label_id_t, 2>{(label_id_t) 2, (label_id_t) 3});

    auto get_v_opt0 = make_getv_opt(
        gs::VOpt::Itself,
        std::array<label_id_t, 2>{(label_id_t) 2, (label_id_t) 3});

    auto path_opt2 = gs::make_path_expandv_opt(
        std::move(edge_expand_opt1), std::move(get_v_opt0), gs::Range(0, 3));
    auto ctx1 = Engine::PathExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
        graph, std::move(ctx0), std::move(path_opt2));
    auto ctx2 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx1),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<int64_t>("id"))});
    return Engine::Sink(graph, ctx2, std::array<int32_t, 2>{2, 3});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

class MatchQuery15 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  MatchQuery15() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto expr0 = gs::make_filter(Query0expr0());
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, std::array<label_id_t, 8>{0, 1, 2, 3, 4, 5, 6, 7},
        std::move(expr0));

    auto edge_expand_opt1 = gs::make_edge_expand_multiv_opt(
        gs::Direction::Out, std::vector<std::array<label_id_t, 3>>{
                                std::array<label_id_t, 3>{2, 2, 2},
                                std::array<label_id_t, 3>{2, 3, 2},
                                std::array<label_id_t, 3>{1, 7, 6},
                                std::array<label_id_t, 3>{6, 6, 13},
                                std::array<label_id_t, 3>{4, 3, 3},
                                std::array<label_id_t, 3>{2, 0, 7},
                                std::array<label_id_t, 3>{1, 0, 7},
                                std::array<label_id_t, 3>{3, 0, 7},
                                std::array<label_id_t, 3>{5, 0, 7},
                                std::array<label_id_t, 3>{1, 1, 8},
                                std::array<label_id_t, 3>{1, 2, 9},
                                std::array<label_id_t, 3>{1, 3, 9},
                                std::array<label_id_t, 3>{0, 0, 11},
                                std::array<label_id_t, 3>{7, 6, 12},
                                std::array<label_id_t, 3>{2, 1, 0},
                                std::array<label_id_t, 3>{3, 1, 0},
                                std::array<label_id_t, 3>{1, 5, 10},
                                std::array<label_id_t, 3>{4, 1, 4},
                                std::array<label_id_t, 3>{1, 5, 14},
                                std::array<label_id_t, 3>{3, 7, 1},
                                std::array<label_id_t, 3>{4, 1, 5}});

    auto get_v_opt0 = make_getv_opt(
        gs::VOpt::Itself,
        std::array<label_id_t, 21>{
            (label_id_t) 2, (label_id_t) 3, (label_id_t) 7, (label_id_t) 6,
            (label_id_t) 3, (label_id_t) 0, (label_id_t) 0, (label_id_t) 0,
            (label_id_t) 0, (label_id_t) 1, (label_id_t) 2, (label_id_t) 3,
            (label_id_t) 0, (label_id_t) 6, (label_id_t) 1, (label_id_t) 1,
            (label_id_t) 5, (label_id_t) 1, (label_id_t) 5, (label_id_t) 7,
            (label_id_t) 1});

    auto path_opt2 = gs::make_path_expandv_opt(
        std::move(edge_expand_opt1), std::move(get_v_opt0), gs::Range(0, 2));
    auto ctx1 = Engine::PathExpandV<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
        graph, std::move(ctx0), std::move(path_opt2));
    auto ctx2 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx1),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<int64_t>("id")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<int64_t>("id"))});
    auto ctx3 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx2),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    auto ctx4 = Engine::Sort(
        graph, std::move(ctx3), gs::Range(0, 10),
        std::tuple{gs::OrderingPropPair<gs::SortOrder::ASC, 0, int64_t>(""),
                   gs::OrderingPropPair<gs::SortOrder::ASC, 1, int64_t>("")});
    return Engine::Sink(graph, ctx4, std::array<int32_t, 2>{2, 3});
  }
  // Wrapper query function for query class
  bool Query(const GraphDBSession& graph, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func
    gs::MutableCSRInterface interface(graph);
    auto res = Query(interface);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
};

// Auto generated expression class definition
struct MatchQuery16expr0 {
 public:
  using result_t = bool;
  static constexpr bool filter_null = true;
  MatchQuery16expr0() {}

  inline auto operator()(LabelKey label) const {
    return (label<WithIn> std::array<int64_t, 2>{0, 2});
  }

 private:
};

// Auto generated query class definition
class MatchQuery16 : public ReadAppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  using gid_t = typename gs::MutableCSRInterface::gid_t;
  // constructor
  MatchQuery16() {}
  // Query function for query class
  results::CollectiveResults Query(gs::MutableCSRInterface& graph) const {
    auto expr0 = gs::make_filter(MatchQuery16expr0(),
                                 gs::PropertySelector<LabelKey>("label"));
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, std::array<label_id_t, 2>{0, 2}, std::move(expr0));

    auto edge_expand_opt0 = gs::make_edge_expand_multie_opt<
        label_id_t, std::tuple<grape::EmptyType>, std::tuple<double>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>,
        std::tuple<grape::EmptyType>, std::tuple<grape::EmptyType>>(
        gs::Direction::Out,
        std::array<std::array<label_id_t, 3>, 6>{
            std::array<label_id_t, 3>{0, 1, 0},
            std::array<label_id_t, 3>{2, 1, 5},
            std::array<label_id_t, 3>{0, 1, 2},
            std::array<label_id_t, 3>{0, 1, 3},
            std::array<label_id_t, 3>{2, 0, 4},
            std::array<label_id_t, 3>{0, 1, 1}},
        std::tuple{PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<double>>{"rating"},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{},
                   PropTupleArrayT<std::tuple<grape::EmptyType>>{}});
    auto ctx1 =
        Engine::template EdgeExpandE<gs::AppendOpt::Persist, INPUT_COL_ID(0)>(
            graph, std::move(ctx0), std::move(edge_expand_opt0));

    auto get_v_opt1 = make_getv_opt(
        gs::VOpt::End,
        std::array<label_id_t, 2>{(label_id_t) 0, (label_id_t) 1});
    auto ctx2 = Engine::template GetV<gs::AppendOpt::Persist, INPUT_COL_ID(-1)>(
        graph, std::move(ctx1), std::move(get_v_opt1));
    auto ctx3 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx2),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
                       gs::PropertySelector<grape::EmptyType>("")),
                   gs::make_mapper_with_variable<INPUT_COL_ID(2)>(
                       gs::PropertySelector<grape::EmptyType>(""))});
    return Engine::Sink(graph, ctx3, std::array<int32_t, 3>{0, 1, 2});
  }

  // Wrapper query function for query class
  bool Query(const GraphDBSession& sess, Decoder& decoder,
             Encoder& encoder) override {
    // decoding params from decoder, and call real query func

    gs::MutableCSRInterface graph(sess);
    auto res = Query(graph);
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    if (!res_str.empty()) {
      encoder.put_string_view(res_str);
    }
    return true;
  }
  // private members
 private:
};

}  // namespace gs
#endif  // TESTS_HQPS_MATCH_QUERY_H_