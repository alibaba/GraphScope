# C++ Stored Procedure

In addition to expressing your query using Cypher, you can also write stored procedures using our provided C++ SDK. In this article, we introduce the interfaces exposed by the computation engine, which allow you to bypass the compiler and directly write out the physical query plan you desire to execute as a stored procedure.

## Sample Stored Procedure
First, let's give a simple example of a stored procedure

```cpp
#include "flex/engines/hqps_db/app/hqps_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"

namespace gs {

struct Expression1 {
 public:
  using result_t = bool;
  Expression1(oid_t oid) : oid_(oid) {}

  inline bool operator()(oid_t data) const { return oid_ == data; }

 private:
  oid_t oid_;
};

class SampleQuery : public HqpsAppBase<gs::MutableCSRInterface> {
 public:
  using GRAPH_INTERFACE = gs::MutableCSRInterface;
  using oid_t = typename GRAPH_INTERFACE::outer_vertex_id_t;
  using vertex_id_t = typename GRAPH_INTERFACE::vertex_id_t;

 public:
  results::CollectiveResults Query(const GRAPH_INTERFACE& graph,
                                   Decoder& input) const override {
    int64_t id = input.get_long();
    int64_t maxDate = input.get_long();
    return Query(graph, id, maxDate);
  }

  results::CollectiveResults Query(const GRAPH_INTERFACE& graph, int64_t id,
                                   int64_t maxDate) const {
    using label_id_t = typename GRAPH_INTERFACE::label_id_t;
    label_id_t person_label_id = graph.GetVertexLabelId("PERSON");
    label_id_t knows_label_id = graph.GetEdgeLabelId("KNOWS");
    label_id_t post_label_id = graph.GetVertexLabelId("POST");
    label_id_t comment_label_id = graph.GetVertexLabelId("COMMENT");
    label_id_t has_creator_label_id = graph.GetEdgeLabelId("HASCREATOR");

    using Engine = SyncEngine<GRAPH_INTERFACE>;

    auto filter =
        gs::make_filter(Expression1(id), gs::PropertySelector<oid_t>("id"));
    auto ctx0 = Engine::template ScanVertex<AppendOpt::Temp>(
        graph, person_label_id, std::move(filter));

    auto edge_expand_opt = gs::make_edge_expandv_opt(
        gs::Direction::Both, knows_label_id, person_label_id);
    auto ctx1 = Engine::template EdgeExpandV<AppendOpt::Persist, LAST_COL>(
        graph, std::move(ctx0), std::move(edge_expand_opt));

    gs::OrderingPropPair<gs::SortOrder::DESC, INPUT_COL_ID(-1), int64_t> pair0(
        "creationDate");  // creationDate.
    gs::OrderingPropPair<gs::SortOrder::ASC, INPUT_COL_ID(-1), oid_t> pair1(
        "id");
    auto ctx2 = Engine::Sort(graph, std::move(ctx1), gs::Range(0, 20),
                             std::tuple{pair0, pair1});

    auto mapper1 = gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
        PropertySelector<oid_t>("id"));
    auto mapper2 = gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
        PropertySelector<std::string_view>("firstName"));
    auto mapper3 = gs::make_mapper_with_variable<INPUT_COL_ID(1)>(
        PropertySelector<std::string_view>("lastName"));
    auto ctx3 = Engine::template Project<PROJ_TO_NEW>(
        graph, std::move(ctx2),
        std::tuple{std::move(mapper1), std::move(mapper2), std::move(mapper3)});
    return Engine::Sink(ctx3);
  }
};
} //namespace gs

extern "C" {
void* CreateApp(gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::SampleQuery* app =
        new gs::Query0();
    return static_cast<void*>(app);
  }
  return nullptr;
}
void DeleteApp(void* app, gs::GraphStoreType store_type) {
  if (store_type == gs::GraphStoreType::Grape) {
    gs::SampleQuery* casted =
        static_cast<gs::Query0*>(app);
    delete casted;
  }
}
}
```

To write your own stored procedure in c++, please refer to [Engine Reference](./engine_reference.md).


## Register to Database

Registering c++ stored procedure is almost the same as registering a cypher stored procedure, the only difference is that you need to write a yaml file to describe the input and output of the stored procedure.

```yaml
name: "query_name"
description: "The description of the query"
mode: READ  # WRITE, SCHEMA
extension: ".so"
params:
  - name: "personId2"  # The name of the parameter
    type: "long"  # The type of the parameter, string, int, float, double, bool
  - name: "maxDate"
    type: "long"
returns:
  - name: "name"
    type: "string"
```

Note that the params describe the input parameters, and the returns section describe the results schema.

## Call the stored procedure

In cypher shell, you can call the stored procedure with query_name and the correponding paramters. If the runtime parameters are not provided correctly, the corresponding exception msg will be displayed in cypher-shell.

```cypher
neo4j> CALL sample_query(123, 123040)
```
