#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/utils/app_utils.h"

namespace gs {
class ExampleQuery : public AppBase {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  ExampleQuery(const GraphDBSession& session) : graph(session) {}
  // Query function for query class
  results::CollectiveResults Query() const {
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 1, Filter<TruePredicate>());

    auto ctx1 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx0),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
            gs::PropertySelector<int64_t>("id"))});
    auto ctx2 = Engine::Limit(std::move(ctx1), 0, 5);
    auto res = Engine::Sink(graph, ctx2, std::array<int32_t, 1>{0});
    LOG(INFO) << "res: " << res.DebugString();
    return res;
  }
  // Wrapper query function for query class
  bool Query(Decoder& decoder, Encoder& encoder) override {
    // decoding params from decoder, and call real query func

    auto res = Query();
    // dump results to string
    std::string res_str = res.SerializeAsString();
    // encode results to encoder
    encoder.put_string(res_str);
    return true;
  }
  gs::MutableCSRInterface graph;
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::ExampleQuery* app = new gs::ExampleQuery(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::ExampleQuery* casted = static_cast<gs::ExampleQuery*>(app);
  delete casted;
}
}