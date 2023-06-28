#ifndef ENGINES_HQPS_APP_CYPHER_APP_BASE_H_
#define ENGINES_HQPS_APP_CYPHER_APP_BASE_H_

#include "flex/engines/hqps/database/grape_graph_interface.h"
#include "flex/utils/app_utils.h"
#include "proto_generated_gie/results.pb.h"

namespace gs {

enum class GraphStoreType {
  Grape = 0,
  Grock = 1,
};

template <typename GRAPH_TYPE>
class HqpsAppBase {
 public:
  virtual ~HqpsAppBase() = default;
  virtual results::CollectiveResults Query(const GRAPH_TYPE& graph, int64_t ts,
                                           Decoder& input) const = 0;
};

}  // namespace gs

#endif  // ENGINES_HQPS_APP_CYPHER_APP_BASE_H_