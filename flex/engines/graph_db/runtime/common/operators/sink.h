#ifndef RUNTIME_COMMON_OPERATORS_SINK_H_
#define RUNTIME_COMMON_OPERATORS_SINK_H_

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/utils/app_utils.h"

namespace gs {
namespace runtime {

class Sink {
 public:
  static void sink(const Context& ctx, Encoder& output);
};

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_SINK_H_