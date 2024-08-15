
#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"

namespace gs {
namespace runtime {
class SinkBuilder {
 public:
  SinkBuilder(BuildingContext& context) : context_(context) {};

  std::string Build() {
    std::stringstream ss;
    ss << "sink(" << context_.GetCurCtxName() << ", txn, output);\n";
    return ss.str();
  }

  BuildingContext& context_;
};

std::string BuildSink(BuildingContext& context) {
  SinkBuilder builder(context);
  return builder.Build();
}

}  // namespace runtime
}  // namespace gs
