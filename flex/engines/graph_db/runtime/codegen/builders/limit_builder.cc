#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"

namespace gs {
namespace runtime {
class LimitBuilder {
 public:
  LimitBuilder(BuildingContext& context) : context_(context) {};

  std::string Build() { return ""; }

  LimitBuilder& Lower(int lower) {
    lower_ = lower;
    return *this;
  }
  LimitBuilder& Upper(int upper) {
    upper_ = upper;
    return *this;
  }

  BuildingContext& context_;
  int lower_;
  int upper_;
};

std::string build_limit(BuildingContext& context, const algebra::Limit& opr) {
  LimitBuilder builder(context);
  int lower = 0;
  int upper = std::numeric_limits<int>::max();
  if (opr.has_range()) {
    lower = std::max(lower, static_cast<int>(opr.range().lower()));
    upper = std::min(upper, static_cast<int>(opr.range().upper()));
  }

  // TODO
  return builder.Build();
}
}  // namespace runtime
}  // namespace gs