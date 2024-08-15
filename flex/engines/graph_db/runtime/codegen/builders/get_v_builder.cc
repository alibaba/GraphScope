#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"
namespace gs {
namespace runtime {
class GetVBuilder {
 public:
  GetVBuilder(BuildingContext& context) : context_(context) {};
  std::string Build(const physical::GetV& opr) {
    std::stringstream ss;
    ss << "GetVBuilder::Build()";
    return ss.str();
  }
  BuildingContext& context_;
};
std::string BuildGetV(BuildingContext& context, const physical::GetV& opr) {
  return GetVBuilder(context).Build(opr);
}
}  // namespace runtime
}  // namespace gs