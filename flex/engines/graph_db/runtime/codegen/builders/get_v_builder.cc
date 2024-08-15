#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"
namespace gs {
namespace runtime {
class GetVBuilder {
 public:
  GetVBuilder(BuildingContext& context) : context_(context) {};
  std::string Build(const physical::GetV& opr) {
    /** int tag = -1;
    if (opr.has_tag()) {
      tag = opr.tag().value();
    }
    VOpt opt = parse_opt(opr.opt());

    int alias = -1;
    if (opr.has_alias()) {
      alias = opr.alias().value();
    }
    */
    std::stringstream ss;

    return ss.str();
  }
  BuildingContext& context_;
};
std::string build_get_v(BuildingContext& context, const physical::GetV& opr) {
  return GetVBuilder(context).Build(opr);
}
}  // namespace runtime
}  // namespace gs