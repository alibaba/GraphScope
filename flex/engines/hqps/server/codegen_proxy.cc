#include "flex/engines/hqps/server/codegen_proxy.h"

namespace snb {
namespace ic {
CodegenProxy& CodegenProxy::get() {
  static CodegenProxy instance;
  return instance;
}
}  // namespace ic

}  // namespace snb
