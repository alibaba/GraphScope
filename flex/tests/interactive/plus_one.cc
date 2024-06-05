#include "flex/engines/graph_db/app/app_base.h"
#include "flex/utils/app_utils.h"

namespace gs {
class PlusOne : public ReadAppBase {
 public:
  PlusOne() {}
  // Query function for query class
  bool Query(const gs::GraphDBSession& sess, Decoder& input,
             Encoder& output) override {
    int32_t param1 = input.get_int();
    LOG(INFO) << "param1: " << param1;
    output.put_int(param1 + 1);
    return true;
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::PlusOne& db) {
  gs::PlusOne* app = new gs::PlusOne();
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::PlusOne* casted = static_cast<gs::PlusOne*>(app);
  delete casted;
}
}