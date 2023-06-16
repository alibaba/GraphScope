#include "flex/storages/mutable_csr/grape_graph_interface.h"

namespace gs {

GrapeGraphInterface& GrapeGraphInterface::get() {
  static GrapeGraphInterface instance;
  return instance;
}


}  // namespace gs