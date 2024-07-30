#include "flex/engines/graph_db/runtime/common/operators/scan.h"

namespace gs {
namespace runtime {

Context Scan::find_vertex(const ReadTransaction& txn, label_t label,
                          const Any& pk, int alias) {
  SLVertexColumnBuilder builder(label);
  vid_t vid;
  if (txn.GetVertexIndex(label, pk, vid)) {
    builder.push_back_opt(vid);
  }
  Context ctx;
  ctx.set(alias, builder.finish());
  return ctx;
}

}  // namespace runtime

}  // namespace gs
