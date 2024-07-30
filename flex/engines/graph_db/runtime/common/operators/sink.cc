#include "flex/engines/graph_db/runtime/common/operators/sink.h"

namespace gs {

namespace runtime {
// TODO: Implement the sink function
void Sink::sink(const Context& ctx, Encoder& output) {
  size_t row_num = ctx.row_num();
  size_t col_num = ctx.col_num();
  for (size_t i = 0; i < row_num; ++i) {
    for (size_t j = 0; j < col_num; ++j) {
      auto col = ctx.get(j);
      if (col == nullptr) {
        continue;
      }
      auto val = col->get_elem(row_num - i - 1);
      val.sink(output);
    }
  }
}

}  // namespace runtime

}  // namespace gs
