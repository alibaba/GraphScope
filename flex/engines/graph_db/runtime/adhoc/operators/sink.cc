#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {

namespace runtime {

void eval_sink(const Context& ctx, Encoder& output) {
  size_t row_num = ctx.row_num();
  size_t col_num = ctx.col_num();
  // LOG(INFO) << "sink: " << row_num;
  for (size_t i = 0; i < row_num; ++i) {
    // LOG(INFO) << "row-" << i << ":";
    for (size_t j = 0; j < col_num; ++j) {
      auto col = ctx.get(j);
      if (col == nullptr) {
        continue;
      }
      auto val = col->get_elem(i);
      LOG(INFO) << "\t" << val.to_string();
      // LOG(INFO) << "\t" << val.to_string();
      val.sink(output);
    }
  }
}

}  // namespace runtime

}  // namespace gs