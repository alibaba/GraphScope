#include "flex/engines/graph_db/runtime/common/operators/dedup.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

namespace gs {

namespace runtime {

Context eval_dedup(const algebra::Dedup& opr, const ReadTransaction& txn,
                   Context&& ctx) {
  std::vector<size_t> keys;
  int keys_num = opr.keys_size();
  for (int k_i = 0; k_i < keys_num; ++k_i) {
    const common::Variable& key = opr.keys(k_i);
    CHECK(!key.has_property());
    CHECK(key.has_tag());
    keys.push_back(key.tag().id());
  }

  Dedup::dedup(txn, ctx, keys);
  return ctx;
}

}  // namespace runtime

}  // namespace gs
