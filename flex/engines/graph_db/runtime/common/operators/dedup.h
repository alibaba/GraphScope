#ifndef RUNTIME_COMMON_OPERATORS_DEDUP_H_
#define RUNTIME_COMMON_OPERATORS_DEDUP_H_

#include <set>
#include <unordered_set>

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

class Dedup {
 public:
  static void dedup(const ReadTransaction& txn, Context& ctx,
                    const std::vector<size_t>& cols);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_DEDUP_H_