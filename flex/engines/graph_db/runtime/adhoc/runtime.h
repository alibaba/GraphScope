#ifndef RUNTIME_ADHOC_RUNTIME_H_
#define RUNTIME_ADHOC_RUNTIME_H_

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

namespace runtime {

Context runtime_eval(const physical::PhysicalPlan& plan,
                     const ReadTransaction& txn,
                     const std::map<std::string, std::string>& params);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_RUNTIME_H_