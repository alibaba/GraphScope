#ifndef RUNTIME_ADHOC_OPERATORS_OPERATORS_H_
#define RUNTIME_ADHOC_OPERATORS_OPERATORS_H_

#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/utils/app_utils.h"

namespace gs {

namespace runtime {

Context eval_dedup(const algebra::Dedup& opr, const ReadTransaction& txn,
                   Context&& ctx);

Context eval_group_by(const physical::GroupBy& opr, const ReadTransaction& txn,
                      Context&& ctx);

Context eval_order_by(const algebra::OrderBy& opr, const ReadTransaction& txn,
                      Context&& ctx);

Context eval_path_expand_v(const physical::PathExpand& opr,
                           const ReadTransaction& txn, Context&& ctx,
                           const std::map<std::string, std::string>& params,
                           const physical::PhysicalOpr_MetaData& meta,
                           int alias);

Context eval_path_expand_p(const physical::PathExpand& opr,
                           const ReadTransaction& txn, Context&& ctx,
                           const std::map<std::string, std::string>& params,
                           const physical::PhysicalOpr_MetaData& meta,
                           int alias);

Context eval_project(const physical::Project& opr, const ReadTransaction& txn,
                     Context&& ctx,
                     const std::map<std::string, std::string>& params,
                     const std::vector<common::IrDataType>& data_types);

Context eval_scan(const physical::Scan& scan_opr, const ReadTransaction& txn,
                  const std::map<std::string, std::string>& params);

Context eval_select(const algebra::Select& opr, const ReadTransaction& txn,
                    Context&& ctx,
                    const std::map<std::string, std::string>& params);

Context eval_edge_expand(const physical::EdgeExpand& opr,
                         const ReadTransaction& txn, Context&& ctx,
                         const std::map<std::string, std::string>& params,
                         const physical::PhysicalOpr_MetaData& meta);

Context eval_get_v(const physical::GetV& opr, const ReadTransaction& txn,
                   Context&& ctx,
                   const std::map<std::string, std::string>& params);

Context eval_intersect(const ReadTransaction& txn,
                       const physical::Intersect& opr,
                       std::vector<Context>&& ctx);

Context eval_join(const physical::Join& opr, Context&& ctx, Context&& ctx2);

void eval_sink(const Context& ctx, Encoder& output);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_OPERATORS_OPERATORS_H_