/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace gs {

namespace runtime {

class OpCost {
 public:
  OpCost() {}
  ~OpCost() {
    double total = 0;
    for (auto& pair : table) {
      total += pair.second;
    }
    LOG(INFO) << "op elapsed time: ";
    for (auto& pair : table) {
      LOG(INFO) << "\t" << pair.first << ": " << pair.second << " ("
                << pair.second / total * 100.0 << "%)";
    }
  }

  static OpCost& get() {
    static OpCost instance;
    return instance;
  }

  std::map<std::string, double> table;
};

static std::string get_opr_name(const physical::PhysicalOpr& opr) {
  switch (opr.opr().op_kind_case()) {
  case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
    return "scan";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
    return "edge_expand";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
    return "get_v";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
    return "order_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
    return "project";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSink: {
    return "sink";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
    return "dedup";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
    return "group_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
    return "select";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
    return "path";
  }
  default:
    return "unknown - " +
           std::to_string(static_cast<int>(opr.opr().op_kind_case()));
  }
}

bl::result<Context> runtime_eval_impl(
    const physical::PhysicalPlan& plan, Context&& ctx,
    const ReadTransaction& txn,
    const std::map<std::string, std::string>& params) {
  Context ret = ctx;

  auto& op_cost = OpCost::get().table;

  int opr_num = plan.plan_size();
  bool terminate = false;
  for (int i = 0; i < opr_num; ++i) {
    const physical::PhysicalOpr& opr = plan.plan(i);
    double t = -grape::GetCurrentTime();
    assert(opr.has_opr());
    switch (opr.opr().op_kind_case()) {
      LOG(INFO) << "eval: " << get_opr_name(opr);
    case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
      BOOST_LEAF_ASSIGN(ret, eval_scan(opr.opr().scan(), txn, params));
      t += grape::GetCurrentTime();
      op_cost["scan"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
      CHECK_EQ(opr.meta_data_size(), 1);
      BOOST_LEAF_ASSIGN(
          ret, eval_edge_expand(opr.opr().edge(), txn, std::move(ret), params,
                                opr.meta_data(0)));
      t += grape::GetCurrentTime();
      op_cost["edge_expand"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
      BOOST_LEAF_ASSIGN(
          ret, eval_get_v(opr.opr().vertex(), txn, std::move(ret), params));
      t += grape::GetCurrentTime();
      op_cost["get_v"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
      std::vector<common::IrDataType> data_types;
      if (opr.meta_data_size() == opr.opr().project().mappings_size()) {
        for (int i = 0; i < opr.meta_data_size(); ++i) {
          if (opr.meta_data(i).type().type_case() ==
              common::IrDataType::TypeCase::TYPE_NOT_SET) {
            LOG(INFO) << "type not set";
          }
          data_types.push_back(opr.meta_data(i).type());
        }
      }
      BOOST_LEAF_ASSIGN(ret, eval_project(opr.opr().project(), txn,
                                          std::move(ret), params, data_types));
      t += grape::GetCurrentTime();
      op_cost["project"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
      BOOST_LEAF_ASSIGN(
          ret, eval_order_by(opr.opr().order_by(), txn, std::move(ret)));
      t += grape::GetCurrentTime();
      op_cost["order_by"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
      BOOST_LEAF_ASSIGN(
          ret, eval_group_by(opr.opr().group_by(), txn, std::move(ret)));
      t += grape::GetCurrentTime();
      op_cost["group_by"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
      BOOST_LEAF_ASSIGN(ret,
                        eval_dedup(opr.opr().dedup(), txn, std::move(ret)));
      t += grape::GetCurrentTime();
      op_cost["dedup"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
      BOOST_LEAF_ASSIGN(
          ret, eval_select(opr.opr().select(), txn, std::move(ret), params));
      t += grape::GetCurrentTime();
      op_cost["select"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
      if ((i + 1) < opr_num) {
        const physical::PhysicalOpr& next_opr = plan.plan(i + 1);
        if (next_opr.opr().has_vertex() &&
            opr.opr().path().result_opt() ==
                physical::PathExpand_ResultOpt::PathExpand_ResultOpt_END_V &&
            opr.opr().path().base().edge_expand().expand_opt() ==
                physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
          int alias = -1;
          if (next_opr.opr().vertex().has_alias()) {
            alias = next_opr.opr().vertex().alias().value();
          }
          BOOST_LEAF_ASSIGN(
              ret, eval_path_expand_v(opr.opr().path(), txn, std::move(ret),
                                      params, opr.meta_data(0), alias));
          ++i;
        } else {
          int alias = -1;
          if (opr.opr().path().has_alias()) {
            alias = opr.opr().path().alias().value();
          }
          BOOST_LEAF_ASSIGN(
              ret, eval_path_expand_p(opr.opr().path(), txn, std::move(ret),
                                      params, opr.meta_data(0), alias));
        }
      } else {
        LOG(ERROR) << "Path Expand to Path is currently not supported";
        RETURN_UNSUPPORTED_ERROR(
            "Path Expand to Path is currently not supported");
      }
      t += grape::GetCurrentTime();
      op_cost["path_expand"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kSink: {
      terminate = true;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kRoot: {
      // do nothing
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kJoin: {
      auto op = opr.opr().join();
      auto ret_dup = ret.dup();
      BOOST_LEAF_AUTO(
          ctx, runtime_eval_impl(op.left_plan(), std::move(ret), txn, params));
      BOOST_LEAF_AUTO(ctx2, runtime_eval_impl(op.right_plan(),
                                              std::move(ret_dup), txn, params));
      BOOST_LEAF_ASSIGN(ret, eval_join(op, std::move(ctx), std::move(ctx2)));

    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
      auto op = opr.opr().intersect();
      size_t num = op.sub_plans_size();
      std::vector<Context> ctxs;
      ret.push_idx_col();
      for (size_t i = 0; i < num; ++i) {
        if (i + 1 < num) {
          auto ret_dup = ret.dup();
          BOOST_LEAF_AUTO(
              ctx, runtime_eval_impl(op.sub_plans(i), std::move(ret_dup), txn,
                                     params));
          ctxs.push_back(std::move(ctx));
        } else {
          BOOST_LEAF_AUTO(ctx, runtime_eval_impl(op.sub_plans(i),
                                                 std::move(ret), txn, params));
          ctxs.push_back(std::move(ctx));
        }
      }
      BOOST_LEAF_ASSIGN(ret, eval_intersect(txn, op, std::move(ctxs)));
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kLimit: {
      BOOST_LEAF_ASSIGN(ret, eval_limit(opr.opr().limit(), std::move(ret)));
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kProcedureCall: {
      std::vector<int32_t> aliases;
      for (int32_t i = 0; i < opr.meta_data_size(); ++i) {
        aliases.push_back(opr.meta_data(i).alias());
      }
      BOOST_LEAF_ASSIGN(
          ret, eval_procedure_call(aliases, opr.opr().procedure_call(), txn,
                                   std::move(ret)));
    } break;

    default:
      LOG(ERROR) << "Unknown operator type: "
                 << static_cast<int>(opr.opr().op_kind_case());
      RETURN_UNSUPPORTED_ERROR(
          "Unknown operator type: " +
          std::to_string(static_cast<int>(opr.opr().op_kind_case())));
      break;
    }
    if (terminate) {
      break;
    }
  }
  return ret;
}

bl::result<Context> runtime_eval(
    const physical::PhysicalPlan& plan, const ReadTransaction& txn,
    const std::map<std::string, std::string>& params) {
  return runtime_eval_impl(plan, Context(), txn, params);
}

}  // namespace runtime

}  // namespace gs