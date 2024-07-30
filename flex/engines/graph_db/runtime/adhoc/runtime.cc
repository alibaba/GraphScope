#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

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

Context runtime_eval_impl(const physical::PhysicalPlan& plan, Context&& ctx,
                          const ReadTransaction& txn,
                          const std::map<std::string, std::string>& params) {
  Context ret = ctx;

  auto& op_cost = OpCost::get().table;

  int opr_num = plan.plan_size();
  bool terminate = false;
  for (int i = 0; i < opr_num; ++i) {
    const physical::PhysicalOpr& opr = plan.plan(i);
    double t = -grape::GetCurrentTime();
    // LOG(INFO) << "before eval: " << get_opr_name(opr);
    assert(opr.has_opr());
    switch (opr.opr().op_kind_case()) {
      LOG(INFO) << "eval: " << get_opr_name(opr);
    case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
      ret = eval_scan(opr.opr().scan(), txn, params);
      t += grape::GetCurrentTime();
      op_cost["scan"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
      CHECK_EQ(opr.meta_data_size(), 1);
      ret = eval_edge_expand(opr.opr().edge(), txn, std::move(ret), params,
                             opr.meta_data(0));
      t += grape::GetCurrentTime();
      op_cost["edge_expand"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
      ret = eval_get_v(opr.opr().vertex(), txn, std::move(ret), params);
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
      ret = eval_project(opr.opr().project(), txn, std::move(ret), params,
                         data_types);
      t += grape::GetCurrentTime();
      op_cost["project"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
      ret = eval_order_by(opr.opr().order_by(), txn, std::move(ret));
      t += grape::GetCurrentTime();
      op_cost["order_by"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
      ret = eval_group_by(opr.opr().group_by(), txn, std::move(ret));
      t += grape::GetCurrentTime();
      op_cost["group_by"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
      ret = eval_dedup(opr.opr().dedup(), txn, std::move(ret));
      t += grape::GetCurrentTime();
      op_cost["dedup"] += t;
    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
      ret = eval_select(opr.opr().select(), txn, std::move(ret), params);
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
          ret = eval_path_expand_v(opr.opr().path(), txn, std::move(ret),
                                   params, opr.meta_data(0), alias);
          ++i;
        } else {
          int alias = -1;
          if (opr.opr().path().has_alias()) {
            alias = opr.opr().path().alias().value();
          }
          ret = eval_path_expand_p(opr.opr().path(), txn, std::move(ret),
                                   params, opr.meta_data(0), alias);
        }
      } else {
        LOG(FATAL) << "not support";
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
      auto ctx = runtime_eval_impl(op.left_plan(), std::move(ret), txn, params);
      auto ctx2 =
          runtime_eval_impl(op.right_plan(), std::move(ret_dup), txn, params);
      LOG(INFO) << ctx.row_num() << " " << ctx2.row_num() << " "
                << ctx.col_num() << " " << ctx2.col_num();
      ret = eval_join(op, std::move(ctx), std::move(ctx2));

    } break;
    case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
      auto op = opr.opr().intersect();
      size_t num = op.sub_plans_size();
      std::vector<Context> ctxs;
      ret.push_idx_col();
      for (size_t i = 0; i < num; ++i) {
        if (i + 1 < num) {
          auto ret_dup = ret.dup();
          ctxs.push_back(runtime_eval_impl(op.sub_plans(i), std::move(ret_dup),
                                           txn, params));
        } else {
          ctxs.push_back(
              runtime_eval_impl(op.sub_plans(i), std::move(ret), txn, params));
        }
      }
      ret = eval_intersect(txn, op, std::move(ctxs));
    } break;
    default:
      LOG(FATAL) << "opr not support..." << get_opr_name(opr);
      break;
    }
    // ret.desc("opr-" + std::to_string(i) + ": " + get_opr_name(opr) + " - " +
    //          std::to_string(t));
    if (terminate) {
      break;
    }
  }
  return ret;
}

Context runtime_eval(const physical::PhysicalPlan& plan,
                     const ReadTransaction& txn,
                     const std::map<std::string, std::string>& params) {
  Context ret;
  // LOG(INFO) << "input: ";
  // for (auto& pair : params) {
  //   LOG(INFO) << "\t" << pair.first << ": " << pair.second;
  // }

  return runtime_eval_impl(plan, std::move(ret), txn, params);
}

}  // namespace runtime

}  // namespace gs