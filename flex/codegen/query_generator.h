#ifndef QUERY_GENERATOR_H
#define QUERY_GENERATOR_H

#include <string>
#include <vector>

#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/building_context.h"
#include "flex/codegen/op_builder/dedup_builder.h"
#include "flex/codegen/op_builder/edge_expand_builder.h"
#include "flex/codegen/op_builder/fold_builder.h"
#include "flex/codegen/op_builder/get_v_builder.h"
#include "flex/codegen/op_builder/path_expand_builder.h"
#include "flex/codegen/op_builder/project_builder.h"
#include "flex/codegen/op_builder/scan_builder.h"
#include "flex/codegen/op_builder/select_builder.h"
#include "flex/codegen/op_builder/sink_builder.h"
#include "flex/codegen/op_builder/sort_builder.h"

#include "flex/codegen/op_builder/join_utils.h"
#include "flex/codegen/codegen_utils.h"

namespace gs {

// declare
template <typename LabelT>
static std::array<std::string, 4> BuildJoinOp(
    BuildingContext& ctx, const physical::Join& join_op_pb,
    const physical::PhysicalOpr::MetaData& meta_data);

// declare
template <typename LabelT>
static std::string BuildApplyOp(
    BuildingContext& ctx, const physical::Apply& apply_op_pb,
    const physical::PhysicalOpr::MetaData& meta_data);

// declare
template <typename LabelT>
static std::array<std::string, 4> BuildIntersectOp(
    BuildingContext& ctx, const physical::Intersect& intersect_op);

// get_v can contains labels and filters.
// what ever it takes, we will always fuse label info into edge_expand,
// but if get_v contains expression, we will not fuse it into edge_expand
bool simple_get_v(const physical::GetV& get_v_op) {
  if (get_v_op.params().has_predicate()) {
    return false;
  }
  return true;
}

bool intermeidate_edge_op(const physical::EdgeExpand& expand_op) {
  if (!expand_op.has_alias() || expand_op.alias().value() == -1) {
    return true;
  }
  return false;
}

template <typename LabelT>
void extract_vertex_labels(const physical::GetV& get_v_op,
                           std::vector<LabelT>& vertex_labels) {
  // get vertex label id from get_
  auto get_v_tables = get_v_op.params().tables();
  for (auto vertex_label_pb : get_v_tables) {
    vertex_labels.emplace_back(
        try_get_label_from_name_or_id<LabelT>(vertex_label_pb));
  }
  LOG(INFO) << "Got vertex labels : " << gs::to_string(vertex_labels);
}

template <typename LabelT>
void build_fused_edge_get_v(
    BuildingContext& ctx, std::stringstream& ss,
    physical::EdgeExpand& edge_expand_op,
    const physical::PhysicalOpr::MetaData& edge_meta_data,
    const physical::GetV& get_v_op, const std::vector<LabelT>& vertex_labels) {
  // build edge expand

  CHECK(vertex_labels.size() > 0);
  edge_expand_op.set_expand_opt(
      physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
  edge_expand_op.mutable_alias()->set_value(get_v_op.alias().value());
  ss << _4_SPACES
     << BuildEdgeExpandOp<LabelT>(ctx, edge_expand_op, edge_meta_data,
                                  vertex_labels)
     << std::endl;
}

// Entrance for generating a parameterized query
// The generated class will have two function
// 1. Query(GraphInterface& graph, int64_t ts, Decoder& input) const override
// 2. Query(GraphInterface& graph, int64_t ts, Params&...params) const
// the first one overrides the base class function, and the second one will be
// called by the first one, with some params(depends on the plan received)
template <typename LabelT>
class QueryGenerator {
 public:
  // if edge expand e is followed by a get_v, we can fuse them into one op
  static constexpr bool FUSE_EDGE_GET_V = true;
  static constexpr bool FUSE_PATH_EXPAND_V = true;
  QueryGenerator(BuildingContext& ctx, const physical::PhysicalPlan& plan)
      : ctx_(ctx), plan_(plan) {}

  std::string GenerateQuery() {
    std::stringstream header, exprs, query_body;
    addHeaders(header);
    startNamespace(header);

    // During generate query body, we will track the parameteres
    // And also generate the expression for needed
    addQueryBody(query_body);
    // for expression collection when visiting operators, append to exprs
    addExprsBody(exprs);

    start_query_class(exprs);
    addGraphTypeAlias(exprs);
    start_query_func(
        exprs);  // prepend function signature after visiting all operators

    end_query_func(
        query_body);  // append function call after visiting all operators
    add_query_func_overide(query_body);
    end_query_class(query_body);
    endNamespace(query_body);
    add_export_func(query_body);
    LOG(INFO) << "Finish generating query...";
    return header.str() + exprs.str() + query_body.str();
  }

  // Generate a subtask for a subplan
  // 0: expr codes.
  // 1. query codes.
  std::pair<std::vector<std::string>, std::string> GenerateSubTask() const {
    std::stringstream exprs, query_body;
    addQueryBody(query_body);
    addExprsBody(exprs);
    return std::make_pair(ctx_.GetExprCode(), query_body.str());
  }

 private:
  void addHeaders(std::stringstream& ss) const {
    ss << "#include \"flex/engines/hqps/engine/sync_engine.h\"" << std::endl;
    ss << "#include \"" << ctx_.GetGraphHeader() << "\"" << std::endl;
    ss << "#include \"" << ctx_.GetAppBaseHeader() << "\"" << std::endl;
    ss << std::endl;
    LOG(INFO) << "Finish adding headers";
  }

  void start_query_class(std::stringstream& ss) const {
    LOG(INFO) << "Start query class";
    ss << "template<typename " << ctx_.GetGraphInterface() << ">" << std::endl;
    ss << "class " << ctx_.GetQueryClassName();
    ss << " : public " << ctx_.GetAppBaseClassName() << "<"
       << ctx_.GetGraphInterface() << ">";
    ss << "{" << std::endl;
    ss << " public:" << std::endl;
  }

  void start_query_func(std::stringstream& ss) const {
    LOG(INFO) << "Start query function";
    ss << ctx_.GetQueryRet() << " Query(const " << ctx_.GetGraphInterface()
       << "& graph, ";
    ss << "int64_t " << ctx_.TimeStampVar();
    if (ctx_.GetParameterVars().size() > 0) {
      auto vars = ctx_.GetParameterVars();
      sort(vars.begin(), vars.end(),
           [](const auto& a, const auto& b) { return a.id < b.id; });
      // FIXME: ENable this line
      // the dynamic params can be duplicate.
      CHECK(vars[0].id == 0);
      for (auto i = 0; i < vars.size(); ++i) {
        if (i > 0 && vars[i].id == vars[i - 1].id) {
          // found duplicate
          CHECK(vars[i] == vars[i - 1]);
          continue;
        } else {
          ss << ", " << data_type_2_string(vars[i].type) << " "
             << vars[i].var_name;
        }
      }
    }
    ss << ") const";
    ss << "{";
    ss << std::endl;
  }

  void end_query_func(std::stringstream& ss) const {
    ss << "}";
    ss << std::endl;
  }
  void end_query_class(std::stringstream& ss) const {
    ss << "};";
    ss << std::endl;
  }

  // implement the function that overrides the base class.
  void add_query_func_overide(std::stringstream& ss) const {
    LOG(INFO) << "Start query function override";
    ss << std::endl;
    ss << ctx_.GetQueryRet() << " Query(const " << ctx_.GetGraphInterface()
       << "& " << ctx_.GraphVar() << ", int64_t ";
    ss << ctx_.TimeStampVar() << ", Decoder& decoder) const override {";
    ss << std::endl;
    // decode params from decoder and call another query function
    std::vector<std::string> param_names;
    auto param_vars = ctx_.GetParameterVars();
    // the param vars itself contains the index, which is the order of the param
    sort(param_vars.begin(), param_vars.end(),
         [](const auto& a, const auto& b) { return a.id < b.id; });
    // FIXME: Enable this check
    if (param_vars.size() > 0) {
      CHECK(param_vars[0].id == 0);  // encoding start from 0
    }

    for (auto i = 0; i < param_vars.size(); ++i) {
      if (i > 0 && param_vars[i].id == param_vars[i - 1].id) {
        CHECK(param_vars[i] == param_vars[i - 1]);
        continue;
      } else {
        auto& cur_param_var = param_vars[i];
        // for each param_var, decode the param from decoder,and one line of
        // code
        auto res_var_name =
            decode_param_from_decoder(ss, cur_param_var, i, "var", "decoder");
        param_names.push_back(res_var_name);
      }
    }
    LOG(INFO) << "Finish decoding params, size: " << param_names.size();
    ss << _4_SPACES << "return Query(" << ctx_.GraphVar() << ", "
       << ctx_.TimeStampVar();
    for (auto i = 0; i < param_names.size(); ++i) {
      ss << ", " << param_names[i];
    }
    ss << ");" << std::endl;
    ss << "}" << std::endl;
  }

  void startNamespace(std::stringstream& ss) const {
    ss << "namespace gs {" << std::endl;
    ss << std::endl;
  }

  void addGraphTypeAlias(std::stringstream& ss) const {
    ss << " using Engine = SyncEngine<" << ctx_.GetGraphInterface() << ">;"
       << std::endl;
    ss << " using label_id_t = typename " << ctx_.GetGraphInterface()
       << "::label_id_t;" << std::endl;
    ss << "using vertex_id_t = typename " << ctx_.GetGraphInterface()
       << "::vertex_id_t;" << std::endl;
  }

  void endNamespace(std::stringstream& ss) const {
    ss << "}  // namespace gs" << std::endl;
    ss << std::endl;
  }

  void add_export_func(std::stringstream& ss) const {
    ss << "extern \"C\" {" << std::endl;
    ss << "void* CreateApp(gs::GraphStoreType store_type) {" << std::endl;
    ss << "  if (store_type == gs::GraphStoreType::Grape) {" << std::endl;
    ss << "    gs::" << ctx_.GetQueryClassName()
       << "<gs::GrapeGraphInterface>* app = new gs::"
       << ctx_.GetQueryClassName() << "<gs::GrapeGraphInterface>();"
       << std::endl;
    ss << "    return static_cast<void*>(app);" << std::endl;
    ss << "  }";
    #if 0
    ss << " else {" << std::endl;
    ss << "    gs::" << ctx_.GetQueryClassName()
       << "<gs::GrockGraphInterface>* app = new gs::"
       << ctx_.GetQueryClassName() << "<gs::GrockGraphInterface>();"
       << std::endl;
    ss << "    return static_cast<void*>(app);" << std::endl;
    ss << "  }" << std::endl;
    #endif
    ss << "}" << std::endl;

    ss << "void DeleteApp(void* app, gs::GraphStoreType store_type) {"
       << std::endl;
    ss << "  if (store_type == gs::GraphStoreType::Grape) {" << std::endl;
    ss << "    gs::" << ctx_.GetQueryClassName()
       << "<gs::GrapeGraphInterface>* casted = static_cast<gs::"
       << ctx_.GetQueryClassName() << "<gs::GrapeGraphInterface>*>(app);"
       << std::endl;
    ss << "    delete casted;" << std::endl;
    ss << "  }";
    #if 0
    ss <"else {" << std::endl;
    ss << "    gs::" << ctx_.GetQueryClassName()
       << "<gs::GrockGraphInterface>* casted = static_cast<gs::"
       << ctx_.GetQueryClassName() << "<gs::GrockGraphInterface>*>(app);"
       << std::endl;
    ss << "   delete casted;" << std::endl;
    ss << "  }" << std::endl;
    #endif
    ss << "}";

    ss << "}" << std::endl;
  }

  void addExprsBody(std::stringstream& ss) const {
    LOG(INFO) << "Adding exprs to the query class";
    auto exprs = ctx_.GetExprCode();
    for (auto& expr : exprs) {
      ss << expr << std::endl;
    }
    ss << std::endl;
  }

  void addQueryBody(std::stringstream& ss) const {
    auto size = plan_.plan_size();

    LOG(INFO) << "Found " << size << " operators in the plan";
    for (auto i = 0; i < size; ++i) {
      auto op = plan_.plan(i);
      auto& meta_datas = op.meta_data();
      // CHECK(meta_datas.size() == 1) << "meta data size: " <<
      // meta_datas.size();
      // physical::PhysicalOpr::MetaData meta_data; //fake meta
      auto opr = op.opr();
      switch (opr.op_kind_case()) {
      case physical::PhysicalOpr::Operator::kScan: {  // scan
        // TODO: meta_data is not found in scan
        physical::PhysicalOpr::MetaData meta_data;

        LOG(INFO) << "Found a scan operator";
        auto& scan_op = opr.scan();

        ss << _4_SPACES << BuildScanOp(ctx_, scan_op, meta_data) << std::endl;
        break;
      }
      case physical::PhysicalOpr::Operator::kEdge: {  // edge expand
        physical::EdgeExpand real_edge_expand = opr.edge();
        // try to use infomation from later operator
        std::vector<LabelT> dst_vertex_labels;
        if (i + 1 < size) {
          auto& get_v_op_opr = plan_.plan(i + 1).opr();
          if (get_v_op_opr.op_kind_case() ==
              physical::PhysicalOpr::Operator::kVertex) {
            auto& get_v_op = get_v_op_opr.vertex();
            extract_vertex_labels(get_v_op, dst_vertex_labels);

            if (FUSE_EDGE_GET_V) {
              if (simple_get_v(get_v_op) &&
                  intermeidate_edge_op(real_edge_expand)) {
                CHECK(dst_vertex_labels.size() > 0);
                LOG(INFO) << "When fuseing edge+get_v, get_v has labels: "
                          << gs::to_string(dst_vertex_labels);
                build_fused_edge_get_v<LabelT>(ctx_, ss, real_edge_expand,
                                               meta_datas[0], get_v_op,
                                               dst_vertex_labels);
                LOG(INFO) << "Fuse edge expand and get_v since get_v is simple";
                i += 1;
                break;
              } else {
                // only fuse get_v label into edge expand
                LOG(INFO)
                    << "Skip fusing edge expand and get_v since simple get v";
              }
            }
          } else {
            LOG(INFO) << "Skip fusing edge expand and get_v since the next "
                         "operator is not get_v";
          }
        } else {
          LOG(INFO) << "EdgeExpand is the last operator";
        }
        auto& meta_data = meta_datas[0];
        LOG(INFO) << "Found a edge expand operator";
        ss << _4_SPACES
           << BuildEdgeExpandOp<LabelT>(ctx_, real_edge_expand, meta_data,
                                        dst_vertex_labels)
           << std::endl;

        break;
      }

      case physical::PhysicalOpr::Operator::kDedup: {  // dedup
        // auto& meta_data = meta_datas[0];
        physical::PhysicalOpr::MetaData meta_data;  // fake meta
        LOG(INFO) << "Found a dedup operator";
        auto& dedup_op = opr.dedup();
        ss << _4_SPACES << BuildDedupOp(ctx_, dedup_op, meta_data) << std::endl;
        break;
      }

      case physical::PhysicalOpr::Operator::kProject: {  // project
        // project op can result into multiple meta data
        // auto& meta_data = meta_datas[0];
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a project operator";
        auto& project_op = opr.project();
        std::string project_opt_code, call_project_code;
        std::tie(project_opt_code, call_project_code) =
            BuildProjectOp(ctx_, project_op, meta_data);
        ss << _4_SPACES << project_opt_code << std::endl;
        ss << _4_SPACES << call_project_code << std::endl;
        break;
      }

      case physical::PhysicalOpr::Operator::kSelect: {
        // auto& meta_data = meta_datas[0];
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a select operator";
        auto& select_op = opr.select();
        std::string select_opt_code, call_select_code;
        std::tie(select_opt_code, call_select_code) =
            BuildSelectOp(ctx_, select_op, meta_data);
        ss << _4_SPACES << select_opt_code << std::endl;
        ss << _4_SPACES << call_select_code << std::endl;
        break;
      }

      case physical::PhysicalOpr::Operator::kVertex: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a get v operator";
        auto& get_v_op = opr.vertex();
        auto get_v_code = BuildGetVOp<LabelT>(ctx_, get_v_op, meta_data);
        // first output code can be empty, just ignore
        if (!get_v_code[0].empty()) {
          ss << _4_SPACES << get_v_code[0] << std::endl;
        }
        ss << _4_SPACES << get_v_code[1] << std::endl;
        ss << _4_SPACES << get_v_code[2] << std::endl;
        break;
      }

      case physical::PhysicalOpr::Operator::kGroupBy: {
        // auto& meta_data = meta_datas[0];
        // meta_data is currenly not used in groupby.
        physical::PhysicalOpr::MetaData meta_data;
        auto& group_by_op = opr.group_by();
        if (group_by_op.mappings_size() > 0) {
          LOG(INFO) << "Found a group by operator";
          std::vector<std::string> code_lines =
              BuildGroupByOp(ctx_, group_by_op, meta_data);
          for (auto& line : code_lines) {
            ss << _4_SPACES << line << std::endl;
          }
        } else {
          LOG(INFO) << "Found a group by operator with no group by keys";
          std::vector<std::string> code_lines =
              BuildGroupWithoutKeyOp(ctx_, group_by_op, meta_data);
          for (auto line : code_lines) {
            ss << _4_SPACES << line << std::endl;
          }
        }
        LOG(INFO) << "Finish groupby operator gen";
        break;
      }

      // Path Expand + GetV shall be always fused.
      case physical::PhysicalOpr::Operator::kPath: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a path operator";
        if (FUSE_PATH_EXPAND_V) {
          if (i + 1 < size) {
            auto& path_op = opr.path();
            auto& next_op = plan_.plan(i + 1).opr();
            CHECK(next_op.op_kind_case() ==
                  physical::PhysicalOpr::Operator::kVertex)
                << "PathExpand must be followed by GetV";
            auto& get_v_op = next_op.vertex();
            int32_t get_v_res_alias = -1;
            if (get_v_op.has_alias()) {
              get_v_res_alias = get_v_op.alias().value();
            }

            auto res = BuildPathExpandOp<LabelT>(ctx_, path_op, meta_datas,
                                                 get_v_res_alias);
            ss << _4_SPACES << res[0] << std::endl;  // expand opt code
            ss << _4_SPACES << res[1] << std::endl;  // get_v opt code
            ss << _4_SPACES << res[2] << std::endl;  // call path expand code
            i += 1;                                  // jump one step
            break;
          } else {
            LOG(FATAL) << "PathExpand is the last operator";
          }
        } else {
          LOG(FATAL) << "Currently not supported: PathExpand without Getv";
        }
      }

      case physical::PhysicalOpr::Operator::kApply: {
        auto& meta_data = meta_datas[0];
        LOG(INFO) << "Found a apply operator";
        auto& apply_op = opr.apply();
        std::string call_apply_code =
            BuildApplyOp<LabelT>(ctx_, apply_op, meta_data);
        ss << _4_SPACES << call_apply_code << std::endl;
        break;
      }

      case physical::PhysicalOpr::Operator::kJoin: {
        // auto& meta_data = meta_datas[0];
        LOG(INFO) << "Found a join operator";
        auto& join_op = opr.join();
        auto join_opt_code = BuildJoinOp<LabelT>(ctx_, join_op);
        for (auto& line : join_opt_code) {
          ss << _4_SPACES << line << std::endl;
        }
        break;
      }

      case physical::PhysicalOpr::Operator::kIntersect: {
        LOG(INFO) << "Found a intersect operator";
        // a intersect op must be followed by a unfold op
        CHECK(i + 1 < size) << " intersect op must be followed by a unfold op";
        auto& next_op = plan_.plan(i + 1).opr();
        CHECK(next_op.op_kind_case() ==
              physical::PhysicalOpr::Operator::kUnfold)
            << "intersect op must be followed by a unfold op";
        auto& intersect_op = opr.intersect();
        auto intersect_opt_code = BuildIntersectOp<LabelT>(ctx_, intersect_op);
        for (auto& line : intersect_opt_code) {
          ss << _4_SPACES << line << std::endl;
        }
        i += 1;  // skip unfold
        break;
      }

      case physical::PhysicalOpr::Operator::kOrderBy: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a order by operator";
        auto& order_by_op = opr.order_by();
        std::string sort_opt_code, sort_code;
        std::tie(sort_opt_code, sort_code) =
            BuildSortOp(ctx_, order_by_op, meta_data);
        ss << sort_opt_code << std::endl;
        ss << sort_code << std::endl;
        break;
      }

      case physical::PhysicalOpr::Operator::kSink: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a sink operator";
        auto& sink_op = opr.sink();
        std::string call_sink_code = BuildSinkOp(ctx_, sink_op, meta_data);
        ss << _4_SPACES << call_sink_code << std::endl;
        break;
      }

      default:
        LOG(FATAL) << "Unsupported operator type: " << opr.op_kind_case();
      }
    }
    LOG(INFO) << "Finish adding query";
  }

  BuildingContext& ctx_;
  const physical::PhysicalPlan& plan_;
};

// When building a join op, we need to consider the following cases:
// 0. tag_id to tag_ind mapping, two plan shoud keep different mappings
// const physical::PhysicalOpr::MetaData& meta_data
template <typename LabelT>
static std::array<std::string, 4> BuildJoinOp(
    BuildingContext& ctx, const physical::Join& join_op_pb) {
  auto join_kind = join_kind_pb_to_internal(join_op_pb.join_kind());
  CHECK(join_op_pb.left_keys_size() == join_op_pb.right_keys_size());
  // these keys are tag_ids.
  auto& left_keys = join_op_pb.left_keys();
  auto& right_keys = join_op_pb.right_keys();
  std::vector<int32_t> join_keys;  // the left_keys and
  for (auto i = 0; i < left_keys.size(); ++i) {
    CHECK(left_keys[i].tag().id() == right_keys[i].tag().id());
    join_keys.push_back(left_keys[i].tag().id());
  }

  LOG(INFO) << "Join tag: " << gs::to_string(join_keys);
  std::string copy_context_code, left_plan_code, right_plan_code, join_code;
  std::string left_res_ctx_name, right_res_ctx_name;
  std::string left_start_ctx_name, right_start_ctx_name;
  // the derived context should preserve the tag_id to tag_inds mappings we
  // already have.
  auto right_context = ctx.CreateSubTaskContext("right_");
  // if join op is the start node, the copy_context_code is empty
  if (ctx.EmptyContext()) {
    // the prefix of left context should be appended.
    // can append fix this problem?
    ctx.AppendContextPrefix("left_");
  } else {
    // copy the context.
    // always copy for right context.
    std::stringstream cur_ss;
    right_start_ctx_name = right_context.GetCurCtxName();
    left_start_ctx_name = ctx.GetCurCtxName();
    cur_ss << "auto " << right_start_ctx_name << "(" << ctx.GetCurCtxName()
           << ");" << std::endl;
    copy_context_code = cur_ss.str();
  }
  {
    // left code.
    // before enter left, we need to rename the context with left.
    auto left_task_generator =
        QueryGenerator<LabelT>(ctx, join_op_pb.left_plan());
    std::vector<std::string> left_exprs;
    // the generate left exprs are already contained in ctx;
    std::tie(left_exprs, left_plan_code) =
        left_task_generator.GenerateSubTask();
    left_res_ctx_name = ctx.GetCurCtxName();
    // for (auto expr : left_exprs) {
    //   ctx.AddExprCode(expr);
    // }
  }

  {
    // right code
    auto right_task_generator =
        QueryGenerator<LabelT>(right_context, join_op_pb.right_plan());
    std::vector<std::string> right_exprs;
    std::tie(right_exprs, right_plan_code) =
        right_task_generator.GenerateSubTask();
    right_res_ctx_name = right_context.GetCurCtxName();
    for (auto expr : right_exprs) {
      ctx.AddExprCode(expr);
    }
    auto right_param_vars = right_context.GetParameterVars();
    for (auto right_param_var : right_param_vars) {
      ctx.AddParameterVar(right_param_var);
    }
  }

  // join code.
  {
    // we need to extract distinct inds for two side join key
    std::stringstream cur_ss;
    std::string cur_ctx_name, prev_ctx_name;
    std::tie(prev_ctx_name, cur_ctx_name) = ctx.GetPrevAndNextCtxName();
    CHECK(prev_ctx_name == left_res_ctx_name)
        << prev_ctx_name << ", " << left_res_ctx_name;
    cur_ss << "auto " << cur_ctx_name << _ASSIGN_STR_;
    if (join_keys.size() == 1) {
      cur_ss << " Engine::template Join";
      cur_ss << "<";
      {
        // TODO:fixme
        cur_ss << ctx.GetTagInd(join_keys[0]) << ", "
               << right_context.GetTagInd(join_keys[0]) << ",";
        cur_ss << join_kind_to_str(join_kind);
      }
    } else if (join_keys.size() == 2) {
      cur_ss << " Engine::template Join";
      cur_ss << "<";
      {
        // TODO:fixme
        cur_ss << ctx.GetTagInd(join_keys[0]) << ", "
               << ctx.GetTagInd(join_keys[1]) << ","
               << right_context.GetTagInd(join_keys[0]) << ","
               << right_context.GetTagInd(join_keys[1]) << ",";
        cur_ss << join_kind_to_str(join_kind);
      }
    } else {
      LOG(FATAL) << "Join on more than two key is not supported yet.";
    }

    cur_ss << ">";
    cur_ss << "(";
    {
      cur_ss << "std::move(" << left_res_ctx_name << "),";
      cur_ss << "std::move(" << right_res_ctx_name << ")";
    }
    cur_ss << ");";
    join_code = cur_ss.str();
  }
  return std::array<std::string, 4>{copy_context_code, left_plan_code,
                                    right_plan_code, join_code};
}

template <typename LabelT>
static std::string BuildApplyOp(
    BuildingContext& ctx, const physical::Apply& apply_op_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  auto join_kind = join_kind_pb_to_internal(apply_op_pb.join_kind());
  auto res_alias = apply_op_pb.alias().value();
  auto& sub_plan = apply_op_pb.sub_plan();
  std::string lambda_func_name, lambda_func_code;
  {
    auto new_building_ctx = ctx.CreateSubTaskContext();
    auto sub_task_generator =
        QueryGenerator<LabelT>(new_building_ctx, sub_plan);
    // QueryGenrator<LabelT> sub_task_generator(new_building_ctx, sub_plan_);
    // gen a lambda function.
    lambda_func_name = ctx.GetNextLambdaFuncName();
    std::stringstream inner_ss;
    // header
    inner_ss << "auto " << lambda_func_name << " = [&]";
    inner_ss << "(auto&& " << new_building_ctx.GetCurCtxName() << ") {"
             << std::endl;

    // body
    std::vector<std::string> exprs;
    std::string query_code;
    std::tie(exprs, query_code) = sub_task_generator.GenerateSubTask();
    inner_ss << query_code;
    for (auto expr : exprs) {
      ctx.AddExprCode(expr);
    }
    // end
    // return last context;
    inner_ss << " return " << new_building_ctx.GetCurCtxName() << ";"
             << std::endl;
    inner_ss << "};" << std::endl;

    lambda_func_code = inner_ss.str();
  }

  std::stringstream inner_ss;
  std::string prev_ctx_name, next_ctx_name;
  std::tie(prev_ctx_name, next_ctx_name) = ctx.GetPrevAndNextCtxName();
  inner_ss << lambda_func_code << std::endl;
  inner_ss << "auto " << next_ctx_name << " = Engine::template";
  inner_ss << " Apply<" << res_alias << "," << join_kind_to_str(join_kind)
           << ">";
  inner_ss << "(std::move(" << prev_ctx_name << "),";
  inner_ss << "std::move(" << lambda_func_name << "));" << std::endl;
  return inner_ss.str();
}

// declare
template <typename LabelT>
static std::array<std::string, 4> BuildIntersectOp(
    BuildingContext& ctx, const physical::Intersect& intersect_op) {
  auto& sub_plans = intersect_op.sub_plans();
  CHECK(sub_plans.size() == 2) << "Only support two sub plans intersect now.";
  auto& left_plan = sub_plans[0];
  auto& right_plan = sub_plans[1];
  auto join_key = intersect_op.key();
  LOG(INFO) << "join on key: " << join_key;

  std::string copy_context_code;
  std::string left_res_ctx_name, right_res_ctx_name;
  std::string left_plan_code, right_plan_code;
  std::string intersect_code;

  auto right_context = ctx.CreateSubTaskContext("right_");
  CHECK(!ctx.EmptyContext());

  {
    std::stringstream cur_ss;
    auto right_start_ctx_name = right_context.GetCurCtxName();
    auto left_start_ctx_name = ctx.GetCurCtxName();
    cur_ss << "auto " << right_start_ctx_name << "(" << left_start_ctx_name
           << ");" << std::endl;
    copy_context_code = cur_ss.str();
  }

  {
    // left code;
    auto left_task_generator = QueryGenerator<LabelT>(ctx, left_plan);
    std::vector<std::string> left_exprs;
    // the generate left exprs are already contained in ctx;
    std::tie(left_exprs, left_plan_code) =
        left_task_generator.GenerateSubTask();
    left_res_ctx_name = ctx.GetCurCtxName();
  }
  {
    // right code
    auto right_task_generator =
        QueryGenerator<LabelT>(right_context, right_plan);
    std::vector<std::string> right_exprs;
    std::tie(right_exprs, right_plan_code) =
        right_task_generator.GenerateSubTask();
    right_res_ctx_name = right_context.GetCurCtxName();
    for (auto expr : right_exprs) {
      ctx.AddExprCode(expr);
    }
  }
  // intersect code;
  {
    std::stringstream cur_ss;
    std::string cur_ctx_name, prev_ctx_name;
    std::tie(prev_ctx_name, cur_ctx_name) = ctx.GetPrevAndNextCtxName();
    CHECK(prev_ctx_name == left_res_ctx_name)
        << prev_ctx_name << ", " << left_res_ctx_name;

    auto right_tag_ind = right_context.GetTagInd(join_key);
    auto left_tag_ind = ctx.GetTagInd(join_key);
    LOG(INFO) << "Intersect on tag ind: " << left_tag_ind << ", "
              << right_tag_ind;

    cur_ss << "auto " << cur_ctx_name << _ASSIGN_STR_;
    cur_ss << " Engine::template Intersect";
    cur_ss << "<" << left_tag_ind << "," << right_tag_ind << ">";
    cur_ss << "(std::move(" << left_res_ctx_name << "),std::move("
           << right_res_ctx_name << "));";
    intersect_code = cur_ss.str();
  }
  return std::array<std::string, 4>{copy_context_code, left_plan_code,
                                    right_plan_code, intersect_code};
}

}  // namespace gs

#endif  // QUERY_GENERATOR_H
