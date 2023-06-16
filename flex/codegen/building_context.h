#ifndef CODEGEN_SRC_BuildingContext_H_
#define CODEGEN_SRC_BuildingContext_H_

#include <vector>

#include "flex/codegen/graph_types.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "glog/logging.h"

namespace gs {
static constexpr const char* time_stamp = "time_stamp";
static constexpr const char* graph_var = "graph";
static constexpr const char* DEFAULT_GRAPH_INTERFACE = "GRAPH_INTERFACE";
static constexpr const char* DEFAULT_GRAPH_HEADER =
    "flex/storages/mutable_csr/grape_graph_interface.h";
static constexpr const char* EDGE_EXPAND_OPT_NAME = "edge_expand_opt";
static constexpr const char* SORT_OPT_NAME = "sort_opt";
static constexpr const char* GET_V_OPT_NAME = "get_v_opt";
static constexpr const char* EXPR_NAME = "expr";
static constexpr const char* PROJECT_NAME = "project_opt";
static constexpr const char* CONTEXT_NAME = "ctx";
static constexpr const char* GROUP_KEY_NAME = "group_key";
static constexpr const char* GROUP_OPT_NAME = "group_opt";
static constexpr const char* AGG_FUNC_NAME = "agg_func";
static constexpr const char* PATH_OPT_NAME = "path_opt";
static constexpr const char* APP_BASE_HEADER =
    "flex/engines/hqps/app/cypher_app_base.h";
static constexpr const char* APP_BASE_CLASS_NAME = "HqpsAppBase";
static constexpr const char* QUERY_FUNC_RETURN = "results::CollectiveResults";

enum class StorageBackend {
  kGrape = 0,
  KGrock = 1,
};

// manages the mapping between tag ids and tag inds.
struct TagIndMapping {
  int32_t GetTagInd(int32_t tag_id) const {
    if (tag_id == -1) {
      return -1;
    }
    print_debug_info();
    CHECK(tag_id < tag_id_2_tag_inds_.size())
        << "tag id: " << tag_id << " not found";
    return tag_id_2_tag_inds_[tag_id];
  }

  int32_t CreateOrGetTagInd(int32_t tag_id) {
    if (tag_id == -1) {
      return -1;
    }
    auto it =
        std::find(tag_ind_2_tag_ids_.begin(), tag_ind_2_tag_ids_.end(), tag_id);
    if (it == tag_ind_2_tag_ids_.end()) {
      auto new_tag_ind = tag_ind_2_tag_ids_.size();
      LOG(INFO) << "tag id: " << tag_id
                << " not found, new tag ind: " << new_tag_ind;
      tag_ind_2_tag_ids_.emplace_back(tag_id);
      auto old_size = tag_id_2_tag_inds_.size();
      if (tag_id + 1 > old_size) {
        tag_id_2_tag_inds_.resize(tag_id + 1);
        for (auto i = old_size; i < tag_id; ++i) {
          tag_id_2_tag_inds_[i] = -1;
        }
      }

      tag_id_2_tag_inds_[tag_id] = new_tag_ind;
      check_variant();
      return new_tag_ind;
    } else {
      return tag_id_2_tag_inds_[tag_id];
    }
  }

  void check_variant() const {
    print_debug_info();
    auto max_ind =
        *std::max_element(tag_id_2_tag_inds_.begin(), tag_id_2_tag_inds_.end());
    auto max_tag_id =
        *std::max_element(tag_ind_2_tag_ids_.begin(), tag_ind_2_tag_ids_.end());
    CHECK(max_ind + 1 == tag_ind_2_tag_ids_.size());
    CHECK(max_tag_id + 1 == tag_id_2_tag_inds_.size());
  }

  void print_debug_info() const {
    VLOG(10) << "tag id to inds : " << gs::to_string(tag_id_2_tag_inds_);
    VLOG(10) << "tag ind to tag ids: " << gs::to_string(tag_ind_2_tag_ids_);
  }

  const std::vector<int32_t>& GetTagInd2TagIds() const {
    return tag_ind_2_tag_ids_;
  }

  // convert tag_ind (us) to tag ids
  std::vector<int32_t> tag_ind_2_tag_ids_;
  // convert tag ids(pb) to tag_inds
  std::vector<int32_t> tag_id_2_tag_inds_;
};

class BuildingContext {
 public:
  BuildingContext(StorageBackend storage_type = StorageBackend::kGrape,
                  std::string query_name = "Query0",
                  std::string ctx_prefix = "")
      : storage_backend_(storage_type),
        graph_interface_(DEFAULT_GRAPH_INTERFACE),
        graph_header_(DEFAULT_GRAPH_HEADER),
        app_base_header_(APP_BASE_HEADER),
        ctx_id_(0),
        var_id_(0),
        query_name_(query_name),
        expr_var_id_(0),
        expr_id_(0),
        ctx_prefix_(ctx_prefix) {}

  BuildingContext(std::string graph_interface, std::string graph_header,
                  StorageBackend storage_type = StorageBackend::kGrape,
                  std::string query_name = "Query0",
                  std::string ctx_prefix = "")
      : storage_backend_(storage_type),
        graph_interface_(graph_interface),
        graph_header_(graph_header),
        app_base_header_(APP_BASE_HEADER),
        ctx_id_(0),
        var_id_(0),
        query_name_(query_name),
        expr_var_id_(0),
        expr_id_(0),
        ctx_prefix_(ctx_prefix) {}

  // int32_t GetCurrentCtxId() const { return ctx_id_; }
  bool EmptyContext() const { return ctx_id_ == 0; }

  // return a pair indicate the direction of assigning, also increate cur ctx id
  std::pair<std::string, std::string> GetPrevAndNextCtxName() {
    std::string ctx_name = ctx_prefix_ + CONTEXT_NAME + std::to_string(ctx_id_);
    std::string ctx_name2 =
        ctx_prefix_ + CONTEXT_NAME + std::to_string(ctx_id_ + 1);
    ctx_id_ += 1;
    return std::make_pair(ctx_name, ctx_name2);
  }

  std::string GetCurCtxName() const {
    return ctx_prefix_ + CONTEXT_NAME + std::to_string(ctx_id_);
  }

  std::string GetNextCtxName() const {
    return ctx_prefix_ + CONTEXT_NAME + std::to_string(ctx_id_ + 1);
  }

  void IncCtxId() { ++ctx_id_; }

  // int32_t GetCurrentVarId() const { return var_id_; }

  // int32_t GetCurrentExprid() const { return expr_id_; }
  std::string GetNextExprName() {
    return ctx_prefix_ + EXPR_NAME + std::to_string(expr_id_++);
  }

  std::string GetNextExprVarName() {
    return ctx_prefix_ + EXPR_NAME + std::to_string(expr_var_id_++);
  }

  std::string GetNextEdgeOptName() {
    return ctx_prefix_ + EDGE_EXPAND_OPT_NAME + std::to_string(var_id_++);
  }

  std::string GetNextSortOptName() {
    return ctx_prefix_ + SORT_OPT_NAME + std::to_string(var_id_++);
  }

  std::string GetNextGetVOptName() {
    return ctx_prefix_ + GET_V_OPT_NAME + std::to_string(var_id_++);
  }

  std::string GetNextProjectOptName() {
    return ctx_prefix_ + PROJECT_NAME + std::to_string(var_id_++);
  }

  std::string GetNextGroupKeyName() {
    return ctx_prefix_ + GROUP_KEY_NAME + std::to_string(var_id_++);
  }

  std::string GetNextAggFuncName() {
    return ctx_prefix_ + AGG_FUNC_NAME + std::to_string(var_id_++);
  }

  std::string GetNextGroupOptName() {
    return ctx_prefix_ + GROUP_OPT_NAME + std::to_string(var_id_++);
  }

  std::string GetNextPathOptName() {
    return ctx_prefix_ + PATH_OPT_NAME + std::to_string(var_id_++);
  }

  std::string GetNextVarName() { return "var" + std::to_string(var_id_++); }

  std::string GetGraphInterface() const { return graph_interface_; }
  std::string GetGraphHeader() const { return graph_header_; }
  std::string GetAppBaseHeader() const { return app_base_header_; }

  std::string GetAppBaseClassName() const { return APP_BASE_CLASS_NAME; }

  std::string ContextPrefix() const { return ctx_prefix_; }

  BuildingContext CreateSubTaskContext(std::string sufix = "inner_") {
    BuildingContext ctx;
    ctx.storage_backend_ = storage_backend_;
    ctx.query_name_ = query_name_;
    ctx.ctx_id_ = ctx_id_;
    ctx.var_id_ = var_id_;
    ctx.expr_id_ = expr_id_;
    ctx.expr_var_id_ = expr_var_id_;
    ctx.graph_interface_ = graph_interface_;
    ctx.app_base_header_ = app_base_header_;
    ctx.graph_header_ = graph_header_;
    ctx.ctx_prefix_ = ctx_prefix_ + sufix;
    ctx.tag_ind_mapping_ = tag_ind_mapping_;
    return ctx;
  }

  void AppendContextPrefix(const std::string sufix) {
    ctx_prefix_ = ctx_prefix_ + sufix;
  }

  // void IncCtxId() { ++ctx_id_; }

  // void IncVarId() { ++var_id_; }

  // void IncExprId() { ++expr_id_; }

  // void IncExprVarId() { ++expr_var_id_; }

  std::string TimeStampVar() const { return time_stamp; }

  std::string GraphVar() const { return graph_var; }

  void AddParameterVar(const codegen::ParamConst& var) {
    parameter_vars.emplace_back(var);
  }

  void AddExprCode(const std::string& code) { expr_code_.emplace_back(code); }

  const std::vector<std::string>& GetExprCode() const { return expr_code_; }

  std::string GetNextLambdaFuncName() {
    int32_t ret = lambda_func_id_;
    ++lambda_func_id_;
    return std::string("lambda") + std::to_string(ret);
  }

  const std::vector<codegen::ParamConst>& GetParameterVars() const {
    return parameter_vars;
  }

  std::string GetQueryClassName() const { return query_name_; }

  std::string GetQueryRet() const { return QUERY_FUNC_RETURN; }

  // get storage type
  StorageBackend GetStorageType() const { return storage_backend_; }

  // for input tag_id,
  // return -1 if tag_id == -1
  // return new asigned tag_ind if it doesn't appears before;
  // return the found tag_ind if it appears before;
  int32_t CreateOrGetTagInd(int tag_id) {
    return tag_ind_mapping_.CreateOrGetTagInd(tag_id);
  }

  int32_t GetTagInd(int32_t tag_id) const {
    return tag_ind_mapping_.GetTagInd(tag_id);
  }

  void UpdateTagIdAndIndMapping(const TagIndMapping& tag_ind_mapping) {
    tag_ind_mapping_ = tag_ind_mapping;
  }

  // get_tag_id_mapping
  const TagIndMapping& GetTagIdAndIndMapping() const {
    return tag_ind_mapping_;
  }

 private:
  StorageBackend storage_backend_;
  std::string query_name_;
  int32_t ctx_id_;
  int32_t var_id_;
  int32_t expr_id_;
  int32_t expr_var_id_;
  int32_t lambda_func_id_;
  std::string graph_interface_;
  std::string graph_header_;
  std::string app_base_header_;
  std::string ctx_prefix_;

  std::vector<codegen::ParamConst> parameter_vars;
  std::vector<std::string> expr_code_;
  TagIndMapping tag_ind_mapping_;
};

}  // namespace gs

#endif  // CODEGEN_SRC_BuildingContext_H_