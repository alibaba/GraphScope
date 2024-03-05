/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#ifndef CODEGEN_SRC_BUILDING_CONTEXT_H_
#define CODEGEN_SRC_BUILDING_CONTEXT_H_

#include <map>
#include <vector>

#include "flex/codegen/src/graph_types.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "glog/logging.h"

namespace gs {
static constexpr const char* time_stamp = "time_stamp";
static constexpr const char* graph_var = "graph";
static constexpr const char* GRAPE_INTERFACE_CLASS = "gs::MutableCSRInterface";
static constexpr const char* GRAPE_INTERFACE_HEADER =
    "flex/engines/hqps_db/database/mutable_csr_interface.h";
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
static constexpr const char* MAPPER_NAME = "mapper";
static constexpr const char* APP_BASE_HEADER =
    "flex/engines/apps/cypher_app_base.h";
static constexpr const char* APP_BASE_CLASS_NAME = "AppBase";
static constexpr const char* QUERY_FUNC_RETURN = "results::CollectiveResults";

enum class StorageBackend {
  kGrape = 0,
};

std::string storage_backend_to_string(StorageBackend storage_backend) {
  switch (storage_backend) {
  case StorageBackend::kGrape:
    return "gs::GraphStoreType::Grape";
  default:
    throw std::runtime_error("unsupported storage backend");
  }
}

// manages the mapping between tag ids and tag inds.
struct TagIndMapping {
  int32_t GetTagInd(int32_t tag_id) const {
    if (tag_id == -1) {
      return -1;
    }
    print_debug_info();
    CHECK(tag_id < (int) tag_id_2_tag_inds_.size())
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
      tag_ind_2_tag_ids_.emplace_back(tag_id);
      auto old_size = (int32_t) tag_id_2_tag_inds_.size();
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
    CHECK(max_ind + 1 == (int32_t) tag_ind_2_tag_ids_.size());
    CHECK(max_tag_id + 1 == (int32_t) tag_id_2_tag_inds_.size());
  }

  void print_debug_info() const {
    VLOG(10) << "tag id to inds : " << gs::to_string(tag_id_2_tag_inds_);
    VLOG(10) << "tag ind to tag ids: " << gs::to_string(tag_ind_2_tag_ids_);
  }

  const std::vector<int32_t>& GetTagInd2TagIds() const {
    return tag_ind_2_tag_ids_;
  }

  const std::vector<int32_t>& GetTagId2TagInds() const {
    return tag_id_2_tag_inds_;
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
        query_name_(query_name),
        ctx_id_(0),
        var_id_(0),
        expr_id_(0),
        expr_var_id_(0),
        mapper_var_id_(0),
        lambda_func_id_(0),
        ctx_prefix_(ctx_prefix),
        app_base_header_(APP_BASE_HEADER),
        alias_size_(0) {
    if (storage_type == StorageBackend::kGrape) {
      graph_header_ = GRAPE_INTERFACE_HEADER;
      graph_interface_ = GRAPE_INTERFACE_CLASS;
    } else {
      LOG(FATAL) << "unsupported storage backend";
    }
  }

  BuildingContext(std::string graph_interface, std::string graph_header,
                  StorageBackend storage_type = StorageBackend::kGrape,
                  std::string query_name = "Query0",
                  std::string ctx_prefix = "")
      : storage_backend_(storage_type),
        query_name_(query_name),
        ctx_id_(0),
        var_id_(0),
        expr_id_(0),
        expr_var_id_(0),
        mapper_var_id_(0),
        lambda_func_id_(0),
        ctx_prefix_(ctx_prefix),
        app_base_header_(APP_BASE_HEADER),
        graph_interface_(graph_interface),
        graph_header_(graph_header),
        alias_size_(0) {}

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

  std::string GetNextMapperName() {
    return ctx_prefix_ + MAPPER_NAME + std::to_string(mapper_var_id_++);
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
    ctx.mapper_var_id_ = mapper_var_id_;
    ctx.graph_interface_ = graph_interface_;
    ctx.app_base_header_ = app_base_header_;
    ctx.graph_header_ = graph_header_;
    ctx.ctx_prefix_ = ctx_prefix_ + sufix;
    ctx.tag_ind_mapping_ = tag_ind_mapping_;

    ctx.contain_head_ = contain_head_;
    ctx.head_type_ = head_type_;
    ctx.alias_size_ = alias_size_;
    ctx.tag_index_ = tag_index_;
    ctx.tag_type_ = tag_type_;
    ctx.cur_outputs_ = cur_outputs_;
    ctx.vertex_properties_set_ = vertex_properties_set_;

    return ctx;
  }

  void MergeSubTaskContext(BuildingContext& ctx) {
    contain_head_ = ctx.contain_head_;
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
    parameter_vars_.emplace_back(var);
  }

  void AddExprCode(const std::string& code) { expr_code_.emplace_back(code); }

  const std::vector<std::string>& GetExprCode() const { return expr_code_; }

  std::string GetNextLambdaFuncName() {
    int32_t ret = lambda_func_id_;
    ++lambda_func_id_;
    return std::string("lambda") + std::to_string(ret);
  }

  const std::vector<codegen::ParamConst>& GetParameterVars() const {
    return parameter_vars_;
  }

  std::string GetQueryClassName() const { return query_name_; }

  std::string GetQueryRet() const { return QUERY_FUNC_RETURN; }

  // get storage type
  StorageBackend GetStorageType() const { return storage_backend_; }

  // for input tag_id,
  // return -1 if tag_id == -1
  // return new assigned tag_ind if it doesn't appears before;
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

  void SetHead(bool contain_head) { contain_head_ = contain_head; }

  bool ContainHead() const { return contain_head_; }

  void SetHeadType(int32_t data_type, std::vector<int32_t> label_list) {
    head_type_ = std::make_pair(data_type, label_list);
  };

  const std::pair<int32_t, std::vector<int32_t>>& GetHeadType() const {
    return head_type_;
  }

  void SetAliasType(int32_t alias, int32_t type,
                    std::vector<int32_t>& label_list) {
    auto index = tag_index_[alias];
    if ((int32_t) tag_type_.size() <= index) {
      tag_type_.resize(index + 1);
    }
    tag_type_[index].first = type;
    tag_type_[index].second = label_list;
  }

  const std::pair<int32_t, std::vector<int32_t>>& GetAliasType(
      int32_t alias) const {
    int32_t index = tag_index_[alias];
    return tag_type_[index];
  }

  int32_t SetAlias(int32_t cur_alias) {
    if (cur_alias >= (int32_t) tag_index_.size()) {
      tag_index_.resize(cur_alias + 1, -1);
    }
    if (tag_index_[cur_alias] != -1) {
      return tag_index_[cur_alias];
    } else {
      tag_index_[cur_alias] = alias_size_;
      alias_size_++;
      return alias_size_ - 1;
    }
  }

  void ResetAlias() {
    tag_index_.resize(0);
    tag_type_.resize(0);
    cur_outputs_.resize(0);
    alias_size_ = 0;
  }

  int32_t GetAliasIndex(int32_t alias) const {
    if (contain_head_) {
      return tag_index_[alias] + 1;
    } else {
      return tag_index_[alias];
    }
  }

  int32_t AliasSize() const { return alias_size_; }

  int32_t InputSize() const {
    if (contain_head_) {
      return alias_size_ + 1;
    } else {
      return alias_size_;
    }
  }

  void SetOutput(size_t index, std::vector<codegen::DataType>& output) {
    if (cur_outputs_.size() <= index) {
      cur_outputs_.resize(index + 1);
    }
    cur_outputs_[index] = output;
  }

  const std::vector<std::vector<codegen::DataType>>& GetOutput() {
    return cur_outputs_;
  }

  void AddVertexProperty(int32_t vertex_label,
                         const codegen::ParamConst& property) {
    vertex_properties_set_[vertex_label].push_back(property);
  }

  const std::unordered_map<int32_t, std::vector<codegen::ParamConst>>&
  GetVertexProperties() {
    return vertex_properties_set_;
  }

 private:
  StorageBackend storage_backend_;
  std::string query_name_;
  int32_t ctx_id_;
  int32_t var_id_;
  int32_t expr_id_;
  int32_t expr_var_id_;
  int32_t mapper_var_id_;
  int32_t lambda_func_id_;
  std::string ctx_prefix_;
  std::string app_base_header_;
  std::string graph_interface_;
  std::string graph_header_;

  std::vector<codegen::ParamConst> parameter_vars_;
  std::vector<std::string> expr_code_;
  TagIndMapping tag_ind_mapping_;

  bool contain_head_;
  std::pair<int32_t, std::vector<int32_t>> head_type_;
  int32_t alias_size_;
  std::vector<int32_t> tag_index_;
  std::vector<std::pair<int32_t, std::vector<int32_t>>> tag_type_;
  std::vector<std::vector<codegen::DataType>> cur_outputs_;
  std::unordered_map<int32_t, std::vector<codegen::ParamConst>>
      vertex_properties_set_;
};

}  // namespace gs

#endif  // CODEGEN_SRC_BUILDING_CONTEXT_H_