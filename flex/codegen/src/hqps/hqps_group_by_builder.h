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
#ifndef CODEGEN_SRC_HQPS_HQPS_GROUP_BY_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_GROUP_BY_BUILDER_H_

#include <sstream>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/hqps/hqps_expr_builder.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

static constexpr const char* GROUP_KEY_TEMPLATE_STR =
    "GroupKey<%1%, %2%> %3%(%4%);\n";

static constexpr const char* GROUP_AGG_TEMPLATE_STR =
    "auto %1% = gs::make_aggregate_prop<%2%>(std::tuple{%3%}, "
    "std::integer_sequence<int32_t, %4%>{});\n";

static constexpr const char* GROUP_BY_OP_TEMPLATE_STR =
    "%1%\n"
    "%2%\n"
    "auto %3% = Engine::GroupBy(%4%, std::move(%5%), std::tuple{%6%}, "
    "std::tuple{%7%});\n";

std::string agg_func_pb_2_str(
    const physical::GroupBy::AggFunc::Aggregate& agg_func) {
  switch (agg_func) {
  case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_SUM:
    return "gs::AggFunc::SUM";
  case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_AVG:
    return "gs::AggFunc::AVG";
  case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_MAX:
    return "gs::AggFunc::MAX";
  case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_MIN:
    return "gs::AggFunc::MIN";
  case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_COUNT:
    return "gs::AggFunc::COUNT";
  case physical::GroupBy::AggFunc::Aggregate::
      GroupBy_AggFunc_Aggregate_COUNT_DISTINCT:
    return "gs::AggFunc::COUNT_DISTINCT";
  case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_TO_LIST:
    return "gs::AggFunc::TO_LIST";
  case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_TO_SET:
    return "gs::AggFunc::TO_SET";
  case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_FIRST:
    return "gs::AggFunc::FIRST";
  default:
    LOG(FATAL) << "Unsupported aggregate function";
    return "";
  }
}

std::pair<std::string, std::string> gen_agg_var_and_code(
    BuildingContext& ctx, TagIndMapping& new_mapping,
    const physical::GroupBy::AggFunc& agg_func) {
  auto agg_func_name = agg_func_pb_2_str(agg_func.aggregate());
  auto cur_var_name = ctx.GetNextAggFuncName();
  std::vector<int32_t> in_tags;
  std::vector<std::string> in_prop_names;
  std::vector<std::string> in_prop_types;
  new_mapping.CreateOrGetTagInd(agg_func.alias().value());
  auto& vars = agg_func.vars();
  for (int32_t i = 0; i < vars.size(); ++i) {
    auto& var = vars[i];
    int32_t raw_tag_id = -1;
    if (var.has_tag()) {
      raw_tag_id = var.tag().id();
    }
    if (raw_tag_id == -1) {
      in_tags.push_back(-1);
    } else if (ctx.GetTagIdAndIndMapping().HasTagId(raw_tag_id)) {
      in_tags.push_back(ctx.GetTagInd(raw_tag_id));
    } else if (raw_tag_id == ctx.GetTagIdAndIndMapping().GetMaxTagId() + 1) {
      in_tags.push_back(-1);
    } else {
      LOG(WARNING) << "tag id " << raw_tag_id << " not found in tag id mapping";
      in_tags.push_back(-1);
    }

    // in_tags.push_back(ctx.GetTagInd(raw_tag_id));
    if (var.has_property()) {
      auto var_prop = var.property();
      if (var_prop.item_case() == common::Property::kId) {
        // IdKey
        VLOG(10) << "aggregate on internal id";
        in_prop_names.push_back("None");
        in_prop_types.push_back(EMPTY_TYPE);
      } else {
        VLOG(10) << "aggregate on property " << var_prop.key().name();
        in_prop_names.push_back(var.property().key().name());
        in_prop_types.push_back(
            single_common_data_type_pb_2_str(var.node_type().data_type()));
      }
    } else {
      // var has no property, which means internal id.
      VLOG(10) << "aggregate on internal id";
      in_prop_names.push_back("None");
      in_prop_types.push_back(EMPTY_TYPE);
    }
  }

  std::string selectors_str, in_tags_str;
  {
    std::stringstream ss;
    for (size_t i = 0; i < in_prop_types.size(); ++i) {
      boost::format selector_formater(PROPERTY_SELECTOR);
      selector_formater % in_prop_types[i] % in_prop_names[i];
      ss << selector_formater.str();
      if (i != in_prop_types.size() - 1) {
        ss << ", ";
      }
    }
    selectors_str = ss.str();
  }
  {
    std::stringstream ss;
    for (size_t i = 0; i < in_tags.size(); ++i) {
      ss << in_tags[i];
      if (i != in_tags.size() - 1) {
        ss << ", ";
      }
    }
    in_tags_str = ss.str();
  }
  boost::format agg_formater(GROUP_AGG_TEMPLATE_STR);
  agg_formater % cur_var_name % agg_func_name % selectors_str % in_tags_str;
  return std::make_pair(cur_var_name, agg_formater.str());
}

class GroupByOpBuilder {
 public:
  GroupByOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  // add group key
  // after groupby, we will clear the previous context, thus we will restart the
  // counting of tag_ind
  GroupByOpBuilder& AddKeyAlias(const physical::GroupBy::KeyAlias& key_alias) {
    CHECK(key_alias.has_alias());
    // CHECK(key_alias.key().has_tag());
    std::string prop_name = "None";
    std::string prop_type;
    auto group_key_var_name = ctx_.GetNextGroupKeyName();

    // first in tag id, then alias id
    int32_t input_tag_id = -1;
    if (key_alias.key().has_tag()) {
      input_tag_id = ctx_.GetTagInd(key_alias.key().tag().id());
    }
    int32_t output_tag_id =
        new_tag_id_mapping.CreateOrGetTagInd(key_alias.alias().value());
    // output_col_id should equal to current key's length
    CHECK(output_tag_id == (int32_t) key_alias_name_and_code.size());
    // we currently assume group key is always on internal id or graph ele
    auto key_alias_key = key_alias.key();
    if (key_alias_key.has_property()) {
      auto& prop = key_alias.key().property();
      if (prop.item_case() == common::Property::kId) {
        VLOG(10) << "Group on " << key_alias.key().tag().id()
                 << ", inner id id";
        prop_type = EMPTY_TYPE;
      } else if (prop.item_case() == common::Property::kKey) {
        auto& prop_key = prop.key();
        prop_name = prop_key.name();
        prop_type = single_common_data_type_pb_2_str(
            key_alias_key.node_type().data_type());
      } else if (prop.item_case() == common::Property::kLabel) {
        prop_type = "LabelKey";
      } else {
        LOG(FATAL) << "Current only support key_alias on internal id or "
                      "property, but got: "
                   << prop.DebugString();
      }
    } else {
      VLOG(10) << "Apply internal id since no property provided";
      prop_type = EMPTY_TYPE;
    }

    std::string property_selector_str;
    {
      boost::format property_selector_fmt(PROPERTY_SELECTOR);
      property_selector_fmt % prop_type % prop_name;
      property_selector_str = property_selector_fmt.str();
    }

    boost::format formater(GROUP_KEY_TEMPLATE_STR);
    formater % input_tag_id % prop_type % group_key_var_name %
        property_selector_str;

    key_alias_name_and_code.emplace_back(group_key_var_name, formater.str());
    return *this;
  }

  // add aggregation function
  GroupByOpBuilder& AddAggFunc(const physical::GroupBy::AggFunc& agg_func) {
    // agg function can apply on multiple tag's prop
    std::string agg_fun_var_name, agg_fun_code;
    std::tie(agg_fun_var_name, agg_fun_code) =
        gen_agg_var_and_code(ctx_, new_tag_id_mapping, agg_func);
    agg_func_name_and_code.emplace_back(agg_fun_var_name, agg_fun_code);
    return *this;
  }

  // return at least one key, at least one agg func
  // and the operator code.
  std::string Build() const {
    CHECK(key_alias_name_and_code.size() > 0);
    CHECK(agg_func_name_and_code.size() > 0);

    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();

    std::string key_alias_con_str;
    std::string agg_func_con_str;
    std::string group_by_keys_vars_str;
    std::string group_by_agg_vars_str;
    {
      std::stringstream ss;
      for (size_t i = 0; i < key_alias_name_and_code.size(); ++i) {
        ss << key_alias_name_and_code[i].second << std::endl;
      }
      key_alias_con_str = ss.str();
    }
    {
      std::stringstream ss;
      for (size_t i = 0; i < agg_func_name_and_code.size(); ++i) {
        ss << agg_func_name_and_code[i].second << std::endl;
      }
      agg_func_con_str = ss.str();
    }
    for (size_t i = 0; i < key_alias_name_and_code.size(); ++i) {
      group_by_keys_vars_str += key_alias_name_and_code[i].first;
      if (i != key_alias_name_and_code.size() - 1) {
        group_by_keys_vars_str += ", ";
      }
    }
    for (size_t i = 0; i < agg_func_name_and_code.size(); ++i) {
      group_by_agg_vars_str += agg_func_name_and_code[i].first;
      if (i != agg_func_name_and_code.size() - 1) {
        group_by_agg_vars_str += ", ";
      }
    }
    boost::format formater(GROUP_BY_OP_TEMPLATE_STR);
    formater % key_alias_con_str % agg_func_con_str % next_ctx_name %
        ctx_.GraphVar() % prev_ctx_name % group_by_keys_vars_str %
        group_by_agg_vars_str;

    // it is safe to update tag_id_mapping here
    ctx_.UpdateTagIdAndIndMapping(new_tag_id_mapping);
    return formater.str();
  }

 private:
  BuildingContext& ctx_;
  std::vector<std::pair<std::string, std::string>> key_alias_name_and_code;
  std::vector<std::pair<std::string, std::string>> agg_func_name_and_code;
  TagIndMapping new_tag_id_mapping;  // only update when build is over.
};

static std::string BuildGroupByOp(
    BuildingContext& ctx, const physical::GroupBy& group_by_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  GroupByOpBuilder builder(ctx);
  auto& key_aliases = group_by_pb.mappings();

  CHECK(group_by_pb.functions_size() >= 1);
  auto& functions = group_by_pb.functions();
  for (int32_t i = 0; i < key_aliases.size(); ++i) {
    auto& key_alias = key_aliases[i];
    builder.AddKeyAlias(key_alias);
  }

  for (int32_t i = 0; i < functions.size(); ++i) {
    auto& func = functions[i];
    builder.AddAggFunc(func);
  }
  return builder.Build();
}
}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_GROUP_BY_BUILDER_H_