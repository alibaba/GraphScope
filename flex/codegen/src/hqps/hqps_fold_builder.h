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
#ifndef CODEGEN_SRC_HQPS_HQPS_FOLD_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_FOLD_BUILDER_H_

#include <sstream>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/hqps/hqps_expr_builder.h"
#include "flex/codegen/src/hqps/hqps_group_by_builder.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

static constexpr const char* AGG_FUNC_TEMPLATE_STR =
    "auto %1% = gs::make_aggregate_prop<%2%>(std::tuple{%3%}, "
    "std::integer_sequence<int32_t, %4%>{});\n";

static constexpr const char* FOLD_OP_TEMPLATE_STR =
    "auto %2% = Engine::GroupByWithoutKey(%3%, std::move(%4%), "
    "std::tuple{%1%});\n";

std::pair<std::string, std::string> gen_agg_var_and_code_for_fold(
    BuildingContext& ctx, const physical::GroupBy::AggFunc& agg_func,
    TagIndMapping& tag_ind_mapping) {
  auto agg_func_name = agg_func_pb_2_str(agg_func.aggregate());
  auto cur_var_name = ctx.GetNextAggFuncName();
  std::vector<int32_t> in_tags;
  std::vector<std::string> in_prop_names;
  std::vector<std::string> in_prop_types;
  tag_ind_mapping.CreateOrGetTagInd(agg_func.alias().value());
  auto& vars = agg_func.vars();
  for (int32_t i = 0; i < vars.size(); ++i) {
    auto& var = vars[i];
    VLOG(10) << "var " << i << " " << var.DebugString();
    int32_t raw_tag_id;
    if (var.has_tag()) {
      raw_tag_id = var.tag().id();
    } else {
      raw_tag_id = -1;
    }

    if (raw_tag_id == -1) {
      in_tags.push_back(-1);
    } else if (ctx.GetTagIdAndIndMapping().HasTagId(raw_tag_id)) {
      in_tags.push_back(ctx.GetTagInd(raw_tag_id));
    } else if (raw_tag_id == ctx.GetTagIdAndIndMapping().GetMaxTagId() + 1) {
      in_tags.push_back(-1);
    } else {
      LOG(WARNING) << "tag id " << raw_tag_id << " not found in tag id mapping";
    }
    VLOG(10) << "var " << i << " tag id " << raw_tag_id << " real tag id "
             << in_tags[i];
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
  CHECK(in_prop_names.size() > 0);
  std::string selectors_str, in_tags_str;
  {
    std::stringstream ss;
    for (size_t i = 0; i < in_prop_types.size(); ++i) {
      boost::format selector_formatter(PROPERTY_SELECTOR);
      selector_formatter % in_prop_types[i] % in_prop_names[i];
      ss << selector_formatter.str();
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

  boost::format formatter(AGG_FUNC_TEMPLATE_STR);
  formatter % cur_var_name % agg_func_name % selectors_str % in_tags_str;
  return std::make_pair(cur_var_name, formatter.str());
}

// i.e. group without key.
class FoldOpBuilder {
 public:
  FoldOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  FoldOpBuilder& AddAggFunc(const physical::GroupBy::AggFunc& agg_func) {
    std::string agg_fun_var_name, agg_fun_code;
    std::tie(agg_fun_var_name, agg_fun_code) =
        gen_agg_var_and_code_for_fold(ctx_, agg_func, new_tag_id_mapping_);
    agg_func_name_and_code.emplace_back(agg_fun_var_name, agg_fun_code);
    return *this;
  }

  std::string Build() const {
    CHECK(agg_func_name_and_code.size() > 0);

    std::string fold_ops_code;
    {
      std::stringstream ss;
      for (size_t i = 0; i < agg_func_name_and_code.size(); ++i) {
        ss << make_move(agg_func_name_and_code[i].first);
        if (i != agg_func_name_and_code.size() - 1) {
          ss << ", ";
        }
      }
      fold_ops_code = ss.str();
    }

    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();

    std::string agg_func_code_con;
    {
      std::stringstream ss;
      for (size_t i = 0; i < agg_func_name_and_code.size(); ++i) {
        ss << agg_func_name_and_code[i].second << std::endl;
      }
      agg_func_code_con = ss.str();
    }

    boost::format formatter(FOLD_OP_TEMPLATE_STR);
    formatter % fold_ops_code % next_ctx_name % ctx_.GraphVar() % prev_ctx_name;
    ctx_.UpdateTagIdAndIndMapping(new_tag_id_mapping_);
    return agg_func_code_con + formatter.str();
  }

 private:
  BuildingContext& ctx_;
  std::vector<std::pair<std::string, std::string>> agg_func_name_and_code;
  // fold remove previous columns, use a new TagIdMapping
  TagIndMapping new_tag_id_mapping_;
};

static std::string BuildGroupWithoutKeyOp(
    BuildingContext& ctx, const physical::GroupBy& group_by_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  CHECK(group_by_pb.mappings_size() == 0);
  FoldOpBuilder fold_op_builder(ctx);
  for (int32_t i = 0; i < group_by_pb.functions_size(); ++i) {
    fold_op_builder.AddAggFunc(group_by_pb.functions(i));
  }
  return fold_op_builder.Build();
}
}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_FOLD_BUILDER_H_