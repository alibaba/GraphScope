#ifndef GROUP_BY_BUILDER_H
#define GROUP_BY_BUILDER_H

#include <sstream>
#include <string>
#include <vector>

#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/op_builder/expr_builder.h"
#include "flex/codegen/pb_parser/query_params_parser.h"
#include "flex/codegen/codegen_utils.h"

namespace gs {

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
  default:
    LOG(FATAL) << "Unsupported aggregate function";
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
  int32_t res_alias = agg_func.alias().value();
  auto real_res_alias = new_mapping.CreateOrGetTagInd(res_alias);
  auto& vars = agg_func.vars();
  for (auto i = 0; i < vars.size(); ++i) {
    auto& var = vars[i];
    auto raw_tag_id = var.tag().id();
    in_tags.push_back(ctx.GetTagInd(raw_tag_id));
    LOG(INFO) << "var " << i << " tag id " << raw_tag_id << " real tag id "
              << in_tags[i];
    if (var.has_property()) {
      auto var_prop = var.property();
      if (var_prop.item_case() == common::Property::kId) {
        // IdKey
        LOG(INFO) << "aggregate on internal id";
        in_prop_names.push_back("None");
        in_prop_types.push_back(EMPTY_TYPE);
      } else {
        LOG(INFO) << "aggregate on property " << var_prop.key().name();
        in_prop_names.push_back(var.property().key().name());
        in_prop_types.push_back(
            common_data_type_pb_2_str(var.node_type().data_type()));
      }
    } else {
      // var has no property, which means internal id.
      LOG(INFO) << "aggregate on internal id";
      in_prop_names.push_back("None");
      in_prop_types.push_back(EMPTY_TYPE);
    }
  }
  std::stringstream ss;
  ss << "auto " << cur_var_name << " = " << make_agg_prop_name;
  ss << "<";
  ss << real_res_alias << ", ";
  ss << agg_func_name << ", ";
  CHECK(in_prop_names.size() > 0);
  for (auto i = 0; i < in_prop_types.size() - 1; ++i) {
    ss << in_prop_types[i] << ", ";
  }
  ss << in_prop_types[in_prop_types.size() - 1];
  ss << ">";
  ss << "(";
  {
    // propnames
    ss << "{";
    for (auto i = 0; i < in_prop_names.size() - 1; ++i) {
      ss << add_quote(in_prop_names[i]) << ", ";
    }
    ss << add_quote(in_prop_names[in_prop_names.size() - 1]);
    ss << "}, ";
  }
  {
    // input tags
    ss << "std::integer_sequence<int32_t, ";
    for (auto i = 0; i < in_tags.size() - 1; ++i) {
      ss << in_tags[i] << ", ";
    }
    ss << in_tags[in_tags.size() - 1];
    ss << ">{}";
  }
  ss << ");" << std::endl;
  return std::make_pair(cur_var_name, ss.str());
}

class GroupByOpBuilder {
 public:
  GroupByOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  // add group key
  // after groupby, we will clear the previous context, thus we will restart the
  // counting of tag_ind
  GroupByOpBuilder& AddKeyAlias(const physical::GroupBy::KeyAlias& key_alias) {
    std::stringstream ss;
    ss << group_key_class_name;
    ss << "<";
    CHECK(key_alias.has_alias());
    CHECK(key_alias.key().has_tag());
    std::string prop_name = "None";

    // first in tag id, then alias id
    ss << ctx_.GetTagInd(key_alias.key().tag().id()) << ",";
    ss << new_tag_id_mapping.CreateOrGetTagInd(key_alias.alias().value())
       << ",";
    // we currently assume group key is always on internal id or graph ele
    auto key_alias_key = key_alias.key();
    if (key_alias_key.has_property()) {
      auto& prop = key_alias.key().property();
      if (prop.item_case() == common::Property::kId) {
        LOG(INFO) << "Group on " << key_alias.key().tag().id()
                  << ", inner id id";
        ss << EMPTY_TYPE;
        // } else if (prop.has_key()) {
      } else if (prop.item_case() == common::Property::kKey) {
        auto& prop_key = prop.key();
        prop_name = prop_key.name();
        auto prop_type =
            common_data_type_pb_2_str(key_alias_key.node_type().data_type());
        ss << prop_type;
      } else {
        LOG(FATAL)
            << "Current only support key_alias on internal id or property";
      }
    } else {
      LOG(INFO) << "Apply internal id since no property provided";
      ss << EMPTY_TYPE;
    }

    ss << "> ";

    auto group_key_var_name = ctx_.GetNextGroupKeyName();
    ss << group_key_var_name << "({";
    ss << with_quote(prop_name);
    ss << "});" << std::endl;
    key_alias_name_and_code.emplace_back(group_key_var_name, ss.str());
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
  std::vector<std::string> Build() const {
    CHECK(key_alias_name_and_code.size() > 0);
    CHECK(agg_func_name_and_code.size() > 0);

    std::string group_opt_var_name = ctx_.GetNextGroupOptName();
    std::string group_opt_code;
    {
      std::stringstream ss;
      ss << "auto " << group_opt_var_name << " = "
         << "gs::make_group_opt(";
      for (auto i = 0; i < key_alias_name_and_code.size(); ++i) {
        ss << "std::move(" << key_alias_name_and_code[i].first << "), ";
      }
      for (auto i = 0; i < agg_func_name_and_code.size() - 1; ++i) {
        ss << "std::move(" << agg_func_name_and_code[i].first << "), ";
      }
      ss << "std::move("
         << agg_func_name_and_code[agg_func_name_and_code.size() - 1].first
         << ")";
      ss << ");" << std::endl;
      group_opt_code = ss.str();
    }
    LOG(INFO) << "group_opt_code: " << group_opt_code;

    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();

    std::string cur_ctx_code;
    {
      std::stringstream ss;
      ss << "auto " << next_ctx_name << " = Engine::GroupBy";
      ss << "(";
      ss << ctx_.TimeStampVar() << ", ";
      ss << ctx_.GraphVar() << ", ";
      ss << "std::move(" << prev_ctx_name << "), ";
      ss << "std::move(" << group_opt_var_name << ")";
      ss << ");" << std::endl;
      cur_ctx_code = ss.str();
    }
    std::vector<std::string> res;
    for (auto i = 0; i < key_alias_name_and_code.size(); ++i) {
      res.push_back(key_alias_name_and_code[i].second);
    }
    for (auto i = 0; i < agg_func_name_and_code.size(); ++i) {
      res.push_back(agg_func_name_and_code[i].second);
    }
    res.push_back(group_opt_code);
    res.push_back(cur_ctx_code);
    // it is safe to update tag_id_mapping here
    ctx_.UpdateTagIdAndIndMapping(new_tag_id_mapping);
    return res;
  }

 private:
  BuildingContext& ctx_;
  std::vector<std::pair<std::string, std::string>> key_alias_name_and_code;
  std::vector<std::pair<std::string, std::string>> agg_func_name_and_code;
  TagIndMapping new_tag_id_mapping;  // only update when build is over.
};

static std::vector<std::string> BuildGroupByOp(
    BuildingContext& ctx, const physical::GroupBy& group_by_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  GroupByOpBuilder builder(ctx);
  CHECK(group_by_pb.mappings_size() == 1)
      << "Currently we only support one key";
  auto& key_aliases = group_by_pb.mappings();

  CHECK(group_by_pb.functions_size() >= 1);
  auto& functions = group_by_pb.functions();
  for (auto i = 0; i < key_aliases.size(); ++i) {
    auto& key_alias = key_aliases[i];
    builder.AddKeyAlias(key_alias);
  }

  for (auto i = 0; i < functions.size(); ++i) {
    auto& func = functions[i];
    builder.AddAggFunc(func);
  }
  return builder.Build();
}
}  // namespace gs

#endif  // GROUP_BY_BUILDER_H