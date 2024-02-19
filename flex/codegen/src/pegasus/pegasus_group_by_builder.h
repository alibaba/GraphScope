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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_GROUP_BY_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_GROUP_BY_BUILDER_H_

#include "flex/codegen/src/building_context.h"

namespace gs {
namespace pegasus {
class GroupByOpBuilder {
 public:
  GroupByOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  GroupByOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  // add group key
  GroupByOpBuilder& AddKeyAlias(const physical::GroupBy::KeyAlias& key_alias) {
    int32_t input_tag = key_alias.key().tag().id();
    int32_t output_tag = key_alias.alias().value();
    codegen::ParamConst param_const;
    if (key_alias.key().has_property()) {
      param_const.var_name = key_alias.key().property().key().name();
      param_const.type = common_data_type_pb_2_data_type(
          key_alias.key().node_type().data_type());
    } else {
      param_const.var_name = "";
    }
    key_input_tag_.emplace_back(input_tag);
    key_output_tag_.emplace_back(output_tag);
    key_input_type_.emplace_back(param_const);
    return *this;
  }

  // add aggregation function
  GroupByOpBuilder& AddAggFunc(const physical::GroupBy::AggFunc& agg_func) {
    // agg function can apply on multiple tag's prop
    std::string agg_fun_var_name, agg_fun_code;
    agg_func_list_.emplace_back(agg_func.aggregate());
    std::vector<common::Variable> var_tags;
    for (int32_t i = 0; i < agg_func.vars_size(); i++) {
      var_tags.emplace_back(agg_func.vars(i));
    }
    group_input_vars_.emplace_back(var_tags);
    group_output_tag_.emplace_back(agg_func.alias().value());
    return *this;
  }

  GroupByOpBuilder& MetaDatas(
      const std::vector<physical::PhysicalOpr::MetaData> meta_datas) {
    meta_datas_ = meta_datas;
    return *this;
  }

  // return at least one key, at least one agg func
  // and the operator code.
  std::string Build() const {
    VLOG(10) << "[GroupBy Builder] Start build groupby operator";
    VLOG(10) << "[GroupBy Builder] Start build key_by operator";
    std::string key_by_code = write_key_by_operator();

    VLOG(10) << "[GroupBy Builder] Start build fold_by_key operator";
    std::string fold_by_key_code = write_fold_by_operator();

    VLOG(10) << "[GroupBy Builder] Start build unfold operator";
    std::string unfold_code = write_unfold_operator();

    // update output info
    VLOG(10) << "[GroupBy Builder] Start update output info";
    VLOG(10) << "key size " << key_output_tag_.size() << ", meta size "
             << meta_datas_.size();
    ctx_.SetHead(false);
    ctx_.ResetAlias();
    for (size_t i = 0; i < key_output_tag_.size(); ++i) {
      int32_t key_output = key_output_tag_[i];
      ctx_.SetAlias(key_output);
      int32_t key_index = ctx_.GetAliasIndex(key_output);
      std::vector<codegen::DataType> output_type;
      auto column_meta = meta_datas_[i];
      std::vector<int32_t> labels;
      if (column_meta.type().has_graph_type()) {
        for (auto j = 0;
             j < column_meta.type().graph_type().graph_data_type_size(); j++) {
          labels.push_back(column_meta.type()
                               .graph_type()
                               .graph_data_type(j)
                               .label()
                               .label());
        }
        if (column_meta.type().graph_type().element_opt() ==
            common::GraphDataType_GraphElementOpt::
                GraphDataType_GraphElementOpt_VERTEX) {
          ctx_.SetAliasType(key_output, 0, labels);
        } else {
          ctx_.SetAliasType(key_output, 1, labels);
        }
        output_type.push_back(codegen::DataType::kInt64);
      } else if (column_meta.type().type_case() ==
                 common::IrDataType::kGraphType) {
        ctx_.SetAliasType(key_output, 2, labels);
        output_type.push_back(
            common_data_type_pb_2_data_type(column_meta.type().data_type()));
      }
      ctx_.SetOutput(key_index, output_type);
    }

    for (size_t i = 0; i < group_output_tag_.size(); ++i) {
      int32_t key_output = group_output_tag_[i];
      ctx_.SetAlias(key_output);
      int32_t key_index = ctx_.GetAliasIndex(key_output);
      std::vector<codegen::DataType> output_type;
      auto column_meta = meta_datas_[i + key_output_tag_.size()];
      std::vector<int32_t> labels;
      if (column_meta.type().has_graph_type()) {
        for (auto j = 0;
             j < column_meta.type().graph_type().graph_data_type_size(); j++) {
          labels.push_back(column_meta.type()
                               .graph_type()
                               .graph_data_type(j)
                               .label()
                               .label());
        }
        if (column_meta.type().graph_type().element_opt() ==
            common::GraphDataType_GraphElementOpt::
                GraphDataType_GraphElementOpt_VERTEX) {
          ctx_.SetAliasType(key_output, 0, labels);
        } else {
          ctx_.SetAliasType(key_output, 1, labels);
        }
        output_type.push_back(codegen::DataType::kInt64);
      } else if (column_meta.type().type_case() ==
                 common::IrDataType::kDataType) {
        ctx_.SetAliasType(key_output, 2, labels);
        output_type.push_back(
            common_data_type_pb_2_data_type(column_meta.type().data_type()));
      }
      ctx_.SetOutput(key_index, output_type);
    }

    return key_by_code + fold_by_key_code + unfold_code;
  }

 private:
  std::string write_key_by_operator() const {
    // codegen for key_by
    boost::format key_by_head_fmter(
        "let stream_%1% = stream_%2%.key_by(|%3%| {\n");
    auto input_size = ctx_.InputSize();
    std::string key_by_input = generate_arg_list("i", input_size);
    key_by_head_fmter % operator_index_ % (operator_index_ - 1) % key_by_input;

    VLOG(10) << "[GroupBy Builder] Key input size is " << key_input_tag_.size();
    std::string key_by_key_code;
    for (size_t i = 0; i < key_input_tag_.size(); ++i) {
      auto in_tag = key_input_tag_[i];
      auto input_index = ctx_.GetAliasIndex(in_tag);
      boost::format key_fmter("let key%1% = i%2%;\n");
      key_fmter % i % input_index;
      key_by_key_code += key_fmter.str();
    }
    VLOG(10) << "[GroupBy Builder] Finished write key";

    std::string key_by_value_code;
    for (size_t i = 0; i < group_input_vars_.size(); ++i) {
      // Only support value with one column
      CHECK(group_input_vars_[i].size() == 1);
      auto in_tag = group_input_vars_[i][0].tag().id();
      auto input_index = ctx_.GetAliasIndex(in_tag);
      boost::format value_fmter("let value%1% = i%2%;\n");
      value_fmter % i % input_index;
      key_by_value_code += value_fmter.str();
    }
    VLOG(10) << "[GroupBy Builder] Finished write value";

    boost::format key_by_end_fmter("Ok((%1%, %2%))\n})?\n");
    std::string key_list = generate_arg_list("key", key_input_tag_.size());
    std::string value_list =
        generate_arg_list("value", group_input_vars_.size());
    key_by_end_fmter % key_list % value_list;

    return key_by_head_fmter.str() + key_by_key_code + key_by_value_code +
           key_by_end_fmter.str();
  }

  std::string write_fold_by_operator() const {
    boost::format fold_by_head_fmter(".fold_by_key(%1%, || |%2%, %3%|{\n");
    std::stringstream fold_by_init_ss;
    if (agg_func_list_.size() > 1) {
      fold_by_init_ss << "(";
    }
    for (size_t i = 0; i < agg_func_list_.size(); ++i) {
      switch (agg_func_list_[i]) {
      case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_SUM:
      case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_AVG:
      case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_MAX:
      case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_MIN:
      case physical::GroupBy::AggFunc::Aggregate::
          GroupBy_AggFunc_Aggregate_COUNT: {
        fold_by_init_ss << "0";
        break;
      }
      case physical::GroupBy::AggFunc::Aggregate::
          GroupBy_AggFunc_Aggregate_COUNT_DISTINCT: {
        fold_by_init_ss << "HashSet::new()";
        break;
      }
      case physical::GroupBy::AggFunc::Aggregate::
          GroupBy_AggFunc_Aggregate_TO_LIST: {
        fold_by_init_ss << "vec![]";
        break;
      }
      default:
        LOG(FATAL) << "Unsupported aggregate function";
      }
      if (i < agg_func_list_.size() - 1) {
        fold_by_init_ss << ", ";
      }
    }
    if (agg_func_list_.size() > 1) {
      fold_by_init_ss << ")";
    }
    std::string fold_by_init = fold_by_init_ss.str();
    std::string agg_params =
        generate_arg_list("mut agg", agg_func_list_.size());
    std::string input_params = generate_arg_list("i", agg_func_list_.size());
    fold_by_head_fmter % fold_by_init % agg_params % input_params;

    std::stringstream agg_func_ss;
    for (size_t i = 0; i < agg_func_list_.size(); ++i) {
      switch (agg_func_list_[i]) {
      case physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_SUM:
      case physical::GroupBy::AggFunc::Aggregate::
          GroupBy_AggFunc_Aggregate_AVG: {
        boost::format sum_fmter("agg%1% += i%1%;\n");
        sum_fmter % i;
        agg_func_ss << sum_fmter.str();
        break;
      }
      case physical::GroupBy::AggFunc::Aggregate::
          GroupBy_AggFunc_Aggregate_MAX: {
        boost::format max_fmter("agg%1% = max(agg%1%, i%1%);\n");
        max_fmter % i;
        agg_func_ss << max_fmter.str();
        break;
      }
      case physical::GroupBy::AggFunc::Aggregate::
          GroupBy_AggFunc_Aggregate_MIN: {
        boost::format min_fmter("agg%1% = max(agg%1%, i%1%);\n");
        min_fmter % i;
        agg_func_ss << min_fmter.str();
        break;
      }
      case physical::GroupBy::AggFunc::Aggregate::
          GroupBy_AggFunc_Aggregate_COUNT: {
        boost::format count_fmter("agg%1% += 1;\n");
        count_fmter % i;
        agg_func_ss << count_fmter.str();
        break;
      }
      case physical::GroupBy::AggFunc::Aggregate::
          GroupBy_AggFunc_Aggregate_COUNT_DISTINCT: {
        boost::format count_distinct_fmter("agg%1%.insert(i%1%);\n");
        count_distinct_fmter % i;
        agg_func_ss << count_distinct_fmter.str();
        break;
      }
      case physical::GroupBy::AggFunc::Aggregate::
          GroupBy_AggFunc_Aggregate_TO_LIST: {
        boost::format to_list_fmter("agg%1%.append(i%1%);\n");
        to_list_fmter % i;
        agg_func_ss << to_list_fmter.str();
        break;
      }
      default:
        LOG(FATAL) << "Unsupported aggregate function";
      }
    }
    std::string agg_func_code = agg_func_ss.str();

    boost::format fold_by_end_fmter("Ok(%1%)\n})?\n");
    std::string fold_by_output =
        generate_arg_list("agg", agg_func_list_.size());
    fold_by_end_fmter % fold_by_output;

    return fold_by_head_fmter.str() + agg_func_code + fold_by_end_fmter.str();
  }

  std::string write_unfold_operator() const {
    boost::format unfold_fmter(
        ".unfold(|group_map|{\n"
        "Ok(group_map.into_iter().map(|(key, value)| (%1%%2%)))\n"
        "})?;ß\n");
    std::string key_outputs;
    if (key_output_tag_.size() == 1) {
      key_outputs = "key, ";
    } else {
      for (size_t i = 0; i < key_output_tag_.size(); i++) {
        key_outputs = key_outputs + "key." + std::to_string(i) + ", ";
      }
    }
    std::string value_outputs;
    if (group_output_tag_.size() == 1) {
      value_outputs = "value";
    } else {
      for (size_t i = 0; i < group_output_tag_.size(); i++) {
        value_outputs = value_outputs + "value." + std::to_string(i);
        if (i != group_output_tag_.size() - 1) {
          value_outputs += ", ";
        }
      }
    }
    unfold_fmter % key_outputs % value_outputs;
    return unfold_fmter.str();
  }

  BuildingContext& ctx_;
  int32_t operator_index_;
  std::vector<std::pair<std::string, std::string>> key_alias_name_and_code;
  std::vector<int32_t> key_input_tag_;
  std::vector<int32_t> key_output_tag_;
  std::vector<codegen::ParamConst> key_input_type_;
  std::vector<physical::GroupBy_AggFunc_Aggregate> agg_func_list_;
  std::vector<std::vector<common::Variable>> group_input_vars_;
  std::vector<int32_t> group_output_tag_;
  std::vector<physical::PhysicalOpr::MetaData> meta_datas_;
};

static std::string BuildGroupByOp(
    BuildingContext& ctx, int32_t operator_index,
    const physical::GroupBy& group_by_pb,
    const std::vector<physical::PhysicalOpr::MetaData>& meta_datas) {
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
  return builder.operator_index(operator_index).MetaDatas(meta_datas).Build();
}
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_GROUP_BY_BUILDER_H_
