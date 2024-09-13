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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_PROJECT_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_PROJECT_BUILDER_H_

#include <sstream>
#include <string>
#include <vector>

#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/codegen/src/pegasus/pegasus_expr_builder.h"

namespace gs {
namespace pegasus {
std::string project_mapping_to_string(
    const physical::Project::ExprAlias& mapping) {
  std::stringstream ss;
  int32_t res_alias = mapping.alias().value();
  int32_t in_tag_id = -2;
  std::vector<std::string> prop_names;
  std::vector<codegen::DataType> data_types;
  bool project_self = false;
  auto& expr = mapping.expr();
  CHECK(expr.operators_size() == 1) << "can only support one variable";
  auto& expr_op = expr.operators(0);
  switch (expr_op.item_case()) {
  case common::ExprOpr::kVar: {
    VLOG(10) << "Got var in projecting";
    auto& var = expr_op.var();
    in_tag_id = var.tag().id();
    if (var.has_property()) {
      auto& prop = var.property();
      if (prop.has_id()) {
        // project itself.
        project_self = true;
      } else if (prop.has_key()) {
        prop_names.push_back(prop.key().name());
        data_types.push_back(
            common_data_type_pb_2_data_type(var.node_type().data_type()));
      } else {
        LOG(FATAL) << "Unknown property type" << prop.DebugString();
      }
    } else {
      VLOG(10) << "receives no property, project itself";
      project_self = true;
    }
    break;
  }
  case common::ExprOpr::kVarMap: {
    VLOG(10) << "Got variable map in projecting";
    LOG(WARNING) << "CURRENTLY we flat the var map to a list of variables";
  }

  case common::ExprOpr::kVars: {
    VLOG(10) << "Got variable keys in projecting";
    // project properties to a list.
    auto& vars =
        expr_op.has_vars() ? expr_op.vars().keys() : expr_op.var_map().keys();
    for (auto i = 0; i < vars.size(); ++i) {
      auto& var = vars[i];
      if (in_tag_id == -2) {
        in_tag_id = var.tag().id();
      } else {
        CHECK(in_tag_id == var.tag().id()) << "can only support one tag";
      }

      auto& prop = var.property();
      if (prop.has_id()) {
        LOG(FATAL) << "Not support project id in projecting with vars";
      } else if (prop.has_key()) {
        prop_names.push_back(prop.key().name());
        data_types.push_back(
            common_data_type_pb_2_data_type(var.node_type().data_type()));
      } else {
        LOG(FATAL) << "Unknown property type" << prop.DebugString();
      }
    }
    break;
  }

  default:
    LOG(FATAL) << "Unknown variable type";
  }

  if (project_self) {
    VLOG(10) << "Projecting self";
    CHECK(prop_names.size() == 0 && data_types.size() == 0);
    ss << PROJECT_SELF_STR << "<" << in_tag_id << ", " << res_alias << ">()";
  } else {
    VLOG(10) << "Projecting properties" << gs::to_string(prop_names);
    CHECK(prop_names.size() == data_types.size());
    CHECK(prop_names.size() > 0);
    ss << PROJECT_PROPS_STR << "<" << in_tag_id << ", " << res_alias;
    for (size_t i = 0; i < data_types.size(); ++i) {
      ss << "," << data_type_2_string(data_types[i]);
    }
    ss << ">({";
    for (size_t i = 0; i < prop_names.size() - 1; ++i) {
      ss << "\"" << prop_names[i] << "\", ";
    }
    ss << "\"" << prop_names[prop_names.size() - 1] << "\"";
    ss << "})";
  }

  return ss.str();
}

class ProjectOpBuilder {
 public:
  ProjectOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  ProjectOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  ProjectOpBuilder& is_append(bool is_append) {
    is_append_ = is_append;
    return *this;
  }

  ProjectOpBuilder& add_mapping(const physical::Project::ExprAlias& mapping) {
    mappings_.push_back(mapping);
    return *this;
  }

  ProjectOpBuilder& meta_data(
      const std::vector<physical::PhysicalOpr::MetaData>& meta_data) {
    meta_data_ = meta_data;
    return *this;
  }

  // return make_project code and call project code.
  std::string Build() const {
    std::stringstream ss;
    std::string head_code = write_head();

    std::string project_body_code;
    for (size_t i = 0; i < mappings_.size(); ++i) {
      project_body_code += project_map_to_code(i);
    }
    if (is_append_) {
      LOG(FATAL) << "Unsupported type";
    } else {
      ctx_.SetHead(false);
      ctx_.ResetAlias();
      for (size_t i = 0; i < mappings_.size(); i++) {
        int32_t output_alias = mappings_[i].alias().value();
        ctx_.SetAlias(output_alias);
        VLOG(10) << "Set alias " << output_alias << ", index "
                 << ctx_.GetAliasIndex(output_alias);
        auto column_meta = meta_data_[i];
        VLOG(10) << "Get meta";
        if (column_meta.type().has_graph_type()) {
          std::vector<codegen::DataType> data_types;
          data_types.push_back(codegen::DataType::kInt64);
          ctx_.SetOutput(i, data_types);
        } else if (column_meta.type().type_case() ==
                   common::IrDataType::kDataType) {
          switch (column_meta.type().data_type()) {
          case common::DataType::INT64: {
            std::vector<codegen::DataType> data_types;
            data_types.push_back(codegen::DataType::kInt64);
            ctx_.SetOutput(i, data_types);
            break;
          }
          case common::DataType::STRING: {
            std::vector<codegen::DataType> data_types;
            data_types.push_back(codegen::DataType::kString);
            ctx_.SetOutput(i, data_types);
            break;
          }
          default:
            std::vector<codegen::DataType> data_types;
            data_types.push_back(codegen::DataType::kString);
            ctx_.SetOutput(i, data_types);
            // LOG(FATAL) << "Unsupported type";
          }
        }
      }
    }
    VLOG(10) << "Project done";
    std::string end_code = write_end();
    return head_code + project_body_code + end_code;
  }

 private:
  std::string write_head() const {
    boost::format head_fmter("let stream_%1% = stream_%2%.map(move |%3%| {\n");
    int32_t input_size = ctx_.InputSize();
    std::string input_params = generate_arg_list("i", input_size);
    head_fmter % operator_index_ % (operator_index_ - 1) % input_params;
    return head_fmter.str();
  }

  std::string project_map_to_code(int32_t index) const {
    std::vector<std::string> prop_names;
    std::vector<codegen::DataType> data_types;
    auto& expr = mappings_[index].expr();
    auto expr_builder = ExprBuilder(ctx_);
    VLOG(10) << "operators size is: " << expr.operators().size() << "\n";
    expr_builder.AddAllExprOpr(expr.operators());

    std::string expression;
    std::vector<std::string> var_names;
    std::vector<int32_t> var_tags;
    std::vector<codegen::ParamConst> properties;
    std::vector<std::string> case_exprs;
    std::tie(expression, var_names, var_tags, properties, case_exprs) =
        expr_builder.BuildRust();
    VLOG(10) << "Start build expr";

    std::string vars_code;
    for (auto i : case_exprs) {
      vars_code += i;
    }
    for (size_t i = 0; i < var_names.size(); i++) {
      int32_t input_index;
      std::pair<int32_t, std::vector<int32_t>> input_type;
      VLOG(10) << "Input tag is " << var_tags[i];
      if (var_tags[i] == -1) {
        input_index = 0;
        input_type = ctx_.GetHeadType();
      } else {
        input_index = ctx_.GetAliasIndex(var_tags[i]);
        input_type = ctx_.GetAliasType(var_tags[i]);
      }
      VLOG(10) << "Property is " << properties[i].var_name << ", var name is "
               << var_names[i];

      boost::format itself_fmter("let %1% = i%2%;\n");
      if (properties[i].var_name == "none") {
        itself_fmter % var_names[i] % input_index;
        vars_code += itself_fmter.str();
      } else {
        CHECK(input_type.first == 0);
        if (input_type.second.size() == 1) {
          boost::format property_fmter(
              "let vertex_id = CSR.get_internal_id(i%1% as usize);\n"
              "let %1% = %2%[vertex_id];");
          int32_t label_id = input_type.second[0];
          std::string property_name =
              get_vertex_prop_column_name(properties[i].var_name, label_id);
          property_fmter % var_names[i] % property_name;
          vars_code += property_fmter.str();
        } else {
          boost::format properties_fmter(
              "let vertex_id = CSR.get_internal_id(i%1% as usize);\n"
              "let vertex_label = LDBCVertexParser::<usize>::get_label_id(i%1% "
              "as usize);\n"
              "let %2% = \n"
              "%3%"  // get property for different labels
              "else {\n"
              "panic!(\"Unexpected label: {}\", vertex_label)"
              "};\n");

          std::string condition_code;
          for (size_t j = 0; j < input_type.second.size(); j++) {
            boost::format condition_fmter(
                "if vertex_label == %1% {\n"
                "%2%[vertex_id]\n"
                "}\n");
            std::string property_name = get_vertex_prop_column_name(
                properties[i].var_name, input_type.second[j]);
            int32_t label_id = input_type.second[j];
            condition_fmter % label_id % property_name;
            if (j > 0) {
              condition_code += "else ";
            }
            condition_code += condition_fmter.str();
          }

          properties_fmter % input_index % var_names[i] % condition_code;
          vars_code += properties_fmter.str();
        }
      }
    }

    boost::format map_fmter(
        "%1%\n"
        "let output%2% = %3%;\n");
    map_fmter % vars_code % index % expression;
    VLOG(10) << "Finished build mapping";
    return map_fmter.str();
  }

  std::string write_end() const {
    boost::format end_fmter("Ok(%1%)\n})?;\n");
    std::string output_params = generate_arg_list("output", mappings_.size());
    end_fmter % output_params;
    return end_fmter.str();
  }

  BuildingContext& ctx_;
  int32_t operator_index_;
  bool is_append_;
  std::vector<physical::Project::ExprAlias> mappings_;
  std::vector<physical::PhysicalOpr::MetaData> meta_data_;
};

static std::string BuildProjectOp(
    BuildingContext& ctx, int32_t operator_index,
    const physical::Project& project_pb,
    const std::vector<physical::PhysicalOpr::MetaData>& meta_data) {
  ProjectOpBuilder builder(ctx);
  builder.is_append(project_pb.is_append());
  auto& mappings = project_pb.mappings();
  for (int32_t i = 0; i < mappings.size(); ++i) {
    builder.add_mapping(mappings[i]);
  }
  return builder.operator_index(operator_index).meta_data(meta_data).Build();
}
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_PROJECT_BUILDER_H_
