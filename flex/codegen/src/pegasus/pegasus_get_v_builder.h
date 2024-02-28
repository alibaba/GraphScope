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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_GET_V_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_GET_V_BUILDER_H_

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
enum class GetVType {
  kStart = 0,
  kEnd = 1,
  kOther = 2,
  kBoth = 3,
  kItself = 4,
};

GetVType vopt_pb_to_internal(const physical::GetV::VOpt& v_opt) {
  switch (v_opt) {
  case physical::GetV_VOpt_START:
    return GetVType::kStart;
  case physical::GetV_VOpt_END:
    return GetVType::kEnd;
  case physical::GetV_VOpt_OTHER:
    return GetVType::kOther;
  case physical::GetV_VOpt_BOTH:
    return GetVType::kBoth;
  case physical::GetV_VOpt_ITSELF:
    return GetVType::kItself;
  default:
    throw std::runtime_error("unknown vopt");
  }
}

template <typename LabelT>
class GetVOpBuilder {
 public:
  GetVOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  GetVOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  GetVOpBuilder& v_opt(const physical::GetV::VOpt& v_opt) {
    v_opt_ = vopt_pb_to_internal(v_opt);
    return *this;
  }

  GetVOpBuilder& in_tag(int32_t in_tag_id) {
    in_tag_id_ = in_tag_id;
    return *this;
  }

  GetVOpBuilder& out_tag(int32_t out_tag_id) {
    out_tag_id_ = out_tag_id;
    return *this;
  }

  GetVOpBuilder& add_vertex_label(const common::NameOrId& vertex_label) {
    vertex_labels_.push_back(
        try_get_label_from_name_or_id<LabelT>(vertex_label));
    return *this;
  }

  GetVOpBuilder& filter(const common::Expression& expr) {
    auto size = expr.operators().size();
    if (size > 0) {
      ExprBuilder expr_builder(ctx_);

      auto& expr_oprs = expr.operators();
      expr_builder.AddAllExprOpr(expr_oprs);
      std::vector<int32_t> predicate_tag;
      std::vector<std::string> case_exprs;
      std::tie(predicate_expr_, var_names_, predicate_tag, properties_,
               case_exprs) = expr_builder.BuildRust();
      has_predicate_ = true;
    } else {
      has_predicate_ = false;
      VLOG(10) << "no expression in getv";
    }
    return *this;
  }

  // code for GetV
  std::string Build() const {
    std::stringstream ss;

    bool filter_label = false;
    std::pair<int, std::vector<int32_t>> input_type;
    int32_t input_index = 0;

    if (in_tag_id_ == -1) {
      input_type = ctx_.GetHeadType();
      CHECK(input_type.first == 0);
      if (!vertex_labels_.empty()) {
        for (size_t i = 0; i < input_type.second.size(); ++i) {
          if (std::find(vertex_labels_.begin(), vertex_labels_.end(),
                        input_type.second[i]) == vertex_labels_.end()) {
            VLOG(10) << "Can not find label " << input_type.second.size();
            filter_label = true;
            break;
          }
        }
      }
    } else {
      input_type = ctx_.GetAliasType(in_tag_id_);
      input_index = ctx_.GetAliasIndex(in_tag_id_);
      if (input_type.first != 0) {
        VLOG(10) << "Unexpected input type " << input_type.first;
      }
      CHECK(input_type.first == 0);
      if (!vertex_labels_.empty()) {
        for (size_t i = 0; i < input_type.second.size(); ++i) {
          if (std::find(vertex_labels_.begin(), vertex_labels_.end(),
                        input_type.second[i]) == vertex_labels_.end()) {
            VLOG(10) << "Can not find label " << input_type.second.size();
            filter_label = true;
            break;
          }
        }
      }
    }
    VLOG(10) << "Labels size " << input_type.second.size();
    VLOG(10) << "Labels type " << input_type.first;

    auto latest_outputs = ctx_.GetOutput();
    std::string getv_head_code = write_head();

    std::string getv_body_code;
    auto in_data_type = latest_outputs[input_index];
    switch (v_opt_) {
    case GetVType::kItself: {
      if (filter_label) {
      } else {
        if (has_predicate_) {
          if (input_type.second.size() == 1) {
            int32_t label_id = input_type.second[0];
            getv_body_code = filter_by_predicate(input_index, label_id);
          } else {
            boost::format multi_labels_fmter(
                "let vertex_label = "
                "LDBCVertexParser::<usize>::get_label_id(i%1% as usize);\n"
                "%2%");
            std::string getv_code;
            for (auto i : input_type.second) {
              boost::format getv_fmter(
                  "if vertex_label == %1% {\n"
                  "%2"
                  "}\n");
              getv_fmter % i % filter_by_predicate(input_index, i);
              getv_code += getv_fmter.str();
            }
            multi_labels_fmter % input_index % getv_code;
            getv_body_code = multi_labels_fmter.str();
          }
        } else {
          // return stream and do nothing
          boost::format empty_fmter("let stream_%1% = stream%2%");
          empty_fmter % operator_index_ % (operator_index_ - 1);
          return empty_fmter.str();
        }
      }
      break;
    }
    case GetVType::kEnd: {
      boost::format get_id_fmter("i%1%%2%");
      if (in_data_type.size() == 1) {
        if (in_data_type[0] == codegen::DataType::kInt64Array) {
          get_id_fmter % input_index % ".last()";
        } else {
          get_id_fmter % input_index % "";
        }
      } else {
        LOG(FATAL) << "Unsupported type";
      }
      if (filter_label) {
        boost::format filter_fmter(
            "vertex_id = %1%;\n"
            "let vertex_label =  "
            "LDBCVertexParser::<usize>::get_label_id(vertex_id as usize);\n"
            "let label_list = vec![%2%];\n"
            "if label_list.contains(vertex_label) {\n"
            "result.push(vertex_id);\n"
            "}\n");

        std::string label_string = generate_label_string();
        filter_fmter % get_id_fmter.str() % label_string;
        getv_body_code = filter_fmter.str();
      } else {
        boost::format no_filter_fmter(
            "vertex_id = %1%;\n"
            "result.push(vertex_id);\n");
        no_filter_fmter % get_id_fmter.str();
        getv_body_code = no_filter_fmter.str();
      }
      break;
    }
    case GetVType::kStart: {
      boost::format get_id_fmter("i%1%%2%");
      if (in_data_type.size() == 1) {
        if (in_data_type[0] == codegen::DataType::kInt64Array) {
          get_id_fmter % input_index % ".start()";
        } else {
          get_id_fmter % input_index % "";
        }
      } else {
        LOG(FATAL) << "Unsupported type";
      }
      if (filter_label) {
        boost::format filter_fmter(
            "vertex_id = %1%;\n"
            "let vertex_label =  "
            "LDBCVertexParser::<usize>::get_label_id(vertex_id as usize);\n"
            "let label_list = vec![%2%];\n"
            "if label_list.contains(vertex_label) {\n"
            "result.push(vertex_id);\n"
            "}\n");

        std::string label_string = generate_label_string();
        filter_fmter % get_id_fmter.str() % label_string;
        getv_body_code = filter_fmter.str();
      } else {
        boost::format no_filter_fmter(
            "vertex_id = %1%;\n"
            "result.push(vertex_id);\n");
        no_filter_fmter % get_id_fmter.str();
        getv_body_code = no_filter_fmter.str();
      }
      break;
    }
    case GetVType::kOther: {
      boost::format get_id_fmter("i%1%%2%");
      if (in_data_type.size() == 1) {
        if (in_data_type[0] == codegen::DataType::kInt64Array) {
          LOG(FATAL) << "Unsupported data type in kOther";
        } else {
          get_id_fmter % input_index % "";
        }
      } else {
        LOG(FATAL) << "Unsupported type";
      }
      if (filter_label) {
        boost::format filter_fmter(
            "vertex_id = %1%;\n"
            "let vertex_label =  "
            "LDBCVertexParser::<usize>::get_label_id(vertex_id as usize);\n"
            "let label_list = vec![%2%];\n"
            "if label_list.contains(vertex_label) {\n"
            "result.push(vertex_id);\n"
            "}\n");

        std::string label_string = generate_label_string();
        filter_fmter % get_id_fmter.str() % label_string;
        getv_body_code = filter_fmter.str();
      } else {
        boost::format no_filter_fmter(
            "vertex_id = %1%;\n"
            "result.push(vertex_id);\n");
        no_filter_fmter % get_id_fmter.str();
        getv_body_code = no_filter_fmter.str();
      }
      break;
    }
    case GetVType::kBoth:
      LOG(FATAL) << "Unsupported getv type";
    }

    std::vector<codegen::DataType> output;
    output.push_back(codegen::DataType::kInt64);
    latest_outputs[0] = output;
    ctx_.SetHeadType(input_type.first, input_type.second);

    int32_t input_size = ctx_.InputSize();
    boost::format edge_expand_output_fmter(
        "Ok(result.into_iter().map(move |res| %1%))\n"
        "})?;");
    ss << "Ok(result.into_iter().map(|res| (res";

    int32_t output_index = -1;
    if (out_tag_id_ != -1) {
      ctx_.SetAlias(out_tag_id_);
      ctx_.SetAliasType(out_tag_id_, input_type.first, input_type.second);
      output_index = ctx_.GetAliasIndex(out_tag_id_);
      ctx_.SetOutput(output_index, output);
    }

    std::string output_params = generate_output_list(
        "i", input_size, "res", output_index, ctx_.ContainHead());
    edge_expand_output_fmter % output_params;
    ctx_.SetHead(true);
    return getv_head_code + getv_body_code + edge_expand_output_fmter.str();
  }

 private:
  std::string write_head() const {
    int32_t input_size = ctx_.InputSize();
    boost::format head_fmter(
        "let stream_%1% = stream_%2%\n"
        ".flat_map(move |%3%|5 {\n"
        "let mut result = vec![];");
    std::string input_params = generate_arg_list("i", input_size);
    head_fmter % operator_index_ % (operator_index_ - 1) % input_params;
    return head_fmter.str();
  }

  std::string generate_label_string() const {
    std::stringstream labels_ss;
    for (size_t i = 0; i < vertex_labels_.size(); i++) {
      labels_ss << vertex_labels_[i];
      if (i != vertex_labels_.size() - 1) {
        labels_ss << ",";
      }
    }
    return labels_ss.str();
  }

  std::string filter_by_predicate(int32_t index,
                                  const int32_t& label_id) const {
    if (predicate_expr_.empty()) {
      boost::format no_predicate_fmter("result.push(i%1%);\n");
      no_predicate_fmter % index;
      return no_predicate_fmter.str();
    }
    boost::format predicate_fmter(
        "let vertex_id = CSR.get_internal_id(i%1% as usize);\n"
        "%2%"
        "if %3% {\n"
        "let vertex_global_id = CSR.get_global_id(i, %4%).unwrap() as u64;\n"
        "result.push(i%1%);\n"
        "}\n");
    std::string vars_code;
    for (size_t i = 0; i < var_names_.size(); ++i) {
      boost::format var_fmter("let %1% = %2%[vertex_id];\n");
      std::string prop_name =
          get_vertex_prop_column_name(properties_[i].var_name, label_id);
      var_fmter % var_names_[i] % prop_name;
      vars_code += var_fmter.str();
    }
    predicate_fmter % index % vars_code % predicate_expr_ % label_id;
    return predicate_fmter.str();
  }

  BuildingContext& ctx_;
  int32_t operator_index_;
  GetVType v_opt_;
  int32_t in_tag_id_, out_tag_id_;
  std::vector<int32_t> vertex_labels_;
  bool has_predicate_;
  std::string predicate_expr_;
  std::vector<std::string> var_names_;
  std::vector<codegen::ParamConst> properties_;
};

template <typename LabelT>
static std::string BuildGetVOp(
    BuildingContext& ctx, int32_t operator_index,
    const physical::GetV& get_v_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  GetVOpBuilder<LabelT> builder(ctx);
  builder.v_opt(get_v_pb.opt());
  if (get_v_pb.has_tag()) {
    builder.in_tag(get_v_pb.tag().value());
  } else {
    builder.in_tag(-1);
  }

  if (get_v_pb.has_alias()) {
    builder.out_tag(get_v_pb.alias().value());
  } else {
    builder.out_tag(-1);
  }
  auto& vertex_labels_pb = get_v_pb.params().tables();
  for (auto vertex_label_pb : vertex_labels_pb) {
    builder.add_vertex_label(vertex_label_pb);
  }

  return builder.operator_index(operator_index)
      .filter(get_v_pb.params().predicate())
      .Build();
}
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_GET_V_BUILDER_H_
