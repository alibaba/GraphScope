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
#ifndef HQPS_CODEGEN_SRC_PEGASUS_GENERATOR_H_
#define HQPS_CODEGEN_SRC_PEGASUS_GENERATOR_H_

#include <string>
#include <vector>

#include "flex/proto_generated_gie/physical.pb.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/json_util.h"

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/pegasus/pegasus_dedup_builder.h"
#include "flex/codegen/src/pegasus/pegasus_edge_expand_builder.h"
#include "flex/codegen/src/pegasus/pegasus_get_v_builder.h"
#include "flex/codegen/src/pegasus/pegasus_group_by_builder.h"
#include "flex/codegen/src/pegasus/pegasus_intersect_builder.h"
#include "flex/codegen/src/pegasus/pegasus_join_builder.h"
#include "flex/codegen/src/pegasus/pegasus_limit_builder.h"
#include "flex/codegen/src/pegasus/pegasus_order_by_builder.h"
#include "flex/codegen/src/pegasus/pegasus_path_expand_builder.h"
#include "flex/codegen/src/pegasus/pegasus_project_builder.h"
#include "flex/codegen/src/pegasus/pegasus_repartition_builder.h"
#include "flex/codegen/src/pegasus/pegasus_scan_builder.h"
#include "flex/codegen/src/pegasus/pegasus_select_builder.h"
#include "flex/codegen/src/pegasus/pegasus_sink_builder.h"
#include "flex/codegen/src/pegasus/pegasus_unfold_builder.h"
#include "flex/codegen/src/pegasus/pegasus_union_builder.h"

namespace gs {

// Entrance for generating a parameterized query
class PegasusGenerator {
 public:
  PegasusGenerator(BuildingContext& ctx, std::string query_name,
                   const physical::PhysicalPlan& plan)
      : ctx_(ctx), query_name_(query_name), plan_(plan) {}

  std::string GenerateQuery() {
    std::stringstream header, exprs, query_body;
    addHeaders(header);

    addQueryBody(query_body);

    startQueryFunc(
        exprs);  // prepend function signature after visiting all operators

    addProperties(exprs);

    endQueryFunc(
        query_body);  // append function call after visiting all operators
    return header.str() + exprs.str() + query_body.str();
  }

 private:
  // add dependency for query function
  void addHeaders(std::stringstream& ss) const {
    ss << "use std::collections::{HashMap, HashSet};\n";
    ss << "use mcsr::columns::*;\n";
    ss << "use mcsr::graph_db_impl::{CsrDB, SingleSubGraph, SubGraph};\n";
    ss << "use mcsr::ldbc_parser::LDBCVertexParser;\n";
    ss << "use pegasus::api::*;\n";
    ss << "use pegasus::errors::BuildJobError;\n";
    ss << "use pegasus::result::ResultSink;\n";
    ss << "use pegasus::{get_servers_len, JobConf};\n";
    ss << "use crate::utils::*;\n";
    ss << std::endl;
    LOG(INFO) << "Finish adding headers";
  }

  // add start part of query function
  void startQueryFunc(std::stringstream& ss) const {
    std::stringstream input_ss;
    if (ctx_.GetParameterVars().size() > 0) {
      auto vars = ctx_.GetParameterVars();
      sort(vars.begin(), vars.end(),
           [](const auto& a, const auto& b) { return a.id < b.id; });
      // FIXME: ENable this line
      // the dynamic params can be duplicate.
      CHECK(vars[0].id == 0);
      for (size_t i = 0; i < vars.size(); ++i) {
        if (i > 0 && vars[i].id == vars[i - 1].id) {
          // found duplicate
          CHECK(vars[i] == vars[i - 1]);
          continue;
        } else {
          input_ss << ", " << vars[i].var_name << ":"
                   << data_type_2_rust_string(vars[i].type);
        }
      }
    }

    ss << "#[no_mangle]\n";
    ss << "pub fn Query(conf: JobConf, graph: &'static CsrDB<usize, usize>"
       << ", input_params: Vec<String>) -> Box<dyn Fn(&mut Source<i32>, "
          "ResultSink<String>) -> Result<(), BuildJobError>> {\n";
    ss << "let workers = conf.workers;\n";
  }

  // add properties handler used in query
  void addProperties(std::stringstream& ss) const {
    for (auto kv : ctx_.GetVertexProperties()) {
      int32_t vertex_label = kv.first;
      std::vector<codegen::ParamConst>& properties = kv.second;
      for (auto property : properties) {
        std::string property_name =
            get_vertex_prop_column_name(property.var_name, vertex_label);
        auto property_type = property.type;
        ss << "let " << property_name << " = &graph.vertex_prop_table["
           << vertex_label << " as usize]\n";
        ss << ".get_column_by_name(\"" << property.var_name << "\")\n";
        ss << ".as_any()\n";
        switch (property_type) {
        case codegen::DataType::kInt32: {
          ss << ".downcast_ref::<Int32Column>()\n";
          break;
        }
        case codegen::DataType::kString: {
          ss << ".downcast_ref::<StringColumn>()\n";
          break;
        }
        default:
          ss << ".downcast_ref::<StringColumn>()\n";
          break;
        }
        ss << ".unwrap()\n";
        ss << ".data;\n";
      }
    }
  }

  void endQueryFunc(std::stringstream& ss) const {
    ss << "})\n";
    ss << "}\n";
  }

  void addQueryBody(std::stringstream& ss) const {
    auto size = plan_.plan_size();

    LOG(INFO) << "Found " << size << " operators in the plan";
    ss << "Box::new(move |input: &mut Source<i32>, output: ResultSink<String>| "
          "{\n";
    ss << "let worker_id = input.get_worker_index() % workers;\n";
    ss << "let stream_0 = input.input_from(vec![0])?;\n";

    std::string plan_json;
    google::protobuf::util::JsonPrintOptions option;
    option.always_print_primitive_fields = true;
    auto st =
        google::protobuf::util::MessageToJsonString(plan_, &plan_json, option);
    for (auto i = 0; i < size; ++i) {
      auto op = plan_.plan(i);
      LOG(INFO) << "Start codegen for operator " << i;
      auto& meta_datas = op.meta_data();
      // CHECK(meta_datas.size() == 1) << "meta data size: " <<
      // meta_datas.size();
      // physical::PhysicalOpr::MetaData meta_data; //fake meta
      auto opr = op.opr();
      LOG(INFO) << "Input size of current operator is " << ctx_.InputSize();
      switch (opr.op_kind_case()) {
      case physical::PhysicalOpr::Operator::kScan: {  // scan
        physical::PhysicalOpr::MetaData meta_data;

        LOG(INFO) << "Found a scan operator";
        auto& scan_op = opr.scan();
        auto scan_codegen =
            pegasus::BuildScanOp(ctx_, i + 1, scan_op, meta_data);
        LOG(INFO) << scan_codegen;
        ss << scan_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kRepartition: {
        physical::PhysicalOpr::MetaData meta_data;

        LOG(INFO) << "Found a repartition operator";
        auto& repartition_op = opr.repartition();
        auto repartition_codegen =
            pegasus::BuildRepartitionOp(ctx_, i + 1, repartition_op, meta_data);
        LOG(INFO) << repartition_codegen;
        ss << repartition_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kGroupBy: {
        std::vector<physical::PhysicalOpr::MetaData> meta_datas;
        for (auto i = 0; i < op.meta_data_size(); i++) {
          meta_datas.push_back(op.meta_data(i));
        }

        LOG(INFO) << "Found a groupby operator";
        auto& groupby_op = opr.group_by();

        ss << pegasus::BuildGroupByOp(ctx_, i + 1, groupby_op, meta_datas);
        break;
      }
      case physical::PhysicalOpr::Operator::kOrderBy: {
        physical::PhysicalOpr::MetaData meta_data;

        LOG(INFO) << "Found a order_by operator";
        auto& orderby_op = opr.order_by();

        ss << pegasus::BuildOrderByOp(ctx_, i + 1, orderby_op, meta_data);
        break;
      }
      case physical::PhysicalOpr::Operator::kProject: {
        std::vector<physical::PhysicalOpr::MetaData> meta_data;
        for (auto i = 0; i < op.meta_data_size(); i++) {
          meta_data.push_back(op.meta_data(i));
        }

        LOG(INFO) << "Found a project operator";
        auto& project_op = opr.project();

        ss << pegasus::BuildProjectOp(ctx_, i + 1, project_op, meta_data);
        break;
      }
      case physical::PhysicalOpr::Operator::kEdge: {  // edge expand
        auto& meta_data = meta_datas[0];
        LOG(INFO) << "Found a edge expand operator";
        auto& edge_op = opr.edge();
        auto edge_codegen = pegasus::BuildEdgeExpandOp<int32_t>(
            ctx_, i + 1, edge_op, meta_data);
        LOG(INFO) << edge_codegen;
        ss << edge_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kVertex: {
        physical::PhysicalOpr::MetaData meta_data;

        LOG(INFO) << "Found a get_v operator";
        auto& vertex_op = opr.vertex();
        auto vertex_codegen =
            pegasus::BuildGetVOp<uint8_t>(ctx_, i + 1, vertex_op, meta_data);
        LOG(INFO) << vertex_codegen;
        ss << vertex_codegen;

        break;
      }
      case physical::PhysicalOpr::Operator::kSink: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a sink operator";
        auto& sink_op = opr.sink();
        std::string call_sink_code =
            pegasus::BuildSinkOp(ctx_, i + 1, sink_op, meta_data);
        ss << call_sink_code;
        break;
      }
      case physical::PhysicalOpr::Operator::kPath: {
        auto& meta_data = meta_datas[0];
        LOG(INFO) << "Found a path expand operator";
        auto& path_op = opr.path();
        auto path_expand_codegen =
            pegasus::BuildPathExpandOp<int32_t>(ctx_, path_op, meta_data);
        LOG(INFO) << path_expand_codegen;
        ss << path_expand_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kIntersect: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a intersect operator";
        auto& intersect_op = opr.intersect();
        auto intersect_codegen =
            pegasus::BuildIntersectOp(ctx_, intersect_op, meta_data);
        LOG(INFO) << intersect_codegen;
        ss << intersect_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kUnfold: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a unfold operator";
        auto& unfold_op = opr.unfold();
        auto unfold_codegen =
            pegasus::BuildUnfoldOp(ctx_, i + 1, unfold_op, meta_data);
        LOG(INFO) << unfold_codegen;
        ss << unfold_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kDedup: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a dedup operator";
        auto& dedup_op = opr.dedup();
        auto dedup_codegen =
            pegasus::BuildDedupOp(ctx_, i + 1, dedup_op, meta_data);
        LOG(INFO) << dedup_codegen;
        ss << dedup_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kUnion: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a union operator";
        auto& union_op = opr.union_();
        auto union_codegen =
            pegasus::BuildUnionOp(ctx_, i + 1, union_op, meta_data);
        LOG(INFO) << union_codegen;
        ss << union_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kJoin: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a join operator";
        auto& join_op = opr.join();
        auto join_codegen =
            pegasus::BuildJoinOp(ctx_, i + 1, join_op, meta_data);
        LOG(INFO) << join_codegen;
        ss << join_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kSelect: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a select operator";
        auto& select_op = opr.select();
        auto select_codegen =
            pegasus::BuildSelectOp(ctx_, i + 1, select_op, meta_data);
        LOG(INFO) << select_codegen;
        ss << select_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kLimit: {
        physical::PhysicalOpr::MetaData meta_data;
        LOG(INFO) << "Found a select operator";
        auto& limit_pb = opr.limit();
        auto limit_codegen =
            pegasus::BuildLimitOp(ctx_, i + 1, limit_pb, meta_data);
        LOG(INFO) << limit_codegen;
        ss << limit_codegen;
        break;
      }
      default:
        LOG(FATAL) << "Unsupported operator type: " << opr.op_kind_case();
      }
    }
    LOG(INFO) << "Finish adding query";
  }

  BuildingContext& ctx_;
  std::string query_name_;
  const physical::PhysicalPlan& plan_;
};

}  // namespace gs

#endif  // HQPS_CODEGEN_SRC_PEGASUS_GENERATOR_H_
