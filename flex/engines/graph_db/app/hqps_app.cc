/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/app/hqps_app.h"
#include "flex/proto_generated_gie/stored_procedure.pb.h"

namespace gs {

void put_argument(gs::Encoder& encoder, const query::Argument& argument) {
  auto& value = argument.value();
  auto item_case = value.item_case();
  switch (item_case) {
  case common::Value::kI32:
    encoder.put_int(value.i32());
    break;
  case common::Value::kI64:
    encoder.put_long(value.i64());
    break;
  case common::Value::kF64:
    encoder.put_double(value.f64());
    break;
  case common::Value::kStr:
    encoder.put_string(value.str());
    break;
  default:
    LOG(ERROR) << "Not recognizable param type" << static_cast<int>(item_case);
  }
}

HQPSAdhocApp::HQPSAdhocApp(GraphDBSession& graph) : graph_(graph) {}

bool HQPSAdhocApp::Query(Decoder& input, Encoder& output) {
  if (input.size() <= 4) {
    LOG(ERROR) << "Invalid input for HQPSAdhocApp, input size: "
               << input.size();
    return false;
  }
  std::string_view str_view(input.data(), input.size());
  std::string input_lib_path = std::string(str_view);
  auto app_factory = std::make_shared<SharedLibraryAppFactory>(input_lib_path);
  AppWrapper app_wrapper;  // wrapper should be destroyed before the factory

  if (app_factory) {
    app_wrapper = app_factory->CreateApp(graph_);
    if (app_wrapper.app() == NULL) {
      LOG(ERROR) << "Fail to create app for adhoc query: " << input_lib_path;
      return false;
    }
  } else {
    LOG(ERROR) << "Fail to evaluate adhoc query: " << input_lib_path;
    return false;
  }

  return app_wrapper.app()->Query(input, output);
}

HQPSProcedureApp::HQPSProcedureApp(GraphDBSession& graph) : graph_(graph) {}

bool HQPSProcedureApp::Query(Decoder& input, Encoder& output) {
  if (input.size() <= 0) {
    LOG(ERROR) << "Invalid input for HQPSProcedureApp, input size: "
               << input.size();
    return false;
  }
  query::Query cur_query;
  if (!cur_query.ParseFromArray(input.data(), input.size())) {
    LOG(ERROR) << "Fail to parse query from input content";
    return false;
  }
  auto query_name = cur_query.query_name().name();

  std::vector<char> input_buffer;
  gs::Encoder input_encoder(input_buffer);
  auto& args = cur_query.arguments();
  for (int32_t i = 0; i < args.size(); ++i) {
    put_argument(input_encoder, args[i]);
  }
  VLOG(10) << "Query name: " << query_name << ", args: " << input_buffer.size()
           << " bytes";
  gs::Decoder input_decoder(input_buffer.data(), input_buffer.size());

  if (query_name.empty()) {
    LOG(ERROR) << "Query name is empty";
    return false;
  }
  auto& app_name_to_path_index = graph_.schema().GetPlugins();
  // get procedure id from name.
  if (app_name_to_path_index.count(query_name) <= 0) {
    LOG(ERROR) << "Query name is not registered: " << query_name;
    return false;
  }

  // get app
  auto type = app_name_to_path_index.at(query_name).second;
  auto app = graph_.GetApp(type);
  if (!app) {
    LOG(ERROR) << "Fail to get app for query: " << query_name
               << ", type: " << type;
    return false;
  }
  return app->Query(input_decoder, output);
}

HQPSAdhocAppFactory::HQPSAdhocAppFactory() {}
HQPSAdhocAppFactory::~HQPSAdhocAppFactory() {}
AppWrapper HQPSAdhocAppFactory::CreateApp(GraphDBSession& graph) {
  return AppWrapper(new HQPSAdhocApp(graph), NULL);
}

HQPSProcedureAppFactory::HQPSProcedureAppFactory() {}
HQPSProcedureAppFactory::~HQPSProcedureAppFactory() {}
AppWrapper HQPSProcedureAppFactory::CreateApp(GraphDBSession& graph) {
  return AppWrapper(new HQPSProcedureApp(graph), NULL);
}

}  // namespace gs
