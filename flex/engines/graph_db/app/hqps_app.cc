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

AppWrapper loadAdhocQuery(const std::string& input_lib_path,
                          std::shared_ptr<SharedLibraryAppFactory> app_factory,
                          const GraphDB& graph) {
  AppWrapper app_wrapper;  // wrapper should be destroyed before the factory
  if (app_factory) {
    app_wrapper = app_factory->CreateApp(graph);
    if (app_wrapper.app() == NULL) {
      LOG(ERROR) << "Fail to create app for adhoc query from path: "
                 << input_lib_path;
    }
  } else {
    LOG(ERROR) << "Fail to evaluate adhoc query: " << input_lib_path;
  }
  return app_wrapper;
}

bool parse_input_argument(gs::Decoder& raw_input, gs::Encoder& argument_encoder,
                          std::string& query_name) {
  if (raw_input.size() <= 0) {
    LOG(ERROR) << "Invalid input size: " << raw_input.size();
    return false;
  }
  query::Query cur_query;
  if (!cur_query.ParseFromArray(raw_input.data(), raw_input.size())) {
    LOG(ERROR) << "Fail to parse query from input content";
    return false;
  }
  query_name = cur_query.query_name().name();
  if (query_name.empty()) {
    LOG(ERROR) << "Query name is empty";
    return false;
  }
  auto& args = cur_query.arguments();
  for (int32_t i = 0; i < args.size(); ++i) {
    put_argument(argument_encoder, args[i]);
  }
  VLOG(10) << "Query name: " << query_name << ", num args: " << args.size();
  return true;
}

AppBase* get_app(const std::string& query_name, GraphDBSession& graph) {
  auto& app_name_to_path_index = graph.schema().GetPlugins();
  // get procedure id from name.
  if (app_name_to_path_index.count(query_name) <= 0) {
    LOG(ERROR) << "Query name is not registered: " << query_name;
    return NULL;
  }

  // get app
  auto type = app_name_to_path_index.at(query_name).second;
  auto app = graph.GetApp(type);
  if (!app) {
    LOG(ERROR) << "Fail to get app for query: " << query_name
               << ", type: " << type;
    return NULL;
  }
  return app;
}

bool HQPSAdhocApp::Query(GraphDBSession& graph, Decoder& input,
                         Encoder& output) {
  if (input.size() <= 4) {
    LOG(ERROR) << "Invalid input for AbstractHQPSAdhocApp, input size: "
               << input.size();
    return false;
  }
  std::string_view str_view(input.data(), input.size());
  std::string input_lib_path = std::string(str_view);
  auto app_factory = std::make_shared<SharedLibraryAppFactory>(input_lib_path);
  auto app_wrapper = loadAdhocQuery(input_lib_path, app_factory, graph.db());
  if (app_wrapper.app() == NULL) {
    LOG(ERROR) << "Fail to load adhoc query: " << input_lib_path;
    return false;
  }
  if (app_wrapper.app()->mode() != AppMode::kWrite) {
    LOG(ERROR) << "Invalid app mode for adhoc query: " << input_lib_path;
    return false;
  }
  return app_wrapper.app()->run(graph, input, output);
}

bool HQPSProcedureApp::Query(GraphDBSession& graph, Decoder& input,
                             Encoder& output) {
  std::string query_name;
  std::vector<char> input_buffer;
  gs::Encoder argument_encoder(input_buffer);
  if (!parse_input_argument(input, argument_encoder, query_name)) {
    return false;
  }

  gs::Decoder argument_decoder(input_buffer.data(), input_buffer.size());
  auto app = get_app(query_name, graph);
  if (!app) {
    LOG(ERROR) << "Fail to get app for query: " << query_name;
    return false;
  }
  return app->run(graph, argument_decoder, output);
}

// GraphDB& db is not used in these functions
AppWrapper HQPSAdhocAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new HQPSAdhocApp(), NULL);
}

AppWrapper HQPSProcedureAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new HQPSProcedureApp(), NULL);
}

}  // namespace gs
