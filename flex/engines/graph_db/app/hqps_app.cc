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

bool HQPSAdhocReadApp::Query(const GraphDBSession& graph, Decoder& input,
                             Encoder& output) {
  if (input.size() <= 4) {
    LOG(ERROR) << "Invalid input for HQPSAdhocReadApp, input size: "
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
  if (app_wrapper.app()->mode() != AppMode::kRead) {
    LOG(ERROR) << "Invalid app mode for adhoc query: " << input_lib_path
               << ",expect " << AppMode::kRead << ", actual "
               << app_wrapper.app()->mode();
    return false;
  }
  // Adhoc read app should not have input, so we pass an empty decoder
  std::vector<char> dummy_input;
  gs::Decoder dummy_decoder(dummy_input.data(), dummy_input.size());
  auto casted = dynamic_cast<ReadAppBase*>(app_wrapper.app());
  return casted->Query(graph, dummy_decoder, output);
}

bool HQPSAdhocWriteApp::Query(GraphDBSession& graph, Decoder& input,
                              Encoder& output) {
  if (input.size() <= 4) {
    LOG(ERROR) << "Invalid input for HQPSAdhocReadApp, input size: "
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
    LOG(ERROR) << "Invalid app mode for adhoc query: " << input_lib_path
               << ",expect " << AppMode::kWrite << ", actual "
               << app_wrapper.app()->mode();
    return false;
  }
  std::vector<char> dummy_input;
  gs::Decoder dummy_decoder(dummy_input.data(), dummy_input.size());
  return app_wrapper.app()->run(graph, dummy_decoder, output);
}

// GraphDB& db is not used in these functions
AppWrapper HQPSAdhocReadAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new HQPSAdhocReadApp(), NULL);
}

AppWrapper HQPSAdhocWriteAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new HQPSAdhocWriteApp(), NULL);
}

}  // namespace gs
