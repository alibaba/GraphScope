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

#include <unistd.h>
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"

#include "grape/grape.h"
#include "grape/types.h"
#include "grape/util.h"
#include "vineyard/basic/ds/types.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/utils/grape_utils.h"

#include "apps/property/sssp_property.h"
#include "core/error.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/javasdk.h"
#include "core/java/type_alias.h"
#include "core/loader/arrow_fragment_loader.h"
#include "java_pie/java_pie_projected_parallel_app.h"
#include "java_pie/java_pie_property_parallel_app.h"

namespace bl = boost::leaf;

using FragmentType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;
using ProjectedFragmentType =
    gs::ArrowProjectedFragment<int64_t, uint64_t, std::string, std::string>;

void QueryProjected(vineyard::Client& client,
                    std::shared_ptr<ProjectedFragmentType> fragment,
                    const grape::CommSpec& comm_spec,
                    const std::string& app_name, const std::string& out_prefix,
                    const std::string& basic_params) {
  using AppType = gs::JavaPIEProjectedParallelAppOE<ProjectedFragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  std::string lib_path = "";
  worker->Query(basic_params, lib_path);
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  std::shared_ptr<gs::JavaPIEProjectedContext<ProjectedFragmentType>> ctx =
      worker->GetContext();
  worker->Finalize();
}

// Running test doesn't require codegen.
void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id, const std::string& app_name) {
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(id));

  boost::property_tree::ptree pt;
  pt.put("src", "4");
  pt.put("threadNum", "1");
  pt.put("app_class", app_name);
  if (getenv("USER_JAR_PATH")) {
    pt.put("jar_name", getenv("USER_JAR_PATH"));
  } else {
    LOG(ERROR) << "JAR_NAME not set";
    return;
  }

  pt.put(
      "frag_name",
      "gs::ArrowProjectedFragment<int64_t,uint64_t,std::string,std::string>");
  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  std::string basic_params = ss.str();
  VLOG(1) << "basic_params" << basic_params;
  VLOG(1) << "running projected";
  VLOG(1) << "vertex properties num: " << fragment->vertex_property_num(0);
  VLOG(1) << "edge properties num: " << fragment->edge_property_num(0);
  std::shared_ptr<ProjectedFragmentType> projected_fragment =
      ProjectedFragmentType::Project(fragment, 0, 0, 0, 2);
  // test get data
  using vertex_t = ProjectedFragmentType::vertex_t;
  vertex_t vertex;
  projected_fragment->GetInnerVertex(4, vertex);
  VLOG(1) << "source vertex" << vertex.GetValue();
  for (uint64_t id = 0; id < 4; id++) {
    vertex.SetValue(id);
    LOG(INFO) << "lid: " << id
              << " vdata: " << projected_fragment->GetData(vertex);
  }

  QueryProjected(client, projected_fragment, comm_spec, app_name, "/tmp",
                 basic_params);
}

int main(int argc, char** argv) {
  if (argc < 7) {
    printf(
        "usage: ./run_java_string_app <ipc_socket> <e_label_num> "
        "<efiles...> "
        "<v_label_num> <vfiles...>"
        "[app_name]\n");
    return 1;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);

  int edge_label_num = atoi(argv[index++]);
  std::vector<std::string> efiles;
  for (int i = 0; i < edge_label_num; ++i) {
    efiles.push_back(argv[index++]);
  }

  int vertex_label_num = atoi(argv[index++]);
  std::vector<std::string> vfiles;
  for (int i = 0; i < vertex_label_num; ++i) {
    vfiles.push_back(argv[index++]);
  }

  std::string app_name = "";
  if (argc > index) {
    app_name = argv[index++];
  }
  VLOG(1) << "app name " << app_name;

  grape::InitMPIComm();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    VLOG(1) << "Connected to IPCServer: " << ipc_socket;

    vineyard::ObjectID fragment_id;
    {
      auto loader = std::make_unique<
          gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                                  vineyard::property_graph_types::VID_TYPE>>(
          client, comm_spec, efiles, vfiles);
      fragment_id =
          bl::try_handle_all([&loader]() { return loader->LoadFragment(); },
                             [](const vineyard::GSError& e) {
                               LOG(ERROR) << e.error_msg;
                               return 0;
                             },
                             [](const bl::error_info& unmatched) {
                               LOG(ERROR) << "Unmatched error " << unmatched;
                               return 0;
                             });
    }

    VLOG(1) << "[worker-" << comm_spec.worker_id()
            << "] loaded graph to vineyard ...";

    MPI_Barrier(comm_spec.comm());

    Run(client, comm_spec, fragment_id, app_name);
    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}
