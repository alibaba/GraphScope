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

#include "boost/lexical_cast.hpp"
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

#include "core/error.h"
#include "core/java/javasdk.h"
#include "core/java/type_alias.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/object/fragment_wrapper.h"
#include "core/utils/transform_utils.h"
#include "core/worker/parallel_property_worker.h"
#include "java_pie/java_pie_projected_default_app.h"
#include "java_pie/java_pie_property_parallel_app.h"

using FragmentType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;
using ProjectedFragmentType =
    gs::ArrowProjectedFragment<int64_t, uint64_t, double, int64_t>;

using AppType = gs::JavaPIEPropertyParallelApp<FragmentType>;

using ProjectedAppType = gs::JavaPIEProjectedDefaultApp<ProjectedFragmentType>;

void QueryProperty(vineyard::Client& client, const grape::CommSpec& comm_spec,
                   std::shared_ptr<FragmentType> fragment,
                   const std::string& app_name, const std::string& out_prefix,
                   const std::string& basic_params) {
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  std::string lib_path = "";
  double t0 = grape::GetCurrentTime();
  worker->Query(basic_params, lib_path);
  double t1 = grape::GetCurrentTime();
  VLOG(1) << "Query time: " << (t1 - t0);
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void QueryProjected(vineyard::Client& client, const grape::CommSpec& comm_spec,
                    std::shared_ptr<ProjectedFragmentType> fragment,
                    const std::string& app_name, const std::string& out_prefix,
                    const std::string& basic_params) {
  auto app = std::make_shared<ProjectedAppType>();
  auto worker = ProjectedAppType::CreateWorker(app, fragment);
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

  worker->Finalize();
}

int main(int argc, char** argv) {
  if (argc < 10) {
    printf(
        "usage: ./property_graph_java_app_benchmark <ipc_socket> "
        "<fragment_0> ... <fragment_n-1> <run_property>"
        "[app_name] [output_prefix] [threadNum] [query times] <pr_delta> "
        "<pr_maxround> <sssp_source>\n");
    return 1;
  }
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  int index = 1;
  int basic_args = 6 + comm_spec.fnum();
  std::string ipc_socket = std::string(argv[index++]);
  std::string frag_id_str = argv[index + comm_spec.fid()];

  vineyard::ObjectID fragment_id = vineyard::ObjectIDFromString(frag_id_str);

  index += comm_spec.fnum();
  int run_property = atoi(argv[index++]);

  int threadNum = 1, query_time = 1;
  std::string app_name = "", output_prefix = "";
  if (argc > index) {
    app_name = argv[index++];
  }
  if (argc > index) {
    output_prefix = argv[index++];
  }
  if (argc > index) {
    threadNum = atoi(argv[index++]);
  }
  if (argc > index) {
    query_time = atoi(argv[index++]);
  }

  VLOG(1) << "Property: " << run_property << ", app name: " << app_name
          << ", thread num: " << threadNum << ", outprefix: " << output_prefix;
  {
    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    VLOG(1) << "Connected to IPCServer: " << ipc_socket;
    VLOG(1) << "Using ArrowFragment with objid: " << fragment_id;

    std::shared_ptr<FragmentType> fragment =
        std::dynamic_pointer_cast<FragmentType>(client.GetObject(fragment_id));

    MPI_Barrier(comm_spec.comm());

    boost::property_tree::ptree pt;
    std::stringstream ss;
    pt.put("app_class", app_name);
    pt.put("threadNum", threadNum);
    if (getenv("USER_JAR_PATH")) {
      pt.put("jar_name", getenv("USER_JAR_PATH"));
    } else {
      LOG(ERROR) << "JAR_NAME not set";
      return 0;
    }

    if (app_name.find("SSSP") != std::string::npos) {
      CHECK_GE(argc, basic_args + 1);
      pt.put("src", atoi(argv[basic_args + 1]));
    } else if (app_name.find("PageRank") != std::string::npos ||
               app_name.find("Pagerank") != std::string::npos) {
      CHECK_GE(argc, basic_args + 2);
      pt.put("delta", boost::lexical_cast<double>(argv[basic_args + 1]));
      pt.put("maxRound", atoi(argv[basic_args + 2]));
    } else if (app_name.find("Traverse") != std::string::npos) {
      CHECK_GE(argc, basic_args + 1);
      pt.put("maxSteps", atoi(argv[basic_args + 1]));
    } else if (app_name.find("WCC") != std::string::npos) {
    } else if (app_name.find("Bfs") != std::string::npos ||
               app_name.find("BFS") != std::string::npos) {
      CHECK_GE(argc, basic_args + 1);
      pt.put("src", atoi(argv[basic_args + 1]));
    }

    if (run_property) {
      pt.put("frag_name", "gs::ArrowFragmentDefault<int64_t>");
      boost::property_tree::json_parser::write_json(ss, pt);
      std::string user_params = ss.str();
      for (int i = 0; i < query_time; ++i) {
        QueryProperty(client, comm_spec, fragment, app_name, output_prefix,
                      user_params);
      }
    } else {
      std::shared_ptr<ProjectedFragmentType> projected_fragment =
          ProjectedFragmentType::Project(fragment, "0", "0", "0", "2");
      pt.put("frag_name",
             "gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>");
      boost::property_tree::json_parser::write_json(ss, pt);
      std::string user_params = ss.str();
      for (int i = 0; i < query_time; ++i) {
        QueryProjected(client, comm_spec, projected_fragment, app_name,
                       output_prefix, user_params);
      }
    }

    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}
