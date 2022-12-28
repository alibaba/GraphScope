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

#ifndef ANALYTICAL_ENGINE_TEST_GIRAPH_RUNNER_H_
#define ANALYTICAL_ENGINE_TEST_GIRAPH_RUNNER_H_

#ifdef ENABLE_JAVA_SDK

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "grape/config.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/fragment/loader.h"
#include "grape/grape.h"

#include "apps/java_pie/java_pie_projected_default_app.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/io/property_parser.h"
#include "core/java/utils.h"
#include "core/loader/arrow_fragment_loader.h"

namespace bl = boost::leaf;

namespace gs {

using FragmentType =
    vineyard::ArrowFragment<int64_t, vineyard::property_graph_types::VID_TYPE>;
using ProjectedFragmentType =
    ArrowProjectedFragment<int64_t, uint64_t, int64_t, int64_t>;

using FragmentLoaderType =
    ArrowFragmentLoader<int64_t, vineyard::property_graph_types::VID_TYPE>;
using APP_TYPE = JavaPIEProjectedDefaultApp<ProjectedFragmentType>;
// using LOADER_TYPE = grape::GiraphFragmentLoader<FragmentType>;

void Init(const std::string& params) {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }
}

vineyard::ObjectID LoadGiraphFragment(
    const grape::CommSpec& comm_spec, const std::string& vfile,
    const std::string& efile, const std::string& vertex_input_format_class,
    const std::string& edge_input_format_class, vineyard::Client& client,
    bool directed) {
  // construct graph info
  auto graph = std::make_shared<detail::Graph>();
  graph->directed = directed;
  graph->generate_eid = false;

  auto vertex = std::make_shared<detail::Vertex>();
  vertex->label = "label1";
  vertex->vid = "0";
  vertex->protocol = "file";
  vertex->values = vfile;
  vertex->vformat = vertex_input_format_class;  // vif

  graph->vertices.push_back(vertex);

  auto edge = std::make_shared<detail::Edge>();
  edge->label = "label2";
  auto subLabel = std::make_shared<detail::Edge::SubLabel>();
  subLabel->src_label = "label1";
  subLabel->src_vid = "0";
  subLabel->dst_label = "label1";
  subLabel->dst_vid = "0";
  subLabel->protocol = "file";
  subLabel->values = efile;
  subLabel->eformat += edge_input_format_class;  // eif
  edge->sub_labels.push_back(*subLabel.get());

  graph->edges.push_back(edge);

  vineyard::ObjectID fragment_id;
  {
    auto loader =
        std::make_unique<FragmentLoaderType>(client, comm_spec, graph);
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

  VLOG(1) << "[worker-" << comm_spec.worker_id() << "] got fragment "
          << fragment_id;
  return fragment_id;
}

template <typename FRAG_T>
void Query(grape::CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
           const std::string& params_str, const std::string user_lib_path,
           int query_times) {
  std::vector<double> query_time(query_times, 0.0);
  double total_time = 0.0;

  for (auto i = 0; i < query_times; ++i) {
    auto app = std::make_shared<APP_TYPE>();
    auto worker = APP_TYPE::CreateWorker(app, fragment);
    auto spec = grape::DefaultParallelEngineSpec();

    worker->Init(comm_spec, spec);

    MPI_Barrier(comm_spec.comm());
    double t = -grape::GetCurrentTime();
    worker->Query(params_str, user_lib_path);
    t += grape::GetCurrentTime();
    MPI_Barrier(comm_spec.comm());

    total_time += t;
    query_time[i] = t;
    std::ofstream unused_stream;
    unused_stream.open("empty");
    worker->Output(unused_stream);
    unused_stream.close();
  }

  //
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Total Query time: " << total_time << " times: " << query_times;
    VLOG(1) << "Average Query time"
            << (total_time / static_cast<double>(query_times));
  }

  std::ostringstream oss;

  if (!query_time.empty()) {
    // Convert all but the last element to avoid a trailing ","
    std::copy(query_time.begin(), query_time.end() - 1,
              std::ostream_iterator<double>(oss, ","));
    oss << query_time.back();
  }
  VLOG(1) << "Separate time: " << oss.str();
}

void CreateAndQuery(std::string params) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  ptree pt;
  string2ptree(params, pt);

  std::string ipc_socket = getFromPtree<std::string>(pt, OPTION_IPC_SOCKET);
  vineyard::Client client;
  vineyard::ObjectID fragment_id;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));
  VLOG(1) << "Connected to IPCServer: " << ipc_socket;

  std::string frag_ids = getFromPtree<std::string>(pt, OPTION_FRAG_IDS);
  std::vector<vineyard::ObjectID> frag_ids_vec;
  if (!frag_ids.empty()) {
    std::stringstream ss(frag_ids);
    for (vineyard::ObjectID frag_id; ss >> frag_id;) {
      frag_ids_vec.push_back(frag_id);
      if (ss.peek() == ',') {
        ss.ignore();
      }
    }
    fragment_id = frag_ids_vec[comm_spec.worker_id()];
    VLOG(1) << "[worker " << comm_spec.worker_id()
            << "] parsed frag id: " << fragment_id;
  } else {
    std::string efile = getFromPtree<std::string>(pt, OPTION_EFILE);
    std::string vfile = getFromPtree<std::string>(pt, OPTION_VFILE);

    std::string vertex_input_format_class =
        getFromPtree<std::string>(pt, OPTION_VERTEX_INPUT_FORMAT_CLASS);
    std::string edge_input_format_class =
        getFromPtree<std::string>(pt, OPTION_EDGE_INPUT_FORMAT_CLASS);

    bool directed = getFromPtree<bool>(pt, OPTION_DIRECTED);
    VLOG(10) << "efile: " << efile << ", vfile: " << vfile
             << " vifc: " << vertex_input_format_class
             << "directed: " << directed
             << ", vif: " << vertex_input_format_class
             << ", eif: " << edge_input_format_class;
    if (efile.empty() || vfile.empty()) {
      LOG(FATAL) << "Make sure efile and vfile are avalibale";
    }
    fragment_id =
        LoadGiraphFragment(comm_spec, vfile, efile, vertex_input_format_class,
                           edge_input_format_class, client, directed);
    VLOG(10) << "[worker " << comm_spec.worker_id()
             << "] loaded frag id: " << fragment_id;
  }
  int query_times = getFromPtree<int>(pt, OPTION_QUERY_TIMES);

  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(fragment_id));

  VLOG(10) << "fid: " << fragment->fid() << "fnum: " << fragment->fnum()
           << "v label num: " << fragment->vertex_label_num()
           << "e label num: " << fragment->edge_label_num()
           << "total v num: " << fragment->GetTotalVerticesNum();
  VLOG(1) << "inner vertices: " << fragment->GetInnerVerticesNum(0);

  std::string frag_name =
      "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>";
  pt.put("frag_name", frag_name);

  std::string jar_name;
  if (getenv("USER_JAR_PATH")) {
    jar_name = getenv("USER_JAR_PATH");
  } else {
    LOG(ERROR) << "JAR_NAME not set";
    return;
  }
  if (getenv("GIRAPH_JAR_PATH")) {
    jar_name += ":";
    jar_name += getenv("GIRAPH_JAR_PATH");
  } else {
    LOG(FATAL) << "GIRAPH_JAR_PATH not set";
    return;
  }
  pt.put("jar_name", jar_name);

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  std::string new_params = ss.str();

  std::string user_lib_path = getFromPtree<std::string>(pt, OPTION_LIB_PATH);

  // Project
  std::shared_ptr<ProjectedFragmentType> projected_fragment =
      ProjectedFragmentType::Project(fragment, 0, 0, 0, 0);

  Query<ProjectedFragmentType>(comm_spec, projected_fragment, new_params,
                               user_lib_path, query_times);
}
void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_TEST_GIRAPH_RUNNER_H_
