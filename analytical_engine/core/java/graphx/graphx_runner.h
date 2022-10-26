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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_RUNNER_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_RUNNER_H_

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
#include "grape/grape.h"
#include "grape/util.h"

#include <boost/asio.hpp>
#include <boost/property_tree/ptree.hpp>

#include "apps/java_pie/java_pie_projected_default_app.h"
#include "apps/java_pie/java_pie_projected_parallel_app.h"
#include "core/io/property_parser.h"
#include "core/java/graphx/graphx_fragment.h"
#include "core/java/javasdk.h"
#include "core/java/utils.h"
#include "core/loader/arrow_fragment_loader.h"

DECLARE_string(task);
DECLARE_string(ipc_socket);
DECLARE_string(local_vm_ids);
DECLARE_string(user_lib_path);
DECLARE_string(app_class);      // graphx_driver_class
DECLARE_string(context_class);  // graphx_driver_class
DECLARE_string(vd_class);
DECLARE_string(ed_class);
DECLARE_string(msg_class);
DECLARE_string(frag_ids);
DECLARE_int32(max_iterations);
DECLARE_string(serial_path);
DECLARE_string(num_part);

namespace gs {

void Init() {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
}
void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(10) << "Workers finalized.";
}
std::string getHostName() { return boost::asio::ip::host_name(); }

vineyard::ObjectID splitAndGet(grape::CommSpec& comm_spec,
                               const std::string& ids) {
  std::vector<std::string> splited;
  boost::split(splited, ids, boost::is_any_of(","));
  CHECK_EQ(splited.size(), comm_spec.worker_num());
  auto my_host_name = getHostName();
  std::vector<std::string> pid_vineyard_id;
  {
    for (auto str : splited) {
      if (str.find(my_host_name) != std::string::npos) {
        auto trimed =
            str.substr(str.find(my_host_name) + my_host_name.size() + 1);
        pid_vineyard_id.push_back(trimed);
      }
    }
  }
  CHECK_EQ(pid_vineyard_id.size(), comm_spec.local_num());
  vineyard::ObjectID res_id;
  int graphx_pid;

  {
    std::vector<std::string> graphx_pid_vm_id;
    boost::split(graphx_pid_vm_id, pid_vineyard_id[comm_spec.local_id()],
                 boost::is_any_of(":"));
    CHECK_EQ(graphx_pid_vm_id.size(), 2);
    res_id = std::stoull(graphx_pid_vm_id[1]);
    graphx_pid = std::stoi(graphx_pid_vm_id[0]);
  }

  LOG(INFO) << "worker [" << comm_spec.worker_id() << "], local id ["
            << comm_spec.local_id() << "] got pid " << graphx_pid << ", id "
            << res_id;
  return res_id;
}
template <typename OID_T, typename VID_T>
void LoadGraphXVertexMapImpl(const std::string local_vm_ids_str,
                             vineyard::Client& client) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  std::vector<std::string> splited;
  boost::split(splited, local_vm_ids_str, boost::is_any_of(","));
  CHECK_EQ(splited.size(), comm_spec.worker_num());

  vineyard::ObjectID global_vm_id;
  int graphx_pid;
  {
    std::string host_name = getHostName();
    std::vector<std::string> local_vm_ids;
    for (auto raw : splited) {
      if (raw.find(host_name) != std::string::npos) {
        // shoud find first
        local_vm_ids.push_back(
            raw.substr(raw.find(":") + 1, std::string::npos));
      }
    }
    if (local_vm_ids.size() == 0) {
      LOG(ERROR) << "Worker [" << comm_spec.worker_id() << "](" + host_name
                 << ") find no suitable ids from" << local_vm_ids_str;
      return;
    }
    CHECK_EQ(local_vm_ids.size(), comm_spec.local_num());

    vineyard::ObjectID partial_map;

    {
      std::vector<std::string> graphx_pid_vm_id;
      boost::split(graphx_pid_vm_id, local_vm_ids[comm_spec.local_id()],
                   boost::is_any_of(":"));
      CHECK_EQ(graphx_pid_vm_id.size(), 2);
      partial_map = std::stoull(graphx_pid_vm_id[1]);
      graphx_pid = std::stoi(graphx_pid_vm_id[0]);
    }

    gs::BasicGraphXVertexMapBuilder<int64_t, uint64_t> builder(
        client, comm_spec, graphx_pid, partial_map);

    global_vm_id = builder.Seal(client)->id();
    VINEYARD_CHECK_OK(client.Persist(global_vm_id));
  }
  LOG(INFO) << "GlobalVertexMapID:" << getHostName() << ":" << graphx_pid << ":"
            << global_vm_id;
}

template <typename OID_T, typename VID_T>
void LoadGraphXVertexMap(const std::string local_vm_ids_str,
                         vineyard::Client& client) {
  gs::Init();
  LoadGraphXVertexMapImpl<OID_T, VID_T>(local_vm_ids_str, client);
  gs::Finalize();
}

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
vineyard::ObjectID LoadFragment(vineyard::Client& client,
                                grape::CommSpec& comm_spec,
                                std::string& frag_ids) {
  auto cur_frag_id = splitAndGet(comm_spec, frag_ids);
  LOG(INFO) << "Worker [" << comm_spec.worker_id()
            << "] got graphx fragment from id: " << cur_frag_id;
  return cur_frag_id;
}

template <typename FRAG_T, typename APP_TYPE>
void Query(grape::CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
           const std::string& params_str, const std::string& user_lib_path) {
  auto app = std::make_shared<APP_TYPE>();
  auto worker = APP_TYPE::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();

  worker->Init(comm_spec, spec);

  MPI_Barrier(comm_spec.comm());
  double t = -grape::GetCurrentTime();
  worker->Query(params_str, user_lib_path);
  t += grape::GetCurrentTime();
  MPI_Barrier(comm_spec.comm());
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Query time cost: " << t;
  }

  std::ofstream unused_stream;
  unused_stream.open("empty");
  worker->Output(unused_stream);
  unused_stream.close();
}

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
void CreateAndQuery(std::string params, const std::string& frag_name) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  using GraphXFragmentType = gs::GraphXFragment<OID_T, VID_T, VD_T, ED_T>;
  boost::property_tree::ptree pt;
  string2ptree(params, pt);

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(FLAGS_ipc_socket));
  // VLOG(1) << "Connected to IPCServer: " << FLAGS_ipc_socket;

  auto fragment_id =
      LoadFragment<OID_T, VID_T, VD_T, ED_T>(client, comm_spec, FLAGS_frag_ids);

  VLOG(10) << "[worker " << comm_spec.worker_id()
           << "] loaded frag id: " << fragment_id;

  double t = -grape::GetCurrentTime();
  std::shared_ptr<GraphXFragmentType> fragment =
      std::dynamic_pointer_cast<GraphXFragmentType>(
          client.GetObject(fragment_id));
  t += grape::GetCurrentTime();
  VLOG(10) << "Work [" << comm_spec.worker_id() << " load fragment cost: " << t
           << " second";

  // As the comm_spec world in this mpirun run may differs from the old
  // fid->comm_spec mapping, we need to know the mapping by gathering info.
  {
    std::vector<int32_t> worker_id_to_fid;
    worker_id_to_fid.resize(comm_spec.fnum());
    auto fid = fragment->fid();
    MPI_Allgather(&fid, 1, MPI_INT, worker_id_to_fid.data(), 1, MPI_INT,
                  comm_spec.comm());
    std::stringstream ss;
    for (grape::fid_t i = 0; i < comm_spec.fnum(); ++i) {
      ss << ";" << i << ":" << worker_id_to_fid[i];
    }
    auto worker_id_to_fid_str = ss.str().substr(1);
    // LOG(INFO) << "worker_id_to_fid_str: " << worker_id_to_fid_str;
    pt.put("worker_id_to_fid", worker_id_to_fid_str);
  }

  pt.put("frag_name", frag_name);

  if (getenv("USER_JAR_PATH")) {
    pt.put("jar_name", getenv("USER_JAR_PATH"));
  } else {
    LOG(ERROR) << "USER_JAR_PATH not set";
    return;
  }

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  std::string new_params = ss.str();

  double t0 = grape::GetCurrentTime();

  for (int i = 0; i < 1; ++i) {
    if (FLAGS_context_class ==
        "com.alibaba.graphscope.context.GraphXParallelAdaptorContext") {
      using APP_TYPE = JavaPIEProjectedParallelAppIE<GraphXFragmentType>;
      Query<GraphXFragmentType, APP_TYPE>(comm_spec, fragment, new_params,
                                          FLAGS_user_lib_path);
    } else {
      LOG(ERROR) << "Not recegonized context clz" << FLAGS_context_class;
    }
  }
  double t1 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "[Total Query time]: " << (t1 - t0);
  }
}

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
void Run(std::string& params) {
  std::string frag_name = "gs::GraphXFragment<" + gs::TypeName<OID_T>::Get() +
                          "," + gs::TypeName<VID_T>::Get() + "," +
                          gs::TypeName<VD_T>::Get() + "," +
                          gs::TypeName<ED_T>::Get() + ">";

  gs::Init();
  gs::CreateAndQuery<OID_T, VID_T, VD_T, ED_T>(params, frag_name);
  gs::Finalize();
}
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_RUNNER_H_
