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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_RUNNER_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_RUNNER_H_

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

#include "gflags/gflags.h"
#include "grape/config.h"
#include "grape/grape.h"
#include "grape/util.h"

#include <boost/property_tree/ptree.hpp>

#include "apps/java_pie/java_pie_projected_default_app.h"
#include "apps/java_pie/java_pie_projected_parallel_app.h"
#include "core/io/property_parser.h"
#include "core/java/graphx_loader.h"
#include "core/java/javasdk.h"
#include "core/java/utils.h"
#include "core/loader/arrow_fragment_loader.h"

DECLARE_string(task);
DECLARE_string(ipc_socket);
DECLARE_string(user_lib_path);
DECLARE_string(app_class);      // graphx_driver_class
DECLARE_string(context_class);  // graphx_driver_class
DECLARE_string(vd_class);
DECLARE_string(ed_class);
DECLARE_string(msg_class);
DECLARE_string(raw_data_ids);
DECLARE_string(frag_ids);
DECLARE_int32(max_iterations);
DECLARE_string(serial_path);
DECLARE_string(num_part);
DECLARE_int32(v_prop_id);
DECLARE_int32(e_prop_id);

namespace gs {

std::string getHostName() { return boost::asio::ip::host_name(); }

void Init() {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(10) << "Workers finalized.";
}

std::string build_generic_class(const std::string& base_class,
                                const std::string& vd_class,
                                const std::string& ed_class,
                                const std::string& msg_class) {
  std::stringstream ss;
  ss << base_class << "<" << vd_class << "," << ed_class << "," << msg_class
     << ">";
  return ss.str();
}

// put all flags in a json str
boost::property_tree::ptree flags2Ptree() {
  boost::property_tree::ptree pt;
  if (FLAGS_user_lib_path.empty()) {
    LOG(ERROR) << "user jni lib not set";
  }
  pt.put("user_lib_path", FLAGS_user_lib_path);
  // Different from other type of apps, we need to specify
  // vd and ed type in app_class for generic class creations
  pt.put("app_class", build_generic_class(FLAGS_app_class, FLAGS_vd_class,
                                          FLAGS_ed_class, FLAGS_msg_class));
  pt.put("graphx_context_class",
         build_generic_class(FLAGS_context_class, FLAGS_vd_class,
                             FLAGS_ed_class, FLAGS_msg_class));
  pt.put("msg_class", FLAGS_msg_class);
  pt.put("vd_class", FLAGS_vd_class);
  pt.put("ed_class", FLAGS_ed_class);
  pt.put("max_iterations", FLAGS_max_iterations);
  pt.put("serial_path", FLAGS_serial_path);
  pt.put("num_part", FLAGS_num_part);
  return pt;
}

std::pair<vineyard::ObjectID, int> splitAndGet(grape::CommSpec& comm_spec,
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
  return std::make_pair(res_id, graphx_pid);
}

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
void LoadFragment(vineyard::Client& client, grape::CommSpec& comm_spec) {
  auto pair = splitAndGet(comm_spec, FLAGS_raw_data_ids);
  auto cur_raw_data_id = pair.first;
  auto graphx_pid = pair.second;
  std::vector<int> pid2Fid;
  {
    std::vector<int> fid2Pid;
    vineyard::GlobalAllGatherv(graphx_pid, fid2Pid, comm_spec);
    pid2Fid.resize(fid2Pid.size());
    for (size_t i = 0; i < fid2Pid.size(); ++i) {
      pid2Fid[fid2Pid[i]] = i;
    }
  }

  LOG(INFO) << "Worker [" << comm_spec.worker_id()
            << "] got raw data id: " << cur_raw_data_id
            << ", graphx pid: " << graphx_pid;
  // Load Fragment.
  gs::GraphXPartitioner<OID_T> partitioner;
  partitioner.Init(pid2Fid);
  gs::GraphXLoader<OID_T, VID_T, VD_T, ED_T> loader(cur_raw_data_id, client,
                                                    comm_spec, partitioner);
  auto arrow_frag_id = boost::leaf::try_handle_all(
      [&loader]() { return loader.LoadFragment(); },
      [](const vineyard::GSError& e) {
        LOG(FATAL) << e.error_msg;
        return 0;
      },
      [](const boost::leaf::error_info& unmatched) {
        LOG(FATAL) << "Unmatched error " << unmatched;
        return 0;
      });
  LOG(INFO) << "Got arrow fragment id: " << arrow_frag_id;
  std::shared_ptr<vineyard::ArrowFragment<OID_T, VID_T>> arrowFragment =
      std::dynamic_pointer_cast<vineyard::ArrowFragment<OID_T, VID_T>>(
          client.GetObject(arrow_frag_id));
  // project
  using ProjectedFragmentType =
      typename gs::ArrowProjectedFragment<OID_T, VID_T, VD_T, ED_T>;
  auto v_prop_num = arrowFragment->vertex_property_num(0);
  auto e_prop_num = arrowFragment->edge_property_num(0);
  LOG(INFO) << "vprop num " << v_prop_num << ", e prop num: " << e_prop_num;
  std::shared_ptr<ProjectedFragmentType> projectedFragment =
      ProjectedFragmentType::Project(arrowFragment, 0, FLAGS_v_prop_id, 0,
                                     FLAGS_e_prop_id);
  LOG(INFO) << gs::LOAD_FRAGMENT_RES_PREFIX << ":" << getHostName() << ":"
            << graphx_pid << ":" << projectedFragment->id();
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
void RunGraphX(vineyard::Client& client, grape::CommSpec& comm_spec,
               std::string& frag_name) {
  using ProjectedFragmentType =
      typename gs::ArrowProjectedFragment<OID_T, VID_T, VD_T, ED_T>;
  boost::property_tree::ptree pt = flags2Ptree();

  auto pair = splitAndGet(comm_spec, FLAGS_frag_ids);
  auto cur_frag_id = pair.first;
  auto graphx_pid_id = pair.second;
  LOG(INFO) << " graphx pid: " << graphx_pid_id << " fid " << comm_spec.fid()
            << " frag id " << cur_frag_id;

  std::shared_ptr<ProjectedFragmentType> fragment =
      std::dynamic_pointer_cast<ProjectedFragmentType>(
          client.GetObject(cur_frag_id));

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
  // get params str
  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  std::string new_params = ss.str();

  double t0 = grape::GetCurrentTime();

  for (int i = 0; i < 1; ++i) {
    if (FLAGS_context_class ==
        "com.alibaba.graphscope.context.GraphXParallelAdaptorContext") {
      using APP_TYPE = JavaPIEProjectedParallelAppIE<ProjectedFragmentType>;
      LOG(INFO) << "Message strategy: "
                << (APP_TYPE::message_strategy ==
                    grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex);
      Query<ProjectedFragmentType, APP_TYPE>(comm_spec, fragment, new_params,
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
void Run() {
  std::string frag_name =
      "gs::ArrowProjectedFragment<" + gs::TypeName<OID_T>::Get() + "," +
      gs::TypeName<VID_T>::Get() + "," + gs::TypeName<VD_T>::Get() + "," +
      gs::TypeName<ED_T>::Get() + ">";
  gs::Init();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);
    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(FLAGS_ipc_socket));

    if (std::strcmp(FLAGS_task.c_str(), gs::LOAD_FRAGMENT) == 0) {
      gs::LoadFragment<OID_T, VID_T, VD_T, ED_T>(client, comm_spec);
    } else if (std::strcmp(FLAGS_task.c_str(), gs::GRAPHX_PREGEL_TASK) == 0) {
      gs::RunGraphX<OID_T, VID_T, VD_T, ED_T>(client, comm_spec, frag_name);
    } else {
      LOG(ERROR) << "Unrecognized task: " << FLAGS_task;
    }
  }
  gs::Finalize();
}
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_RUNNER_H_
