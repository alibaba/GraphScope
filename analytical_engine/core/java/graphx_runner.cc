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
#include "core/java/graphx_runner.h"

#include <dlfcn.h>
#include <unistd.h>
#include <memory>
#include <string>
#include <utility>

#include <boost/leaf/error.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"

#include "grape/config.h"
#include "grape/grape.h"
#include "vineyard/client/client.h"

#include "core/java/utils.h"

DEFINE_string(task, "", "load_fragment or graphx_pregel");

DEFINE_string(ipc_socket, "/tmp/vineyard.sock", "vineyard socket addr");
// for fragment loading
DEFINE_string(raw_data_ids, "", "raw data ids");
// for graphx pregel
DEFINE_string(user_lib_path, "/opt/graphscope/lib/libgrape-jni.so",
              "user jni lib");
DEFINE_string(app_class, "com.alibaba.graphscope.app.GraphXParallelAdaptor",
              "graphx driver class");  // graphx_driver_class
DEFINE_string(context_class,
              "com.alibaba.graphscope.context.GraphXParallelAdaptorContext",
              "graphx driver context class");  // graphx_driver_class
DEFINE_string(vd_class, "", "int64_t,int32_t,double,std::string");
DEFINE_string(ed_class, "", "int64_t,int32_t,double,std::string");
DEFINE_string(msg_class, "", "int64_t,int32_t,double,std::string");
DEFINE_int32(max_iterations, 100, "max iterations");
DEFINE_string(frag_ids, "", "frag ids");
DEFINE_string(serial_path, "", "serial path");
DEFINE_string(num_part, "", "num partition in total, specified in graphx");
DEFINE_int32(v_prop_id, 0, "projected vertex property id");
DEFINE_int32(e_prop_id, 0, "projected edge property id");

void run() {
  const char* vd_class_type = FLAGS_vd_class.c_str();
  const char* ed_class_type = FLAGS_ed_class.c_str();
  if (std::strcmp(vd_class_type, "int64_t") == 0) {
    if (std::strcmp(ed_class_type, "int64_t") == 0) {
      gs::Run<int64_t, uint64_t, int64_t, int64_t>();
    } else if (std::strcmp(ed_class_type, "int32_t") == 0) {
      gs::Run<int64_t, uint64_t, int64_t, int32_t>();
    } else if (std::strcmp(ed_class_type, "double") == 0) {
      gs::Run<int64_t, uint64_t, int64_t, double>();
    } else if (std::strcmp(ed_class_type, "std::string") == 0) {
      gs::Run<int64_t, uint64_t, int64_t, std::string>();
    } else {
      LOG(ERROR) << "Unrecognized edata type " << ed_class_type;
    }
  } else if (std::strcmp(vd_class_type, "int32_t") == 0) {
    if (std::strcmp(ed_class_type, "int64_t") == 0) {
      gs::Run<int64_t, uint64_t, int32_t, int64_t>();
    } else if (std::strcmp(ed_class_type, "int32_t") == 0) {
      gs::Run<int64_t, uint64_t, int32_t, int32_t>();
    } else if (std::strcmp(ed_class_type, "double") == 0) {
      gs::Run<int64_t, uint64_t, int32_t, double>();
    } else if (std::strcmp(ed_class_type, "std::string") == 0) {
      gs::Run<int64_t, uint64_t, int32_t, std::string>();
    } else {
      LOG(ERROR) << "Unrecognized edata type " << ed_class_type;
    }
  } else if (std::strcmp(vd_class_type, "double") == 0) {
    if (std::strcmp(ed_class_type, "int64_t") == 0) {
      gs::Run<int64_t, uint64_t, double, int64_t>();
    } else if (std::strcmp(ed_class_type, "int32_t") == 0) {
      gs::Run<int64_t, uint64_t, double, int32_t>();
    } else if (std::strcmp(ed_class_type, "double") == 0) {
      gs::Run<int64_t, uint64_t, double, double>();
    } else if (std::strcmp(ed_class_type, "std::string") == 0) {
      gs::Run<int64_t, uint64_t, double, std::string>();
    } else {
      LOG(ERROR) << "Unrecognized edata type " << ed_class_type;
    }
  } else if (std::strcmp(vd_class_type, "std::string") == 0) {
    if (std::strcmp(ed_class_type, "int64_t") == 0) {
      gs::Run<int64_t, uint64_t, std::string, int64_t>();
    } else if (std::strcmp(ed_class_type, "int32_t") == 0) {
      gs::Run<int64_t, uint64_t, std::string, int32_t>();
    } else if (std::strcmp(ed_class_type, "double") == 0) {
      gs::Run<int64_t, uint64_t, std::string, double>();
    } else if (std::strcmp(ed_class_type, "std::string") == 0) {
      gs::Run<int64_t, uint64_t, std::string, std::string>();
    } else {
      LOG(ERROR) << "Unrecognized edata type " << ed_class_type;
    }
  } else {
    LOG(ERROR) << "current not supported: " << FLAGS_vd_class << ", "
               << FLAGS_ed_class;
  }
}

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  grape::gflags::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./graphx_runner [options]");
  if (argc == 1) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "graphx-runner");
    exit(1);
  }
  grape::gflags::ParseCommandLineFlags(&argc, &argv, true);
  grape::gflags::ShutDownCommandLineFlags();

  google::InitGoogleLogging("graphx-runner");
  google::InstallFailureSignalHandler();

  run();

  google::ShutdownGoogleLogging();
}
