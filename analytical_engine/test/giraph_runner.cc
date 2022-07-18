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
#include "test/giraph_runner.h"

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

DEFINE_string(vertex_input_format_class, "",
              "java class defines the vertex input format");
DEFINE_string(edge_input_format_class, "",
              "java class defines the edge input format");
DEFINE_string(vertex_output_format_class, "",
              "java class defines the output format");
DEFINE_string(user_app_class, "", "app class to run");
DEFINE_string(vertex_output_path, "", "output file path");
DEFINE_string(master_compute_class, "", "master compute class");
DEFINE_string(aggregator_class, "", "aggregator class");
DEFINE_string(message_combiner_class, "", "combiner for message processing");
DEFINE_string(resolver_class, "", "resolver for graph loading");
DEFINE_string(worker_context_class, "", "worker context");
DEFINE_string(lib_path, "",
              "path for dynamic lib where the desired entry function exists");
DEFINE_string(loading_thread_num, "1",
              "number of threads will be used in loading the graph");
DEFINE_string(efile, "", "path to efile");
DEFINE_string(vfile, "", "path to vfile");
DEFINE_string(giraph_driver_class,
              "com.alibaba.graphscope.app.GiraphComputationAdaptor",
              "the driver app used in java");
// DEFINE_string(java_driver_context,
//              "com.alibaba.graphscope.context.GiraphComputationAdaptorContext",
//              "the driver context used in java");
DEFINE_bool(serialize, false, "whether to serialize loaded graph.");
DEFINE_bool(deserialize, false, "whether to deserialize graph while loading.");
DEFINE_string(serialize_prefix, "",
              "where to load/store the serialization files");
DEFINE_bool(grape_loader, false,
            "whether to use grape loader rather than udf loader");
DEFINE_bool(directed, true, "load direct graph or indirect graph");
DEFINE_string(ipc_socket, "/tmp/vineyard.sock", "vineyard socket");
DEFINE_string(jar_name, "", "user jar name,full path");
DEFINE_string(edge_manager, "default",
              "type of edge manager, default, eager or laze.");
DEFINE_int32(query_times, 1, "How many times to repeat");
DEFINE_string(frag_ids, "", "concatenation of frag strings");
DEFINE_int32(concurrency, 1, "How many thread to invoke when computing");

// put all flags in a json str
std::string flags2JsonStr() {
  boost::property_tree::ptree pt;
  pt.put("vertex_input_format_class", FLAGS_vertex_input_format_class);
  pt.put("edge_input_format_class", FLAGS_edge_input_format_class);
  pt.put("vertex_output_format_class", FLAGS_vertex_output_format_class);
  pt.put("app_class", FLAGS_giraph_driver_class);  // giraph driver class
  pt.put("user_app_class", FLAGS_user_app_class);
  pt.put("vertex_output_path", FLAGS_vertex_output_path);
  pt.put("master_compute_class", FLAGS_master_compute_class);
  pt.put("aggregator_class", FLAGS_aggregator_class);
  pt.put("message_combiner_class", FLAGS_message_combiner_class);
  pt.put("resolver_class", FLAGS_resolver_class);
  pt.put("worker_context_class", FLAGS_worker_context_class);
  pt.put("lib_path", FLAGS_lib_path);
  pt.put("loading_thread_num", FLAGS_loading_thread_num);
  pt.put("efile", FLAGS_efile);
  pt.put("vfile", FLAGS_vfile);
  pt.put("serialize", FLAGS_serialize);
  pt.put("deserialize", FLAGS_deserialize);
  pt.put("serialize_prefix", FLAGS_serialize_prefix);
  pt.put("grape_loader", FLAGS_grape_loader);
  pt.put("directed", FLAGS_directed);
  pt.put("ipc_socket", FLAGS_ipc_socket);
  pt.put("jar_name", FLAGS_jar_name);
  pt.put("edge_manager", FLAGS_edge_manager);
  pt.put("query_times", FLAGS_query_times);
  pt.put("frag_ids", FLAGS_frag_ids);
  pt.put("concurrency", FLAGS_concurrency);

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  return ss.str();
}

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  grape::gflags::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./giraph_runner [options]");
  if (argc == 1) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "giraph-runner");
    exit(1);
  }
  grape::gflags::ParseCommandLineFlags(&argc, &argv, true);
  grape::gflags::ShutDownCommandLineFlags();

  google::InitGoogleLogging("giraph-runner");
  google::InstallFailureSignalHandler();

  VLOG(1) << "Finish option parsing";

  std::string params = flags2JsonStr();
  gs::Init(params);
  gs::CreateAndQuery(params);
  gs::Finalize();
  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
}
