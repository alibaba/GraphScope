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

#include <sys/stat.h>

#include <cstdio>
#include <fstream>
#include <memory>
#include <string>

#include "gflags/gflags.h"

#include "gnn_sampler/kafka_consumer.h"
#include "grape/util.h"
#include "vineyard/client/client.h"

#include "apps/property/sssp_property_append.h"
#include "core/flags.h"
#include "core/loader/append_only_arrow_fragment_loader.h"
#include "core/loader/arrow_fragment_appender.h"

DEFINE_string(efile, "", "edge file");
DEFINE_string(vfile, "", "vertex file");
DEFINE_bool(directed, false, "input graph is directed or not.");
DEFINE_int64(sssp_source, 0, "Source vertex of sssp.");
DEFINE_string(vineyard_socket, "", "Unix domain socket path for vineyardd");
// for append frag
DEFINE_int32(elabel_num, 1, "");
DEFINE_int32(vlabel_num, 1, "");
DEFINE_bool(debug, false, "");

DEFINE_string(input_topic, "append_only_frag", "kafka input topic.");
DEFINE_string(
    broker_list, "localhost:9092",
    "list of kafka brokers, with formati: \'server1:port,server2:port,...\'.");
DEFINE_string(group_id, "grape_consumer", "kafka consumer group id.");
DEFINE_int32(partition_num, 1, "kafka topic partition number.");
DEFINE_int32(batch_size, 10000, "kafka consume messages batch size.");
DEFINE_int64(time_interval, 10, "kafka consume time interval/s");

template <typename FRAG_T>
void RunSSSP(std::shared_ptr<FRAG_T> fragment, const grape::CommSpec& comm_spec,
             const std::string& out_prefix, typename FRAG_T::oid_t src_oid) {
  using AppType = gs::SSSPPropertyAppend<FRAG_T>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  spec.thread_num = 1;
  worker->Init(comm_spec, spec);

  worker->Query(src_oid);

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void read_lines(std::string path,
                std::function<void(std::vector<std::string>&)> cb) {
  std::vector<std::string> buffer;
  std::ifstream in(path);
  std::string line;
  bool first = true;

  while (std::getline(in, line)) {
    if (first) {
      first = false;
      continue;
    }
    buffer.push_back(line);
    if (buffer.size() == (size_t) FLAGS_batch_size) {
      cb(buffer);
      buffer.clear();
    }
  }
  if (!buffer.empty()) {
    cb(buffer);
  }
  in.close();
}

int main(int argc, char** argv) {
  FLAGS_stderrthreshold = 0;

  google::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./run_append_frag <ipc_socket> <efile_prefix> "
      "<e_label_num> <vfile_prefix> <v_label_num> <kafka config>");
  if (argc == 1) {
    google::ShowUsageWithFlagsRestrict(argv[0], "run_append_frag");
    exit(1);
  }

  google::ParseCommandLineFlags(&argc, &argv, true);
  google::ShutDownCommandLineFlags();

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  std::string ipc_socket = FLAGS_vineyard_socket;
  std::string epath = FLAGS_efile;
  int edge_label_num = FLAGS_elabel_num;
  std::string vpath = FLAGS_vfile;
  int vertex_label_num = FLAGS_vlabel_num;

  CHECK(vertex_label_num > 0 && vertex_label_num < 10);
  CHECK(edge_label_num > 0 && edge_label_num < 10);

  grape::InitMPIComm();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    grape::Communicator communicator;
    communicator.InitCommunicator(comm_spec.comm());

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

    vineyard::ObjectID fragment_id;
    using OID_T = vineyard::property_graph_types::OID_TYPE;
    //    using OID_T = std::string;
    using VID_T = vineyard::property_graph_types::VID_TYPE;
    using GraphType = gs::AppendOnlyArrowFragment<OID_T, VID_T>;

    if (FLAGS_debug) {
      volatile int i = 0;
      char hostname[256];
      gethostname(hostname, sizeof(hostname));
      printf("PID %d on %s ready for attach\n", getpid(), hostname);
      fflush(stdout);
      while (0 == i) {
        sleep(1);
      }
    }

    {
      auto loader =
          std::make_unique<gs::AppendOnlyArrowFragmentLoader<OID_T, VID_T>>(
              client, comm_spec, vertex_label_num, edge_label_num, epath, vpath,
              FLAGS_directed);
      fragment_id = boost::leaf::try_handle_all(
          [&loader]() { return loader->LoadFragment(); },
          [](const vineyard::GSError& e) {
            LOG(FATAL) << "Failed to load fragment: " << e.error_msg;
            return 0;
          },
          [](boost::leaf::error_info const& unmatched) {
            LOG(FATAL) << "Unknown failure detected" << unmatched;
            return 0;
          });
      if (fragment_id == 0) {
        return 1;
      }
    }

    LOG(INFO) << "[worker-" << comm_spec.worker_id()
              << "] loaded graph to vineyard ...";

    MPI_Barrier(comm_spec.comm());

    std::shared_ptr<GraphType> fragment =
        std::dynamic_pointer_cast<GraphType>(client.GetObject(fragment_id));

    gs::ArrowFragmentAppender<OID_T, VID_T> appender(comm_spec, fragment);

    mkdir("/tmp/sssp_out", 0777);

    MPI_Barrier(comm_spec.comm());
    {
      auto begin = grape::GetCurrentTime();
      RunSSSP<GraphType>(fragment, comm_spec, "/tmp/sssp_out",
                         FLAGS_sssp_source);
      LOG(INFO) << "SSSP(original) time: " << grape::GetCurrentTime() - begin;
    }

    bool is_coordinator = (comm_spec.worker_id() == grape::kCoordinatorRank);
    std::unique_ptr<KafkaConsumer> consumer;

    if (is_coordinator) {
      consumer = std::make_unique<KafkaConsumer>(
          comm_spec.worker_id(), FLAGS_broker_list, FLAGS_group_id,
          FLAGS_input_topic, FLAGS_partition_num, FLAGS_time_interval,
          FLAGS_batch_size);
    }

    int64_t total_new_edges_num;
    do {
      std::vector<std::vector<std::string>> label_vertex_messages;
      std::vector<std::vector<std::string>> label_edge_messages;

      if (is_coordinator) {
        label_vertex_messages.resize(FLAGS_vlabel_num);
        label_edge_messages.resize(FLAGS_elabel_num);

        std::vector<std::string> vertex_messages;
        std::vector<std::string> edge_messages;

        consumer->ConsumeMessages(vertex_messages, edge_messages);
        // expected message format: vertex msg: "vlabel id,properties..."
        //                          edge   msg: "elabel
        //                          src,dst,src_label,dst_label,properties..."
        for (auto& v_msg : vertex_messages) {
          auto v_label = v_msg[0] - '0';

          CHECK(v_label >= 0 && v_label < FLAGS_vlabel_num);
          label_vertex_messages[v_label].push_back(
              v_msg.substr(2, v_msg.size() - 2));
        }

        for (auto& e_msg : edge_messages) {
          auto e_label = e_msg[0] - '0';

          CHECK(e_label >= 0 && e_label < FLAGS_elabel_num);
          auto line = e_msg.substr(2, e_msg.size() - 2);
          label_edge_messages[e_label].push_back(line);
        }
      }
      {
        auto begin = grape::GetCurrentTime();
        int64_t new_edges_num = boost::leaf::try_handle_all(
            [&] {
              return appender.ExtendFragment(label_vertex_messages,
                                             label_edge_messages, false, ',',
                                             FLAGS_directed);
            },
            [](const vineyard::GSError& e) {
              LOG(FATAL) << e.error_msg;
              return 0;
            },
            [](boost::leaf::error_info const& unmatched) {
              LOG(FATAL) << "Unknown failure detected" << unmatched;
              return 0;
            });
        communicator.Sum(new_edges_num, total_new_edges_num);
        MPI_Barrier(comm_spec.comm());
        if (is_coordinator) {
          LOG(INFO) << "New edges: " << total_new_edges_num
                    << " Expend time: " << grape::GetCurrentTime() - begin;
        }
      }
      {
        auto begin = grape::GetCurrentTime();
        RunSSSP<GraphType>(fragment, comm_spec, "/tmp/sssp_out",
                           FLAGS_sssp_source);
        if (is_coordinator) {
          LOG(INFO) << "SSSP(appended) time: "
                    << grape::GetCurrentTime() - begin;
        }
      }
    } while (total_new_edges_num > 0);
  }

  grape::FinalizeMPIComm();
  return 0;
}
