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

#include <csignal>
#include <limits>
#include <thread>
#include <utility>

#include "core/config.h"
#include "core/flags.h"
#include "core/grape_instance.h"
#include "core/launcher.h"
#include "core/server/analytical_server.h"
#include "core/server/dispatcher.h"
#include "core/server/rpc_utils.h"
namespace gs {
/**
 * @brief GrapeEngine is responsible to assemble and start four major
 * components of analytical engine, including VineyardServer, GrapeInstance,
 * Dispatcher and GraphScopeServer.
 */
class GrapeEngine {
 public:
  /**
   * @brief Construct a new GrapeEngine object in service mode, listening at the
   * host:port for incoming rpc requests.
   *
   * @param host
   * @param port
   */
  GrapeEngine(std::string host, int port) : GrapeEngine() {
    if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
      rpc_server_ =
          std::make_shared<rpc::AnalyticalServer>(dispatcher_, host, port);
    }
  }

  /**
   * @brief Construct a new GrapeEngine object in job mode, processing the
   * workflow defined in the dagfile.
   *
   * @param dag_file
   */
  explicit GrapeEngine(std::string dag_file) : GrapeEngine() {
    dag_file_ = std::move(dag_file);
  }

  void Start() {
    vineyard_server_->Start();
    grape_instance_->Init(vineyard_server_->vineyard_socket());
    dispatcher_->Subscribe(grape_instance_);

    if (rpc_server_ != nullptr) {
      service_thread_ = std::thread([this]() { rpc_server_->StartServer(); });
    }
    dispatcher_->Start();
  }

  void Stop() {
    if (rpc_server_) {
      LOG(INFO) << "grape-engine (master) RPC server is stopping...";
      rpc_server_->StopServer();
      service_thread_.join();
    }

    if (dispatcher_) {
      LOG(INFO) << "grape-engine dispatcher is stopping...";
      dispatcher_->Stop();
    }

    if (vineyard_server_) {
      LOG(INFO) << "vineyardd instance is stopping...";
      vineyard_server_->Stop();
    }
  }

  int RunDAGFile() {
    return bl::try_handle_all(
        [this]() -> bl::result<int> {
          BOOST_LEAF_AUTO(dag_def, rpc::ReadDagFromFile(dag_file_));
          for (const auto& op : dag_def.op()) {
            auto cmd = OpToCmd(op);
            dispatcher_->SetCommand(cmd);
          }

          vineyard_server_->Stop();
          return 0;
        },
        [](vineyard::GSError& e) {
          LOG(ERROR) << ErrorCodeToString(e.error_code) << " " << e.error_msg;
          return 1;
        },
        [](const bl::error_info& e) {
          LOG(ERROR) << "Unmatched error" << e;
          return 1;
        });
  }

 private:
  GrapeEngine() {
    comm_spec_.Init(MPI_COMM_WORLD);
    vineyard_server_ = std::make_shared<VineyardServer>(comm_spec_);
    grape_instance_ = std::make_shared<GrapeInstance>(comm_spec_);
    dispatcher_ = std::make_shared<Dispatcher>(comm_spec_);
  }

  grape::CommSpec comm_spec_;
  std::shared_ptr<VineyardServer> vineyard_server_;
  std::shared_ptr<GrapeInstance> grape_instance_;
  std::shared_ptr<Dispatcher> dispatcher_;
  std::shared_ptr<rpc::AnalyticalServer> rpc_server_;
  std::string dag_file_;
  std::thread service_thread_;
};
}  // namespace gs
static std::shared_ptr<gs::GrapeEngine> grape_engine_ptr;

// do proper cleanup in response to signals
void InstallSignalHandlers(void (*signal_handler)(int, siginfo_t*, void*)) {
  struct sigaction sig_action;
  memset(&sig_action, 0, sizeof(sig_action));
  sigemptyset(&sig_action.sa_mask);
  sig_action.sa_flags |= SA_SIGINFO;
  sig_action.sa_sigaction = signal_handler;

  sigaction(SIGTERM, &sig_action, NULL);
  sigaction(SIGINT, &sig_action, NULL);
}

extern "C" void master_signal_handler(int sig, siginfo_t* info, void* context) {
  if (sig == SIGTERM || sig == SIGINT) {
    grape_engine_ptr->Stop();
    //    grape::FinalizeMPIComm();
    std::exit(0);
  }
}

// A custom sinker to redirect glog messages to stdout/stderr.
class RedirectLogSink : public google::LogSink {
  virtual void send(google::LogSeverity severity, const char* full_filename,
                    const char* base_filename, int line,
                    const struct ::tm* tm_time, const char* message,
                    size_t message_len) {
    // we redirect GLOG_ERROR/GLOG_WARNING message to stderr, others to stdout
    if (severity == google::GLOG_ERROR || severity == google::GLOG_WARNING) {
      std::cerr << google::LogSink::ToString(severity, full_filename, line,
                                             tm_time, message, message_len)
                << std::endl;
    } else {
      std::cout << google::LogSink::ToString(severity, full_filename, line,
                                             tm_time, message, message_len)
                << std::endl;
    }
  }
};

int main(int argc, char* argv[]) {
  int exit_code = 0;
  // not output any log to stderr by glog.
  FLAGS_stderrthreshold = std::numeric_limits<int>::max();

  grape::gflags::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./grape_engine [grape_opts]");
  if (argc == 1) {
    grape::gflags::ShowUsageWithFlagsRestrict(argv[0], "core/flags");
    exit(1);
  }
  grape::gflags::ParseCommandLineFlags(&argc, &argv, true);
  grape::gflags::ShutDownCommandLineFlags();

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  RedirectLogSink redirect_log_sink;
  google::AddLogSink(&redirect_log_sink);

  // Init MPI
  grape::InitMPIComm();

  auto host = FLAGS_host;
  auto port = FLAGS_port;
  auto dag_file = FLAGS_dag_file;

  if (dag_file.empty()) {
    grape_engine_ptr = std::make_shared<gs::GrapeEngine>(host, port);
  } else {
    grape_engine_ptr = std::make_shared<gs::GrapeEngine>(dag_file);
  }

  InstallSignalHandlers(&master_signal_handler);
  grape_engine_ptr->Start();

  if (!dag_file.empty()) {
    exit_code = grape_engine_ptr->RunDAGFile();
  }

  grape::FinalizeMPIComm();
  //////////////////////////////////////////////
  google::ShutdownGoogleLogging();
  return exit_code;
}
