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

#include "test/run_app.h"

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

DEFINE_string(datasource, "local",
              "datasource type, available options: local, odps, oss");
DEFINE_string(jobid, "", "jobid, only used in LDBC graphanalytics.");
DEFINE_int32(app_concurrency, -1, "concurrency of the application.");

void Init() {
  if (FLAGS_out_prefix.empty()) {
    LOG(FATAL) << "Please assign an output prefix.";
  }
  if (FLAGS_deserialize && FLAGS_serialization_prefix.empty()) {
    LOG(FATAL) << "Please assign a serialization prefix.";
  } else if (FLAGS_vfile.empty() || FLAGS_efile.empty()) {
    LOG(FATAL) << "Please assign input vertex/edge files.";
  }

  if (access(FLAGS_out_prefix.c_str(), 0) != 0) {
    mkdir(FLAGS_out_prefix.c_str(), 0777);
  }

  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  grape::gflags::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./run_app [grape_opts]");
  if (argc == 1) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "analytical_apps");
    exit(1);
  }
  grape::gflags::ParseCommandLineFlags(&argc, &argv, true);
  grape::gflags::ShutDownCommandLineFlags();

  google::InitGoogleLogging("analytical_apps");
  google::InstallFailureSignalHandler();

  Init();

  std::string name = FLAGS_application;
  if (name.find("sssp") != std::string::npos ||
      name.find("eigenvector") != std::string::npos) {
    if (FLAGS_segmented_partition) {
      gs::Run<int64_t, uint32_t, grape::EmptyType, double,
              grape::SegmentedPartitioner<int64_t>>();
    } else {
      gs::Run<int64_t, uint32_t, grape::EmptyType, double,
              grape::HashPartitioner<int64_t>>();
    }
  } else {
    if (FLAGS_segmented_partition) {
      gs::Run<int64_t, uint32_t, grape::EmptyType, grape::EmptyType,
              grape::SegmentedPartitioner<int64_t>>();
    } else {
      gs::Run<int64_t, uint32_t, grape::EmptyType, grape::EmptyType,
              grape::HashPartitioner<int64_t>>();
    }
  }

  Finalize();

  google::ShutdownGoogleLogging();
}
