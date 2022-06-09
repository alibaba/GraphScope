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

#include "vineyard_store_test_env.h"

#include <stdlib.h>
#include <cstdio>
#include <string>
#include <system_error>
#include <thread>
#include <chrono>
#include <vector>
#include <random>
#include <algorithm>

#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>

#include "vineyard/client/client.h"
#include <grape/communication/sync_comm.h>

using std::vector;
using std::string;

namespace vineyard_store_test {

static bool CheckPortInUse(ushort port) {
  // support linux only
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock == -1) {
    return false;
  }
  bool in_use = false;
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  addr.sin_port = htons(port);
  if (bind(sock, (struct sockaddr*)(&addr), sizeof(addr)) < 0) {
    in_use = true;
  }
  close(sock);
  return in_use;
}

static int FindUnusedPort(vector<int> used_ports) {
  std::random_device rd{};
  std::mt19937_64 rand_engine(rd());
  const ushort min_port = 10000;
  const ushort max_port = 60000;
  std::uniform_int_distribution<ushort> dist(min_port, max_port);
  const int tries = 10;
  for (int i = 0; i < tries; i++) {
    ushort port = dist(rand_engine);
    if (std::find(used_ports.cbegin(), used_ports.cend(), port) != used_ports.cend()) {
      continue;
    }
    if (!CheckPortInUse(port)) {
      return port;
    }
  }
  return 0;
}

void VineyardStoreTestEnv::SetUp() {
  Environment::SetUp();
  grape::InitMPIComm();
  StartVineyardd();
}

void VineyardStoreTestEnv::TearDown() {
  {
    // terminate the child processes
    boost::process::group vineyardd_group_(std::move(vineyardd_group_));
    boost::process::child etcd_proc(std::move(etcd_proc_));
    boost::process::child vineyardd_proc(std::move(vineyardd_proc_));
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));
  if (!test_root_dir_.empty()) {
    boost::system::error_code boost_ec;
    boost::filesystem::remove_all(test_root_dir_, boost_ec);
    test_root_dir_.clear();
  }
  grape::FinalizeMPIComm();
  Environment::TearDown();
}

void VineyardStoreTestEnv::StartVineyardd() {
  //TODO: make etcd, vineyardd location configurable
  auto etcd_exe = boost::process::search_path("etcd").native();
  auto vineyardd_exe = boost::process::search_path("vineyardd").native();
  boost::system::error_code boost_ec;
  auto tmp_dir = boost::filesystem::temp_directory_path(boost_ec);
  ASSERT_TRUE(!boost_ec) << "boost::filesystem::temp_directory_path() failed."
      << " error code: " << boost_ec.value()
      << " error message: " << boost_ec.message();
  auto test_dir = boost::filesystem::unique_path("gstest_%%%%%%%%", boost_ec);
  ASSERT_TRUE(!boost_ec) << "boost::filesystem::unique_path() failed."
      << " error code: " << boost_ec.value()
      << " error message: " << boost_ec.message();
  auto test_root_dir = tmp_dir / test_dir;
  ASSERT_TRUE(boost::filesystem::create_directory(test_root_dir, boost_ec))
      << "Failed to create directory \"" << test_root_dir.native()
      << " error code: " << boost_ec.value()
      << " error message: " << boost_ec.message();

  vector<int> used_ports;
  int peer_port = FindUnusedPort(used_ports);
  ASSERT_NE(peer_port, 0) << "Failed to find a free port as etcd peer port";
  used_ports.push_back(peer_port);
  int client_port = FindUnusedPort(used_ports);
  ASSERT_NE(client_port, 0) << "Failed to find a free port as etcd client port";
  used_ports.push_back(client_port);
  boost::process::group vineyardd_group;
  vector<string> etcd_args;
  etcd_args.push_back("--data-dir");
  etcd_args.push_back((test_root_dir / "etcd").native());
  etcd_args.push_back("--listen-peer-urls");
  etcd_args.push_back("http://127.0.0.1:" + std::to_string(peer_port));
  etcd_args.push_back("--listen-client-urls");
  etcd_args.push_back("http://127.0.0.1:" + std::to_string(client_port));
  etcd_args.push_back("--advertise-client-urls");
  etcd_args.push_back("http://127.0.0.1:" + std::to_string(client_port));
  etcd_args.push_back("--initial-cluster");
  etcd_args.push_back("default=http://127.0.0.1:" + std::to_string(peer_port));
  etcd_args.push_back("--initial-advertise-peer-urls");
  etcd_args.push_back("http://127.0.0.1:" + std::to_string(peer_port));
  std::error_code std_ec;
  boost::process::child etcd_proc(etcd_exe, etcd_args, vineyardd_group, std_ec);
  ASSERT_TRUE(!std_ec) << "fails to start '" << etcd_exe
      << "' error code: " << std_ec.value()
      << " error message: " << std_ec.message();
  //TODO: make max_tries configurable
  const int max_tries = 30;
  bool etcd_ready = false;
  auto etcdctl_exe = boost::process::search_path("etcdctl").native();
  std::vector<string> etcdctl_args;
  etcdctl_args.push_back("--endpoints");
  etcdctl_args.push_back("http://127.0.0.1:" + std::to_string(client_port));
  etcdctl_args.push_back("get");
  etcdctl_args.push_back("/");
  etcdctl_args.push_back("--prefix");
  etcdctl_args.push_back("--keys-only");
  etcdctl_args.push_back("--limit");
  etcdctl_args.push_back("1");
  for (int i = 0; i < max_tries; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto etcdctl_env = boost::this_process::environment();
    etcdctl_env["ETCDCTL_API"]="3";
    boost::process::child etcdctl_proc(etcdctl_env, etcdctl_exe, etcdctl_args, std_ec);
    ASSERT_TRUE(!std_ec) << "fails to start '" << etcdctl_exe
        << "' error code: " << std_ec.value()
        << " error message: " << std_ec.message();
    etcdctl_proc.wait(std_ec);
    ASSERT_TRUE(!std_ec) << "fails to wait for etcdctl exit_code."
        << " error code: " << std_ec.value()
        << " error message: " << std_ec.message();
    if (etcdctl_proc.native_exit_code() == 0) {
      etcd_ready = true;
      break;
    }
  }
  ASSERT_TRUE(etcd_ready)  << "fails to connect to etcd ";
  std::vector<string> vineyardd_args;
  vineyardd_args.push_back("--socket");
  auto test_socket_path = test_root_dir / "vineyard_store_test.sock";
  vineyardd_args.push_back(test_socket_path.native());
  vineyardd_args.push_back("--norpc");
  vineyardd_args.push_back("--logtostderr=1");
  vineyardd_args.push_back("--etcd_endpoint");
  vineyardd_args.push_back("http://127.0.0.1:" + std::to_string(client_port));
  boost::process::child vineyardd_proc(vineyardd_exe, vineyardd_args, vineyardd_group, std_ec);
  ASSERT_TRUE(!std_ec) << "fails to start '" << vineyardd_exe
      << "' error code: " << std_ec.value()
      << " error message: " << std_ec.message();
  string test_socket_path_str = test_socket_path.string();
  bool vineyardd_ready = false;
  for (int i = 0; i < max_tries; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    vineyard::Client client;
    auto status = client.Connect(test_socket_path_str);
    if (status.ok()) {
      vineyardd_ready = true;
      break;
    }
  }
  ASSERT_TRUE(vineyardd_ready) << "fails to connect to vineyardd.";

  auto default_client_env_variable = "VINEYARD_IPC_SOCKET";
  auto env = boost::this_process::environment();
  ASSERT_NO_THROW(env[default_client_env_variable] = test_socket_path_str.c_str())
      << "set env " << default_client_env_variable << " to "
      << test_socket_path.c_str() << " fails.";
  etcd_proc_ = std::move(etcd_proc);
  vineyardd_proc_ = std::move(vineyardd_proc);
  vineyardd_group_ = std::move(vineyardd_group);
  test_root_dir_ = std::move(test_root_dir);
}


} // namespace vineyard_store