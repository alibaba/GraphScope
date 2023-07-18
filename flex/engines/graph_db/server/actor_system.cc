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

#include "flex/engines/graph_db/server/actor_system.h"

#include <hiactor/core/actor-app.hh>
#include <seastar/core/alien.hh>

#include <iostream>
#include <vector>

namespace server {

actor_system::~actor_system() { terminate(); }

void actor_system::launch_worker() {
  char prog_name[] = "actor_system";
  char affinity[] = "--thread-affinity=false";
  char enable_native_stack[] = "--network-stack=native";
  char close_dhcp[] = "--dhcp=false";
  char ipv4_addr[] = "--host-ipv4-addr=172.24.253.73";
  char gateway[] = "--gw-ipv4-addr=172.24.255.253";
  char net_mask[] = "--netmask-ipv4-addr=255.255.240.0";
  char enable_dpdk[] = "--dpdk-pmd";
  char shards[16];
  snprintf(shards, sizeof(shards), "-c%d", num_shards_);

  std::vector<char*> argv = {prog_name, shards};
  if (enable_dpdk_) {
    argv.push_back(enable_native_stack);
    argv.push_back(close_dhcp);
    argv.push_back(ipv4_addr);
    argv.push_back(gateway);
    argv.push_back(net_mask);
    argv.push_back(enable_dpdk);
  } else {
    argv.push_back(affinity);
  }
  int argc = static_cast<int>(argv.size());

  seastar::app_template::config conf;
  conf.auto_handle_sigint_sigterm = false;
  hiactor::actor_app app{std::move(conf)};
  app.run(argc, argv.data(), [this] {
    sem_post(&ready_);
    return seastar::make_ready_future<>();
  });
}

void actor_system::launch() {
  if (running_.load()) {
    std::cerr << "Actor system is running. "
                 "Trying to launch a new one is not allowed!"
              << std::endl;
    return;
  }
  sem_init(&ready_, 0, 0);
  main_thread_ =
      std::make_unique<std::thread>(&actor_system::launch_worker, this);
  sem_wait(&ready_);
  sem_destroy(&ready_);
  running_.store(true);
}

void actor_system::terminate() {
  if (running_.load()) {
    seastar::alien::run_on(*seastar::alien::internal::default_instance, 0,
                           [] { hiactor::actor_engine().exit(); });
    main_thread_->join();
    running_.store(false);
  }
}

}  // namespace server
