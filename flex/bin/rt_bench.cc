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

#include "grape/util.h"

#include <boost/program_options.hpp>
#include <chrono>
#include <fstream>
#include <hiactor/core/actor-app.hh>
#include <iostream>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/generated/actor/executor_ref.act.autogen.h"
#include "flex/engines/http_server/service/graph_db_service.h"

namespace bpo = boost::program_options;
using namespace std::chrono_literals;
class Req {
 public:
  static Req& get() {
    static Req r;
    return r;
  }
  void init(int warmup_num, int benchmark_num) {
    warmup_num_ = warmup_num;
    num_of_reqs_ = warmup_num + benchmark_num;

    if (num_of_reqs_ == warmup_num_ || num_of_reqs_ >= reqs_.size()) {
      num_of_reqs_ = reqs_.size();
    }
    std::cout << "warmup count: " << warmup_num_
              << "; benchmark count: " << num_of_reqs_ << "\n";
  }
  void load(const std::string& file) {
    std::cout << "load queries from " << file << "\n";
    std::ifstream fi(file, std::ios::binary);
    const size_t size = 4096;
    std::vector<char> buffer(size);
    std::vector<char> tmp(size);
    size_t index = 0;
    while (fi.read(buffer.data(), size)) {
      std::streamsize len = fi.gcount();
      for (std::streamsize i = 0; i < len; ++i) {
        if (index >= 4 && tmp[index - 1] == '#') {
          if (tmp[index - 4] == 'e' && tmp[index - 3] == 'o' &&
              tmp[index - 2] == 'r') {
            reqs_.emplace_back(
                std::string(tmp.begin(), tmp.begin() + index - 4));

            index = 0;
          }
        }
        tmp[index++] = buffer[i];
      }
      buffer.clear();
    }
    fi.close();
    std::cout << "load " << reqs_.size() << " queries\n";
    num_of_reqs_ = reqs_.size();
    // reqs_.resize(100000);
    start_.resize(reqs_.size());
    end_.resize(reqs_.size());
  }

  seastar::future<> do_query(server::executor_ref& ref) {
    auto id = cur_.fetch_add(1);
    if (id >= num_of_reqs_) {
      return seastar::make_ready_future<>();
    }
    start_[id] = std::chrono::system_clock::now();
    return ref.run_graph_db_query(server::query_param{reqs_[id]})
        .then_wrapped(
            [&, id](seastar::future<server::query_result>&& fut) mutable {
              auto result = fut.get0();
              end_[id] = std::chrono::system_clock::now();
            })
        .then([&] { return do_query(ref); });
  }

  seastar::future<> simulate() {
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<server::executor_group>(0));
    return seastar::do_with(
        builder.build_ref<server::executor_ref>(0),
        [&](server::executor_ref& ref) { return do_query(ref); });
  }

  void output() {
    std::vector<long long> vec(29, 0);
    std::vector<int> count(29, 0);
    std::vector<std::vector<long long>> ts(29);
    for (size_t idx = warmup_num_; idx < num_of_reqs_; idx++) {
      auto& s = reqs_[idx];
      size_t id = static_cast<size_t>(s.back()) - 1;
      auto tmp = std::chrono::duration_cast<std::chrono::microseconds>(
                     end_[idx] - start_[idx])
                     .count();
      ts[id].emplace_back(tmp);
      vec[id] += tmp;
      count[id] += 1;
    }
    std::vector<std::string> queries = {
        "IC1", "IC2",  "IC3",  "IC4",  "IC5",  "IC6",  "IC7", "IC8",
        "IC9", "IC10", "IC11", "IC12", "IC13", "IC14", "IS1", "IS2",
        "IS3", "IS4",  "IS5",  "IS6",  "IS7",  "IU1",  "IU2", "IU3",
        "IU4", "IU5",  "IU6",  "IU7",  "IU8"};
    for (size_t i = 0; i < vec.size(); ++i) {
      size_t sz = ts[i].size();
      if (sz > 0) {
        std::cout << queries[i] << "; mean: " << vec[i] * 1. / count[i]
                  << "; counts: " << count[i] << "; ";

        std::sort(ts[i].begin(), ts[i].end());
        std::cout << " min: " << ts[i][0] << "; ";
        std::cout << " max: " << ts[i].back() << "; ";
        std::cout << " P50: " << ts[i][sz / 2] << "; ";
        std::cout << " P90: " << ts[i][sz * 9 / 10] << "; ";
        std::cout << " P95: " << ts[i][sz * 95 / 100] << "; ";
        std::cout << " P99: " << ts[i][sz * 99 / 100] << "\n";
      }
    }
    std::cout << "unit: MICROSECONDS\n";
  }

 private:
  Req() : cur_(0), warmup_num_(0) {}

  std::atomic<uint32_t> cur_;
  uint32_t warmup_num_;
  uint32_t num_of_reqs_;
  std::vector<std::string> reqs_;
  std::vector<std::chrono::system_clock::time_point> start_;
  std::vector<std::chrono::system_clock::time_point> end_;

  // std::vector<executor_ref> executor_refs_;
};

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("shard-num,s",
                                      bpo::value<uint32_t>()->default_value(1),
                                      "shard number of actor system")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "warmup-num,w", bpo::value<uint32_t>()->default_value(0),
      "num of warmup reqs")("benchmark-num,b",
                            bpo::value<uint32_t>()->default_value(0),
                            "num of benchmark reqs")(
      "req-file,r", bpo::value<std::string>(), "requests file");

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }
  if (vm.count("version")) {
    std::cout << "GraphScope/Flex version " << FLEX_VERSION << std::endl;
    return 0;
  }

  bool enable_dpdk = false;
  uint32_t shard_num = vm["shard-num"].as<uint32_t>();

  std::string graph_schema_path = "";
  std::string data_path = "";

  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  db.Open(schema, data_path, shard_num);

  t0 += grape::GetCurrentTime();
  uint32_t warmup_num = vm["warmup-num"].as<uint32_t>();
  uint32_t benchmark_num = vm["benchmark-num"].as<uint32_t>();
  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";
  std::string req_file = vm["req-file"].as<std::string>();
  Req::get().load(req_file);
  Req::get().init(warmup_num, benchmark_num);
  hiactor::actor_app app;

  auto begin = std::chrono::system_clock::now();
  int ac = 1;
  char* av[] = {(char*) "rt_bench"};
  app.run(ac, av, [shard_num] {
    return seastar::parallel_for_each(boost::irange<unsigned>(0u, shard_num),
                                      [](unsigned id) {
                                        return seastar::smp::submit_to(id, [] {
                                          return Req::get().simulate();
                                        });
                                      })
        .then([] {
          hiactor::actor_engine().exit();
          fmt::print("Exit actor system.\n");
        });
  });
  auto end = std::chrono::system_clock::now();
  std::cout << "cost time:"
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     begin)
                   .count()
            << "\n";
  Req::get().output();
  // std::cout << timer.get_time() / 1us << " microseconds\n";
}
