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
#include "flex/utils/app_utils.h"

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>

static constexpr int32_t REL_TYPE_MAX = 19;

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

    std::cout << "warmup count: " << warmup_num_
              << "; benchmark count: " << num_of_reqs_ << "\n";
    start_.resize(num_of_reqs_);
    end_.resize(num_of_reqs_);
  }
  // void load(const std::string& file) {
  //   std::cout << "load queries from " << file << "\n";
  //   std::ifstream fi(file, std::ios::binary);
  //   const size_t size = 4096;
  //   std::vector<char> buffer(size);
  //   std::vector<char> tmp(size);
  //   size_t index = 0;
  //   while (fi.read(buffer.data(), size)) {
  //     std::streamsize len = fi.gcount();
  //     for (std::streamsize i = 0; i < len; ++i) {
  //       if (index >= 4 && tmp[index - 1] == '#') {
  //         if (tmp[index - 4] == 'e' && tmp[index - 3] == 'o' &&
  //             tmp[index - 2] == 'r') {
  //           reqs_.emplace_back(
  //               std::string(tmp.begin(), tmp.begin() + index - 4));

  //           index = 0;
  //         }
  //       }
  //       tmp[index++] = buffer[i];
  //     }
  //     buffer.clear();
  //   }
  //   fi.close();
  //   std::cout << "load " << reqs_.size() << " queries\n";
  //   num_of_reqs_ = reqs_.size();
  //   // reqs_.resize(100000);
  //   start_.resize(reqs_.size());
  //   end_.resize(reqs_.size());
  // }

  // Load a csv file with input oids and names,
  void load(const std::string& file) {
    std::cout << "load input from " << file << "\n";
    // use csv arrow reader to read the csv file

    auto read_result = arrow::io::ReadableFile::Open(file);
    if (!read_result.ok()) {
      LOG(FATAL) << "Failed to open file: " << file
                 << " error: " << read_result.status().message();
    }
    std::shared_ptr<arrow::io::ReadableFile> file_ = read_result.ValueOrDie();
    arrow::csv::ReadOptions read_options;
    arrow::csv::ParseOptions parse_options;
    arrow::csv::ConvertOptions convert_options;
    auto reader_res = arrow::csv::TableReader::Make(
        arrow::io::default_io_context(), file_, read_options, parse_options,
        convert_options);
    if (!reader_res.ok()) {
      LOG(FATAL) << "Failed to create table reader: "
                 << reader_res.status().message();
    }

    auto reader = reader_res.ValueOrDie();
    auto read_res = reader->Read();
    if (!read_res.ok()) {
      LOG(FATAL) << "Failed to read table: " << read_res.status().message();
    }
    auto table = read_res.ValueOrDie();
    std::shared_ptr<arrow::Array> oid_array = table->column(0)->chunk(0);
    // set to oids_
    std::vector<int64_t> oids;
    auto i64_array = std::static_pointer_cast<arrow::Int64Array>(oid_array);
    for (int64_t i = 0; i < i64_array->length(); i++) {
      oids.emplace_back(i64_array->Value(i));
    }
    std::vector<char> query_char;
    gs::Encoder encoder(query_char);
    encoder.put_int(5);             // hop_limit
    encoder.put_int(200);           // result limit
    encoder.put_int(REL_TYPE_MAX);  // rel_type_max
    for (int i = 0; i < REL_TYPE_MAX; i++) {
      encoder.put_int(i);  // rel_type_limit
    }
    encoder.put_int(oids.size());
    for (auto oid : oids) {
      encoder.put_long(oid);
    }
    encoder.put_byte(1);

    LOG(INFO) << "load " << oid_array->length()
              << " oids, size: " << query_char.size();
    query_ = std::string(query_char.begin(), query_char.end());
    num_of_reqs_ = 1;
    start_.resize(num_of_reqs_);
    end_.resize(num_of_reqs_);
  }

  seastar::future<> do_query(server::executor_ref& ref) {
    auto id = cur_.fetch_add(1);
    if (id >= num_of_reqs_) {
      return seastar::make_ready_future<>();
    }
    start_[id] = std::chrono::system_clock::now();
    LOG(INFO) << "query: " << query_.size();
    return ref.run_graph_db_query(server::query_param{query_})
        .then_wrapped(
            [&, id](seastar::future<server::query_result>&& fut) mutable {
              auto result = fut.get0().content;
              gs::Decoder decoder(result.begin(), result.size());
              int32_t size = decoder.get_int();
              LOG(INFO) << "result size: " << size;
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
    std::vector<long long> vec(1, 0);
    std::vector<int> count(1, 0);
    std::vector<std::vector<long long>> ts(1);
    for (size_t idx = warmup_num_; idx < num_of_reqs_; idx++) {
      size_t id = 0;
      auto tmp = std::chrono::duration_cast<std::chrono::microseconds>(
                     end_[idx] - start_[idx])
                     .count();
      ts[id].emplace_back(tmp);
      vec[id] += tmp;
      count[id] += 1;
    }
    std::vector<std::string> queries = {"HuoYan"};
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
  std::vector<std::chrono::system_clock::time_point> start_;
  std::vector<std::chrono::system_clock::time_point> end_;

  // std::vector<char> query_;
  std::string query_;

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
  if (!schema.ok()) {
    LOG(ERROR) << "Failed to load graph schema from " << graph_schema_path;
    return -1;
  }
  db.Open(schema.value(), data_path, shard_num);

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
