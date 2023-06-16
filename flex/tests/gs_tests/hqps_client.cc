#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "flex/utils/app_utils.h"
#include "flex/third_party/httplib.h"

#include "proto_generated_gie/results.pb.h"
#include "proto_generated_gie/stored_procedure.pb.h"

#include "glog/logging.h"

int main(int argc, char** argv) {
  std::string url = "127.0.0.1";
  httplib::Client cli(url, 1000);
  LOG(INFO) << "Here";

  query::Query query;
  query.mutable_query_name()->set_name("query_ic2");
  {
    auto node = query.add_arguments();
    node->set_param_name("personIdQ2");
    node->set_param_ind(0);
    node->mutable_value()->set_i64(19791209300143);
  }
  {
    auto node = query.add_arguments();
    node->set_param_name("maxDate");
    node->set_param_ind(1);
    node->mutable_value()->set_i64(1354060800000);
  }
  auto query_str = query.SerializeAsString();

  std::string content(query_str.data(), query_str.size());
  auto res = cli.Post("/interactive/query", content, "text/plain");
  LOG(INFO) << "After post";

  if (res.error() != httplib::Error::Success) {
    LOG(INFO) << httplib::to_string(res.error());
    return 1;
  }
  // we assume the body is serialized binary, can be decoded by protobuf
  std::string ret = res->body;
  LOG(INFO) << "Return body size: " << ret;
  if (!ret.empty()) {
    results::CollectiveResults results;
    results.ParseFromArray(ret.data(), ret.size());
    LOG(INFO) << "results:";
    for (int i = 0; i < results.results_size(); ++i) {
      LOG(INFO) << results.results(i).DebugString();
    }
  }

  return 0;
}
