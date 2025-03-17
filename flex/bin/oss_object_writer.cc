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

#include "flex/utils/remote/oss_storage.h"

#include <glog/logging.h>

#include <boost/program_options.hpp>

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "access-key,k", bpo::value<std::string>(), "OSS access key")(
      "access-secret,s", bpo::value<std::string>(), "OSS access secret")(
      "endpoint,e", bpo::value<std::string>(), "OSS endpoint")(
      "bucket,b", bpo::value<std::string>(), "OSS bucket")(
      "object,o", bpo::value<std::string>(), "OSS object")(
      "input-file,f", bpo::value<std::string>(), "output file");

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  gs::OSSConf conf;
  if (vm.count("access-key")) {
    conf.accesskey_id_ = vm["access-key"].as<std::string>();
  }
  if (vm.count("access-secret")) {
    conf.accesskey_secret_ = vm["access-secret"].as<std::string>();
  }
  if (vm.count("endpoint")) {
    conf.endpoint_ = vm["endpoint"].as<std::string>();
  }
  if (vm.count("bucket")) {
    conf.bucket_name_ = vm["bucket"].as<std::string>();
  }

  gs::OSSRemoteStorageWriter writer(conf);

  CHECK(writer.Open().ok()) << "Open OSS reader failed";
  if (!vm.count("input-file")) {
    LOG(FATAL) << "input-file is required";
  }

  if (!vm.count("object")) {
    LOG(FATAL) << "object is required";
  }

  CHECK(writer
            .Put(vm["input-file"].as<std::string>(),
                 vm["object"].as<std::string>())
            .ok())
      << "Put object to OSS failed";
  LOG(INFO) << "Successfully put object to OSS";

  CHECK(writer.Delete(vm["object"].as<std::string>()).ok())
      << "Delete object from OSS failed";

  return 0;
}