
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

int main(int argc, char** argv) {
  if (argc != 6) {
    std::cerr << "usage: oss_object_writer <access-key> <access-secret> "
                 "<endpoint> <bucket> <input-file> "
              << std::endl;
  }

  gs::OSSConf conf;
  conf.accesskey_id_ = argv[1];
  conf.accesskey_secret_ = argv[2];
  conf.endpoint_ = argv[3];
  conf.bucket_name_ = argv[4];

  gs::OSSRemoteStorageUploader writer(conf);
  gs::OSSRemoteStorageDownloader reader(conf);

  CHECK(writer.Open().ok()) << "Open OSS writer failed";
  CHECK(reader.Open().ok()) << "Open OSS reader failed";

  std::string input_file = argv[5];
  std::string object_name = "test_object";
  if (writer.Put(input_file, object_name).ok()) {
    LOG(INFO) << "Put object " << object_name << " success";
  } else {
    LOG(ERROR) << "Put object " << object_name << " failed";
  }

  std::string output_file = "output_file";
  if (reader.Get(object_name, output_file).ok()) {
    LOG(INFO) << "Get object " << object_name << " success";
  } else {
    LOG(ERROR) << "Get object " << object_name << " failed";
  }

  // delete object
  if (writer.Delete(object_name).ok()) {
    LOG(INFO) << "Delete object " << object_name << " success";
  } else {
    LOG(ERROR) << "Delete object " << object_name << " failed";
  }

  return 0;
}