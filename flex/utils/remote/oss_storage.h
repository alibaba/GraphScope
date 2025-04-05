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

#ifndef FLEX_UTILS_REMOTE_OSS_STORAGE_H_
#define FLEX_UTILS_REMOTE_OSS_STORAGE_H_

#ifdef BUILD_WITH_OSS

#include <filesystem>
#include <string>
#include "flex/third_party/aliyun-oss-cpp-sdk/sdk/include/alibabacloud/oss/OssClient.h"
#include "flex/third_party/aliyun-oss-cpp-sdk/sdk/include/alibabacloud/oss/client/ClientConfiguration.h"
#include "flex/utils/remote/remote_storage.h"
#include "flex/utils/result.h"

namespace gs {

struct OSSConf {
  static constexpr const char* kOSSAccessKeyId = "OSS_ACCESS_KEY_ID";
  static constexpr const char* kOSSAccessKeySecret = "OSS_ACCESS_KEY_SECRET";
  static constexpr const char* kOSSEndpoint = "OSS_ENDPOINT";
  static constexpr const char* kOSSBucketName = "OSS_BUCKET_NAME";
  static constexpr const char* kOSSConcurrency = "OSS_CONCURRENCY";
  // Avoid storing or printing the accesskey_id and accesskey_secret
  std::string accesskey_id_;
  std::string accesskey_secret_;
  std::string endpoint_;
  std::string bucket_name_;
  int32_t concurrency_ = 4;
  uint64_t partition_size_ = 1024 * 1024 * 128;
  AlibabaCloud::OSS::ClientConfiguration client_conf_;

  void load_conf_from_env();
};

class OSSRemoteStorageUploader : public RemoteStorageUploader {
 public:
  OSSRemoteStorageUploader(OSSConf conf = {}) : conf_(conf) {}

  ~OSSRemoteStorageUploader() override = default;

  // Will try to load the accesskey_id and accesskey_secret from the environment
  // variables if they are not set in the OSSConf.
  Status Open() override;

  Status Put(const std::string& local_path, const std::string& remote_path,
             bool override = false) override;

  Status Delete(const std::string& remote_path) override;

  Status Close() override;

 private:
  OSSConf conf_;
  std::shared_ptr<AlibabaCloud::OSS::OssClient> client_;
};

class OSSRemoteStorageDownloader : public RemoteStorageDownloader {
 public:
  OSSRemoteStorageDownloader(OSSConf conf = {}) : conf_(conf) {}

  ~OSSRemoteStorageDownloader() override = default;

  Status Open() override;

  Status Get(const std::string& remote_path,
             const std::string& local_path) override;

  Status List(const std::string& remote_path,
              std::vector<std::string>& list) override;

  Status Close() override;

 private:
  bool get_metadata_etag(const std::string& remote_path, std::string& etag);
  OSSConf conf_;
  std::shared_ptr<AlibabaCloud::OSS::OssClient> client_;
};
}  // namespace gs

#endif  // BUILD_WITH_OSS

#endif  // FLEX_UTILS_REMOTE_OSS_STORAGE_H_