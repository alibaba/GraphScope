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

#ifdef BUILD_WITH_OSS

#include "flex/utils/remote/oss_storage.h"
#include <filesystem>
#include "flex/third_party/aliyun-oss-cpp-sdk/sdk/include/alibabacloud/oss/client/RetryStrategy.h"
#include "flex/utils/file_utils.h"

namespace gs {

void OSSConf::load_conf_from_env() {
  if (accesskey_id_.empty()) {
    const char* accesskey_id = std::getenv(kOSSAccessKeyId);
    if (accesskey_id) {
      accesskey_id_ = accesskey_id;
    }
  }

  if (accesskey_secret_.empty()) {
    const char* accesskey_secret = std::getenv(kOSSAccessKeySecret);
    if (accesskey_secret) {
      accesskey_secret_ = accesskey_secret;
    }
  }

  if (endpoint_.empty()) {
    const char* endpoint = std::getenv(kOSSEndpoint);
    if (endpoint) {
      endpoint_ = endpoint;
    }
  }

  if (bucket_name_.empty()) {
    const char* bucket_name = std::getenv(kOSSBucketName);
    if (bucket_name) {
      bucket_name_ = bucket_name;
    }
  }
  if (std::getenv(kOSSConcurrency)) {
    concurrency_ = std::stoi(std::getenv(kOSSConcurrency));
  }
  LOG(INFO) << "OSS concurrency: " << concurrency_;
}

class UserRetryStrategy : public AlibabaCloud::OSS::RetryStrategy {
 public:
  UserRetryStrategy(long maxRetries = 3, long scaleFactor = 300)
      : m_scaleFactor(scaleFactor), m_maxRetries(maxRetries) {}

  bool shouldRetry(const AlibabaCloud::OSS::Error& error,
                   long attemptedRetries) const {
    if (attemptedRetries >= m_maxRetries)
      return false;

    long responseCode = error.Status();

    // http code
    if ((responseCode == 403 &&
         error.Message().find("RequestTimeTooSkewed") != std::string::npos) ||
        (responseCode > 499 && responseCode < 599)) {
      return true;
    } else {
      switch (responseCode) {
      // curl error code
      case (AlibabaCloud::OSS::ERROR_CURL_BASE + 7):   // CURLE_COULDNT_CONNECT
      case (AlibabaCloud::OSS::ERROR_CURL_BASE + 18):  // CURLE_PARTIAL_FILE
      case (AlibabaCloud::OSS::ERROR_CURL_BASE + 23):  // CURLE_WRITE_ERROR
      case (AlibabaCloud::OSS::ERROR_CURL_BASE +
            28):  // CURLE_OPERATION_TIMEDOUT
      case (AlibabaCloud::OSS::ERROR_CURL_BASE + 52):  // CURLE_GOT_NOTHING
      case (AlibabaCloud::OSS::ERROR_CURL_BASE + 55):  // CURLE_SEND_ERROR
      case (AlibabaCloud::OSS::ERROR_CURL_BASE + 56):  // CURLE_RECV_ERROR
        return true;
      default:
        break;
      };
    }

    return false;
  }

  long calcDelayTimeMs(const AlibabaCloud::OSS::Error& error,
                       long attemptedRetries) const {
    return (1 << attemptedRetries) * m_scaleFactor;
  }

 private:
  long m_scaleFactor;
  long m_maxRetries;
};

template <typename ResultT>
std::string oss_outcome_to_string(
    const std::string& additional_info,
    const AlibabaCloud::OSS::Outcome<AlibabaCloud::OSS::OssError, ResultT>&
        outcome) {
  return additional_info + ", Outcome: code: " + outcome.error().Code() +
         ", message: " + outcome.error().Message() +
         ", requestId: " + outcome.error().RequestId();
}

std::string object_summary_to_string(
    const AlibabaCloud::OSS::ObjectSummary& summary) {
  return "ObjectSummary: key: " + summary.Key() + ", ETag: " + summary.ETag() +
         ", size: " + std::to_string(summary.Size()) +
         ", lastModified: " + summary.LastModified() +
         ", storageClass: " + summary.StorageClass() +
         ", type: " + summary.Type() + ", owner: " + summary.Owner().Id() +
         ", restoreInfo: " + summary.RestoreInfo();
}

gs::Status OSSRemoteStorageUploader::Open() {
  if (conf_.accesskey_id_.empty() || conf_.accesskey_secret_.empty()) {
    conf_.load_conf_from_env();
  }

  auto defaultRetryStrategy = std::make_shared<UserRetryStrategy>(5);
  conf_.client_conf_.retryStrategy = defaultRetryStrategy;

  client_ = std::make_shared<AlibabaCloud::OSS::OssClient>(
      conf_.endpoint_, conf_.accesskey_id_, conf_.accesskey_secret_,
      conf_.client_conf_);

  return Status::OK();
}

gs::Status OSSRemoteStorageUploader::Put(const std::string& local_path,
                                         const std::string& remote_path,
                                         bool override) {
  LOG(INFO) << "OSS Put local file " << local_path << " to remote "
            << remote_path;
  if (!client_ || local_path.empty() || remote_path.empty()) {
    return gs::Status(gs::StatusCode::INVALID_ARGUMENT,
                      "OSS Put invalid argument");
  }

  // check local path is exist
  if (!std::filesystem::exists(local_path)) {
    LOG(ERROR) << "OSS Put local file " << local_path << " not exist";
    return gs::Status(gs::StatusCode::INVALID_ARGUMENT,
                      "OSS Put local file not exist");
  }

  AlibabaCloud::OSS::UploadObjectRequest request(conf_.bucket_name_,
                                                 remote_path, local_path);
  request.MetaData().addHeader("x-oss-forbid-overwrite", "true");
  request.setPartSize(conf_.partition_size_);
  request.setThreadNum(conf_.concurrency_);  // Increase the thread number to
                                             // improve the upload speed
  auto outcome = client_->ResumableUploadObject(request);
  if (!outcome.isSuccess()) {
    std::string error_string = "OSS ResumableUploadObject from local " +
                               local_path + " to remote " + remote_path +
                               " failed, code: " + outcome.error().Code() +
                               ", message: " + outcome.error().Message() +
                               ", requestId: " + outcome.error().RequestId();
    LOG(ERROR) << error_string;
    return gs::Status(gs::StatusCode::IO_ERROR, error_string);
  }

  return Status::OK();
}
gs::Status OSSRemoteStorageUploader::Delete(const std::string& remote_path) {
  AlibabaCloud::OSS::DeleteObjectRequest request(conf_.bucket_name_,
                                                 remote_path);
  auto outcome = client_->DeleteObject(request);
  if (!outcome.isSuccess()) {
    std::string error_string = "OSS DeleteObject " + remote_path +
                               " failed, code: " + outcome.error().Code() +
                               ", message: " + outcome.error().Message() +
                               ", requestId: " + outcome.error().RequestId();
    LOG(ERROR) << error_string;
    return gs::Status(gs::StatusCode::IO_ERROR, error_string);
  }
  return Status::OK();
}
gs::Status OSSRemoteStorageUploader::Close() {
  client_.reset();
  return Status::OK();
}

/// OSSRemote storage reader
gs::Status OSSRemoteStorageDownloader::Open() {
  if (conf_.accesskey_id_.empty() || conf_.accesskey_secret_.empty()) {
    conf_.load_conf_from_env();
  }

  auto defaultRetryStrategy = std::make_shared<UserRetryStrategy>(5);
  conf_.client_conf_.retryStrategy = defaultRetryStrategy;

  client_ = std::make_shared<AlibabaCloud::OSS::OssClient>(
      conf_.endpoint_, conf_.accesskey_id_, conf_.accesskey_secret_,
      conf_.client_conf_);

  return Status::OK();
}

gs::Status OSSRemoteStorageDownloader::Get(const std::string& remote_path,
                                           const std::string& local_path) {
  LOG(INFO) << "OSS Get remote file " << remote_path << " to local "
            << local_path;
  if (local_path.empty() || remote_path.empty()) {
    return gs::Status(
        gs::StatusCode::INVALID_ARGUMENT,
        "OSS Get invalid argument, local path or remote path is empty");
  }

  // check local etag file is exist and equal to remote oss etag
  std::string etag_file = local_path + ".etag";
  std::string local_etag, oss_etag;

  if (std::filesystem::exists(local_path) &&
      gs::read_string_from_file(etag_file, local_etag) &&
      get_metadata_etag(remote_path, oss_etag) && !local_etag.empty() &&
      !oss_etag.empty() && oss_etag == local_etag) {
    LOG(INFO) << "OSS Get local file " << local_path << " is up to date";
    return Status::OK();
  }

  AlibabaCloud::OSS::DownloadObjectRequest request(conf_.bucket_name_,
                                                   remote_path, local_path);
  request.setPartSize(conf_.partition_size_);
  request.setThreadNum(conf_.concurrency_);  // Increase the thread number to
                                             // improve the download speed
  auto outcome = client_->ResumableDownloadObject(request);
  if (!outcome.isSuccess()) {
    std::string error_string = oss_outcome_to_string(
        "OSS ResumableDownloadObject from remote " + remote_path +
            " to local " + local_path + " failed",
        outcome);
    LOG(ERROR) << error_string;
    return gs::Status(gs::StatusCode::IO_ERROR, error_string);
  }

  if (std::filesystem::exists(local_path)) {
    // get size
    uint64_t file_size = std::filesystem::file_size(local_path);
    LOG(INFO) << "OSS Get local file " << local_path
              << " success, size: " << file_size;
  } else {
    LOG(ERROR) << "OSS Get local file " << local_path << " failed";
    return gs::Status(gs::StatusCode::IO_ERROR, "OSS Get local file failed");
  }

  if (!(get_metadata_etag(remote_path, oss_etag) &&
        gs::write_string_to_file(oss_etag, etag_file))) {
    LOG(ERROR) << "OSS Get write etag file " << etag_file << " failed";
    return gs::Status(gs::StatusCode::IO_ERROR,
                      "OSS Get write etag file failed");
  }

  return Status::OK();
}

gs::Status OSSRemoteStorageDownloader::List(
    const std::string& remote_prefix, std::vector<std::string>& path_list) {
  std::string nextMarker = "";
  bool isTruncated = false;
  do {
    AlibabaCloud::OSS::ListObjectsRequest request(conf_.bucket_name_);
    request.setPrefix(remote_prefix);
    request.setMarker(nextMarker);
    auto outcome = client_->ListObjects(request);
    if (!outcome.isSuccess()) {
      std::string error_string = oss_outcome_to_string(
          "OSS ListObjects from remote " + remote_prefix + " failed", outcome);
      LOG(ERROR) << error_string;
      return gs::Status(gs::StatusCode::IO_ERROR, error_string);
    }
    for (const auto& object : outcome.result().ObjectSummarys()) {
      LOG(INFO) << "OSS ListObject:  " << object_summary_to_string(object);
      path_list.push_back(object.Key());
    }

    nextMarker = outcome.result().NextMarker();
    isTruncated = outcome.result().IsTruncated();
  } while (isTruncated);

  return Status::OK();
}

gs::Status OSSRemoteStorageDownloader::Close() { return Status::OK(); }

bool OSSRemoteStorageDownloader::get_metadata_etag(
    const std::string& remote_path, std::string& etag) {
  if (remote_path.empty()) {
    LOG(ERROR)
        << "OSS get_metadata_etag invalid argument, remote path is empty";
    return false;
  }

  auto outcome = client_->GetObjectMeta(conf_.bucket_name_, remote_path);
  if (!outcome.isSuccess()) {
    std::string error_string = oss_outcome_to_string(
        "OSS GetObjectMeta from remote " + remote_path + " failed", outcome);
    LOG(ERROR) << error_string;
    return false;
  } else {
    auto metadata = outcome.result();
    LOG(INFO) << "OSS GetObjectMeta " << remote_path
              << " success, etag: " << metadata.ETag();
    etag = metadata.ETag();
  }
  return true;
}

}  // namespace gs

#endif  // BUILD_WITH_OSS