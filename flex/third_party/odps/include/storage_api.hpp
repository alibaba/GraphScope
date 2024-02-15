#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include "common/configuration.h"
#include "common/http_flags.h"

#define CPPHTTPLIB_SEND_FLAGS MSG_NOSIGNAL
#include "common/log.h"
#include "common/md5.h"
#include "common/signer.h"
#include "flex/third_party/httplib.h"
#include "nlohmann/json.hpp"

#define STORAGE_VERSION "1"
#define URL_PREFIX "/api/storage/v" STORAGE_VERSION

namespace apsara {
namespace odps {
namespace sdk {
namespace storage_api {

struct TableBatchScanReq;
struct TableBatchScanResp;
struct ReadRowsReq;
struct ReadRowsResp;
struct TableBatchWriteReq;
struct TableBatchWriteResp;
struct WriteRowsReq;
struct WriteRowsResp;
struct SessionReq;

/*
 * @brief set the log level of the storage api
 * @param level: ODPS_STORAGE_API_LOG_DEBUG, ODPS_STORAGE_API_LOG_INFO,
 * ODPS_STORAGE_API_LOG_ERROR
 */
inline void SetLogLevel(uint32_t level) {
  apsara::odps::sdk::LogMessage::GetInstance()->SetLevel(level);
}

class Client {
 public:
  /**
   *  @param configuration Configuration for AK and endpoint.
   */
  Client(const Configuration& configuration);

  /**
   *  @brief Create a batch read session.
   *
   *  @param request Create read session request parameters.
   *  @param response Session info returned from the server.
   */
  void CreateReadSession(const TableBatchScanReq& request,
                         TableBatchScanResp& response);

  /**
   *  @brief Get the batch read session.
   *
   *  @param request Read session request info.
   *  @param response Read session response.
   */
  void GetReadSession(const SessionReq& request, TableBatchScanResp& response);

  /**
   *  @brief Read one split of the read session.
   *
   *  @param request Read rows request parameters.
   *  @param response Read rows response.
   *  @param read_stream The data stream is returned through this callback. The
   * callback may be invoked multiple times.
   */
  void ReadRows(const ReadRowsReq& request, ReadRowsResp& response,
                std::function<bool(const char* buf, size_t len)> read_stream);

  /**
   *  @brief Create a batch write session.
   *
   *  @param request Create write session request parameters.
   *  @param response Write session response.
   */
  void CreateWriteSession(const TableBatchWriteReq& request,
                          TableBatchWriteResp& response);

  /**
   *  @brief Get the batch write session.
   *
   *  @param request Write session request info.
   *  @param response Write session response.
   */
  void GetWriteSession(const SessionReq& request,
                       TableBatchWriteResp& response);

  /**
   *  @brief Write one block of data to the write session.
   *
   *  @param request Write rows request parameters.
   *  @param response Write rows response which contains the commit message if
   * the request succeeds.
   *  @param sink_func Use sink.write() to push the block data stream to the
   * server and sink.done() to finish the stream.
   */
  void WriteRows(const WriteRowsReq& request, WriteRowsResp& response,
                 std::function<bool(httplib::DataSink& sink)> sink_func);

  /**
   *  @brief Commit the write session.
   *
   *  @param request Commit write session request info.
   *  @param commit_msg Commit messages collected from the WriteRows().
   *  @param response Commit session response from the server.
   */
  void CommitWriteSession(const SessionReq& request,
                          const std::vector<std::string>& commit_msg,
                          TableBatchWriteResp& response);

 private:
  Configuration configuration_;
  std::map<std::string, std::string> project_to_tunnel_endpoint_;
  std::mutex mutex_;

  std::string GetRoutedEndpoint(const std::string& project);
  std::shared_ptr<httplib::Client> GetHttpClient(const std::string& project);
};

enum Status {
  OK = 0,
  FAIL,
  WAIT,  // CreateReadSession() and CommitWriteSession() may process the request
         // asynchronously
  CANCELED,
};

enum SessionStatus {
  INIT = 0,
  NORMAL,
  CRITICAL,
  EXPIRED,
  COMMITTING,
  COMMITTED
};

NLOHMANN_JSON_SERIALIZE_ENUM(SessionStatus,
                             {
                                 {SessionStatus::INIT, "INIT"},
                                 {SessionStatus::NORMAL, "NORMAL"},
                                 {SessionStatus::CRITICAL, "CRITICAL"},
                                 {SessionStatus::EXPIRED, "EXPIRED"},
                                 {SessionStatus::COMMITTING, "COMMITTING"},
                                 {SessionStatus::COMMITTED, "COMMITTED"},
                             })

struct SplitOptions {
  enum SplitMode { SIZE, PARALLELISM, ROW_OFFSET, BUCKET };

  enum SplitMode split_mode_;
  long split_number_;
  bool cross_partition_;

  static SplitOptions GetDefaultOptions(SplitOptions::SplitMode mode) {
    SplitOptions options;
    options.cross_partition_ = true;
    switch (mode) {
    case SIZE:
      options.split_mode_ = SIZE;
      options.split_number_ = 256 * 1024 * 1024;
      break;
    case PARALLELISM:
      options.split_mode_ = PARALLELISM;
      options.split_number_ = 32;
      break;
    case ROW_OFFSET:
      options.split_mode_ = ROW_OFFSET;
      options.split_number_ = 0;
      break;
    case BUCKET:
      options.split_mode_ = BUCKET;
      break;
    default:
      break;
    }
    return options;
  }

  friend void to_json(nlohmann::json& j, const SplitOptions& obj) {
    j = nlohmann::json{
        {"SplitNumber", obj.split_number_},
        {"CrossPartition", obj.cross_partition_},
        {"SplitMode", obj.split_mode_},
    };
  }
  friend void from_json(const nlohmann::json& j, SplitOptions& obj) {
    j.at("SplitNumber").get_to(obj.split_number_);
    j.at("CrossPartition").get_to(obj.cross_partition_);
    j.at("SplitMode").get_to(obj.split_mode_);
  }
};

NLOHMANN_JSON_SERIALIZE_ENUM(
    SplitOptions::SplitMode,
    {
        {SplitOptions::SplitMode::SIZE, "Size"},
        {SplitOptions::SplitMode::PARALLELISM, "Parallelism"},
        {SplitOptions::SplitMode::ROW_OFFSET, "RowOffset"},
        {SplitOptions::SplitMode::BUCKET, "Bucket"},
    })

struct ArrowOptions {
  enum TimestampUnit { SECOND, MILLI, MICRO, NANO };

  enum TimestampUnit timestamp_unit_ = NANO;
  enum TimestampUnit date_time_unit_ = MILLI;

  friend void to_json(nlohmann::json& j, const ArrowOptions& obj) {
    j = nlohmann::json{
        {"TimestampUnit", obj.timestamp_unit_},
        {"DatetimeUnit", obj.date_time_unit_},
    };
  }
  friend void from_json(const nlohmann::json& j, ArrowOptions& obj) {
    j.at("TimestampUnit").get_to(obj.timestamp_unit_);
    j.at("DatetimeUnit").get_to(obj.date_time_unit_);
  }
};

NLOHMANN_JSON_SERIALIZE_ENUM(ArrowOptions::TimestampUnit,
                             {{ArrowOptions::TimestampUnit::SECOND, "second"},
                              {ArrowOptions::TimestampUnit::MILLI, "milli"},
                              {ArrowOptions::TimestampUnit::MICRO, "micro"},
                              {ArrowOptions::TimestampUnit::NANO, "nano"}})

struct TableIdentifier {
  std::string project_;
  std::string table_;
  std::string schema_ = "default";

  friend void to_json(nlohmann::json& j, const TableIdentifier& obj) {
    j = nlohmann::json{
        {"Project", obj.project_},
        {"Table", obj.table_},
        {"Schema", obj.schema_},
    };
  }
  friend void from_json(const nlohmann::json& j, TableIdentifier& obj) {
    j.at("Project").get_to(obj.project_);
    j.at("Table").get_to(obj.table_);
    j.at("Schema").get_to(obj.schema_);
  }
};

struct Column {
  std::string name_;
  std::string type_;
  std::string comment_;
  bool nullable_;

  friend void to_json(nlohmann::json& j, const Column& obj) {
    j = nlohmann::json{
        {"Name", obj.name_},
        {"Type", obj.type_},
        {"Comment", obj.comment_},
        {"Nullable", obj.nullable_},
    };
  }
  friend void from_json(const nlohmann::json& j, Column& obj) {
    j.at("Name").get_to(obj.name_);
    j.at("Type").get_to(obj.type_);
    j.at("Comment").get_to(obj.comment_);
    j.at("Nullable").get_to(obj.nullable_);
  }
};

struct DataSchema {
  std::vector<Column> data_columns_;
  std::vector<Column> partition_columns_;
  friend void to_json(nlohmann::json& j, const DataSchema& obj) {
    j = nlohmann::json{
        {"DataColumns", obj.data_columns_},
        {"PartitionColumns", obj.partition_columns_},
    };
  }
  friend void from_json(const nlohmann::json& j, DataSchema& obj) {
    j.at("DataColumns").get_to(obj.data_columns_);
    j.at("PartitionColumns").get_to(obj.partition_columns_);
  }
};

struct DataFormat {
  std::string type_;
  std::string version_;

  friend void to_json(nlohmann::json& j, const DataFormat& obj) {
    j = nlohmann::json{
        {"Type", obj.type_},
        {"Version", obj.version_},
    };
  }
  friend void from_json(const nlohmann::json& j, DataFormat& obj) {
    j.at("Type").get_to(obj.type_);
    j.at("Version").get_to(obj.version_);
  }
};

struct DynamicPartitionOptions {
  std::string invalid_strategy_ = "Exception";
  int invalid_limit_ = 1;
  int dynamic_partition_limit_ = 512;

  friend void to_json(nlohmann::json& j, const DynamicPartitionOptions& obj) {
    j = nlohmann::json{
        {"InvalidStrategy", obj.invalid_strategy_},
        {"InvalidLimit", obj.invalid_limit_},
        {"DynamicPartitionLimit", obj.dynamic_partition_limit_},
    };
  }
  friend void from_json(const nlohmann::json& j, DynamicPartitionOptions& obj) {
    j.at("InvalidStrategy").get_to(obj.invalid_strategy_);
    j.at("InvalidLimit").get_to(obj.invalid_limit_);
    j.at("DynamicPartitionLimit").get_to(obj.dynamic_partition_limit_);
  }
};

struct Order {
  std::string name_;
  std::string sort_direction_;

  friend void to_json(nlohmann::json& j, const Order& obj) {
    j = nlohmann::json{
        {"Name", obj.name_},
        {"SortDirection", obj.sort_direction_},
    };
  }
  friend void from_json(const nlohmann::json& j, Order& obj) {
    j.at("Name").get_to(obj.name_);
    j.at("SortDirection").get_to(obj.sort_direction_);
  }
};

struct RequiredDistribution {
  std::string type_;
  std::vector<std::string> cluster_keys_;
  int buckets_number_;

  friend void to_json(nlohmann::json& j, const RequiredDistribution& obj) {
    j = nlohmann::json{
        {"Type", obj.type_},
        {"ClusterKeys", obj.cluster_keys_},
        {"BucketsNumber", obj.buckets_number_},
    };
  }
  friend void from_json(const nlohmann::json& j, RequiredDistribution& obj) {
    j.at("Type").get_to(obj.type_);
    j.at("ClusterKeys").get_to(obj.cluster_keys_);
    j.at("BucketsNumber").get_to(obj.buckets_number_);
  }
};

struct Compression {
  enum type { UNCOMPRESSED = 0, ZSTD, LZ4_FRAME };

  static std::string ToString(type);
};

struct TableBatchScanReq {
  TableIdentifier table_identifier_;
  std::vector<std::string> required_data_columns_;
  std::vector<std::string> required_partition_columns_;
  std::vector<std::string> required_partitions_;
  std::vector<int> required_bucket_ids_;
  SplitOptions split_options_;
  ArrowOptions arrow_options_;
  std::string filter_predicate_;

  friend void to_json(nlohmann::json& j, const TableBatchScanReq& obj) {
    j = nlohmann::json{
        {"RequiredDataColumns", obj.required_data_columns_},
        {"RequiredPartitionColumns", obj.required_partition_columns_},
        {"RequiredPartitions", obj.required_partitions_},
        {"RequiredBucketIds", obj.required_bucket_ids_},
        {"SplitOptions", obj.split_options_},
        {"ArrowOptions", obj.arrow_options_},
        {"FilterPredicate", obj.filter_predicate_},
    };
  }
  friend void from_json(const nlohmann::json& j, TableBatchScanReq& obj) {
    j.at("RequiredDataColumns").get_to(obj.required_data_columns_);
    j.at("RequiredPartitionColumns").get_to(obj.required_partition_columns_);
    j.at("RequiredPartitions").get_to(obj.required_partitions_);
    j.at("RequiredBucketIds").get_to(obj.required_bucket_ids_);
    j.at("SplitOptions").get_to(obj.split_options_);
    j.at("ArrowOptions").get_to(obj.arrow_options_);
    j.at("FilterPredicate").get_to(obj.filter_predicate_);
  }
};

struct TableBatchScanResp {
  Status status_;
  std::string request_id_;
  std::string session_id_;
  std::string session_type_;
  std::string error_message_;
  long expiration_time_;
  long split_count_;
  long record_count_;
  SessionStatus session_status_;
  DataSchema data_schema_;
  std::vector<DataFormat> supported_data_format_;

  friend void to_json(nlohmann::json& j, const TableBatchScanResp& obj) {
    j = nlohmann::json{
        {"SessionId", obj.session_id_},
        {"SessionType", obj.session_type_},
        {"SessionStatus", obj.session_status_},
        {"ExpirationTime", obj.expiration_time_},
        {"Message", obj.error_message_},
        {"SupportedDataFormat", obj.supported_data_format_},
        {"DataSchema", obj.data_schema_},
        {"SplitsCount", obj.split_count_},
        {"RecordCount", obj.record_count_},
    };
  }
  friend void from_json(const nlohmann::json& j, TableBatchScanResp& obj) {
    j.at("SessionId").get_to(obj.session_id_);
    j.at("SessionType").get_to(obj.session_type_);
    j.at("SessionStatus").get_to(obj.session_status_);
    j.at("ExpirationTime").get_to(obj.expiration_time_);
    j.at("Message").get_to(obj.error_message_);
    j.at("DataSchema").get_to(obj.data_schema_);
    j.at("SupportedDataFormat").get_to(obj.supported_data_format_);
    j.at("SplitsCount").get_to(obj.split_count_);
    j.at("RecordCount").get_to(obj.record_count_);
  }
};

struct SessionReq {
  std::string session_id_;
  TableIdentifier table_identifier_;
};

struct TableBatchWriteReq {
  TableIdentifier table_identifier_;
  std::string partition_spec_;
  ArrowOptions arrow_options_;
  DynamicPartitionOptions dynamic_partition_options_;
  bool overwrite_ = true;
  bool support_write_cluster_ = false;

  friend void to_json(nlohmann::json& j, const TableBatchWriteReq& obj) {
    j = nlohmann::json{
        {"DynamicPartitionOptions", obj.dynamic_partition_options_},
        {"ArrowOptions", obj.arrow_options_},
        {"Overwrite", obj.overwrite_},
        {"PartitionSpec", obj.partition_spec_},
        {"SupportWriteCluster", obj.support_write_cluster_}};
  }
  friend void from_json(const nlohmann::json& j, TableBatchWriteReq& obj) {
    j.at("DynamicPartitionOptions").get_to(obj.dynamic_partition_options_);
    j.at("ArrowOptions").get_to(obj.arrow_options_);
    j.at("Overwrite").get_to(obj.overwrite_);
    j.at("PartitionSpec").get_to(obj.partition_spec_);
    j.at("SupportWriteCluster").get_to(obj.support_write_cluster_);
  }
};

struct TableBatchWriteResp {
  Status status_;
  std::string request_id_;
  std::string session_id_;
  std::string error_message_;
  SessionStatus session_status_;
  DataSchema data_schema_;
  int max_block_num_;
  long expiration_time_;
  RequiredDistribution required_distribution_;
  std::vector<DataFormat> supported_data_format_;
  std::vector<Order> required_ordering_;

  friend void to_json(nlohmann::json& j, const TableBatchWriteResp& obj) {
    j = nlohmann::json{
        {"SessionId", obj.session_id_},
        {"SessionStatus", obj.session_status_},
        {"ExpirationTime", obj.expiration_time_},
        {"Message", obj.error_message_},
        {"DataSchema", obj.data_schema_},
        {"SupportedDataFormat", obj.supported_data_format_},
        {"MaxBlockNumber", obj.max_block_num_},
        {"RequiredOrdering", obj.required_ordering_},
        {"RequiredDistribution", obj.required_distribution_},
    };
  }
  friend void from_json(const nlohmann::json& j, TableBatchWriteResp& obj) {
    j.at("SessionId").get_to(obj.session_id_);
    j.at("SessionStatus").get_to(obj.session_status_);
    j.at("ExpirationTime").get_to(obj.expiration_time_);
    j.at("Message").get_to(obj.error_message_);
    j.at("DataSchema").get_to(obj.data_schema_);
    j.at("SupportedDataFormat").get_to(obj.supported_data_format_);
    j.at("MaxBlockNumber").get_to(obj.max_block_num_);
    j.at("RequiredOrdering").get_to(obj.required_ordering_);
    j.at("RequiredDistribution").get_to(obj.required_distribution_);
  }
};

struct ReadRowsReq {
  TableIdentifier table_identifier_;
  std::string session_id_;
  long split_index_;
  long row_index_;
  long row_count_;
  int max_batch_rows_ = 4096;
  Compression::type compression_ = Compression::LZ4_FRAME;
  DataFormat data_format_;
};

struct ReadRowsResp {
  Status status_;
  std::string error_message_;
  std::string request_id_;
};

struct WriteRowsReq {
  TableIdentifier table_identifier_;
  std::string session_id_;
  int block_number_ = 0;
  int attempt_number_ = 0;
  int bucket_id_ = 0;
  Compression::type compression_ = Compression::LZ4_FRAME;
  DataFormat data_format_;
};

struct WriteRowsResp {
  Status status_;
  std::string request_id_;
  std::string error_message_;
  std::string commit_message_;
};

namespace internal {
template <typename T>
void UpdateRequestID(T& response, httplib::Result& result) {
  if (result->has_header("x-odps-request-id")) {
    response.request_id_ = result->get_header_value("x-odps-request-id");
  }
}
}  // namespace internal

inline std::string Compression::ToString(Compression::type t) {
  switch (t) {
  case Compression::UNCOMPRESSED:
    return "UNCOMPRESSED";
  case Compression::ZSTD:
    return "ZSTD";
  case Compression::LZ4_FRAME:
    return "LZ4_FRAME";
  default:
    return "UNKNOWN";
  }
}

inline Client::Client(const Configuration& configuration)
    : configuration_(configuration) {}

inline std::shared_ptr<httplib::Client> Client::GetHttpClient(
    const std::string& project) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (project_to_tunnel_endpoint_[project] == "") {
    project_to_tunnel_endpoint_[project] = GetRoutedEndpoint(project);
  }

  std::shared_ptr<httplib::Client> http_client =
      std::make_shared<httplib::Client>(
          project_to_tunnel_endpoint_[project].c_str());
  http_client->set_connection_timeout(configuration_.socketConnectTimeout);
  http_client->set_read_timeout(configuration_.socketTimeout);
  http_client->set_write_timeout(configuration_.socketTimeout);

  return http_client;
}

inline std::string Client::GetRoutedEndpoint(const std::string& project) {
  if (!configuration_.tunnelEndpoint.empty()) {
    return configuration_.tunnelEndpoint;
  }
  if (project.empty()) {
    APSARA_THROW(ParameterInvalidException, "Project name is invalid");
  }

  const static std::regex re(R"(^(?:([a-z]+)://)?([^/?#]+)?(.*)?$)");

  std::cmatch m;
  if (!std::regex_match(configuration_.odpsEndpoint.c_str(), m, re)) {
    APSARA_THROW(ParameterInvalidException, "Odps endpoint url format error");
  }

  std::string host_port = m[2].str();
  std::string url_prefix = m[3].str();

  if (!url_prefix.empty()) {
    int index = url_prefix.length() - 1;
    while (index >= 0 && url_prefix[index] == '/') {
      --index;
    }
    url_prefix = url_prefix.substr(0, index + 1);
  }

  ODPS_LOG_DEBUG("host_port: %s, url_prefix: %s\n", host_port.c_str(),
                 url_prefix.c_str());

  std::shared_ptr<httplib::Client> http_client =
      std::make_shared<httplib::Client>(host_port.c_str());
  http_client->set_connection_timeout(configuration_.socketConnectTimeout);
  http_client->set_read_timeout(configuration_.socketTimeout);
  http_client->set_write_timeout(configuration_.socketTimeout);

  std::string method = "GET";

  httplib::Headers headers;
  httplib::Params params;
  params.insert({"service", ""});
  if (!configuration_.quotaName.empty()) {
    params.insert({"quotaName", configuration_.quotaName});
  }

  std::ostringstream url_stream;
  url_stream << "/projects/" << project << "/tunnel";

  Sign(configuration_, method, url_stream.str(), params, headers);

  std::string separater = "?";
  for (auto it = params.begin(); it != params.end(); ++it) {
    url_stream << separater << it->first << "=" << it->second;
    separater = "&";
  }

  auto res = http_client->Get((url_prefix + url_stream.str()).c_str(), headers);

  if (!res) {
    APSARA_THROW(
        ParameterInvalidException,
        "Fail to get tunnel endpoint: " + httplib::to_string(res.error()));
  }
  if (res->status != HTTP_OK) {
    APSARA_THROW(ParameterInvalidException,
                 "Fail to get tunnel endpoint: " + res->body);
  }

  ODPS_LOG_INFO("tunnel endpoint: %s\n", res->body.c_str());
  return res->body;
}

inline void Client::CreateReadSession(const TableBatchScanReq& request,
                                      TableBatchScanResp& response) {
  std::shared_ptr<httplib::Client> http_client =
      GetHttpClient(request.table_identifier_.project_);
  nlohmann::json j = request;
  std::string body = j.dump(4);

  std::string method = "POST";

  std::ostringstream url_stream;
  url_stream << URL_PREFIX << "/projects/" << request.table_identifier_.project_
             << "/schemas/" << request.table_identifier_.schema_ << "/tables/"
             << request.table_identifier_.table_ << "/sessions";

  httplib::Headers headers;
  headers.insert({CONTENT_TYPE, "application/json"});

  if (body != "") {
    apsara::ErrorDetection::MD5 md5;
    md5.update(body);
    headers.insert({CONTENT_MD5, md5.toString()});
  }

  httplib::Params params;
  params.insert({"session_type", "batch_read"});
  params.insert({"curr_project", configuration_.defaultProject});

  Sign(configuration_, method, url_stream.str(), params, headers);

  std::string separater = "?";
  for (auto it = params.begin(); it != params.end(); ++it) {
    url_stream << separater << it->first << "=" << it->second;
    separater = "&";
  }

  auto res = http_client->Post(url_stream.str().c_str(), headers, body,
                               "application/json");

  if (!res) {
    response.error_message_ = httplib::to_string(res.error());
    response.status_ = Status::FAIL;
    return;
  }

  ODPS_LOG_DEBUG("http status: %d, response: %s\n", res->status,
                 res->body.c_str());
  try {
    nlohmann::json resp_json = nlohmann::json::parse(res->body);
    if (res->status == HTTP_CREATED || res->status == HTTP_ACCEPTED) {
      response = resp_json;
      response.status_ =
          res->status == HTTP_CREATED ? Status::OK : Status::WAIT;
    } else {
      if (resp_json.contains("Message")) {
        response.error_message_ = resp_json["Message"].get<std::string>();
      }
      response.status_ = Status::FAIL;
    }
  } catch (nlohmann::detail::exception& e) {
    ODPS_LOG_ERROR("Fail to parse json body: %s\n", e.what());
    response.status_ = Status::FAIL;
  }
  internal::UpdateRequestID<TableBatchScanResp>(response, res);
}

inline void Client::GetReadSession(const SessionReq& request,
                                   TableBatchScanResp& response) {
  std::shared_ptr<httplib::Client> http_client =
      GetHttpClient(request.table_identifier_.project_);
  std::string method = "GET";

  std::ostringstream url_stream;
  url_stream << URL_PREFIX << "/projects/" << request.table_identifier_.project_
             << "/schemas/" << request.table_identifier_.schema_ << "/tables/"
             << request.table_identifier_.table_ << "/sessions/"
             << request.session_id_;

  httplib::Headers headers;

  httplib::Params params;
  params.insert({"session_type", "batch_read"});
  params.insert({"curr_project", configuration_.defaultProject});

  Sign(configuration_, method, url_stream.str(), params, headers);

  std::string separater = "?";
  for (auto it = params.begin(); it != params.end(); ++it) {
    url_stream << separater << it->first << "=" << it->second;
    separater = "&";
  }

  auto res = http_client->Get(url_stream.str().c_str(), headers);

  if (!res) {
    response.error_message_ = httplib::to_string(res.error());
    response.status_ = Status::FAIL;
    return;
  }

  ODPS_LOG_DEBUG("http status: %d, response: %s\n", res->status,
                 res->body.c_str());
  try {
    nlohmann::json resp_json = nlohmann::json::parse(res->body);
    if (res->status == HTTP_OK) {
      response = resp_json;
      response.status_ = Status::OK;
    } else {
      if (resp_json.contains("Message")) {
        response.error_message_ = resp_json["Message"].get<std::string>();
      }
      response.status_ = Status::FAIL;
    }
  } catch (nlohmann::detail::exception& e) {
    ODPS_LOG_ERROR("Fail to parse json body: %s\n", e.what());
    response.status_ = Status::FAIL;
  }
  internal::UpdateRequestID<TableBatchScanResp>(response, res);
}

inline void Client::ReadRows(
    const ReadRowsReq& request, ReadRowsResp& response,
    std::function<bool(const char* buf, size_t len)> read_stream) {
  std::shared_ptr<httplib::Client> http_client =
      GetHttpClient(request.table_identifier_.project_);
  std::string method = "GET";

  std::ostringstream url_stream;
  url_stream << URL_PREFIX << "/projects/" << request.table_identifier_.project_
             << "/schemas/" << request.table_identifier_.schema_ << "/tables/"
             << request.table_identifier_.table_ << "/data";

  httplib::Headers headers;
  headers.insert({"Connection", "Keep-Alive"});
  if (request.compression_ != Compression::UNCOMPRESSED) {
    headers.insert(
        {"Accept-Encoding", Compression::ToString(request.compression_)});
  }

  httplib::Params params;
  params.insert({"session_id", request.session_id_});
  params.insert({"max_batch_rows", std::to_string(request.max_batch_rows_)});
  params.insert({"split_index", std::to_string(request.split_index_)});
  params.insert({"row_count", std::to_string(request.row_count_)});
  params.insert({"row_index", std::to_string(request.row_index_)});
  if (request.data_format_.type_ != "") {
    params.insert({"data_format_type", request.data_format_.type_});
  }
  if (request.data_format_.version_ != "") {
    params.insert({"data_format_version", request.data_format_.version_});
  }
  params.insert({"curr_project", configuration_.defaultProject});

  Sign(configuration_, method, url_stream.str(), params, headers);

  std::string separater = "?";
  for (auto it = params.begin(); it != params.end(); ++it) {
    url_stream << separater << it->first << "=" << it->second;
    separater = "&";
  }

  std::string body;
  // The error message length is not longer than 1024
  size_t err_size = 1024;
  body.reserve(err_size);
  auto res = http_client->Get(url_stream.str().c_str(), headers,
                              [&](const char* data, size_t len) {
                                if (body.size() + len < body.capacity()) {
                                  // if there is error, the server will return a
                                  // json string, not a stream we need parse the
                                  // body later to know the failure reason
                                  body.append(data, len);
                                }
                                return read_stream(data, len);
                              });

  if (!res) {
    response.status_ = Status::FAIL;
    response.error_message_ = httplib::to_string(res.error());
    return;
  }

  if (res->status != HTTP_OK) {
    response.status_ = Status::FAIL;
    try {
      nlohmann::json resp_json = nlohmann::json::parse(body);
      if (resp_json.contains("Message")) {
        response.error_message_ = resp_json["Message"].get<std::string>();
      }
    } catch (nlohmann::detail::exception& e) {
      ODPS_LOG_ERROR("Fail to parse json body: %s\n", e.what());
    }
  } else {
    response.status_ = Status::OK;
  }
  internal::UpdateRequestID<ReadRowsResp>(response, res);
}

inline void Client::CreateWriteSession(const TableBatchWriteReq& request,
                                       TableBatchWriteResp& response) {
  std::shared_ptr<httplib::Client> http_client =
      GetHttpClient(request.table_identifier_.project_);
  nlohmann::json j = request;
  std::string body = j.dump(4);

  std::string method = "POST";

  std::ostringstream url_stream;
  url_stream << URL_PREFIX << "/projects/" << request.table_identifier_.project_
             << "/schemas/" << request.table_identifier_.schema_ << "/tables/"
             << request.table_identifier_.table_ << "/sessions";

  httplib::Headers headers;
  headers.insert({CONTENT_TYPE, "application/json"});

  if (body != "") {
    apsara::ErrorDetection::MD5 md5;
    md5.update(body);
    headers.insert({CONTENT_MD5, md5.toString()});
  }

  httplib::Params params;
  params.insert({"session_type", "batch_write"});
  params.insert({"curr_project", configuration_.defaultProject});

  Sign(configuration_, method, url_stream.str(), params, headers);

  std::string separater = "?";
  for (auto it = params.begin(); it != params.end(); ++it) {
    url_stream << separater << it->first << "=" << it->second;
    separater = "&";
  }

  auto res = http_client->Post(url_stream.str().c_str(), headers, body,
                               "application/json");

  if (!res) {
    response.error_message_ = httplib::to_string(res.error());
    response.status_ = Status::FAIL;
    return;
  }

  ODPS_LOG_DEBUG("http status: %d, response: %s\n", res->status,
                 res->body.c_str());

  try {
    nlohmann::json resp_json = nlohmann::json::parse(res->body);
    if (res->status == HTTP_CREATED) {
      response = resp_json;
      response.status_ = Status::OK;
    } else {
      response.status_ = Status::FAIL;
      if (resp_json.contains("Message")) {
        response.error_message_ = resp_json["Message"].get<std::string>();
      }
    }
  } catch (nlohmann::detail::exception& e) {
    ODPS_LOG_ERROR("Fail to parse json body: %s\n", e.what());
    response.status_ = Status::FAIL;
  }
  internal::UpdateRequestID<TableBatchWriteResp>(response, res);
}

inline void Client::GetWriteSession(const SessionReq& request,
                                    TableBatchWriteResp& response) {
  std::shared_ptr<httplib::Client> http_client =
      GetHttpClient(request.table_identifier_.project_);
  std::string method = "GET";

  std::ostringstream url_stream;
  url_stream << URL_PREFIX << "/projects/" << request.table_identifier_.project_
             << "/schemas/" << request.table_identifier_.schema_ << "/tables/"
             << request.table_identifier_.table_ << "/sessions/"
             << request.session_id_;

  httplib::Headers headers;
  headers.insert({CONTENT_TYPE, "application/json"});

  httplib::Params params;
  params.insert({"session_type", "batch_write"});
  params.insert({"curr_project", configuration_.defaultProject});

  Sign(configuration_, method, url_stream.str(), params, headers);

  std::string separater = "?";
  for (auto it = params.begin(); it != params.end(); ++it) {
    url_stream << separater << it->first << "=" << it->second;
    separater = "&";
  }

  auto res = http_client->Get(url_stream.str().c_str(), headers);

  if (!res) {
    response.error_message_ = httplib::to_string(res.error());
    response.status_ = Status::FAIL;
    return;
  }

  ODPS_LOG_DEBUG("http status: %d, response: %s\n", res->status,
                 res->body.c_str());

  try {
    nlohmann::json resp_json = nlohmann::json::parse(res->body);

    if (res->status == HTTP_OK) {
      response = resp_json;
      response.status_ = Status::OK;
    } else {
      response.status_ = Status::FAIL;
      if (resp_json.contains("Message")) {
        response.error_message_ = resp_json["Message"].get<std::string>();
      }
    }
  } catch (nlohmann::detail::exception& e) {
    ODPS_LOG_ERROR("Fail to parse json body: %s\n", e.what());
    response.status_ = Status::FAIL;
  }
  internal::UpdateRequestID<TableBatchWriteResp>(response, res);
}

inline void Client::WriteRows(
    const WriteRowsReq& request, WriteRowsResp& response,
    std::function<bool(httplib::DataSink& sink)> sink_func) {
  std::shared_ptr<httplib::Client> http_client =
      GetHttpClient(request.table_identifier_.project_);
  http_client->set_socket_options([](socket_t sock) {
    int snd_buf = 16 * 1024 * 1024l;
    int buf;
    socklen_t tmp = sizeof(buf);
    getsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buf, &tmp);
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &snd_buf, sizeof(snd_buf));
    getsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buf, &tmp);
  });

  std::string method = "POST";

  std::ostringstream url_stream;
  url_stream << URL_PREFIX << "/projects/" << request.table_identifier_.project_
             << "/schemas/" << request.table_identifier_.schema_ << "/tables/"
             << request.table_identifier_.table_ << "/sessions/"
             << request.session_id_ << "/data";

  httplib::Headers headers;
  headers.insert({"Content-Type", "application/octet-stream"});

  httplib::Params params;
  params.insert({"attempt_number", std::to_string(request.attempt_number_)});
  params.insert({"block_number", std::to_string(request.block_number_)});
  if (request.data_format_.type_ != "") {
    params.insert({"data_format_type", request.data_format_.type_});
  }
  if (request.data_format_.version_ != "") {
    params.insert({"data_format_version", request.data_format_.version_});
  }
  params.insert({"curr_project", configuration_.defaultProject});

  Sign(configuration_, method, url_stream.str(), params, headers);

  std::string separater = "?";
  for (auto it = params.begin(); it != params.end(); ++it) {
    url_stream << separater << it->first << "=" << it->second;
    separater = "&";
  }

  auto res = http_client->Post(
      url_stream.str().c_str(), headers,
      [&](size_t offset, httplib::DataSink& sink) { return sink_func(sink); },
      "application/octet-stream");

  if (!res) {
    response.status_ = Status::FAIL;
    response.error_message_ = httplib::to_string(res.error());
    return;
  }

  ODPS_LOG_DEBUG("status: %d, response: %s\n", res->status, res->body.c_str());
  try {
    auto resp_json = nlohmann::json::parse(res->body);
    if (res->status == HTTP_OK) {
      response.commit_message_ = resp_json["CommitMessage"].get<std::string>();
      response.status_ = Status::OK;
    } else {
      response.status_ = Status::FAIL;
      if (resp_json.contains("Message")) {
        response.error_message_ = resp_json["Message"].get<std::string>();
      }
    }
  } catch (nlohmann::detail::exception& e) {
    ODPS_LOG_ERROR("Fail to parse json body: %s\n", e.what());
    response.status_ = Status::FAIL;
  }
  internal::UpdateRequestID<WriteRowsResp>(response, res);
}

inline void Client::CommitWriteSession(
    const SessionReq& request, const std::vector<std::string>& commit_msg,
    TableBatchWriteResp& response) {
  std::shared_ptr<httplib::Client> http_client =
      GetHttpClient(request.table_identifier_.project_);
  std::string method = "POST";

  std::ostringstream url_stream;
  url_stream << URL_PREFIX << "/projects/" << request.table_identifier_.project_
             << "/schemas/" << request.table_identifier_.schema_ << "/tables/"
             << request.table_identifier_.table_ << "/commit";

  nlohmann::json j;
  j["CommitMessages"] = commit_msg;
  std::string body = j.dump(4);

  httplib::Headers headers;
  headers.insert({CONTENT_TYPE, "application/json"});

  if (body != "") {
    apsara::ErrorDetection::MD5 md5;
    md5.update(body);
    headers.insert({CONTENT_MD5, md5.toString()});
  }

  httplib::Params params;
  params.insert({"session_id", request.session_id_});
  params.insert({"curr_project", configuration_.defaultProject});

  Sign(configuration_, method, url_stream.str(), params, headers);

  std::string separater = "?";
  for (auto it = params.begin(); it != params.end(); ++it) {
    url_stream << separater << it->first << "=" << it->second;
    separater = "&";
  }

  auto res = http_client->Post(url_stream.str().c_str(), headers, body,
                               "application/json");

  if (!res) {
    response.error_message_ = httplib::to_string(res.error());
    response.status_ = Status::FAIL;
    return;
  }

  ODPS_LOG_DEBUG("http status: %d, response: %s\n", res->status,
                 res->body.c_str());
  try {
    nlohmann::json resp_json = nlohmann::json::parse(res->body);

    if (res->status == HTTP_CREATED || res->status == HTTP_ACCEPTED) {
      response = resp_json;
      response.status_ =
          res->status == HTTP_CREATED ? Status::OK : Status::WAIT;
    } else {
      if (resp_json.contains("Message")) {
        response.error_message_ = resp_json["Message"].get<std::string>();
      }
      response.status_ = Status::FAIL;
    }
  } catch (nlohmann::detail::exception& e) {
    ODPS_LOG_ERROR("Fail to parse json body: %s\n", e.what());
    response.status_ = Status::FAIL;
  }
  internal::UpdateRequestID<TableBatchWriteResp>(response, res);
}

}  // namespace storage_api
}  // namespace sdk
}  // namespace odps
}  // namespace apsara