#pragma once

#include <queue>
#include "storage_api.hpp"

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "arrow/type_fwd.h"
#include "common/log.h"
#include "common/thread_pool.h"

namespace apsara {
namespace odps {
namespace sdk {
namespace storage_api {
namespace arrow_adapter {
namespace internal {
class ArrowStreamListener;
}  // namespace internal

class Reader;
class Writer;

class ArrowClient {
 public:
  /**
   *  @param configuration Configuration for AK and endpoint.
   */
  ArrowClient(const Configuration& configuration);

  /**
   *  @brief Create a batch read session.
   *
   *  @param request Split parameter sent to the server.
   *  @param response Split response returned from the server.
   */
  void CreateReadSession(const TableBatchScanReq& request,
                         TableBatchScanResp& response);

  /**
   *  @brief Get the batch read session.
   *
   *  @param request  Create read session request parameters.
   *  @param response Session info returned from the server.
   */
  void GetReadSession(const SessionReq& request, TableBatchScanResp& response);

  /**
   *  @brief Read one split of the read session.
   *
   *  @param request Read rows request parameters.
   *  @param cache_size Number of not read record baches cached in the memory.
   *
   *  @return Record batch reader.
   */
  std::shared_ptr<Reader> ReadRows(const ReadRowsReq& request,
                                   size_t cache_size = 2);

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
   *  @param cache_size Number of not written record baches cached in the
   * memory.
   *
   *  @return Record batch writer.
   */
  std::shared_ptr<Writer> WriteRows(const WriteRowsReq& request,
                                    size_t cache_size = 2);

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
  std::shared_ptr<Client> client_;
};

class Reader {
 public:
  Reader(size_t cache_size);
  ~Reader();

  /**
   *  @brief Read one record batch.
   *
   *  @param record_batch Data read by the reader.
   *
   *  @return True if has record_batch, false means all the data is read or
   * there is error.
   */
  bool Read(std::shared_ptr<arrow::RecordBatch>& record_batch);

  /**
   *  @brief Get the status of the reader.
   *
   *  @return Status::OK or Status::FAIL.
   */
  Status GetStatus();

  /**
   *  @brief Get the error message if the request fail.
   *
   *  @return Error message.
   */
  const std::string& GetErrorMessage();

  /**
   *  @brief Get the request id.
   *
   *  @return Request id.
   */
  const std::string& GetRequestID();

  /**
   *  @brief Cancel the arrow reader.
   *
   *  @return True or False.
   */
  bool Cancel();

 private:
  friend class ArrowClient;
  friend class internal::ArrowStreamListener;

  // the data are async read from the server and cached in the record_batches_
  BlockingQueue<std::shared_ptr<arrow::RecordBatch>> record_batches_;
  ReadRowsResp resp_;

  bool canceled_ = false;
  std::thread read_rows_thread_;

 private:
  bool Push(std::shared_ptr<arrow::RecordBatch> record_batch);
  void RequestDone(ReadRowsResp& resp);
  void CreateReadRowsThread(const ReadRowsReq request,
                            std::shared_ptr<Client> client);
  void ReadRowsThread(const ReadRowsReq request,
                      std::shared_ptr<Client> client);
};

class Writer {
 public:
  Writer(size_t cache_size);
  ~Writer();

  /**
   *  @brief Finish the write stream.
   *
   *  @param commmit_message Returned by the server, user should bring this
   * message to do the final batch commit. Note: After invoking Finish(), the
   * following invoke of Write() will fail.
   *
   *  @return Success or not.
   */
  bool Finish(std::string& commmit_message);

  /**
   *  @brief Write one record batch.
   *
   *  @param record_batch The record batch to be written.
   *
   *  @return Whether there is error when writing the data.
   *          Note: As the record batch is first cached in the memory and sent
   * to server later, return true doesn't mean the data has been transferred to
   * the server.
   */
  bool Write(std::shared_ptr<arrow::RecordBatch> record_batch);

  /**
   *  @brief Get the record batch write request status.
   *
   *  @return Status::OK or Status::FAIL.
   */
  Status GetStatus();

  /**
   *  @brief Get the error message if the request fail.
   *
   *  @return Error message.
   */
  const std::string& GetErrorMessage();

  /**
   *  @brief Get the request id.
   *
   *  @return Request id.
   */
  const std::string& GetRequestID();

 private:
  friend class ArrowClient;
  // the data to be written cached in record_batches_
  BlockingQueue<std::shared_ptr<arrow::RecordBatch>> record_batches_;
  WriteRowsResp resp_;
  bool stopped_ = false;
  std::thread write_rows_thread_;

 private:
  void RequestDone(WriteRowsResp& resp);
  bool Pop(std::shared_ptr<arrow::RecordBatch>& record_batch);
  void CreateWriteRowsThread(const WriteRowsReq request,
                             std::shared_ptr<Client> client);
  void WriteRowsThread(const WriteRowsReq request,
                       std::shared_ptr<Client> client);
};

namespace internal {

class ArrowStreamListener : public arrow::ipc::Listener {
 public:
  ArrowStreamListener(Reader* reader) : reader_(reader) {}

  virtual ~ArrowStreamListener() = default;

  arrow::Status OnSchemaDecoded(
      std::shared_ptr<arrow::Schema> schema) override {
    return arrow::Status::OK();
  }

  arrow::Status OnRecordBatchDecoded(
      std::shared_ptr<arrow::RecordBatch> record_batch) override {
    auto status = record_batch->Validate();
    if (!status.ok()) {
      return arrow::Status::Invalid("Record batch is not valid");
    }
    bool suc = reader_->Push(record_batch);
    if (suc) {
      return arrow::Status::OK();
    } else {
      ODPS_LOG_ERROR("Fail to push record batch to the blocking queue\n");
      return arrow::Status::IOError("Fail to push record batch");
    }
  }

 private:
  Reader* reader_ = nullptr;
};

class ArrowWriter : public arrow::io::OutputStream {
 public:
  ArrowWriter(httplib::DataSink& sink) : sink_(sink) {}
  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    return arrow::Result<int64_t>(already_sent_);
  }

  arrow::Status Write(const void* data, int64_t nbytes) override {
    bool suc = sink_.write((char*) data, nbytes);
    if (!suc) {
      ODPS_LOG_ERROR("Fail to write arrow bytes: %s\n", strerror(errno));
      return arrow::Status::IOError("Fail to write data");
    }
    already_sent_ += nbytes;
    return arrow::Status::OK();
  }
  bool closed() const override { return closed_; }

 private:
  bool closed_ = false;
  uint64_t already_sent_ = 0;
  httplib::DataSink& sink_;
};

inline arrow::Compression::type ToArrowCompression(Compression::type t) {
  switch (t) {
  case Compression::UNCOMPRESSED:
    return arrow::Compression::UNCOMPRESSED;
  case Compression::ZSTD:
    return arrow::Compression::ZSTD;
  case Compression::LZ4_FRAME:
    return arrow::Compression::LZ4_FRAME;
  default:
    return arrow::Compression::UNCOMPRESSED;
  }
}

}  // namespace internal

inline ArrowClient::ArrowClient(const Configuration& configuration) {
  client_ = std::make_shared<Client>(configuration);
}

inline void ArrowClient::CreateReadSession(const TableBatchScanReq& request,
                                           TableBatchScanResp& response) {
  client_->CreateReadSession(request, response);
}

inline void ArrowClient::GetReadSession(const SessionReq& request,
                                        TableBatchScanResp& response) {
  client_->GetReadSession(request, response);
}

inline std::shared_ptr<Reader> ArrowClient::ReadRows(const ReadRowsReq& request,
                                                     size_t cache_size) {
  std::shared_ptr<Reader> reader = std::make_shared<Reader>(cache_size);
  reader->CreateReadRowsThread(request, client_);
  return reader;
}

inline void ArrowClient::CreateWriteSession(const TableBatchWriteReq& request,
                                            TableBatchWriteResp& response) {
  client_->CreateWriteSession(request, response);
}

inline void ArrowClient::GetWriteSession(const SessionReq& request,
                                         TableBatchWriteResp& response) {
  client_->GetWriteSession(request, response);
}

inline std::shared_ptr<Writer> ArrowClient::WriteRows(
    const WriteRowsReq& request, size_t cache_size) {
  std::shared_ptr<Writer> writer = std::make_shared<Writer>(cache_size);
  writer->CreateWriteRowsThread(request, client_);
  return writer;
}

inline void ArrowClient::CommitWriteSession(
    const SessionReq& request, const std::vector<std::string>& commit_msg,
    TableBatchWriteResp& response) {
  client_->CommitWriteSession(request, commit_msg, response);
}

inline Reader::Reader(size_t cache_size) : record_batches_(cache_size) {}

inline Reader::~Reader() {
  record_batches_.ShutDown();
  if (read_rows_thread_.joinable()) {
    read_rows_thread_.join();
  }
}

inline bool Reader::Read(std::shared_ptr<arrow::RecordBatch>& record_batch) {
  if (canceled_) {
    return false;
  }
  return record_batches_.Get(record_batch);
}

inline Status Reader::GetStatus() { return resp_.status_; }

inline const std::string& Reader::GetErrorMessage() {
  return resp_.error_message_;
}

inline const std::string& Reader::GetRequestID() { return resp_.request_id_; }

inline bool Reader::Cancel() {
  if (canceled_) {
    return true;
  }

  canceled_ = true;
  record_batches_.ShutDown();
  if (read_rows_thread_.joinable()) {
    read_rows_thread_.join();
  }

  resp_.status_ = Status::CANCELED;
  resp_.error_message_ = "Client canceled";
  return true;
}

inline bool Reader::Push(std::shared_ptr<arrow::RecordBatch> record_batch) {
  return record_batches_.Put(record_batch);
}

inline void Reader::RequestDone(ReadRowsResp& resp) {
  resp_ = resp;
  record_batches_.ShutDown();
}

inline void Reader::CreateReadRowsThread(const ReadRowsReq request,
                                         std::shared_ptr<Client> client) {
  read_rows_thread_ =
      std::thread(&Reader::ReadRowsThread, this, request, client);
}

inline void Reader::ReadRowsThread(const ReadRowsReq request,
                                   std::shared_ptr<Client> client) {
  struct timeval start, end;
  size_t total = 0;
  ReadRowsResp response;
  auto listener = std::make_shared<internal::ArrowStreamListener>(this);
  auto decoder = std::make_shared<arrow::ipc::StreamDecoder>(listener);
  gettimeofday(&start, NULL);
  client->ReadRows(request, response, [&](const char* data, size_t len) {
    total += len;
    auto result = arrow::AllocateBuffer(len);
    if (!result.ok()) {
      ODPS_LOG_ERROR("Allocate buffer error: Out of memory\n");
      throw std::bad_alloc();
    }
    auto buffer = std::move(result).ValueOrDie();
    // we should guarantee the memory is valid during record_batch processing
    // and so copy the memory here
    memcpy(buffer->mutable_data(), data, len);
    auto status = decoder->Consume(std::move(buffer));
    if (status.ok()) {
      return true;
    } else {
      return false;
    }
  });
  gettimeofday(&end, NULL);
  auto cost =
      ((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec)) /
      1000000.0;
  ODPS_LOG_DEBUG(
      "compression: %s, total: %ld, cost %f seconds, read speed: %f MB/s\n",
      Compression::ToString(request.compression_).c_str(), total, cost,
      total / cost / 1024 / 1024);
  RequestDone(response);
}

inline Writer::Writer(size_t cache_size) : record_batches_(cache_size) {}

inline Writer::~Writer() {
  record_batches_.ShutDown();
  if (write_rows_thread_.joinable()) {
    write_rows_thread_.join();
  }
}

inline bool Writer::Finish(std::string& commit_message) {
  stopped_ = true;
  record_batches_.ShutDown();
  if (write_rows_thread_.joinable()) {
    write_rows_thread_.join();
  }

  if (resp_.status_ == Status::OK) {
    commit_message = resp_.commit_message_;
    return true;
  } else {
    return false;
  }
}

inline void Writer::RequestDone(WriteRowsResp& resp) {
  record_batches_.ShutDown();
  resp_ = resp;
}

inline bool Writer::Pop(std::shared_ptr<arrow::RecordBatch>& record_batch) {
  return record_batches_.Get(record_batch);
}

inline bool Writer::Write(std::shared_ptr<arrow::RecordBatch> record_batch) {
  if (stopped_) {
    ODPS_LOG_ERROR(
        "The stream is stopped or the record_batch will not be written\n");
    return false;
  }
  return record_batches_.Put(record_batch);
}

inline Status Writer::GetStatus() { return resp_.status_; }

inline const std::string& Writer::GetErrorMessage() {
  return resp_.error_message_;
}

inline const std::string& Writer::GetRequestID() { return resp_.request_id_; }

inline void Writer::CreateWriteRowsThread(const WriteRowsReq request,
                                          std::shared_ptr<Client> client) {
  write_rows_thread_ =
      std::thread(&Writer::WriteRowsThread, this, request, client);
}

inline void Writer::WriteRowsThread(const WriteRowsReq request,
                                    std::shared_ptr<Client> client) {
  WriteRowsResp response;
  arrow::ipc::IpcWriteOptions ipc_options =
      arrow::ipc::IpcWriteOptions::Defaults();
  ipc_options.codec = arrow::util::Codec::Create(
                          internal::ToArrowCompression(request.compression_))
                          .ValueUnsafe();
  bool has_error = false;
  struct timeval start, end;
  size_t total;
  gettimeofday(&start, NULL);
  client->WriteRows(request, response, [&](httplib::DataSink& sink) {
    std::shared_ptr<arrow::RecordBatch> record_batch;
    auto arrow_writer = std::make_shared<internal::ArrowWriter>(sink);
    std::shared_ptr<arrow::ipc::RecordBatchWriter> stream_writer;
    while (Pop(record_batch)) {
      // write record_batch
      if (stream_writer == nullptr) {
        stream_writer =
            arrow::ipc::MakeStreamWriter(arrow_writer.get(),
                                         record_batch->schema(), ipc_options)
                .ValueUnsafe();
        if (stream_writer == nullptr) {
          has_error = true;
          ODPS_LOG_DEBUG("Fail to create arrow stream writer\n");
          break;
        }
      }
      auto result = stream_writer->WriteRecordBatch(*record_batch);
      if (!result.ok()) {
        has_error = true;
        break;
      }
    }
    sink.done();
    total = arrow_writer->Tell().ValueUnsafe();
    return true;
  });
  gettimeofday(&end, NULL);
  auto cost =
      ((end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec)) /
      1000000.0;
  ODPS_LOG_DEBUG(
      "compression: %s, total: %ld, cost %f seconds, write speed: %f MB/s\n",
      Compression::ToString(request.compression_).c_str(), total, cost,
      total / cost / 1024 / 1024);
  if (has_error) {
    response.status_ = Status::FAIL;
  }
  RequestDone(response);
}

}  // namespace arrow_adapter
}  // namespace storage_api
}  // namespace sdk
}  // namespace odps
}  // namespace apsara
