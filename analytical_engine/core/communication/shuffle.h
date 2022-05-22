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
#ifndef ANALYTICAL_ENGINE_CORE_COMMUNICATION_SHUFFLE_H_
#define ANALYTICAL_ENGINE_CORE_COMMUNICATION_SHUFFLE_H_

#include <string>

#include "grape/communication/shuffle.h"  // IWYU pragma: export
#include "vineyard/graph/utils/string_collection.h"

namespace grape {
/**
 * @brief ShuffleUnit wraps a vector, for data shuffling between workers.
 * The templated ShuffleUnit is defined in libgrape-lite. This is the
 * specialized ShuffleUnit for string data type to achieve high performance.
 */
template <>
class ShuffleUnit<std::string> {
 public:
  ShuffleUnit() {}
  ~ShuffleUnit() {}
  using BufferT = RSVector;
  using ValueT = RefString;

  void emplace(const ValueT& v) { buffer_.emplace(v); }
  void clear() { buffer_.clear(); }
  size_t size() const { return buffer_.size(); }
  BufferT& data() { return buffer_; }
  const BufferT& data() const { return buffer_; }

  void SendTo(int dst_worker_id, int tag, MPI_Comm comm) {
    rsv_header header(buffer_.size_in_bytes(), buffer_.size());
    MPI_Send(&header, static_cast<int>(sizeof(rsv_header)), MPI_CHAR,
             dst_worker_id, tag, comm);
    if (header.size) {
      MPI_Send(buffer_.data(), static_cast<int>(header.size), MPI_CHAR,
               dst_worker_id, tag, comm);
    }
  }

  void RecvFrom(int src_worker_id, int tag, MPI_Comm comm) {
    size_t old_size = buffer_.size_in_bytes();
    rsv_header header;
    MPI_Recv(&header, static_cast<int>(sizeof(rsv_header)), MPI_CHAR,
             src_worker_id, tag, comm, MPI_STATUS_IGNORE);
    if (header.size) {
      buffer_.resize(header.size + old_size, header.count + buffer_.size());
      MPI_Recv(buffer_.data() + old_size, static_cast<int>(header.size),
               MPI_CHAR, src_worker_id, tag, comm, MPI_STATUS_IGNORE);
    }
  }

 private:
  struct rsv_header {
    rsv_header() {}
    rsv_header(size_t s, size_t c) : size(s), count(c) {}
    size_t size;
    size_t count;
  };
  BufferT buffer_;
};

}  // namespace grape
#endif  // ANALYTICAL_ENGINE_CORE_COMMUNICATION_SHUFFLE_H_
