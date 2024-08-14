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

#include "flex/storages/rt_mutable_graph/csr/mutable_csr.h"

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

void read_file(const std::string& filename, void* buffer, size_t size,
               size_t num) {
  FILE* fin = fopen(filename.c_str(), "r");
  if (fin == nullptr) {
    std::stringstream ss;
    ss << "Failed to open file " << filename << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
  size_t ret_len = 0;
  if ((ret_len = fread(buffer, size, num, fin)) != num) {
    std::stringstream ss;
    ss << "Failed to read file " << filename << ", expected " << num << ", got "
       << ret_len << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
  int ret = 0;
  if ((ret = fclose(fin)) != 0) {
    std::stringstream ss;
    ss << "Failed to close file " << filename << ", error code: " << ret << " "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
}

void write_file(const std::string& filename, const void* buffer, size_t size,
                size_t num) {
  FILE* fout = fopen(filename.c_str(), "wb");
  if (fout == nullptr) {
    std::stringstream ss;
    ss << "Failed to open file " << filename << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
  size_t ret_len = 0;
  if ((ret_len = fwrite(buffer, size, num, fout)) != num) {
    std::stringstream ss;
    ss << "Failed to write file " << filename << ", expected " << num
       << ", got " << ret_len << ", " << strerror(errno);
    LOG(ERROR) << ss.str();
  }
  int ret = 0;
  if ((ret = fclose(fout)) != 0) {
    std::stringstream ss;
    ss << "Failed to close file " << filename << ", error code: " << ret << " "
       << strerror(errno);
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
}
template class SingleMutableCsr<grape::EmptyType>;
template class MutableCsr<grape::EmptyType>;

template class SingleMutableCsr<bool>;
template class MutableCsr<bool>;

template class SingleMutableCsr<int32_t>;
template class MutableCsr<int32_t>;

template class SingleMutableCsr<uint32_t>;
template class MutableCsr<uint32_t>;

template class SingleMutableCsr<Date>;
template class MutableCsr<Date>;

template class SingleMutableCsr<int64_t>;
template class MutableCsr<int64_t>;

template class SingleMutableCsr<uint64_t>;
template class MutableCsr<uint64_t>;

template class SingleMutableCsr<double>;
template class MutableCsr<double>;

template class SingleMutableCsr<float>;
template class MutableCsr<float>;
}  // namespace gs
