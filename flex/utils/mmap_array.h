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

#ifndef GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
#define GRAPHSCOPE_UTILS_MMAP_ARRAY_H_

#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <string_view>

#include "glog/logging.h"

namespace gs {

template <typename T>
class mmap_array {
 public:
  mmap_array() : fd_(-1), data_(NULL), size_(0) {}
  ~mmap_array() { release(); }

  void open_for_read(const std::string& filename) {
    release();

    size_t filesize = std::filesystem::file_size(filename);
    fd_ = ::open(filename.c_str(), O_RDWR, 0640);
    size_ = filesize / sizeof(T);
    if (size_ != 0) {
      size_t size_in_bytes = size_ * sizeof(T);
      data_ = static_cast<T*>(mmap(NULL, size_in_bytes, PROT_READ | PROT_WRITE,
                                   MAP_PRIVATE | MAP_NORESERVE, fd_, 0));
      if (data_ == MAP_FAILED) {
        LOG(FATAL) << "mmap failed...";
      }
    }
  }

  void dump_to_file(const std::string& filename,
                    size_t size = std::numeric_limits<size_t>::max()) const {
    if (data_ == NULL || size_ == 0) {
      return;
    }
    size_t size_in_bytes = std::min(size, size_) * sizeof(T);
    if (size_in_bytes == 0) {
      return;
    }
    int fd = ::open(filename.c_str(), O_RDWR | O_CREAT, 0640);
    if (ftruncate(fd, size_ * sizeof(T)) != 0) {
      LOG(FATAL) << "ftruncate file failed...";
      return;
    }
    T* md = static_cast<T*>(
        mmap(NULL, size_in_bytes, PROT_WRITE, MAP_SHARED, fd, 0));
    if (md == MAP_FAILED) {
      perror("mmap: ");
      LOG(FATAL) << "mmap " << filename << ", with length " << size_in_bytes
                 << " failed...";
      return;
    }
    memcpy(md, data_, size_in_bytes);
    munmap(md, size_in_bytes);
    ::close(fd);
  }

  void clear() { release(); }

  void release() {
    if (data_ != NULL) {
      munmap(data_, size_ * sizeof(T));
    }
    data_ = NULL;
    size_ = 0;

    if (fd_ != -1) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  void resize(size_t size) {
    if (size == size_) {
      return;
    }
    T* new_data = static_cast<T*>(
        mmap(NULL, size * sizeof(T), PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0));
    if (data_ != NULL) {
      size_t copy_size = std::min(size_, size);
      if (copy_size > 0) {
        memcpy(new_data, data_, copy_size * sizeof(T));
      }
      munmap(data_, size_ * sizeof(T));
    }
    data_ = new_data;
    size_ = size;
    if (fd_ != 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  void resize_fill(size_t new_size, const T& value) {
    size_t old_size = size();
    resize(new_size);
    if (new_size > old_size) {
      for (size_t i = old_size; i != new_size; ++i) {
        data_[i] = value;
      }
    }
  }

  T* data() { return data_; }
  const T* data() const { return data_; }

  T& operator[](size_t index) { return data_[index]; }
  const T& operator[](size_t index) const { return data_[index]; }

  void insert(size_t index, const T& value) { data_[index] = value; }

  size_t size() const { return size_; }

  void swap(mmap_array<T>& rhs) {
    std::swap(fd_, rhs.fd_);
    std::swap(data_, rhs.data_);
    std::swap(size_, rhs.size_);
  }

 private:
  int fd_;
  T* data_;
  size_t size_;
};

struct string_item {
  uint64_t offset : 48;
  uint32_t length : 16;
};

template <>
class mmap_array<std::string_view> {
 public:
  mmap_array() : string_items_(), buffer_(), buffer_loc_(0) {}
  ~mmap_array() { release(); }

  void resize(size_t size) {
    size_t old_size = string_items_.size();
    if (old_size == size) {
      return;
    }
    if (old_size == 0 && buffer_.size() == 0) {
      string_items_.resize(size);
      buffer_.resize(size * 1024);
      buffer_loc_.store(0);
    } else if (buffer_.size() >= (size * 1024)) {
      mmap_array<string_item> new_string_items;
      new_string_items.resize(size);
      size_t accum_length = 0;
      size_t valid_length = buffer_loc_.load();
      for (size_t i = 0; i != old_size; ++i) {
        if (accum_length >= valid_length) {
          break;
        }
        new_string_items[i] = string_items_[i];
        accum_length += string_items_[i].length;
      }
      new_string_items.swap(string_items_);
      buffer_loc_.store(accum_length);
    } else {
      mmap_array<char> new_buffer;
      new_buffer.resize(size * 1024);
      memcpy(new_buffer.data(), buffer_.data(), buffer_loc_.load());

      mmap_array<string_item> new_string_items;
      new_string_items.resize(size);

      size_t accum_length = 0;
      size_t valid_length = buffer_loc_.load();
      for (size_t i = 0; i != old_size; ++i) {
        if (accum_length >= valid_length) {
          break;
        }

        new_string_items[i] = string_items_[i];
        accum_length += string_items_[i].length;
      }
      buffer_loc_.store(accum_length);
      buffer_.swap(new_buffer);
    }
  }

  void resize_fill(size_t new_size, const std::string_view& value) {
    size_t old_size = size();
    resize(new_size);

    size_t loc =
        buffer_loc_.fetch_add(value.length(), std::memory_order_relaxed);
    char* ptr = buffer_.data() + loc;
    memcpy(ptr, value.data(), value.length());

    if (new_size > old_size) {
      for (size_t i = old_size; i != new_size; ++i) {
        string_items_[i].offset = loc;
        string_items_[i].length = value.length();
      }
    }
  }

  void insert(size_t index, const std::string_view& value) {
    size_t loc =
        buffer_loc_.fetch_add(value.length(), std::memory_order_relaxed);
    char* ptr = buffer_.data() + loc;
    memcpy(ptr, value.data(), value.length());
    string_items_[index].offset = loc;
    string_items_[index].length = value.length();
  }

  void insert(size_t index, const std::string& value) {
    size_t loc =
        buffer_loc_.fetch_add(value.length(), std::memory_order_relaxed);
    char* ptr = buffer_.data() + loc;
    memcpy(ptr, value.data(), value.length());
    string_items_[index].offset = loc;
    string_items_[index].length = value.length();
  }

  void open_for_read(const std::string& filename) {
    release();

    {
      std::string meta_path = filename + ".meta";
      FILE* fin = fopen(meta_path.c_str(), "r");
      size_t buffer_loc;
      CHECK_EQ(fread(&buffer_loc, sizeof(size_t), 1, fin), 1);
      buffer_loc_.store(buffer_loc);
      fclose(fin);
    }
    string_items_.open_for_read(filename + ".items");
    buffer_.open_for_read(filename + ".data");
  }

  void dump_to_file(const std::string& filename,
                    size_t size = std::numeric_limits<size_t>::max()) {
    size = std::min(size, string_items_.size());
    size_t buffer_loc = buffer_loc_.load();
    {
      std::string meta_path = filename + ".meta";
      FILE* fout = fopen(meta_path.c_str(), "wb");
      CHECK_EQ(fwrite(&buffer_loc, sizeof(size_t), 1, fout), 1);
      fflush(fout);
      fclose(fout);
    }
    string_items_.dump_to_file(filename + ".items", size);
    buffer_.dump_to_file(filename + ".data", buffer_loc);
  }

  std::string_view operator[](size_t index) const {
    return std::string_view(buffer_.data() + string_items_[index].offset,
                            string_items_[index].length);
  }

  size_t size() const { return string_items_.size(); }

  void release() {
    string_items_.release();
    buffer_.release();
    buffer_loc_.store(0);
  }

  void clear() { release(); }

  void swap(mmap_array<std::string_view>& rhs) {
    string_items_.swap(rhs.string_items_);
    buffer_.swap(rhs.buffer_);
    size_t tmp = buffer_loc_.load();
    buffer_loc_.store(rhs.buffer_loc_.load());
    rhs.buffer_loc_.store(tmp);
  }

 private:
  mmap_array<string_item> string_items_;
  mmap_array<char> buffer_;
  std::atomic<size_t> buffer_loc_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
