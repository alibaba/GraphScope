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

#include <assert.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <string_view>

#include "flex/storages/rt_mutable_graph/file_names.h"
#include "glog/logging.h"
#include "grape/util.h"

namespace gs {
template <typename T>
class mmap_array {
 public:
  mmap_array()
      : filename_(""),
        fd_(-1),
        data_(NULL),
        size_(0),
        read_only_(false),
        memory_only_(true) {}
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() {}

  void reset() {
    filename_ = "";
    if (data_ != NULL) {
      munmap(data_, size_ * sizeof(T));
      data_ = NULL;
    }
    if (fd_ != -1) {
      close(fd_);
      fd_ = -1;
    }
    size_ = 0;
    read_only_ = false;
    memory_only_ = true;
  }

  void open(const std::string& filename, bool read_only) {
    reset();
    filename_ = filename;
    read_only_ = read_only;
    memory_only_ = false;
    if (read_only) {
      if (!std::filesystem::exists(filename)) {
        LOG(ERROR) << "file not exists: " << filename;
        fd_ = 1;
        size_ = 0;
        data_ = NULL;
      } else {
        fd_ = ::open(filename.c_str(), O_RDONLY, 0777);
        size_t file_size = std::filesystem::file_size(filename);
        size_ = file_size / sizeof(T);
        if (size_ == 0) {
          data_ = NULL;
        } else {
          data_ = reinterpret_cast<T*>(
              mmap(NULL, size_ * sizeof(T), PROT_READ, MAP_PRIVATE, fd_, 0));
        }
      }
    } else {
      bool creat = false;
      if (!std::filesystem::exists(filename)) {
        creat = true;
      }
      fd_ = ::open(filename.c_str(), O_RDWR | O_CREAT, 0777);
      if (fd_ == -1) {
        LOG(FATAL) << "open file failed " << filename << strerror(errno)
                   << "\n";
      }
      if (creat) {
        std::filesystem::perms readWritePermission =
            std::filesystem::perms::owner_read |
            std::filesystem::perms::owner_write;
        std::error_code errorCode;
        std::filesystem::permissions(filename, readWritePermission,
                                     std::filesystem::perm_options::add,
                                     errorCode);
        if (errorCode) {
          LOG(INFO) << "Failed to set read/write permission for file: "
                    << filename << " " << errorCode.message() << std::endl;
        }
      }

      size_t file_size = std::filesystem::file_size(filename);
      size_ = file_size / sizeof(T);
      if (size_ == 0) {
        data_ = NULL;
      } else {
        data_ = reinterpret_cast<T*>(mmap(NULL, size_ * sizeof(T),
                                          PROT_READ | PROT_WRITE, MAP_SHARED,
                                          fd_, 0));
      }
    }
    if (data_ == MAP_FAILED) {
      LOG(FATAL) << "mmap failed " << filename << " " << strerror(errno)
                 << "..\n";
    }
    madvise(data_, size_ * sizeof(T), MADV_RANDOM | MADV_WILLNEED);
  }

#ifdef HUGEPAGE
#ifdef __ia64__
#define ADDR (void*) (0x8000000000000000UL)
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_FIXED)
#else
#define ADDR (void*) (0x0UL)
#define FLAGS (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB)
#endif

#define PROTECTION (PROT_READ | PROT_WRITE)

#define HUGEPAGE_SIZE (2UL * 1024 * 1024)
#define HUGEPAGE_MASK (2UL * 1024 * 1024 - 1UL)
#define ROUND_UP(size) (((size) + HUGEPAGE_MASK) & (~HUGEPAGE_MASK))
#endif

  void open_in_memory(const std::string& filename) {
    reset();
#ifndef HUGEPAGE
    filename_ = filename;
    if (!filename_.empty() && std::filesystem::exists(filename_)) {
      size_t file_size = std::filesystem::file_size(filename_);
      fd_ = ::open(filename_.c_str(), O_RDWR, 0777);
      size_ = file_size / sizeof(T);
      if (size_ != 0) {
        size_t size_in_bytes = size_ * sizeof(T);
        data_ = reinterpret_cast<T*>(mmap(NULL, size_in_bytes,
                                          PROT_READ | PROT_WRITE,
                                          MAP_PRIVATE | MAP_NORESERVE, fd_, 0));
        if (data_ == MAP_FAILED) {
          LOG(FATAL) << "mmap failed " << filename_ << " " << strerror(errno)
                     << "..\n";
        }
      }
    }
#else
    if (!filename.empty() && std::filesystem::exists(filename)) {
      LOG(INFO) << "open in memory with hugepage...";
      size_t file_size = std::filesystem::file_size(filename);
      size_ = file_size / sizeof(T);
      if (size_ != 0) {
        size_t size_in_bytes = size_ * sizeof(T);
        data_ = static_cast<T*>(
            mmap(ADDR, ROUND_UP(size_in_bytes), PROTECTION, FLAGS, -1, 0));
        if (data_ == MAP_FAILED) {
          LOG(FATAL) << "mmap failed " << filename << " " << strerror(errno)
                     << "..\n";
        }
        FILE* fin = fopen(filename.c_str(), "rb");
        CHECK_EQ(fread(data_, size_in_bytes, 1, fin), 1);
      }
    }
#endif
  }

  void dump(const std::string& filename) {
    assert(!filename_.empty());
    assert(std::filesystem::exists(filename_));
    std::string old_filename = filename_;
    reset();
    if (read_only_) {
      std::filesystem::create_hard_link(old_filename, filename);
    } else {
      std::filesystem::rename(old_filename, filename);
    }
    std::filesystem::perms readPermission = std::filesystem::perms::owner_read;

    std::error_code errorCode;
    std::filesystem::permissions(filename, readPermission,
                                 std::filesystem::perm_options::add, errorCode);

    if (errorCode) {
      LOG(INFO) << "Failed to set read permission for file: " << filename << " "
                << errorCode.message() << std::endl;
    }
  }

  void resize(size_t size) {
    if (size == size_) {
      return;
    }

    if (memory_only_) {
      if (size == 0) {
        reset();
      } else {
#ifndef HUGEPAGE
        T* new_data = static_cast<T*>(
            mmap(NULL, size * sizeof(T), PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0));
        if (new_data == MAP_FAILED) {
          LOG(FATAL) << "mmap failed " << strerror(errno) << "..\n";
        }
        if (data_ != NULL) {
          size_t copy_size = std::min(size, size_);
          if (copy_size > 0) {
            memcpy((void*) new_data, (void*) data_, copy_size * sizeof(T));
          }
          munmap(data_, size_ * sizeof(T));
        }
        data_ = new_data;
        size_ = size;
        if (fd_ != -1) {
          filename_.clear();
          close(fd_);
          fd_ = -1;
        }
#else
        LOG(INFO) << "resize with hugepage...";
        T* new_data = static_cast<T*>(
            mmap(ADDR, ROUND_UP(size * sizeof(T)), PROTECTION, FLAGS, -1, 0));
        if (new_data == MAP_FAILED) {
          LOG(FATAL) << "mmap failed " << strerror(errno) << "..\n";
        }

        if (data_ != NULL) {
          size_t copy_size = std::min(size, size_);
          if (copy_size > 0) {
            memcpy((void*)new_data, (void*) data_, copy_size * sizeof(T));
          }
          munmap(data_, ROUND_UP(size_ * sizeof(T)));
        }
        data_ = new_data;
        size_ = size;
#endif
      }
    } else {
      if (read_only_) {
        if (size < size_) {
          munmap(data_, size_ * sizeof(T));
          size_ = size;
          data_ = reinterpret_cast<T*>(
              mmap(NULL, size_ * sizeof(T), PROT_READ, MAP_PRIVATE, fd_, 0));

        } else if (size * sizeof(T) < std::filesystem::file_size(filename_)) {
          munmap(data_, size_ * sizeof(T));
          size_ = size;
          data_ = reinterpret_cast<T*>(
              mmap(NULL, size_ * sizeof(T), PROT_READ, MAP_PRIVATE, fd_, 0));

        } else {
          LOG(FATAL)
              << "cannot resize read-only mmap_array to larger size than file";
        }
      } else {
        if (data_ != NULL) {
          munmap(data_, size_ * sizeof(T));
        }
        int rt = ftruncate(fd_, size * sizeof(T));
        if (rt == -1) {
          LOG(FATAL) << "ftruncate failed: " << rt << " " << strerror(errno)
                     << "\n";
        }
        if (size == 0) {
          data_ = NULL;
        } else {
          data_ = static_cast<T*>(::mmap(NULL, size * sizeof(T),
                                         PROT_READ | PROT_WRITE, MAP_SHARED,
                                         fd_, 0));
        }
        size_ = size;
      }
      if (data_ == MAP_FAILED) {
        LOG(FATAL) << "mmap failed " << strerror(errno) << "\n";
      }
    }
  }

  bool read_only() const { return read_only_; }

#undef ADDR
#undef FLAGS
#undef HUGEPAGE_SIZE
#undef HUGEPAGE_MASK
#undef ROUND_UP

  void touch(const std::string& filename) {
    if (read_only_) {
      if (filename_ != "") {
        copy_file(filename_, filename);
      }
      open(filename, false);
    }
  }

  T* data() { return data_; }
  const T* data() const { return data_; }

  void set(size_t idx, const T& val) { data_[idx] = val; }

  const T& get(size_t idx) const { return data_[idx]; }

  const T& operator[](size_t idx) const { return data_[idx]; }
  T& operator[](size_t idx) { return data_[idx]; }

  size_t size() const { return size_; }

  void swap(mmap_array<T>& rhs) {
    std::swap(filename_, rhs.filename_);
    std::swap(fd_, rhs.fd_);
    std::swap(data_, rhs.data_);
    std::swap(size_, rhs.size_);
    std::swap(read_only_, rhs.read_only_);
    std::swap(memory_only_, rhs.memory_only_);
  }

  const std::string& filename() const { return filename_; }

 private:
  std::string filename_;
  int fd_;
  T* data_;
  size_t size_;

  bool read_only_;
  bool memory_only_;
};

struct string_item {
  uint64_t offset : 48;
  uint32_t length : 16;
};

template <>
class mmap_array<std::string_view> {
 public:
  mmap_array() {}
  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() {}

  void reset() {
    items_.reset();
    data_.reset();
  }

  void open(const std::string& filename, bool read_only) {
    items_.open(filename + ".items", read_only);
    data_.open(filename + ".data", read_only);
  }

  void open_in_memory(const std::string& filename) {
    items_.open_in_memory(filename + ".items");
    data_.open_in_memory(filename + ".data");
  }

  bool read_only() const { return items_.read_only(); }

  void touch(const std::string& filename) {
    items_.touch(filename + ".items");
    data_.touch(filename + ".data");
  }

  void dump(const std::string& filename) {
    items_.dump(filename + ".items");
    data_.dump(filename + ".data");
  }

  void resize(size_t size, size_t data_size) {
    items_.resize(size);
    data_.resize(data_size);
  }

  void set(size_t idx, size_t offset, const std::string_view& val) {
    items_.set(idx, {offset, static_cast<uint32_t>(val.size())});
    memcpy(data_.data() + offset, val.data(), val.size());
  }

  std::string_view get(size_t idx) const {
    const string_item& item = items_.get(idx);
    return std::string_view(data_.data() + item.offset, item.length);
  }

  size_t size() const { return items_.size(); }

  size_t data_size() const { return data_.size(); }

  void swap(mmap_array& rhs) {
    items_.swap(rhs.items_);
    data_.swap(rhs.data_);
  }

 private:
  mmap_array<string_item> items_;
  mmap_array<char> data_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_UTILS_MMAP_ARRAY_H_
