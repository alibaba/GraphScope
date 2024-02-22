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

inline void* allocate_hugepages(size_t size) {
  return mmap(ADDR, ROUND_UP(size), PROTECTION, FLAGS, -1, 0);
}

inline size_t hugepage_round_up(size_t size) { return ROUND_UP(size); }

#undef ADDR
#undef FLAGS
#undef HUGEPAGE_SIZE
#undef HUGEPAGE_MASK
#undef ROUND_UP

namespace gs {

enum class MemoryStrategy {
  kSyncToFile,
  kMemoryOnly,
  kHugepagePrefered,
};

template <typename T>
class mmap_array {
 public:
  mmap_array()
      : filename_(""),
        fd_(-1),
        data_(NULL),
        size_(0),
        mmap_size_(0),
        sync_to_file_(false),
        hugepage_prefered_(false) {}

  mmap_array(const mmap_array<T>& rhs) : fd_(-1) {
    resize(rhs.size_);
    memcpy(data_, rhs.data_, size_ * sizeof(T));
  }

  mmap_array(mmap_array&& rhs) : mmap_array() { swap(rhs); }
  ~mmap_array() {}

  void reset() {
    filename_ = "";
    if (data_ != NULL && mmap_size_ != 0) {
      munmap(data_, mmap_size_);
    }
    data_ = NULL;
    size_ = 0;
    mmap_size_ = 0;
    if (fd_ != -1) {
      close(fd_);
      fd_ = -1;
    }
    sync_to_file_ = false;
  }

  void set_hugepage_prefered(bool val) {
    hugepage_prefered_ = (val && !sync_to_file_);
  }

  void open(const std::string& filename, bool sync_to_file = false) {
    reset();
    filename_ = filename;
    sync_to_file_ = sync_to_file;
    hugepage_prefered_ = false;
    if (sync_to_file_) {
      bool creat = !std::filesystem::exists(filename_);
      fd_ = ::open(filename_.c_str(), O_RDWR | O_CREAT, 0777);
      if (fd_ == -1) {
        LOG(FATAL) << "open file [" << filename_ << "] failed, "
                   << strerror(errno);
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

      size_t file_size = std::filesystem::file_size(filename_);
      size_ = file_size / sizeof(T);
      mmap_size_ = file_size;
      if (mmap_size_ == 0) {
        data_ = NULL;
      } else {
        data_ = reinterpret_cast<T*>(
            mmap(NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        if (data_ == MAP_FAILED) {
          LOG(FATAL) << "mmap file [" << filename_ << "] failed, "
                     << strerror(errno);
        }
        madvise(data_, mmap_size_, MADV_RANDOM | MADV_WILLNEED);
      }
    } else {
      if (!filename_.empty() && std::filesystem::exists(filename_)) {
        size_t file_size = std::filesystem::file_size(filename_);
        fd_ = ::open(filename_.c_str(), O_RDWR, 0777);
        size_ = file_size / sizeof(T);
        mmap_size_ = file_size;
        if (mmap_size_ == 0) {
          data_ = NULL;
        } else {
          data_ = reinterpret_cast<T*>(mmap(
              NULL, mmap_size_, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd_, 0));
          if (data_ == MAP_FAILED) {
            LOG(FATAL) << "mmap file [" << filename_ << "] failed, "
                       << strerror(errno);
          }
        }
      }
    }
  }

  void open_with_hugepages(const std::string& filename, size_t capacity = 0) {
    reset();
    hugepage_prefered_ = true;
    if (!filename.empty() && std::filesystem::exists(filename)) {
      size_t file_size = std::filesystem::file_size(filename);
      size_ = file_size / sizeof(T);
      if (size_ != 0) {
        capacity = std::max(capacity, size_);
        mmap_size_ = hugepage_round_up(capacity * sizeof(T));
        data_ = static_cast<T*>(allocate_hugepages(mmap_size_));
        if (data_ != MAP_FAILED) {
          FILE* fin = fopen(filename.c_str(), "rb");
          CHECK_EQ(fread(data_, sizeof(T), size_, fin), size_);
          fclose(fin);
        } else {
          LOG(ERROR) << "allocating hugepage failed, " << strerror(errno)
                     << ", try with normal pages";
          open(filename, false);
        }
      } else {
        mmap_size_ = 0;
      }
    }
  }

  void dump(const std::string& filename) {
    if (sync_to_file_) {
      std::string old_filename = filename_;
      reset();
      std::filesystem::rename(old_filename, filename);
    } else {
      FILE* fout = fopen(filename.c_str(), "wb");
      CHECK_EQ(fwrite(data_, sizeof(T), size_, fout), size_);
      fflush(fout);
      fclose(fout);
      reset();
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

    if (sync_to_file_) {
      if (data_ != NULL && mmap_size_ != 0) {
        munmap(data_, mmap_size_);
      }
      size_t new_mmap_size = size * sizeof(T);
      int rt = ftruncate(fd_, new_mmap_size);
      if (rt == -1) {
        LOG(FATAL) << "ftruncate failed: " << rt << ", " << strerror(errno);
      }
      if (new_mmap_size == 0) {
        data_ = NULL;
      } else {
        data_ = reinterpret_cast<T*>(mmap(
            NULL, new_mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        if (data_ == MAP_FAILED) {
          LOG(FATAL) << "mmap failed " << strerror(errno);
        }
      }
      size_ = size;
      mmap_size_ = new_mmap_size;
    } else {
      size_t target_mmap_size = size * sizeof(T);
      if (target_mmap_size <= mmap_size_) {
        size_ = size;
      } else {
        T* new_data = NULL;
        size_t new_mmap_size = size * sizeof(T);
        if (hugepage_prefered_) {
          new_data = reinterpret_cast<T*>(allocate_hugepages(new_mmap_size));
          if (new_data == MAP_FAILED) {
            LOG(ERROR) << "mmap with hugepage failed, " << strerror(errno)
                       << ", try with normal pages";
            new_data = NULL;
          } else {
            new_mmap_size = hugepage_round_up(new_mmap_size);
          }
        }
        if (new_data == NULL) {
          new_data = reinterpret_cast<T*>(
              mmap(NULL, new_mmap_size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
          if (new_data == MAP_FAILED) {
            LOG(FATAL) << "mmap failed " << strerror(errno);
          }
        }

        size_t copy_size = std::min(size, size_);
        if (copy_size > 0 && data_ != NULL) {
          memcpy(reinterpret_cast<void*>(new_data),
                 reinterpret_cast<void*>(data_), copy_size * sizeof(T));
        }

        reset();

        data_ = new_data;
        size_ = size;
        mmap_size_ = new_mmap_size;
      }
    }
  }

  void touch(const std::string& filename) {
    dump(filename);
    open(filename, true);
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
    std::swap(mmap_size_, rhs.mmap_size_);
    std::swap(hugepage_prefered_, rhs.hugepage_prefered_);
  }

  const std::string& filename() const { return filename_; }

 private:
  std::string filename_;
  int fd_;
  T* data_;
  size_t size_;

  size_t mmap_size_;

  bool sync_to_file_;
  bool hugepage_prefered_;
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

  void set_hugepage_prefered(bool val) {
    items_.set_hugepage_prefered(val);
    data_.set_hugepage_prefered(val);
  }

  void open(const std::string& filename, bool sync_to_file) {
    items_.open(filename + ".items", sync_to_file);
    data_.open(filename + ".data", sync_to_file);
  }

  void open_with_hugepages(const std::string& filename) {
    items_.open_with_hugepages(filename + ".items");
    data_.open_with_hugepages(filename + ".data");
  }

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
