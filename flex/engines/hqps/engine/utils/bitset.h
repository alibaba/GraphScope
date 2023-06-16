/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPHSCOPE_UTILS_BITSET_H_
#define GRAPHSCOPE_UTILS_BITSET_H_

#include <stdlib.h>
#include <cassert>

#include <algorithm>
#include <cstring>
#include <utility>
#include <cstring>
#include <string>

#include "glog/logging.h"

#define WORD_SIZE(n) (((n) + 63ul) >> 6)

#define WORD_INDEX(i) ((i) >> 6)
#define BIT_OFFSET(i) ((i) &0x3f)

#define ROUND_UP(i) (((i) + 63ul) & (~63ul))
#define ROUND_DOWN(i) ((i) & (~63ul))

namespace gs {

/**
 * @brief Bitset is a highly-optimized bitset implementation.
 */
class Bitset {
 public:
  Bitset() : data_(NULL), size_(0), size_in_words_(0) {}

  Bitset(Bitset&& other)
      : data_(other.data_),
        size_(other.size_),
        size_in_words_(other.size_in_words_) {
    other.data_ = NULL;
    other.size_ = 0;
    other.size_in_words_ = 0;
  }

  Bitset(const Bitset& other) = delete;

  explicit Bitset(size_t size) : size_(size) {
    size_in_words_ = WORD_SIZE(size_);
    data_ = static_cast<uint64_t*>(malloc(size_in_words_ * sizeof(uint64_t)));
    clear();
  }
  ~Bitset() {
    if (data_ != NULL) {
      free(data_);
    } else {
    }
  }

  Bitset Clone() const {
    Bitset ret;
    ret.init(size_);
    ret.size_in_words_ = size_in_words_;
    for (auto i = 0; i < size_in_words_; ++i) {
      ret.data_[i] = data_[i];
    }
    return ret;
  }

  void init(size_t size) {
    if (data_ != NULL) {
      free(data_);
    }
    size_ = size;
    size_in_words_ = WORD_SIZE(size_);
    data_ = static_cast<uint64_t*>(malloc(size_in_words_ * sizeof(uint64_t)));
    clear();
  }

  void clear() {
    for (size_t i = 0; i < size_in_words_; ++i) {
      data_[i] = 0;
    }
  }

  size_t size() const { return size_; }

  void resize(size_t size) {
    // VLOG(10) << "resize to : " << size;
    if (data_ == NULL) {
      init(size);
      return;
    }
    size_t new_size_in_words = WORD_SIZE(size);
    if (size_in_words_ != new_size_in_words) {
      uint64_t* new_data =
          static_cast<uint64_t*>(malloc(new_size_in_words * sizeof(uint64_t)));
      if (size_in_words_ > new_size_in_words) {
        // VLOG(10) << "old size in word: " << size_in_words_
        //  << ", new size in word: " << new_size_in_words;
        for (size_t i = 0; i < new_size_in_words; ++i) {
          new_data[i] = data_[i];
        }
        // VLOG(10) << "bit offset: " << BIT_OFFSET(size);
        // __sync_fetch_and_and(new_data + new_size_in_words - 1,
        //  63ul << BIT_OFFSET(size));
      } else if (size_in_words_ < new_size_in_words) {
        for (size_t i = 0; i < size_in_words_; ++i) {
          new_data[i] = data_[i];
        }
        for (size_t i = size_in_words_; i < new_size_in_words; ++i) {
          new_data[i] = 0;
        }
      }
      free(data_);
      data_ = new_data;
    } else {
      // num words same, but size different.
      if (size_ > size) {
        // need to set old to zero.
        for (auto i = size; i < size_; ++i) {
          set_bit(i);
        }
      } else if (size_ < size) {
        for (auto i = size; i < size_; ++i) {
          reset_bit(i);
        }
      }
    }
    size_ = size;
    size_in_words_ = new_size_in_words;
  }

  void copy(const Bitset& other) {
    assert(this != &other);
    if (data_ != NULL) {
      LOG(INFO) << "before free: " << data_;
      free(data_);
      LOG(INFO) << "after free:" << data_;
    }
    size_ = other.size_;
    size_in_words_ = other.size_in_words_;
    if (other.data_ != NULL) {
      data_ = static_cast<uint64_t*>(malloc(size_in_words_ * sizeof(uint64_t)));
      memcpy(data_, other.data_, size_in_words_ * sizeof(uint64_t));
    } else {
      data_ = NULL;
    }
  }

  // void parallel_clear(ThreadPool& thread_pool) {
  //   uint32_t thread_num = thread_pool.GetThreadNum();
  //   size_t chunk_size =
  //       std::max(1024ul, (size_in_words_ + thread_num - 1) / thread_num);
  //   size_t thread_begin = 0, thread_end = std::min(chunk_size,
  //   size_in_words_); std::vector<std::future<void>> results(thread_num); for
  //   (uint32_t tid = 0; tid < thread_num; ++tid) {
  //     results[tid] = thread_pool.enqueue([thread_begin, thread_end, this]() {
  //       for (size_t i = thread_begin; i < thread_end; ++i)
  //         data_[i] = 0;
  //     });
  //     thread_begin = thread_end;
  //     thread_end = std::min(thread_end + chunk_size, size_in_words_);
  //   }
  //   thread_pool.WaitEnd(results);
  // }

  bool empty() const {
    for (size_t i = 0; i < size_in_words_; ++i) {
      if (data_[i]) {
        return false;
      }
    }
    return true;
  }

  bool partial_empty(size_t begin, size_t end) const {
    end = std::min(end, size_);
    size_t cont_beg = ROUND_UP(begin);
    size_t cont_end = ROUND_DOWN(end);
    size_t word_beg = WORD_INDEX(cont_beg);
    size_t word_end = WORD_INDEX(cont_end);
    for (size_t i = word_beg; i < word_end; ++i) {
      if (data_[i]) {
        return false;
      }
    }
    if (cont_beg != begin) {
      uint64_t first_word = data_[WORD_INDEX(begin)];
      first_word = (first_word >> (64 - (cont_beg - begin)));
      if (first_word) {
        return false;
      }
    }
    if (cont_end != end) {
      uint64_t last_word = data_[WORD_INDEX(end)];
      last_word = (last_word & ((1ul << (end - cont_end)) - 1));
      if (last_word) {
        return false;
      }
    }
    return true;
  }

  inline bool get_bit(size_t i) const {
    // CHECK(i < size_) << " out of range: " << i << ", limit: " << size_;
    return data_[WORD_INDEX(i)] & (1ul << BIT_OFFSET(i));
  }

  inline void set_bit(size_t i) {
    // CHECK(i < size_) << "out of range: " << i << ", limit: " << size_;
    if (i >= size_) {
      LOG(INFO) << "resize bitset from " << size_;
      resize(i * 2);
      LOG(INFO) << "to " << size_;
    }
    //__sync_fetch_and_or(data_ + WORD_INDEX(i), 1ul << BIT_OFFSET(i));
    auto cur_ptr = data_ + WORD_INDEX(i);
    *cur_ptr |= (1ul << BIT_OFFSET(i));
  }

  bool set_bit_with_ret(size_t i) {
    uint64_t mask = 1ul << BIT_OFFSET(i);
    uint64_t ret = __sync_fetch_and_or(data_ + WORD_INDEX(i), mask);
    return !(ret & mask);
  }

  void reset_bit(size_t i) {
    __sync_fetch_and_and(data_ + WORD_INDEX(i), ~(1ul << BIT_OFFSET(i)));
  }

  bool reset_bit_with_ret(size_t i) {
    uint64_t mask = 1ul << BIT_OFFSET(i);
    uint64_t ret = __sync_fetch_and_and(data_ + WORD_INDEX(i), ~mask);
    return (ret & mask);
  }

  void swap(Bitset& other) {
    std::swap(size_, other.size_);
    std::swap(data_, other.data_);
    std::swap(size_in_words_, other.size_in_words_);
  }

  size_t count() const {
    size_t ret = 0;
    for (size_t i = 0; i < size_in_words_; ++i) {
      ret += __builtin_popcountll(data_[i]);
    }
    return ret;
  }

  // size_t parallel_count(ThreadPool& thread_pool) const {
  //   size_t ret = 0;
  //   uint32_t thread_num = thread_pool.GetThreadNum();
  //   size_t chunk_size =
  //       std::max(1024ul, (size_in_words_ + thread_num - 1) / thread_num);
  //   size_t thread_start = 0, thread_end = std::min(chunk_size,
  //   size_in_words_); std::vector<std::future<void>> results(thread_num); for
  //   (uint32_t tid = 0; tid < thread_num; ++tid) {
  //     results[tid] =
  //         thread_pool.enqueue([thread_start, thread_end, this, &ret]() {
  //           size_t ret_t = 0;
  //           for (size_t i = thread_start; i < thread_end; ++i)
  //             ret_t += __builtin_popcountll(data_[i]);
  //           __sync_fetch_and_add(&ret, ret_t);
  //         });
  //     thread_start = thread_end;
  //     thread_end = std::min(thread_end + chunk_size, size_in_words_);
  //   }
  //   thread_pool.WaitEnd(results);
  //   return ret;
  // }

  size_t partial_count(size_t begin, size_t end) const {
    size_t ret = 0;
    size_t cont_beg = ROUND_UP(begin);
    size_t cont_end = ROUND_DOWN(end);
    size_t word_beg = WORD_INDEX(cont_beg);
    size_t word_end = WORD_INDEX(cont_end);
    for (size_t i = word_beg; i < word_end; ++i) {
      ret += __builtin_popcountll(data_[i]);
    }
    if (cont_beg != begin) {
      uint64_t first_word = data_[WORD_INDEX(begin)];
      first_word = (first_word >> (64 - (cont_beg - begin)));
      ret += __builtin_popcountll(first_word);
    }
    if (cont_end != end) {
      uint64_t last_word = data_[WORD_INDEX(end)];
      last_word = (last_word & ((1ul << (end - cont_end)) - 1));
      ret += __builtin_popcountll(last_word);
    }
    return ret;
  }

  // size_t parallel_partial_count(ThreadPool& thread_pool, size_t begin,
  //                               size_t end) const {
  //   size_t ret = 0;
  //   size_t cont_beg = ROUND_UP(begin);
  //   size_t cont_end = ROUND_DOWN(end);
  //   size_t word_beg = WORD_INDEX(cont_beg);
  //   size_t word_end = WORD_INDEX(cont_end);
  //   uint32_t thread_num = thread_pool.GetThreadNum();
  //   size_t chunk_size =
  //       std::max(1024ul, (word_end - word_beg + thread_num - 1) /
  //       thread_num);
  //   size_t thread_begin = word_beg,
  //          thread_end = std::min(word_beg + chunk_size, word_end);
  //   std::vector<std::future<void>> results(thread_num);
  //   for (uint32_t tid = 0; tid < thread_num; ++tid) {
  //     results[tid] =
  //         thread_pool.enqueue([thread_begin, thread_end, this, &ret]() {
  //           size_t ret_t = 0;
  //           for (size_t i = thread_begin; i < thread_end; ++i)
  //             ret_t += __builtin_popcountll(data_[i]);
  //           __sync_fetch_and_add(&ret, ret_t);
  //         });
  //     thread_begin = thread_end;
  //     thread_end = std::min(thread_end + chunk_size, word_end);
  //   }
  //   thread_pool.WaitEnd(results);
  //   if (cont_beg != begin) {
  //     uint64_t first_word = data_[WORD_INDEX(begin)];
  //     first_word = (first_word >> (64 - (cont_beg - begin)));
  //     ret += __builtin_popcountll(first_word);
  //   }
  //   if (cont_end != end) {
  //     uint64_t last_word = data_[WORD_INDEX(end)];
  //     last_word = (last_word & ((1ul << (end - cont_end)) - 1));
  //     ret += __builtin_popcountll(last_word);
  //   }
  //   return ret;
  // }

  inline uint64_t get_word(size_t i) const { return data_[WORD_INDEX(i)]; }

  inline const uint64_t* get_word_ptr(size_t i) const {
    return &data_[WORD_INDEX(i)];
  }

 private:
  uint64_t* data_;
  size_t size_;
  size_t size_in_words_;
};

class RefBitset {
 public:
  RefBitset() : data(NULL) {}
  RefBitset(void* d, size_t b, size_t e) {
    data = static_cast<uint64_t*>(d);
    begin = b / 64 * 64;
    end = (e + 63) / 64 * 64;

    data[0] &= (~((1ul << (b - begin)) - 1ul));
    data[((end - begin) / 64) - 1] &= ((1ul << (64 - (end - e))) - 1ul);
  }
  ~RefBitset() {}

  bool get_bit(size_t loc) const {
    return data[WORD_INDEX(loc - begin)] & (1ul << BIT_OFFSET(loc));
  }

  uint64_t get_word_by_index(size_t index) { return data[index]; }

  size_t get_word_num() const { return (end - begin) / 64; }

  uint64_t* data;
  size_t begin;
  size_t end;
};

#undef WORD_SIZE
#undef WORD_INDEX
#undef BIT_OFFSET
#undef ROUND_UP
#undef ROUND_DOWN

}  // namespace gs

#endif  // GRAPHSCOPE_UTILS_BITSET_H_
