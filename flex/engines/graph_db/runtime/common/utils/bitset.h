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

#ifndef RUNTIME_COMMON_UTILS_BITSET_H_
#define RUNTIME_COMMON_UTILS_BITSET_H_

#include "flex/engines/graph_db/runtime/common/utils/allocator.h"

#define WORD_SIZE(n) (((n) + 63ul) >> 6)
#define BYTE_SIZE(n) (((n) + 63ul) >> 3)

#define WORD_INDEX(i) ((i) >> 6)
#define BIT_OFFSET(i) ((i) &0x3f)

#define ROUND_UP(i) (((i) + 63ul) & (~63ul))
#define ROUND_DOWN(i) ((i) & (~63ul))

class Bitset : public SPAllocator<uint64_t> {
 public:
  Bitset()
      : data_(NULL),
        size_(0),
        size_in_words_(0),
        capacity_(0),
        capacity_in_words_(0) {}
  ~Bitset() {
    if (data_ != NULL) {
      this->deallocate(data_, capacity_in_words_);
    }
  }

  void reserve(size_t cap) {
    size_t new_cap_in_words = WORD_SIZE(cap);
    if (new_cap_in_words <= capacity_in_words_) {
      capacity_ = cap;
      return;
    }
    uint64_t* new_data = this->allocate(new_cap_in_words);
    if (data_ != NULL) {
      memcpy(new_data, data_, size_in_words_ * sizeof(uint64_t));
      this->deallocate(data_, capacity_in_words_);
    }
    data_ = new_data;
    capacity_ = cap;
    capacity_in_words_ = new_cap_in_words;
  }

  void clear() {
    size_ = 0;
    size_in_words_ = 0;
  }

  void reset_all() { memset(data_, 0, size_in_words_ * sizeof(uint64_t)); }

  void resize(size_t new_size) {
    if (new_size <= size_) {
      size_ = new_size;
      size_in_words_ = WORD_SIZE(size_);
      return;
    }
    reserve(new_size);

    size_t new_size_in_words = WORD_SIZE(new_size);
    memset(&data_[size_in_words_], 0,
           (new_size_in_words - size_in_words_) * sizeof(uint64_t));

    if (size_in_words_) {
      uint64_t mask = ((1ul << BIT_OFFSET(size_)) - 1);
      data_[size_in_words_ - 1] &= mask;
    }

    size_ = new_size;
    size_in_words_ = new_size_in_words;
  }

  void set(size_t i) { data_[WORD_INDEX(i)] |= (1ul << BIT_OFFSET(i)); }

  void reset(size_t i) { data_[WORD_INDEX(i)] &= (~(1ul << BIT_OFFSET(i))); }

  bool get(size_t i) const {
    return data_[WORD_INDEX(i)] & (1ul << BIT_OFFSET(i));
  }

 private:
  uint64_t* data_;
  size_t size_;
  size_t size_in_words_;

  size_t capacity_;
  size_t capacity_in_words_;
};

#undef WORD_SIZE
#undef BYTE_SIZE
#undef WORD_INDEX
#undef BIT_OFFSET
#undef ROUND_UP
#undef ROUND_DOWN

#endif  // RUNTIME_COMMON_UTILS_BITSET_H_
