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

#ifndef GRAPHSCOPE_PTHASH_UTILS_SINGLE_PHF_VIEW_H_
#define GRAPHSCOPE_PTHASH_UTILS_SINGLE_PHF_VIEW_H_

#ifdef USE_PTHASH

#include "flex/utils/mmap_array.h"
#include "flex/utils/pthash_utils/encoders_view.h"
#include "utils/bucketers.hpp"
#include "utils/hasher.hpp"

namespace gs {

struct default_loader {
  default_loader(const char* buffer) : buffer_(buffer) {}
  ~default_loader() = default;

  template <typename T>
  void visit(T& val) {
    if constexpr (std::is_pod<T>::value) {
      memcpy(&val, buffer_, sizeof(T));
      buffer_ += sizeof(T);
    } else {
      val.visit(*this);
    }
  }

  template <typename T>
  void visit_vec(ref_vector<T>& vec) {
    size_t size;
    visit(size);
    vec.init(reinterpret_cast<const T*>(buffer_), size);
    buffer_ += sizeof(T) * size;
  }

  const char* buffer() const { return buffer_; }

 private:
  const char* buffer_;
};

// This code is an adaptation from
// https://github.com/jermp/pthash/blob/master/include/single_phf.hpp
template <typename Hasher>
struct SinglePHFView {
 public:
  SinglePHFView() = default;
  ~SinglePHFView() = default;

  SinglePHFView(const SinglePHFView& rhs) : buffer_(rhs.buffer_) {
    default_loader loader(buffer_.data());
    loader.visit(m_seed);
    loader.visit(m_num_keys);
    loader.visit(m_table_size);
    loader.visit(m_M);
    loader.visit(m_bucketer);
    loader.visit(m_pilots);
    loader.visit(m_free_slots);
  }

  void Open(const std::string& filename) {
    buffer_.open(filename);
    default_loader loader(buffer_.data());
    loader.visit(m_seed);
    loader.visit(m_num_keys);
    loader.visit(m_table_size);
    loader.visit(m_M);
    loader.visit(m_bucketer);
    loader.visit(m_pilots);
    loader.visit(m_free_slots);
  }

  void Init(const std::vector<char>& buffer) {
    buffer_.resize(buffer.size());
    memcpy(buffer_.data(), buffer.data(), buffer.size());
    default_loader loader(buffer_.data());
    loader.visit(m_seed);
    loader.visit(m_num_keys);
    loader.visit(m_table_size);
    loader.visit(m_M);
    loader.visit(m_bucketer);
    loader.visit(m_pilots);
    loader.visit(m_free_slots);
  }

  void Save(const std::string& filename) { buffer_.dump(filename); }

  template <typename T>
  uint64_t operator()(T const& key) const {
    auto hash = Hasher::hash(key, m_seed);
    return position(hash);
  }

  uint64_t position(typename Hasher::hash_type hash) const {
    uint64_t bucket = m_bucketer.bucket(hash.first());
    uint64_t pilot = m_pilots.access(bucket);
    uint64_t hashed_pilot = pthash::default_hash64(pilot, m_seed);
    uint64_t p =
        fastmod::fastmod_u64(hash.second() ^ hashed_pilot, m_M, m_table_size);
    if (PTHASH_LIKELY(p < m_num_keys))
      return p;
    return m_free_slots.access(p - m_num_keys);
  }

 private:
  uint64_t m_seed;
  uint64_t m_num_keys;
  uint64_t m_table_size;
  __uint128_t m_M;
  pthash::skew_bucketer m_bucketer;
  dual_dictionary_view m_pilots;
  ef_sequence_view m_free_slots;
  mmap_array<char> buffer_;
};

}  // namespace gs

#endif  // USE_PTHASH

#endif  // GRAPHSCOPE_PTHASH_UTILS_SINGLE_PHF_VIEW_H_