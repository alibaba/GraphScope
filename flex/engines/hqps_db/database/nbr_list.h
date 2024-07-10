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

#ifndef ENGINES_HQPS_DATABASE_NBR_LIST_H_
#define ENGINES_HQPS_DATABASE_NBR_LIST_H_

#include <vector>
#include "flex/utils/property/types.h"

namespace gs {
namespace mutable_csr_graph_impl {
class Nbr {
 public:
  Nbr() = default;
  explicit Nbr(vid_t neighbor) : neighbor_(neighbor) {}
  ~Nbr() = default;

  inline vid_t neighbor() const { return neighbor_; }

 private:
  vid_t neighbor_;
};

class NbrList {
 public:
  NbrList(const Nbr* b, const Nbr* e) : begin_(b), end_(e) {}
  NbrList() : begin_(nullptr), end_(nullptr) {}
  ~NbrList() = default;

  const Nbr* begin() const { return begin_; }
  const Nbr* end() const { return end_; }
  inline size_t size() const { return end_ - begin_; }

 private:
  const Nbr* begin_;
  const Nbr* end_;
};

class NbrListArray {
 public:
  NbrListArray() {}
  ~NbrListArray() = default;

  NbrList get(size_t index) const {
    auto& list = nbr_lists_[index];
    return NbrList(list.data(), list.data() + list.size());
  }

  void put(std::vector<Nbr>&& list) { nbr_lists_.push_back(std::move(list)); }

  size_t size() const { return nbr_lists_.size(); }

  void resize(size_t size) { nbr_lists_.resize(size); }

  std::vector<Nbr>& get_vector(size_t index) { return nbr_lists_[index]; }

 private:
  std::vector<std::vector<Nbr>> nbr_lists_;
};

NbrListArray create_nbr_list_array(const CsrBase* csr0, const CsrBase* csr1,
                                   const std::vector<vid_t>& vids) {
  NbrListArray ret;
  ret.resize(vids.size());
  for (size_t i = 0; i < vids.size(); ++i) {
    auto v = vids[i];
    auto& vec = ret.get_vector(i);
    if (csr0) {
      auto iter = csr0->edge_iter(v);
      while (iter->is_valid()) {
        vec.emplace_back(Nbr(iter->get_neighbor()));
        iter->next();
      }
    }
    if (csr1) {
      auto iter = csr1->edge_iter(v);
      while (iter->is_valid()) {
        vec.emplace_back(Nbr(iter->get_neighbor()));
        iter->next();
      }
    }
  }
  return ret;
}

NbrListArray create_nbr_list_array(const CsrBase* csr,
                                   const std::vector<vid_t>& vids) {
  return create_nbr_list_array(csr, nullptr, vids);
}
}  // namespace mutable_csr_graph_impl

}  // namespace gs

#endif  // ENGINES_HQPS_DATABASE_NBR_LIST_H_