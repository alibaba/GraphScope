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

#ifndef GRAPHSCOPE_DS_PATH_H_
#define GRAPHSCOPE_DS_PATH_H_

#include <memory>
#include <string>
#include <vector>

namespace gs {

// Path Set contains all vertices and edges along each path.
// Note that path doesn't have to be full length.
// i.e. :[1], [1,2], [1,2,3]

// Assumes all path share same label
template <typename VID_T>
struct Path {
  std::vector<VID_T> vids_;
  std::vector<int32_t> label_ids_;

  Path(std::vector<VID_T>&& vids, std::vector<int32_t>&& label_ids)
      : vids_(std::move(vids)), label_ids_(std::move(label_ids)) {}

  size_t length() const { return vids_.size() - 1; }

  const std::vector<VID_T>& GetVertices() const { return vids_; }

  std::string to_string() const {
    std::stringstream ss;
    for (auto i = 0; i < vids_.size() - 1; ++i) {
      ss << vids_[i] << "->";
    }
    ss << vids_[vids_.size() - 1];
    return ss.str();
  }
};

template <typename VID_T, typename LabelT>
class PathSetIter {
 public:
  using self_type_t = PathSetIter<VID_T, LabelT>;
  using index_ele_tuple_t = std::pair<size_t, const Path<VID_T>&>;

  PathSetIter(const std::vector<Path<VID_T>>& paths, size_t ind)
      : paths_(paths), ind_(ind) {}

  const Path<VID_T>& GetElement() const { return paths_[ind_]; }

  std::tuple<Path<VID_T>> GetData() const {
    return std::make_tuple(paths_[ind_]);
  }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_tuple(ind_, paths_[ind_]);
  }

  inline const self_type_t& operator++() {
    ++ind_;
    return *this;
  }

  // We may never compare to other kind of iterators
  inline bool operator==(const self_type_t& rhs) const {
    return ind_ == rhs.ind_;
  }

  inline bool operator!=(const self_type_t& rhs) const {
    return ind_ != rhs.ind_;
  }

  inline bool operator<(const self_type_t& rhs) const {
    return ind_ < rhs.ind_;
  }

  inline const self_type_t& operator*() const { return *this; }

  inline const self_type_t* operator->() const { return this; }

 private:
  const std::vector<Path<VID_T>>& paths_;
  size_t ind_;
};

template <typename VID_T, typename LabelT>
class PathSet {
 public:
  using flat_t = PathSet<VID_T, LabelT>;
  using self_type_t = PathSet<VID_T, LabelT>;
  using iterator = PathSetIter<VID_T, LabelT>;
  using data_tuple_t = std::tuple<Path<VID_T>>;
  using index_ele_tuple_t = std::pair<size_t, const Path<VID_T>&>;
  PathSet(std::vector<LabelT>&& labels) : labels_(std::move(labels)){};

  PathSet(std::vector<Path<VID_T>>&& paths, std::vector<LabelT>&& labels)
      : paths_(std::move(paths)), labels_(std::move(labels)) {}

  void EmplacePath(Path<VID_T>&& path) { paths_.emplace_back(std::move(path)); }

  const Path<VID_T>& get(size_t i) const {
    CHECK(i < paths_.size());
    return paths_[i];
  }

  size_t Size() const { return paths_.size(); }

  iterator begin() const { return iterator(paths_, 0); }

  iterator end() const { return iterator(paths_, paths_.size()); }

 private:
  std::vector<LabelT> labels_;
  std::vector<Path<VID_T>> paths_;
};

template <typename VID_T, typename LabelT>
auto make_empty_path_set(std::vector<LabelT>&& labels) {
  return PathSet<VID_T, LabelT>(std::move(labels));
}

}  // namespace gs

#endif  // GRAPHSCOPE_DS_PATH_H_
