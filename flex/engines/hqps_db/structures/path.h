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
template <typename VID_T, typename LabelT>
struct Path {
  using vid_t = VID_T;
  using label_id_t = LabelT;
  std::vector<VID_T> vids_;
  std::vector<label_id_t> label_ids_;

  Path(const Path<VID_T, LabelT>& other)
      : vids_(other.vids_), label_ids_(other.label_ids_) {}

  Path(vid_t vid, label_id_t label_id) {
    vids_.emplace_back(vid);
    label_ids_.emplace_back(label_id);
  }

  Path(std::vector<VID_T>&& vids, std::vector<label_id_t>&& label_ids)
      : vids_(std::move(vids)), label_ids_(std::move(label_ids)) {}

  inline int32_t length() const {
    if (vids_.size() > 0) {
      return vids_.size() - 1;
    }
    return 0;
  }

  void EmplaceBack(VID_T vid, label_id_t label_id) {
    vids_.emplace_back(vid);
    label_ids_.emplace_back(label_id);
  }

  inline const std::vector<VID_T>& GetVertices() const { return vids_; }

  inline VID_T GetEnd() const {
    CHECK(vids_.size() > 0);
    return vids_.back();
  }
  inline VID_T GetStart() const {
    CHECK(vids_.size() > 0);
    return vids_.front();
  }

  std::string to_string() const {
    std::stringstream ss;
    for (size_t i = 0; i < vids_.size(); ++i) {
      ss << vids_[i];
      if (i + 1 < vids_.size()) {
        ss << "->";
      }
    }
    return ss.str();
  }

  bool operator==(const Path& rhs) const {
    if (vids_.size() != rhs.vids_.size()) {
      return false;
    }
    for (size_t i = 0; i < vids_.size(); ++i) {
      if (vids_[i] != rhs.vids_[i]) {
        return false;
      }
    }
    return true;
  }
};

template <typename VID_T, typename LabelT>
class PathSetIter {
 public:
  using self_type_t = PathSetIter<VID_T, LabelT>;
  using index_ele_tuple_t = std::pair<size_t, Path<VID_T, LabelT>>;

  PathSetIter(const std::vector<Path<VID_T, LabelT>>& paths, size_t ind)
      : paths_(paths), ind_(ind) {}

  inline Path<VID_T, LabelT> GetElement() const { return paths_[ind_]; }

  inline Path<VID_T, LabelT> GetData() const { return paths_[ind_]; }

  inline index_ele_tuple_t GetIndexElement() const {
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
  const std::vector<Path<VID_T, LabelT>>& paths_;
  size_t ind_;
};

template <typename VID_T, typename LabelT>
class PathSet {
 public:
  using flat_t = PathSet<VID_T, LabelT>;
  using self_type_t = PathSet<VID_T, LabelT>;
  using iterator = PathSetIter<VID_T, LabelT>;
  using data_tuple_t = Path<VID_T, LabelT>;
  using index_ele_tuple_t = std::pair<size_t, Path<VID_T, LabelT>>;

  PathSet(std::vector<Path<VID_T, LabelT>>&& paths)
      : paths_(std::move(paths)) {}

  void EmplacePath(Path<VID_T, LabelT>&& path) {
    paths_.emplace_back(std::move(path));
  }

  const Path<VID_T, LabelT>& get(size_t i) const {
    CHECK(i < paths_.size());
    return paths_[i];
  }

  size_t Size() const { return paths_.size(); }

  iterator begin() const { return iterator(paths_, 0); }

  iterator end() const { return iterator(paths_, paths_.size()); }

 private:
  std::vector<Path<VID_T, LabelT>> paths_;
};

template <typename VID_T, typename LabelT>
class CompressedPathSetIter {
 public:
  using self_type_t = CompressedPathSetIter<VID_T, LabelT>;
  using index_ele_tuple_t = std::pair<size_t, Path<VID_T, LabelT>>;

  CompressedPathSetIter(std::vector<Path<VID_T, LabelT>>&& paths, size_t ind)
      : paths_(std::move(paths)), ind_(ind) {}

  Path<VID_T, LabelT> GetElement() const { return paths_[ind_]; }

  Path<VID_T, LabelT> GetData() const { return paths_[ind_]; }

  index_ele_tuple_t GetIndexElement() const {
    return std::make_pair(ind_, paths_[ind_]);
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
  std::vector<Path<VID_T, LabelT>> paths_;
  size_t ind_;
};

template <typename VID_T, typename LabelT>
class CompressedPathSet {
 public:
  using vid_t = VID_T;
  using flat_t = PathSet<VID_T, LabelT>;
  using self_type_t = CompressedPathSet<VID_T, LabelT>;
  using iterator = CompressedPathSetIter<VID_T, LabelT>;
  using ele_tuple_t = Path<VID_T, LabelT>;
  using data_tuple_t = Path<VID_T, LabelT>;
  using index_ele_tuple_t = std::pair<size_t, Path<VID_T, LabelT>>;
  // Empty
  static constexpr bool is_path_set = true;

  CompressedPathSet() {}

  // We assume we have a dummy head, so min_len_ is 1.
  // store the path is a csr format.
  CompressedPathSet(std::vector<std::vector<vid_t>>&& vids,
                    std::vector<std::vector<offset_t>>&& offsets,
                    std::vector<LabelT>&& labels, size_t min_len)
      : vids_(std::move(vids)),
        offsets_(std::move(offsets)),
        labels_(std::move(labels)),
        min_len_(min_len) {
    CHECK(min_len_ < vids_.size() && min_len_ > 0);
    // vids.size() is the max length of path.
    // offset[i].back() is the number of paths with length i.
    CHECK(vids_.size() == offsets_.size())
        << "vids and offsets size not match" << vids_.size() << ", "
        << offsets_.size();
    CHECK(vids_.size() == labels_.size());
    for (size_t i = 0; i < vids_.size(); ++i) {
      CHECK(vids_[i].size() == offsets_[i].back());
    }
  }

  size_t Size() const {
    size_t res = 0;
    for (size_t i = min_len_; i < offsets_.size(); ++i) {
      res += offsets_[i].back();
    }
    return res;
  }

  iterator begin() const {
    auto paths = get_all_valid_paths();
    VLOG(10) << "got paths of size: " << paths.size();
    CHECK(paths.size() == Size());
    return iterator(std::move(paths), 0);
  }

  iterator end() const {
    // use a dummy PathSet.
    std::vector<Path<VID_T, LabelT>> paths;
    return iterator(std::move(paths), Size());
  }

  const std::vector<LabelT>& GetLabels() const { return labels_; }

  template <typename FILTER_T, typename PROP_GETTER_T>
  std::pair<DefaultRowVertexSet<LabelT, vid_t>, std::vector<offset_t>>
  GetVertices(VOpt vopt, const FILTER_T& expr,
              const std::vector<PROP_GETTER_T>& prop_getters) const {
    // get vertices from path set, current we only have one label is path, so we
    // don't have label params.
    std::vector<vid_t> vids;
    std::vector<offset_t> offsets;
    offsets.reserve(Size() + 1);
    offsets.emplace_back(0);
    CHECK(prop_getters.size() == 1) << "only support one label now";

    auto paths = get_all_valid_paths();
    if (vopt == VOpt::End) {
      for (auto& path : paths) {
        vids.emplace_back(path.GetEnd());
        offsets.emplace_back(vids.size());
      }
    } else if (vopt == VOpt::Start) {
      for (auto& path : paths) {
        vids.emplace_back(path.GetStart());
        offsets.emplace_back(vids.size());
      }
    } else {
      LOG(FATAL) << "Not supported vopt: " << gs::to_string(vopt);
    }

    auto row_set = make_default_row_vertex_set(std::move(vids), labels_[0]);
    return std::make_pair(row_set, std::move(offsets));
  }

  std::vector<Path<VID_T, LabelT>> get_all_valid_paths() const {
    std::vector<Path<VID_T, LabelT>> paths;
    std::vector<std::vector<Path<VID_T, LabelT>>> paths_by_len;
    auto path_len = vids_.size();
    VLOG(10) << "path len: " << path_len;
    for (size_t i = 0; i < path_len; ++i) {
      std::vector<Path<VID_T, LabelT>> cur_paths;
      if (i == 0) {
        for (size_t j = 0; j < vids_[i].size(); ++j) {
          cur_paths.emplace_back(Path<VID_T, LabelT>(vids_[i][j], labels_[i]));
        }
      } else {
        // expand path from last level.
        for (size_t j = 0; j < paths_by_len[i - 1].size(); ++j) {
          auto path = paths_by_len[i - 1][j];
          CHECK(offsets_[i].back() == vids_[i].size());
          for (auto k = offsets_[i][j]; k < offsets_[i][j + 1]; ++k) {
            auto cur_vid = vids_[i][k];
            // copy the path
            auto copied_path(path);
            copied_path.EmplaceBack(cur_vid, labels_[i]);
            cur_paths.emplace_back(copied_path);
          }
        }
      }
      VLOG(10) << "got valid paths size: " << cur_paths.size()
               << " for path len: " << i;
      paths_by_len.emplace_back(std::move(cur_paths));
    }

    std::vector<Path<VID_T, LabelT>> res;
    // rearrange the paths in right order.
    std::vector<std::vector<offset_t>> offset_amplify(
        vids_.size(), std::vector<offset_t>(offsets_[0].size(), 0));
    offset_amplify[0] = offsets_[0];
    for (size_t i = 1; i < offset_amplify.size(); ++i) {
      for (size_t j = 0; j < offset_amplify[i].size(); ++j) {
        offset_amplify[i][j] = offsets_[i][offset_amplify[i - 1][j]];
      }
    }
    VLOG(10) << "amplify: " << gs::to_string(offset_amplify);

    CHECK(vids_.size() > 0);
    for (size_t i = 0; i < vids_[0].size(); ++i) {
      // len - 1 is the key.
      for (size_t j = min_len_; j < paths_by_len.size(); ++j) {
        auto start_ind = offset_amplify[j][i];
        auto end_ind = offset_amplify[j][i + 1];
        for (size_t k = start_ind; k < end_ind; ++k) {
          res.emplace_back(paths_by_len[j][k]);
        }
      }
    }
    VLOG(10) << "Rearrange paths cost: " << res.size()
             << ", min_len: " << min_len_ << ", path_len: " << path_len;

    return res;
  }

 private:
  std::vector<std::vector<VID_T>> vids_;
  std::vector<std::vector<offset_t>> offsets_;
  std::vector<LabelT> labels_;
  size_t min_len_;
};

template <typename VID_T, typename LabelT>
auto make_empty_path_set(std::vector<LabelT>&& labels) {
  return PathSet<VID_T, LabelT>(std::move(labels));
}

}  // namespace gs

#endif  // GRAPHSCOPE_DS_PATH_H_
