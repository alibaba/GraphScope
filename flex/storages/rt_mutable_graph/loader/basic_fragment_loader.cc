
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

#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"

namespace gs {

BasicFragmentLoader::BasicFragmentLoader(const Schema& schema)
    : schema_(schema),
      vertex_label_num_(schema_.vertex_label_num()),
      edge_label_num_(schema_.edge_label_num()) {
  vertex_data_.resize(vertex_label_num_);
  ie_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  oe_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  lf_indexers_.resize(vertex_label_num_);

  init_vertex_data();
}

void BasicFragmentLoader::init_vertex_data() {
  for (label_t v_label = 0; v_label < vertex_label_num_; v_label++) {
    auto& v_data = vertex_data_[v_label];
    auto label_name = schema_.get_vertex_label_name(v_label);
    auto& property_types = schema_.get_vertex_properties(v_label);
    auto& property_names = schema_.get_vertex_property_names(v_label);
    v_data.init(property_names, property_types,
                schema_.get_vertex_storage_strategies(label_name),
                schema_.get_max_vnum(label_name));
  }
  VLOG(10) << "Finish init vertex data";
}

void BasicFragmentLoader::LoadFragment(MutablePropertyFragment& res_fragment) {
  CHECK(res_fragment.ie_.empty()) << "Fragment is not empty";
  CHECK(res_fragment.oe_.empty()) << "Fragment is not empty";
  CHECK(res_fragment.vertex_data_.empty()) << "Fragment is not empty";

  res_fragment.schema_ = schema_;
  res_fragment.vertex_label_num_ = vertex_label_num_;
  res_fragment.edge_label_num_ = edge_label_num_;
  res_fragment.ie_.swap(ie_);
  res_fragment.oe_.swap(oe_);
  res_fragment.vertex_data_.swap(vertex_data_);
  res_fragment.lf_indexers_.swap(lf_indexers_);
  VLOG(10) << "Finish Building Fragment, " << res_fragment.vertex_label_num_
           << " vertices labels, " << res_fragment.edge_label_num_
           << " edges labels";
}

void BasicFragmentLoader::AddVertexBatch(
    label_t v_label, const std::vector<vid_t>& vids,
    const std::vector<std::vector<Any>>& props) {
  auto& table = vertex_data_[v_label];
  CHECK(props.size() == table.col_num());
  for (auto i = 0; i < props.size(); ++i) {
    CHECK(props[i].size() == vids.size())
        << "vids size: " << vids.size() << ", props size: " << props.size()
        << ", props[i] size: " << props[i].size();
  }
  auto dst_columns = table.column_ptrs();
  for (auto j = 0; j < props.size(); ++j) {
    auto& cur_vec = props[j];
    for (auto i = 0; i < vids.size(); ++i) {
      auto index = vids[i];
      dst_columns[j]->set_any(index, cur_vec[i]);
    }
  }
}

const LFIndexer<vid_t>& BasicFragmentLoader::GetLFIndexer(
    label_t v_label) const {
  CHECK(v_label < vertex_label_num_);
  return lf_indexers_[v_label];
}

}  // namespace gs
