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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_BASIC_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_BASIC_FRAGMENT_LOADER_H_

#include "flex/storages/rt_mutable_graph/file_names.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/schema.h"

namespace gs {

template <typename EDATA_T>
TypedMutableCsrBase<EDATA_T>* create_typed_csr(EdgeStrategy es,
                                               PropertyType edge_property) {
  if (es == EdgeStrategy::kSingle) {
    return new SingleMutableCsr<EDATA_T>(edge_property);
  } else if (es == EdgeStrategy::kMultiple) {
    return new MutableCsr<EDATA_T>(edge_property);
  } else if (es == EdgeStrategy::kNone) {
    return new EmptyCsr<EDATA_T>();
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
  return nullptr;
}

// FragmentLoader should use this BasicFragmentLoader to construct
// mutable_csr_fragment.
class BasicFragmentLoader {
 public:
  BasicFragmentLoader(const Schema& schema, const std::string& prefix);

  void LoadFragment();

  // props vector is column_num X batch_size
  void AddVertexBatch(label_t v_label, const std::vector<vid_t>& vids,
                      const std::vector<std::vector<Any>>& props);

  inline void SetVertexProperty(label_t v_label, size_t col_ind, vid_t vid,
                                Any&& prop) {
    auto& table = vertex_data_[v_label];
    auto dst_columns = table.column_ptrs();
    CHECK(col_ind < dst_columns.size());
    dst_columns[col_ind]->set_any(vid, prop);
  }
#ifndef USE_PTHASH
  template <typename KEY_T>
  void FinishAddingVertex(label_t v_label,
                          const IdIndexer<KEY_T, vid_t>& indexer) {
    CHECK(v_label < vertex_label_num_);
    std::string filename =
        vertex_map_prefix(schema_.get_vertex_label_name(v_label));
    auto primary_keys = schema_.get_vertex_primary_key(v_label);
    auto type = std::get<0>(primary_keys[0]);

    build_lf_indexer<KEY_T, vid_t>(
        indexer, LFIndexer<vid_t>::prefix() + "_" + filename,
        lf_indexers_[v_label], snapshot_dir(work_dir_, 0), tmp_dir(work_dir_),
        type);
  }
#else
  template <typename KEY_T>
  void FinishAddingVertex(label_t v_label,
                          PTIndexerBuilder<KEY_T, vid_t>& indexer_builder) {
    CHECK(v_label < vertex_label_num_);
    std::string filename =
        vertex_map_prefix(schema_.get_vertex_label_name(v_label));
    indexer_builder.finish(PTIndexer<vid_t>::prefix() + "_" + filename,
                           snapshot_dir(work_dir_, 0), lf_indexers_[v_label]);
  }
#endif

  template <typename EDATA_T>
  void AddNoPropEdgeBatch(label_t src_label_id, label_t dst_label_id,
                          label_t edge_label_id) {
    size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                   dst_label_id * edge_label_num_ + edge_label_id;
    CHECK(ie_[index] == NULL);
    CHECK(oe_[index] == NULL);
    auto src_label_name = schema_.get_vertex_label_name(src_label_id);
    auto dst_label_name = schema_.get_vertex_label_name(dst_label_id);
    auto edge_label_name = schema_.get_edge_label_name(edge_label_id);
    EdgeStrategy oe_strategy = schema_.get_outgoing_edge_strategy(
        src_label_name, dst_label_name, edge_label_name);
    EdgeStrategy ie_strategy = schema_.get_incoming_edge_strategy(
        src_label_name, dst_label_name, edge_label_name);
    if constexpr (std::is_same_v<EDATA_T, std::string_view>) {
      const auto& prop = schema_.get_edge_properties(src_label_id, dst_label_id,
                                                     edge_label_id);
      dual_csr_list_[index] = new DualCsr<std::string_view>(
          oe_strategy, ie_strategy, prop[0].additional_type_info.max_length);
    } else {
      bool oe_mutable = schema_.outgoing_edge_mutable(
          src_label_name, dst_label_name, edge_label_name);
      bool ie_mutable = schema_.incoming_edge_mutable(
          src_label_name, dst_label_name, edge_label_name);
      dual_csr_list_[index] = new DualCsr<EDATA_T>(oe_strategy, ie_strategy,
                                                   oe_mutable, ie_mutable);
    }
    ie_[index] = dual_csr_list_[index]->GetInCsr();
    oe_[index] = dual_csr_list_[index]->GetOutCsr();
    dual_csr_list_[index]->BatchInit(
        oe_prefix(src_label_name, dst_label_name, edge_label_name),
        ie_prefix(src_label_name, dst_label_name, edge_label_name),
        edata_prefix(src_label_name, dst_label_name, edge_label_name),
        tmp_dir(work_dir_), {}, {});
  }

  template <typename EDATA_T>
  void PutEdges(label_t src_label_id, label_t dst_label_id,
                label_t edge_label_id,
                const std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& edges,
                const std::vector<int32_t>& ie_degree,
                const std::vector<int32_t>& oe_degree) {
    size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                   dst_label_id * edge_label_num_ + edge_label_id;
    auto& src_indexer = lf_indexers_[src_label_id];
    auto& dst_indexer = lf_indexers_[dst_label_id];
    CHECK(ie_[index] == NULL);
    CHECK(oe_[index] == NULL);
    auto src_label_name = schema_.get_vertex_label_name(src_label_id);
    auto dst_label_name = schema_.get_vertex_label_name(dst_label_id);
    auto edge_label_name = schema_.get_edge_label_name(edge_label_id);
    EdgeStrategy oe_strategy = schema_.get_outgoing_edge_strategy(
        src_label_name, dst_label_name, edge_label_name);
    EdgeStrategy ie_strategy = schema_.get_incoming_edge_strategy(
        src_label_name, dst_label_name, edge_label_name);
    if constexpr (std::is_same_v<EDATA_T, std::string_view>) {
      const auto& prop = schema_.get_edge_properties(src_label_id, dst_label_id,
                                                     edge_label_id);
      auto dual_csr = new DualCsr<std::string_view>(
          oe_strategy, ie_strategy, prop[0].additional_type_info.max_length);
      dual_csr_list_[index] = dual_csr;
      ie_[index] = dual_csr_list_[index]->GetInCsr();
      oe_[index] = dual_csr_list_[index]->GetOutCsr();
      CHECK(ie_degree.size() == dst_indexer.size());
      CHECK(oe_degree.size() == src_indexer.size());
      dual_csr->BatchInit(
          oe_prefix(src_label_name, dst_label_name, edge_label_name),
          ie_prefix(src_label_name, dst_label_name, edge_label_name),
          edata_prefix(src_label_name, dst_label_name, edge_label_name),
          tmp_dir(work_dir_), oe_degree, ie_degree);
      for (auto& edge : edges) {
        dual_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                               std::get<2>(edge));
      }

    } else {
      bool oe_mutable = schema_.outgoing_edge_mutable(
          src_label_name, dst_label_name, edge_label_name);
      bool ie_mutable = schema_.incoming_edge_mutable(
          src_label_name, dst_label_name, edge_label_name);

      auto dual_csr = new DualCsr<EDATA_T>(oe_strategy, ie_strategy, oe_mutable,
                                           ie_mutable);

      dual_csr_list_[index] = dual_csr;
      ie_[index] = dual_csr_list_[index]->GetInCsr();
      oe_[index] = dual_csr_list_[index]->GetOutCsr();
      CHECK(ie_degree.size() == dst_indexer.size());
      CHECK(oe_degree.size() == src_indexer.size());
      dual_csr->BatchInit(
          oe_prefix(src_label_name, dst_label_name, edge_label_name),
          ie_prefix(src_label_name, dst_label_name, edge_label_name),
          edata_prefix(src_label_name, dst_label_name, edge_label_name),
          tmp_dir(work_dir_), oe_degree, ie_degree);
      for (auto& edge : edges) {
        dual_csr->BatchPutEdge(std::get<0>(edge), std::get<1>(edge),
                               std::get<2>(edge));
      }
    }
    VLOG(10) << "Finish adding edge batch of size: " << edges.size();
  }

  Table& GetVertexTable(size_t ind) {
    CHECK(ind < vertex_data_.size());
    return vertex_data_[ind];
  }

  // get lf_indexer
  const IndexerType& GetLFIndexer(label_t v_label) const;
  IndexerType& GetLFIndexer(label_t v_label);

 private:
  void init_vertex_data();
  const Schema& schema_;
  std::string work_dir_;
  size_t vertex_label_num_, edge_label_num_;
  std::vector<IndexerType> lf_indexers_;
  std::vector<CsrBase*> ie_, oe_;
  std::vector<DualCsrBase*> dual_csr_list_;
  std::vector<Table> vertex_data_;
};
}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_BASIC_FRAGMENT_LOADER_H_