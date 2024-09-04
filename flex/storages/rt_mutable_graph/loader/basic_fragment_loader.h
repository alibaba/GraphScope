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

enum class LoadingStatus {
  kLoading = 0,
  kLoaded = 1,
  kCommited = 2,
  kUnknown = 3,
};

// define << and >> for LoadingStatus
std::ostream& operator<<(std::ostream& os, const LoadingStatus& status);

std::istream& operator>>(std::istream& is, LoadingStatus& status);

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
    append_vertex_loading_progress(schema_.get_vertex_label_name(v_label),
                                   LoadingStatus::kLoaded);
    auto& v_data = vertex_data_[v_label];
    auto label_name = schema_.get_vertex_label_name(v_label);
    v_data.resize(lf_indexers_[v_label].size());
    v_data.dump(vertex_table_prefix(label_name), snapshot_dir(work_dir_, 0));
    append_vertex_loading_progress(label_name, LoadingStatus::kCommited);
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
    append_vertex_loading_progress(schema_.get_vertex_label_name(v_label),
                                   LoadingStatus::kLoaded);
    auto& v_data = vertex_data_[v_label];
    auto label_name = schema_.get_vertex_label_name(v_label);
    v_data.resize(lf_indexers_[v_label].size());
    v_data.dump(vertex_table_prefix(label_name), snapshot_dir(work_dir_, 0));
    append_vertex_loading_progress(label_name, LoadingStatus::kCommited);
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

      size_t max_length = PropertyType::STRING_DEFAULT_MAX_LENGTH;
      if (prop[0].IsVarchar()) {
        max_length = prop[0].additional_type_info.max_length;
      }
      dual_csr_list_[index] =
          new DualCsr<std::string_view>(oe_strategy, ie_strategy, max_length);
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
  static decltype(auto) get_casted_dual_csr(DualCsrBase* dual_csr) {
    if constexpr (std::is_same_v<EDATA_T, RecordView>) {
      auto casted_dual_csr = dynamic_cast<DualCsr<RecordView>*>(dual_csr);
      CHECK(casted_dual_csr != NULL);
      return casted_dual_csr;
    } else {
      auto casted_dual_csr = dynamic_cast<DualCsr<EDATA_T>*>(dual_csr);
      CHECK(casted_dual_csr != NULL);
      return casted_dual_csr;
    }
  }
  template <typename EDATA_T, typename VECTOR_T>
  void PutEdges(label_t src_label_id, label_t dst_label_id,
                label_t edge_label_id, const std::vector<VECTOR_T>& edges_vec,
                const std::vector<int32_t>& ie_degree,
                const std::vector<int32_t>& oe_degree, bool build_csr_in_mem) {
    size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                   dst_label_id * edge_label_num_ + edge_label_id;
    auto dual_csr = dual_csr_list_[index];
    CHECK(dual_csr != NULL);
    auto casted_dual_csr = get_casted_dual_csr<EDATA_T>(dual_csr);
    CHECK(casted_dual_csr != NULL);
    auto& src_indexer = lf_indexers_[src_label_id];
    auto& dst_indexer = lf_indexers_[dst_label_id];
    auto src_label_name = schema_.get_vertex_label_name(src_label_id);
    auto dst_label_name = schema_.get_vertex_label_name(dst_label_id);
    auto edge_label_name = schema_.get_edge_label_name(edge_label_id);

    auto INVALID_VID = std::numeric_limits<vid_t>::max();
    std::atomic<size_t> edge_count(0);
    if constexpr (std::is_same_v<EDATA_T, std::string_view>) {
      CHECK(ie_degree.size() == dst_indexer.size());
      CHECK(oe_degree.size() == src_indexer.size());
      if (build_csr_in_mem) {
        dual_csr->BatchInitInMemory(
            edata_prefix(src_label_name, dst_label_name, edge_label_name),
            tmp_dir(work_dir_), oe_degree, ie_degree);
      } else {
        dual_csr->BatchInit(
            oe_prefix(src_label_name, dst_label_name, edge_label_name),
            ie_prefix(src_label_name, dst_label_name, edge_label_name),
            edata_prefix(src_label_name, dst_label_name, edge_label_name),
            tmp_dir(work_dir_), oe_degree, ie_degree);
      }
      std::vector<std::thread> work_threads;
      for (size_t i = 0; i < edges_vec.size(); ++i) {
        work_threads.emplace_back(
            [&](int idx) {
              edge_count.fetch_add(edges_vec[idx].size());
              for (auto& edge : edges_vec[idx]) {
                if (std::get<1>(edge) == INVALID_VID ||
                    std::get<0>(edge) == INVALID_VID) {
                  VLOG(10) << "Skip invalid edge:" << std::get<0>(edge) << "->"
                           << std::get<1>(edge);
                  continue;
                }
                casted_dual_csr->BatchPutEdge(
                    std::get<0>(edge), std::get<1>(edge), std::get<2>(edge));
              }
            },
            i);
      }
      for (auto& t : work_threads) {
        t.join();
      }

      append_edge_loading_progress(src_label_name, dst_label_name,
                                   edge_label_name, LoadingStatus::kLoaded);
      if (schema_.get_sort_on_compaction(src_label_name, dst_label_name,
                                         edge_label_name)) {
        dual_csr->SortByEdgeData(1);
      }
      dual_csr->Dump(
          oe_prefix(src_label_name, dst_label_name, edge_label_name),
          ie_prefix(src_label_name, dst_label_name, edge_label_name),
          edata_prefix(src_label_name, dst_label_name, edge_label_name),
          snapshot_dir(work_dir_, 0));
    } else {
      CHECK(ie_degree.size() == dst_indexer.size());
      CHECK(oe_degree.size() == src_indexer.size());

      if (build_csr_in_mem) {
        dual_csr->BatchInitInMemory(
            edata_prefix(src_label_name, dst_label_name, edge_label_name),
            tmp_dir(work_dir_), oe_degree, ie_degree);
      } else {
        dual_csr->BatchInit(
            oe_prefix(src_label_name, dst_label_name, edge_label_name),
            ie_prefix(src_label_name, dst_label_name, edge_label_name),
            edata_prefix(src_label_name, dst_label_name, edge_label_name),
            tmp_dir(work_dir_), oe_degree, ie_degree);
      }

      std::vector<std::thread> work_threads;
      for (size_t i = 0; i < edges_vec.size(); ++i) {
        work_threads.emplace_back(
            [&](int idx) {
              edge_count.fetch_add(edges_vec[idx].size());
              for (auto& edge : edges_vec[idx]) {
                if (std::get<1>(edge) == INVALID_VID ||
                    std::get<0>(edge) == INVALID_VID) {
                  VLOG(10) << "Skip invalid edge:" << std::get<0>(edge) << "->"
                           << std::get<1>(edge);
                  continue;
                }
                casted_dual_csr->BatchPutEdge(
                    std::get<0>(edge), std::get<1>(edge), std::get<2>(edge));
              }
            },
            i);
      }
      for (auto& t : work_threads) {
        t.join();
      }
      append_edge_loading_progress(src_label_name, dst_label_name,
                                   edge_label_name, LoadingStatus::kLoaded);
      if (schema_.get_sort_on_compaction(src_label_name, dst_label_name,
                                         edge_label_name)) {
        dual_csr->SortByEdgeData(1);
      }

      dual_csr->Dump(
          oe_prefix(src_label_name, dst_label_name, edge_label_name),
          ie_prefix(src_label_name, dst_label_name, edge_label_name),
          edata_prefix(src_label_name, dst_label_name, edge_label_name),
          snapshot_dir(work_dir_, 0));
    }
    append_edge_loading_progress(src_label_name, dst_label_name,
                                 edge_label_name, LoadingStatus::kCommited);
    VLOG(10) << "Finish adding edge batch of size: " << edge_count.load();
  }

  Table& GetVertexTable(size_t ind) {
    CHECK(ind < vertex_data_.size());
    return vertex_data_[ind];
  }

  // get lf_indexer
  const IndexerType& GetLFIndexer(label_t v_label) const;
  IndexerType& GetLFIndexer(label_t v_label);

  const std::string& work_dir() const { return work_dir_; }

  void set_csr(label_t src_label_id, label_t dst_label_id,
               label_t edge_label_id, DualCsrBase* dual_csr);

  DualCsrBase* get_csr(label_t src_label_id, label_t dst_label_id,
                       label_t edge_label_id);

  void init_edge_table(label_t src_label_id, label_t dst_label_id,
                       label_t edge_label_id);

 private:
  // create status files for each vertex label and edge triplet pair.
  void append_vertex_loading_progress(const std::string& label_name,
                                      LoadingStatus status);

  void append_edge_loading_progress(const std::string& src_label_name,
                                    const std::string& dst_label_name,
                                    const std::string& edge_label_name,
                                    LoadingStatus status);
  void init_loading_status_file();
  void init_vertex_data();
  const Schema& schema_;
  std::string work_dir_;
  size_t vertex_label_num_, edge_label_num_;
  std::vector<IndexerType> lf_indexers_;
  std::vector<CsrBase*> ie_, oe_;
  std::vector<DualCsrBase*> dual_csr_list_;
  std::vector<Table> vertex_data_;

  // loading progress related
  std::mutex loading_progress_mutex_;
};
}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_BASIC_FRAGMENT_LOADER_H_