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

#ifndef GRAPHSCOPE_FRAGMENT_MUTABLE_PROPERTY_FRAGMENT_H_
#define GRAPHSCOPE_FRAGMENT_MUTABLE_PROPERTY_FRAGMENT_H_

#include <thread>
#include <tuple>
#include <vector>

#include "flex/storages/rt_mutable_graph/schema.h"

#include "flex/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/dual_csr.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/arrow_utils.h"
#include "flex/utils/indexers.h"
#include "flex/utils/property/table.h"
#include "flex/utils/yaml_utils.h"
#include "grape/io/local_io_adaptor.h"
#include "grape/serialization/out_archive.h"

namespace gs {

class MutablePropertyFragment {
 public:
  MutablePropertyFragment();

  ~MutablePropertyFragment();

  void IngestEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label, timestamp_t ts,
                  grape::OutArchive& arc, Allocator& alloc);

  void UpdateEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label, timestamp_t ts,
                  const Any& arc, Allocator& alloc);

  void Open(const std::string& work_dir, int memory_level);

  void Compact(uint32_t version);

  void Warmup(int thread_num);

  void Dump(const std::string& work_dir, uint32_t version);

  void DumpSchema(const std::string& filename);

  const Schema& schema() const;

  Schema& mutable_schema();

  void Clear();

  inline Table& get_vertex_table(label_t vertex_label) {
    return vertex_data_[vertex_label];
  }

  inline const Table& get_vertex_table(label_t vertex_label) const {
    return vertex_data_[vertex_label];
  }

  vid_t vertex_num(label_t vertex_label) const;

  size_t edge_num(label_t src_label, label_t edge_label,
                  label_t dst_label) const;

  bool get_lid(label_t label, const Any& oid, vid_t& lid) const;

  Any get_oid(label_t label, vid_t lid) const;

  vid_t add_vertex(label_t label, const Any& id);
  std::shared_ptr<CsrConstEdgeIterBase> get_outgoing_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<CsrConstEdgeIterBase> get_incoming_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<CsrEdgeIterBase> get_outgoing_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  std::shared_ptr<CsrEdgeIterBase> get_incoming_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  CsrConstEdgeIterBase* get_outgoing_edges_raw(label_t label, vid_t u,
                                               label_t neighbor_label,
                                               label_t edge_label) const;

  CsrConstEdgeIterBase* get_incoming_edges_raw(label_t label, vid_t u,
                                               label_t neighbor_label,
                                               label_t edge_label) const;

  inline CsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                             label_t edge_label) {
    size_t index =
        schema_.get_edge_triplet_id(label, neighbor_label, edge_label);
    return oe_[index];
  }

  inline const CsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const {
    size_t index =
        schema_.get_edge_triplet_id(label, neighbor_label, edge_label);
    return oe_[index];
  }

  inline CsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                             label_t edge_label) {
    size_t index =
        schema_.get_edge_triplet_id(neighbor_label, label, edge_label);
    return ie_[index];
  }

  inline const CsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const {
    size_t index =
        schema_.get_edge_triplet_id(neighbor_label, label, edge_label);
    return ie_[index];
  }

  void loadSchema(const std::string& filename);
  inline std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& prop) const {
    return vertex_data_[label].get_column(prop);
  }

  inline std::shared_ptr<RefColumnBase> get_vertex_id_column(
      uint8_t label) const {
    if (lf_indexers_[label].get_type() == PropertyType::kInt64) {
      return std::make_shared<TypedRefColumn<int64_t>>(
          dynamic_cast<const TypedColumn<int64_t>&>(
              lf_indexers_[label].get_keys()));
    } else if (lf_indexers_[label].get_type() == PropertyType::kInt32) {
      return std::make_shared<TypedRefColumn<int32_t>>(
          dynamic_cast<const TypedColumn<int32_t>&>(
              lf_indexers_[label].get_keys()));
    } else if (lf_indexers_[label].get_type() == PropertyType::kUInt64) {
      return std::make_shared<TypedRefColumn<uint64_t>>(
          dynamic_cast<const TypedColumn<uint64_t>&>(
              lf_indexers_[label].get_keys()));
    } else if (lf_indexers_[label].get_type() == PropertyType::kUInt32) {
      return std::make_shared<TypedRefColumn<uint32_t>>(
          dynamic_cast<const TypedColumn<uint32_t>&>(
              lf_indexers_[label].get_keys()));
    } else if (lf_indexers_[label].get_type() == PropertyType::kStringView) {
      return std::make_shared<TypedRefColumn<std::string_view>>(
          dynamic_cast<const TypedColumn<std::string_view>&>(
              lf_indexers_[label].get_keys()));
    } else {
      LOG(ERROR) << "Unsupported vertex id type: "
                 << lf_indexers_[label].get_type();
      return nullptr;
    }
  }

  void generateStatistics(const std::string& work_dir) const;

  Schema schema_;
  std::vector<IndexerType> lf_indexers_;
  std::vector<CsrBase*> ie_, oe_;
  std::vector<DualCsrBase*> dual_csr_list_;
  std::vector<Table> vertex_data_;

  size_t vertex_label_num_, edge_label_num_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_MUTABLE_PROPERTY_FRAGMENT_H_
