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

#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/id_indexer.h"
#include "flex/utils/property/table.h"
#include "flex/utils/yaml_utils.h"
#include "grape/io/local_io_adaptor.h"
#include "grape/serialization/out_archive.h"

namespace gs {

class MutablePropertyFragment {
 public:
  MutablePropertyFragment();

  ~MutablePropertyFragment();

  void Init(const Schema& schema, const LoadingConfig& loading_config,
            int thread_num = 1);

  void IngestEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label, timestamp_t ts,
                  grape::OutArchive& arc, ArenaAllocator& alloc);

  const Schema& schema() const;

  void Serialize(const std::string& prefix);

  void Deserialize(const std::string& prefix);

  Table& get_vertex_table(label_t vertex_label);

  const Table& get_vertex_table(label_t vertex_label) const;

  vid_t vertex_num(label_t vertex_label) const;

  bool get_lid(label_t label, oid_t oid, vid_t& lid) const;

  oid_t get_oid(label_t label, vid_t lid) const;

  vid_t add_vertex(label_t label, oid_t id);
  std::shared_ptr<MutableCsrConstEdgeIterBase> get_outgoing_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<MutableCsrConstEdgeIterBase> get_incoming_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<MutableCsrEdgeIterBase> get_outgoing_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  std::shared_ptr<MutableCsrEdgeIterBase> get_incoming_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  MutableCsrConstEdgeIterBase* get_outgoing_edges_raw(label_t label, vid_t u,
                                                      label_t neighbor_label,
                                                      label_t edge_label) const;

  MutableCsrConstEdgeIterBase* get_incoming_edges_raw(label_t label, vid_t u,
                                                      label_t neighbor_label,
                                                      label_t edge_label) const;

  MutableCsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                             label_t edge_label);

  const MutableCsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const;

  MutableCsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                             label_t edge_label);

  const MutableCsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                                   label_t edge_label) const;

  void parseVertexFiles(
      const std::string& vertex_label,
      const std::vector<std::string>& filenames,
      const std::vector<std::pair<size_t, std::string>>& vertex_column_mappings,
      IdIndexer<oid_t, vid_t>& indexer);

  void initVertices(label_t v_label_i,
                    const std::vector<std::string>& vertex_files,
                    const std::vector<std::pair<size_t, std::string>>&
                        vertex_column_mappings);

  void initEdges(
      label_t src_label_i, label_t dst_label_i, label_t edge_label_i,
      const std::vector<std::string>& edge_files,
      const std::vector<std::pair<size_t, std::string>>& edge_col_mappings,
      size_t src_col_id, size_t dst_col_id);

  Schema schema_;
  std::vector<LFIndexer<vid_t>> lf_indexers_;
  std::vector<MutableCsrBase*> ie_, oe_;
  std::vector<Table> vertex_data_;

  size_t vertex_label_num_, edge_label_num_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_MUTABLE_PROPERTY_FRAGMENT_H_
