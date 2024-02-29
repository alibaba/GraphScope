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

#ifndef GRAPHSCOPE_DATABASE_UPDATE_TRANSACTION_H_
#define GRAPHSCOPE_DATABASE_UPDATE_TRANSACTION_H_

#include <limits>
#include <utility>

#include "flat_hash_map/flat_hash_map.hpp"
#include "flex/engines/graph_db/database/transaction_utils.h"
#include "flex/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/allocators.h"
#include "flex/utils/id_indexer.h"
#include "flex/utils/property/table.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/in_archive.h"

namespace gs {

class MutablePropertyFragment;
class WalWriter;
class VersionManager;

class UpdateTransaction {
 public:
  UpdateTransaction(MutablePropertyFragment& graph, Allocator& alloc,
                    const std::string& work_dir, WalWriter& logger,
                    VersionManager& vm, timestamp_t timestamp);

  ~UpdateTransaction();

  timestamp_t timestamp() const;

  void Commit();

  void Abort();

  bool AddVertex(label_t label, const Any& oid, const std::vector<Any>& props);

  bool AddEdge(label_t src_label, const Any& src, label_t dst_label,
               const Any& dst, label_t edge_label, const Any& value);

  class vertex_iterator {
   public:
    vertex_iterator(label_t label, vid_t cur, vid_t& num,
                    UpdateTransaction* txn);
    ~vertex_iterator();
    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    Any GetId() const;

    vid_t GetIndex() const;

    Any GetField(int col_id) const;

    bool SetField(int col_id, const Any& value);

   private:
    label_t label_;
    vid_t cur_;

    vid_t& num_;
    UpdateTransaction* txn_;
  };

  class edge_iterator {
   public:
    edge_iterator(bool dir, label_t label, vid_t v, label_t neighbor_label,
                  label_t edge_label, const vid_t* aeb, const vid_t* aee,
                  std::shared_ptr<CsrConstEdgeIterBase> init_iter,
                  UpdateTransaction* txn);
    ~edge_iterator();

    Any GetData() const;

    void SetData(const Any& value);

    bool IsValid() const;

    void Next();

    void Forward(size_t offset);

    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    bool dir_;

    label_t label_;
    vid_t v_;

    label_t neighbor_label_;
    label_t edge_label_;

    const vid_t* added_edges_cur_;
    const vid_t* added_edges_end_;

    std::shared_ptr<CsrConstEdgeIterBase> init_iter_;
    UpdateTransaction* txn_;
    size_t offset_;
  };

  vertex_iterator GetVertexIterator(label_t label);

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighbor_label, label_t edge_label);

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighbor_label, label_t edge_label);

  Any GetVertexField(label_t label, vid_t lid, int col_id) const;

  bool SetVertexField(label_t label, vid_t lid, int col_id, const Any& value);

  void SetEdgeData(bool dir, label_t label, vid_t v, label_t neighbor_label,
                   vid_t nbr, label_t edge_label, const Any& value);

  bool GetUpdatedEdgeData(bool dir, label_t label, vid_t v,
                          label_t neighbor_label, vid_t nbr, label_t edge_label,
                          Any& ret) const;

  static void IngestWal(MutablePropertyFragment& graph,
                        const std::string& work_dir, uint32_t timestamp,
                        char* data, size_t length, Allocator& alloc);

 private:
  friend class GraphDBSession;
  void batch_commit(UpdateBatch& batch);

  void set_edge_data_with_offset(bool dir, label_t label, vid_t v,
                                 label_t neighbor_label, vid_t nbr,
                                 label_t edge_label, const Any& value,
                                 size_t offset);

  size_t get_in_csr_index(label_t src_label, label_t dst_label,
                          label_t edge_label) const;

  size_t get_out_csr_index(label_t src_label, label_t dst_label,
                           label_t edge_label) const;

  bool oid_to_lid(label_t label, const Any& oid, vid_t& lid) const;

  Any lid_to_oid(label_t label, vid_t lid) const;

  void release();

  void applyVerticesUpdates();

  void applyEdgesUpdates();

  MutablePropertyFragment& graph_;
  Allocator& alloc_;
  WalWriter& logger_;
  VersionManager& vm_;
  timestamp_t timestamp_;

  grape::InArchive arc_;
  int op_num_;

  size_t vertex_label_num_;
  size_t edge_label_num_;

  std::vector<std::shared_ptr<IdIndexerBase<vid_t>>> added_vertices_;
  std::vector<vid_t> added_vertices_base_;
  std::vector<vid_t> vertex_nums_;
  std::vector<ska::flat_hash_map<vid_t, vid_t>> vertex_offsets_;
  std::vector<Table> extra_vertex_properties_;

  std::vector<ska::flat_hash_map<vid_t, std::vector<vid_t>>> added_edges_;
  std::vector<ska::flat_hash_map<
      vid_t, ska::flat_hash_map<vid_t, std::pair<Any, size_t>>>>
      updated_edge_data_;

  std::vector<std::string> sv_vec_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_UPDATE_TRANSACTION_H_
