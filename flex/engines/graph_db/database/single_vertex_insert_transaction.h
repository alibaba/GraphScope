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

#ifndef GRAPHSCOPE_DATABASE_SINGLE_VERTEX_INSERT_TRANSACTION_H_
#define GRAPHSCOPE_DATABASE_SINGLE_VERTEX_INSERT_TRANSACTION_H_

#include "flex/storages/rt_mutable_graph/types.h"
#include "grape/serialization/in_archive.h"

namespace gs {

class MutablePropertyFragment;
class ArenaAllocator;
class WalWriter;
class VersionManager;
class Any;

class SingleVertexInsertTransaction {
 public:
  SingleVertexInsertTransaction(MutablePropertyFragment& graph,
                                ArenaAllocator& alloc, WalWriter& logger,
                                VersionManager& vm, timestamp_t timestamp);
  ~SingleVertexInsertTransaction();

  bool AddVertex(label_t label, oid_t id, const std::vector<Any>& props);

  bool AddEdge(label_t src_label, oid_t src, label_t dst_label, oid_t dst,
               label_t edge_label, const Any& prop);

  void Commit();

  void Abort();

  timestamp_t timestamp() const;

  void ingestWal();

 private:
  void clear();

  grape::InArchive arc_;

  label_t added_vertex_label_;
  oid_t added_vertex_id_;
  vid_t added_vertex_vid_;
  std::vector<vid_t> parsed_endpoints_;

  MutablePropertyFragment& graph_;
  ArenaAllocator& alloc_;
  WalWriter& logger_;
  VersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_SINGLE_VERTEX_INSERT_TRANSACTION_H_
