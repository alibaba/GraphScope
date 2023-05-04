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

#ifndef GRAPHSCOPE_DATABASE_INSERT_TRANSACTION_H_
#define GRAPHSCOPE_DATABASE_INSERT_TRANSACTION_H_

#include <limits>

#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/types.h"
#include "grape/serialization/in_archive.h"

namespace gs {

class MutablePropertyFragment;
class ArenaAllocator;
class WalWriter;
class VersionManager;

class InsertTransaction {
 public:
  InsertTransaction(MutablePropertyFragment& graph, ArenaAllocator& alloc,
                    WalWriter& logger, VersionManager& vm,
                    timestamp_t timestamp);

  ~InsertTransaction();

  bool AddVertex(label_t label, oid_t id, const std::vector<Property>& props);

  bool AddVertex(label_t label, oid_t id, const Property& props);

  bool AddEdge(label_t src_label, oid_t src, label_t dst_label, oid_t dst,
               label_t edge_label, const Property& prop);

  void Commit();

  void Abort();

  timestamp_t timestamp() const;

  static void IngestWal(MutablePropertyFragment& graph, uint32_t timestamp,
                        char* data, size_t length, ArenaAllocator& alloc);

 private:
  void clear();

  static bool get_vertex_with_retries(MutablePropertyFragment& graph,
                                      label_t label, oid_t oid, vid_t& lid);

  grape::InArchive arc_;

  std::set<std::pair<label_t, oid_t>> added_vertices_;

  MutablePropertyFragment& graph_;
  ArenaAllocator& alloc_;
  WalWriter& logger_;
  VersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_INSERT_TRANSACTION_H_
