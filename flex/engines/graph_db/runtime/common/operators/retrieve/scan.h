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

#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_SCAN_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_SCAN_H_

#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"

namespace gs {

namespace runtime {
struct ScanParams {
  int alias;
  std::vector<label_t> tables;
};
class Scan {
 public:
  template <typename PRED_T>
  static bl::result<Context> scan_vertex(const GraphReadInterface& graph,
                                         const ScanParams& params,
                                         const PRED_T& predicate) {
    Context ctx;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      SLVertexColumnBuilder builder(label);
      auto vertices = graph.GetVertexSet(label);
      for (auto vid : vertices) {
        if (predicate(label, vid)) {
          builder.push_back_opt(vid);
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      MSVertexColumnBuilder builder;

      for (auto label : params.tables) {
        auto vertices = graph.GetVertexSet(label);
        builder.start_label(label);
        for (auto vid : vertices) {
          if (predicate(label, vid)) {
            builder.push_back_opt(vid);
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    }
    return ctx;
  }

  static bl::result<Context> scan_vertex_with_special_vertex_predicate(
      const GraphReadInterface& graph, const ScanParams& params,
      const SPVertexPredicate& pred);

  template <typename PRED_T>
  static bl::result<Context> filter_gids(const GraphReadInterface& graph,
                                         const ScanParams& params,
                                         const PRED_T& predicate,
                                         const std::vector<int64_t>& gids) {
    Context ctx;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      SLVertexColumnBuilder builder(label);
      for (auto gid : gids) {
        vid_t vid = GlobalId::get_vid(gid);
        if (GlobalId::get_label_id(gid) == label && predicate(label, vid)) {
          builder.push_back_opt(vid);
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      MLVertexColumnBuilder builder;

      for (auto label : params.tables) {
        for (auto gid : gids) {
          vid_t vid = GlobalId::get_vid(gid);
          if (GlobalId::get_label_id(gid) == label && predicate(label, vid)) {
            builder.push_back_vertex({label, vid});
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    }
    return ctx;
  }

  static bl::result<Context> filter_gids_with_special_vertex_predicate(
      const GraphReadInterface& graph, const ScanParams& params,
      const SPVertexPredicate& predicate, const std::vector<int64_t>& oids);

  template <typename PRED_T>
  static bl::result<Context> filter_oids(const GraphReadInterface& graph,
                                         const ScanParams& params,
                                         const PRED_T& predicate,
                                         const std::vector<Any>& oids) {
    Context ctx;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      SLVertexColumnBuilder builder(label);
      for (auto oid : oids) {
        vid_t vid;
        if (graph.GetVertexIndex(label, oid, vid)) {
          if (predicate(label, vid)) {
            builder.push_back_opt(vid);
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      std::vector<std::pair<label_t, vid_t>> vids;

      for (auto label : params.tables) {
        for (auto oid : oids) {
          vid_t vid;
          if (graph.GetVertexIndex(label, oid, vid)) {
            if (predicate(label, vid)) {
              vids.emplace_back(label, vid);
            }
          }
        }
      }
      if (vids.size() == 1) {
        SLVertexColumnBuilder builder(vids[0].first);
        builder.push_back_opt(vids[0].second);
        ctx.set(params.alias, builder.finish());
      } else {
        MLVertexColumnBuilder builder;
        for (auto& pair : vids) {
          builder.push_back_vertex({pair.first, pair.second});
        }
        ctx.set(params.alias, builder.finish());
      }
    }
    return ctx;
  }

  static bl::result<Context> filter_oids_with_special_vertex_predicate(
      const GraphReadInterface& graph, const ScanParams& params,
      const SPVertexPredicate& predicate, const std::vector<Any>& oids);

  static bl::result<Context> find_vertex_with_oid(
      const GraphReadInterface& graph, label_t label, const Any& pk, int alias);

  static bl::result<Context> find_vertex_with_gid(
      const GraphReadInterface& graph, label_t label, int64_t pk, int alias);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_SCAN_H_