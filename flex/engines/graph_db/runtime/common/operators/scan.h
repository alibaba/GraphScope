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

#ifndef RUNTIME_COMMON_OPERATORS_SCAN_H_
#define RUNTIME_COMMON_OPERATORS_SCAN_H_

#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace bl = boost::leaf;
namespace gs {

namespace runtime {
struct ScanParams {
  int alias;
  std::vector<label_t> tables;
};
class Scan {
 public:
  template <typename PRED_T>
  static bl::result<Context> scan_vertex(const ReadTransaction& txn,
                                         const ScanParams& params,
                                         const PRED_T& predicate) {
    Context ctx;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      SLVertexColumnBuilder builder(label);
      vid_t vnum = txn.GetVertexNum(label);
      for (vid_t vid = 0; vid != vnum; ++vid) {
        if (predicate(label, vid)) {
          builder.push_back_opt(vid);
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      MLVertexColumnBuilder builder;

      for (auto label : params.tables) {
        vid_t vnum = txn.GetVertexNum(label);
        for (vid_t vid = 0; vid != vnum; ++vid) {
          if (predicate(label, vid)) {
            builder.push_back_vertex(std::make_pair(label, vid));
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    } else {
      LOG(ERROR) << "No vertex labels in scan_vertex";
      RETURN_BAD_REQUEST_ERROR("No valid vertex labels in scan_vertex");
    }
    return ctx;
  }

  template <typename PRED_T>
  static Context filter_gids(const ReadTransaction& txn,
                             const ScanParams& params, const PRED_T& predicate,
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
            builder.push_back_vertex(std::make_pair(label, vid));
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    }
    return ctx;
  }

  template <typename PRED_T, typename KEY_T>
  static Context filter_oids(const ReadTransaction& txn,
                             const ScanParams& params, const PRED_T& predicate,
                             const std::vector<KEY_T>& oids) {
    Context ctx;
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      SLVertexColumnBuilder builder(label);
      for (auto oid : oids) {
        vid_t vid;
        if (txn.GetVertexIndex(label, oid, vid)) {
          if (predicate(label, vid)) {
            builder.push_back_opt(vid);
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      MLVertexColumnBuilder builder;

      for (auto label : params.tables) {
        for (auto oid : oids) {
          vid_t vid;
          if (txn.GetVertexIndex(label, oid, vid)) {
            if (predicate(label, vid)) {
              builder.push_back_vertex(std::make_pair(label, vid));
            }
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    }
    return ctx;
  }

  // EXPR() is a function that returns the oid of the vertex
  template <typename EXPR>
  static Context find_vertex(const ReadTransaction& txn, label_t label,
                             const EXPR& expr, int alias, bool scan_oid) {
    Context ctx;
    SLVertexColumnBuilder builder(label);
    if (scan_oid) {
      auto oid = expr();
      vid_t vid;
      if (txn.GetVertexIndex(label, oid, vid)) {
        builder.push_back_opt(vid);
      }
    } else {
      int64_t gid = expr();
      if (GlobalId::get_label_id(gid) == label) {
        builder.push_back_opt(GlobalId::get_vid(gid));
      } else {
        LOG(ERROR) << "Invalid label id: "
                   << static_cast<int>(GlobalId::get_label_id(gid));
      }
    }
    ctx.set(alias, builder.finish());
    return ctx;
  }

  static bl::result<Context> find_vertex_with_id(const ReadTransaction& txn,
                                                 label_t label, const Any& pk,
                                                 int alias, bool scan_oid);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_SCAN_H_