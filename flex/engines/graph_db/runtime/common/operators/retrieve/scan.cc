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

#include "flex/engines/graph_db/runtime/common/operators/retrieve/scan.h"

namespace gs {
namespace runtime {

Context Scan::find_vertex_with_oid(const GraphReadInterface& graph,
                                   label_t label, const Any& oid,
                                   int32_t alias) {
  SLVertexColumnBuilder builder(label);
  vid_t vid;
  if (graph.GetVertexIndex(label, oid, vid)) {
    builder.push_back_opt(vid);
  }
  Context ctx;
  ctx.set(alias, builder.finish());
  return ctx;
}

Context Scan::find_vertex_with_gid(const GraphReadInterface& graph,
                                   label_t label, int64_t gid, int32_t alias) {
  SLVertexColumnBuilder builder(label);
  if (GlobalId::get_label_id(gid) == label) {
    builder.push_back_opt(GlobalId::get_vid(gid));
  } else {
    LOG(ERROR) << "Invalid label id: "
               << static_cast<int>(GlobalId::get_label_id(gid));
  }
  Context ctx;
  ctx.set(alias, builder.finish());
  return ctx;
}

template <typename T>
static Context _scan_vertex_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& pred) {
  if (pred.type() == SPPredicateType::kPropertyEQ) {
    return Scan::scan_vertex<VertexPropertyEQPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyEQPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyGE) {
    return Scan::scan_vertex<VertexPropertyGEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyGEPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyGT) {
    return Scan::scan_vertex<VertexPropertyGTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyGTPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyLE) {
    return Scan::scan_vertex<VertexPropertyLEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyLEPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyLT) {
    return Scan::scan_vertex<VertexPropertyLTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyLTPredicateBeta<T>&>(pred));
  } else if (pred.type() == SPPredicateType::kPropertyNE) {
    return Scan::scan_vertex<VertexPropertyNEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyNEPredicateBeta<T>&>(pred));
  } else {
    LOG(FATAL) << "not impl... - " << static_cast<int>(pred.type());
    return Context();
  }
}

Context Scan::scan_vertex_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& pred) {
  if (pred.data_type() == RTAnyType::kI64Value) {
    return _scan_vertex_with_special_vertex_predicate<int64_t>(graph, params,
                                                               pred);
  } else if (pred.data_type() == RTAnyType::kI32Value) {
    return _scan_vertex_with_special_vertex_predicate<int32_t>(graph, params,
                                                               pred);
  } else if (pred.data_type() == RTAnyType::kStringValue) {
    return _scan_vertex_with_special_vertex_predicate<std::string_view>(
        graph, params, pred);
  } else if (pred.data_type() == RTAnyType::kF64Value) {
    return _scan_vertex_with_special_vertex_predicate<double>(graph, params,
                                                              pred);
  } else if (pred.data_type() == RTAnyType::kDate32) {
    return _scan_vertex_with_special_vertex_predicate<Day>(graph, params, pred);
  } else if (pred.data_type() == RTAnyType::kTimestamp) {
    return _scan_vertex_with_special_vertex_predicate<Date>(graph, params,
                                                            pred);
  } else {
    LOG(FATAL) << "not impl... - " << static_cast<int>(pred.data_type());
    return Context();
  }

  LOG(FATAL) << "not impl... - " << static_cast<int>(pred.type());
  return Context();
}

template <typename T>
static Context _filter_gids_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& pred, const std::vector<int64_t>& gids) {
  if (pred.type() == SPPredicateType::kPropertyEQ) {
    return Scan::filter_gids<VertexPropertyEQPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyEQPredicateBeta<T>&>(pred), gids);
  } else if (pred.type() == SPPredicateType::kPropertyGE) {
    return Scan::filter_gids<VertexPropertyGEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyGEPredicateBeta<T>&>(pred), gids);
  } else if (pred.type() == SPPredicateType::kPropertyGT) {
    return Scan::filter_gids<VertexPropertyGTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyGTPredicateBeta<T>&>(pred), gids);
  } else if (pred.type() == SPPredicateType::kPropertyLE) {
    return Scan::filter_gids<VertexPropertyLEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyLEPredicateBeta<T>&>(pred), gids);
  } else if (pred.type() == SPPredicateType::kPropertyLT) {
    return Scan::filter_gids<VertexPropertyLTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyLTPredicateBeta<T>&>(pred), gids);
  } else if (pred.type() == SPPredicateType::kPropertyNE) {
    return Scan::filter_gids<VertexPropertyNEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyNEPredicateBeta<T>&>(pred), gids);
  } else {
    LOG(FATAL) << "not impl... - " << static_cast<int>(pred.type());
    return Context();
  }
}

Context Scan::filter_gids_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& predicate, const std::vector<int64_t>& oids) {
  if (predicate.data_type() == RTAnyType::kI64Value) {
    return _filter_gids_with_special_vertex_predicate<int64_t>(graph, params,
                                                               predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kI32Value) {
    return _filter_gids_with_special_vertex_predicate<int32_t>(graph, params,
                                                               predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kStringValue) {
    return _filter_gids_with_special_vertex_predicate<std::string_view>(
        graph, params, predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kF64Value) {
    return _filter_gids_with_special_vertex_predicate<double>(graph, params,
                                                              predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kDate32) {
    return _filter_gids_with_special_vertex_predicate<Day>(graph, params,
                                                           predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kTimestamp) {
    return _filter_gids_with_special_vertex_predicate<Date>(graph, params,
                                                            predicate, oids);
  } else {
    LOG(FATAL) << "not support type :"
               << static_cast<int>(predicate.data_type());
    return Context();
  }
}

template <typename T>
static Context _filter_oid_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& pred, const std::vector<Any>& oids) {
  if (pred.type() == SPPredicateType::kPropertyEQ) {
    return Scan::filter_oids<VertexPropertyEQPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyEQPredicateBeta<T>&>(pred), oids);
  } else if (pred.type() == SPPredicateType::kPropertyGE) {
    return Scan::filter_oids<VertexPropertyGEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyGEPredicateBeta<T>&>(pred), oids);
  } else if (pred.type() == SPPredicateType::kPropertyGT) {
    return Scan::filter_oids<VertexPropertyGTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyGTPredicateBeta<T>&>(pred), oids);
  } else if (pred.type() == SPPredicateType::kPropertyLE) {
    return Scan::filter_oids<VertexPropertyLEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyLEPredicateBeta<T>&>(pred), oids);
  } else if (pred.type() == SPPredicateType::kPropertyLT) {
    return Scan::filter_oids<VertexPropertyLTPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyLTPredicateBeta<T>&>(pred), oids);
  } else if (pred.type() == SPPredicateType::kPropertyNE) {
    return Scan::filter_oids<VertexPropertyNEPredicateBeta<T>>(
        graph, params,
        dynamic_cast<const VertexPropertyNEPredicateBeta<T>&>(pred), oids);
  } else {
    LOG(FATAL) << "not impl... - " << static_cast<int>(pred.type());
    return Context();
  }
}

Context Scan::filter_oids_with_special_vertex_predicate(
    const GraphReadInterface& graph, const ScanParams& params,
    const SPVertexPredicate& predicate, const std::vector<Any>& oids) {
  if (predicate.data_type() == RTAnyType::kI64Value) {
    return _filter_oid_with_special_vertex_predicate<int64_t>(graph, params,
                                                              predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kI32Value) {
    return _filter_oid_with_special_vertex_predicate<int32_t>(graph, params,
                                                              predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kStringValue) {
    return _filter_oid_with_special_vertex_predicate<std::string_view>(
        graph, params, predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kF64Value) {
    return _filter_oid_with_special_vertex_predicate<double>(graph, params,
                                                             predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kDate32) {
    return _filter_oid_with_special_vertex_predicate<Day>(graph, params,
                                                          predicate, oids);
  } else if (predicate.data_type() == RTAnyType::kTimestamp) {
    return _filter_oid_with_special_vertex_predicate<Date>(graph, params,
                                                           predicate, oids);
  } else {
    LOG(FATAL) << "not support type: "
               << static_cast<int>(predicate.data_type());
    return Context();
  }
}
}  // namespace runtime

}  // namespace gs
