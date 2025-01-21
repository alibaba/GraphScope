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

#include "flex/engines/graph_db/runtime/common/accessors.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"

namespace gs {

namespace runtime {

std::shared_ptr<IAccessor> create_context_value_accessor(const Context& ctx,
                                                         int tag,
                                                         RTAnyType type) {
  auto col = ctx.get(tag);
  switch (type) {
  case RTAnyType::kI64Value:
    return std::make_shared<ContextValueAccessor<int64_t>>(ctx, tag);
  case RTAnyType::kI32Value:
    return std::make_shared<ContextValueAccessor<int>>(ctx, tag);
  case RTAnyType::kU64Value:
    return std::make_shared<ContextValueAccessor<uint64_t>>(ctx, tag);
  case RTAnyType::kStringValue:
    return std::make_shared<ContextValueAccessor<std::string_view>>(ctx, tag);
  case RTAnyType::kDate32:
    return std::make_shared<ContextValueAccessor<Day>>(ctx, tag);
  case RTAnyType::kTimestamp:
    return std::make_shared<ContextValueAccessor<Date>>(ctx, tag);
  case RTAnyType::kStringSetValue:
    return std::make_shared<ContextValueAccessor<std::set<std::string>>>(ctx,
                                                                         tag);
  case RTAnyType::kBoolValue:
    return std::make_shared<ContextValueAccessor<bool>>(ctx, tag);
  case RTAnyType::kTuple:
    return std::make_shared<ContextValueAccessor<Tuple>>(ctx, tag);
  case RTAnyType::kList:
    return std::make_shared<ContextValueAccessor<List>>(ctx, tag);

  case RTAnyType::kRelation:
    return std::make_shared<ContextValueAccessor<Relation>>(ctx, tag);
  case RTAnyType::kF64Value:
    return std::make_shared<ContextValueAccessor<double>>(ctx, tag);
  case RTAnyType::kSet:
    return std::make_shared<ContextValueAccessor<Set>>(ctx, tag);
  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type);
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_vertex_property_path_accessor(
    const GraphReadInterface& graph, const Context& ctx, int tag,
    RTAnyType type, const std::string& prop_name) {
  switch (type) {
  case RTAnyType::kI64Value:
    return std::make_shared<VertexPropertyPathAccessor<int64_t>>(
        graph, ctx, tag, prop_name);
  case RTAnyType::kI32Value:
    return std::make_shared<VertexPropertyPathAccessor<int>>(graph, ctx, tag,
                                                             prop_name);
  case RTAnyType::kU64Value:
    return std::make_shared<VertexPropertyPathAccessor<uint64_t>>(
        graph, ctx, tag, prop_name);
  case RTAnyType::kStringValue:
    return std::make_shared<VertexPropertyPathAccessor<std::string_view>>(
        graph, ctx, tag, prop_name);
  case RTAnyType::kDate32:
    return std::make_shared<VertexPropertyPathAccessor<Day>>(graph, ctx, tag,
                                                             prop_name);
  case RTAnyType::kTimestamp:
    return std::make_shared<VertexPropertyPathAccessor<Date>>(graph, ctx, tag,
                                                              prop_name);
  case RTAnyType::kF64Value:
    return std::make_shared<VertexPropertyPathAccessor<double>>(graph, ctx, tag,
                                                                prop_name);
  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type);
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_vertex_label_path_accessor(const Context& ctx,
                                                             int tag) {
  return std::make_shared<VertexLabelPathAccessor>(ctx, tag);
}

std::shared_ptr<IAccessor> create_vertex_property_vertex_accessor(
    const GraphReadInterface& graph, RTAnyType type,
    const std::string& prop_name) {
  switch (type) {
  case RTAnyType::kI64Value:
    return std::make_shared<VertexPropertyVertexAccessor<int64_t>>(graph,
                                                                   prop_name);
  case RTAnyType::kI32Value:
    return std::make_shared<VertexPropertyVertexAccessor<int32_t>>(graph,
                                                                   prop_name);
  case RTAnyType::kU64Value:
    return std::make_shared<VertexPropertyVertexAccessor<uint64_t>>(graph,
                                                                    prop_name);
  case RTAnyType::kStringValue:
    return std::make_shared<VertexPropertyVertexAccessor<std::string_view>>(
        graph, prop_name);
  case RTAnyType::kDate32:
    return std::make_shared<VertexPropertyVertexAccessor<Day>>(graph,
                                                               prop_name);
  case RTAnyType::kTimestamp:
    return std::make_shared<VertexPropertyVertexAccessor<Date>>(graph,
                                                                prop_name);
  case RTAnyType::kF64Value:
    return std::make_shared<VertexPropertyVertexAccessor<double>>(graph,
                                                                  prop_name);
  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type);
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_edge_property_path_accessor(
    const GraphReadInterface& graph, const std::string& name,
    const Context& ctx, int tag, RTAnyType type) {
  auto col = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag));
  const auto& labels = col->get_labels();
  bool multip_properties = false;
  if (graph.schema().has_multi_props_edge()) {
    for (auto label : labels) {
      auto& properties = graph.schema().get_edge_properties(
          label.src_label, label.dst_label, label.edge_label);
      if (properties.size() > 1) {
        multip_properties = true;
        break;
      }
    }
  }
  if (multip_properties) {
    switch (type) {
    case RTAnyType::kI64Value:
      return std::make_shared<MultiPropsEdgePropertyPathAccessor<int64_t>>(
          graph, name, ctx, tag);
    case RTAnyType::kI32Value:
      return std::make_shared<MultiPropsEdgePropertyPathAccessor<int32_t>>(
          graph, name, ctx, tag);
    case RTAnyType::kU64Value:
      return std::make_shared<MultiPropsEdgePropertyPathAccessor<uint64_t>>(
          graph, name, ctx, tag);
    case RTAnyType::kStringValue:
      return std::make_shared<
          MultiPropsEdgePropertyPathAccessor<std::string_view>>(graph, name,
                                                                ctx, tag);
    case RTAnyType::kDate32:
      return std::make_shared<MultiPropsEdgePropertyPathAccessor<Day>>(
          graph, name, ctx, tag);
    case RTAnyType::kTimestamp:
      return std::make_shared<MultiPropsEdgePropertyPathAccessor<Date>>(
          graph, name, ctx, tag);
    case RTAnyType::kF64Value:
      return std::make_shared<MultiPropsEdgePropertyPathAccessor<double>>(
          graph, name, ctx, tag);
    default:
      LOG(FATAL) << "not implemented - " << static_cast<int>(type);
    }
  } else {
    switch (type) {
    case RTAnyType::kI64Value:
      return std::make_shared<EdgePropertyPathAccessor<int64_t>>(graph, name,
                                                                 ctx, tag);
    case RTAnyType::kI32Value:
      return std::make_shared<EdgePropertyPathAccessor<int>>(graph, name, ctx,
                                                             tag);
    case RTAnyType::kU64Value:
      return std::make_shared<EdgePropertyPathAccessor<uint64_t>>(graph, name,
                                                                  ctx, tag);
    case RTAnyType::kStringValue:
      return std::make_shared<EdgePropertyPathAccessor<std::string_view>>(
          graph, name, ctx, tag);
    case RTAnyType::kDate32:
      return std::make_shared<EdgePropertyPathAccessor<Day>>(graph, name, ctx,
                                                             tag);

    case RTAnyType::kTimestamp:
      return std::make_shared<EdgePropertyPathAccessor<Date>>(graph, name, ctx,
                                                              tag);

    case RTAnyType::kF64Value:
      return std::make_shared<EdgePropertyPathAccessor<double>>(graph, name,
                                                                ctx, tag);
    default:
      LOG(FATAL) << "not implemented - " << static_cast<int>(type);
    }
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_edge_label_path_accessor(const Context& ctx,
                                                           int tag) {
  return std::make_shared<EdgeLabelPathAccessor>(ctx, tag);
}

std::shared_ptr<IAccessor> create_edge_property_edge_accessor(
    const GraphReadInterface& graph, const std::string& prop_name,
    RTAnyType type) {
  bool multip_properties = graph.schema().has_multi_props_edge();

  if (multip_properties) {
    switch (type) {
    case RTAnyType::kI64Value:
      return std::make_shared<MultiPropsEdgePropertyEdgeAccessor<int64_t>>(
          graph, prop_name);
    case RTAnyType::kI32Value:
      return std::make_shared<MultiPropsEdgePropertyEdgeAccessor<int>>(
          graph, prop_name);
    case RTAnyType::kU64Value:
      return std::make_shared<MultiPropsEdgePropertyEdgeAccessor<uint64_t>>(
          graph, prop_name);
    case RTAnyType::kStringValue:
      return std::make_shared<
          MultiPropsEdgePropertyEdgeAccessor<std::string_view>>(graph,
                                                                prop_name);
    case RTAnyType::kDate32:
      return std::make_shared<MultiPropsEdgePropertyEdgeAccessor<Day>>(
          graph, prop_name);
    case RTAnyType::kTimestamp:
      return std::make_shared<MultiPropsEdgePropertyEdgeAccessor<Date>>(
          graph, prop_name);
    case RTAnyType::kF64Value:
      return std::make_shared<MultiPropsEdgePropertyEdgeAccessor<double>>(
          graph, prop_name);
    default:
      LOG(FATAL) << "not implemented - " << static_cast<int>(type);
    }
  } else {
    switch (type) {
    case RTAnyType::kI64Value:
      return std::make_shared<EdgePropertyEdgeAccessor<int64_t>>(graph,
                                                                 prop_name);
    case RTAnyType::kI32Value:
      return std::make_shared<EdgePropertyEdgeAccessor<int>>(graph, prop_name);
    case RTAnyType::kU64Value:
      return std::make_shared<EdgePropertyEdgeAccessor<uint64_t>>(graph,
                                                                  prop_name);
    case RTAnyType::kStringValue:
      return std::make_shared<EdgePropertyEdgeAccessor<std::string_view>>(
          graph, prop_name);
    case RTAnyType::kDate32:
      return std::make_shared<EdgePropertyEdgeAccessor<Day>>(graph, prop_name);

    case RTAnyType::kTimestamp:
      return std::make_shared<EdgePropertyEdgeAccessor<Date>>(graph, prop_name);
    case RTAnyType::kF64Value:
      return std::make_shared<EdgePropertyEdgeAccessor<double>>(graph,
                                                                prop_name);
    default:
      LOG(FATAL) << "not implemented - " << static_cast<int>(type);
    }
  }
  return nullptr;
}

}  // namespace runtime

}  // namespace gs
