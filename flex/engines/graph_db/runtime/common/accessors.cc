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
  switch (type.type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value:
    return std::make_shared<ContextValueAccessor<int64_t>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kI32Value:
    return std::make_shared<ContextValueAccessor<int>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kU64Value:
    return std::make_shared<ContextValueAccessor<uint64_t>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kStringValue:
    return std::make_shared<ContextValueAccessor<std::string_view>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kDate32:
    return std::make_shared<ContextValueAccessor<Date>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kStringSetValue:
    return std::make_shared<ContextValueAccessor<std::set<std::string>>>(ctx,
                                                                         tag);
  case RTAnyType::RTAnyTypeImpl::kBoolValue:
    return std::make_shared<ContextValueAccessor<bool>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kTuple:
    return std::make_shared<ContextValueAccessor<Tuple>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kList:
    return std::make_shared<ContextValueAccessor<List>>(ctx, tag);

  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type.type_enum_);
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_vertex_property_path_accessor(
    const ReadTransaction& txn, const Context& ctx, int tag, RTAnyType type,
    const std::string& prop_name) {
  switch (type.type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value:
    return std::make_shared<VertexPropertyPathAccessor<int64_t>>(txn, ctx, tag,
                                                                 prop_name);
  case RTAnyType::RTAnyTypeImpl::kI32Value:
    return std::make_shared<VertexPropertyPathAccessor<int>>(txn, ctx, tag,
                                                             prop_name);
  case RTAnyType::RTAnyTypeImpl::kU64Value:
    return std::make_shared<VertexPropertyPathAccessor<uint64_t>>(txn, ctx, tag,
                                                                  prop_name);
  case RTAnyType::RTAnyTypeImpl::kStringValue:
    return std::make_shared<VertexPropertyPathAccessor<std::string_view>>(
        txn, ctx, tag, prop_name);
  case RTAnyType::RTAnyTypeImpl::kDate32:
    return std::make_shared<VertexPropertyPathAccessor<Date>>(txn, ctx, tag,
                                                              prop_name);
  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type.type_enum_);
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_vertex_label_path_accessor(const Context& ctx,
                                                             int tag) {
  return std::make_shared<VertexLabelPathAccessor>(ctx, tag);
}

std::shared_ptr<IAccessor> create_vertex_property_vertex_accessor(
    const ReadTransaction& txn, RTAnyType type, const std::string& prop_name) {
  switch (type.type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value:
    return std::make_shared<VertexPropertyVertexAccessor<int64_t>>(txn,
                                                                   prop_name);
  case RTAnyType::RTAnyTypeImpl::kI32Value:
    return std::make_shared<VertexPropertyVertexAccessor<int>>(txn, prop_name);
  case RTAnyType::RTAnyTypeImpl::kU64Value:
    return std::make_shared<VertexPropertyVertexAccessor<uint64_t>>(txn,
                                                                    prop_name);
  case RTAnyType::RTAnyTypeImpl::kStringValue:
    return std::make_shared<VertexPropertyVertexAccessor<std::string_view>>(
        txn, prop_name);
  case RTAnyType::RTAnyTypeImpl::kDate32:
    return std::make_shared<VertexPropertyVertexAccessor<Date>>(txn, prop_name);
  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type.type_enum_);
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_edge_property_path_accessor(
    const Context& ctx, int tag, RTAnyType type) {
  switch (type.type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value:
    return std::make_shared<EdgePropertyPathAccessor<int64_t>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kI32Value:
    return std::make_shared<EdgePropertyPathAccessor<int>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kU64Value:
    return std::make_shared<EdgePropertyPathAccessor<uint64_t>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kStringValue:
    return std::make_shared<EdgePropertyPathAccessor<std::string_view>>(ctx,
                                                                        tag);
  case RTAnyType::RTAnyTypeImpl::kDate32:
    return std::make_shared<EdgePropertyPathAccessor<Date>>(ctx, tag);
  case RTAnyType::RTAnyTypeImpl::kF64Value:
    return std::make_shared<EdgePropertyPathAccessor<double>>(ctx, tag);
  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type.type_enum_);
  }
  return nullptr;
}

std::shared_ptr<IAccessor> create_edge_label_path_accessor(const Context& ctx,
                                                           int tag) {
  return std::make_shared<EdgeLabelPathAccessor>(ctx, tag);
}

std::shared_ptr<IAccessor> create_edge_property_edge_accessor(RTAnyType type) {
  switch (type.type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value:
    return std::make_shared<EdgePropertyEdgeAccessor<int64_t>>();
  case RTAnyType::RTAnyTypeImpl::kI32Value:
    return std::make_shared<EdgePropertyEdgeAccessor<int>>();
  case RTAnyType::RTAnyTypeImpl::kU64Value:
    return std::make_shared<EdgePropertyEdgeAccessor<uint64_t>>();
  case RTAnyType::RTAnyTypeImpl::kStringValue:
    return std::make_shared<EdgePropertyEdgeAccessor<std::string_view>>();
  case RTAnyType::RTAnyTypeImpl::kDate32:
    return std::make_shared<EdgePropertyEdgeAccessor<Date>>();
  case RTAnyType::RTAnyTypeImpl::kF64Value:
    return std::make_shared<EdgePropertyEdgeAccessor<double>>();
  default:
    LOG(FATAL) << "not implemented - " << static_cast<int>(type.type_enum_);
  }
  return nullptr;
}

}  // namespace runtime

}  // namespace gs
