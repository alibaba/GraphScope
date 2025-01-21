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

#include "flex/engines/graph_db/runtime/utils/var.h"
#include "flex/engines/graph_db/runtime/common/accessors.h"
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {

namespace runtime {

Var::Var(const GraphReadInterface& graph, const Context& ctx,
         const common::Variable& pb, VarType var_type)
    : getter_(nullptr) {
  int tag = -1;
  type_ = RTAnyType::kUnknown;
  if (pb.has_node_type()) {
    type_ = parse_from_ir_data_type(pb.node_type());
  }
  if (pb.has_tag()) {
    tag = pb.tag().id();
  }

  if (type_ == RTAnyType::kUnknown) {
    if (pb.has_tag()) {
      tag = pb.tag().id();
      assert(ctx.get(tag) != nullptr);
      type_ = ctx.get(tag)->elem_type();
    } else if (pb.has_property() && pb.property().has_label()) {
      type_ = RTAnyType::kI64Value;
    } else {
      LOG(FATAL) << "not support" << pb.DebugString();
    }
  }

  if (pb.has_tag() || var_type == VarType::kPathVar) {
    assert(ctx.get(tag) != nullptr);
    if (ctx.get(tag)->column_type() == ContextColumnType::kVertex) {
      if (pb.has_property()) {
        auto& pt = pb.property();
        if (pt.has_id()) {
          getter_ = std::make_shared<VertexGIdPathAccessor>(ctx, tag);
        } else if (pt.has_key()) {
          if (pt.key().name() == "id") {
            if (type_ == RTAnyType::kStringValue) {
              getter_ =
                  std::make_shared<VertexIdPathAccessor<std::string_view>>(
                      graph, ctx, tag);
            } else if (type_ == RTAnyType::kI32Value) {
              getter_ = std::make_shared<VertexIdPathAccessor<int32_t>>(
                  graph, ctx, tag);
            } else if (type_ == RTAnyType::kI64Value) {
              getter_ = std::make_shared<VertexIdPathAccessor<int64_t>>(
                  graph, ctx, tag);
            } else {
              LOG(FATAL) << "not support for " << static_cast<int>(type_);
            }
          } else {
            getter_ = create_vertex_property_path_accessor(
                graph, ctx, tag, type_, pt.key().name());
          }
        } else if (pt.has_label()) {
          getter_ = create_vertex_label_path_accessor(ctx, tag);
        } else {
          LOG(FATAL) << "not support for " << pt.DebugString();
        }
      } else {
        getter_ = std::make_shared<VertexPathAccessor>(ctx, tag);
      }
    } else if (ctx.get(tag)->column_type() == ContextColumnType::kValue ||
               ctx.get(tag)->column_type() ==
                   ContextColumnType::kOptionalValue) {
      if (type_ == RTAnyType::kEdge) {
        type_ = RTAnyType::kRelation;
      }
      getter_ = create_context_value_accessor(ctx, tag, type_);
    } else if (ctx.get(tag)->column_type() == ContextColumnType::kEdge) {
      if (pb.has_property()) {
        auto& pt = pb.property();
        if (pt.has_key()) {
          auto name = pt.key().name();
          getter_ =
              create_edge_property_path_accessor(graph, name, ctx, tag, type_);
        } else if (pt.has_label()) {
          getter_ = create_edge_label_path_accessor(ctx, tag);
        } else {
          LOG(FATAL) << "parse failed for " << pt.DebugString();
        }
      } else {
        getter_ = std::make_shared<EdgeIdPathAccessor>(ctx, tag);
      }
    } else if (ctx.get(tag)->column_type() == ContextColumnType::kPath) {
      if (pb.has_property()) {
        auto& pt = pb.property();
        if (pt.has_len()) {
          getter_ = std::make_shared<PathLenPathAccessor>(ctx, tag);
        } else {
          LOG(FATAL) << "not support for path column - " << pt.DebugString();
        }
      } else {
        getter_ = std::make_shared<PathIdPathAccessor>(ctx, tag);
      }
    } else {
      LOG(FATAL) << "not support for " << ctx.get(tag)->column_info();
    }
  } else {
    if (var_type == VarType::kVertexVar) {
      if (pb.has_property()) {
        auto& pt = pb.property();
        if (pt.has_id()) {
          getter_ = std::make_shared<VertexGIdVertexAccessor>();
        } else if (pt.has_key()) {
          if (pt.key().name() == "id") {
            if (type_ == RTAnyType::kStringValue) {
              getter_ =
                  std::make_shared<VertexIdVertexAccessor<std::string_view>>(
                      graph);
            } else if (type_ == RTAnyType::kI32Value) {
              getter_ =
                  std::make_shared<VertexIdVertexAccessor<int32_t>>(graph);
            } else if (type_ == RTAnyType::kI64Value) {
              getter_ =
                  std::make_shared<VertexIdVertexAccessor<int64_t>>(graph);
            } else {
              LOG(FATAL) << "not support for " << static_cast<int>(type_);
            }
          } else {
            getter_ = create_vertex_property_vertex_accessor(graph, type_,
                                                             pt.key().name());
          }
        } else if (pt.has_label()) {
          getter_ = std::make_shared<VertexLabelVertexAccessor>();
        } else {
          LOG(FATAL) << "not support for " << pt.DebugString();
        }
      } else {
        getter_ = std::make_shared<VertexIdVertexAccessor<int64_t>>(graph);
      }
    } else if (var_type == VarType::kEdgeVar) {
      if (pb.has_property()) {
        auto& pt = pb.property();
        if (pt.has_key()) {
          auto name = pt.key().name();
          getter_ = create_edge_property_edge_accessor(graph, name, type_);
        } else {
          LOG(FATAL) << "parse failed for " << pt.DebugString();
        }
      } else {
        LOG(FATAL) << "not support" << pb.DebugString();
      }
    } else {
      LOG(FATAL) << "not support for " << pb.DebugString();
    }
  }
}

Var::~Var() {}

RTAny Var::get(size_t path_idx) const { return getter_->eval_path(path_idx); }

RTAny Var::get(size_t path_idx, int) const {
  return getter_->eval_path(path_idx, 0);
}

RTAny Var::get_vertex(label_t label, vid_t v, size_t idx) const {
  return getter_->eval_vertex(label, v, idx);
}

RTAny Var::get_vertex(label_t label, vid_t v, size_t idx, int) const {
  return getter_->eval_vertex(label, v, idx, 0);
}

RTAny Var::get_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                    const Any& data, size_t idx) const {
  return getter_->eval_edge(label, src, dst, data, idx);
}

RTAny Var::get_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                    const Any& data, size_t idx, int) const {
  return getter_->eval_edge(label, src, dst, data, idx, 0);
}

RTAnyType Var::type() const { return type_; }

std::shared_ptr<IContextColumnBuilder> Var::builder() const {
  return getter_->builder();
}

}  // namespace runtime

}  // namespace gs
