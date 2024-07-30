#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/accessors.h"
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {

namespace runtime {

Var::Var(const ReadTransaction& txn, const Context& ctx,
         const common::Variable& pb, VarType var_type)
    : getter_(nullptr) {
  int tag = -1;
  LOG(INFO) << "pb: " << pb.DebugString();
  //  CHECK(pb.has_node_type());
  type_ = RTAnyType::kUnknown;
  if (pb.has_node_type()) {
    type_ = parse_from_ir_data_type(pb.node_type());
  }

  if (type_ == RTAnyType::kUnknown) {
    if (pb.has_tag()) {
      tag = pb.tag().id();
      CHECK(ctx.get(tag) != nullptr);
      // ?
      type_ = ctx.get(tag)->elem_type();
    } else {
      LOG(FATAL) << "not support";
    }
  }
  LOG(INFO) << "type: " << static_cast<int>(type_.type_enum_);

  if (pb.has_tag() || var_type == VarType::kPathVar) {
    tag = pb.tag().id();
    LOG(INFO) << "tag: " << tag;
    LOG(INFO) << "column nullptr: " << (ctx.get(tag) == nullptr);
    if (ctx.get(tag)->column_type() == ContextColumnType::kVertex) {
      if (pb.has_property()) {
        auto& pt = pb.property();
        if (pt.has_id()) {
          getter_ = std::make_shared<VertexIdPathAccessor>(txn, ctx, tag);
        } else if (pt.has_key()) {
          if (pt.key().name() == "id") {
            getter_ = std::make_shared<VertexIdPathAccessor>(txn, ctx, tag);
          } else {
            getter_ = create_vertex_property_path_accessor(txn, ctx, tag, type_,
                                                           pt.key().name());
          }
        } else if (pt.has_label()) {
          getter_ = create_vertex_label_path_accessor(ctx, tag);
        } else {
          LOG(FATAL) << "xxx, " << pt.item_case();
        }
      } else {
        getter_ = std::make_shared<VertexPathAccessor>(ctx, tag);
      }
    } else if (ctx.get(tag)->column_type() == ContextColumnType::kValue ||
               ctx.get(tag)->column_type() ==
                   ContextColumnType::kOptionalValue) {
      getter_ = create_context_value_accessor(ctx, tag, type_);
    } else if (ctx.get(tag)->column_type() == ContextColumnType::kEdge) {
      if (pb.has_property()) {
        auto& pt = pb.property();
        if (pt.has_key()) {
          getter_ = create_edge_property_path_accessor(ctx, tag, type_);
        } else if (pt.has_label()) {
          getter_ = create_edge_label_path_accessor(ctx, tag);
        } else {
          LOG(FATAL) << "not support...";
        }
      } else {
        getter_ = std::make_shared<EdgeIdPathAccessor>(ctx, tag);
        // LOG(FATAL) << "not support for edge column - " << tag;
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
          getter_ = std::make_shared<VertexIdVertexAccessor>(txn);
        } else if (pt.has_key()) {
          if (pt.key().name() == "id") {
            getter_ = std::make_shared<VertexIdVertexAccessor>(txn);
          } else {
            getter_ = create_vertex_property_vertex_accessor(txn, type_,
                                                             pt.key().name());
          }
        } else {
          LOG(FATAL) << "xxx, " << pt.item_case();
        }
      } else {
        LOG(FATAL) << "not support";
      }
    } else if (var_type == VarType::kEdgeVar) {
      if (pb.has_property()) {
        auto& pt = pb.property();
        if (pt.has_key()) {
          getter_ = create_edge_property_edge_accessor(type_);
        } else {
          LOG(FATAL) << "not support";
        }
      } else {
        LOG(FATAL) << "not support";
      }
    } else {
      LOG(FATAL) << "not support";
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
