/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "lgraph/common/check.h"
#include "lgraph/common/types.h"
#include "lgraph/proto/model.pb.h"

namespace LGRAPH_NAMESPACE {
namespace log_subscription {

class PropertyInfo {
public:
  PropertyInfo(DataType type, std::string&& bytes) : data_type_(type), value_bytes_(std::move(bytes)) {}
  ~PropertyInfo() = default;

  PropertyInfo(const PropertyInfo &) = default;
  PropertyInfo(PropertyInfo &&) = default;
  PropertyInfo &operator=(const PropertyInfo &) = default;
  PropertyInfo &operator=(PropertyInfo &&) = default;

  DataType GetDataType() const {
    return data_type_;
  }

  const std::string &GetValueBytes() const {
    return value_bytes_;
  }

  std::string *GetMutValueBytes() {
    return &value_bytes_;
  }

  int32_t GetAsInt32() const;
  int64_t GetAsInt64() const;
  float GetAsFloat() const;
  double GetAsDouble() const;
  const std::string &GetAsStr() const;

private:
  DataType data_type_;
  std::string value_bytes_;
};

class VertexInsertInfo {
public:
  VertexInsertInfo(VertexId id, LabelId label_id, std::unordered_map<PropertyId, PropertyInfo> &&prop_map)
      : id_(id), label_id_(label_id), prop_map_(std::move(prop_map)) {}
  ~VertexInsertInfo() = default;

  VertexInsertInfo(const VertexInsertInfo &) = default;
  VertexInsertInfo(VertexInsertInfo &&) = default;
  VertexInsertInfo &operator=(const VertexInsertInfo &) = default;
  VertexInsertInfo &operator=(VertexInsertInfo &&) = default;

  VertexId GetVertexId() const {
    return id_;
  }

  LabelId GetLabelId() const {
    return label_id_;
  }

  const std::unordered_map<PropertyId, PropertyInfo> &GetPropMap() const {
    return prop_map_;
  }

  std::unordered_map<PropertyId, PropertyInfo> *GetMutPropMap() {
    return &prop_map_;
  }

  const PropertyInfo &GetPropInfo(PropertyId prop_id) const {
    return prop_map_.at(prop_id);
  }

  PropertyInfo *GetMutPropInfo(PropertyId prop_id) {
    return &prop_map_.at(prop_id);
  }

private:
  VertexId id_;
  LabelId label_id_;
  std::unordered_map<PropertyId, PropertyInfo> prop_map_;
};

class EdgeInsertInfo {
public:
  EdgeInsertInfo(EdgeInnerId edge_inner_id, VertexId src_id, VertexId dst_id,
                 LabelId edge_label_id, LabelId src_label_id, LabelId dst_label_id,
                 bool forward, std::unordered_map<PropertyId, PropertyInfo> &&prop_map)
      : edge_id_(edge_inner_id, src_id, dst_id)
      , edge_relation_(edge_label_id, src_label_id, dst_label_id)
      , forward_(forward), prop_map_(std::move(prop_map)) {}
  ~EdgeInsertInfo() = default;

  EdgeInsertInfo(const EdgeInsertInfo &) = default;
  EdgeInsertInfo(EdgeInsertInfo &&) = default;
  EdgeInsertInfo &operator=(const EdgeInsertInfo &) = default;
  EdgeInsertInfo &operator=(EdgeInsertInfo &&) = default;

  const EdgeId &GetEdgeId() const {
    return edge_id_;
  }

  const EdgeRelation &GetEdgeRelation() const {
    return edge_relation_;
  }

  bool IsForward() const {
    return forward_;
  }

  const std::unordered_map<PropertyId, PropertyInfo> &GetPropMap() const {
    return prop_map_;
  }

  std::unordered_map<PropertyId, PropertyInfo> *GetMutPropMap() {
    return &prop_map_;
  }

  const PropertyInfo &GetPropInfo(PropertyId prop_id) const {
    return prop_map_.at(prop_id);
  }

  PropertyInfo *GetMutPropInfo(PropertyId prop_id) {
    return &prop_map_.at(prop_id);
  }

private:
  EdgeId edge_id_;
  EdgeRelation edge_relation_;
  bool forward_;
  std::unordered_map<PropertyId, PropertyInfo> prop_map_;
};

class Operation {
public:
  explicit Operation(OperationPb *op_proto) : op_proto_(op_proto), op_owner_() {}
  Operation(OperationPb *op_proto, const std::shared_ptr<OperationBatchPb> &op_owner)
      : op_proto_(op_proto), op_owner_(op_owner) {}
  ~Operation() = default;

  Operation(const Operation &) = default;
  Operation &operator=(const Operation &) = default;
  Operation(Operation &&) noexcept = default;
  Operation &operator=(Operation &&) noexcept = default;

  OpType GetOpType() const {
    return static_cast<OpType>(op_proto_->optype());
  }

  VertexInsertInfo GetInfoAsVertexInsertOp() const;
  EdgeInsertInfo GetInfoAsEdgeInsertOp() const;

private:
  OperationPb *op_proto_;
  std::shared_ptr<OperationBatchPb> op_owner_;
};

}
}

