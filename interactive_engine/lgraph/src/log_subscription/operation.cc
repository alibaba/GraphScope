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

#include "lgraph/log_subscription/operation.h"
#include "lgraph/util/endian.h"

namespace LGRAPH_NAMESPACE {
namespace log_subscription {

int32_t PropertyInfo::GetAsInt32() const {
  Check(value_bytes_.size() == sizeof(int32_t), "Get int32 with wrong value bytes size!");
  return Endian::ToBigEndian(*reinterpret_cast<const int32_t *>(value_bytes_.data()));
}

int64_t PropertyInfo::GetAsInt64() const {
  Check(value_bytes_.size() == sizeof(int64_t), "Get int64 with wrong value bytes size!");
  return Endian::ToBigEndian(*reinterpret_cast<const int64_t *>(value_bytes_.data()));
}

float PropertyInfo::GetAsFloat() const {
  Check(value_bytes_.size() == sizeof(float), "Get float with wrong value bytes size!");
  return Endian::ToBigEndian(*reinterpret_cast<const float *>(value_bytes_.data()));
}

double PropertyInfo::GetAsDouble() const {
  Check(value_bytes_.size() == sizeof(double), "Get double with wrong value bytes size!");
  return Endian::ToBigEndian(*reinterpret_cast<const double *>(value_bytes_.data()));
}

const std::string &PropertyInfo::GetAsStr() const {
  return value_bytes_;
}

std::unordered_map<PropertyId, PropertyInfo> ExtractPropMap(DataOperationPb* data_op_proto) {
  std::unordered_map<PropertyId, PropertyInfo> prop_map;
  prop_map.reserve(data_op_proto->props_size());
  for (auto &prop_pb : *data_op_proto->mutable_props()) {
    prop_map.emplace(
        static_cast<PropertyId>(prop_pb.first),
        PropertyInfo{static_cast<DataType>(prop_pb.second.datatype()), std::move(*prop_pb.second.release_val())});
  }
  return prop_map;
}

VertexInsertInfo Operation::GetInfoAsVertexInsertOp() const {
  OpType op_type = GetOpType();
  assert(op_type == OpType::OVERWRITE_VERTEX || op_type == OpType::UPDATE_VERTEX);

  auto &data_op_bytes = op_proto_->databytes();
  DataOperationPb data_op_proto;
  Check(data_op_proto.ParseFromArray(data_op_bytes.data(), static_cast<int>(data_op_bytes.size())),
        "Parse DataOperationPb Failed!");

  VertexIdPb vertex_id_proto;
  Check(vertex_id_proto.ParseFromString(data_op_proto.keyblob()), "Parse VertexIdPb Failed!");
  LabelIdPb label_id_proto;
  Check(label_id_proto.ParseFromString(data_op_proto.locationblob()), "Parse LabelIdPb Failed!");
  auto prop_map = ExtractPropMap(&data_op_proto);
  return {static_cast<VertexId>(vertex_id_proto.id()), static_cast<LabelId>(label_id_proto.id()), std::move(prop_map)};
}

EdgeInsertInfo Operation::GetInfoAsEdgeInsertOp() const {
  OpType op_type = GetOpType();
  assert(op_type == OpType::OVERWRITE_EDGE || op_type == OpType::UPDATE_EDGE);

  auto &data_op_bytes = op_proto_->databytes();
  DataOperationPb data_op_proto;
  Check(data_op_proto.ParseFromArray(data_op_bytes.data(), static_cast<int>(data_op_bytes.size())),
        "Parse DataOperationPb Failed!");

  EdgeIdPb edge_id_proto;
  Check(edge_id_proto.ParseFromString(data_op_proto.keyblob()), "Parse EdgeIdPb Failed!");
  EdgeLocationPb edge_location_proto;
  Check(edge_location_proto.ParseFromString(data_op_proto.locationblob()), "Parse EdgeLocationPb Failed!");
  auto &edge_kind_proto = edge_location_proto.edgekind();
  bool forward = edge_location_proto.forward();
  auto prop_map = ExtractPropMap(&data_op_proto);
  return {
    static_cast<EdgeInnerId>(edge_id_proto.id()),
    static_cast<VertexId>(edge_id_proto.srcid().id()),
    static_cast<VertexId>(edge_id_proto.dstid().id()),
    static_cast<LabelId>(edge_kind_proto.edgelabelid().id()),
    static_cast<LabelId>(edge_kind_proto.srcvertexlabelid().id()),
    static_cast<LabelId>(edge_kind_proto.dstvertexlabelid().id()),
    forward, std::move(prop_map)
  };
}

}
}
