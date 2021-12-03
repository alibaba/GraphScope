/**
 * Copyright 2020 Alibaba Group Holding Limited.
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
#include "arrow/type.h"

#include "property_graph_stream.h"

namespace vineyard {

namespace detail {

std::shared_ptr<arrow::DataType> PropertyTypeToDataType(
    ::PropertyType type) {
  static const std::map<::PropertyType, std::shared_ptr<arrow::DataType>>
      parse_type_dict = {
          {BOOL, arrow::boolean()},   {CHAR, arrow::int8()},
          {SHORT, arrow::int16()},    {INT, arrow::int32()},
          {LONG, arrow::int64()},     {FLOAT, arrow::float32()},
          {DOUBLE, arrow::float64()}, {STRING, arrow::large_utf8()},
      };
  auto iter = parse_type_dict.find(type);
  if (iter != parse_type_dict.end()) {
    return iter->second;
  } else {
    throw std::runtime_error("Not supported property type: " +
                             std::to_string(type));
  }
}

::PropertyType PropertyTypeFromDataType(std::shared_ptr<arrow::DataType> const &type) {
  switch (type->id()) {
    case arrow::Type::BOOL:
      return BOOL;
    case arrow::Type::INT8:
      return CHAR;
    case arrow::Type::INT16:
      return SHORT;
    case arrow::Type::INT32:
      return INT;
    case arrow::Type::INT64:
      return LONG;
    case arrow::Type::FLOAT:
      return FLOAT;
    case arrow::Type::DOUBLE:
      return DOUBLE;
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      return STRING;
    case arrow::Type::BINARY:
      return BYTES;
    default:
      LOG(ERROR) << "Unknown arrow data type: " << type->ToString();
      return INVALID;
  }
}

static std::shared_ptr<arrow::Field> PropertyToField(
    Entry::PropertyDef const& prop) {
  return arrow::field(prop.name, prop.type);
}

static std::shared_ptr<arrow::Schema> ToArrowSchema(
    Entry const& entry) {
  auto kv = std::make_shared<arrow::KeyValueMetadata>();
  kv->Append("type", entry.type);
  kv->Append("label", entry.label);
  kv->Append("label_id", std::to_string(entry.id));
  if (entry.type == "VERTEX") {
    kv->Append("id_column", "0");
  } else {
    kv->Append("src_column", "0");
    kv->Append("dst_column", "1");
  }
  kv->Append("label_name", entry.label);
  kv->Append("label_index", std::to_string(entry.id));
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (auto const& prop : entry.props_) {
    LOG(INFO) << "prop.id = " << prop.id << ", " << prop.name << " -> " << prop.type;
    fields.emplace_back(PropertyToField(prop));
  }
  return std::make_shared<arrow::Schema>(fields, kv);
}

PropertyTableAppender::PropertyTableAppender(
    std::shared_ptr<arrow::Schema> schema) {
  for (const auto& field : schema->fields()) {
    std::shared_ptr<arrow::DataType> type = field->type();
    if (type == arrow::boolean()) {
      funcs_.push_back(AppendProperty<bool>::append);
    } else if (type == arrow::int8()) {
      funcs_.push_back(AppendProperty<char>::append);
    } else if (type == arrow::int16()) {
      funcs_.push_back(AppendProperty<int16_t>::append);
    } else if (type == arrow::int32()) {
      funcs_.push_back(AppendProperty<int32_t>::append);
    } else if (type == arrow::int64()) {
      funcs_.push_back(AppendProperty<int64_t>::append);
    } else if (type == arrow::float32()) {
      funcs_.push_back(AppendProperty<float>::append);
    } else if (type == arrow::float64()) {
      funcs_.push_back(AppendProperty<double>::append);
    } else if (type == arrow::large_utf8()) {
      funcs_.push_back(AppendProperty<std::string>::append);
    } else if (type->id() == arrow::Type::TIMESTAMP) {
      funcs_.push_back(AppendProperty<arrow::TimestampType>::append);
    } else if (type == arrow::null()) {
      funcs_.push_back(AppendProperty<void>::append);
    } else {
      LOG(FATAL) << "Datatype [" << type->ToString() << "] not implemented...";
    }
  }
  col_num_ = funcs_.size();
}

void PropertyTableAppender::Apply(
    std::unique_ptr<arrow::RecordBatchBuilder>& builder, VertexId id,
    size_t property_size, Property* properties,
    std::map<int, int> const& property_id_mapping,
    std::shared_ptr<arrow::RecordBatch>& batch_out) {
  std::set<int> prop_ids, processed;
  processed.emplace(0);
  generic_appender<VertexId>(builder->GetField(0), id);
  for (size_t i = 0; i < property_size; ++i) {
#ifndef NDEBUG
    LOG(INFO) << "vertex property: " << i << " -> id = " << properties[i].id;
#endif
    if (prop_ids.find(properties[i].id) != prop_ids.end()) {
#ifndef NDEBUG
      LOG(WARNING) << "detect duplicate vertex property id, ignored: "
                   << properties[i].id;
#endif
      continue;
    }
    prop_ids.emplace(properties[i].id);
    int index = property_id_mapping.at(properties[i].id);
    processed.emplace(index);
#ifndef NDEBUG
    LOG(INFO) << "vertex properties: i = " << i
              << ", type = " << properties[i].type
              << ", len = " << properties[i].len
              << ", prop_id = " << properties[i].id;
#endif
    funcs_[index](builder->GetField(index), properties + i);
  }
  for (size_t i = 0; i < builder->num_fields(); ++i) {
    // fill a null value for missing values to make sure columns
    // has the same length before being finalized.
    if (processed.find(i) == processed.end()) {
      builder->GetField(i)->AppendNull();
    }
  }
  if (builder->GetField(0)->length() == builder->initial_capacity()) {
    CHECK_ARROW_ERROR(builder->Flush(&batch_out));
  }
}

void PropertyTableAppender::Apply(
    std::unique_ptr<arrow::RecordBatchBuilder>& builder, EdgeId edge_id,
    VertexId src_id, VertexId dst_id, LabelId src_label, LabelId dst_label,
    size_t property_size, Property* properties,
    std::map<int, int> const& property_id_mapping,
    std::shared_ptr<arrow::RecordBatch>& batch_out) {
  std::set<int> prop_ids, processed;
  processed.emplace(0);
  processed.emplace(1);
  generic_appender<VertexId>(builder->GetField(0), src_id);
  generic_appender<VertexId>(builder->GetField(1), dst_id);
  for (size_t i = 0; i < property_size; ++i) {
#ifndef NDEBUG
    LOG(INFO) << "edge property: " << i << " -> id = " << properties[i].id;
#endif
    if (prop_ids.find(properties[i].id) != prop_ids.end()) {
#ifndef NDEBUG
      LOG(WARNING) << "detect duplicate edge property id, ignored: "
                   << properties[i].id;
#endif
      continue;
    }
    prop_ids.emplace(properties[i].id);
    int index = property_id_mapping.at(properties[i].id);
    processed.emplace(index);
#ifndef NDEBUG
    LOG(INFO) << "edge properties: i = "
              << i << ", type = "
              << properties[i].type
              << ", len = "
              << properties[i].len
              << ", prop_id = " << properties[i].id;
#endif
    funcs_[index](builder->GetField(index), properties + i);
  }
  for (size_t i = 0; i < builder->num_fields(); ++i) {
    // fill a null value for missing values to make sure columns
    // has the same length before being finalized.
    if (processed.find(i) == processed.end()) {
      builder->GetField(i)->AppendNull();
    }
  }
  if (builder->GetField(0)->length() == builder->initial_capacity()) {
    CHECK_ARROW_ERROR(builder->Flush(&batch_out));
  }
}

void PropertyTableAppender::Flush(
    std::unique_ptr<arrow::RecordBatchBuilder>& builder,
    std::shared_ptr<arrow::RecordBatch>& batches_out, bool allow_empty) {
  if (allow_empty || builder->GetField(0)->length() != 0) {
    CHECK_ARROW_ERROR(builder->Flush(&batches_out));
  } else {
    batches_out = nullptr;
  }
}

}  // namespace detail

void PropertyGraphOutStream::AddVertex(VertexId id, LabelId labelid,
                                       size_t property_size,
                                       Property* properties) {
#ifndef NDEBUG
  LOG(INFO) << "add vertex: id = " << id
            << ", labelid = " << labelid
            << ", property_size = " << property_size;
#endif
  auto& builder = vertex_builders_[labelid];
  auto& appender = vertex_appenders_[labelid];
  VINEYARD_ASSERT(builder != nullptr && appender != nullptr);
  std::shared_ptr<arrow::RecordBatch> batch_chunk = nullptr;
  appender->Apply(builder, id, property_size, properties,
                  vertex_property_id_mapping_[labelid], batch_chunk);
  if (batch_chunk != nullptr && vertex_primary_key_column_[labelid] != kNoPrimaryKeyColumn) {
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
    ARROW_OK_OR_RAISE(
      batch_chunk->RemoveColumn(vertex_primary_key_column_[labelid], &batch_chunk));
#else
    CHECK_ARROW_ERROR_AND_ASSIGN(batch_chunk,
      batch_chunk->RemoveColumn(vertex_primary_key_column_[labelid]));
#endif
  }
  this->buildTableChunk(batch_chunk, vertex_stream_, vertex_writer_, 1,
                        vertex_property_id_mapping_[labelid]);
}

void PropertyGraphOutStream::AddEdge(EdgeId edge_id, VertexId src_id,
                                     VertexId dst_id, LabelId label,
                                     LabelId src_label, LabelId dst_label,
                                     size_t property_size,
                                     Property* properties) {
#ifndef NDEBUG
  LOG(INFO) << "add edge: id = " << edge_id
            << ", labelid = " << label
            << ", property_size = " << property_size;
#endif
  auto src_dst_key = std::make_pair(src_label, dst_label);
  if (edge_builders_[label][src_dst_key] == nullptr) {
    std::shared_ptr<arrow::KeyValueMetadata> metadata;
    if (edge_schemas_[label]->metadata() != nullptr) {
      metadata = edge_schemas_[label]->metadata()->Copy();
    } else {
      metadata.reset(new arrow::KeyValueMetadata());
    }
    metadata->Append("src_label_id", std::to_string(src_label));
    metadata->Append("src_label", graph_schema_->GetLabelName(src_label));
    metadata->Append("dst_label_id", std::to_string(dst_label));
    metadata->Append("dst_label", graph_schema_->GetLabelName(dst_label));
    auto schema = edge_schemas_[label]->WithMetadata(metadata);

    std::unique_ptr<arrow::RecordBatchBuilder> builder = nullptr;
    CHECK_ARROW_ERROR(arrow::RecordBatchBuilder::Make(
        schema, arrow::default_memory_pool(), 10240, &builder));
    edge_builders_[label][src_dst_key].reset(builder.release());
  }
  auto &builder = edge_builders_[label][src_dst_key];
  auto &appender = edge_appenders_[label];
  VINEYARD_ASSERT(appender != nullptr, "edge label = " + std::to_string(label));
  std::shared_ptr<arrow::RecordBatch> batch_chunk = nullptr;
  appender->Apply(builder, edge_id, src_id, dst_id, src_label, dst_label,
                  property_size, properties,
                  edge_property_id_mapping_[label], batch_chunk);
  this->buildTableChunk(batch_chunk, edge_stream_, edge_writer_, 2,
                        edge_property_id_mapping_[label]);
}

void PropertyGraphOutStream::AddVertices(size_t vertex_size, VertexId* ids,
                                         LabelId* labelids,
                                         size_t* property_sizes,
                                         Property* properties) {
  size_t cumsum = 0;
  for (size_t idx = 0; idx < vertex_size; ++idx) {
    AddVertex(ids[idx], labelids[idx], property_sizes[idx],
              properties + cumsum);
    cumsum += property_sizes[idx];
  }
}

void PropertyGraphOutStream::AddEdges(size_t edge_size, EdgeId* edge_ids,
                                      VertexId* src_ids, VertexId* dst_ids,
                                      LabelId* labels, LabelId* src_labels,
                                      LabelId* dst_labels,
                                      size_t* property_sizes,
                                      Property* properties) {
  size_t cumsum = 0;
  for (size_t idx = 0; idx < edge_size; ++idx) {
    AddEdge(edge_ids[idx], src_ids[idx], dst_ids[idx], labels[idx],
            src_labels[idx], dst_labels[idx], property_sizes[idx],
            properties + cumsum);
    cumsum += property_sizes[idx];
  }
}

Status PropertyGraphOutStream::Abort() {
  VINEYARD_CHECK_OK(vertex_writer_->Abort());
  VINEYARD_CHECK_OK(edge_writer_->Abort());
  return Status::OK();
}

Status PropertyGraphOutStream::Finish() {
  FinishAllVertices();
  FinishAllEdges();
  return Status::OK();
}

void PropertyGraphOutStream::initialTables() {
  // c.f.: https://yuque.antfin-inc.com/7br/graphscope/wwqldo#d774a8ef
  for (auto const& entry : graph_schema_->VertexEntries()) {
    auto schema = detail::ToArrowSchema(entry);

    auto vertex_id_type = ConvertToArrowType<VertexId>::TypeValue();
    auto edge_id_type = ConvertToArrowType<EdgeId>::TypeValue();
    auto label_id_type = ConvertToArrowType<LabelId>::TypeValue();

    VINEYARD_ASSERT(entry.type == "VERTEX");

    std::string field_name = "__vertex_id__";

    if (!entry.primary_keys.empty()) {
      field_name = entry.primary_keys[0];
    }
    vertex_primary_key_column_[entry.id] = kNoPrimaryKeyColumn;

    for (size_t idx = 0; idx < entry.props_.size(); ++idx) {
#ifndef NDEBUG
      LOG(INFO) << "vertex prop id mapping: entry.label = " << entry.label
                << ", entry.id = " << entry.id
                << ", mapping prop " << entry.props_[idx].id
                << " to " << (1 + idx);
#endif
      vertex_property_id_mapping_[entry.id].emplace(entry.props_[idx].id,
                                                    1 + idx);
      if (!entry.primary_keys.empty()) {
        if (entry.primary_keys[0] == entry.label) {
          LOG(INFO) << "Found primary key column in props: " << entry.label;
          vertex_primary_key_column_[entry.id] = 1 + idx;
        }
      }
    }

#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
    CHECK_ARROW_ERROR(
        schema->AddField(0, arrow::field(field_name, vertex_id_type), &schema));
#else
    CHECK_ARROW_ERROR_AND_ASSIGN(schema,
        schema->AddField(0, arrow::field(field_name, vertex_id_type)));
#endif

    std::unique_ptr<arrow::RecordBatchBuilder> builder = nullptr;
    CHECK_ARROW_ERROR(arrow::RecordBatchBuilder::Make(
        schema, arrow::default_memory_pool(), 10240, &builder));
    vertex_builders_.emplace(entry.id, std::move(builder));
    vertex_schemas_.emplace(entry.id, schema);
    vertex_appenders_.emplace(
        entry.id, std::make_shared<detail::PropertyTableAppender>(schema));
  }
  for (auto const& entry : graph_schema_->EdgeEntries()) {
    auto schema = detail::ToArrowSchema(entry);

    auto vertex_id_type = ConvertToArrowType<VertexId>::TypeValue();
    auto edge_id_type = ConvertToArrowType<EdgeId>::TypeValue();
    auto label_id_type = ConvertToArrowType<LabelId>::TypeValue();

    VINEYARD_ASSERT(entry.type == "EDGE");
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
    CHECK_ARROW_ERROR(
        schema->AddField(0, arrow::field("__src_id__", vertex_id_type), &schema));
    CHECK_ARROW_ERROR(
        schema->AddField(1, arrow::field("__dst_id__", vertex_id_type), &schema));
#else
    CHECK_ARROW_ERROR_AND_ASSIGN(schema,
        schema->AddField(0, arrow::field("__src_id__", vertex_id_type)));
    CHECK_ARROW_ERROR_AND_ASSIGN(schema,
        schema->AddField(1, arrow::field("__dst_id__", vertex_id_type)));
#endif
    for (size_t idx = 0; idx < entry.props_.size(); ++idx) {
#ifndef NDEBUG
      LOG(INFO) << "edge prop id mapping: entry.label = " << entry.label
                << ", entry.id = " << entry.id
                << ", mapping prop " << entry.props_[idx].id
                << " to " << (2 + idx);
#endif
      edge_property_id_mapping_[entry.id].emplace(entry.props_[idx].id,
                                                  2 + idx);
      for (auto const &rel: entry.relations) {
        auto src_label = graph_schema_->GetLabelId(rel.first);
        auto dst_label = graph_schema_->GetLabelId(rel.second);
        auto src_dst_key = std::make_pair(src_label, dst_label);

        std::shared_ptr<arrow::KeyValueMetadata> metadata;
        if (schema->metadata() != nullptr) {
          metadata = schema->metadata()->Copy();
        } else {
          metadata.reset(new arrow::KeyValueMetadata());
        }
        metadata->Append("src_label_id", std::to_string(src_label));
        metadata->Append("src_label", rel.first);
        metadata->Append("dst_label_id", std::to_string(dst_label));
        metadata->Append("dst_label", rel.second);
        auto subschema = schema->WithMetadata(metadata);

        std::unique_ptr<arrow::RecordBatchBuilder> builder = nullptr;
        CHECK_ARROW_ERROR(arrow::RecordBatchBuilder::Make(
            subschema, arrow::default_memory_pool(), 10240, &builder));
        edge_builders_[entry.id][src_dst_key].reset(builder.release());
      }
    }
    edge_schemas_.emplace(entry.id, schema);
    edge_appenders_.emplace(
        entry.id, std::make_shared<detail::PropertyTableAppender>(schema));
  }
}

void PropertyGraphOutStream::buildTableChunk(
    std::shared_ptr<arrow::RecordBatch> batch,
    std::shared_ptr<vineyard::DataframeStream> &output_stream,
    std::unique_ptr<vineyard::DataframeStreamWriter>& stream_writer,
    int const property_offset,
    std::map<int, int> const& property_id_mapping) {
#ifndef NDEBUG
  LOG(INFO) << "buildTableChunk: batch is " << batch;
#endif
  if (batch == nullptr) {
    return;
  }

  if (stream_writer == nullptr) {
    VINEYARD_CHECK_OK(this->Open(output_stream, stream_writer));
  }

#ifndef NDEBUG
  LOG(INFO) << "chunk schema: batch is " << batch->schema()->ToString();
#endif
  auto status = stream_writer->WriteBatch(batch);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to write recordbatch to stream: " << status.ToString();
  }
}

void PropertyGraphOutStream::FinishAllVertices() {
  if (vertex_finished_) {
    return;
  }
  for (auto& vertices : vertex_builders_) {
    auto &builder = vertices.second;
    auto appender = vertex_appenders_[vertices.first];
    VINEYARD_ASSERT(builder != nullptr && appender != nullptr);
    std::shared_ptr<arrow::RecordBatch> batch = nullptr;
    appender->Flush(builder, batch, true);
#ifndef NDEBUG
    LOG(INFO) << "finish vertices: " << batch;
#endif
    if (batch != nullptr && vertex_primary_key_column_[vertices.first] != kNoPrimaryKeyColumn) {
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
      ARROW_OK_OR_RAISE(
        batch->RemoveColumn(vertex_primary_key_column_[vertices.first], &batch));
#else
      CHECK_ARROW_ERROR_AND_ASSIGN(batch,
        batch->RemoveColumn(vertex_primary_key_column_[vertices.first]));
#endif
    }
    buildTableChunk(batch, vertex_stream_, vertex_writer_, 1,
                    vertex_property_id_mapping_[vertices.first]);
  }
  if (!vertex_writer_) {
    VINEYARD_CHECK_OK(Open(vertex_stream_, vertex_writer_));
  }
  VINEYARD_CHECK_OK(vertex_writer_->Finish());
  vertex_finished_ = true;
}

void PropertyGraphOutStream::FinishAllEdges() {
  if (edge_finished_) {
    return;
  }
  for (auto& edges : edge_builders_) {
    for (auto &subedges: edges.second) {
      auto &builder = subedges.second;
      auto appender = edge_appenders_[edges.first];
      VINEYARD_ASSERT(builder != nullptr && appender != nullptr);
      std::shared_ptr<arrow::RecordBatch> batch = nullptr;
      appender->Flush(builder, batch, true);
#ifndef NDEBUG
    LOG(INFO) << "finish edges: " << batch;
#endif
      buildTableChunk(batch, edge_stream_, edge_writer_, 2,
                      edge_property_id_mapping_[edges.first]);
    }
  }
  if (!edge_writer_) {
    VINEYARD_CHECK_OK(Open(edge_stream_, edge_writer_));
  }
  VINEYARD_CHECK_OK(edge_writer_->Finish());
  edge_finished_ = true;
}

void GlobalPGStream::Construct(const ObjectMeta& meta) {
  std::string __type_name = type_name<GlobalPGStream>();
  CHECK(meta.GetTypeName() == __type_name);
  this->meta_ = meta;
  this->id_ = meta.GetId();

  meta.GetKeyValue("total_stream_chunks", total_stream_chunks_);
  for (size_t idx = 0; idx < total_stream_chunks_; ++idx) {
    std::string member_key = "stream_chunk_" + std::to_string(idx);
    if (meta.GetMemberMeta(member_key).IsLocal()) {
      local_streams_.emplace_back(
        std::dynamic_pointer_cast<PropertyGraphOutStream>(meta.GetMember(member_key)));
    }
  }
  LOG(INFO) << "local stream chunk size: " << local_streams_.size();
}

std::shared_ptr<Object> GlobalPGStreamBuilder::_Seal(Client& client) {
  VINEYARD_CHECK_OK(this->Build(client));

  auto gstream = std::make_shared<GlobalPGStream>();
  gstream->total_stream_chunks_ = total_stream_chunks_;
  gstream->meta_.SetTypeName(type_name<GlobalPGStream>());
  gstream->meta_.SetGlobal(true);
  gstream->meta_.AddKeyValue("total_stream_chunks", total_stream_chunks_);

  for (size_t idx = 0; idx < stream_chunks_.size(); ++idx) {
    gstream->meta_.AddMember("stream_chunk_" + std::to_string(idx),
                             stream_chunks_[idx]);
  }

  VINEYARD_CHECK_OK(client.CreateMetaData(gstream->meta_, gstream->id_));
  return std::dynamic_pointer_cast<Object>(gstream);
}

}  // namespace vineyard
