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
#ifndef SRC_CLIENT_DS_STREAM_PROPERTY_GRAPH_H_
#define SRC_CLIENT_DS_STREAM_PROPERTY_GRAPH_H_

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/config.h"
#include "arrow/util/logging.h"
#include "arrow/util/key_value_metadata.h"

#include "grape/worker/comm_spec.h"

#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/stream/dataframe_stream.h"
#include "vineyard/basic/stream/parallel_stream.h"
#include "vineyard/client/client.h"
#include "vineyard/client/ds/blob.h"
#include "vineyard/common/util/json.h"

#include "graph_schema.h"
#include "htap_types.h"
#include "graph_builder_ffi.h"

namespace vineyard {

namespace detail {

std::shared_ptr<arrow::DataType> PropertyTypeToDataType(::PropertyType type);
::PropertyType PropertyTypeFromDataType(std::shared_ptr<arrow::DataType> const &type);

template <typename T>
struct AppendProperty {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    LOG(FATAL) << "Unimplemented...";
  }
};

template <>
struct AppendProperty<bool> {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    vineyard::htap_types::PodProperties pp;
    pp.long_value = prop->len;
    CHECK_ARROW_ERROR(
        dynamic_cast<arrow::BooleanBuilder*>(builder)->Append(pp.bool_value));
  }
};

template <>
struct AppendProperty<char> {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    vineyard::htap_types::PodProperties pp;
    pp.long_value = prop->len;
    CHECK_ARROW_ERROR(
        dynamic_cast<arrow::Int8Builder*>(builder)->Append(pp.char_value));
  }
};

template <>
struct AppendProperty<int16_t> {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    vineyard::htap_types::PodProperties pp;
    pp.long_value = prop->len;
    CHECK_ARROW_ERROR(
        dynamic_cast<arrow::Int16Builder*>(builder)->Append(pp.int16_value));
  }
};

template <>
struct AppendProperty<int32_t> {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    vineyard::htap_types::PodProperties pp;
    pp.long_value = prop->len;
    CHECK_ARROW_ERROR(
        dynamic_cast<arrow::Int32Builder*>(builder)->Append(pp.int_value));
  }
};

template <>
struct AppendProperty<int64_t> {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    CHECK_ARROW_ERROR(
        dynamic_cast<arrow::Int64Builder*>(builder)->Append(prop->len));
  }
};

template <>
struct AppendProperty<float> {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    vineyard::htap_types::PodProperties pp;
    pp.long_value = prop->len;
    CHECK_ARROW_ERROR(
        dynamic_cast<arrow::FloatBuilder*>(builder)->Append(pp.float_value));
  }
};

template <>
struct AppendProperty<double> {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    vineyard::htap_types::PodProperties pp;
    pp.long_value = prop->len;
    CHECK_ARROW_ERROR(
        dynamic_cast<arrow::DoubleBuilder*>(builder)->Append(pp.double_value));
  }
};

template <>
struct AppendProperty<std::string> {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    CHECK_ARROW_ERROR(dynamic_cast<arrow::LargeStringBuilder*>(builder)->Append(
        static_cast<uint8_t*>(prop->data), prop->len));
  }
};

template <>
struct AppendProperty<void> {
  static void append(arrow::ArrayBuilder* builder, Property const* prop) {
    CHECK_ARROW_ERROR(
        dynamic_cast<arrow::NullBuilder*>(builder)->Append(nullptr));
  }
};

template <typename T>
void generic_appender(arrow::ArrayBuilder* builder, T const& value) {
  CHECK_ARROW_ERROR(
      dynamic_cast<typename ConvertToArrowType<T>::BuilderType*>(builder)
          ->Append(value));
}

using property_appender_func = void (*)(arrow::ArrayBuilder*,
                                        Property const* prop);

class PropertyTableAppender {
 public:
  explicit PropertyTableAppender(std::shared_ptr<arrow::Schema> schema);

  // apply for vertex values and properties.
  void Apply(std::unique_ptr<arrow::RecordBatchBuilder>& builder, VertexId id,
             size_t property_size, Property* properties,
             std::map<int, int> const& property_id_mapping,
             std::shared_ptr<arrow::RecordBatch>& batch_out);

  // apply for edge values and properties.
  void Apply(std::unique_ptr<arrow::RecordBatchBuilder>& builder,
             EdgeId edge_id, VertexId src_id, VertexId dst_id,
             LabelId src_label, LabelId dst_label, size_t property_size,
             Property* properties,
             std::map<int, int> const& property_id_mapping,
             std::shared_ptr<arrow::RecordBatch>& batch_out);

  void Flush(std::unique_ptr<arrow::RecordBatchBuilder>& builder,
             std::shared_ptr<arrow::RecordBatch>& batches_out,
             const bool allow_empty = false);

 private:
  std::vector<property_appender_func> funcs_;
  size_t col_num_;
};

}  // namespace detail

class PropertyGraphInStream;

class PropertyGraphOutStream : public Registered<PropertyGraphOutStream> {
 public:
  const uint64_t instance_id() const {
    return meta_.GetClient()->instance_id();
  }

  static std::unique_ptr<PropertyGraphOutStream> Create(
      Client& client, const char* graph_name, MGPropertyGraphSchema* schema,
      const int index) {
    auto s = std::unique_ptr<PropertyGraphOutStream>(new PropertyGraphOutStream());

    // take ownership of the `MGPropertyGraphSchema` object.
    s->graph_schema_ = std::shared_ptr<MGPropertyGraphSchema>(schema);

    // create two streams for vertices and edges
    {
      vineyard::DataframeStreamBuilder builder(client);
      builder.SetParam("kind", "vertex");
      builder.SetParam("graph_name", graph_name);
      s->vertex_stream_ =
        std::dynamic_pointer_cast<vineyard::DataframeStream>(builder.Seal(client));
      client.Persist(s->vertex_stream_->id());
      // Don't "OpenWriter" when creating, it will be "Get and Construct" again
      // VINEYARD_CHECK_OK(s->vertex_stream_->OpenWriter(client, s->vertex_writer_));
    }
    {
      vineyard::DataframeStreamBuilder builder(client);
      builder.SetParam("kind", "edge");
      builder.SetParam("graph_name", graph_name);
      s->edge_stream_ =
        std::dynamic_pointer_cast<vineyard::DataframeStream>(builder.Seal(client));
      client.Persist(s->edge_stream_->id());
      // Don't "OpenWriter" when creating, it will be "Get and Construct" again
      // VINEYARD_CHECK_OK(s->edge_stream_->OpenWriter(client, s->edge_writer_));
    }

    s->stream_index_ = index;

    s->meta_.SetTypeName(type_name<PropertyGraphOutStream>());
    s->meta_.AddKeyValue("graph_name", std::string(graph_name));
    s->meta_.AddKeyValue("stream_index", s->stream_index_);
    s->meta_.AddKeyValue("graph_schema", s->graph_schema_->ToJSONString());
    s->meta_.AddMember("vertex_stream", s->vertex_stream_->meta());
    s->meta_.AddMember("edge_stream", s->edge_stream_->meta());
    s->initialTables();

    VINEYARD_CHECK_OK(client.CreateMetaData(s->meta_, s->id_));
    return s;
  }

  static std::unique_ptr<Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<Object>(
        std::unique_ptr<PropertyGraphOutStream>(new PropertyGraphOutStream()));
  }

  void Construct(const ObjectMeta& meta) override {
    meta_ = meta;
    this->id_ = ObjectIDFromString(meta.GetKeyValue("id"));
    this->stream_index_ = meta.GetKeyValue<int>("stream_index");
    this->vertex_stream_ =
      std::dynamic_pointer_cast<vineyard::DataframeStream>(meta.GetMember("vertex_stream"));
    this->edge_stream_ =
      std::dynamic_pointer_cast<vineyard::DataframeStream>(meta.GetMember("edge_stream"));

    this->graph_schema_ = std::make_shared<MGPropertyGraphSchema>();
    graph_schema_->FromJSONString(meta.GetKeyValue("graph_schema"));
    this->initialTables();
  }

  Status Open(std::shared_ptr<vineyard::DataframeStream> &output_stream,
              std::unique_ptr<vineyard::DataframeStreamWriter> &writer) {
    auto client = dynamic_cast<vineyard::Client *>(meta_.GetClient());
    auto status = output_stream->OpenWriter(*client, writer);
    if (!status.ok()) {
      LOG(INFO) << "Failed to open writer for stream: " << status.ToString();
    }
    return status;
  }

  Status GetNext(size_t const size,
                 std::unique_ptr<arrow::MutableBuffer>& buffer,
                 std::shared_ptr<vineyard::DataframeStreamWriter>& stream_writer) {
    return stream_writer->GetNext(size, buffer);
  }

  void AddVertex(VertexId id, LabelId labelid, size_t property_size,
                 Property* properties);

  void AddEdge(EdgeId edge_id, VertexId src_id, VertexId dst_id, LabelId label,
               LabelId src_label, LabelId dst_label, size_t property_size,
               Property* properties);

  void AddVertices(size_t vertex_size, VertexId* ids, LabelId* labelids,
                   size_t* property_sizes, Property* properties);

  void AddEdges(size_t edge_size, EdgeId* edge_ids, VertexId* src_ids,
                VertexId* dst_ids, LabelId* labels, LabelId* src_labels,
                LabelId* dst_labels, size_t* property_sizes,
                Property* properties);

  Status Abort();

  Status Finish();
  void FinishAllVertices();
  void FinishAllEdges();

  int stream_index() const { return stream_index_; }

 private:
  void initialTables();
  void buildTableChunk(std::shared_ptr<arrow::RecordBatch> batch,
                       std::shared_ptr<vineyard::DataframeStream> &output_stream,
                       std::unique_ptr<vineyard::DataframeStreamWriter>& stream_writer,
                       int const property_offset,
                       std::map<int, int> const& property_id_mapping);

  std::shared_ptr<MGPropertyGraphSchema> graph_schema_;

  // record the mapping between property_id and table column index
  std::map<LabelId, std::map<int, int>> vertex_property_id_mapping_;
  std::map<LabelId, std::map<int, int>> edge_property_id_mapping_;

  std::map<LabelId, std::unique_ptr<arrow::RecordBatchBuilder>>
      vertex_builders_;
  // vertex label id to its primary key column (assuming only signle column key) ordinal mapping
  // -1 means no primary key column
  static constexpr size_t kNoPrimaryKeyColumn = static_cast<size_t>(-1);
  std::map<LabelId, size_t> vertex_primary_key_column_;
  std::map<LabelId, std::shared_ptr<detail::PropertyTableAppender>>
      vertex_appenders_;
  std::map<LabelId, std::map<std::pair<LabelId, LabelId>,
                             std::unique_ptr<arrow::RecordBatchBuilder>>>
      edge_builders_;
  std::map<LabelId, std::shared_ptr<detail::PropertyTableAppender>>
      edge_appenders_;

  std::map<LabelId, std::shared_ptr<arrow::Schema>> vertex_schemas_;
  std::map<LabelId, std::shared_ptr<arrow::Schema>> edge_schemas_;

  bool vertex_finished_ = false;
  bool edge_finished_ = false;
  int stream_index_;
  std::shared_ptr<vineyard::DataframeStream> vertex_stream_;
  std::shared_ptr<vineyard::DataframeStream> edge_stream_;
  std::unique_ptr<vineyard::DataframeStreamWriter> vertex_writer_;
  std::unique_ptr<vineyard::DataframeStreamWriter> edge_writer_;

  friend class PropertyGraphInStream;
};

class PropertyGraphInStream {
 public:
  explicit PropertyGraphInStream(vineyard::Client &client, PropertyGraphOutStream& stream)
      : vertex_stream_(stream.vertex_stream_),
        edge_stream_(stream.edge_stream_),
        graph_schema_(stream.graph_schema_) {
    VINEYARD_CHECK_OK(vertex_stream_->OpenReader(client, vertex_reader_));
    VINEYARD_CHECK_OK(edge_stream_->OpenReader(client, edge_reader_));
  }

  Status GetNextVertices(Client& client,
                         std::shared_ptr<arrow::RecordBatch>& vertices) {
    return vertex_reader_->ReadBatch(vertices);
  }

  Status GetNextEdges(Client& client,
                      std::shared_ptr<arrow::RecordBatch>& edges) {
    return edge_reader_->ReadBatch(edges);
  }

  std::shared_ptr<MGPropertyGraphSchema> graph_schema() const {
    return graph_schema_;
  }

 private:
  std::shared_ptr<vineyard::DataframeStream> vertex_stream_;
  std::shared_ptr<vineyard::DataframeStream> edge_stream_;
  std::unique_ptr<vineyard::DataframeStreamReader> vertex_reader_;
  std::unique_ptr<vineyard::DataframeStreamReader> edge_reader_;
  std::shared_ptr<MGPropertyGraphSchema> graph_schema_;
};

class GlobalPGStreamBuilder;

class GlobalPGStream : public Registered<GlobalPGStream>, GlobalObject {
 public:
  static std::unique_ptr<Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<Object>(
        std::unique_ptr<GlobalPGStream>(new GlobalPGStream()));
  }

  std::shared_ptr<PropertyGraphOutStream> StreamAt(size_t const index) const {
    for (auto const& ptr : local_streams_) {
      if (static_cast<size_t>(ptr->stream_index()) == index) {
        return ptr;
      }
    }
    LOG(ERROR) << "No such stream: index = " << index;
    return nullptr;
  }

  const std::vector<std::shared_ptr<PropertyGraphOutStream>>& AvailableStreams(
      Client& client) const {
    return local_streams_;
  }

  void Construct(const ObjectMeta& meta) override;

 private:
  size_t total_stream_chunks_;
  std::vector<std::shared_ptr<PropertyGraphOutStream>> local_streams_;

  friend class Client;
  friend class GlobalPGStreamBuilder;
};

class GlobalPGStreamBuilder : public ObjectBuilder {
 public:
  explicit GlobalPGStreamBuilder(Client& client)
      : total_stream_chunks_(0) {}
  ~GlobalPGStreamBuilder() = default;

  void AddStream(int const /* index */, ObjectID const stream_id,
                 uint64_t const /* instance_id */) {
    total_stream_chunks_ += 1;
    stream_chunks_.push_back(stream_id);
  }

  Status Build(Client& client) {
    return Status::OK();
  }

  std::shared_ptr<Object> _Seal(Client& client);

 private:
  std::vector<ObjectID> stream_chunks_;
  size_t total_stream_chunks_;
};

}  // namespace vineyard

#endif  // SRC_CLIENT_DS_STREAM_PROPERTY_GRAPH_H_
