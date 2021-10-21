/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <cstring>
#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include <unordered_map>

#include <boost/leaf/all.hpp>
#include <boost/dynamic_bitset.hpp>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <vineyard/graph/loader/arrow_fragment_loader.h>
#include <vineyard/client/client.h>

#include "../global_store_ffi.h"
#include "../graph_builder_ffi.h"
#include "../htap_ds_impl.h"
#include "vineyard_store_test_env.h"

using std::string;
using std::vector;
using std::unique_ptr;
using std::ostream;
using std::unordered_map;
using std::unordered_multimap;
using std::pair;

using boost::dynamic_bitset;

using vineyard::Client;

namespace vineyard_store_test {

template<class TypeBuilder>
class TypeBuilderBase {
 private:
  TypeBuilder& AsTypeBuilder() { return static_cast<TypeBuilder&>(*this); }
 public:

  virtual ~TypeBuilderBase() = default;

  TypeBuilder& id(LabelId id) { id_ = id; return AsTypeBuilder(); }

  TypeBuilder& label(const std::string& label) {
    label_ = label;
    return AsTypeBuilder();
  }

  TypeBuilder& AddProperty(PropertyId id, const std::string& name,
                           ::PropertyType type = INT) {
    AsTypeBuilder().BuildProperty(id, name, type);
    return AsTypeBuilder();
  }

 private:
  LabelId id_;
  std::string label_;
};

class SchemaBuilder {
 public:
  SchemaBuilder() {
    schema_ = create_schema_builder();
  }

  Schema Build() {
    return finish_build_schema(schema_);
  }

  class VertexTypeBuilder : public TypeBuilderBase<VertexTypeBuilder> {
   public:
    using VineyardVertexTypeBuilder = ::VertexTypeBuilder;

    VertexTypeBuilder(VineyardVertexTypeBuilder vineyard_builder)
        : builder_(vineyard_builder) {
    }

    VertexTypeBuilder& PrimaryKey(const char* primary_key_property_name) {
      build_vertex_primary_keys(builder_, 1, &primary_key_property_name);
      return *this;
    }

    void BuildProperty(PropertyId id, const std::string& name,
                       ::PropertyType type) {
      build_vertex_property(builder_, id, name.c_str(), type);
    }

    void Build() {
      finish_build_vertex(builder_);
    }

   private:

    VineyardVertexTypeBuilder builder_;
  };

  class EdgeTypeBuilder : public TypeBuilderBase<EdgeTypeBuilder> {
   public:
    using VineyardEdgeTypeBuilder = ::EdgeTypeBuilder;

    EdgeTypeBuilder(VineyardEdgeTypeBuilder vineyard_builder)
        : builder_(vineyard_builder) {
    }

    void BuildProperty(PropertyId id, const std::string& name,
                       ::PropertyType type) {
      build_edge_property(builder_, id, name.c_str(), type);
    }

    void Build() {
      finish_build_edge(builder_);
    }

   private:
    VineyardEdgeTypeBuilder builder_;
  };

  VertexTypeBuilder AddVertexType(LabelId id, const std::string& label) {
    auto type = build_vertex_type(schema_, id, label.c_str());
    return VertexTypeBuilder(type);
  }

  EdgeTypeBuilder AddEdgeType(LabelId id, const std::string& label) {
    auto type = build_edge_type(schema_, id, label.c_str());
    return EdgeTypeBuilder(type);
  }

 private:
  Schema schema_;
};

class GraphBuilderDeleter {
 public:
  void operator()(GraphBuilder graph_builder) {
    destroy(graph_builder);
  }
};

using GraphBuilderRaii = unique_ptr<void, GraphBuilderDeleter>;

class GraphHandleDeleter {
 public:
  void operator()(GraphHandle graph_handle) {
    free_graph_handle(graph_handle);
  }
};

using GraphRaii = unique_ptr<void, GraphHandleDeleter>;

class SchemaDeleter {
 public:
  void operator()(Schema schema) {
    free_schema(schema);
  }
};

using SchemaRaii = unique_ptr<void, SchemaDeleter>;

class GetVertexIteratorDeleter {
 public:
  void operator()(GetVertexIterator get_vertex_iterator) {
    free_get_vertex_iterator(get_vertex_iterator);
  }
};

using GetVertexIteratorRaii = unique_ptr<void, GetVertexIteratorDeleter>;

class GetAllVerticesIteratorDeleter {
 public:
  void operator()(GetAllVerticesIterator get_all_vertices_iterator) {
    free_get_all_vertices_iterator(get_all_vertices_iterator);
  }
};

using GetAllVerticesIteratorRaii =
    unique_ptr<void, GetAllVerticesIteratorDeleter>;

class GetAllEdgesIteratorDeleter {
 public:
  void operator()(GetAllEdgesIterator get_all_edges_iterator) {
    free_get_all_edges_iterator(get_all_edges_iterator);
  }
};

using GetAllEdgesIteratorRaii =
unique_ptr<void, GetAllEdgesIteratorDeleter>;

class OutEdgeIteratorDeleter {
 public:
  void operator()(OutEdgeIterator out_edge_iterator) {
    free_out_edge_iterator(out_edge_iterator);
  }
};

using OutEdgeIteratorRaii =
unique_ptr<void, OutEdgeIteratorDeleter>;

class InEdgeIteratorDeleter {
 public:
  void operator()(OutEdgeIterator out_edge_iterator) {
    free_in_edge_iterator(out_edge_iterator);
  }
};

using InEdgeIteratorRaii =
unique_ptr<void, InEdgeIteratorDeleter>;

class PropertiesIteratorDeleter {
 public:
  void operator()(PropertiesIterator properties_iterator) {
    free_properties_iterator(properties_iterator);
  }
};

using PropertiesIteratorRaii = unique_ptr<void, PropertiesIteratorDeleter>;

class VineyardStoreTestGraphBuilder {
 public:
  VineyardStoreTestGraphBuilder(const char* graph_name, Schema schema)
      : graph_name_{graph_name}, builder_{},
        global_graph_stream_id_{-1} {
    builder_.reset(create_graph_builder(graph_name, schema, 0));
    ::ObjectId object_id;
    ::InstanceId instance_id;
    get_builder_id(builder_.get(), &object_id, &instance_id);
    global_graph_stream_id_ = build_global_graph_stream(graph_name,
        1, &object_id, &instance_id);
  }

  ::GraphBuilder GetGraphBuilder() const { return builder_.get(); }

  GraphRaii Build() {
    ::build(builder_.get());
    auto& client = vineyard::Client::Default();
    // TODO: make global stream expose ObjectId of its vertex_stream and
    // edge_stream. Now as a hack get it by hard coded names
    vineyard::ObjectID vs_obj_id;
    VINEYARD_CHECK_OK(client.GetName(std::string("__") + graph_name_ + "_vertex_stream", vs_obj_id));
    vineyard::ObjectID es_obj_id;
    VINEYARD_CHECK_OK(client.GetName(std::string("__") + graph_name_ + "_edge_stream", es_obj_id));

    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    std::vector<vineyard::ObjectID> vstreams;
    vstreams.push_back(vs_obj_id);
    // TODO: why vector of vector, should be vector only
    std::vector<std::vector<vineyard::ObjectID>> estreams;
    std::vector<vineyard::ObjectID> e_substreams;
    e_substreams.push_back(es_obj_id);
    estreams.push_back(std::move(e_substreams));
    auto loader = std::make_unique<
        vineyard::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
            vineyard::property_graph_types::VID_TYPE>>(
        client, comm_spec, vstreams, estreams, true);
    auto graph_id = loader->LoadFragmentAsFragmentGroup().value();
    return GraphRaii{get_graph_handle(graph_id, 1)};
  }

 private:
  string graph_name_;
  GraphBuilderRaii builder_;
  ::ObjectId global_graph_stream_id_;
};

template<class T>
static void CopyBitsToLength(int64_t& length, T value) {
  static_assert(sizeof(length) >= sizeof(value));
  std::memcpy(&length, std::addressof(value), sizeof(value));
}

static ::Property MakeBoolProperty(int property_id, bool value) {
  ::Property prop{ property_id, ::PropertyType::BOOL, nullptr, 0 };
  CopyBitsToLength(prop.len, value);
  return prop;
}

static ::Property MakeCharProperty(int property_id, char value) {
  ::Property prop{ property_id, ::PropertyType::CHAR, nullptr, 0 };
  CopyBitsToLength(prop.len, value);
  return prop;
}

static ::Property MakeShortProperty(int property_id, int16_t value) {
  ::Property prop{ property_id, ::PropertyType::SHORT, nullptr, 0 };
  CopyBitsToLength(prop.len, value);
  return prop;
}

static ::Property MakeIntProperty(int property_id, int32_t value) {
  ::Property prop{ property_id, ::PropertyType::INT, nullptr, 0 };
  CopyBitsToLength(prop.len, value);
  return prop;
}

static ::Property MakeLongProperty(int property_id, int64_t value) {
  return { property_id, ::PropertyType::LONG, nullptr, value };
}

static ::Property MakeFloatProperty(int property_id, float value) {
  ::Property prop{ property_id, ::PropertyType::FLOAT, nullptr, 0 };
  CopyBitsToLength(prop.len, value);
  return prop;
}

static ::Property MakeDoubleProperty(int property_id, double value) {
  ::Property prop{ property_id, ::PropertyType::DOUBLE, nullptr, 0 };
  CopyBitsToLength(prop.len, value);
  return prop;
}

static ::Property MakeStringProperty(int property_id, string& str) {
  // in vineyard, the underlining memory is immutable
  // so should not update data pointed to by Property's data member
  // or Property should declare it as const char* to begin with
  return { property_id, ::PropertyType::STRING,
           const_cast<char*>(str.data()), static_cast<int64_t>(str.size())
  };
}

static ::Property MakeStringProperty(int property_id, const char* c_str) {
  return { property_id, ::PropertyType::STRING,
           const_cast<char*>(c_str), static_cast<int64_t>(strlen(c_str))
  };
}

static bool Eq(const ::Property& left, const ::Property& right) {
  return //left.id == right.id &&
         left.type == right.type &&
         left.len == right.len &&
         ((left.data == nullptr && right.data == nullptr) ||
          (left.data != nullptr && right.data != nullptr &&
           std::memcmp(left.data, right.data, left.len) == 0));
}

static void PrintPropertyValue(const ::Property& property, ostream& os){
  // this is undefined behavior though should work in gcc
  // TODO: fix all of these type punnings
  htap_impl::PodProperties pod_properties;
  pod_properties.long_value = property.len;
  switch (property.type) {
  case ::PropertyType::INVALID:
    os << "(INVALID)";
    break;
  case ::PropertyType::BOOL:
    os << ((property.len != 0) ? "true" : "false");
    break;
  case ::PropertyType::CHAR:
    os << pod_properties.char_value;
    break;
  case ::PropertyType::SHORT:
    os << pod_properties.int16_value;
    break;
  case ::PropertyType::INT:
    os << pod_properties.int_value;
    break;
  case ::PropertyType::LONG:
    os << pod_properties.long_value;
    break;
  case ::PropertyType::FLOAT:
    os << pod_properties.float_value;
    break;
  case ::PropertyType::DOUBLE:
    os << pod_properties.double_value;
    break;
  case ::PropertyType::STRING: {
    string str((char*)property.data, (size_t)property.len);
    os << str.c_str();
    break;
  }
  case ::PropertyType::BYTES:
    os << "(NOT SUPPORTED)";
    break;
  case ::PropertyType::INT_LIST:
  case ::PropertyType::LONG_LIST:
  case ::PropertyType::FLOAT_LIST:
  case ::PropertyType::DOUBLE_LIST:
  case ::PropertyType::STRING_LIST:
    os << "(NOT SUPPORTED)";
    break;
  default:
    os << "(ERROR)";
    break;
  }
}

class GraphPropertyInfo {
 public:
  GraphPropertyInfo(::PropertyId property_id, const char* property_name,
      ::PropertyType property_type) : prop_id_{property_id},
          name_(property_name), prop_type_(property_type) {}

  ~GraphPropertyInfo() = default;

  ::PropertyId GetPropertyId() const { return prop_id_; }

  const string& GetName() const { return name_; }

  ::PropertyType GetPropertyType() const { return prop_type_; }

 private:
  ::PropertyId prop_id_;
  string name_;
  ::PropertyType prop_type_;
};

class GraphElementSchema {
 public:
  static constexpr const ::PropertyId kInvalidPropertyId = -1;

  GraphElementSchema(Schema schema, ::LabelId label_id,
      const string& label, const vector<string>& property_names)
          : label_id_(label_id), label_(label) {
    for (auto& name : property_names) {
      ::PropertyId property_id = kInvalidPropertyId;
      if (get_property_id(schema, name.c_str(), &property_id) != 0) {
        property_id = kInvalidPropertyId;
      }
      ::PropertyType property_type = ::PropertyType::INVALID;
      if (property_id == kInvalidPropertyId ||
          get_property_type(schema, label_id_,
                            property_id, &property_type) != 0) {
        property_type = ::PropertyType::INVALID;
      }
      properties_.emplace_back(property_id, name.c_str(), property_type);
    }
  }

  ::LabelId GetLabelId() const { return label_id_; }

  const string& GetLabel() const { return label_; }

  size_t GetNumOfProperties() const { return properties_.size(); }

  const GraphPropertyInfo& operator[](size_t ordinal) const {
    return properties_[ordinal];
  };

  friend bool operator==(const GraphElementSchema& left,
                         const GraphElementSchema& right) {
    if (left.GetLabelId() != right.GetLabelId()) {
      return false;
    }
    if (left.GetNumOfProperties() != right.GetNumOfProperties()) {
      return false;
    }
    for (size_t i = 0; i < left.GetNumOfProperties(); i++){
      if (left[i].GetPropertyType() != right[i].GetPropertyType() ||
          left[i].GetName() != right[i].GetName()) {
        return false;
      }
    }
    return true;
  }

  friend bool operator!=(const GraphElementSchema& left,
                         const GraphElementSchema& right) {
    return !(left == right);
  }

 private:
  ::LabelId label_id_;
  string label_;
  vector<GraphPropertyInfo> properties_;
};

class GraphElement {
 public:
  explicit GraphElement(const GraphElementSchema& schema) : schema_(schema),
      num_properties_(schema_.GetNumOfProperties()),
      properties_(num_properties_),
      null_flags_(num_properties_) {}

  GraphElement(const GraphElement& other) = default;

  GraphElement(GraphElement&& other) = default;

  const GraphElementSchema& GetElementSchema() const { return schema_; }

  size_t GetNumOfProperties() const { return num_properties_; }

  ::Property& operator[](size_t ordinal) { return properties_[ordinal]; }

  const ::Property& operator[](size_t ordinal) const {
    return properties_[ordinal];
  }

  void SetProperty(size_t ordinal, const ::Property& property) {
    properties_[ordinal] = property;
    ClearNull(ordinal);
  }

  bool IsNull(size_t ordinal) const { return null_flags_.test(ordinal); }

  void SetNull(size_t ordinal) { null_flags_.set(ordinal); }

  void ClearNull(size_t ordinal) { null_flags_.reset(ordinal); }

  void Reset() { null_flags_.set(); }

  virtual ~GraphElement() = default;

  bool operator==(const GraphElement& other) const {
    auto num_props = GetNumOfProperties();
    if (num_props != other.GetNumOfProperties()) {
      return false;
    }
    for (size_t i = 0; i < num_props; ++i) {
      bool is_null = IsNull(i);
      bool is_other_null = other.IsNull(i);
      if (!is_null && !is_other_null && !Eq((*this)[i], other[i])) {
        return false;
      }
      if (is_null ^ is_other_null) {
        return false;
      }
    }
    return true;
  }

  bool operator!=(const GraphElement& other) const {
    return !operator==(other);
  }

 protected:
  void PrintProperties(ostream& os) const {
    os << '[';
    for (size_t i = 0; i < num_properties_; ++i){
      if (IsNull(i)){
        os << "(null)";
      }else{
        PrintPropertyValue(this->operator[](i), os);
      }
      if (i != (num_properties_ - 1)){
        os << ", ";
      }
    }
    os << ']';
  }
 private:
  const GraphElementSchema& schema_;
  size_t num_properties_;
  vector<::Property> properties_;
  dynamic_bitset<uint64_t> null_flags_;
};

class VertexElement : public GraphElement {
 public:
  static const ::VertexId kInvalidId = -1;

  explicit VertexElement(const GraphElementSchema& schema)
      : GraphElement(schema), vertex_id_(kInvalidId) {}

  VertexElement(const VertexElement& other) = default;

  VertexElement(VertexElement&& other) = default;

  ::VertexId GetVertexId() const { return vertex_id_; }

  void SetVertexId(::VertexId vertex_id) { vertex_id_ = vertex_id; }

  bool operator==(const VertexElement& other) const {
    return GetVertexId() == other.GetVertexId() &&
        this->GraphElement::operator==(other);
  }

  bool operator!=(const VertexElement& other) const {
    return !operator==(other);
  }

  // for googletest
  friend void PrintTo(const VertexElement& vertex, ostream* os) {
    auto& schema = vertex.GetElementSchema();
    *os << schema.GetLabel();
    *os << '(';
    auto vid = vertex.GetVertexId();
    if (vid == kInvalidId) {
      *os << "INVALID";
    }else{
      *os << vid;
    }
    *os << ", ";
    vertex.PrintProperties(*os);
    *os << ')';
  }

 private:
  ::VertexId vertex_id_;
};

class EdgeElement : public GraphElement {
 public:
  static const ::EdgeId kInvalidId = -1;

  explicit EdgeElement(const GraphElementSchema& schema)
      : GraphElement(schema), edge_id_(kInvalidId),
        src_id_(VertexElement::kInvalidId),
        dest_id_(VertexElement::kInvalidId) {}

  EdgeElement(const EdgeElement& other) = default;

  EdgeElement(EdgeElement&& other) = default;

  ::EdgeId GetEdgeId() const { return edge_id_; }
  void SetEdgeId(::EdgeId edge_id) { edge_id_ = edge_id; }

  ::VertexId GetSrcId() const { return src_id_; }
  void SetSrcId(::VertexId src_id) { src_id_ = src_id; }
  ::VertexId GetDestId() const { return dest_id_; }
  void SetDestId(::VertexId dest_id) { dest_id_ = dest_id; }

  bool operator==(const EdgeElement& other) const {
    return // GetEdgeId() == other.GetEdgeId() &&
           GetSrcId() == other.GetSrcId() && GetDestId() == other.GetDestId() &&
           this->GraphElement::operator==(other);
  }

  bool operator!=(const EdgeElement& other) const {
    return !operator==(other);
  }

  // for googletest
  friend void PrintTo(const EdgeElement& edge, ostream* os) {
    auto& schema = edge.GetElementSchema();
    *os << schema.GetLabel();
    *os << '(';
    // edgeid not supported yet
    //auto eid = edge.GetEdgeId();
    //if (eid == kInvalidId) {
    //  *os << "INVALID";
    //}else{
    //  *os << eid;
    //}
    *os << edge.GetSrcId();
    *os << " -> ";
    *os << edge.GetDestId();
    *os << ", ";
    edge.PrintProperties(*os);
    *os << ')';
  }

 private:
  ::EdgeId edge_id_;
  ::VertexId src_id_;
  ::VertexId dest_id_;
};

static int ReadVertexElement(::GraphHandle graph, ::Vertex vertex,
                             VertexElement& vertexElement) {
  vertexElement.Reset();
  vertexElement.SetVertexId(get_outer_id(graph, vertex));
  auto& vertexSchema = vertexElement.GetElementSchema();
  for (size_t i = 0; i < vertexSchema.GetNumOfProperties(); i++){
    auto result = get_vertex_property(graph, vertex,
        vertexSchema[i].GetPropertyId(), &vertexElement[i]);
    if (result != -1) {
      vertexElement.ClearNull(i);
    }else{
      return result;
    }
  }
  return 0;
}

static int ReadEdgeElement(::GraphHandle graph, struct Edge* edge,
                             EdgeElement& edgeElement) {
  edgeElement.Reset();
  // edgeElement.SetEdgeId(get_edge_id(graph, edge));
  edgeElement.SetSrcId(get_outer_id(graph, get_edge_src_id(graph, edge)));
  edgeElement.SetDestId(get_outer_id(graph, get_edge_dst_id(graph, edge)));
  auto& edgeSchema = edgeElement.GetElementSchema();
  for (size_t i = 0; i < edgeElement.GetNumOfProperties(); i++){
    auto result = get_edge_property(graph, edge, edgeSchema[i].GetPropertyId(),
                                    &edgeElement[i]);
    if (result != -1) {
      edgeElement.ClearNull(i);
    }else{
      return result;
    }
  }
  return 0;
}

// vineyard cannot handle graph with no edges, disable such tests for now
class DISABLED_OneVertexOnePropertyTest
    : public ::testing::TestWithParam<std::pair<::PropertyType, ::Property>> {
 protected:
  static SchemaRaii MakeOneVertexOnePropertySchema(::PropertyId property_id,
      const char* property_name, ::PropertyType property_type) {
    SchemaBuilder schema_builder;
    schema_builder.AddVertexType(0, "test_vertex") // label id start from 0
        .AddProperty(property_id, property_name, property_type)
        .Build();
    return SchemaRaii{schema_builder.Build()};
  }
};

TEST_P(DISABLED_OneVertexOnePropertyTest, AddVertexThenReadBack) {
  const int property_id = 1; // property id expected starting from 1
  // const char* property_name = "test_property";
  const char* property_name = "__vertex_id__";
  auto& param = GetParam();
  ::PropertyType prop_type = param.first;
  ::Property prop = param.second;
  ASSERT_EQ(prop_type, prop.type);
  SchemaRaii schema_raii = MakeOneVertexOnePropertySchema(property_id,
      property_name,prop_type);
  VineyardStoreTestGraphBuilder graph_builder{"test_graph", schema_raii.get()};
  auto builder_handle = graph_builder.GetGraphBuilder();
  add_vertex(builder_handle, 101, 0, 1, &prop);
  auto graph_raii = graph_builder.Build();
  auto graph = graph_raii.get();
  auto read_back_schema = get_schema(graph);
  ::PropertyId property_id1;
  auto res = get_property_id(read_back_schema, "", &property_id1);
  ::PropertyId read_back_prop_id = -1;
  ASSERT_NE(-1,
      get_property_id(read_back_schema, property_name, &read_back_prop_id));
  LabelId label_id = 0;
  GetAllVerticesIteratorRaii iter_raii{
      get_all_vertices(graph, 0, &label_id, 1, 10)};
  Vertex v;
  ASSERT_NE(-1, get_all_vertices_next(iter_raii.get(), &v));
  EXPECT_EQ(get_vertex_label(graph, v), 0);
  EXPECT_EQ(get_outer_id(graph, v), 101);
  ::Property read_back_prop;
  std::memset(&read_back_prop, 0, sizeof(read_back_prop));
  get_vertex_property(graph, v, read_back_prop_id, &read_back_prop);
  EXPECT_EQ(read_back_prop.id, read_back_prop_id);
  EXPECT_EQ(read_back_prop.type, prop_type);
  EXPECT_PRED2(Eq, param.second, read_back_prop);
  ASSERT_EQ(-1, get_all_vertices_next(iter_raii.get(), &v));
}

INSTANTIATE_TEST_SUITE_P(
    OneVertexOnePropertyTest,
    DISABLED_OneVertexOnePropertyTest,
    ::testing::Values(
        // vineyard does not support bool, char, short type yet
        // std::make_pair(::PropertyType::BOOL, MakeBoolProperty(1, true))
        //std::make_pair(::PropertyType::CHAR, MakeCharProperty(1, 'a')),
        // std::make_pair(::PropertyType::SHORT, MakeShortProperty(1, 1001)),
        std::make_pair(::PropertyType::INT, MakeIntProperty(1, 10001)),
        std::make_pair(::PropertyType::LONG, MakeLongProperty(1, 9876543210)),
        std::make_pair(::PropertyType::FLOAT, MakeFloatProperty(1, 0.5)),
        std::make_pair(::PropertyType::DOUBLE, MakeDoubleProperty(1, -0.5)),
        std::make_pair(::PropertyType::STRING, MakeStringProperty(1, "abcde"))
    ));

class OneEdgeOnePropertyTest
    : public testing::TestWithParam<std::pair<::PropertyType, ::Property>> {
 protected:
  static SchemaRaii MakeOneEdgeOnePropertySchema(::PropertyId property_id,
      const char* property_name, ::PropertyType property_type) {
    SchemaBuilder schema_builder;
    schema_builder.AddVertexType(0, "test_vertex") // label id start from 0
        .Build();
    schema_builder.AddEdgeType(1, "test_edge")
        .AddProperty(property_id, property_name, property_type)
        .Build();
    return SchemaRaii{schema_builder.Build()};
  }
 };

TEST_P(OneEdgeOnePropertyTest, AddEdgeThenReadBack) {
  const int property_id = 1; // property id expected starting from 1
  const char* property_name = "test_property";
  auto& param = GetParam();
  ::PropertyType prop_type = param.first;
  ::Property prop = param.second;
  ASSERT_EQ(prop_type, prop.type);
  SchemaRaii schema_raii = MakeOneEdgeOnePropertySchema(property_id,
      property_name,prop_type);
  VineyardStoreTestGraphBuilder graph_builder{"test_graph", schema_raii.get()};
  auto builder_handle = graph_builder.GetGraphBuilder();
  add_vertex(builder_handle, 1001, 0, 0, nullptr);
  add_edge(builder_handle, 2001, 1001, 1001, 1, 0, 0, 1, &prop);
  auto graph_raii = graph_builder.Build();
  auto graph = graph_raii.get();
  auto read_back_schema = get_schema(graph);
  ::PropertyId read_back_prop_id = -1;
  ASSERT_NE(-1,
      get_property_id(read_back_schema, property_name, &read_back_prop_id));
  GetAllEdgesIteratorRaii all_edges_iterator_raii{
    get_all_edges(graph, 0, nullptr, 0, 10)
  };
  Edge e;
  ASSERT_NE(-1, get_all_edges_next(all_edges_iterator_raii.get(), &e));
  // get_edge_id() not supported
  // EXPECT_EQ(get_edge_id(graph, &e), 2001);
  EXPECT_EQ(get_edge_label(graph, &e), 1);
  EXPECT_EQ(get_edge_src_label(graph, &e), 0);
  EXPECT_EQ(get_outer_id(graph, get_edge_src_id(graph, &e)), 1001);
  EXPECT_EQ(get_edge_dst_label(graph, &e), 0);
  EXPECT_EQ(get_outer_id(graph, get_edge_dst_id(graph, &e)), 1001);
  ::Property read_back_prop;
  std::memset(&read_back_prop, 0, sizeof(read_back_prop));
  get_edge_property(graph, &e, read_back_prop_id, &read_back_prop);
  EXPECT_EQ(read_back_prop.id, read_back_prop_id);
  EXPECT_EQ(read_back_prop.type, prop_type);
  EXPECT_PRED2(Eq, param.second, read_back_prop);
  ASSERT_EQ(-1, get_all_edges_next(all_edges_iterator_raii.get(), &e));
}

INSTANTIATE_TEST_SUITE_P(
    OneEdgeOnePropertyTest,
    OneEdgeOnePropertyTest,
    ::testing::Values(
        // vineyard does not support bool, char, short type yet
        // std::make_pair(::PropertyType::BOOL, MakeBoolProperty(1, true))
        //std::make_pair(::PropertyType::CHAR, MakeCharProperty(1, 'a')),
        // std::make_pair(::PropertyType::SHORT, MakeShortProperty(1, 1001)),
        std::make_pair(::PropertyType::INT, MakeIntProperty(1, 10001)),
        std::make_pair(::PropertyType::LONG, MakeLongProperty(1, 9876543210)),
        std::make_pair(::PropertyType::FLOAT, MakeFloatProperty(1, 3.14)),
        std::make_pair(::PropertyType::DOUBLE, MakeDoubleProperty(1, -3.14)),
        std::make_pair(::PropertyType::STRING, MakeStringProperty(1, "abcdefg"))
    ));

class ModernGraphTest : public ::testing::Test {
 protected:
  class ModernGraph {
   public:
    static constexpr const char* kGraphName = "Modern";

    // workaround for constexpr inline initialization until c++17
    // static constexpr const ::LabelId kPersonLabelId = 0;
    static constexpr ::LabelId PersonLabelId() { return 0; }
    static constexpr const char* kPersonLabel = "person";
    // static constexpr const ::LabelId kSoftwareLabelId = 1;
    static constexpr const ::LabelId SoftwareLabelId() { return 1; }
    static constexpr const char* kSoftwareLabel = "software";

    static constexpr const ::LabelId KnowsLabelId() { return 2; }
    static constexpr const char* kKnowsLabel = "knows";
    static constexpr const ::LabelId CreatesLabelId() { return 3; }
    static constexpr const char* kCreatesLabel = "creates";

    static const ::PropertyId kIdPropertyId = 1;
    static constexpr const char* kIdPropertyName = "id";
    static const ::PropertyType kIdPropertyType = ::PropertyType::LONG;
    static const ::PropertyId kNamePropertyId = 2;
    static constexpr const char* kNamePropertyName = "name";
    static const ::PropertyType kNamePropertyType = ::PropertyType::STRING;
    static const ::PropertyId kLanguagePropertyId = 3;
    static constexpr const char* kLanguagePropertyName = "language";
    static const ::PropertyType kLanguagePropertyType = ::PropertyType::STRING;

    static const ::PropertyId kWeightPropertyId = 4;
    static constexpr const char* kWeightPropertyName = "weight";
    static const ::PropertyType kWeightPropertyType = ::PropertyType::FLOAT;

    class PersonVertex {
     public:
      static GraphElementSchema GetElementSchema(Schema schema) {
        return GraphElementSchema(schema, PersonLabelId(), kPersonLabel,
                                  {kIdPropertyName, kNamePropertyName});
      }

      PersonVertex(::VertexId vertex_id, int64_t id, const char* name)
          : vertex_id_(vertex_id), id_(id), name_(name) {
        id_prop_ = MakeLongProperty(kIdPropertyId, id_);
        name_prop_ = MakeStringProperty(kNamePropertyId, name_);
      }

      int CopyToVertexElement(VertexElement& vertex_element) {
        auto& element_schema = vertex_element.GetElementSchema();
        if (element_schema.GetNumOfProperties() != 2 &&
            element_schema[0].GetName() != kIdPropertyName &&
            element_schema[1].GetName() != kNamePropertyName) {
          return -1;
        }
        vertex_element.SetVertexId(GetVertexId());
        vertex_element.SetProperty(0, id_prop_);
        vertex_element.SetProperty(1, name_prop_);
        return 0;
      }

      ::VertexId GetVertexId() const { return vertex_id_; }

      const ::Property& GetIdProperty() const { return id_prop_; }

      const ::Property& GetNameProperty() const { return name_prop_; }

     private:
      ::VertexId vertex_id_;
      int64_t id_;
      string name_;
      Property id_prop_;
      Property name_prop_;
    };

    class SoftwareVertex {
     public:
      static GraphElementSchema GetElementSchema(Schema schema) {
        return GraphElementSchema(schema, SoftwareLabelId(), kSoftwareLabel,
            {kIdPropertyName, kNamePropertyName, kLanguagePropertyName});
      }

      SoftwareVertex(::VertexId vertex_id, int64_t id,
                     const char* name, const char* language)
          : vertex_id_(vertex_id), id_(id),
            name_(name), language_(language) {
        id_prop_ = MakeLongProperty(kIdPropertyId, id_);
        name_prop_ = MakeStringProperty(kNamePropertyId, name_);
        language_prop_ = MakeStringProperty(kLanguagePropertyId, language_);
      }

      int CopyToVertexElement(VertexElement& vertex_element) {
        auto& element_schema = vertex_element.GetElementSchema();
        if (element_schema.GetNumOfProperties() != 3 &&
            element_schema[0].GetName() != kIdPropertyName &&
            element_schema[1].GetName() != kNamePropertyName &&
            element_schema[2].GetName() != kLanguagePropertyName) {
          return -1;
        }
        vertex_element.SetVertexId(GetVertexId());
        vertex_element.SetProperty(0, id_prop_);
        vertex_element.SetProperty(1, name_prop_);
        vertex_element.SetProperty(2, language_prop_);
        return 0;
      }

      ::VertexId GetVertexId() const { return vertex_id_; }

      const ::Property& GetIdProperty() const { return id_prop_; }

      const ::Property& GetNameProperty() const { return name_prop_; }

      const ::Property& GetLanguangeProperty() const { return language_prop_; }

     private:
      ::VertexId vertex_id_;
      int64_t id_;
      string name_;
      string language_;
      Property id_prop_;
      Property name_prop_;
      Property language_prop_;

    };

    class KnowsEdge {
     public:
      static GraphElementSchema GetElementSchema(Schema schema) {
        return GraphElementSchema(schema,
                                  KnowsLabelId(),
                                  kKnowsLabel,
                                  {kWeightPropertyName});
      }

      KnowsEdge(::EdgeId edge_id,
                const PersonVertex& src,
                const PersonVertex& dest,
                float weight)
          : edge_id_(edge_id), src_id_(src.GetVertexId()),
            dest_id_(dest.GetVertexId()), weight_(weight) {
        weight_prop_ = MakeFloatProperty(kWeightPropertyId, weight);
      }

      ::EdgeId GetEdgeId() const { return edge_id_; }
      ::VertexId GetSrcId() const { return src_id_; }
      ::VertexId GetDestId() const { return dest_id_; }
      float GetWeight() const { return weight_; }
      ::Property GetWeightProperty() const { return weight_prop_; }

      int CopyToEdgeElement(EdgeElement& edge_element) {
        auto& element_schema = edge_element.GetElementSchema();
        if (element_schema.GetNumOfProperties() != 1 &&
            element_schema[0].GetName() != kWeightPropertyName) {
          return -1;
        }
        edge_element.SetEdgeId(GetEdgeId());
        edge_element.SetSrcId(GetSrcId());
        edge_element.SetDestId(GetDestId());
        edge_element.SetProperty(0, GetWeightProperty());
        return 0;
      }

     private:
      ::EdgeId edge_id_;
      ::VertexId src_id_;
      ::VertexId dest_id_;
      float weight_;
      ::Property weight_prop_;
    };

    class CreatesEdge {
     public:
      static GraphElementSchema GetElementSchema(Schema schema) {
        return GraphElementSchema(schema,
                                  CreatesLabelId(),
                                  kCreatesLabel,
                                  {kWeightPropertyName});
      }

      CreatesEdge(::EdgeId edge_id,
                const PersonVertex& src,
                const SoftwareVertex& dest,
                double weight)
          : edge_id_(edge_id), src_id_(src.GetVertexId()),
            dest_id_(dest.GetVertexId()), weight_(weight) {
        weight_prop_ = MakeFloatProperty(kWeightPropertyId, weight);
      }

      ::EdgeId GetEdgeId() const { return edge_id_; }
      ::VertexId GetSrcId() const { return src_id_; }
      ::VertexId GetDestId() const { return dest_id_; }
      float GetWeight() const { return weight_; }
      ::Property GetWeightProperty() const { return weight_prop_; }

      int CopyToEdgeElement(EdgeElement& edge_element) {
        auto& element_schema = edge_element.GetElementSchema();
        if (element_schema.GetNumOfProperties() != 1 &&
            element_schema[0].GetName() != kWeightPropertyName) {
          return -1;
        }
        edge_element.SetEdgeId(GetEdgeId());
        edge_element.SetSrcId(GetSrcId());
        edge_element.SetDestId(GetDestId());
        edge_element.SetProperty(0, GetWeightProperty());
        return 0;
      }

     private:
      ::EdgeId edge_id_;
      ::VertexId src_id_;
      ::VertexId dest_id_;
      float weight_;
      ::Property weight_prop_;
    };

    class Builder {
     public:
      Builder& AddPerson(const PersonVertex& person) {
        vertex_labels_.push_back(PersonLabelId());
        vertex_ids_.push_back(person.GetVertexId());
        vertex_properties_.push_back(person.GetIdProperty());
        vertex_properties_.push_back(person.GetNameProperty());
        vertex_property_sizes_.push_back(2);
        return *this;
      }

      Builder& AddSoftware(const SoftwareVertex& software) {
        vertex_labels_.push_back(SoftwareLabelId());
        vertex_ids_.push_back(software.GetVertexId());
        vertex_properties_.push_back(software.GetIdProperty());
        vertex_properties_.push_back(software.GetNameProperty());
        vertex_properties_.push_back(software.GetLanguangeProperty());
        vertex_property_sizes_.push_back(3);
        return *this;
      }

      Builder& AddKnows(const KnowsEdge& knows) {
        edge_labels_.push_back(KnowsLabelId());
        edge_ids_.push_back(knows.GetEdgeId());
        src_ids_.push_back(knows.GetSrcId());
        src_labels_.push_back(PersonLabelId());
        dest_ids_.push_back(knows.GetDestId());
        dest_labels_.push_back(PersonLabelId());
        edge_properties_.push_back(knows.GetWeightProperty());
        edge_property_sizes_.push_back(1);
        return *this;
      }

      Builder& AddCreates(const CreatesEdge& creates) {
        edge_labels_.push_back(CreatesLabelId());
        edge_ids_.push_back(creates.GetEdgeId());
        src_ids_.push_back(creates.GetSrcId());
        src_labels_.push_back(PersonLabelId());
        dest_ids_.push_back(creates.GetDestId());
        dest_labels_.push_back(SoftwareLabelId());
        edge_properties_.push_back(creates.GetWeightProperty());
        edge_property_sizes_.push_back(1);
        return *this;
      }

      GraphRaii Build() {
        SchemaRaii schema_raii{CreateModernGraphSchema()};
        VineyardStoreTestGraphBuilder builder{kGraphName, schema_raii.get()};
        ::add_vertices(builder.GetGraphBuilder(),
              vertex_ids_.size(),vertex_ids_.data(),
              vertex_labels_.data(),
              vertex_property_sizes_.data(), vertex_properties_.data());
        ::add_edges(builder.GetGraphBuilder(),
              edge_ids_.size(), edge_ids_.data(),
              src_ids_.data(), dest_ids_.data(),
              edge_labels_.data(), src_labels_.data(), dest_labels_.data(),
              edge_property_sizes_.data(), edge_properties_.data());
        return builder.Build();
      }

     private:
      friend ModernGraph;
      vector<::VertexId> vertex_ids_;
      vector<::LabelId> vertex_labels_;
      vector<size_t> vertex_property_sizes_;
      vector<::Property> vertex_properties_;
      vector<::EdgeId> edge_ids_;
      vector<::VertexId> src_ids_;
      vector<::VertexId> dest_ids_;
      vector<::LabelId> edge_labels_;
      vector<::LabelId> src_labels_;
      vector<::LabelId> dest_labels_;
      vector<size_t> edge_property_sizes_;
      vector<::Property> edge_properties_;
    };

   private:
    static Schema CreateModernGraphSchema() {
      SchemaBuilder schema_builder;
      schema_builder.AddVertexType(PersonLabelId(), kPersonLabel)
          .AddProperty(kIdPropertyId, kIdPropertyName, kIdPropertyType)
          .AddProperty(kNamePropertyId, kNamePropertyName, kNamePropertyType)
          .Build();
      schema_builder.AddVertexType(SoftwareLabelId(), kSoftwareLabel)
          .AddProperty(kIdPropertyId, kIdPropertyName, kIdPropertyType)
          .AddProperty(kNamePropertyId, kNamePropertyName, kNamePropertyType)
          .AddProperty(kLanguagePropertyId, kLanguagePropertyName,
                       kLanguagePropertyType)
          .Build();
      schema_builder.AddEdgeType(KnowsLabelId(), kKnowsLabel)
          .AddProperty(kWeightPropertyId, kWeightPropertyName,
                       kWeightPropertyType)
          .Build();
      schema_builder.AddEdgeType(CreatesLabelId(), kCreatesLabel)
          .AddProperty(kWeightPropertyId, kWeightPropertyName,
                       kWeightPropertyType)
          .Build();
      auto schema = schema_builder.Build();
      return schema;
    }
  };

  struct DefaultModernGraph {
    ModernGraph::PersonVertex alice{1, 101, "alice"};
    ModernGraph::PersonVertex bob{2, 102, "bob"};
    ModernGraph::PersonVertex carol{3, 103, "carol"};

    ModernGraph::SoftwareVertex linux_os{ 4, 201, "linux", "c" };
    ModernGraph::SoftwareVertex tensorflow{ 5, 202, "tensorflow", "c++" };

    ModernGraph::KnowsEdge alice_knows_bob {1, alice, bob, 0.1f };
    ModernGraph::KnowsEdge alice_knows_carol {2, alice, carol, 0.2f };

    ModernGraph::CreatesEdge alice_creates_linux_os
        {3, alice, linux_os, 0.3f };
    ModernGraph::CreatesEdge alice_creates_tensorflow
        {4, alice, tensorflow, 0.4f };
    ModernGraph::CreatesEdge bob_creates_linux_os
        {5, bob, linux_os, 0.5f };
    ModernGraph::CreatesEdge carol_creates_tensorflow
        {6, carol, tensorflow, 0.6f };
  };

  class ModernGraphElementSchemas {
   public:
    ModernGraphElementSchemas(Schema schema) {
      // after saving a graph to vineyard, the label id and property id got
      // shuffled, need to read back from schema
      get_label_id(schema, ModernGraph::kPersonLabel, &person_label_id_);
      get_label_id(schema, ModernGraph::kSoftwareLabel, &software_label_id_);
      get_label_id(schema, ModernGraph::kKnowsLabel, &knows_label_id_);
      get_label_id(schema, ModernGraph::kCreatesLabel, &creates_label_id_);
      label_schema_map_.emplace(ModernGraph::PersonLabelId(),
          ModernGraph::PersonVertex::GetElementSchema(schema));
      label_schema_map_.emplace(ModernGraph::SoftwareLabelId(),
          ModernGraph::SoftwareVertex::GetElementSchema(schema));
      label_schema_map_.emplace(ModernGraph::KnowsLabelId(),
          ModernGraph::KnowsEdge::GetElementSchema(schema));
      label_schema_map_.emplace(ModernGraph::CreatesLabelId(),
          ModernGraph::CreatesEdge::GetElementSchema(schema));
    }

    ::LabelId GetPersonLabelId() const { return person_label_id_; }
    ::LabelId GetSoftwareLabelId() const { return software_label_id_; }
    ::LabelId GetKnowsLabelId() const { return knows_label_id_; }
    ::LabelId GetCreatesLabelId() const { return creates_label_id_; }


    const GraphElementSchema& GetElementSchema(::LabelId label_id) const {
      return label_schema_map_.find(label_id)->second;
    }

    const GraphElementSchema& GetPersonSchema() const {
      return GetElementSchema(ModernGraph::PersonLabelId());
    }

    const GraphElementSchema& GetSoftwareSchema() const {
      return GetElementSchema(ModernGraph::SoftwareLabelId());
    }

    const GraphElementSchema& GetKnowsSchema() const {
      return GetElementSchema(ModernGraph::KnowsLabelId());
    }
    const GraphElementSchema& GetCreatesSchema() const {
      return GetElementSchema(ModernGraph::CreatesLabelId());
    }

   private:
    ::LabelId person_label_id_;
    ::LabelId software_label_id_;
    ::LabelId knows_label_id_;
    ::LabelId creates_label_id_;
    unordered_map<::LabelId, GraphElementSchema> label_schema_map_;
  };
};

// vineyard cannot handle graph with no edges, disable such tests for now
TEST_F(ModernGraphTest, DISABLED_GetAllVerticesOfAllLabels) {
  DefaultModernGraph default_graph;
  ModernGraph::Builder modern_graph_builder;
  GraphRaii graph_raii = modern_graph_builder
      .AddPerson(default_graph.alice)
      .AddPerson(default_graph.bob)
      .AddPerson(default_graph.carol)
      .AddSoftware(default_graph.linux_os)
      .AddSoftware(default_graph.tensorflow)
      .Build();
  auto graph = graph_raii.get();
  ModernGraphElementSchemas schemas(get_schema(graph));

  auto person_label_id = schemas.GetPersonLabelId();
  auto software_label_id = schemas.GetSoftwareLabelId();
  unordered_multimap<::LabelId, VertexElement> label_vertex_map;
  GetAllVerticesIteratorRaii iterator_raii{
      get_all_vertices(graph, 0, nullptr,
            0, std::numeric_limits<int64_t>::max())};
  auto iter = iterator_raii.get();
  Vertex v;
  while (get_all_vertices_next(iter, &v) != -1) {
    auto label_id = get_vertex_label(graph, v);
    ASSERT_THAT(label_id,
                testing::AnyOf(person_label_id, software_label_id));
    VertexElement vertex_element(schemas.GetElementSchema(label_id));
    ReadVertexElement(graph, v, vertex_element);
    label_vertex_map.emplace(label_id, std::move(vertex_element));
  }

  VertexElement alice_ele(schemas.GetPersonSchema());
  ASSERT_NE(default_graph.alice.CopyToVertexElement(alice_ele), -1);
  VertexElement bob_ele(schemas.GetPersonSchema());
  ASSERT_NE(default_graph.bob.CopyToVertexElement(bob_ele), -1);
  VertexElement carol_ele(schemas.GetPersonSchema());
  ASSERT_NE(default_graph.carol.CopyToVertexElement(carol_ele), -1);
  VertexElement linux_os_ele(schemas.GetSoftwareSchema());
  ASSERT_NE(default_graph.linux_os.CopyToVertexElement(linux_os_ele),-1);
  VertexElement tensorflow_ele(schemas.GetSoftwareSchema());
  ASSERT_NE(default_graph.tensorflow.CopyToVertexElement(tensorflow_ele),-1);
  EXPECT_THAT(label_vertex_map, testing::UnorderedElementsAre(
      testing::Pair(person_label_id, std::move(alice_ele)),
      testing::Pair(person_label_id, std::move(bob_ele)),
      testing::Pair(person_label_id, std::move(carol_ele)),
      testing::Pair(software_label_id, std::move(linux_os_ele)),
      testing::Pair(software_label_id, std::move(tensorflow_ele))));
  // client.DelData(graph_id);
}

// vineyard cannot handle graph with no edges, disable such tests for now
TEST_F(ModernGraphTest, DISABLED_GetAllVerticesOfOneLabel) {
  DefaultModernGraph default_graph;
  ModernGraph::Builder modern_graph_builder;
  GraphRaii graph_raii = modern_graph_builder
      .AddPerson(default_graph.alice)
      .AddPerson(default_graph.bob)
      .AddPerson(default_graph.carol)
      .AddSoftware(default_graph.linux_os)
      .AddSoftware(default_graph.tensorflow)
      .Build();
  auto graph = graph_raii.get();
  ModernGraphElementSchemas schemas(get_schema(graph));

  auto person_label_id = schemas.GetPersonLabelId();
  vector<VertexElement> vertex_elements;
  GetAllVerticesIteratorRaii iterator_raii{
      get_all_vertices(graph, 0, &person_label_id,
                       1, std::numeric_limits<int64_t>::max())};
  auto iter = iterator_raii.get();
  Vertex v;
  while (get_all_vertices_next(iter, &v) != -1) {
    auto label_id = get_vertex_label(graph, v);
    EXPECT_EQ(label_id, person_label_id);
    VertexElement vertex_element(schemas.GetPersonSchema());
    ReadVertexElement(graph, v, vertex_element);
    vertex_elements.push_back(std::move(vertex_element));
  }

  VertexElement alice_ele(schemas.GetPersonSchema());
  ASSERT_NE(default_graph.alice.CopyToVertexElement(alice_ele), -1);
  VertexElement bob_ele(schemas.GetPersonSchema());
  ASSERT_NE(default_graph.bob.CopyToVertexElement(bob_ele), -1);
  VertexElement carol_ele(schemas.GetPersonSchema());
  ASSERT_NE(default_graph.carol.CopyToVertexElement(carol_ele), -1);
  EXPECT_THAT(vertex_elements, testing::UnorderedElementsAre(
      alice_ele,
      bob_ele,
      carol_ele
  ));
  // client.DelData(graph_id);
}

TEST_F(ModernGraphTest, GetAllEdgesOfAllLabels) {
  DefaultModernGraph default_graph;
  ModernGraph::Builder modern_graph_builder;
  GraphRaii graph_raii = modern_graph_builder
      .AddPerson(default_graph.alice)
      .AddPerson(default_graph.bob)
      .AddPerson(default_graph.carol)
      .AddSoftware(default_graph.linux_os)
      .AddSoftware(default_graph.tensorflow)
      .AddKnows(default_graph.alice_knows_bob)
      .AddKnows(default_graph.alice_knows_carol)
      .AddCreates(default_graph.alice_creates_linux_os)
      .AddCreates(default_graph.alice_creates_tensorflow)
      .AddCreates(default_graph.bob_creates_linux_os)
      .AddCreates(default_graph.carol_creates_tensorflow)
      .Build();
  auto graph = graph_raii.get();
  ModernGraphElementSchemas schemas(get_schema(graph));
  auto knows_label_id = schemas.GetKnowsLabelId();
  auto creates_label_id = schemas.GetCreatesLabelId();

  unordered_multimap<::LabelId, EdgeElement> label_edge_map;
  GetAllEdgesIteratorRaii iterator_raii{
      get_all_edges(graph, 0, nullptr,
                       0, std::numeric_limits<int64_t>::max())};
  auto iter = iterator_raii.get();
  struct Edge edge;
  while (get_all_edges_next(iter, &edge) != -1) {
    auto label_id = get_edge_label(graph, &edge);
    ASSERT_THAT(label_id,
                testing::AnyOf(knows_label_id, creates_label_id));
    EdgeElement edge_element(schemas.GetElementSchema(label_id));
    ReadEdgeElement(graph, &edge, edge_element);
    label_edge_map.emplace(label_id, std::move(edge_element));
  }

  EdgeElement alice_knows_bob_ele(schemas.GetKnowsSchema());
  ASSERT_NE(
      default_graph.alice_knows_bob.CopyToEdgeElement(alice_knows_bob_ele),
      -1);
  EdgeElement alice_knows_carol_ele(schemas.GetKnowsSchema());
  ASSERT_NE(
      default_graph.alice_knows_carol.CopyToEdgeElement(alice_knows_carol_ele),
      -1);
  EdgeElement alice_creates_linux_os_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.alice_creates_linux_os.CopyToEdgeElement(
              alice_creates_linux_os_ele),
      -1);
  EdgeElement alice_creates_tensorflow_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.alice_creates_tensorflow.CopyToEdgeElement(
              alice_creates_tensorflow_ele),
      -1);
  EdgeElement bob_creates_linux_os_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.bob_creates_linux_os.CopyToEdgeElement(
              bob_creates_linux_os_ele),
      -1);
  EdgeElement carol_creates_tensorflow_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.carol_creates_tensorflow.CopyToEdgeElement(
              carol_creates_tensorflow_ele),
      -1);
  using ::testing::Pair;
  EXPECT_THAT(label_edge_map, testing::UnorderedElementsAre(
      Pair(knows_label_id, std::move(alice_knows_bob_ele)),
      Pair(knows_label_id, std::move(alice_knows_carol_ele)),
      Pair(creates_label_id, std::move(alice_creates_linux_os_ele)),
      Pair(creates_label_id, std::move(alice_creates_tensorflow_ele)),
      Pair(creates_label_id, std::move(bob_creates_linux_os_ele)),
      Pair(creates_label_id, std::move(carol_creates_tensorflow_ele))));
  // client.DelData(graph_id);
}

TEST_F(ModernGraphTest, GetAllEdgesOfOneLabel) {
  DefaultModernGraph default_graph;
  ModernGraph::Builder modern_graph_builder;
  GraphRaii graph_raii = modern_graph_builder
      .AddPerson(default_graph.alice)
      .AddPerson(default_graph.bob)
      .AddPerson(default_graph.carol)
      .AddSoftware(default_graph.linux_os)
      .AddSoftware(default_graph.tensorflow)
      .AddKnows(default_graph.alice_knows_bob)
      .AddKnows(default_graph.alice_knows_carol)
      .AddCreates(default_graph.alice_creates_linux_os)
      .AddCreates(default_graph.alice_creates_tensorflow)
      .AddCreates(default_graph.bob_creates_linux_os)
      .AddCreates(default_graph.carol_creates_tensorflow)
      .Build();
  auto graph = graph_raii.get();
  ModernGraphElementSchemas schemas(get_schema(graph));
  auto knows_label_id = schemas.GetKnowsLabelId();

  vector<EdgeElement> edge_elements;
  GetAllEdgesIteratorRaii iterator_raii{
      get_all_edges(graph, 0, &knows_label_id,
                    1, std::numeric_limits<int64_t>::max())};
  auto iter = iterator_raii.get();
  struct Edge edge;
  while (get_all_edges_next(iter, &edge) != -1) {
    auto label_id = get_edge_label(graph, &edge);
    ASSERT_EQ(label_id, knows_label_id);
    EdgeElement edge_element(schemas.GetElementSchema(label_id));
    ReadEdgeElement(graph, &edge, edge_element);
    edge_elements.push_back(std::move(edge_element));
  }

  EdgeElement alice_knows_bob_ele(schemas.GetKnowsSchema());
  ASSERT_NE(
      default_graph.alice_knows_bob.CopyToEdgeElement(alice_knows_bob_ele),
      -1);
  EdgeElement alice_knows_carol_ele(schemas.GetKnowsSchema());
  ASSERT_NE(
      default_graph.alice_knows_carol.CopyToEdgeElement(alice_knows_carol_ele),
      -1);
  EXPECT_THAT(edge_elements, testing::UnorderedElementsAre(
      alice_knows_bob_ele, alice_knows_carol_ele));
  // client.DelData(graph_id);
}

TEST_F(ModernGraphTest, GetOutEdgesOfAllLabels) {
  DefaultModernGraph default_graph;
  ModernGraph::Builder modern_graph_builder;
  GraphRaii graph_raii = modern_graph_builder
      .AddPerson(default_graph.alice)
      .AddPerson(default_graph.bob)
      .AddPerson(default_graph.carol)
      .AddSoftware(default_graph.linux_os)
      .AddSoftware(default_graph.tensorflow)
      .AddKnows(default_graph.alice_knows_bob)
      .AddKnows(default_graph.alice_knows_carol)
      .AddCreates(default_graph.alice_creates_linux_os)
      .AddCreates(default_graph.alice_creates_tensorflow)
      .AddCreates(default_graph.bob_creates_linux_os)
      .AddCreates(default_graph.carol_creates_tensorflow)
      .Build();
  auto graph = graph_raii.get();
  ModernGraphElementSchemas schemas(get_schema(graph));
  auto person_label_id = schemas.GetPersonLabelId();
  auto knows_label_id = schemas.GetKnowsLabelId();
  auto creates_label_id = schemas.GetCreatesLabelId();

  ::Vertex alice_v;
  ASSERT_NE(-1, get_vertex_by_outer_id(graph, person_label_id,
                   default_graph.alice.GetVertexId(), &alice_v));
  unordered_multimap<::LabelId, EdgeElement> label_edge_map;
  OutEdgeIteratorRaii iterator_raii{
      get_out_edges(graph, 0, get_vertex_id(graph, alice_v),
                    nullptr, 0, std::numeric_limits<int64_t>::max())};
  auto iter = iterator_raii.get();
  struct Edge edge;
  while (out_edge_next(iter, &edge) != -1) {
    auto label_id = get_edge_label(graph, &edge);
    ASSERT_THAT(label_id,
                testing::AnyOf(knows_label_id, creates_label_id));
    EdgeElement edge_element(schemas.GetElementSchema(label_id));
    ReadEdgeElement(graph, &edge, edge_element);
    label_edge_map.emplace(label_id, std::move(edge_element));
  }

  EdgeElement alice_knows_bob_ele(schemas.GetKnowsSchema());
  ASSERT_NE(
      default_graph.alice_knows_bob.CopyToEdgeElement(alice_knows_bob_ele),
      -1);
  EdgeElement alice_knows_carol_ele(schemas.GetKnowsSchema());
  ASSERT_NE(
      default_graph.alice_knows_carol.CopyToEdgeElement(alice_knows_carol_ele),
      -1);
  EdgeElement alice_creates_linux_os_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.alice_creates_linux_os.CopyToEdgeElement(
          alice_creates_linux_os_ele),
      -1);
  EdgeElement alice_creates_tensorflow_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.alice_creates_tensorflow.CopyToEdgeElement(
          alice_creates_tensorflow_ele),
      -1);
  using ::testing::Pair;
  EXPECT_THAT(label_edge_map, testing::UnorderedElementsAre(
      Pair(knows_label_id, std::move(alice_knows_bob_ele)),
      Pair(knows_label_id, std::move(alice_knows_carol_ele)),
      Pair(creates_label_id, std::move(alice_creates_linux_os_ele)),
      Pair(creates_label_id, std::move(alice_creates_tensorflow_ele))));
  // client.DelData(graph_id);
}

TEST_F(ModernGraphTest, GetOutEdgesOfOneLabel) {
  DefaultModernGraph default_graph;
  ModernGraph::Builder modern_graph_builder;
  GraphRaii graph_raii = modern_graph_builder
      .AddPerson(default_graph.alice)
      .AddPerson(default_graph.bob)
      .AddPerson(default_graph.carol)
      .AddSoftware(default_graph.linux_os)
      .AddSoftware(default_graph.tensorflow)
      .AddKnows(default_graph.alice_knows_bob)
      .AddKnows(default_graph.alice_knows_carol)
      .AddCreates(default_graph.alice_creates_linux_os)
      .AddCreates(default_graph.alice_creates_tensorflow)
      .AddCreates(default_graph.bob_creates_linux_os)
      .AddCreates(default_graph.carol_creates_tensorflow)
      .Build();
  auto graph = graph_raii.get();
  ModernGraphElementSchemas schemas(get_schema(graph));
  auto person_label_id = schemas.GetPersonLabelId();
  auto creates_label_id = schemas.GetCreatesLabelId();

  ::Vertex alice_v;
  ASSERT_NE(-1, get_vertex_by_outer_id(graph, person_label_id,
                                       default_graph.alice.GetVertexId(), &alice_v));
  vector<EdgeElement> edge_elements;
  OutEdgeIteratorRaii iterator_raii{
      get_out_edges(graph, 0, get_vertex_id(graph, alice_v),
          &creates_label_id, 1, std::numeric_limits<int64_t>::max())};
  auto iter = iterator_raii.get();
  struct Edge edge;
  while (out_edge_next(iter, &edge) != -1) {
    auto label_id = get_edge_label(graph, &edge);
    ASSERT_EQ(label_id,creates_label_id);
    EdgeElement edge_element(schemas.GetElementSchema(label_id));
    ReadEdgeElement(graph, &edge, edge_element);
    edge_elements.push_back(std::move(edge_element));
  }

  EdgeElement alice_creates_linux_os_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.alice_creates_linux_os.CopyToEdgeElement(
          alice_creates_linux_os_ele),
      -1);
  EdgeElement alice_creates_tensorflow_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.alice_creates_tensorflow.CopyToEdgeElement(
          alice_creates_tensorflow_ele),
      -1);
  EXPECT_THAT(edge_elements,
              testing::UnorderedElementsAre(
                  alice_creates_linux_os_ele, alice_creates_tensorflow_ele));
  // client.DelData(graph_id);
}

TEST_F(ModernGraphTest, GetInEdgesOfAllLabels) {
  DefaultModernGraph default_graph;
  ModernGraph::Builder modern_graph_builder;
  GraphRaii graph_raii = modern_graph_builder
      .AddPerson(default_graph.alice)
      .AddPerson(default_graph.bob)
      .AddPerson(default_graph.carol)
      .AddSoftware(default_graph.linux_os)
      .AddSoftware(default_graph.tensorflow)
      .AddKnows(default_graph.alice_knows_bob)
      .AddKnows(default_graph.alice_knows_carol)
      .AddCreates(default_graph.alice_creates_linux_os)
      .AddCreates(default_graph.alice_creates_tensorflow)
      .AddCreates(default_graph.bob_creates_linux_os)
      .AddCreates(default_graph.carol_creates_tensorflow)
      .Build();
  auto graph = graph_raii.get();
  ModernGraphElementSchemas schemas(get_schema(graph));
  auto software_label_id = schemas.GetSoftwareLabelId();
  auto creates_label_id = schemas.GetCreatesLabelId();

  ::Vertex tensorflow_v;
  ASSERT_NE(-1,
            get_vertex_by_outer_id(graph, software_label_id,
                    default_graph.tensorflow.GetVertexId(), &tensorflow_v));
  vector<EdgeElement> edge_elements;
  InEdgeIteratorRaii iterator_raii{
      get_in_edges(graph, 0, get_vertex_id(graph, tensorflow_v),
                    nullptr, 0, std::numeric_limits<int64_t>::max())};
  auto iter = iterator_raii.get();
  struct Edge edge;
  while (in_edge_next(iter, &edge) != -1) {
    auto label_id = get_edge_label(graph, &edge);
    ASSERT_EQ(label_id,creates_label_id);
    EdgeElement edge_element(schemas.GetElementSchema(label_id));
    ReadEdgeElement(graph, &edge, edge_element);
    edge_elements.push_back(std::move(edge_element));
  }

  EdgeElement alice_creates_tensorflow_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.alice_creates_tensorflow.CopyToEdgeElement(
          alice_creates_tensorflow_ele),
      -1);
  EdgeElement carol_creates_tensorflow_ele(schemas.GetCreatesSchema());
  ASSERT_NE(
      default_graph.carol_creates_tensorflow.CopyToEdgeElement(
          carol_creates_tensorflow_ele),
      -1);
  EXPECT_THAT(edge_elements,
              testing::UnorderedElementsAre(
                  alice_creates_tensorflow_ele, carol_creates_tensorflow_ele));
  // client.DelData(graph_id);
}

TEST_F(ModernGraphTest, GetInEdgesOfOneLabel) {
  DefaultModernGraph default_graph;
  ModernGraph::Builder modern_graph_builder;
  GraphRaii graph_raii = modern_graph_builder
      .AddPerson(default_graph.alice)
      .AddPerson(default_graph.bob)
      .AddPerson(default_graph.carol)
      .AddSoftware(default_graph.linux_os)
      .AddSoftware(default_graph.tensorflow)
      .AddKnows(default_graph.alice_knows_bob)
      .AddKnows(default_graph.alice_knows_carol)
      .AddCreates(default_graph.alice_creates_linux_os)
      .AddCreates(default_graph.alice_creates_tensorflow)
      .AddCreates(default_graph.bob_creates_linux_os)
      .AddCreates(default_graph.carol_creates_tensorflow)
      .Build();
  auto graph = graph_raii.get();
  ModernGraphElementSchemas schemas(get_schema(graph));
  auto software_label_id = schemas.GetSoftwareLabelId();
  auto knows_label_id = schemas.GetKnowsLabelId();

  ::Vertex tensorflow_v;
  ASSERT_NE(-1,
            get_vertex_by_outer_id(graph, software_label_id,
                                   default_graph.tensorflow.GetVertexId(), &tensorflow_v));
  InEdgeIteratorRaii iterator_raii{
      get_in_edges(graph, 0, get_vertex_id(graph, tensorflow_v),
                   &knows_label_id, 1, std::numeric_limits<int64_t>::max())};

  struct Edge edge;
  ASSERT_EQ(in_edge_next(iterator_raii.get(), &edge), -1);

  // client.DelData(graph_id);
}

}

// unfortunately, Property is defined in global namespace, so operator << needs
// to be in global namespace for adl to kick in
std::ostream& operator<<(std::ostream& os, const ::Property& property) {
  os << "(type: " << property.type << ", value: ";
  vineyard_store_test::PrintPropertyValue(property, os);
  os << ")";
  return os;
}