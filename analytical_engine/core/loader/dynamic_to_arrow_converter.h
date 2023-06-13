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

#ifndef ANALYTICAL_ENGINE_CORE_LOADER_DYNAMIC_TO_ARROW_CONVERTER_H_
#define ANALYTICAL_ENGINE_CORE_LOADER_DYNAMIC_TO_ARROW_CONVERTER_H_

#ifdef NETWORKX

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/utils/table_shuffler.h"

#include "core/error.h"
#include "core/fragment/dynamic_fragment.h"
#include "core/loader/arrow_fragment_loader.h"

namespace bl = boost::leaf;

namespace gs {

/**
 * @brief A util to extract data on the vertices to arrow array from
 * DynamicFragment
 * @tparam T data type
 */
template <typename T>
struct VertexArrayBuilder {};

/**
 * @brief This is a specialized VertexArrayBuilder for arrow::Int64Builder
 */
template <>
struct VertexArrayBuilder<arrow::Int64Builder> {
  static bl::result<std::shared_ptr<arrow::Array>> build(
      const std::shared_ptr<DynamicFragment>& src_frag, std::string& prop_key) {
    arrow::Int64Builder builder;
    std::shared_ptr<arrow::Array> array;

    for (const auto& u : src_frag->InnerVertices()) {
      if (!src_frag->IsAliveInnerVertex(u)) {
        continue;
      }
      auto& data = src_frag->GetData(u);

      if (data.HasMember(prop_key) == 0) {
        ARROW_OK_OR_RAISE(builder.AppendNull());
      } else {
        ARROW_OK_OR_RAISE(builder.Append(data[prop_key].GetInt64()));
      }
    }

    ARROW_OK_OR_RAISE(builder.Finish(&array));
    return array;
  }
};

/**
 * @brief This is a specialized VertexArrayBuilder for arrow::DoubleBuilder
 */
template <>
struct VertexArrayBuilder<arrow::DoubleBuilder> {
  static bl::result<std::shared_ptr<arrow::Array>> build(
      const std::shared_ptr<DynamicFragment>& src_frag, std::string& prop_key) {
    arrow::DoubleBuilder builder;
    std::shared_ptr<arrow::Array> array;

    for (const auto& u : src_frag->InnerVertices()) {
      if (!src_frag->IsAliveInnerVertex(u)) {
        continue;
      }

      auto& data = src_frag->GetData(u);

      if (data.HasMember(prop_key) == 0) {
        ARROW_OK_OR_RAISE(builder.AppendNull());
      } else {
        ARROW_OK_OR_RAISE(builder.Append(data[prop_key].GetDouble()));
      }
    }

    ARROW_OK_OR_RAISE(builder.Finish(&array));
    return array;
  }
};

/**
 * @brief This is a specialized VertexArrayBuilder for arrow::LargeStringBuilder
 */
template <>
struct VertexArrayBuilder<arrow::LargeStringBuilder> {
  static bl::result<std::shared_ptr<arrow::Array>> build(
      const std::shared_ptr<DynamicFragment>& src_frag, std::string& prop_key) {
    arrow::LargeStringBuilder builder;
    std::shared_ptr<arrow::Array> array;

    for (const auto& u : src_frag->InnerVertices()) {
      if (!src_frag->IsAliveInnerVertex(u)) {
        continue;
      }

      auto& data = src_frag->GetData(u);
      if (data.HasMember(prop_key) == 0) {
        ARROW_OK_OR_RAISE(builder.AppendNull());
      } else {
        ARROW_OK_OR_RAISE(builder.Append(data[prop_key].GetString()));
      }
    }

    ARROW_OK_OR_RAISE(builder.Finish(&array));
    return array;
  }
};

/**
 * @brief A util to extract data on the edges to arrow array from
 * DynamicFragment
 * @tparam T data type
 */
template <typename T>
struct EdgeArrayBuilder {};

/**
 * @brief This is a specialized EdgeArrayBuilder for arrow::Int64Builder
 */
template <>
struct EdgeArrayBuilder<arrow::Int64Builder> {
  static bl::result<std::shared_ptr<arrow::Array>> build(
      const std::shared_ptr<DynamicFragment>& src_frag, std::string& prop_key) {
    auto inner_vertices = src_frag->InnerVertices();
    arrow::Int64Builder builder;
    std::shared_ptr<arrow::Array> array;

    for (const auto& u : inner_vertices) {
      if (!src_frag->IsAliveInnerVertex(u)) {
        continue;
      }
      for (auto& e : src_frag->GetOutgoingAdjList(u)) {
        if (!src_frag->directed() && (u.GetValue() > e.neighbor.GetValue())) {
          continue;  // if src_frag is undirected, just append one edge.
        }

        auto& data = e.data;
        if (data.HasMember(prop_key) == 0) {
          ARROW_OK_OR_RAISE(builder.AppendNull());
        } else {
          ARROW_OK_OR_RAISE(builder.Append(data[prop_key].GetInt64()));
        }
      }
      if (src_frag->directed()) {
        for (auto& e : src_frag->GetIncomingAdjList(u)) {
          auto& v = e.neighbor;
          if (src_frag->IsOuterVertex(v)) {
            auto& data = e.data;
            if (data.HasMember(prop_key) == 0) {
              ARROW_OK_OR_RAISE(builder.AppendNull());
            } else {
              ARROW_OK_OR_RAISE(builder.Append(data[prop_key].GetInt64()));
            }
          }
        }
      }
    }

    ARROW_OK_OR_RAISE(builder.Finish(&array));
    return array;
  }
};

/**
 * @brief This is a specialized EdgeArrayBuilder for arrow::DoubleBuilder
 */
template <>
struct EdgeArrayBuilder<arrow::DoubleBuilder> {
  static bl::result<std::shared_ptr<arrow::Array>> build(
      const std::shared_ptr<DynamicFragment>& src_frag, std::string& prop_key) {
    auto inner_vertices = src_frag->InnerVertices();
    arrow::DoubleBuilder builder;
    std::shared_ptr<arrow::Array> array;
    for (const auto& u : inner_vertices) {
      if (!src_frag->IsAliveInnerVertex(u)) {
        continue;
      }
      for (auto& e : src_frag->GetOutgoingAdjList(u)) {
        if (!src_frag->directed() && (u.GetValue() > e.neighbor.GetValue())) {
          continue;  // if src_frag is undirected, just append one edge.
        }

        auto& data = e.data;
        if (data.HasMember(prop_key) == 0) {
          ARROW_OK_OR_RAISE(builder.AppendNull());
        } else {
          ARROW_OK_OR_RAISE(builder.Append(data[prop_key].GetDouble()));
        }
      }
      if (src_frag->directed()) {
        for (auto& e : src_frag->GetIncomingAdjList(u)) {
          auto& v = e.neighbor;
          if (src_frag->IsOuterVertex(v)) {
            auto& data = e.data;
            if (data.HasMember(prop_key) == 0) {
              ARROW_OK_OR_RAISE(builder.AppendNull());
            } else {
              ARROW_OK_OR_RAISE(builder.Append(data[prop_key].GetDouble()));
            }
          }
        }
      }
    }

    ARROW_OK_OR_RAISE(builder.Finish(&array));
    return array;
  }
};

/**
 * @brief This is a specialized EdgeArrayBuilder for arrow::LargeStringBuilder
 */
template <>
struct EdgeArrayBuilder<arrow::LargeStringBuilder> {
  static bl::result<std::shared_ptr<arrow::Array>> build(
      const std::shared_ptr<DynamicFragment>& src_frag, std::string& prop_key) {
    auto inner_vertices = src_frag->InnerVertices();
    arrow::LargeStringBuilder builder;
    std::shared_ptr<arrow::Array> array;

    for (const auto& u : inner_vertices) {
      if (!src_frag->IsAliveInnerVertex(u)) {
        continue;
      }
      for (auto& e : src_frag->GetOutgoingAdjList(u)) {
        if (!src_frag->directed() && (u.GetValue() > e.neighbor.GetValue())) {
          continue;  // if src_frag is undirected, just append one edge.
        }

        auto& data = e.data;
        if (data.HasMember(prop_key) == 0) {
          ARROW_OK_OR_RAISE(builder.AppendNull());
        } else {
          ARROW_OK_OR_RAISE(builder.Append(data[prop_key].GetString()));
        }
      }
      if (src_frag->directed()) {
        for (auto& e : src_frag->GetIncomingAdjList(u)) {
          auto& v = e.neighbor;
          if (src_frag->IsOuterVertex(v)) {
            auto& data = e.data;
            if (data.HasMember(prop_key) == 0) {
              ARROW_OK_OR_RAISE(builder.AppendNull());
            } else {
              ARROW_OK_OR_RAISE(builder.Append(data[prop_key].GetString()));
            }
          }
        }
      }
    }

    ARROW_OK_OR_RAISE(builder.Finish(&array));
    return array;
  }
};

/**
 * @brief A util to build src and dst oid array from DynamicFragment
 * @tparam DST_FRAG_T The type of destination fragment
 * @tparam OID_T OID type
 */
template <typename DST_FRAG_T, typename OID_T>
struct COOBuilder {};

/**
 * @brief This is a specialized EdgeArrayBuilder for int32_t
 * @tparam DST_FRAG_T
 */
template <typename DST_FRAG_T>
struct COOBuilder<DST_FRAG_T, int32_t> {
  using oid_t = int32_t;
  using src_fragment_t = DynamicFragment;
  using dst_fragment_t = DST_FRAG_T;

  auto Build(
      const std::shared_ptr<src_fragment_t>& src_frag,
      const std::shared_ptr<typename dst_fragment_t::vertex_map_t>& dst_vm)
      -> bl::result<std::pair<std::shared_ptr<arrow::Array>,
                              std::shared_ptr<arrow::Array>>> {
    auto fid = src_frag->fid();

    auto inner_vertices = src_frag->InnerVertices();
    arrow::UInt64Builder src_builder, dst_builder;
    std::shared_ptr<arrow::Array> src_array, dst_array;

    for (const auto& u : inner_vertices) {
      if (!src_frag->IsAliveInnerVertex(u)) {
        continue;
      }
      auto u_oid = src_frag->GetId(u);
      vineyard::property_graph_types::VID_TYPE u_gid;

      CHECK(dst_vm->GetGid(fid, 0, u_oid.GetInt(), u_gid));

      for (auto& e : src_frag->GetOutgoingAdjList(u)) {
        auto& v = e.neighbor;
        if (!src_frag->directed() && u.GetValue() > v.GetValue()) {
          continue;
        }
        auto v_oid = src_frag->GetId(v);
        vineyard::property_graph_types::VID_TYPE v_gid;

        CHECK(dst_vm->GetGid(0, v_oid.GetInt(), v_gid));
        ARROW_OK_OR_RAISE(src_builder.Append(u_gid));
        ARROW_OK_OR_RAISE(dst_builder.Append(v_gid));
      }
      if (src_frag->directed()) {
        for (auto& e : src_frag->GetIncomingAdjList(u)) {
          auto& v = e.neighbor;
          if (src_frag->IsOuterVertex(v)) {
            auto v_oid = src_frag->GetId(v);
            vineyard::property_graph_types::VID_TYPE v_gid;

            CHECK(dst_vm->GetGid(0, v_oid.GetInt(), v_gid));
            ARROW_OK_OR_RAISE(src_builder.Append(v_gid));
            ARROW_OK_OR_RAISE(dst_builder.Append(u_gid));
          }
        }
      }
    }

    ARROW_OK_OR_RAISE(src_builder.Finish(&src_array));
    ARROW_OK_OR_RAISE(dst_builder.Finish(&dst_array));

    return std::make_pair(src_array, dst_array);
  }
};

/**
 * @brief This is a specialized EdgeArrayBuilder for int64_t
 * @tparam DST_FRAG_T
 */
template <typename DST_FRAG_T>
struct COOBuilder<DST_FRAG_T, int64_t> {
  using oid_t = int64_t;
  using src_fragment_t = DynamicFragment;
  using dst_fragment_t = DST_FRAG_T;

  auto Build(
      const std::shared_ptr<src_fragment_t>& src_frag,
      const std::shared_ptr<typename dst_fragment_t::vertex_map_t>& dst_vm)
      -> bl::result<std::pair<std::shared_ptr<arrow::Array>,
                              std::shared_ptr<arrow::Array>>> {
    auto fid = src_frag->fid();

    auto inner_vertices = src_frag->InnerVertices();
    arrow::UInt64Builder src_builder, dst_builder;
    std::shared_ptr<arrow::Array> src_array, dst_array;

    for (const auto& u : inner_vertices) {
      if (!src_frag->IsAliveInnerVertex(u)) {
        continue;
      }
      auto u_oid = src_frag->GetId(u);
      vineyard::property_graph_types::VID_TYPE u_gid;

      CHECK(dst_vm->GetGid(fid, 0, u_oid.GetInt64(), u_gid));

      for (auto& e : src_frag->GetOutgoingAdjList(u)) {
        auto& v = e.neighbor;
        if (!src_frag->directed() && u.GetValue() > v.GetValue()) {
          continue;
        }
        auto v_oid = src_frag->GetId(v);
        vineyard::property_graph_types::VID_TYPE v_gid;

        CHECK(dst_vm->GetGid(0, v_oid.GetInt64(), v_gid));
        ARROW_OK_OR_RAISE(src_builder.Append(u_gid));
        ARROW_OK_OR_RAISE(dst_builder.Append(v_gid));
      }
      if (src_frag->directed()) {
        for (auto& e : src_frag->GetIncomingAdjList(u)) {
          auto& v = e.neighbor;
          if (src_frag->IsOuterVertex(v)) {
            auto v_oid = src_frag->GetId(v);
            vineyard::property_graph_types::VID_TYPE v_gid;

            CHECK(dst_vm->GetGid(0, v_oid.GetInt64(), v_gid));
            ARROW_OK_OR_RAISE(src_builder.Append(v_gid));
            ARROW_OK_OR_RAISE(dst_builder.Append(u_gid));
          }
        }
      }
    }

    ARROW_OK_OR_RAISE(src_builder.Finish(&src_array));
    ARROW_OK_OR_RAISE(dst_builder.Finish(&dst_array));

    return std::make_pair(src_array, dst_array);
  }
};

/**
 * @brief This is a specialized EdgeArrayBuilder for std::string
 * @tparam DST_FRAG_T
 */
template <typename DST_FRAG_T>
struct COOBuilder<DST_FRAG_T, std::string> {
  using oid_t = int64_t;
  using src_fragment_t = DynamicFragment;
  using dst_fragment_t = DST_FRAG_T;

  auto Build(
      const std::shared_ptr<src_fragment_t>& src_frag,
      const std::shared_ptr<typename dst_fragment_t::vertex_map_t>& dst_vm)
      -> bl::result<std::pair<std::shared_ptr<arrow::Array>,
                              std::shared_ptr<arrow::Array>>> {
    auto fid = src_frag->fid();

    auto inner_vertices = src_frag->InnerVertices();
    arrow::UInt64Builder src_builder, dst_builder;
    std::shared_ptr<arrow::Array> src_array, dst_array;

    for (const auto& u : inner_vertices) {
      if (!src_frag->IsAliveInnerVertex(u)) {
        continue;
      }
      auto u_oid = src_frag->GetId(u);
      vineyard::property_graph_types::VID_TYPE u_gid;

      CHECK(dst_vm->GetGid(fid, 0, u_oid.GetString(), u_gid));

      for (auto& e : src_frag->GetOutgoingAdjList(u)) {
        auto& v = e.neighbor;
        if (!src_frag->directed() && u.GetValue() > v.GetValue()) {
          continue;
        }
        auto v_oid = src_frag->GetId(v);
        vineyard::property_graph_types::VID_TYPE v_gid;

        CHECK(dst_vm->GetGid(0, v_oid.GetString(), v_gid));
        ARROW_OK_OR_RAISE(src_builder.Append(u_gid));
        ARROW_OK_OR_RAISE(dst_builder.Append(v_gid));
      }
      if (src_frag->directed()) {
        for (auto& e : src_frag->GetIncomingAdjList(u)) {
          auto& v = e.neighbor;
          if (src_frag->IsOuterVertex(v)) {
            auto v_oid = src_frag->GetId(v);
            vineyard::property_graph_types::VID_TYPE v_gid;

            CHECK(dst_vm->GetGid(0, v_oid.GetString(), v_gid));
            ARROW_OK_OR_RAISE(src_builder.Append(v_gid));
            ARROW_OK_OR_RAISE(dst_builder.Append(u_gid));
          }
        }
      }
    }

    ARROW_OK_OR_RAISE(src_builder.Finish(&src_array));
    ARROW_OK_OR_RAISE(dst_builder.Finish(&dst_array));

    return std::make_pair(src_array, dst_array);
  }
};

/**
 * @brief VertexMapConverter is intended to build ArrowVertexMap from
 * DynamicFragment
 * @tparam OID_T OID type
 */
template <typename VERTEX_MAP_T>
struct VertexMapConverter {
  explicit VertexMapConverter(const grape::CommSpec& comm_spec,
                              vineyard::Client& client) {}

  bl::result<vineyard::ObjectID> Convert(
      const std::shared_ptr<DynamicFragment>& dynamic_frag) {
    RETURN_GS_ERROR(
        vineyard::ErrorCode::kUnimplementedMethod,
        ""
        "Unimplemented vertex map converter for the vertex map type");
  }
};

/**
 * @brief This is a specialized VertexMapConverter for int64_t
 */
template <>
class VertexMapConverter<vineyard::ArrowVertexMap<int64_t, uint64_t>> {
  using oid_t = int64_t;
  using origin_oid_t = typename DynamicFragment::oid_t;
  using vid_t = typename DynamicFragment::vid_t;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using oid_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;

 public:
  explicit VertexMapConverter(const grape::CommSpec& comm_spec,
                              vineyard::Client& client)
      : comm_spec_(comm_spec), client_(client) {}

  bl::result<vineyard::ObjectID> Convert(
      const std::shared_ptr<DynamicFragment>& dynamic_frag) {
    // label_id->frag_id->oid array
    std::vector<std::vector<std::shared_ptr<oid_array_t>>> oid_lists(1);
    std::shared_ptr<oid_array_t> local_oid_array;
    const auto& vm_ptr = dynamic_frag->GetVertexMap();
    auto fid = dynamic_frag->fid();
    auto fnum = dynamic_frag->fnum();
    origin_oid_t origin_id;
    oid_builder_t builder;
    for (auto& v : dynamic_frag->InnerVertices()) {
      if (!dynamic_frag->IsAliveInnerVertex(v)) {
        continue;
      }
      CHECK(vm_ptr->GetOid(fid, v.GetValue(), origin_id));
      CHECK(origin_id.IsInt64());
      ARROW_OK_OR_RAISE(builder.Append(origin_id.GetInt64()));
    }
    ARROW_OK_OR_RAISE(builder.Finish(&local_oid_array));

    VY_OK_OR_RAISE(vineyard::FragmentAllGatherArray<oid_array_t>(
        comm_spec_, local_oid_array, oid_lists[0]));

    vineyard::BasicArrowVertexMapBuilder<
        typename vineyard::InternalType<oid_t>::type, vid_t>
        vm_builder(client_, fnum, oid_lists.size(), oid_lists);
    auto vm = vm_builder.Seal(client_);
    return vm->id();
  }

 private:
  grape::CommSpec comm_spec_;
  vineyard::Client& client_;
};

/**
 * @brief This is a specialized VertexMapConverter for std::string
 */
template <>
class VertexMapConverter<vineyard::ArrowVertexMap<
    typename vineyard::InternalType<std::string>::type, uint64_t>> {
  using oid_t = std::string;
  using origin_oid_t = typename DynamicFragment::oid_t;
  using vid_t = typename DynamicFragment::vid_t;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using oid_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;

 public:
  explicit VertexMapConverter(const grape::CommSpec& comm_spec,
                              vineyard::Client& client)
      : comm_spec_(comm_spec), client_(client) {}

  bl::result<vineyard::ObjectID> Convert(
      const std::shared_ptr<DynamicFragment>& dynamic_frag) {
    // label_id->frag_id->oid array
    std::vector<std::vector<std::shared_ptr<oid_array_t>>> oid_lists(1);
    std::shared_ptr<oid_array_t> local_oid_array;
    const auto& vm_ptr = dynamic_frag->GetVertexMap();
    auto fid = dynamic_frag->fid();
    auto fnum = dynamic_frag->fnum();
    origin_oid_t origin_id;
    oid_builder_t builder;
    for (auto& v : dynamic_frag->InnerVertices()) {
      if (!dynamic_frag->IsAliveInnerVertex(v)) {
        continue;
      }
      CHECK(vm_ptr->GetOid(fid, v.GetValue(), origin_id));
      CHECK(origin_id.IsString());
      ARROW_OK_OR_RAISE(builder.Append(origin_id.GetString()));
    }
    ARROW_OK_OR_RAISE(builder.Finish(&local_oid_array));

    VY_OK_OR_RAISE(vineyard::FragmentAllGatherArray<oid_array_t>(
        comm_spec_, local_oid_array, oid_lists[0]));

    vineyard::BasicArrowVertexMapBuilder<
        typename vineyard::InternalType<oid_t>::type, vid_t>
        vm_builder(client_, fnum, oid_lists.size(), oid_lists);
    auto vm = vm_builder.Seal(client_);
    return vm->id();
  }

 private:
  grape::CommSpec comm_spec_;
  vineyard::Client& client_;
};

/**
 * @brief A DynamicFragment to ArrowFragment converter. The conversion is
 * proceeded by traversing the source graph.
 *
 * @tparam OID_T OID type
 */
template <typename OID_T, typename VERTEX_MAP_T, bool COMPACT = false>
class DynamicToArrowConverter {
  using src_fragment_t = DynamicFragment;
  using oid_t = OID_T;
  using vid_t = typename src_fragment_t::vid_t;
  using vertex_map_t = VERTEX_MAP_T;
  using dst_fragment_t =
      vineyard::ArrowFragment<oid_t, vid_t, vertex_map_t, COMPACT>;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

 public:
  DynamicToArrowConverter(const grape::CommSpec& comm_spec,
                          vineyard::Client& client)
      : comm_spec_(comm_spec), client_(client) {}

  bl::result<std::shared_ptr<dst_fragment_t>> Convert(
      const std::shared_ptr<src_fragment_t>& dynamic_frag) {
    VertexMapConverter<vertex_map_t> converter(comm_spec_, client_);

    BOOST_LEAF_AUTO(vm_id, converter.Convert(dynamic_frag));
    auto dst_vm =
        std::dynamic_pointer_cast<vertex_map_t>(client_.GetObject(vm_id));

    return convertFragment(dynamic_frag, dst_vm);
  }

 private:
  bl::result<std::shared_ptr<dst_fragment_t>> convertFragment(
      const std::shared_ptr<src_fragment_t>& src_frag,
      const std::shared_ptr<vertex_map_t>& dst_vm) {
    auto fid = src_frag->fid();
    auto fnum = src_frag->fnum();
    BOOST_LEAF_AUTO(v_table, buildVTable(src_frag));
    BOOST_LEAF_AUTO(e_table, buildETable(src_frag, dst_vm));

    {
      std::shared_ptr<arrow::KeyValueMetadata> meta(
          new arrow::KeyValueMetadata());
      meta->Append("type", "VERTEX");
      meta->Append("label_index", "0");
      meta->Append("label", "_");
      v_table = v_table->ReplaceSchemaMetadata(meta);
    }
    {
      std::shared_ptr<arrow::KeyValueMetadata> meta(
          new arrow::KeyValueMetadata());
      meta->Append("type", "EDGE");
      meta->Append("label_index", "0");
      meta->Append("label", "_");
      meta->Append("sub_label_num", "1");
      meta->Append("src_label_0", "_");
      meta->Append("dst_label_0", "_");
      e_table = e_table->ReplaceSchemaMetadata(meta);
    }

    vineyard::PropertyGraphSchema schema;
    schema.set_fnum(comm_spec_.fnum());
    {
      std::unordered_map<std::string, std::string> kvs;
      v_table->schema()->metadata()->ToUnorderedMap(&kvs);
      std::string type = kvs["type"];
      std::string label = kvs["label"];

      auto entry = schema.CreateEntry(label, type);
      // entry->add_primary_keys(1, table->schema()->field_names());

      // N.B. ID columns is already been removed.
      for (int64_t i = 0; i < v_table->num_columns(); ++i) {
        entry->AddProperty(v_table->schema()->field(i)->name(),
                           v_table->schema()->field(i)->type());
      }
    }
    {
      std::unordered_map<std::string, std::string> kvs;
      e_table->schema()->metadata()->ToUnorderedMap(&kvs);
      std::string type = kvs["type"];
      std::string label = kvs["label"];
      std::string src_label = kvs["src_label_0"];
      std::string dst_label = kvs["dst_label_0"];

      auto entry = schema.CreateEntry(label, type);
      if (!src_label.empty() && !dst_label.empty()) {
        entry->AddRelation(src_label, dst_label);
      }
      // N.B. Skip first two id columns.
      for (int64_t i = 2; i < e_table->num_columns(); ++i) {
        entry->AddProperty(e_table->schema()->field(i)->name(),
                           e_table->schema()->field(i)->type());
      }
    }

    auto frag_builder = std::make_shared<vineyard::BasicArrowFragmentBuilder<
        typename dst_fragment_t::oid_t, typename dst_fragment_t::vid_t,
        vertex_map_t, dst_fragment_t::compact_v>>(client_, dst_vm);
    BOOST_LEAF_CHECK(frag_builder->Init(fid, fnum, {v_table}, {e_table},
                                        src_frag->directed()));
    frag_builder->SetPropertyGraphSchema(std::move(schema));

    return std::dynamic_pointer_cast<dst_fragment_t>(
        frag_builder->Seal(client_));
  }

  bl::result<std::shared_ptr<arrow::Table>> buildVTable(
      const std::shared_ptr<src_fragment_t>& src_frag) {
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    const auto& vertex_schema = src_frag->GetSchema()["vertex"];

    // build schema and array
    for (const auto& p : vertex_schema.GetObject()) {
      std::string key = p.name.GetString();
      int type = p.value.GetInt();
      LOG(INFO) << key << " got type " << p.value.GetInt() << " " << type;

      switch (type) {
      case rpc::graph::DataTypePb::LONG: {
        auto r = VertexArrayBuilder<arrow::Int64Builder>::build(src_frag, key);

        BOOST_LEAF_AUTO(array, r);
        schema_vector.push_back(arrow::field(key, arrow::int64()));
        arrays.push_back(array);
        break;
      }
      case rpc::graph::DataTypePb::DOUBLE: {
        auto r = VertexArrayBuilder<arrow::DoubleBuilder>::build(src_frag, key);

        BOOST_LEAF_AUTO(array, r);
        schema_vector.push_back(arrow::field(key, arrow::float64()));
        arrays.push_back(array);
        break;
      }
      case rpc::graph::DataTypePb::STRING: {
        auto r =
            VertexArrayBuilder<arrow::LargeStringBuilder>::build(src_frag, key);

        BOOST_LEAF_AUTO(array, r);
        schema_vector.push_back(arrow::field(key, arrow::large_utf8()));
        arrays.push_back(array);
        break;
      }
      default:
        RETURN_GS_ERROR(vineyard::ErrorCode::kDataTypeError,
                        "Unsupported type: " + std::to_string(type));
      }
    }

    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    auto v_table = arrow::Table::Make(schema, arrays);
    std::shared_ptr<arrow::KeyValueMetadata> meta(
        new arrow::KeyValueMetadata());
    meta->Append("type", "VERTEX");
    meta->Append("label_index", "0");
    meta->Append("label", "default_0");

    return v_table->ReplaceSchemaMetadata(meta);
  }

  bl::result<std::shared_ptr<arrow::Table>> buildETable(
      const std::shared_ptr<src_fragment_t>& src_frag,
      const std::shared_ptr<vertex_map_t>& dst_vm) {
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        std::make_shared<arrow::Field>("src", arrow::uint64()),
        std::make_shared<arrow::Field>("dst", arrow::uint64())};
    COOBuilder<dst_fragment_t, oid_t> builder;
    BOOST_LEAF_AUTO(src_dst_array, builder.Build(src_frag, dst_vm));
    std::shared_ptr<arrow::Array> src_array = src_dst_array.first,
                                  dst_array = src_dst_array.second;
    CHECK_EQ(src_array->length(), dst_array->length());
    std::vector<std::shared_ptr<arrow::Array>> arrays{src_array, dst_array};

    // build schema and array
    const auto& edge_schema = src_frag->GetSchema()["edge"];
    for (const auto& p : edge_schema.GetObject()) {
      std::string key = p.name.GetString();
      int type = p.value.GetInt();
      switch (type) {
      case rpc::graph::DataTypePb::LONG: {
        auto r = EdgeArrayBuilder<arrow::Int64Builder>::build(src_frag, key);

        BOOST_LEAF_AUTO(array, r);
        schema_vector.push_back(arrow::field(key, arrow::int64()));
        CHECK_EQ(array->length(), src_array->length());
        arrays.push_back(array);
        break;
      }
      case rpc::graph::DataTypePb::DOUBLE: {
        auto r = EdgeArrayBuilder<arrow::DoubleBuilder>::build(src_frag, key);

        BOOST_LEAF_AUTO(array, r);
        schema_vector.push_back(arrow::field(key, arrow::float64()));
        arrays.push_back(array);
        break;
      }
      case rpc::graph::DataTypePb::STRING: {
        auto r =
            EdgeArrayBuilder<arrow::LargeStringBuilder>::build(src_frag, key);

        BOOST_LEAF_AUTO(array, r);
        schema_vector.push_back(arrow::field(key, arrow::large_utf8()));
        arrays.push_back(array);
        break;
      }
      default:
        RETURN_GS_ERROR(vineyard::ErrorCode::kDataTypeError,
                        "Unsupported type: " + std::to_string(type));
      }
    }

    auto schema = std::make_shared<arrow::Schema>(schema_vector);
    auto e_table = arrow::Table::Make(schema, arrays);
    std::shared_ptr<arrow::KeyValueMetadata> meta(
        new arrow::KeyValueMetadata());
    meta->Append("type", "EDGE");
    meta->Append("label_index", "0");
    meta->Append("label", "default_0");

    return e_table->ReplaceSchemaMetadata(meta);
  }

  grape::CommSpec comm_spec_;
  vineyard::Client& client_;
};

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_LOADER_DYNAMIC_TO_ARROW_CONVERTER_H_
