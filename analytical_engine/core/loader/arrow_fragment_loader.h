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

#ifndef ANALYTICAL_ENGINE_CORE_LOADER_ARROW_FRAGMENT_LOADER_H_
#define ANALYTICAL_ENGINE_CORE_LOADER_ARROW_FRAGMENT_LOADER_H_

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/worker/comm_spec.h"
#include "vineyard/basic/stream/byte_stream.h"
#include "vineyard/basic/stream/dataframe_stream.h"
#include "vineyard/basic/stream/parallel_stream.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/loader/arrow_fragment_loader.h"
#include "vineyard/io/io/io_factory.h"
#include "vineyard/io/io/local_io_adaptor.h"

#include "core/error.h"
#include "core/io/property_parser.h"

#define HASH_PARTITION

namespace gs {
/**
 * @brief This loader can load a ArrowFragment from the data source including
 * local file, oss, numpy, pandas and vineyard.
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <typename OID_T = vineyard::property_graph_types::OID_TYPE,
          typename VID_T = vineyard::property_graph_types::VID_TYPE>
class ArrowFragmentLoader {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vertex_map_t = vineyard::ArrowVertexMap<internal_oid_t, vid_t>;
  static constexpr const char* LABEL_TAG = "label";
  static constexpr const char* SRC_LABEL_TAG = "src_label";
  static constexpr const char* DST_LABEL_TAG = "dst_label";

  const int id_column = 0;

#ifdef HASH_PARTITION
  using partitioner_t = vineyard::HashPartitioner<oid_t>;
#else
  using partitioner_t = SegmentedPartitioner<oid_t>;
#endif

 public:
  ArrowFragmentLoader(vineyard::Client& client,
                      const grape::CommSpec& comm_spec,
                      const std::vector<std::string>& efiles,
                      const std::vector<std::string>& vfiles,
                      bool directed = true)
      : client_(client),
        comm_spec_(comm_spec),
        efiles_(efiles),
        vfiles_(vfiles),
        graph_info_(nullptr),
        directed_(directed),
        generate_eid_(false) {}

  ArrowFragmentLoader(vineyard::Client& client,
                      const grape::CommSpec& comm_spec,
                      std::shared_ptr<detail::Graph> graph_info)
      : client_(client),
        comm_spec_(comm_spec),
        efiles_(),
        vfiles_(),
        graph_info_(graph_info),
        directed_(graph_info->directed),
        generate_eid_(graph_info->generate_eid) {}

  ~ArrowFragmentLoader() = default;

  boost::leaf::result<vineyard::ObjectID> LoadFragment() {
    BOOST_LEAF_CHECK(initPartitioner());

    std::vector<std::shared_ptr<arrow::Table>> partial_v_tables;
    std::vector<std::vector<std::shared_ptr<arrow::Table>>> partial_e_tables;

    bool load_with_ve;

    if (!efiles_.empty()) {
      auto load_e_procedure = [&]() {
        return loadEdgeTables(efiles_, comm_spec_.worker_id(),
                              comm_spec_.worker_num());
      };
      BOOST_LEAF_AUTO(tmp_e,
                      vineyard::sync_gs_error(comm_spec_, load_e_procedure));
      partial_e_tables = tmp_e;
      if (!vfiles_.empty()) {
        auto load_v_procedure = [&]() {
          return loadVertexTables(vfiles_, comm_spec_.worker_id(),
                                  comm_spec_.worker_num());
        };
        BOOST_LEAF_AUTO(tmp_v,
                        vineyard::sync_gs_error(comm_spec_, load_v_procedure));
        partial_v_tables = tmp_v;
        load_with_ve = true;
      } else {
        load_with_ve = false;
      }
    } else {
      auto load_e_procedure = [&]() {
        return loadEdgeTables(graph_info_->edges, comm_spec_.worker_id(),
                              comm_spec_.worker_num());
      };
      BOOST_LEAF_AUTO(tmp_e,
                      vineyard::sync_gs_error(comm_spec_, load_e_procedure));
      partial_e_tables = tmp_e;
      if (!graph_info_->vertices.empty()) {
        auto load_v_procedure = [&]() {
          return loadVertexTables(graph_info_->vertices, comm_spec_.worker_id(),
                                  comm_spec_.worker_num());
        };
        BOOST_LEAF_AUTO(tmp_v,
                        vineyard::sync_gs_error(comm_spec_, load_v_procedure));
        partial_v_tables = tmp_v;
        load_with_ve = true;
      } else {
        load_with_ve = false;
      }
    }

    if (load_with_ve) {
      std::shared_ptr<
          vineyard::BasicEVFragmentLoader<OID_T, VID_T, partitioner_t>>
          basic_fragment_loader = std::make_shared<
              vineyard::BasicEVFragmentLoader<OID_T, VID_T, partitioner_t>>(
              client_, comm_spec_, partitioner_, directed_, true,
              generate_eid_);

      for (auto table : partial_v_tables) {
        auto meta = table->schema()->metadata();
        if (meta == nullptr) {
          RETURN_GS_ERROR(
              vineyard::ErrorCode::kInvalidValueError,
              "Metadata of input vertex tables shouldn't be empty.");
        }
        int label_meta_index = meta->FindKey(LABEL_TAG);
        if (label_meta_index == -1) {
          RETURN_GS_ERROR(
              vineyard::ErrorCode::kInvalidValueError,
              "Metadata of input vertex tables should contain label name.");
        }
        std::string label_name = meta->value(label_meta_index);
        BOOST_LEAF_CHECK(
            basic_fragment_loader->AddVertexTable(label_name, table));
      }

      partial_v_tables.clear();

      BOOST_LEAF_CHECK(basic_fragment_loader->ConstructVertices());

      for (auto& table_vec : partial_e_tables) {
        for (auto table : table_vec) {
          auto meta = table->schema()->metadata();
          if (meta == nullptr) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kInvalidValueError,
                "Metadata of input edge tables shouldn't be empty.");
          }

          int label_meta_index = meta->FindKey(LABEL_TAG);
          if (label_meta_index == -1) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kInvalidValueError,
                "Metadata of input edge tables should contain label name.");
          }
          std::string label_name = meta->value(label_meta_index);

          int src_label_meta_index = meta->FindKey(SRC_LABEL_TAG);
          if (src_label_meta_index == -1) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kInvalidValueError,
                "Metadata of input edge tables should contain src label name.");
          }
          std::string src_label_name = meta->value(src_label_meta_index);

          int dst_label_meta_index = meta->FindKey(DST_LABEL_TAG);
          if (dst_label_meta_index == -1) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kInvalidValueError,
                "Metadata of input edge tables should contain dst label name.");
          }
          std::string dst_label_name = meta->value(dst_label_meta_index);

          BOOST_LEAF_CHECK(basic_fragment_loader->AddEdgeTable(
              src_label_name, dst_label_name, label_name, table));
        }
      }

      partial_e_tables.clear();

      BOOST_LEAF_CHECK(basic_fragment_loader->ConstructEdges());

      return basic_fragment_loader->ConstructFragment();
    } else {
      std::shared_ptr<
          vineyard::BasicEFragmentLoader<OID_T, VID_T, partitioner_t>>
          basic_fragment_loader = std::make_shared<
              vineyard::BasicEFragmentLoader<OID_T, VID_T, partitioner_t>>(
              client_, comm_spec_, partitioner_, directed_, true,
              generate_eid_);

      for (auto& table_vec : partial_e_tables) {
        for (auto table : table_vec) {
          auto meta = table->schema()->metadata();
          if (meta == nullptr) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kInvalidValueError,
                "Metadata of input edge tables shouldn't be empty.");
          }

          int label_meta_index = meta->FindKey(LABEL_TAG);
          if (label_meta_index == -1) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kInvalidValueError,
                "Metadata of input edge tables should contain label name.");
          }
          std::string label_name = meta->value(label_meta_index);

          int src_label_meta_index = meta->FindKey(SRC_LABEL_TAG);
          if (src_label_meta_index == -1) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kInvalidValueError,
                "Metadata of input edge tables should contain src label name.");
          }
          std::string src_label_name = meta->value(src_label_meta_index);

          int dst_label_meta_index = meta->FindKey(DST_LABEL_TAG);
          if (dst_label_meta_index == -1) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kInvalidValueError,
                "Metadata of input edge tables should contain dst label name.");
          }
          std::string dst_label_name = meta->value(dst_label_meta_index);

          BOOST_LEAF_CHECK(basic_fragment_loader->AddEdgeTable(
              src_label_name, dst_label_name, label_name, table));
        }
      }

      partial_e_tables.clear();

      BOOST_LEAF_CHECK(basic_fragment_loader->ConstructEdges());

      return basic_fragment_loader->ConstructFragment();
    }
  }

  boost::leaf::result<vineyard::ObjectID> LoadFragmentAsFragmentGroup() {
    BOOST_LEAF_AUTO(frag_id, LoadFragment());
    VY_OK_OR_RAISE(client_.Persist(frag_id));
    return vineyard::ConstructFragmentGroup(client_, frag_id, comm_spec_);
  }

 private:
  boost::leaf::result<void> initPartitioner() {
#ifdef HASH_PARTITION
    partitioner_.Init(comm_spec_.fnum());
#else
    if (vfiles_.empty() &&
        (graph_info_ != nullptr && graph_info_->vertices.empty())) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidOperationError,
          "Segmented partitioner is not supported when the v-file is "
          "not provided");
    }
    std::vector<std::shared_ptr<arrow::Table>> vtables;
    if (graph_info_) {
      BOOST_LEAF_ASSIGN(vtables, loadVertexTables(graph_info_->vertices, 0, 1));
    } else {
      BOOST_LEAF_ASSIGN(vtables, loadVertexTables(vfiles_, 0, 1));
    }
    std::vector<oid_t> oid_list;

    for (auto& table : vtables) {
      std::shared_ptr<arrow::ChunkedArray> oid_array_chunks =
          table->column(id_column);
      size_t chunk_num = oid_array_chunks->num_chunks();

      for (size_t chunk_i = 0; chunk_i != chunk_num; ++chunk_i) {
        std::shared_ptr<oid_array_t> array =
            std::dynamic_pointer_cast<oid_array_t>(
                oid_array_chunks->chunk(chunk_i));
        int64_t length = array->length();
        for (int64_t i = 0; i < length; ++i) {
          oid_list.emplace_back(oid_t(array->GetView(i)));
        }
      }
    }

    partitioner_.Init(comm_spec_.fnum(), oid_list);
#endif
    return {};
  }

  boost::leaf::result<std::shared_ptr<arrow::Table>> readTableFromNumpy(
      std::vector<std::string>& data, size_t row_num, size_t col_num, int index,
      int total_parts,
      std::vector<std::pair<std::string, rpc::DataType>> properties) {
    int chunk_start = index * (row_num / total_parts);
    int chunk_size = row_num / total_parts;
    if (index == total_parts - 1) {
      chunk_size += row_num % total_parts;
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> schemas;
    CHECK_EQ(properties.size(), col_num);
    for (size_t i = 0; i < col_num; ++i) {
      std::shared_ptr<arrow::Array> array;
      const char* bytes = data[i].data();
      auto& prop = properties[i];
      if (prop.second == rpc::INT64 || prop.second == rpc::LONG) {
        schemas.push_back(arrow::field(prop.first, arrow::int64()));
        arrow::Int64Builder builder;
        ARROW_OK_OR_RAISE(builder.AppendValues(
            reinterpret_cast<const int64_t*>(bytes +
                                             chunk_start * sizeof(int64_t)),
            chunk_size));
        ARROW_OK_OR_RAISE(builder.Finish(&array));
      } else if (prop.second == rpc::DOUBLE) {
        schemas.push_back(arrow::field(prop.first, arrow::float64()));
        arrow::DoubleBuilder builder;
        ARROW_OK_OR_RAISE(builder.AppendValues(
            reinterpret_cast<const double*>(bytes +
                                            chunk_start * sizeof(double)),
            chunk_size));
        ARROW_OK_OR_RAISE(builder.Finish(&array));
      } else {
        CHECK(0);
      }
      arrays.push_back(array);
    }
    auto schema = std::make_shared<arrow::Schema>(schemas);
    auto table = arrow::Table::Make(schema, arrays);
    ARROW_OK_OR_RAISE(table->Validate());
    return table;
  }

  boost::leaf::result<std::shared_ptr<arrow::Table>> readTableFromLocation(
      const std::string& location, int index, int total_parts) {
    std::shared_ptr<arrow::Table> table;
    auto io_adaptor = vineyard::IOFactory::CreateIOAdaptor(location);
    ARROW_OK_OR_RAISE(io_adaptor->SetPartialRead(index, total_parts));
    ARROW_OK_OR_RAISE(io_adaptor->Open());
    ARROW_OK_OR_RAISE(io_adaptor->ReadTable(&table));
    ARROW_OK_OR_RAISE(io_adaptor->Close());
    return table;
  }

  boost::leaf::result<std::vector<std::shared_ptr<arrow::Table>>>
  loadVertexTables(const std::vector<std::string>& files, int index,
                   int total_parts) {
    auto label_num = static_cast<label_id_t>(files.size());
    std::vector<std::shared_ptr<arrow::Table>> tables(label_num);

    for (label_id_t label_id = 0; label_id < label_num; ++label_id) {
      std::unique_ptr<vineyard::LocalIOAdaptor,
                      std::function<void(vineyard::LocalIOAdaptor*)>>
          io_adaptor(new vineyard::LocalIOAdaptor(files[label_id] +
                                                  "#header_row=true"),
                     io_deleter_);
      auto read_procedure =
          [&]() -> boost::leaf::result<std::shared_ptr<arrow::Table>> {
        VY_OK_OR_RAISE(io_adaptor->SetPartialRead(index, total_parts));
        VY_OK_OR_RAISE(io_adaptor->Open());
        std::shared_ptr<arrow::Table> table;
        VY_OK_OR_RAISE(io_adaptor->ReadTable(&table));
        return table;
      };

      BOOST_LEAF_AUTO(table,
                      vineyard::sync_gs_error(comm_spec_, read_procedure));

      auto sync_schema_procedure =
          [&]() -> boost::leaf::result<std::shared_ptr<arrow::Table>> {
        return vineyard::SyncSchema(table, comm_spec_);
      };

      BOOST_LEAF_AUTO(normalized_table, vineyard::sync_gs_error(
                                            comm_spec_, sync_schema_procedure));

      auto meta = std::make_shared<arrow::KeyValueMetadata>();

      auto adaptor_meta = io_adaptor->GetMeta();
      // Check if label name is in meta
      if (adaptor_meta.find(LABEL_TAG) == adaptor_meta.end()) {
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kIOError,
            "Metadata of input vertex files should contain label name");
      }
      auto v_label_name = adaptor_meta.find(LABEL_TAG)->second;

#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
      meta->Append(LABEL_TAG, v_label_name);
#else
      CHECK_ARROW_ERROR(meta->Set(LABEL_TAG, v_label_name));
#endif

      tables[label_id] = normalized_table->ReplaceSchemaMetadata(meta);
    }
    return tables;
  }

  boost::leaf::result<std::vector<std::shared_ptr<arrow::Table>>>
  loadVertexTables(const std::vector<std::shared_ptr<detail::Vertex>>& vertices,
                   int index, int total_parts) {
    // a special code path when multiple labeled vertex batches are mixed.
    if (vertices.size() == 1 && vertices[0]->protocol == "vineyard") {
      auto read_procedure = [&]()
          -> boost::leaf::result<std::vector<std::shared_ptr<arrow::Table>>> {
        BOOST_LEAF_AUTO(
            tables,
            vineyard::GatherVTables(
                client_, {vineyard::ObjectIDFromString(vertices[0]->values)},
                comm_spec_.local_id(), comm_spec_.local_num()));
        if (tables.size() == 1 && tables[0] != nullptr) {
          std::shared_ptr<arrow::KeyValueMetadata> meta;
          if (tables[0]->schema()->metadata() == nullptr) {
            meta = std::make_shared<arrow::KeyValueMetadata>();
          } else {
            meta = tables[0]->schema()->metadata()->Copy();
          }
          if (meta->FindKey(LABEL_TAG) == -1) {
            meta->Append(LABEL_TAG, vertices[0]->label);
          }
          tables[0] = tables[0]->ReplaceSchemaMetadata(meta);
        }
        return tables;
      };
      BOOST_LEAF_AUTO(tables,
                      vineyard::sync_gs_error(comm_spec_, read_procedure));
      auto sync_schema_procedure =
          [&](std::shared_ptr<arrow::Table> const& table) {
            return vineyard::SyncSchema(table, comm_spec_);
          };
      std::vector<std::shared_ptr<arrow::Table>> normalized_tables;
      for (auto const& table : tables) {
        BOOST_LEAF_AUTO(
            normalized_table,
            vineyard::sync_gs_error(comm_spec_, sync_schema_procedure, table));
        normalized_tables.emplace_back(normalized_table);
      }
      return normalized_tables;
    }
    size_t label_num = vertices.size();
    std::vector<std::shared_ptr<arrow::Table>> tables(label_num);
    for (size_t i = 0; i < label_num; ++i) {
      auto read_procedure =
          [&]() -> boost::leaf::result<std::shared_ptr<arrow::Table>> {
        std::shared_ptr<arrow::Table> table;
        if (vertices[i]->protocol == "file") {
          auto path = vertices[i]->values;
          BOOST_LEAF_AUTO(tmp, readTableFromLocation(vertices[i]->values, index,
                                                     total_parts));
          table = tmp;
        } else if (vertices[i]->protocol == "numpy" ||
                   vertices[i]->protocol == "pandas") {
          BOOST_LEAF_AUTO(
              tmp, readTableFromNumpy(vertices[i]->data, vertices[i]->row_num,
                                      vertices[i]->column_num, index,
                                      total_parts, vertices[i]->properties));
          table = tmp;
        } else if (vertices[i]->protocol == "vineyard") {
          VLOG(2) << "read vertex table from vineyard: " << vertices[i]->values;
          VY_OK_OR_RAISE(vineyard::ReadTableFromVineyard(
              client_, vineyard::ObjectIDFromString(vertices[i]->values), table,
              comm_spec_.local_id(), comm_spec_.local_num()));
          if (table != nullptr) {
            VLOG(2) << "schema of vertex table: "
                    << table->schema()->ToString();
          } else {
            VLOG(2) << "vertex table is null";
          }
        } else {
          LOG(ERROR) << "Unsupported protocol: " << vertices[i]->protocol;
        }
        return table;
      };
      BOOST_LEAF_AUTO(table,
                      vineyard::sync_gs_error(comm_spec_, read_procedure));

      auto sync_schema_procedure = [&]() {
        return vineyard::SyncSchema(table, comm_spec_);
      };

      BOOST_LEAF_AUTO(normalized_table, vineyard::sync_gs_error(
                                            comm_spec_, sync_schema_procedure));

      std::shared_ptr<arrow::KeyValueMetadata> meta(
          new arrow::KeyValueMetadata());
      meta->Append(LABEL_TAG, vertices[i]->label);
      tables[i] = normalized_table->ReplaceSchemaMetadata(meta);
    }
    return tables;
  }

  boost::leaf::result<std::vector<std::vector<std::shared_ptr<arrow::Table>>>>
  loadEdgeTables(const std::vector<std::string>& files, int index,
                 int total_parts) {
    auto label_num = static_cast<label_id_t>(files.size());
    std::vector<std::vector<std::shared_ptr<arrow::Table>>> tables(label_num);

    try {
      for (label_id_t label_id = 0; label_id < label_num; ++label_id) {
        std::vector<std::string> sub_label_files;
        boost::split(sub_label_files, files[label_id], boost::is_any_of(";"));

        for (size_t j = 0; j < sub_label_files.size(); ++j) {
          std::unique_ptr<vineyard::LocalIOAdaptor,
                          std::function<void(vineyard::LocalIOAdaptor*)>>
              io_adaptor(new vineyard::LocalIOAdaptor(sub_label_files[j] +
                                                      "#header_row=true"),
                         io_deleter_);
          auto read_procedure =
              [&]() -> boost::leaf::result<std::shared_ptr<arrow::Table>> {
            VY_OK_OR_RAISE(io_adaptor->SetPartialRead(index, total_parts));
            VY_OK_OR_RAISE(io_adaptor->Open());
            std::shared_ptr<arrow::Table> table;
            VY_OK_OR_RAISE(io_adaptor->ReadTable(&table));
            return table;
          };
          BOOST_LEAF_AUTO(table,
                          vineyard::sync_gs_error(comm_spec_, read_procedure));

          auto sync_schema_procedure =
              [&]() -> boost::leaf::result<std::shared_ptr<arrow::Table>> {
            return vineyard::SyncSchema(table, comm_spec_);
          };
          BOOST_LEAF_AUTO(
              normalized_table,
              vineyard::sync_gs_error(comm_spec_, sync_schema_procedure));

          std::shared_ptr<arrow::KeyValueMetadata> meta(
              new arrow::KeyValueMetadata());

          auto adaptor_meta = io_adaptor->GetMeta();
          auto it = adaptor_meta.find(LABEL_TAG);
          if (it == adaptor_meta.end()) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kIOError,
                "Metadata of input edge files should contain label name");
          }
          std::string edge_label_name = it->second;

          it = adaptor_meta.find(SRC_LABEL_TAG);
          if (it == adaptor_meta.end()) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kIOError,
                "Metadata of input edge files should contain src label name");
          }
          std::string src_label_name = it->second;

          it = adaptor_meta.find(DST_LABEL_TAG);
          if (it == adaptor_meta.end()) {
            RETURN_GS_ERROR(
                vineyard::ErrorCode::kIOError,
                "Metadata of input edge files should contain dst label name");
          }
          std::string dst_label_name = it->second;

#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
          meta->Append(LABEL_TAG, edge_label_name);
          meta->Append(SRC_LABEL_TAG, src_label_name);
          meta->Append(DST_LABEL_TAG, dst_label_name);
#else
          CHECK_ARROW_ERROR(meta->Set(LABEL_TAG, edge_label_name));
          CHECK_ARROW_ERROR(meta->Set(SRC_LABEL_TAG, src_label_name));
          CHECK_ARROW_ERROR(meta->Set(DST_LABEL_TAG, dst_label_name));
#endif

          tables[label_id].emplace_back(
              normalized_table->ReplaceSchemaMetadata(meta));
        }
      }
    } catch (std::exception& e) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kIOError, std::string(e.what()));
    }
    return tables;
  }

  boost::leaf::result<std::vector<std::vector<std::shared_ptr<arrow::Table>>>>
  loadEdgeTables(const std::vector<std::shared_ptr<detail::Edge>>& edges,
                 int index, int total_parts) {
    // a special code path when multiple labeled edge batches are mixed.
    if (edges.size() == 1 && edges[0]->sub_labels.size() == 1 &&
        edges[0]->sub_labels[0].protocol == "vineyard") {
      auto read_procedure =
          [&]() -> boost::leaf::result<
                    std::vector<std::vector<std::shared_ptr<arrow::Table>>>> {
        BOOST_LEAF_AUTO(tables,
                        vineyard::GatherETables(
                            client_,
                            {{vineyard::ObjectIDFromString(
                                edges[0]->sub_labels[0].values)}},
                            comm_spec_.local_id(), comm_spec_.local_num()));
        if (tables.size() == 1 && tables[0].size() == 1 &&
            tables[0][0] != nullptr) {
          std::shared_ptr<arrow::KeyValueMetadata> meta;
          if (tables[0][0]->schema()->metadata() == nullptr) {
            meta = std::make_shared<arrow::KeyValueMetadata>();
          } else {
            meta = tables[0][0]->schema()->metadata()->Copy();
          }
          if (meta->FindKey(LABEL_TAG) == -1 ||
              meta->FindKey(SRC_LABEL_TAG) == -1 ||
              meta->FindKey(DST_LABEL_TAG) == -1) {
            meta->Append(LABEL_TAG, edges[0]->label);
            meta->Append(SRC_LABEL_TAG, edges[0]->sub_labels[0].src_label);
            meta->Append(DST_LABEL_TAG, edges[0]->sub_labels[0].dst_label);
          }
          tables[0][0] = tables[0][0]->ReplaceSchemaMetadata(meta);
        }
        return tables;
      };
      BOOST_LEAF_AUTO(tables,
                      vineyard::sync_gs_error(comm_spec_, read_procedure));
      auto sync_schema_procedure =
          [&](std::shared_ptr<arrow::Table> const& table) {
            return vineyard::SyncSchema(table, comm_spec_);
          };
      std::vector<std::vector<std::shared_ptr<arrow::Table>>> normalized_tables;
      for (auto const& subtables : tables) {
        std::vector<std::shared_ptr<arrow::Table>> normalized_subtables;
        for (auto const& table : subtables) {
          BOOST_LEAF_AUTO(normalized_table,
                          vineyard::sync_gs_error(
                              comm_spec_, sync_schema_procedure, table));
          normalized_subtables.emplace_back(normalized_table);
        }
        normalized_tables.emplace_back(normalized_subtables);
      }
      return normalized_tables;
    }
    size_t edge_type_num = edges.size();
    std::vector<std::vector<std::shared_ptr<arrow::Table>>> tables(
        edge_type_num);

    for (size_t i = 0; i < edge_type_num; ++i) {
      auto sub_labels = edges[i]->sub_labels;

      for (size_t j = 0; j < sub_labels.size(); ++j) {
        std::shared_ptr<arrow::KeyValueMetadata> meta(
            new arrow::KeyValueMetadata());
        meta->Append(LABEL_TAG, edges[i]->label);

        auto load_procedure =
            [&]() -> boost::leaf::result<std::shared_ptr<arrow::Table>> {
          std::shared_ptr<arrow::Table> table;
          if (sub_labels[j].protocol == "file") {
            BOOST_LEAF_ASSIGN(table, readTableFromLocation(sub_labels[j].values,
                                                           index, total_parts));
          } else if (sub_labels[j].protocol == "numpy" ||
                     sub_labels[j].protocol == "pandas") {
            BOOST_LEAF_ASSIGN(
                table,
                readTableFromNumpy(sub_labels[j].data, sub_labels[j].row_num,
                                   sub_labels[j].column_num, index, total_parts,
                                   sub_labels[j].properties));
          } else if (sub_labels[j].protocol == "vineyard") {
            LOG(INFO) << "read edge table from vineyard: "
                      << sub_labels[j].values;
            VY_OK_OR_RAISE(vineyard::ReadTableFromVineyard(
                client_, vineyard::ObjectIDFromString(sub_labels[j].values),
                table, comm_spec_.local_id(), comm_spec_.local_num()));
            if (table == nullptr) {
              VLOG(2) << "edge table is null";
            } else {
              VLOG(2) << "schema of edge table: "
                      << table->schema()->ToString();
            }
          } else {
            LOG(ERROR) << "Unrecognized protocol: " << sub_labels[j].protocol;
          }
          return table;
        };

        BOOST_LEAF_AUTO(table,
                        vineyard::sync_gs_error(comm_spec_, load_procedure));

        auto sync_schema_procedure = [&]() {
          return vineyard::SyncSchema(table, comm_spec_);
        };

        BOOST_LEAF_AUTO(
            normalized_table,
            vineyard::sync_gs_error(comm_spec_, sync_schema_procedure));

        meta->Append(SRC_LABEL_TAG, sub_labels[j].src_label);
        meta->Append(DST_LABEL_TAG, sub_labels[j].dst_label);
        tables[i].emplace_back(normalized_table->ReplaceSchemaMetadata(meta));
      }
    }
    return tables;
  }

  vineyard::Client& client_;
  grape::CommSpec comm_spec_;

  partitioner_t partitioner_;

  std::vector<std::string> efiles_;
  std::vector<std::string> vfiles_;

  std::shared_ptr<detail::Graph> graph_info_;

  bool directed_;
  bool generate_eid_;

  std::function<void(vineyard::LocalIOAdaptor*)> io_deleter_ =
      [](vineyard::LocalIOAdaptor* adaptor) {
        VINEYARD_CHECK_OK(adaptor->Close());
        delete adaptor;
      };
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_LOADER_ARROW_FRAGMENT_LOADER_H_
