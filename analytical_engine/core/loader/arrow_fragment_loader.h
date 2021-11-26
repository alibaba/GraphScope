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
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/stream/byte_stream.h"
#include "vineyard/basic/stream/dataframe_stream.h"
#include "vineyard/basic/stream/parallel_stream.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/graph/loader/arrow_fragment_loader.h"
#include "vineyard/io/io/i_io_adaptor.h"
#include "vineyard/io/io/io_factory.h"

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
  using vertex_table_info_t =
      std::map<std::string, std::shared_ptr<arrow::Table>>;
  using edge_table_info_t = std::vector<vineyard::InputTable>;

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

  boost::leaf::result<std::vector<std::shared_ptr<arrow::Table>>>
  LoadVertexTables() {
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-READ-VERTEX-0";
    std::vector<std::shared_ptr<arrow::Table>> v_tables;
    if (!vfiles_.empty()) {
      auto load_v_procedure = [&]() {
        return loadVertexTables(vfiles_, comm_spec_.worker_id(),
                                comm_spec_.worker_num());
      };
      BOOST_LEAF_AUTO(tmp_v,
                      vineyard::sync_gs_error(comm_spec_, load_v_procedure));
      v_tables = tmp_v;
    } else if (!graph_info_->vertices.empty()) {
      auto load_v_procedure = [&]() {
        return loadVertexTables(graph_info_->vertices, comm_spec_.worker_id(),
                                comm_spec_.worker_num());
      };
      BOOST_LEAF_AUTO(tmp_v,
                      vineyard::sync_gs_error(comm_spec_, load_v_procedure));
      v_tables = tmp_v;
    }
    for (const auto& table : v_tables) {
      BOOST_LEAF_CHECK(sanityChecks(table));
    }
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-READ-VERTEX-100";
    return v_tables;
  }

  boost::leaf::result<std::vector<std::vector<std::shared_ptr<arrow::Table>>>>
  LoadEdgeTables() {
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-READ-EDGE-0";
    std::vector<std::vector<std::shared_ptr<arrow::Table>>> e_tables;
    if (!efiles_.empty()) {
      auto load_e_procedure = [&]() {
        return loadEdgeTables(efiles_, comm_spec_.worker_id(),
                              comm_spec_.worker_num());
      };
      BOOST_LEAF_AUTO(tmp_e,
                      vineyard::sync_gs_error(comm_spec_, load_e_procedure));
      e_tables = tmp_e;
    } else {
      auto load_e_procedure = [&]() {
        return loadEdgeTables(graph_info_->edges, comm_spec_.worker_id(),
                              comm_spec_.worker_num());
      };
      BOOST_LEAF_AUTO(tmp_e,
                      vineyard::sync_gs_error(comm_spec_, load_e_procedure));
      e_tables = tmp_e;
    }
    for (const auto& table_vec : e_tables) {
      for (const auto& table : table_vec) {
        BOOST_LEAF_CHECK(sanityChecks(table));
      }
    }
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-READ-EDGE-100";
    return e_tables;
  }

  boost::leaf::result<vineyard::ObjectID> AddLabelsToGraph(
      vineyard::ObjectID frag_id) {
    if (!graph_info_->vertices.empty() && !graph_info_->edges.empty()) {
      return addVerticesAndEdges(frag_id);
    } else if (!graph_info_->vertices.empty()) {
      return addVertices(frag_id);
    } else {
      return addEdges(frag_id);
    }
    return vineyard::InvalidObjectID();
  }

  boost::leaf::result<vineyard::ObjectID> addVerticesAndEdges(
      vineyard::ObjectID frag_id) {
    BOOST_LEAF_AUTO(partitioner, initPartitioner());
    BOOST_LEAF_AUTO(partial_v_tables, LoadVertexTables());
    BOOST_LEAF_AUTO(partial_e_tables, LoadEdgeTables());

    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-0";
    auto frag = std::static_pointer_cast<vineyard::ArrowFragment<oid_t, vid_t>>(
        client_.GetObject(frag_id));
    auto schema = frag->schema();

    std::map<std::string, label_id_t> vertex_label_to_index;
    std::set<std::string> previous_labels;

    for (auto& entry : schema.vertex_entries()) {
      vertex_label_to_index[entry.label] = entry.id;
      previous_labels.insert(entry.label);
    }

    BOOST_LEAF_AUTO(v_e_tables,
                    preprocessInputs(partitioner, partial_v_tables,
                                     partial_e_tables, previous_labels));

    auto vertex_tables_with_label = v_e_tables.first;
    auto edge_tables_with_label = v_e_tables.second;

    auto basic_fragment_loader = std::make_shared<
        vineyard::BasicEVFragmentLoader<OID_T, VID_T, partitioner_t>>(
        client_, comm_spec_, partitioner, directed_, true, generate_eid_);

    for (auto& pair : vertex_tables_with_label) {
      BOOST_LEAF_CHECK(
          basic_fragment_loader->AddVertexTable(pair.first, pair.second));
    }
    auto old_vm_ptr = frag->GetVertexMap();
    BOOST_LEAF_CHECK(
        basic_fragment_loader->ConstructVertices(old_vm_ptr->id()));

    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-100";
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-0";
    partial_v_tables.clear();
    vertex_tables_with_label.clear();

    label_id_t pre_label_num = old_vm_ptr->label_num();

    auto new_labels_index = basic_fragment_loader->get_vertex_label_to_index();
    for (auto& pair : new_labels_index) {
      vertex_label_to_index[pair.first] = pair.second + pre_label_num;
    }
    basic_fragment_loader->set_vertex_label_to_index(
        std::move(vertex_label_to_index));
    for (auto& table : edge_tables_with_label) {
      BOOST_LEAF_CHECK(basic_fragment_loader->AddEdgeTable(
          table.src_label, table.dst_label, table.edge_label, table.table));
    }

    partial_e_tables.clear();
    edge_tables_with_label.clear();

    BOOST_LEAF_CHECK(basic_fragment_loader->ConstructEdges(
        schema.all_edge_label_num(), schema.all_vertex_label_num()));
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-100";
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-SEAL-0";
    return basic_fragment_loader->AddVerticesAndEdgesToFragment(frag);
  }

  boost::leaf::result<vineyard::ObjectID> addVertices(
      vineyard::ObjectID frag_id) {
    BOOST_LEAF_AUTO(partitioner, initPartitioner());
    BOOST_LEAF_AUTO(partial_v_tables, LoadVertexTables());
    // For printing the progress report stub
    BOOST_LEAF_CHECK(LoadEdgeTables());

    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-0";

    auto basic_fragment_loader = std::make_shared<
        vineyard::BasicEVFragmentLoader<OID_T, VID_T, partitioner_t>>(
        client_, comm_spec_, partitioner, directed_, true, generate_eid_);
    auto frag = std::static_pointer_cast<vineyard::ArrowFragment<oid_t, vid_t>>(
        client_.GetObject(frag_id));

    for (auto table : partial_v_tables) {
      auto meta = table->schema()->metadata();
      if (meta == nullptr) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
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

    BOOST_LEAF_CHECK(
        basic_fragment_loader->ConstructVertices(frag->GetVertexMap()->id()));
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-100";
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-0";
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-100";
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-SEAL-0";
    return basic_fragment_loader->AddVerticesToFragment(frag);
  }

  boost::leaf::result<vineyard::ObjectID> addEdges(vineyard::ObjectID frag_id) {
    return addVerticesAndEdges(frag_id);
  }

  boost::leaf::result<vineyard::ObjectID> LoadFragment() {
    BOOST_LEAF_AUTO(partitioner, initPartitioner());
    BOOST_LEAF_AUTO(partial_v_tables, LoadVertexTables());
    BOOST_LEAF_AUTO(partial_e_tables, LoadEdgeTables());

    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-0";

    BOOST_LEAF_AUTO(v_e_tables, preprocessInputs(partitioner, partial_v_tables,
                                                 partial_e_tables));

    auto vertex_tables_with_label = v_e_tables.first;
    auto edge_tables_with_label = v_e_tables.second;

    std::shared_ptr<
        vineyard::BasicEVFragmentLoader<OID_T, VID_T, partitioner_t>>
        basic_fragment_loader = std::make_shared<
            vineyard::BasicEVFragmentLoader<OID_T, VID_T, partitioner_t>>(
            client_, comm_spec_, partitioner, directed_, true, generate_eid_);

    for (auto& pair : vertex_tables_with_label) {
      BOOST_LEAF_CHECK(
          basic_fragment_loader->AddVertexTable(pair.first, pair.second));
    }
    BOOST_LEAF_CHECK(basic_fragment_loader->ConstructVertices());
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-100";
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-0";

    partial_v_tables.clear();
    vertex_tables_with_label.clear();

    for (auto& table : edge_tables_with_label) {
      BOOST_LEAF_CHECK(basic_fragment_loader->AddEdgeTable(
          table.src_label, table.dst_label, table.edge_label, table.table));
    }
    partial_e_tables.clear();
    edge_tables_with_label.clear();

    BOOST_LEAF_CHECK(basic_fragment_loader->ConstructEdges());
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-100";
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-SEAL-0";
    return basic_fragment_loader->ConstructFragment();
  }

  boost::leaf::result<vineyard::ObjectID> AddLabelsToGraphAsFragmentGroup(
      vineyard::ObjectID frag_id) {
    BOOST_LEAF_AUTO(new_frag_id, AddLabelsToGraph(frag_id));
    VY_OK_OR_RAISE(client_.Persist(new_frag_id));
    return vineyard::ConstructFragmentGroup(client_, new_frag_id, comm_spec_);
  }

  boost::leaf::result<vineyard::ObjectID> LoadFragmentAsFragmentGroup() {
    BOOST_LEAF_AUTO(frag_id, LoadFragment());
    VY_OK_OR_RAISE(client_.Persist(frag_id));
    return vineyard::ConstructFragmentGroup(client_, frag_id, comm_spec_);
  }

  boost::leaf::result<partitioner_t> initPartitioner() {
    partitioner_t partitioner;
#ifdef HASH_PARTITION
    partitioner.Init(comm_spec_.fnum());
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

    partitioner.Init(comm_spec_.fnum(), oid_list);
#endif
    return partitioner;
  }

 private:
  boost::leaf::result<std::shared_ptr<arrow::Table>> readTableFromPandas(
      const std::string& data) {
    std::shared_ptr<arrow::Table> table;
    if (!data.empty()) {
      std::shared_ptr<arrow::Buffer> buffer =
          arrow::Buffer::Wrap(data.data(), data.size());
      VY_OK_OR_RAISE(vineyard::DeserializeTable(buffer, &table));
    }
    return table;
  }

  boost::leaf::result<std::shared_ptr<arrow::Table>> readTableFromLocation(
      const std::string& location, int index, int total_parts) {
    std::shared_ptr<arrow::Table> table;
    std::string expanded = vineyard::ExpandEnvironmentVariables(location);
    auto io_adaptor = vineyard::IOFactory::CreateIOAdaptor(expanded);
    if (io_adaptor == nullptr) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kIOError,
                      "Cannot find a supported adaptor for " + location);
    }
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
      std::unique_ptr<vineyard::IIOAdaptor,
                      std::function<void(vineyard::IIOAdaptor*)>>
          io_adaptor(vineyard::IOFactory::CreateIOAdaptor(files[label_id] +
                                                          "#header_row=true")
                         .release(),
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
      VLOG(2) << "read vertex table from vineyard: " << vertices[0]->values;
      BOOST_LEAF_AUTO(sourceId, resolveVYObject(vertices[0]->values));
      auto read_procedure = [&]()
          -> boost::leaf::result<std::vector<std::shared_ptr<arrow::Table>>> {
        BOOST_LEAF_AUTO(tables, vineyard::GatherVTables(
                                    client_, {sourceId}, comm_spec_.local_id(),
                                    comm_spec_.local_num()));
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
        if (vertices[i]->protocol == "numpy" ||
            vertices[i]->protocol == "pandas") {
          BOOST_LEAF_AUTO(tmp, readTableFromPandas(vertices[i]->values));
          table = tmp;
        } else if (vertices[i]->protocol == "vineyard") {
          VLOG(2) << "read vertex table from vineyard: " << vertices[i]->values;
          BOOST_LEAF_AUTO(sourceId, resolveVYObject(vertices[i]->values));
          VY_OK_OR_RAISE(vineyard::ReadTableFromVineyard(
              client_, sourceId, table, comm_spec_.local_id(),
              comm_spec_.local_num()));
          if (table != nullptr) {
            VLOG(2) << "schema of vertex table: "
                    << table->schema()->ToString();
          } else {
            VLOG(2) << "vertex table is null";
          }
        } else {
          // Let the IOFactory to parse other protocols.
          auto path = vertices[i]->values;
          BOOST_LEAF_AUTO(tmp, readTableFromLocation(vertices[i]->values, index,
                                                     total_parts));
          table = tmp;
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
          std::unique_ptr<vineyard::IIOAdaptor,
                          std::function<void(vineyard::IIOAdaptor*)>>
              io_adaptor(vineyard::IOFactory::CreateIOAdaptor(
                             sub_label_files[j] + "#header_row=true")
                             .release(),
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
      LOG(INFO) << "read edge table from vineyard: "
                << edges[0]->sub_labels[0].values;
      BOOST_LEAF_AUTO(sourceId,
                      resolveVYObject(edges[0]->sub_labels[0].values));
      auto read_procedure =
          [&]() -> boost::leaf::result<
                    std::vector<std::vector<std::shared_ptr<arrow::Table>>>> {
        BOOST_LEAF_AUTO(tables,
                        vineyard::GatherETables(client_, {{sourceId}},
                                                comm_spec_.local_id(),
                                                comm_spec_.local_num()));
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
          if (sub_labels[j].protocol == "pandas") {
            BOOST_LEAF_ASSIGN(table, readTableFromPandas(sub_labels[j].values));
          } else if (sub_labels[j].protocol == "vineyard") {
            LOG(INFO) << "read edge table from vineyard: "
                      << sub_labels[j].values;
            BOOST_LEAF_AUTO(sourceId, resolveVYObject(sub_labels[j].values));
            VY_OK_OR_RAISE(vineyard::ReadTableFromVineyard(
                client_, sourceId, table, comm_spec_.local_id(),
                comm_spec_.local_num()));
            if (table == nullptr) {
              VLOG(2) << "edge table is null";
            } else {
              VLOG(2) << "schema of edge table: "
                      << table->schema()->ToString();
            }
          } else {
            // Let the IOFactory to parse other protocols.
            BOOST_LEAF_ASSIGN(table, readTableFromLocation(sub_labels[j].values,
                                                           index, total_parts));
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

  boost::leaf::result<std::pair<vertex_table_info_t, edge_table_info_t>>
  preprocessInputs(
      partitioner_t partitioner,
      const std::vector<std::shared_ptr<arrow::Table>>& v_tables,
      const std::vector<std::vector<std::shared_ptr<arrow::Table>>>& e_tables,
      const std::set<std::string>& previous_vertex_labels =
          std::set<std::string>()) {
    vertex_table_info_t vertex_tables_with_label;
    edge_table_info_t edge_tables_with_label;
    std::set<std::string> deduced_labels;
    for (auto table : v_tables) {
      auto meta = table->schema()->metadata();
      if (meta == nullptr) {
        RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                        "Metadata of input vertex files shouldn't be empty");
      }

      int label_meta_index = meta->FindKey(LABEL_TAG);
      if (label_meta_index == -1) {
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kInvalidValueError,
            "Metadata of input vertex files should contain label name");
      }
      std::string label_name = meta->value(label_meta_index);
      vertex_tables_with_label[label_name] = table;
    }

    auto label_not_exists = [&](const std::string& label) {
      return vertex_tables_with_label.find(label) ==
                 vertex_tables_with_label.end() &&
             previous_vertex_labels.find(label) == previous_vertex_labels.end();
    };

    for (auto& table_vec : e_tables) {
      for (auto table : table_vec) {
        auto meta = table->schema()->metadata();
        int label_meta_index = meta->FindKey(LABEL_TAG);
        std::string label_name = meta->value(label_meta_index);
        int src_label_meta_index = meta->FindKey(SRC_LABEL_TAG);
        std::string src_label_name = meta->value(src_label_meta_index);
        int dst_label_meta_index = meta->FindKey(DST_LABEL_TAG);
        std::string dst_label_name = meta->value(dst_label_meta_index);
        edge_tables_with_label.emplace_back(src_label_name, dst_label_name,
                                            label_name, table);
        // Find vertex labels that need to be deduced, i.e. not assigned by user
        // directly

        if (label_not_exists(src_label_name)) {
          deduced_labels.insert(src_label_name);
        }
        if (label_not_exists(dst_label_name)) {
          deduced_labels.insert(dst_label_name);
        }
      }
    }

    if (!deduced_labels.empty()) {
      vineyard::FragmentLoaderUtils<OID_T, VID_T, partitioner_t> loader_utils(
          comm_spec_, partitioner);
      BOOST_LEAF_AUTO(vertex_labels,
                      loader_utils.GatherVertexLabels(edge_tables_with_label));
      BOOST_LEAF_AUTO(vertex_label_to_index,
                      loader_utils.GetVertexLabelToIndex(vertex_labels));
      BOOST_LEAF_AUTO(v_tables_map, loader_utils.BuildVertexTableFromEdges(
                                        edge_tables_with_label,
                                        vertex_label_to_index, deduced_labels));
      for (auto& pair : v_tables_map) {
        vertex_tables_with_label[pair.first] = pair.second;
      }
    }
    return std::make_pair(vertex_tables_with_label, edge_tables_with_label);
  }

  boost::leaf::result<vineyard::ObjectID> resolveVYObject(
      std::string const& source) {
    vineyard::ObjectID sourceId = vineyard::InvalidObjectID();
    // encoding: 'o' prefix for object id, and 's' prefix for object name.
    CHECK_OR_RAISE(!source.empty() && (source[0] == 'o' || source[0] == 's'));
    if (source[0] == 'o') {
      sourceId = vineyard::ObjectIDFromString(source.substr(1));
    } else {
      VY_OK_OR_RAISE(client_.GetName(source.substr(1), sourceId, true));
    }
    CHECK_OR_RAISE(sourceId != vineyard::InvalidObjectID());
    return sourceId;
  }

  /// Do some necessary sanity checks.
  boost::leaf::result<void> sanityChecks(std::shared_ptr<arrow::Table> table) {
    // We require that there are no identical column names
    auto names = table->ColumnNames();
    std::sort(names.begin(), names.end());
    const auto duplicate = std::adjacent_find(names.begin(), names.end());
    if (duplicate != names.end()) {
      auto meta = table->schema()->metadata();
      int label_meta_index = meta->FindKey(LABEL_TAG);
      std::string label_name = meta->value(label_meta_index);
      std::stringstream msg;
      msg << "Label " << label_name
          << " has identical property names, which is not allowed. The "
             "original names are: ";
      auto origin_names = table->ColumnNames();
      msg << "[";
      for (size_t i = 0; i < origin_names.size(); ++i) {
        if (i != 0) {
          msg << ", ";
        }
        msg << origin_names[i];
      }
      msg << "]";
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError, msg.str());
    }
    return {};
  }

  vineyard::Client& client_;
  grape::CommSpec comm_spec_;

  std::vector<std::string> efiles_;
  std::vector<std::string> vfiles_;

  std::shared_ptr<detail::Graph> graph_info_;

  bool directed_;
  bool generate_eid_;

  std::function<void(vineyard::IIOAdaptor*)> io_deleter_ =
      [](vineyard::IIOAdaptor* adaptor) {
        VINEYARD_CHECK_OK(adaptor->Close());
        delete adaptor;
      };
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_LOADER_ARROW_FRAGMENT_LOADER_H_
