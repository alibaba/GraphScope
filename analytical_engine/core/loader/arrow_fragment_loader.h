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

#ifdef ENABLE_JAVA_SDK
#include <jni.h>
#endif

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/graph/loader/arrow_fragment_loader.h"
#include "vineyard/graph/loader/fragment_loader_utils.h"
#include "vineyard/io/io/i_io_adaptor.h"
#include "vineyard/io/io/io_factory.h"

#include "core/error.h"
#include "core/io/property_parser.h"
#ifdef ENABLE_JAVA_SDK
#include "core/java/java_loader_invoker.h"
#endif

#define HASH_PARTITION

namespace bl = boost::leaf;

namespace gs {
/**
 * @brief This loader can load a ArrowFragment from the data source including
 * local file, oss, numpy, pandas and vineyard.
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <
    typename OID_T = vineyard::property_graph_types::OID_TYPE,
    typename VID_T = vineyard::property_graph_types::VID_TYPE,
    template <typename OID_T_ = typename vineyard::InternalType<OID_T>::type,
              typename VID_T_ = VID_T>
    class VERTEX_MAP_T = vineyard::ArrowVertexMap>
class ArrowFragmentLoader : public vineyard::ArrowFragmentLoader<OID_T, VID_T> {
  using Base = vineyard::ArrowFragmentLoader<OID_T, VID_T>;

  using typename Base::internal_oid_t;
  using typename Base::label_id_t;
  using typename Base::oid_array_t;
  using typename Base::oid_t;
  using typename Base::vid_t;

  using vertex_map_t = VERTEX_MAP_T<internal_oid_t, VID_T>;

  using Base::CONSOLIDATE_TAG;
  using Base::DST_LABEL_TAG;
  using Base::LABEL_TAG;
  using Base::MARKER;
  using Base::SRC_LABEL_TAG;

  using Base::id_column;

  using typename Base::partitioner_t;

  using typename Base::edge_table_info_t;
  using typename Base::oid_array_vec_t;
  using typename Base::vertex_table_info_t;
  using typename Base::vid_array_vec_t;

  // not sure why 'using' doesn't work for table_vec_t.
  using table_vec_t = std::vector<std::shared_ptr<arrow::Table>>;

  using Base::client_;
  using Base::comm_spec_;
  using Base::directed_;
  using Base::efiles_;
  using Base::generate_eid_;
  using Base::retain_oid_;
  using Base::vfiles_;

 public:
  ArrowFragmentLoader(vineyard::Client& client,
                      const grape::CommSpec& comm_spec,
                      const std::vector<std::string>& efiles,
                      const std::vector<std::string>& vfiles,
                      bool directed = true, bool generate_eid = false,
                      bool retain_oid = false, bool compact_edges = false,
                      bool use_perfect_hash = false)
      : Base(client, comm_spec, efiles, vfiles, directed, generate_eid,
             retain_oid, vineyard::is_local_vertex_map<vertex_map_t>::value,
             compact_edges, use_perfect_hash),
        graph_info_(nullptr),
        giraph_enabled_(false) {}

  ArrowFragmentLoader(vineyard::Client& client,
                      const grape::CommSpec& comm_spec,
                      std::shared_ptr<detail::Graph> graph_info)
      : Base(client, comm_spec, std::vector<std::string>{},
             std::vector<std::string>{}, graph_info->directed,
             graph_info->generate_eid, graph_info->retain_oid,
             vineyard::is_local_vertex_map<vertex_map_t>::value,
             graph_info->compact_edges, graph_info->use_perfect_hash),
        graph_info_(graph_info) {
#ifdef ENABLE_JAVA_SDK
    // check when vformat or eformat start with giraph. if not, we
    // giraph_enabled is false;
    giraph_enabled_ = false;
    for (auto v : graph_info_->vertices) {
      if (v->vformat.find("giraph") != std::string::npos) {
        giraph_enabled_ = true;
      }
    }
    for (auto e : graph_info_->edges) {
      for (auto sub : e->sub_labels) {
        if (sub.eformat.find("giraph") != std::string::npos) {
          giraph_enabled_ = true;
        }
      }
    }
    LOG(INFO) << "giraph enabled " << giraph_enabled_;

    if (giraph_enabled_) {
      java_loader_invoker_.SetWorkerInfo(comm_spec_.worker_id(),
                                         comm_spec_.worker_num(), comm_spec_);
      java_loader_invoker_.InitJavaLoader("giraph");
    }
#endif
  }

  ~ArrowFragmentLoader() = default;

#ifdef ENABLE_JAVA_SDK
  JavaLoaderInvoker& GetJavaLoaderInvoker() { return java_loader_invoker_; }
#endif

  boost::leaf::result<std::pair<table_vec_t, std::vector<table_vec_t>>>
  LoadVertexEdgeTables() {
    if (graph_info_) {
      std::stringstream labels;
      labels << "Loading ";
      if (graph_info_->vertices.empty() && graph_info_->edges.empty()) {
        labels << "empty graph";
      } else {
        for (size_t i = 0; i < graph_info_->vertices.size(); ++i) {
          if (i == 0) {
            labels << "vertex labeled ";  // prefix
          } else {
            labels << ", ";  // label separator
          }
          labels << graph_info_->vertices[i]->label;
        }

        if (!graph_info_->vertices.empty()) {
          labels << " and ";
        }
        for (size_t i = 0; i < graph_info_->edges.size(); ++i) {
          if (i == 0) {
            labels << "edge labeled ";  // prefix
          } else {
            labels << ", ";  // label separator
          }
          labels << graph_info_->edges[i]->label;
        }
      }
      LOG_IF(INFO, !comm_spec_.worker_id())
          << MARKER << "DESCRIPTION-" << labels.str();
    }
    BOOST_LEAF_AUTO(v_tables, LoadVertexTables());
    BOOST_LEAF_AUTO(e_tables, LoadEdgeTables());
    return std::make_pair(v_tables, e_tables);
  }

  bl::result<table_vec_t> LoadVertexTables() {
    LOG_IF(INFO, !comm_spec_.worker_id()) << MARKER << "READ-VERTEX-0";
    table_vec_t v_tables;
    if (!vfiles_.empty()) {
      auto load_v_procedure = [&]() {
        return loadVertexTables(vfiles_, comm_spec_.worker_id(),
                                comm_spec_.worker_num());
      };
      BOOST_LEAF_ASSIGN(v_tables,
                        vineyard::sync_gs_error(comm_spec_, load_v_procedure));
    } else if (graph_info_) {
      auto load_v_procedure = [&]() {
        return loadVertexTables(graph_info_->vertices, comm_spec_.worker_id(),
                                comm_spec_.worker_num());
      };
      BOOST_LEAF_ASSIGN(v_tables,
                        vineyard::sync_gs_error(comm_spec_, load_v_procedure));
    }
    for (const auto& table : v_tables) {
      BOOST_LEAF_CHECK(this->sanityChecks(table));
    }
    LOG_IF(INFO, !comm_spec_.worker_id()) << MARKER << "READ-VERTEX-100";
    return v_tables;
  }

  bl::result<std::vector<table_vec_t>> LoadEdgeTables() {
    LOG_IF(INFO, !comm_spec_.worker_id()) << MARKER << "READ-EDGE-0";
    std::vector<table_vec_t> e_tables;
    if (!efiles_.empty()) {
      auto load_e_procedure = [&]() {
        return loadEdgeTables(efiles_, comm_spec_.worker_id(),
                              comm_spec_.worker_num());
      };
      BOOST_LEAF_ASSIGN(e_tables,
                        vineyard::sync_gs_error(comm_spec_, load_e_procedure));
    } else if (graph_info_) {
      auto load_e_procedure = [&]() {
        return loadEdgeTables(graph_info_->edges, comm_spec_.worker_id(),
                              comm_spec_.worker_num());
      };
      BOOST_LEAF_ASSIGN(e_tables,
                        vineyard::sync_gs_error(comm_spec_, load_e_procedure));
    }
    for (const auto& table_vec : e_tables) {
      for (const auto& table : table_vec) {
        BOOST_LEAF_CHECK(sanityChecks(table));
      }
    }
    LOG_IF(INFO, !comm_spec_.worker_id()) << MARKER << "READ-EDGE-100";
    return e_tables;
  }

  boost::leaf::result<vineyard::ObjectID> LoadFragment() {
    BOOST_LEAF_CHECK(initPartitioner());
    BOOST_LEAF_AUTO(raw_v_e_tables, LoadVertexEdgeTables());
    return Base::LoadFragment(std::move(raw_v_e_tables));
  }

  boost::leaf::result<vineyard::ObjectID> LoadFragmentAsFragmentGroup() {
    BOOST_LEAF_AUTO(frag_id, LoadFragment());
    auto frag = std::dynamic_pointer_cast<vineyard::ArrowFragmentBase>(
        client_.GetObject(frag_id));
    if (frag == nullptr) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "fragment is null, means it is failed to be constructed");
    }

    BOOST_LEAF_AUTO(group_id,
                    ConstructFragmentGroup(client_, frag_id, comm_spec_));
    return group_id;
  }

  bl::result<vineyard::ObjectID> AddLabelsToFragment(
      vineyard::ObjectID frag_id) {
    BOOST_LEAF_CHECK(initPartitioner());
    BOOST_LEAF_AUTO(raw_v_e_tables, LoadVertexEdgeTables());
    return Base::addVerticesAndEdges(frag_id, std::move(raw_v_e_tables));
  }

  bl::result<vineyard::ObjectID> AddDataToExistedVLabel(
      vineyard::ObjectID frag_id, label_id_t label_id) {
    BOOST_LEAF_CHECK(initPartitioner());
    BOOST_LEAF_AUTO(raw_v_e_tables, LoadVertexEdgeTables());
    return Base::addDataToExistedVLabel(frag_id, label_id,
                                        std::move(raw_v_e_tables));
  }

  bl::result<vineyard::ObjectID> AddDataToExistedELabel(
      vineyard::ObjectID frag_id, label_id_t label_id) {
    BOOST_LEAF_CHECK(initPartitioner());
    BOOST_LEAF_AUTO(raw_v_e_tables, LoadVertexEdgeTables());
    return Base::addDataToExistedELabel(frag_id, label_id,
                                        std::move(raw_v_e_tables));
  }

  boost::leaf::result<vineyard::ObjectID> AddLabelsToFragmentAsFragmentGroup(
      vineyard::ObjectID frag_id) {
    BOOST_LEAF_AUTO(new_frag_id, AddLabelsToFragment(frag_id));
    VY_OK_OR_RAISE(client_.Persist(new_frag_id));
    return vineyard::ConstructFragmentGroup(client_, new_frag_id, comm_spec_);
  }

  bl::result<vineyard::ObjectID> ExtendLabelData(vineyard::ObjectID frag_id,
                                                 int extend_type) {
    // find duplicate label id
    assert(extend_type);
    auto frag = std::dynamic_pointer_cast<vineyard::ArrowFragmentBase>(
        client_.GetObject(frag_id));
    vineyard::PropertyGraphSchema schema = frag->schema();
    std::vector<std::string> labels;
    label_id_t target_label_id = -1;
    if (extend_type == 1)
      labels = schema.GetVertexLabels();
    else if (extend_type == 2)
      labels = schema.GetEdgeLabels();

    std::map<std::string, label_id_t> label_set;
    for (size_t i = 0; i < labels.size(); ++i)
      label_set[labels[i]] = i;

    if (extend_type == 1) {
      for (size_t i = 0; i < graph_info_->vertices.size(); ++i) {
        auto it = label_set.find(graph_info_->vertices[i]->label);
        if (it != label_set.end()) {
          target_label_id = it->second;
          break;
        }
      }
    } else if (extend_type == 2) {
      for (size_t i = 0; i < graph_info_->edges.size(); ++i) {
        auto it = label_set.find(graph_info_->edges[i]->label);
        if (it != label_set.end()) {
          target_label_id = it->second;
          break;
        }
      }
    } else {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "extend type is invalid");
    }

    if (target_label_id == -1)
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "label not found");
    vineyard::ObjectID new_frag_id;
    if (extend_type == 1) {
      BOOST_LEAF_ASSIGN(new_frag_id,
                        AddDataToExistedVLabel(frag_id, target_label_id));
    } else if (extend_type == 2) {
      BOOST_LEAF_ASSIGN(new_frag_id,
                        AddDataToExistedELabel(frag_id, target_label_id));
    }
    return vineyard::ConstructFragmentGroup(client_, new_frag_id, comm_spec_);
  }

  bl::result<void> initPartitioner() {
#ifdef HASH_PARTITION
    Base::partitioner_.Init(comm_spec_.fnum());
#else
    if (vfiles_.empty() &&
        (graph_info_ != nullptr && graph_info_->vertices.empty())) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidOperationError,
          "Segmented partitioner is not supported when the v-file is "
          "not provided");
    }
    table_vec_t vtables;
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

    Base::partitioner_.Init(comm_spec_.fnum(), oid_list);
#endif
    return {};
  }

 private:
  using Base::loadEdgeTables;
  using Base::loadVertexTables;
  using Base::preprocessInputs;
  using Base::resolveVineyardObject;
  using Base::sanityChecks;

#ifdef ENABLE_JAVA_SDK
  // Location like giraph://filename#input_format_class=className
  bl::result<std::shared_ptr<arrow::Table>> readTableFromGiraph(
      bool load_vertex, const std::string& file_path, int index,
      int total_parts, const std::string formatter) {
    if (giraph_enabled_) {
      if (load_vertex) {
        // There are cases both vertex and edges are specified in vertex file.
        // In this case, we load the data in this function, and suppose call
        // add_edges will be called(empty location),
        // if location is empty, we just return the previous loaded data.
        java_loader_invoker_.load_vertices_and_edges(file_path, formatter);
        return java_loader_invoker_.get_vertex_table();
      } else {
        java_loader_invoker_.load_edges(file_path, formatter);
        return java_loader_invoker_.get_edge_table();
      }
    } else {
      RETURN_GS_ERROR(vineyard::ErrorCode::kIOError,
                      "Please enable giraph in constructor");
    }
    // once set, we will read.
  }
#endif

  bl::result<table_vec_t> loadVertexTables(
      const std::vector<std::shared_ptr<detail::Vertex>>& vertices, int index,
      int total_parts) {
    // a special code path when multiple labeled vertex batches are mixed: for
    // subgraph
    if (vertices.size() == 1 && vertices[0]->protocol == "vineyard") {
      VLOG(2) << "read vertex table from vineyard: " << vertices[0]->values;
      BOOST_LEAF_AUTO(sourceId,
                      this->resolveVineyardObject(vertices[0]->values));
      auto read_procedure = [&]() -> bl::result<table_vec_t> {
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
      table_vec_t normalized_tables;
      for (auto const& table : tables) {
        BOOST_LEAF_AUTO(
            normalized_table,
            vineyard::sync_gs_error(comm_spec_, sync_schema_procedure, table));
        normalized_tables.emplace_back(normalized_table);
      }
      return normalized_tables;
    }
    size_t label_num = vertices.size();
    table_vec_t tables(label_num);
    for (size_t i = 0; i < label_num; ++i) {
      auto read_procedure = [&]() -> bl::result<std::shared_ptr<arrow::Table>> {
        std::shared_ptr<arrow::Table> table;
        if (vertices[i]->protocol == "numpy" ||
            vertices[i]->protocol == "pandas") {
          VY_OK_OR_RAISE(
              vineyard::ReadTableFromPandas(vertices[i]->values, table));
        } else if (vertices[i]->protocol == "vineyard") {
          VLOG(2) << "read vertex table from vineyard: " << vertices[i]->values;
          BOOST_LEAF_AUTO(sourceId,
                          this->resolveVineyardObject(vertices[i]->values));
          VY_OK_OR_RAISE(vineyard::ReadTableFromVineyard(
              client_, sourceId, table, comm_spec_.local_id(),
              comm_spec_.local_num()));
#ifdef ENABLE_JAVA_SDK
        } else if (vertices[i]->protocol == "file" &&
                   vertices[i]->vformat.find("giraph") != std::string::npos) {
          BOOST_LEAF_ASSIGN(
              table, readTableFromGiraph(
                         true, vertices[i]->values, index, total_parts,
                         vertices[i]->vformat));  // true means to load vertex.
#endif
        } else {
          // Let the IOFactory to parse other protocols.
          auto path = vertices[i]->values;
          VY_OK_OR_RAISE(vineyard::ReadTableFromLocation(
              vertices[i]->values, table, index, total_parts));
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

  bl::result<std::vector<table_vec_t>> loadEdgeTables(
      const std::vector<std::shared_ptr<detail::Edge>>& edges, int index,
      int total_parts) {
    // a special code path when multiple labeled edge batches are mixed.
    if (edges.size() == 1 && edges[0]->sub_labels.size() == 1 &&
        edges[0]->sub_labels[0].protocol == "vineyard") {
      LOG(INFO) << "read edge table from vineyard: "
                << edges[0]->sub_labels[0].values;
      BOOST_LEAF_AUTO(sourceId, this->resolveVineyardObject(
                                    edges[0]->sub_labels[0].values));
      auto read_procedure = [&]() -> bl::result<std::vector<table_vec_t>> {
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
      std::vector<table_vec_t> normalized_tables;
      for (auto const& subtables : tables) {
        table_vec_t normalized_subtables;
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
    std::vector<table_vec_t> tables(edge_type_num);

    for (size_t i = 0; i < edge_type_num; ++i) {
      auto sub_labels = edges[i]->sub_labels;

      for (size_t j = 0; j < sub_labels.size(); ++j) {
        std::shared_ptr<arrow::KeyValueMetadata> meta(
            new arrow::KeyValueMetadata());
        meta->Append(LABEL_TAG, edges[i]->label);

        auto load_procedure =
            [&]() -> bl::result<std::shared_ptr<arrow::Table>> {
          std::shared_ptr<arrow::Table> table;
          if (sub_labels[j].protocol == "pandas") {
            VY_OK_OR_RAISE(
                vineyard::ReadTableFromPandas(sub_labels[j].values, table));
          } else if (sub_labels[j].protocol == "vineyard") {
            LOG(INFO) << "read edge table from vineyard: "
                      << sub_labels[j].values;
            BOOST_LEAF_AUTO(sourceId,
                            this->resolveVineyardObject(sub_labels[j].values));
            VY_OK_OR_RAISE(vineyard::ReadTableFromVineyard(
                client_, sourceId, table, comm_spec_.local_id(),
                comm_spec_.local_num()));
            if (table == nullptr) {
              VLOG(2) << "edge table is null";
            } else {
              VLOG(2) << "schema of edge table: "
                      << table->schema()->ToString();
            }
#ifdef ENABLE_JAVA_SDK
          } else if (sub_labels[j].protocol == "file" &&
                     sub_labels[j].eformat.find("giraph") !=
                         std::string::npos) {
            BOOST_LEAF_ASSIGN(
                table, readTableFromGiraph(false, sub_labels[j].values, index,
                                           total_parts, sub_labels[j].eformat));
#endif
          } else {
            // Let the IOFactory to parse other protocols.
            VY_OK_OR_RAISE(vineyard::ReadTableFromLocation(
                sub_labels[j].values, table, index, total_parts));
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

  std::shared_ptr<detail::Graph> graph_info_;

  bool giraph_enabled_;
#ifdef ENABLE_JAVA_SDK
  JavaLoaderInvoker java_loader_invoker_;
#endif
};

namespace detail {

template <typename OID_T, typename VID_T, typename VERTEX_MAP_T>
struct rebind_arrow_fragment_loader;

template <typename OID_T, typename VID_T>
struct rebind_arrow_fragment_loader<
    OID_T, VID_T,
    vineyard::ArrowVertexMap<typename vineyard::InternalType<OID_T>::type,
                             VID_T>> {
  using type = ArrowFragmentLoader<OID_T, VID_T, vineyard::ArrowVertexMap>;
};

template <typename OID_T, typename VID_T>
struct rebind_arrow_fragment_loader<
    OID_T, VID_T,
    vineyard::ArrowLocalVertexMap<typename vineyard::InternalType<OID_T>::type,
                                  VID_T>> {
  using type = ArrowFragmentLoader<OID_T, VID_T, vineyard::ArrowLocalVertexMap>;
};

}  // namespace detail

template <typename OID_T, typename VID_T, typename VERTEX_MAP_T>
using arrow_fragment_loader_t =
    typename detail::rebind_arrow_fragment_loader<OID_T, VID_T,
                                                  VERTEX_MAP_T>::type;

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_LOADER_ARROW_FRAGMENT_LOADER_H_
