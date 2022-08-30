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

#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/graph/loader/basic_ev_fragment_builder.h"
#include "vineyard/graph/loader/fragment_loader_utils.h"
#include "vineyard/io/io/i_io_adaptor.h"
#include "vineyard/io/io/io_factory.h"
#include "vineyard/graph/utils/partitioner.h"

#include "gsf/graph_info.h"
#include "gsf/utils/trans.h"
#include "gsf/reader/arrow_chunk_reader.h"

#include "core/error.h"
#include "core/io/property_parser.h"

namespace bl = boost::leaf;

namespace gs {
/**
 * @brief This builder can load a ArrowFragment from graph store format data source
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <typename OID_T = vineyard::property_graph_types::OID_TYPE,
          typename VID_T = vineyard::property_graph_types::VID_TYPE>
class ArrowFragmentBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vertex_map_t = vineyard::ArrowVertexMap<internal_oid_t, vid_t>;
  using range_t = std::pair<gsf::IdType, gsf::IdType>;
  static constexpr const char* LABEL_TAG = "label";
  static constexpr const char* SRC_LABEL_TAG = "src_label";
  static constexpr const char* DST_LABEL_TAG = "dst_label";

  const int id_column = 0;

// #ifdef HASH_PARTITION
//   using partitioner_t = vineyard::HashPartitioner<oid_t>;
// #else
  using partitioner_t = typename vineyard::SegmentedPartitioner<oid_t>;
// #endif
  using table_vec_t = std::vector<std::shared_ptr<arrow::Table>>;
  using vertex_table_info_t =
      std::map<std::string, std::pair<range_t, std::shared_ptr<arrow::Table>>>;
  using edge_table_info_t = std::vector<vineyard::InputTable>;

 public:
  ArrowFragmentBuilder(vineyard::Client& client,
                      const grape::CommSpec& comm_spec,
                      std::shared_ptr<gsf::GraphInfo> graph_info)
      : client_(client),
        comm_spec_(comm_spec),
        graph_info_(graph_info),
        directed_(true),
        generate_eid_(false) {}

  ~ArrowFragmentBuilder() = default;

  bl::result<std::pair<vertex_table_info_t, edge_table_info_t>>
  LoadVertexEdgeTables() {
    if (graph_info_) {
      std::stringstream labels;
      labels << "Loading ";
      if (graph_info_->GetAllVertexInfo().empty() && graph_info_->GetAllAdjListInfo().empty()) {
        labels << "empty graph";
      } else {
        for (auto it = graph_info_->GetAllVertexInfo().begin(); it != graph_info_->GetAllVertexInfo().end(); ++it) {
          if (it == graph_info_->GetAllVertexInfo().begin()) {
            labels << "vertex labeled ";  // prefix
          } else {
            labels << ", ";  // label seperator
          }
          labels << it->first;
        }

        if (!graph_info_->GetAllVertexInfo().empty()) {
          labels << " and ";
        }
        for (auto it = graph_info_->GetAllAdjListInfo().begin(); it != graph_info_->GetAllAdjListInfo().end(); ++it) {
          if (it == graph_info_->GetAllAdjListInfo().begin()) {
            labels << "edge labeled ";  // prefix
          } else {
            labels << ", ";  // label seperator
          }
          labels << it->first;
        }
      }
      LOG_IF(INFO, comm_spec_.worker_id() == 0)
          << "PROGRESS--GRAPH-LOADING-DESCRIPTION-" << labels.str();
    }
    BOOST_LEAF_AUTO(v_tables, LoadVertexTables(comm_spec_.worker_id(), comm_spec_.worker_num()));
    BOOST_LEAF_AUTO(e_tables, LoadEdgeTables(comm_spec_.worker_id(), comm_spec_.worker_num()));
    return std::make_pair(v_tables, e_tables);
  }

  bl::result<vertex_table_info_t> LoadVertexTables(int index, int total_parts) {
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-READ-VERTEX-0";
    vertex_table_info_t v_tables;
    if (graph_info_) {
      const auto& vertex_infos = graph_info_->GetAllVertexInfo();
      for (const auto& item : vertex_infos) {
        const auto& vertex_info = item.second;
        int distribute_chunk_num = vertex_info.ChunkNum() / total_parts;
        gsf::IdType start_id = index * distribute_chunk_num;
        range_t id_range;
        id_range.first = start_id;
        id_range.second = (index + 1) * distribute_chunk_num;
        if (comm_spec_.worker_id() == comm_spec_.worker_num() - 1) {
          distribute_chunk_num += vertex_info.ChunkNum() % total_parts;
          id_range.second = vertex_info.GetOffsetEnd();
        }
        LOG(INFO) << "distribute num: " << distribute_chunk_num;
        for (const auto& pg : vertex_info.GetPropertyGroups()) {
          auto expect = gsf::ConstructVertexPropertyArrowChunkReader(*(graph_info_.get()), vertex_info.GetLabel(), pg);
          CHECK(!expect.has_error());
          auto& reader = expect.value();
          CHECK(reader.seek(start_id).ok());
          table_vec_t chunk_tables(distribute_chunk_num);
          for (int i = 0; i < distribute_chunk_num; ++i) {
            auto result = reader.GetChunk();
            chunk_tables[i] = result.value();
            // TODO: concatenate the result to property table.
            reader.next_chunk();
          }
          v_tables[vertex_info.GetLabel()] = std::make_pair(id_range, arrow::ConcatenateTables(chunk_tables).ValueOrDie());
        }
      }
    }
    // for (const auto& table : v_tables) {
    //   BOOST_LEAF_CHECK(sanityChecks(table));
    // }
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-READ-VERTEX-100";
    return v_tables;
  }

  bl::result<edge_table_info_t> LoadEdgeTables(int index, int total_parts) {
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-READ-EDGE-0";
    edge_table_info_t e_tables;
    if (graph_info_) {
      const auto& adj_list_infos = graph_info_->GetAllAdjListInfo();
      for (const auto& item : adj_list_infos) {
        const auto& adj_list_info = item.second;
        auto src_label = adj_list_info.GetSrcLabel();
        const auto& vertex_info  = graph_info_->GetVertexInfo(src_label).value();
        int distribute_chunk_num = vertex_info.ChunkNum() / total_parts;
        gsf::IdType start_id = index * distribute_chunk_num;
        gsf::IdType end_id = (index + 1) * distribute_chunk_num;
        gsf::IdType begin_offset, end_offset;
        if (comm_spec_.worker_id() == comm_spec_.worker_num() - 1) {
          distribute_chunk_num += vertex_info.ChunkNum() % total_parts;
          end_offset = adj_list_info.GetOffsetEnd();
        }
        auto expect = gsf::ConstructAdjListArrowChunkReader(*(graph_info_.get()), adj_list_info.GetSrcLabel(), adj_list_info.GetEdgeLabel(),
            adj_list_info.GetDstLabel(), gsf::AdjListType::ordered_by_source);
        CHECK(!expect.has_error());
        auto& reader = expect.value();
        begin_offset = gsf::vertex_id_to_adj_list_offset(adj_list_info, vertex_info, reader.GetPrefix(), gsf::AdjListType::ordered_by_source, start_id).value();
        if (comm_spec_.worker_id() != comm_spec_.worker_num() - 1) {
          end_offset = gsf::vertex_id_to_adj_list_offset(adj_list_info, vertex_info, reader.GetPrefix(), gsf::AdjListType::ordered_by_source, end_id).value();
        }
        gsf::IdType edge_num = 0;
        CHECK(reader.seek_src(start_id).ok());
        table_vec_t chunk_tables;
        chunk_tables.push_back(reader.GetChunk().value());
        edge_num += chunk_tables.back()->num_rows();
        while (reader.next_chunk().ok()) {
          auto table = reader.GetChunk().value();
          if (edge_num + table->num_rows() <= (end_offset - begin_offset)) {
            chunk_tables.push_back(table);
            edge_num += table->num_rows();
          } else {
            int64_t slice_num = (end_offset - begin_offset) - edge_num;
            LOG(INFO) << "slice_num=" << slice_num;
            chunk_tables.push_back(table->Slice(0, slice_num));
          }
          if (edge_num == (end_offset - begin_offset)) {
            break;
          }
        }
        auto table = arrow::ConcatenateTables(chunk_tables).ValueOrDie();
        e_tables.emplace_back(src_label, adj_list_info.GetDstLabel(), adj_list_info.GetEdgeLabel(), table);
      }
    }
    // for (const auto& table_vec : e_tables) {
    //   for (const auto& table : table_vec) {
    //    BOOST_LEAF_CHECK(sanityChecks(table));
    //   }
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-READ-EDGE-100";
    return e_tables;
  }

  bl::result<vineyard::ObjectID> LoadFragment() {
    partitioner_t partitioner;
    initPartitioner(partitioner);
    BOOST_LEAF_AUTO(v_e_tables, LoadVertexEdgeTables());
    // auto& partial_v_tables = raw_v_e_tables.first;
    // auto& partial_e_tables = raw_v_e_tables.second;

    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-0";

    // BOOST_LEAF_AUTO(v_e_tables, preprocessInputs(partitioner, partial_v_tables,
    //                                              partial_e_tables));

    auto& vertex_tables_with_label = v_e_tables.first;
    auto& edge_tables_with_label = v_e_tables.second;

    std::shared_ptr<
        vineyard::BasicEVFragmentBuilder<OID_T, VID_T, partitioner_t>>
        basic_fragment_loader = std::make_shared<
            vineyard::BasicEVFragmentBuilder<OID_T, VID_T, partitioner_t>>(
            client_, comm_spec_, partitioner, directed_, false, generate_eid_);

    for (auto& pair : vertex_tables_with_label) {
      BOOST_LEAF_CHECK(
          basic_fragment_loader->AddVertexTable(pair.first, pair.second));
    }
    BOOST_LEAF_CHECK(basic_fragment_loader->ConstructVertices());
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-100";
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-0";

    vertex_tables_with_label.clear();

    for (auto& table : edge_tables_with_label) {
      BOOST_LEAF_CHECK(basic_fragment_loader->AddEdgeTable(
          table.src_label, table.dst_label, table.edge_label, table.table));
    }
    edge_tables_with_label.clear();

    BOOST_LEAF_CHECK(basic_fragment_loader->ConstructEdges());
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-100";
    LOG_IF(INFO, comm_spec_.worker_id() == 0)
        << "PROGRESS--GRAPH-LOADING-SEAL-0";
    return basic_fragment_loader->ConstructFragment();
  }

 private:
 /*
  bl::result<std::pair<vertex_table_info_t, edge_table_info_t>>
  preprocessInputs(partitioner_t partitioner, const table_vec_t& v_tables,
                   const table_vec_t& e_tables,
                   const std::set<std::string>& previous_vertex_labels =
                       std::set<std::string>()) {
    vertex_table_info_t vertex_tables_with_label;
    edge_table_info_t edge_tables_with_label;
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

    for (auto table : e_table) {
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
    }

    return std::make_pair(vertex_tables_with_label, edge_tables_with_label);
  }
  */

/*
  bl::result<vineyard::ObjectID> resolveVYObject(std::string const& source) {
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
  bl::result<void> sanityChecks(std::shared_ptr<arrow::Table> table) {
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
  */

  bl::result<void> initPartitioner(partitioner_t& partitioner) {
    if (graph_info_ == nullptr) {
      RETURN_GS_ERROR(
          vineyard::ErrorCode::kInvalidOperationError,
          "Segmented partitioner is not supported when the v-file is "
          "not provided");
    }
    if (graph_info_) {
      const auto& vertex_infos = graph_info_->GetAllVertexInfo();
      for (auto& item : vertex_infos) {
        const auto& vertex_info = item.second;
        int distribute_chunk_num = vertex_info.ChunkNum() / comm_spec_.worker_num();
        for (int wid = 0; wid < comm_spec_.worker_num(); ++wid) {
          gsf::IdType begin = vertex_info.GetOffsetBegin() + wid * distribute_chunk_num;
          gsf::IdType end;
          if (wid != comm_spec_.worker_num() - 1) {
            end = std::min(vertex_info.GetOffsetBegin() + (wid+1) * distribute_chunk_num, vertex_info.GetOffsetEnd());
          } else {
            end = vertex_info.GetOffsetEnd();
          }
          for (gsf::IdType id = begin; id < end; ++id) {
            partitioner.SetPartitionId(id, wid);
          }
        }
      }
    }
    return {};
  }

  vineyard::Client& client_;
  grape::CommSpec comm_spec_;

  std::shared_ptr<gsf::GraphInfo> graph_info_;

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
