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

#ifndef ANALYTICAL_ENGINE_CORE_LOADER_ARROW_FRAGMENT_APPENDER_H_
#define ANALYTICAL_ENGINE_CORE_LOADER_ARROW_FRAGMENT_APPENDER_H_

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "arrow/csv/api.h"
#include "arrow/type_fwd.h"
#include "arrow/util/config.h"
#include "arrow/util/key_value_metadata.h"
#include "boost/algorithm/string/split.hpp"
#include "boost/algorithm/string/trim.hpp"

#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/graph/loader/basic_arrow_fragment_loader.h"
#include "vineyard/graph/utils/table_shuffler.h"

#include "core/fragment/append_only_arrow_fragment.h"

namespace gs {

/**
 * @brief BroadcastOIDs is a utility to broadcast oids
 * @tparam OID_T
 */
template <typename OID_T>
typename std::enable_if<!std::is_same<OID_T, std::string>::value,
                        std::vector<std::vector<OID_T>>>::type
bcast_oids(std::vector<std::shared_ptr<arrow::Table>>& v_tables,
           vineyard::property_graph_types::LABEL_ID_TYPE vertex_label_num,
           grape::CommSpec& comm_spec) {
  using oid_t = OID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

  std::vector<std::vector<oid_t>> oids_list;

  for (auto v_label_id = 0; v_label_id < vertex_label_num; v_label_id++) {
    if (comm_spec.worker_id() == grape::kCoordinatorRank) {
      auto& v_table = v_tables[v_label_id];

      CHECK_EQ(v_table->field(0)->type(), arrow::int64());
      CHECK_EQ(v_table->column(0)->num_chunks(), 1);
      auto array =
          std::dynamic_pointer_cast<oid_array_t>(v_table->column(0)->chunk(0));

      std::vector<oid_t> oids(array->raw_values(),
                              array->raw_values() + array->length());

      grape::sync_comm::Bcast(oids, grape::kCoordinatorRank, comm_spec.comm());
      oids_list.push_back(oids);
    } else {
      std::vector<oid_t> oids;
      grape::sync_comm::Bcast(oids, grape::kCoordinatorRank, comm_spec.comm());
      oids_list.push_back(oids);
    }
  }
  return oids_list;
}

/**
 * @brief This is a specialized BroadcastOIDs for string type
 */
template <typename OID_T>
typename std::enable_if<std::is_same<OID_T, std::string>::value,
                        std::vector<std::vector<OID_T>>>::type
bcast_oids(std::vector<std::shared_ptr<arrow::Table>>& v_tables,
           vineyard::property_graph_types::LABEL_ID_TYPE vertex_label_num,
           grape::CommSpec& comm_spec) {
  using oid_t = std::string;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

  std::vector<std::vector<oid_t>> oids_list;

  for (auto v_label_id = 0; v_label_id < vertex_label_num; v_label_id++) {
    if (comm_spec.worker_id() == grape::kCoordinatorRank) {
      auto& v_table = v_tables[v_label_id];

      CHECK_EQ(v_table->field(0)->type(), arrow::large_utf8());
      CHECK_EQ(v_table->column(0)->num_chunks(), 1);
      auto array =
          std::dynamic_pointer_cast<oid_array_t>(v_table->column(0)->chunk(0));
      std::vector<oid_t> oids(array->length());

      for (int64_t i = 0; i < array->length(); i++) {
        oids[i] = array->GetString(i);
      }

      grape::sync_comm::Bcast(oids, grape::kCoordinatorRank, comm_spec.comm());
      oids_list.push_back(oids);
    } else {
      std::vector<oid_t> oids;
      grape::sync_comm::Bcast(oids, grape::kCoordinatorRank, comm_spec.comm());
      oids_list.push_back(oids);
    }
  }
  return oids_list;
}

/**
 * @brief ArrowFragmentAppender is a utility to modify AppendOnlyArrowFragment
 * in append fashion.
 * @tparam OID_T
 * @tparam VID_T
 */
template <typename OID_T, typename VID_T>
class ArrowFragmentAppender {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using partitioner_t = vineyard::HashPartitioner<oid_t>;
  const int id_column = 0;
  const int src_column = 0;
  const int dst_column = 1;

 public:
  explicit ArrowFragmentAppender(
      grape::CommSpec& comm_spec,
      std::shared_ptr<AppendOnlyArrowFragment<OID_T, VID_T>> fragment)
      : comm_spec_(comm_spec),
        fragment_(fragment),
        vm_ptr_(fragment->GetVertexMap()),
        extra_vm_ptr_(fragment->GetExtraVertexMap()),
        vertex_label_num_(fragment->vertex_label_num_),
        edge_label_num_(fragment->edge_label_num_) {
    partitioner_.Init(comm_spec_.fnum());
  }

  /**
   * Only should be invoked on Coordinator process
   *
   * @param vertex_messages
   * @param edge_messages
   * @param directed
   */
  bl::result<int64_t> ExtendFragment(
      std::vector<std::vector<std::string>>& vertex_messages,
      std::vector<std::vector<std::string>>& edge_messages, bool header_row,
      char delimiter, bool directed) {
    std::vector<std::shared_ptr<arrow::Table>> v_tables(vertex_label_num_);

    if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
      CHECK_EQ(vertex_messages.size(), fragment_->vertex_label_num());
      CHECK_EQ(edge_messages.size(), fragment_->edge_label_num());
    } else {
      CHECK_EQ(vertex_messages.size(), 0);
      CHECK_EQ(edge_messages.size(), 0);
    }

    // parse vertex message and convert it into arrow::table
    for (auto v_label = 0; v_label < vertex_label_num_; v_label++) {
      auto existed_schema = fragment_->vertex_data_table(v_label)->schema();

      if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
        auto& msgs = vertex_messages[v_label];

        if (msgs.empty() || (header_row && msgs.size() == 1)) {
          VY_OK_OR_RAISE(vineyard::EmptyTableBuilder::Build(existed_schema,
                                                            v_tables[v_label]));
        } else {
          BOOST_LEAF_AUTO(tmp_v_table, ReadTable(msgs, header_row, delimiter));

          if (header_row) {
            std::shared_ptr<arrow::Schema> schema_without_id;

            // make sure later append schema is the same with the existed one
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
            ARROW_OK_OR_RAISE(tmp_v_table->schema()->RemoveField(
                id_column, &schema_without_id));
#else
            ARROW_OK_ASSIGN_OR_RAISE(
                schema_without_id,
                tmp_v_table->schema()->RemoveField(id_column));
#endif
            CHECK(schema_without_id->Equals(existed_schema, false));
          }
          v_tables[v_label] = tmp_v_table;
        }
      } else {
        // build an empty table on other workers
        std::shared_ptr<arrow::Schema> schema_with_id;
        auto id_field = std::make_shared<arrow::Field>(
            "id", vineyard::ConvertToArrowType<oid_t>::TypeValue());

#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
        ARROW_OK_OR_RAISE(
            existed_schema->AddField(id_column, id_field, &schema_with_id));
#else
        ARROW_OK_ASSIGN_OR_RAISE(schema_with_id,
                                 existed_schema->AddField(id_column, id_field));
#endif
        VY_OK_OR_RAISE(vineyard::EmptyTableBuilder::Build(schema_with_id,
                                                          v_tables[v_label]));
      }
    }

    BOOST_LEAF_CHECK(updateVertices(v_tables));

    std::vector<std::shared_ptr<arrow::Table>> e_tables(edge_label_num_);

    auto src_gid_field = std::make_shared<arrow::Field>(
        "src", vineyard::ConvertToArrowType<vid_t>::TypeValue());
    auto dst_gid_field = std::make_shared<arrow::Field>(
        "dst", vineyard::ConvertToArrowType<vid_t>::TypeValue());

    for (auto e_label = 0; e_label < edge_label_num_; e_label++) {
      auto existed_schema = fragment_->edge_data_table(e_label)->schema();

      if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
        auto& msgs = edge_messages[e_label];

        if (msgs.empty() || (header_row && msgs.size() == 1)) {
          VY_OK_OR_RAISE(vineyard::EmptyTableBuilder::Build(existed_schema,
                                                            e_tables[e_label]));
        } else {
          auto meta = std::make_shared<arrow::KeyValueMetadata>();
          BOOST_LEAF_AUTO(tmp_table, ReadTable(msgs, header_row, delimiter));
          BOOST_LEAF_AUTO(src_gid_array,
                          parseOidChunkedArray(tmp_table->column(src_column)));
          BOOST_LEAF_AUTO(dst_gid_array,
                          parseOidChunkedArray(tmp_table->column(dst_column)));
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
          ARROW_OK_OR_RAISE(tmp_table->SetColumn(src_column, src_gid_field,
                                                 src_gid_array, &tmp_table));
          ARROW_OK_OR_RAISE(tmp_table->SetColumn(dst_column, dst_gid_field,
                                                 dst_gid_array, &tmp_table));
#else
          ARROW_OK_ASSIGN_OR_RAISE(
              tmp_table,
              tmp_table->SetColumn(src_column, src_gid_field, src_gid_array));
          ARROW_OK_ASSIGN_OR_RAISE(
              tmp_table,
              tmp_table->SetColumn(dst_column, dst_gid_field, dst_gid_array));
#endif

#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
          ARROW_OK_OR_RAISE(tmp_table->RemoveColumn(3, &tmp_table));
          ARROW_OK_OR_RAISE(tmp_table->RemoveColumn(2, &e_tables[e_label]));
#else
          ARROW_OK_ASSIGN_OR_RAISE(tmp_table, tmp_table->RemoveColumn(3));
          ARROW_OK_ASSIGN_OR_RAISE(e_tables[e_label],
                                   tmp_table->RemoveColumn(2));
#endif
        }
      } else {
        VY_OK_OR_RAISE(vineyard::EmptyTableBuilder::Build(existed_schema,
                                                          e_tables[e_label]));
      }
    }

    return updateEdges(e_tables, directed);
  }

 private:
  bl::result<std::shared_ptr<arrow::Table>> ReadTable(
      std::vector<std::string>& lines, bool header_row, char delimiter) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    size_t line_idx = 0;

    if (header_row) {
      std::vector<std::string> column_names;
      auto& line = lines[line_idx++];

      ::boost::algorithm::trim(line);
      ::boost::split(column_names, line,
                     ::boost::is_any_of(std::string(1, delimiter)));

      read_options.column_names = column_names;
    } else {
      read_options.autogenerate_column_names = true;
    }
    parse_options.delimiter = delimiter;

    std::stringstream ss;
    arrow::BufferBuilder builder;

    while (line_idx < lines.size()) {
      std::string line = lines[line_idx++] + '\n';
      ARROW_OK_OR_RAISE(builder.Append(line.data(), line.size()));
    }
    std::shared_ptr<arrow::Buffer> buffer;
    ARROW_OK_OR_RAISE(builder.Finish(&buffer));

    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    auto input = arrow::io::RandomAccessFile::GetStream(buffer_reader, 0,
                                                        buffer->size());

    std::shared_ptr<arrow::csv::TableReader> reader;
#if defined(ARROW_VERSION) && ARROW_VERSION >= 4000000
    ARROW_OK_ASSIGN_OR_RAISE(
        reader, arrow::csv::TableReader::Make(
                    arrow::io::IOContext(pool), input, read_options,
                    parse_options, arrow::csv::ConvertOptions::Defaults()));
#else
    ARROW_OK_ASSIGN_OR_RAISE(
        reader,
        arrow::csv::TableReader::Make(pool, input, read_options, parse_options,
                                      arrow::csv::ConvertOptions::Defaults()));
#endif

    std::shared_ptr<arrow::Table> table;
    ARROW_OK_ASSIGN_OR_RAISE(table, reader->Read());
    ARROW_OK_OR_RAISE(table->Validate());

    return table;
  }

  bl::result<std::shared_ptr<arrow::ChunkedArray>> parseOidChunkedArray(
      const std::shared_ptr<arrow::ChunkedArray>& oid_arrays_in) {
    size_t chunk_num = oid_arrays_in->num_chunks();
    std::vector<std::shared_ptr<arrow::Array>> chunks_out(chunk_num);

    for (size_t chunk_i = 0; chunk_i != chunk_num; ++chunk_i) {
      auto oid_array =
          std::dynamic_pointer_cast<oid_array_t>(oid_arrays_in->chunk(chunk_i));

      typename vineyard::ConvertToArrowType<vid_t>::BuilderType builder;
      size_t size = oid_array->length();
      ARROW_OK_OR_RAISE(builder.Resize(size));

      for (size_t i = 0; i != size; ++i) {
        internal_oid_t oid = oid_array->GetView(i);
        fid_t fid = partitioner_.GetPartitionId(oid_t(oid));
        CHECK(vm_ptr_->GetGid(fid, 0, oid, builder[i]) ||
              extra_vm_ptr_->GetGid(fid, oid_t(oid), builder[i]));
      }
      ARROW_OK_OR_RAISE(builder.Advance(size));
      ARROW_OK_OR_RAISE(builder.Finish(&chunks_out[chunk_i]));
    }

    return std::make_shared<arrow::ChunkedArray>(chunks_out);
  }

  bl::result<void> updateVertices(
      std::vector<std::shared_ptr<arrow::Table>>& v_tables) {
    // every worker got a copy of oids
    auto oids_list = bcast_oids<oid_t>(v_tables, vertex_label_num_, comm_spec_);
    std::vector<ska::flat_hash_set<oid_t>> appended_oid_list;
    auto vid_parser = fragment_->vid_parser_;

    appended_oid_list.resize(vertex_label_num_);
    // maintain vertex map & ivnum
    for (auto v_label = 0; v_label < vertex_label_num_; v_label++) {
      auto& oids = oids_list[v_label];
      auto& curr_ivnum = fragment_->curr_ivnums_[v_label];

      for (auto oid : oids) {
        auto fid = partitioner_.GetPartitionId(oid);
        vid_t gid;

        if (vm_ptr_->GetGid(fid, 0, oid, gid)) {
          auto existed_v_label = vid_parser.GetLabelId(gid);

          CHECK_EQ(existed_v_label, v_label);
        } else if (extra_vm_ptr_->GetGid(fid, oid, gid)) {
          auto existed_v_label = vid_parser.GetLabelId(gid);

          CHECK_EQ(existed_v_label, v_label);
        } else {
          CHECK(extra_vm_ptr_->AddVertex(fid, v_label, oid, gid));

          if (fid == fragment_->fid_) {
            curr_ivnum++;
            appended_oid_list[v_label].emplace(oid);
          }
        }
      }

      for (auto e_label = 0; e_label < edge_label_num_; e_label++) {
        fragment_->extra_oe_indices_[v_label][e_label].resize(
            fragment_->GetInnerVerticesNum(v_label), -1);
      }
      fragment_->curr_tvnums_[v_label] =
          fragment_->curr_ivnums_[v_label] + fragment_->curr_ovnums_[v_label];
    }

    // update extra vertex table
    for (auto v_label_id = 0; v_label_id < vertex_label_num_; v_label_id++) {
      ska::flat_hash_set<oid_t>& appended_oids = appended_oid_list[v_label_id];
      std::shared_ptr<arrow::Table> local_v_table;
      std::shared_ptr<arrow::Table> tmp_table;
      VY_OK_OR_RAISE(::vineyard::ShufflePropertyVertexTable<partitioner_t>(
          comm_spec_, partitioner_, v_tables[v_label_id], tmp_table));
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
      ARROW_OK_OR_RAISE(
          tmp_table->CombineChunks(arrow::default_memory_pool(), &tmp_table));
#else
      ARROW_OK_ASSIGN_OR_RAISE(
          tmp_table, tmp_table->CombineChunks(arrow::default_memory_pool()));
#endif
      auto oid_array = std::dynamic_pointer_cast<oid_array_t>(
          tmp_table->column(id_column)->chunk(0));
      // remove oid column
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
      ARROW_OK_OR_RAISE(tmp_table->RemoveColumn(id_column, &local_v_table));
#else
      ARROW_OK_ASSIGN_OR_RAISE(local_v_table,
                               tmp_table->RemoveColumn(id_column));
#endif

      for (int64_t row = 0; row < oid_array->length(); row++) {
        auto oid = oid_array->GetView(row);

        if (appended_oids.find(oid_t(oid)) != appended_oids.end()) {
          fragment_->extra_vertex_tables_[v_label_id]->AppendValue(
              local_v_table, row);
        }
      }
    }
    return {};
  }

  bl::result<uint64_t> updateEdges(
      std::vector<std::shared_ptr<arrow::Table>>& e_tables, bool directed) {
    std::vector<std::shared_ptr<arrow::Table>> local_e_table(edge_label_num_);

    for (label_id_t i = 0; i < edge_label_num_; ++i) {
      std::shared_ptr<arrow::Table> tmp_table;
      VY_OK_OR_RAISE(::vineyard::ShufflePropertyEdgeTable<vid_t>(
          comm_spec_, fragment_->vid_parser_, src_column, dst_column,
          e_tables[i], tmp_table));
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
      ARROW_OK_OR_RAISE(tmp_table->CombineChunks(arrow::default_memory_pool(),
                                                 &local_e_table[i]));
#else
      ARROW_OK_ASSIGN_OR_RAISE(
          local_e_table[i],
          tmp_table->CombineChunks(arrow::default_memory_pool()));
#endif
    }

    return addExtraEdges(local_e_table, directed);
  }

  bl::result<uint64_t> addExtraEdges(
      const std::vector<std::shared_ptr<arrow::Table>>& edge_tables,
      bool directed) {
    CHECK_EQ(edge_tables.size(), edge_label_num_);
    std::vector<std::vector<vid_t>> collected_ovgids(vertex_label_num_);
    auto& vid_parser = fragment_->vid_parser_;

    auto collect_outer_vertices = [this, &vid_parser,
                                   &collected_ovgids](const auto& gid_array) {
      const vid_t* arr = gid_array->raw_values();
      int64_t length = gid_array->length();

      for (int64_t i = 0; i < length; ++i) {
        if (vid_parser.GetFid(arr[i]) != fragment_->fid_) {
          collected_ovgids[vid_parser.GetLabelId(arr[i])].push_back(arr[i]);
        }
      }
    };

    for (label_id_t e_label = 0; e_label < edge_label_num_; e_label++) {
      auto e_table = edge_tables[e_label];

      CHECK_EQ(e_table->column(0)->num_chunks(), 1);
      CHECK_EQ(e_table->column(1)->num_chunks(), 1);
      auto src_gids = std::dynamic_pointer_cast<
          typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
          e_table->column(0)->chunk(0));
      auto dst_gids = std::dynamic_pointer_cast<
          typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
          e_table->column(1)->chunk(0));

      collect_outer_vertices(src_gids);
      collect_outer_vertices(dst_gids);
    }

    // generate g2l map
    for (label_id_t v_label = 0; v_label < vertex_label_num_; v_label++) {
      auto& gids = collected_ovgids[v_label];
      auto& ovg2l = fragment_->ovg2l_maps_[v_label];
      auto& extra_gids = fragment_->extra_ovgid_lists_[v_label];
      auto& extra_ovg2l = fragment_->extra_ovg2l_maps_[v_label];

      std::sort(gids.begin(), gids.end());
      for (size_t k = 0; k < gids.size(); ++k) {
        if (k == 0 || gids[k] != gids[k - 1]) {
          auto gid = gids[k];

          if (ovg2l->find(gid) == ovg2l->end() &&
              extra_ovg2l.find(gid) == extra_ovg2l.end()) {
            auto lid = vid_parser.GenerateId(
                0, v_label,
                vid_parser.offset_mask() - fragment_->curr_ovnums_[v_label]);

            extra_ovg2l.emplace(gid, lid);
            extra_gids.push_back(gid);
            fragment_->curr_ovnums_[v_label]++;
          }
        }
      }
      fragment_->curr_tvnums_[v_label] =
          fragment_->curr_ivnums_[v_label] + fragment_->curr_ovnums_[v_label];
    }

    // now, insert edges into fragment
    uint64_t total_added_enum = 0;
    for (label_id_t e_label = 0; e_label < edge_label_num_; e_label++) {
      auto& e_table = edge_tables[e_label];  // arrow table with src, dst column
      auto& internal_e_table = fragment_->extra_edge_tables_[e_label];
      // which contains with gids
      auto src_gids = std::dynamic_pointer_cast<
          typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
          e_table->column(src_column)->chunk(0));
      auto dst_gids = std::dynamic_pointer_cast<
          typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
          e_table->column(dst_column)->chunk(0));

      // N.B.: remove src and dst columns;
      std::shared_ptr<arrow::Table> tmp_table;
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
      ARROW_OK_OR_RAISE(e_table->RemoveColumn(dst_column, &tmp_table));
      ARROW_OK_OR_RAISE(tmp_table->RemoveColumn(src_column, &tmp_table));
#else
      ARROW_OK_ASSIGN_OR_RAISE(tmp_table, e_table->RemoveColumn(dst_column));
      ARROW_OK_ASSIGN_OR_RAISE(tmp_table, tmp_table->RemoveColumn(src_column));
#endif

      for (int64_t row = 0; row < e_table->num_rows(); row++) {
        auto src_gid = src_gids->Value(row), dst_gid = dst_gids->Value(row);
        auto src_v_label = vid_parser.GetLabelId(src_gid),
             dst_v_label = vid_parser.GetLabelId(dst_gid);
        vid_t src_lid, dst_lid;
        oid_t src_oid, dst_oid;
        auto& eid = fragment_->extra_oe_nums_[e_label];
        uint32_t added_enum = 0;

        CHECK(fragment_->getOid(src_gid, src_oid));
        CHECK(fragment_->getOid(dst_gid, dst_oid));

        if (vid_parser.GetFid(src_gid) == fragment_->fid_) {
          src_lid = vid_parser.GenerateId(0, src_v_label,
                                          vid_parser.GetOffset(src_gid));
          if (vid_parser.GetFid(dst_gid) == fragment_->fid_) {
            dst_lid = vid_parser.GenerateId(0, dst_v_label,
                                            vid_parser.GetOffset(dst_gid));

            if (!directed) {
              added_enum +=
                  fragment_->addOutgoingEdge(dst_lid, src_lid, e_label, eid);
            }
          } else {
            CHECK(fragment_->ovg2l(dst_gid, dst_lid));
          }
          added_enum +=
              fragment_->addOutgoingEdge(src_lid, dst_lid, e_label, eid);
        } else if (!directed && vid_parser.GetFid(dst_gid) == fragment_->fid_) {
          dst_lid = vid_parser.GenerateId(0, dst_v_label,
                                          vid_parser.GetOffset(dst_gid));
          CHECK(fragment_->ovg2l(src_gid, src_lid));
          added_enum +=
              fragment_->addOutgoingEdge(dst_lid, src_lid, e_label, eid);
        }
        if (added_enum > 0) {
          internal_e_table->AppendValue(tmp_table, row);
          eid++;
          CHECK_EQ(eid, internal_e_table->size());
          total_added_enum += added_enum;
        }
      }
    }
    return total_added_enum;
  }

  grape::CommSpec comm_spec_;
  std::shared_ptr<AppendOnlyArrowFragment<OID_T, VID_T>> fragment_;
  std::shared_ptr<vineyard::ArrowVertexMap<internal_oid_t, VID_T>> vm_ptr_;
  std::shared_ptr<ExtraVertexMap<OID_T, VID_T>> extra_vm_ptr_;
  label_id_t vertex_label_num_;
  label_id_t edge_label_num_;
  partitioner_t partitioner_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_LOADER_ARROW_FRAGMENT_APPENDER_H_
