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

#ifndef ANALYTICAL_ENGINE_CORE_LOADER_APPEND_ONLY_ARROW_FRAGMENT_LOADER_H_
#define ANALYTICAL_ENGINE_CORE_LOADER_APPEND_ONLY_ARROW_FRAGMENT_LOADER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/config.h"

#include "grape/worker/comm_spec.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/loader/basic_arrow_fragment_loader.h"
#include "vineyard/graph/vertex_map/arrow_vertex_map.h"
#include "vineyard/io/io/local_io_adaptor.h"

#include "core/fragment/append_only_arrow_fragment.h"

namespace gs {

/**
 * @brief AppendOnlyArrowFragmentLoader loads
 * AppendOnlyArrowFragment from local files.
 * @tparam OID_T
 * @tparam VID_T
 */
template <typename OID_T = vineyard::property_graph_types::OID_TYPE,
          typename VID_T = vineyard::property_graph_types::VID_TYPE>
class AppendOnlyArrowFragmentLoader {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vertex_map_t = vineyard::ArrowVertexMap<internal_oid_t, vid_t>;
  const int id_column = 0;   // index of vertex id column in vertex table
  const int src_column = 0;  // index of src id column in edge table
  const int dst_column = 1;  // index of dst id column in edge table
  using partitioner_t = vineyard::HashPartitioner<oid_t>;
  using basic_loader_t =
      vineyard::BasicArrowFragmentLoader<oid_t, vid_t, partitioner_t>;

 public:
  AppendOnlyArrowFragmentLoader(vineyard::Client& client,
                                const grape::CommSpec& comm_spec,
                                label_id_t vertex_label_num,
                                label_id_t edge_label_num, std::string efile,
                                std::string vfile, bool directed = true)
      : client_(client),
        comm_spec_(comm_spec),
        efile_(std::move(efile)),
        vfile_(std::move(vfile)),
        vertex_label_num_(vertex_label_num),
        edge_label_num_(edge_label_num),
        directed_(directed),
        basic_arrow_fragment_loader_(comm_spec) {}

  ~AppendOnlyArrowFragmentLoader() = default;

  bl::result<vineyard::ObjectID> LoadFragment() {
    BOOST_LEAF_CHECK(initBasicLoader());

    BOOST_LEAF_AUTO(local_v_tables,
                    basic_arrow_fragment_loader_.ShuffleVertexTables(false));
    auto oid_lists = basic_arrow_fragment_loader_.GetOidLists();

    vineyard::BasicArrowVertexMapBuilder<
        typename vineyard::InternalType<oid_t>::type, vid_t>
        vm_builder(client_, comm_spec_.fnum(), vertex_label_num_, oid_lists);
    auto vm = vm_builder.Seal(client_);
    auto vm_ptr =
        std::dynamic_pointer_cast<vertex_map_t>(client_.GetObject(vm->id()));
    auto mapper = [&vm_ptr](fid_t fid, label_id_t label, internal_oid_t oid,
                            vid_t& gid) {
      return vm_ptr->GetGid(fid, label, oid, gid);
    };
    BOOST_LEAF_AUTO(local_e_tables,
                    basic_arrow_fragment_loader_.ShuffleEdgeTables(mapper));
    BasicAppendOnlyArrowFragmentBuilder<oid_t, vid_t> frag_builder(client_,
                                                                   vm_ptr);

    BOOST_LEAF_CHECK(frag_builder.Init(comm_spec_.fid(), comm_spec_.fnum(),
                                       std::move(local_v_tables),
                                       std::move(local_e_tables), directed_));

    auto frag =
        std::dynamic_pointer_cast<AppendOnlyArrowFragment<oid_t, vid_t>>(
            frag_builder.Seal(client_));

    return frag->id();
  }

 private:
  bl::result<void> initBasicLoader() {
    std::vector<std::string> v_list;
    boost::split(v_list, vfile_, boost::is_any_of(";"));
    std::vector<std::shared_ptr<arrow::Table>> partial_v_tables(
        v_list.size() * vertex_label_num_);

    std::vector<std::string> e_list;
    boost::split(e_list, efile_, boost::is_any_of(";"));
    std::vector<std::vector<std::shared_ptr<arrow::Table>>> partial_e_tables(
        e_list.size());
    auto io_deleter = [](vineyard::LocalIOAdaptor* adaptor) {
      VINEYARD_SUPPRESS(adaptor->Close());
      delete adaptor;
    };

    for (size_t i = 0; i < v_list.size(); ++i) {
      for (label_id_t j = 0; j < vertex_label_num_; ++j) {
        std::unique_ptr<vineyard::LocalIOAdaptor,
                        std::function<void(vineyard::LocalIOAdaptor*)>>
            io_adaptor(
                new vineyard::LocalIOAdaptor(
                    v_list[i] + "_" + std::to_string(j) + "#header_row=true"),
                io_deleter);
        VY_OK_OR_RAISE(io_adaptor->SetPartialRead(comm_spec_.worker_id(),
                                                  comm_spec_.worker_num()));
        VY_OK_OR_RAISE(io_adaptor->Open());
        std::shared_ptr<arrow::Table> tmp_table;
        VY_OK_OR_RAISE(io_adaptor->ReadTable(&tmp_table));
        std::shared_ptr<arrow::KeyValueMetadata> meta(
            new arrow::KeyValueMetadata());
        meta->Append("type", "VERTEX");
        meta->Append("label_index", std::to_string(j));

        meta->Append(basic_loader_t::ID_COLUMN, "0");

        auto adaptor_meta = io_adaptor->GetMeta();
        if (adaptor_meta.count("label") == 0) {
          return bl::new_error(
              vineyard::ErrorCode::kIOError,
              "Metadata of input vertex files should contain label name");
        }
        std::string label_name = adaptor_meta.find("label")->second;
        meta->Append("label", label_name);

        partial_v_tables[i * vertex_label_num_ + j] =
            tmp_table->ReplaceSchemaMetadata(meta);

        vertex_label_to_index_[label_name] = j;
      }
    }

    for (size_t got = 0; got < e_list.size(); ++got) {
      for (label_id_t j = 0; j < edge_label_num_; ++j) {
        std::unique_ptr<vineyard::LocalIOAdaptor,
                        std::function<void(vineyard::LocalIOAdaptor*)>>
            io_adaptor(
                new vineyard::LocalIOAdaptor(
                    e_list[got] + "_" + std::to_string(j) + "#header_row=true"),
                io_deleter);
        VY_OK_OR_RAISE(io_adaptor->SetPartialRead(comm_spec_.worker_id(),
                                                  comm_spec_.worker_num()));
        VY_OK_OR_RAISE(io_adaptor->Open());
        std::shared_ptr<arrow::Table> tmp_table;
        VY_OK_OR_RAISE(io_adaptor->ReadTable(&tmp_table));

#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
        ARROW_OK_OR_RAISE(tmp_table->RemoveColumn(3, &tmp_table));
        ARROW_OK_OR_RAISE(tmp_table->RemoveColumn(2, &tmp_table));
#else
        ARROW_OK_ASSIGN_OR_RAISE(tmp_table, tmp_table->RemoveColumn(3));
        ARROW_OK_ASSIGN_OR_RAISE(tmp_table, tmp_table->RemoveColumn(2));
#endif

        std::shared_ptr<arrow::KeyValueMetadata> meta(
            new arrow::KeyValueMetadata());
        auto adaptor_meta = io_adaptor->GetMeta();
        meta->Append("type", "EDGE");
        meta->Append("label_index", std::to_string(j));
        meta->Append(basic_loader_t::SRC_COLUMN, std::to_string(src_column));
        meta->Append(basic_loader_t::DST_COLUMN, std::to_string(dst_column));

        auto search = adaptor_meta.find("label");
        if (search == adaptor_meta.end()) {
          return bl::new_error(
              vineyard::ErrorCode::kIOError,
              "Metadata of input edge files should contain label name");
        }
        meta->Append("label", search->second);

        search = adaptor_meta.find("src_label");
        if (search == adaptor_meta.end()) {
          return bl::new_error(
              vineyard::ErrorCode::kIOError,
              "Metadata of input edge files should contain src label name");
        }
        meta->Append(basic_loader_t::SRC_LABEL_ID,
                     std::to_string(vertex_label_to_index_.at(search->second)));

        search = adaptor_meta.find("dst_label");
        if (search == adaptor_meta.end()) {
          return bl::new_error(
              vineyard::ErrorCode::kIOError,
              "Metadata of input edge files should contain dst label name");
        }

        partial_e_tables[j].emplace_back(
            tmp_table->ReplaceSchemaMetadata(meta));
      }
    }

    basic_arrow_fragment_loader_.Init(partial_v_tables, partial_e_tables);
    partitioner_t partitioner;
    partitioner.Init(comm_spec_.fnum());
    basic_arrow_fragment_loader_.SetPartitioner(partitioner);
    return {};
  }

  std::map<std::string, label_id_t> vertex_label_to_index_;

  vineyard::Client& client_;
  grape::CommSpec comm_spec_;
  std::string efile_, vfile_;

  label_id_t vertex_label_num_, edge_label_num_;

  bool directed_;
  basic_loader_t basic_arrow_fragment_loader_;
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_LOADER_APPEND_ONLY_ARROW_FRAGMENT_LOADER_H_
