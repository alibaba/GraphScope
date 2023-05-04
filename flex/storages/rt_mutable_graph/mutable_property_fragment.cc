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

#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

MutablePropertyFragment::MutablePropertyFragment() {}

MutablePropertyFragment::~MutablePropertyFragment() {
  for (auto ptr : dual_csr_list_) {
    if (ptr != NULL) {
      delete ptr;
    }
  }
}

void MutablePropertyFragment::initVertices(
    label_t v_label_i,
    const std::vector<std::pair<std::string, std::string>>& vertex_files) {
  IdIndexer<oid_t, vid_t> indexer;
  std::string v_label_name = schema_.get_vertex_label_name(v_label_i);
  std::vector<std::string> filenames;
  for (auto& pair : vertex_files) {
    if (pair.first == v_label_name) {
      filenames.push_back(pair.second);
    }
  }
  auto& table = vertex_data_[v_label_i];
  auto& property_types = schema_.get_vertex_properties(v_label_name);
  size_t col_num = property_types.size();
  std::vector<std::string> col_names;
  for (size_t col_i = 0; col_i < col_num; ++col_i) {
    col_names.push_back("col_" + std::to_string(col_i));
  }
  table.init(col_names, property_types,
             schema_.get_vertex_storage_strategies(v_label_name),
             schema_.get_max_vnum(v_label_name));
  parseVertexFiles(v_label_name, filenames, indexer);
  if (indexer.bucket_count() == 0) {
    indexer._rehash(schema_.get_max_vnum(v_label_name));
  }
  build_lf_indexer(indexer, lf_indexers_[v_label_i]);
}

void MutablePropertyFragment::initEdges(
    label_t src_label_i, label_t dst_label_i, label_t edge_label_i,
    const std::vector<std::tuple<std::string, std::string, std::string,
                                 std::string>>& edge_files) {
  std::string src_label_name = schema_.get_vertex_label_name(src_label_i);
  std::string dst_label_name = schema_.get_vertex_label_name(dst_label_i);
  std::string edge_label_name = schema_.get_edge_label_name(edge_label_i);
  std::vector<std::string> filenames;
  for (auto& tuple : edge_files) {
    if (std::get<0>(tuple) == src_label_name &&
        std::get<1>(tuple) == dst_label_name &&
        std::get<2>(tuple) == edge_label_name) {
      filenames.push_back(std::get<3>(tuple));
    }
  }
  auto& property_types = schema_.get_edge_properties(
      src_label_name, dst_label_name, edge_label_name);
  size_t col_num = property_types.size();
  CHECK_LE(col_num, 1) << "Only single or no property is supported for edge.";

  size_t index = src_label_i * vertex_label_num_ * edge_label_num_ +
                 dst_label_i * edge_label_num_ + edge_label_i;
  EdgeStrategy oe_strtagy = schema_.get_outgoing_edge_strategy(
      src_label_name, dst_label_name, edge_label_name);
  EdgeStrategy ie_strtagy = schema_.get_incoming_edge_strategy(
      src_label_name, dst_label_name, edge_label_name);

  dual_csr_list_[index] = create_dual_csr<vid_t, timestamp_t>(
      ie_strtagy, oe_strtagy, property_types);
  if (filenames.empty()) {
    dual_csr_list_[index]->ConstructEmptyCsr();
  } else {
    dual_csr_list_[index]->BulkLoad(lf_indexers_[src_label_i],
                                    lf_indexers_[dst_label_i], filenames);
  }
  ie_[index] = dual_csr_list_[index]->GetInCsr();
  oe_[index] = dual_csr_list_[index]->GetOutCsr();
}

void MutablePropertyFragment::Init(
    const Schema& schema,
    const std::vector<std::pair<std::string, std::string>>& vertex_files,
    const std::vector<std::tuple<std::string, std::string, std::string,
                                 std::string>>& edge_files,
    int thread_num) {
  schema_ = schema;
  vertex_label_num_ = schema_.vertex_label_num();
  edge_label_num_ = schema_.edge_label_num();
  vertex_data_.resize(vertex_label_num_);
  ie_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  oe_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  dual_csr_list_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_,
                        NULL);
  lf_indexers_.resize(vertex_label_num_);

  if (thread_num == 1) {
    for (size_t v_label_i = 0; v_label_i != vertex_label_num_; ++v_label_i) {
      initVertices(v_label_i, vertex_files);
    }
    if (!vertex_files.empty()) {
      LOG(INFO) << "finished loading vertices";
    }

    for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
         ++src_label_i) {
      std::string src_label_name = schema_.get_vertex_label_name(src_label_i);
      for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
           ++dst_label_i) {
        std::string dst_label_name = schema_.get_vertex_label_name(dst_label_i);
        for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
          std::string e_label_name = schema_.get_edge_label_name(e_label_i);
          if (schema_.valid_edge_property(src_label_name, dst_label_name,
                                          e_label_name)) {
            initEdges(src_label_i, dst_label_i, e_label_i, edge_files);
          }
        }
      }
    }
    if (!edge_files.empty()) {
      LOG(INFO) << "finished loading edges";
    }
  } else {
    {
      std::atomic<size_t> v_label_id(0);
      std::vector<std::thread> threads(thread_num);
      for (int i = 0; i < thread_num; ++i) {
        threads[i] = std::thread([&]() {
          while (true) {
            size_t cur = v_label_id.fetch_add(1);
            if (cur >= vertex_label_num_) {
              break;
            }
            initVertices(cur, vertex_files);
          }
        });
      }
      for (auto& thrd : threads) {
        thrd.join();
      }
      if (!vertex_files.empty()) {
        LOG(INFO) << "finished loading vertices";
      }
    }
    {
      std::atomic<size_t> e_label_index(0);
      size_t e_label_num =
          vertex_label_num_ * vertex_label_num_ * edge_label_num_;
      std::vector<std::thread> threads(thread_num);
      for (int i = 0; i < thread_num; ++i) {
        threads[i] = std::thread([&]() {
          while (true) {
            size_t cur = e_label_index.fetch_add(1);
            if (cur >= e_label_num) {
              break;
            }
            size_t e_label_i = cur % edge_label_num_;
            cur = cur / edge_label_num_;
            size_t dst_label_i = cur % vertex_label_num_;
            size_t src_label_i = cur / vertex_label_num_;
            std::string src_label_name =
                schema_.get_vertex_label_name(src_label_i);
            std::string dst_label_name =
                schema_.get_vertex_label_name(dst_label_i);
            std::string e_label_name = schema_.get_edge_label_name(e_label_i);
            if (schema_.valid_edge_property(src_label_name, dst_label_name,
                                            e_label_name)) {
              initEdges(src_label_i, dst_label_i, e_label_i, edge_files);
            }
          }
        });
      }
      for (auto& thrd : threads) {
        thrd.join();
      }
      if (!edge_files.empty()) {
        LOG(INFO) << "finished loading edges";
      }
    }
  }
}

void MutablePropertyFragment::IngestEdge(label_t src_label, vid_t src_lid,
                                         label_t dst_label, vid_t dst_lid,
                                         label_t edge_label, timestamp_t ts,
                                         grape::OutArchive& arc,
                                         ArenaAllocator& alloc) {
  size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                 dst_label * edge_label_num_ + edge_label;
  dual_csr_list_[index]->IngestEdge(src_lid, dst_lid, ts, arc, alloc);
}

void MutablePropertyFragment::PutEdge(label_t src_label, vid_t src_lid,
                                      label_t dst_label, vid_t dst_lid,
                                      label_t edge_label, timestamp_t ts,
				      const Property& data,
                                      ArenaAllocator& alloc) {
  size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                 dst_label * edge_label_num_ + edge_label;
  dual_csr_list_[index]->PutEdge(src_lid, dst_lid, ts, data, alloc);
}

const Schema& MutablePropertyFragment::schema() const { return schema_; }

void MutablePropertyFragment::Serialize(const std::string& prefix) {
  std::string data_dir = prefix + "/data";
  if (!std::filesystem::exists(data_dir)) {
    std::filesystem::create_directory(data_dir);
  }
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(prefix + "/init_snapshot.bin"));
  io_adaptor->Open("wb");
  schema_.Serialize(io_adaptor);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    lf_indexers_[i].Serialize(data_dir + "/indexer_" + std::to_string(i));
  }
  label_t cur_index = 0;
  for (auto& table : vertex_data_) {
    table.Serialize(io_adaptor,
                    data_dir + "/vtable_" + std::to_string(cur_index),
                    vertex_num(cur_index));
    ++cur_index;
  }
  for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
       ++src_label_i) {
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
         ++dst_label_i) {
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        size_t index = src_label_i * vertex_label_num_ * edge_label_num_ +
                       dst_label_i * edge_label_num_ + e_label_i;
        dual_csr_list_[index]->Serialize(data_dir + "/e_" + src_label + "_" +
                                         dst_label + "_" + edge_label);
      }
    }
  }

  io_adaptor->Close();
}

void MutablePropertyFragment::Deserialize(const std::string& prefix) {
  std::string data_dir = prefix + "/data";
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(prefix + "/init_snapshot.bin"));
  io_adaptor->Open();
  schema_.Deserialize(io_adaptor);

  vertex_label_num_ = schema_.vertex_label_num();
  edge_label_num_ = schema_.edge_label_num();
  lf_indexers_.resize(vertex_label_num_);
  vertex_data_.resize(vertex_label_num_);
  ie_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  oe_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  dual_csr_list_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_,
                        NULL);

  for (size_t i = 0; i < vertex_label_num_; ++i) {
    lf_indexers_[i].Deserialize(data_dir + "/indexer_" + std::to_string(i));
  }
  label_t cur_index = 0;
  for (auto& table : vertex_data_) {
    table.Deserialize(io_adaptor,
                      data_dir + "/vtable_" + std::to_string(cur_index));
    cur_index += 1;
  }
  for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
       ++src_label_i) {
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
         ++dst_label_i) {
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        size_t index = src_label_i * vertex_label_num_ * edge_label_num_ +
                       dst_label_i * edge_label_num_ + e_label_i;
        auto& properties =
            schema_.get_edge_properties(src_label, dst_label, edge_label);
        EdgeStrategy oe_strategy = schema_.get_outgoing_edge_strategy(
            src_label, dst_label, edge_label);
        EdgeStrategy ie_strategy = schema_.get_incoming_edge_strategy(
            src_label, dst_label, edge_label);
        dual_csr_list_[index] = create_dual_csr<vid_t, timestamp_t>(
            ie_strategy, oe_strategy, properties);
        dual_csr_list_[index]->Deserialize(data_dir + "/e_" + src_label + "_" +
                                           dst_label + "_" + edge_label);
        ie_[index] = dual_csr_list_[index]->GetInCsr();
        oe_[index] = dual_csr_list_[index]->GetOutCsr();
      }
    }
  }
}

Table& MutablePropertyFragment::get_vertex_table(label_t vertex_label) {
  return vertex_data_[vertex_label];
}

const Table& MutablePropertyFragment::get_vertex_table(
    label_t vertex_label) const {
  return vertex_data_[vertex_label];
}

vid_t MutablePropertyFragment::vertex_num(label_t vertex_label) const {
  return static_cast<vid_t>(lf_indexers_[vertex_label].size());
}

bool MutablePropertyFragment::get_lid(label_t label, oid_t oid,
                                      vid_t& lid) const {
  return lf_indexers_[label].get_index(oid, lid);
}

oid_t MutablePropertyFragment::get_oid(label_t label, vid_t lid) const {
  return lf_indexers_[label].get_key(lid);
}

vid_t MutablePropertyFragment::add_vertex(label_t label, oid_t id) {
  return lf_indexers_[label].insert(id);
}

std::shared_ptr<GenericNbrIterator<vid_t>>
MutablePropertyFragment::get_outgoing_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label,
                                            timestamp_t ts) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->get_generic_basic_graph_view(ts)->get_generic_basic_edges(
      u);
}

std::shared_ptr<GenericNbrIterator<vid_t>>
MutablePropertyFragment::get_incoming_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label,
                                            timestamp_t ts) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->get_generic_basic_graph_view(ts)->get_generic_basic_edges(
      u);
}

std::shared_ptr<GenericNbrIteratorMut<vid_t>>
MutablePropertyFragment::get_outgoing_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label,
                                                timestamp_t ts) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->generic_edge_iter_mut(u, ts);
}

std::shared_ptr<GenericNbrIteratorMut<vid_t>>
MutablePropertyFragment::get_incoming_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label,
                                                timestamp_t ts) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->generic_edge_iter_mut(u, ts);
}

MutableCsrBase<vid_t, timestamp_t>* MutablePropertyFragment::get_oe_csr(
    label_t label, label_t neighbor_label, label_t edge_label) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

const MutableCsrBase<vid_t, timestamp_t>* MutablePropertyFragment::get_oe_csr(
    label_t label, label_t neighbor_label, label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

MutableCsrBase<vid_t, timestamp_t>* MutablePropertyFragment::get_ie_csr(
    label_t label, label_t neighbor_label, label_t edge_label) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index];
}

const MutableCsrBase<vid_t, timestamp_t>* MutablePropertyFragment::get_ie_csr(
    label_t label, label_t neighbor_label, label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index];
}

void MutablePropertyFragment::parseVertexFiles(
    const std::string& vertex_label, const std::vector<std::string>& filenames,
    IdIndexer<oid_t, vid_t>& indexer) {
  if (filenames.empty()) {
    return;
  }

  size_t label_index = schema_.get_vertex_label_id(vertex_label);
  auto& table = vertex_data_[label_index];
  auto& property_types = schema_.get_vertex_properties(vertex_label);
  size_t col_num = property_types.size();
  std::vector<Property> properties(col_num);
  for (size_t col_i = 0; col_i != col_num; ++col_i) {
    properties[col_i].set_type(property_types[col_i] == PropertyType::kString
                                   ? PropertyType::kStringView
                                   : property_types[col_i]);
  }

  char line_buf[4096];
  oid_t oid;
  vid_t v_index;
  bool first_file = true;
  std::vector<Property> header(col_num + 1);
  for (auto& item : header) {
    item.set_type(PropertyType::kString);
  }
  for (auto filename : filenames) {
    FILE* fin = fopen(filename.c_str(), "r");
    if (fgets(line_buf, 4096, fin) == NULL) {
      continue;
    }
    preprocess_line(line_buf);
    if (first_file) {
      ParseRecord(line_buf, header);
      std::vector<std::string> col_names(col_num);
      for (size_t i = 0; i < col_num; ++i) {
        col_names[i] = header[i + 1].get_value<std::string>();
      }
      table.reset_header(col_names);
      first_file = false;
    }

    while (fgets(line_buf, 4096, fin) != NULL) {
      preprocess_line(line_buf);
      ParseRecord(line_buf, oid, properties);
      if (indexer.add(oid, v_index)) {
        table.insert(v_index, properties);
      }
    }

    fclose(fin);
  }
}

}  // namespace gs
