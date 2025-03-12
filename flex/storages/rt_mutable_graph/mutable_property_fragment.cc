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

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/storages/rt_mutable_graph/file_names.h"
#include "flex/utils/property/types.h"

namespace gs {

MutablePropertyFragment::MutablePropertyFragment() {}

MutablePropertyFragment::~MutablePropertyFragment() {
  std::vector<size_t> degree_list(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    degree_list[i] = lf_indexers_[i].size();
    vertex_data_[i].resize(degree_list[i]);
  }
  for (size_t src_label = 0; src_label != vertex_label_num_; ++src_label) {
    for (size_t dst_label = 0; dst_label != vertex_label_num_; ++dst_label) {
      for (size_t e_label = 0; e_label != edge_label_num_; ++e_label) {
        size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                       dst_label * edge_label_num_ + e_label;
        if (dual_csr_list_[index] != NULL) {
          dual_csr_list_[index]->Resize(degree_list[src_label],
                                        degree_list[dst_label]);
          delete dual_csr_list_[index];
        }
      }
    }
  }
}

void MutablePropertyFragment::loadSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open();
  schema_.Deserialize(io_adaptor);
}

void MutablePropertyFragment::Clear() {
  for (auto ptr : dual_csr_list_) {
    if (ptr != NULL) {
      delete ptr;
    }
  }
  lf_indexers_.clear();
  vertex_data_.clear();
  ie_.clear();
  oe_.clear();
  dual_csr_list_.clear();
  vertex_label_num_ = 0;
  edge_label_num_ = 0;
  schema_.Clear();
}

void MutablePropertyFragment::DumpSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open("wb");
  schema_.Serialize(io_adaptor);
  io_adaptor->Close();
}

inline DualCsrBase* create_csr(EdgeStrategy oes, EdgeStrategy ies,
                               const std::vector<PropertyType>& properties,
                               bool oe_mutable, bool ie_mutable,
                               const std::vector<std::string>& prop_names) {
  if (properties.empty()) {
    return new DualCsr<grape::EmptyType>(oes, ies, oe_mutable, ie_mutable);
  } else if (properties.size() == 1) {
    if (properties[0] == PropertyType::kBool) {
      return new DualCsr<bool>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kInt32) {
      return new DualCsr<int32_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kUInt32) {
      return new DualCsr<uint32_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kDate) {
      return new DualCsr<Date>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kInt64) {
      return new DualCsr<int64_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kUInt64) {
      return new DualCsr<uint64_t>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kDouble) {
      return new DualCsr<double>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0] == PropertyType::kFloat) {
      return new DualCsr<float>(oes, ies, oe_mutable, ie_mutable);
    } else if (properties[0].type_enum == impl::PropertyTypeImpl::kVarChar) {
      return new DualCsr<std::string_view>(
          oes, ies, properties[0].additional_type_info.max_length, oe_mutable,
          ie_mutable);
    } else if (properties[0] == PropertyType::kStringView) {
      return new DualCsr<std::string_view>(
          oes, ies, gs::PropertyType::GetStringDefaultMaxLength(), oe_mutable,
          ie_mutable);
    }
  } else {
    // TODO: fix me, storage strategy not set
    return new DualCsr<RecordView>(oes, ies, prop_names, properties, {},
                                   oe_mutable, ie_mutable);
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
  return nullptr;
}

void MutablePropertyFragment::Open(const std::string& work_dir,
                                   int memory_level) {
  std::string schema_file = schema_path(work_dir);
  std::string snapshot_dir{};
  bool build_empty_graph = false;
  if (std::filesystem::exists(schema_file)) {
    loadSchema(schema_file);
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
    lf_indexers_.resize(vertex_label_num_);
    snapshot_dir = get_latest_snapshot(work_dir);
  } else {
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
    lf_indexers_.resize(vertex_label_num_);
    build_empty_graph = true;
    for (size_t i = 0; i < vertex_label_num_; ++i) {
      lf_indexers_[i].init(std::get<0>(schema_.get_vertex_primary_key(i)[0]));
    }
  }

  vertex_data_.resize(vertex_label_num_);
  std::string tmp_dir_path = tmp_dir(work_dir);

  if (std::filesystem::exists(tmp_dir_path)) {
    std::filesystem::remove_all(tmp_dir_path);
  }

  std::filesystem::create_directories(tmp_dir_path);

  std::vector<size_t> vertex_capacities(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    std::string v_label_name = schema_.get_vertex_label_name(i);

    if (memory_level == 0) {
      lf_indexers_[i].open(
          IndexerType::prefix() + "_" + vertex_map_prefix(v_label_name),
          snapshot_dir, tmp_dir_path);
      vertex_data_[i].open(vertex_table_prefix(v_label_name), snapshot_dir,
                           tmp_dir_path, schema_.get_vertex_property_names(i),
                           schema_.get_vertex_properties(i),
                           schema_.get_vertex_storage_strategies(v_label_name));
      if (!build_empty_graph) {
        vertex_data_[i].copy_to_tmp(vertex_table_prefix(v_label_name),
                                    snapshot_dir, tmp_dir_path);
      }
    } else if (memory_level == 1) {
      lf_indexers_[i].open_in_memory(snapshot_dir + "/" +
                                     IndexerType::prefix() + "_" +
                                     vertex_map_prefix(v_label_name));
      vertex_data_[i].open_in_memory(
          vertex_table_prefix(v_label_name), snapshot_dir,
          schema_.get_vertex_property_names(i),
          schema_.get_vertex_properties(i),
          schema_.get_vertex_storage_strategies(v_label_name));
    } else if (memory_level == 2) {
      lf_indexers_[i].open_with_hugepages(snapshot_dir + "/" +
                                              IndexerType::prefix() + "_" +
                                              vertex_map_prefix(v_label_name),
                                          false);
      vertex_data_[i].open_with_hugepages(
          vertex_table_prefix(v_label_name), snapshot_dir,
          schema_.get_vertex_property_names(i),
          schema_.get_vertex_properties(i),
          schema_.get_vertex_storage_strategies(v_label_name), false);
    } else {
      assert(memory_level == 3);
      lf_indexers_[i].open_with_hugepages(snapshot_dir + "/" +
                                              IndexerType::prefix() + "_" +
                                              vertex_map_prefix(v_label_name),
                                          true);
      vertex_data_[i].open_with_hugepages(
          vertex_table_prefix(v_label_name), snapshot_dir,
          schema_.get_vertex_property_names(i),
          schema_.get_vertex_properties(i),
          schema_.get_vertex_storage_strategies(v_label_name), true);
    }

    // We will reserve the at least 4096 slots for each vertex label
    size_t vertex_capacity =
        std::max(lf_indexers_[i].capacity(), (size_t) 4096);
    if (vertex_capacity > lf_indexers_[i].capacity()) {
      lf_indexers_[i].reserve(vertex_capacity);
    }
    vertex_data_[i].resize(vertex_capacity);
    vertex_capacities[i] = vertex_capacity;
  }

  ie_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  oe_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);

  dual_csr_list_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_,
                        NULL);

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
        bool oe_mutable =
            schema_.outgoing_edge_mutable(src_label, dst_label, edge_label);
        bool ie_mutable =
            schema_.incoming_edge_mutable(src_label, dst_label, edge_label);

        auto& prop_names =
            schema_.get_edge_property_names(src_label, dst_label, edge_label);

        dual_csr_list_[index] = create_csr(oe_strategy, ie_strategy, properties,
                                           oe_mutable, ie_mutable, prop_names);
        ie_[index] = dual_csr_list_[index]->GetInCsr();
        oe_[index] = dual_csr_list_[index]->GetOutCsr();
        if (memory_level == 0) {
          dual_csr_list_[index]->Open(
              oe_prefix(src_label, dst_label, edge_label),
              ie_prefix(src_label, dst_label, edge_label),
              edata_prefix(src_label, dst_label, edge_label), snapshot_dir,
              tmp_dir_path);
        } else if (memory_level >= 2) {
          dual_csr_list_[index]->OpenWithHugepages(
              oe_prefix(src_label, dst_label, edge_label),
              ie_prefix(src_label, dst_label, edge_label),
              edata_prefix(src_label, dst_label, edge_label), snapshot_dir,
              vertex_capacities[src_label_i], vertex_capacities[dst_label_i]);
        } else {
          dual_csr_list_[index]->OpenInMemory(
              oe_prefix(src_label, dst_label, edge_label),
              ie_prefix(src_label, dst_label, edge_label),
              edata_prefix(src_label, dst_label, edge_label), snapshot_dir,
              vertex_capacities[src_label_i], vertex_capacities[dst_label_i]);
        }
        dual_csr_list_[index]->Resize(vertex_capacities[src_label_i],
                                      vertex_capacities[dst_label_i]);
      }
    }
  }
}

void MutablePropertyFragment::Compact(uint32_t version) {
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
        if (dual_csr_list_[index] != NULL) {
          if (schema_.get_sort_on_compaction(src_label, dst_label,
                                             edge_label)) {
            dual_csr_list_[index]->SortByEdgeData(version);
          }
        }
      }
    }
  }
}

void MutablePropertyFragment::Dump(const std::string& work_dir,
                                   uint32_t version) {
  std::string snapshot_dir_path = snapshot_dir(work_dir, version);
  std::error_code errorCode;
  std::filesystem::create_directories(snapshot_dir_path, errorCode);
  if (errorCode) {
    std::stringstream ss;
    ss << "Failed to create snapshot directory: " << snapshot_dir_path << ", "
       << errorCode.message();
    LOG(ERROR) << ss.str();
    throw std::runtime_error(ss.str());
  }
  std::vector<size_t> vertex_num(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    vertex_num[i] = lf_indexers_[i].size();
    lf_indexers_[i].dump(
        IndexerType::prefix() + "_" +
            vertex_map_prefix(schema_.get_vertex_label_name(i)),
        snapshot_dir_path);
    vertex_data_[i].resize(vertex_num[i]);
    vertex_data_[i].dump(vertex_table_prefix(schema_.get_vertex_label_name(i)),
                         snapshot_dir_path);
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
        if (dual_csr_list_[index] != NULL) {
          dual_csr_list_[index]->Resize(vertex_num[src_label_i],
                                        vertex_num[dst_label_i]);
          if (schema_.get_sort_on_compaction(src_label, dst_label,
                                             edge_label)) {
            dual_csr_list_[index]->SortByEdgeData(version + 1);
          }
          dual_csr_list_[index]->Dump(
              oe_prefix(src_label, dst_label, edge_label),
              ie_prefix(src_label, dst_label, edge_label),
              edata_prefix(src_label, dst_label, edge_label),
              snapshot_dir_path);
        }
      }
    }
  }
  set_snapshot_version(work_dir, version);
}

void MutablePropertyFragment::Warmup(int thread_num) {
  double t = -grape::GetCurrentTime();
  for (auto ptr : dual_csr_list_) {
    if (ptr != NULL) {
      ptr->Warmup(thread_num);
    }
  }
  for (auto& indexer : lf_indexers_) {
    indexer.warmup(thread_num);
  }
  t += grape::GetCurrentTime();
  LOG(INFO) << "Warmup takes: " << t << " s";
}
void MutablePropertyFragment::IngestEdge(label_t src_label, vid_t src_lid,
                                         label_t dst_label, vid_t dst_lid,
                                         label_t edge_label, timestamp_t ts,
                                         grape::OutArchive& arc,
                                         Allocator& alloc) {
  size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                 dst_label * edge_label_num_ + edge_label;
  dual_csr_list_[index]->IngestEdge(src_lid, dst_lid, arc, ts, alloc);
}

void MutablePropertyFragment::UpdateEdge(label_t src_label, vid_t src_lid,
                                         label_t dst_label, vid_t dst_lid,
                                         label_t edge_label, timestamp_t ts,
                                         const Any& arc, Allocator& alloc) {
  size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                 dst_label * edge_label_num_ + edge_label;
  dual_csr_list_[index]->UpdateEdge(src_lid, dst_lid, arc, ts, alloc);
}
const Schema& MutablePropertyFragment::schema() const { return schema_; }

Schema& MutablePropertyFragment::mutable_schema() { return schema_; }

vid_t MutablePropertyFragment::vertex_num(label_t vertex_label) const {
  return static_cast<vid_t>(lf_indexers_[vertex_label].size());
}

size_t MutablePropertyFragment::edge_num(label_t src_label, label_t edge_label,
                                         label_t dst_label) const {
  size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                 dst_label * edge_label_num_ + edge_label;
  if (dual_csr_list_[index] != NULL) {
    return dual_csr_list_[index]->EdgeNum();
  } else {
    return 0;
  }
}

bool MutablePropertyFragment::get_lid(label_t label, const Any& oid,
                                      vid_t& lid) const {
  return lf_indexers_[label].get_index(oid, lid);
}

Any MutablePropertyFragment::get_oid(label_t label, vid_t lid) const {
  return lf_indexers_[label].get_key(lid);
}

vid_t MutablePropertyFragment::add_vertex(label_t label, const Any& id) {
  return lf_indexers_[label].insert(id);
}

std::shared_ptr<CsrConstEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  return get_oe_csr(label, neighbor_label, edge_label)->edge_iter(u);
}

std::shared_ptr<CsrConstEdgeIterBase>
MutablePropertyFragment::get_incoming_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  return get_ie_csr(label, neighbor_label, edge_label)->edge_iter(u);
}

CsrConstEdgeIterBase* MutablePropertyFragment::get_outgoing_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  return get_oe_csr(label, neighbor_label, edge_label)->edge_iter_raw(u);
}

CsrConstEdgeIterBase* MutablePropertyFragment::get_incoming_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  return get_ie_csr(label, neighbor_label, edge_label)->edge_iter_raw(u);
}

std::shared_ptr<CsrEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  return get_oe_csr(label, neighbor_label, edge_label)->edge_iter_mut(u);
}

std::shared_ptr<CsrEdgeIterBase>
MutablePropertyFragment::get_incoming_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  return get_ie_csr(label, neighbor_label, edge_label)->edge_iter_mut(u);
}

void MutablePropertyFragment::generateStatistics(
    const std::string& work_dir) const {
  std::string filename = work_dir + "/statistics.json";
  size_t vertex_count = 0;

  std::string ss = "\"vertex_type_statistics\": [\n";
  size_t vertex_label_num = schema_.vertex_label_num();
  for (size_t idx = 0; idx < vertex_label_num; ++idx) {
    ss += "{\n\"type_id\": " + std::to_string(idx) + ", \n";
    ss += "\"type_name\": \"" + schema_.get_vertex_label_name(idx) + "\", \n";
    size_t count = lf_indexers_[idx].size();
    ss += "\"count\": " + std::to_string(count) + "\n}";
    vertex_count += count;
    if (idx != vertex_label_num - 1) {
      ss += ", \n";
    } else {
      ss += "\n";
    }
  }
  ss += "]\n";
  size_t edge_count = 0;

  size_t edge_label_num = schema_.edge_label_num();
  std::vector<std::thread> count_threads;
  std::vector<size_t> edge_count_list(dual_csr_list_.size(), 0);
  for (size_t src_label = 0; src_label < vertex_label_num; ++src_label) {
    const auto& src_label_name = schema_.get_vertex_label_name(src_label);
    for (size_t dst_label = 0; dst_label < vertex_label_num; ++dst_label) {
      const auto& dst_label_name = schema_.get_vertex_label_name(dst_label);
      for (size_t edge_label = 0; edge_label < edge_label_num; ++edge_label) {
        const auto& edge_label_name = schema_.get_edge_label_name(edge_label);
        if (schema_.exist(src_label_name, dst_label_name, edge_label_name)) {
          size_t index = src_label * vertex_label_num * edge_label_num +
                         dst_label * edge_label_num + edge_label;
          if (dual_csr_list_[index] != NULL) {
            count_threads.emplace_back([&edge_count_list, index, this] {
              edge_count_list[index] = dual_csr_list_[index]->EdgeNum();
            });
          }
        }
      }
    }
  }
  for (auto& t : count_threads) {
    t.join();
  }
  ss += ",\n";
  ss += "\"edge_type_statistics\": [";

  for (size_t edge_label = 0; edge_label < edge_label_num; ++edge_label) {
    const auto& edge_label_name = schema_.get_edge_label_name(edge_label);

    ss += "{\n\"type_id\": " + std::to_string(edge_label) + ", \n";
    ss += "\"type_name\": \"" + edge_label_name + "\", \n";
    ss += "\"vertex_type_pair_statistics\": [\n";
    bool first = true;
    std::string props_content{};
    for (size_t src_label = 0; src_label < vertex_label_num; ++src_label) {
      const auto& src_label_name = schema_.get_vertex_label_name(src_label);
      for (size_t dst_label = 0; dst_label < vertex_label_num; ++dst_label) {
        const auto& dst_label_name = schema_.get_vertex_label_name(dst_label);
        size_t index = src_label * vertex_label_num * edge_label_num +
                       dst_label * edge_label_num + edge_label;
        if (schema_.exist(src_label_name, dst_label_name, edge_label_name)) {
          if (!first) {
            ss += ",\n";
          }
          first = false;
          ss += "{\n\"source_vertex\" : \"" + src_label_name + "\", \n";
          ss += "\"destination_vertex\" : \"" + dst_label_name + "\", \n";
          ss += "\"count\" : " + std::to_string(edge_count_list[index]) + "\n";
          edge_count += edge_count_list[index];
          ss += "}";
        }
      }
    }

    ss += "\n]\n}";
    if (edge_label != edge_label_num - 1) {
      ss += ", \n";
    } else {
      ss += "\n";
    }
  }
  ss += "]\n";
  {
    std::ofstream out(filename);
    out << "{\n\"total_vertex_count\": " << vertex_count << ",\n";
    out << "\"total_edge_count\": " << edge_count << ",\n";
    out << ss;
    out << "}\n";
    out.close();
  }
}

}  // namespace gs
