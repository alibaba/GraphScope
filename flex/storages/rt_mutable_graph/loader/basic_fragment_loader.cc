
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

#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/file_names.h"

namespace gs {

std::ostream& operator<<(std::ostream& os, const LoadingStatus& status) {
  if (status == LoadingStatus::kLoading) {
    os << "Loading";
  } else if (status == LoadingStatus::kLoaded) {
    os << "Loaded";
  } else if (status == LoadingStatus::kCommited) {
    os << "Commited";
  } else {
    os << "Unknown";
  }
  return os;
}

std::istream& operator>>(std::istream& is, LoadingStatus& status) {
  std::string tmp;
  is >> tmp;
  if (tmp == "Loading") {
    status = LoadingStatus::kLoading;
  } else if (tmp == "Loaded") {
    status = LoadingStatus::kLoaded;
  } else if (tmp == "Commited") {
    status = LoadingStatus::kCommited;
  } else {
    status = LoadingStatus::kUnknown;
  }
  return is;
}

BasicFragmentLoader::BasicFragmentLoader(const Schema& schema,
                                         const std::string& prefix)
    : schema_(schema),
      work_dir_(prefix),
      vertex_label_num_(schema_.vertex_label_num()),
      edge_label_num_(schema_.edge_label_num()) {
  vertex_data_.resize(vertex_label_num_);
  ie_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  oe_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  dual_csr_list_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_,
                        NULL);
  lf_indexers_.resize(vertex_label_num_);
  std::filesystem::create_directories(runtime_dir(prefix));
  std::filesystem::create_directories(snapshot_dir(prefix, 0));
  std::filesystem::create_directories(wal_dir(prefix));
  std::filesystem::create_directories(tmp_dir(prefix));

  init_vertex_data();
  // initially create all status files for vertices and edges.
  init_loading_status_file();
}

void BasicFragmentLoader::append_vertex_loading_progress(
    const std::string& label_name, LoadingStatus status) {
  auto status_file_path = bulk_load_progress_file(work_dir_);
  std::lock_guard<std::mutex> lock(loading_progress_mutex_);
  std::ofstream status_file(status_file_path, std::ios::app);
  std::stringstream ss;
  ss << "[VertexLabel]:" << label_name << ", [Status]:" << status << "\n";
  status_file << ss.str();
  status_file.close();
  return;
}

void BasicFragmentLoader::append_edge_loading_progress(
    const std::string& src_label_name, const std::string& dst_label_name,
    const std::string& edge_label_name, LoadingStatus status) {
  auto status_file_path = bulk_load_progress_file(work_dir_);
  std::lock_guard<std::mutex> lock(loading_progress_mutex_);
  std::ofstream status_file(status_file_path, std::ios::app);
  std::stringstream ss;
  ss << "[SrcVertexLabel]:" << src_label_name
     << " -> [DstVertexLabel]:" << dst_label_name << " : [EdgeLabel]"
     << edge_label_name << ", [Status]:" << status << "\n";
  status_file << ss.str();
  status_file.close();
  return;
}

void BasicFragmentLoader::init_loading_status_file() {
  for (label_t v_label = 0; v_label < vertex_label_num_; v_label++) {
    auto label_name = schema_.get_vertex_label_name(v_label);
    append_vertex_loading_progress(label_name, LoadingStatus::kLoading);
  }
  VLOG(1) << "Finish init vertex status files";
  for (size_t src_label = 0; src_label < vertex_label_num_; src_label++) {
    std::string src_label_name = schema_.get_vertex_label_name(src_label);
    for (size_t dst_label = 0; dst_label < vertex_label_num_; dst_label++) {
      std::string dst_label_name = schema_.get_vertex_label_name(dst_label);
      for (size_t edge_label = 0; edge_label < edge_label_num_; edge_label++) {
        std::string edge_label_name = schema_.get_edge_label_name(edge_label);
        if (schema_.exist(src_label_name, dst_label_name, edge_label_name)) {
          append_edge_loading_progress(src_label_name, dst_label_name,
                                       edge_label_name,
                                       LoadingStatus::kLoading);
        }
      }
    }
  }
}

void BasicFragmentLoader::init_vertex_data() {
  for (label_t v_label = 0; v_label < vertex_label_num_; v_label++) {
    auto& v_data = vertex_data_[v_label];
    auto label_name = schema_.get_vertex_label_name(v_label);
    auto& property_types = schema_.get_vertex_properties(v_label);
    auto& property_names = schema_.get_vertex_property_names(v_label);
    v_data.init(vertex_table_prefix(label_name), tmp_dir(work_dir_),
                property_names, property_types,
                schema_.get_vertex_storage_strategies(label_name));
    v_data.resize(schema_.get_max_vnum(label_name));
  }
  VLOG(10) << "Finish init vertex data";
}

void BasicFragmentLoader::LoadFragment() {
  std::string schema_filename = schema_path(work_dir_);
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_filename));
  io_adaptor->Open("wb");
  schema_.Serialize(io_adaptor);
  io_adaptor->Close();

  set_snapshot_version(work_dir_, 0);
  clear_tmp(work_dir_);
}

void BasicFragmentLoader::AddVertexBatch(
    label_t v_label, const std::vector<vid_t>& vids,
    const std::vector<std::vector<Any>>& props) {
  auto& table = vertex_data_[v_label];
  CHECK(props.size() == table.col_num());
  for (size_t i = 0; i < props.size(); ++i) {
    CHECK(props[i].size() == vids.size())
        << "vids size: " << vids.size() << ", props size: " << props.size()
        << ", props[i] size: " << props[i].size();
  }
  auto dst_columns = table.column_ptrs();
  for (size_t j = 0; j < props.size(); ++j) {
    auto& cur_vec = props[j];
    for (size_t i = 0; i < vids.size(); ++i) {
      auto index = vids[i];
      dst_columns[j]->set_any(index, cur_vec[i]);
    }
  }
}

const IndexerType& BasicFragmentLoader::GetLFIndexer(label_t v_label) const {
  CHECK(v_label < vertex_label_num_);
  return lf_indexers_[v_label];
}

IndexerType& BasicFragmentLoader::GetLFIndexer(label_t v_label) {
  CHECK(v_label < vertex_label_num_);
  return lf_indexers_[v_label];
}

void BasicFragmentLoader::set_csr(label_t src_label_id, label_t dst_label_id,
                                  label_t edge_label_id,
                                  DualCsrBase* dual_csr) {
  size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                 dst_label_id * edge_label_num_ + edge_label_id;
  dual_csr_list_[index] = dual_csr;
  ie_[index] = dual_csr->GetInCsr();
  oe_[index] = dual_csr->GetOutCsr();
}

DualCsrBase* BasicFragmentLoader::get_csr(label_t src_label_id,
                                          label_t dst_label_id,
                                          label_t edge_label_id) {
  size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                 dst_label_id * edge_label_num_ + edge_label_id;
  return dual_csr_list_[index];
}

void BasicFragmentLoader::init_edge_table(label_t src_label_id,
                                          label_t dst_label_id,
                                          label_t edge_label_id) {
  size_t index = src_label_id * vertex_label_num_ * edge_label_num_ +
                 dst_label_id * edge_label_num_ + edge_label_id;
  auto cast_dual_csr =
      dynamic_cast<DualCsr<RecordView>*>(dual_csr_list_[index]);
  CHECK(cast_dual_csr != nullptr);
  auto src_label_name = schema_.get_vertex_label_name(src_label_id);
  auto dst_label_name = schema_.get_vertex_label_name(dst_label_id);
  auto edge_label_name = schema_.get_edge_label_name(edge_label_id);
  cast_dual_csr->InitTable(
      edata_prefix(src_label_name, dst_label_name, edge_label_name),
      tmp_dir(work_dir_));
}

}  // namespace gs
