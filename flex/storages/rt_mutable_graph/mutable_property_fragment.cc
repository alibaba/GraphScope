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
  for (auto ptr : ie_) {
    if (ptr != NULL) {
      delete ptr;
    }
  }
  for (auto ptr : oe_) {
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

template <typename EDATA_T>
class EmptyCsr : public TypedMutableCsrBase<EDATA_T> {
  using slice_t = MutableNbrSlice<EDATA_T>;

 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {}

  slice_t get_edges(vid_t i) const override { return slice_t::empty(); }

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        ArenaAllocator& alloc) override {}

  void Serialize(const std::string& path) override {}

  void Deserialize(const std::string& path) override {}

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    EDATA_T value;
    arc >> value;
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        const timestamp_t ts, ArenaAllocator& alloc) override {}

  std::shared_ptr<MutableCsrConstEdgeIterBase> edge_iter(
      vid_t v) const override {
    return std::make_shared<TypedMutableCsrConstEdgeIter<EDATA_T>>(
        MutableNbrSlice<EDATA_T>::empty());
  }

  std::shared_ptr<MutableCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedMutableCsrEdgeIter<EDATA_T>>(
        MutableNbrSliceMut<EDATA_T>::empty());
  }
};

template <typename EDATA_T>
TypedMutableCsrBase<EDATA_T>* create_typed_csr(EdgeStrategy es) {
  if (es == EdgeStrategy::kSingle) {
    return new SingleMutableCsr<EDATA_T>();
  } else if (es == EdgeStrategy::kMultiple) {
    return new MutableCsr<EDATA_T>();
  } else if (es == EdgeStrategy::kNone) {
    return new EmptyCsr<EDATA_T>();
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
}

template <typename EDATA_T>
std::pair<MutableCsrBase*, MutableCsrBase*> construct_empty_csr(
    EdgeStrategy ie_strategy, EdgeStrategy oe_strategy) {
  TypedMutableCsrBase<EDATA_T>* ie_csr = create_typed_csr<EDATA_T>(ie_strategy);
  TypedMutableCsrBase<EDATA_T>* oe_csr = create_typed_csr<EDATA_T>(oe_strategy);
  ie_csr->batch_init(0, {});
  oe_csr->batch_init(0, {});
  return std::make_pair(ie_csr, oe_csr);
}

void preprocess_line(char* line) {
  size_t len = strlen(line);
  while (len >= 0) {
    if (line[len] != '\0' && line[len] != '\n' && line[len] != '\r' &&
        line[len] != ' ' && line[len] != '\t') {
      break;
    } else {
      --len;
    }
  }
  line[len + 1] = '\0';
}

template <typename EDATA_T>
std::pair<MutableCsrBase*, MutableCsrBase*> construct_csr(
    const std::vector<std::string>& filenames,
    const std::vector<PropertyType>& property_types, EdgeStrategy ie_strategy,
    EdgeStrategy oe_strategy, const LFIndexer<vid_t>& src_indexer,
    const LFIndexer<vid_t>& dst_indexer) {
  TypedMutableCsrBase<EDATA_T>* ie_csr = create_typed_csr<EDATA_T>(ie_strategy);
  TypedMutableCsrBase<EDATA_T>* oe_csr = create_typed_csr<EDATA_T>(oe_strategy);

  std::vector<int> odegree(src_indexer.size(), 0);
  std::vector<int> idegree(dst_indexer.size(), 0);

  std::vector<std::tuple<vid_t, vid_t, EDATA_T>> parsed_edges;
  vid_t src_index, dst_index;
  char line_buf[4096];
  oid_t src, dst;
  EDATA_T data;

  bool first_file = true;
  size_t col_num = property_types.size();
  std::vector<Any> header(col_num + 2);
  for (auto& item : header) {
    item.type = PropertyType::kString;
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
        col_names[i] = std::string(header[i + 2].value.s.data(),
                                   header[i + 2].value.s.size());
      }
      first_file = false;
    }

    while (fgets(line_buf, 4096, fin) != NULL) {
      ParseRecordX(line_buf, src, dst, data);
      src_index = src_indexer.get_index(src);
      dst_index = dst_indexer.get_index(dst);
      ++idegree[dst_index];
      ++odegree[src_index];
      parsed_edges.emplace_back(src_index, dst_index, data);
    }
    fclose(fin);
  }

  ie_csr->batch_init(dst_indexer.size(), idegree);
  oe_csr->batch_init(src_indexer.size(), odegree);

  for (auto& edge : parsed_edges) {
    ie_csr->batch_put_edge(std::get<1>(edge), std::get<0>(edge),
                           std::get<2>(edge));
    oe_csr->batch_put_edge(std::get<0>(edge), std::get<1>(edge),
                           std::get<2>(edge));
  }

  return std::make_pair(ie_csr, oe_csr);
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

  if (col_num == 0) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<grape::EmptyType>(ie_strtagy, oe_strtagy);
    } else {
      std::tie(ie_[index], oe_[index]) = construct_csr<grape::EmptyType>(
          filenames, property_types, ie_strtagy, oe_strtagy,
          lf_indexers_[src_label_i], lf_indexers_[dst_label_i]);
    }
  } else if (property_types[0] == PropertyType::kDate) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<Date>(ie_strtagy, oe_strtagy);
    } else {
      std::tie(ie_[index], oe_[index]) = construct_csr<Date>(
          filenames, property_types, ie_strtagy, oe_strtagy,
          lf_indexers_[src_label_i], lf_indexers_[dst_label_i]);
    }
  } else if (property_types[0] == PropertyType::kInt32) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<int>(ie_strtagy, oe_strtagy);
    } else {
      std::tie(ie_[index], oe_[index]) = construct_csr<int>(
          filenames, property_types, ie_strtagy, oe_strtagy,
          lf_indexers_[src_label_i], lf_indexers_[dst_label_i]);
    }
  } else if (property_types[0] == PropertyType::kInt64) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<int64_t>(ie_strtagy, oe_strtagy);
    } else {
      LOG(FATAL) << "Unsupported edge property type.";
    }
  } else if (property_types[0] == PropertyType::kString) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<std::string>(ie_strtagy, oe_strtagy);
    } else {
      LOG(FATAL) << "Unsupported edge property type.";
    }
  } else {
    LOG(FATAL) << "Unsupported edge property type.";
  }
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
  ie_[index]->peek_ingest_edge(dst_lid, src_lid, arc, ts, alloc);
  oe_[index]->ingest_edge(src_lid, dst_lid, arc, ts, alloc);
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
        ie_[index]->Serialize(data_dir + "/ie_" + src_label + "_" + dst_label +
                              "_" + edge_label);
        oe_[index]->Serialize(data_dir + "/oe_" + src_label + "_" + dst_label +
                              "_" + edge_label);
      }
    }
  }

  io_adaptor->Close();
}

inline MutableCsrBase* create_csr(EdgeStrategy es,
                                  const std::vector<PropertyType>& properties) {
  if (properties.empty()) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<grape::EmptyType>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<grape::EmptyType>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<grape::EmptyType>();
    }
  } else if (properties[0] == PropertyType::kInt32) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<int>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<int>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<int>();
    }
  } else if (properties[0] == PropertyType::kDate) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<Date>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<Date>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<Date>();
    }
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
  return nullptr;
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
        ie_[index] = create_csr(ie_strategy, properties);
        oe_[index] = create_csr(oe_strategy, properties);
        ie_[index]->Deserialize(data_dir + "/ie_" + src_label + "_" +
                                dst_label + "_" + edge_label);
        oe_[index]->Deserialize(data_dir + "/oe_" + src_label + "_" +
                                dst_label + "_" + edge_label);
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

std::shared_ptr<MutableCsrConstEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter(u);
}

std::shared_ptr<MutableCsrConstEdgeIterBase>
MutablePropertyFragment::get_incoming_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter(u);
}

std::shared_ptr<MutableCsrEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter_mut(u);
}

std::shared_ptr<MutableCsrEdgeIterBase>
MutablePropertyFragment::get_incoming_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter_mut(u);
}

MutableCsrBase* MutablePropertyFragment::get_oe_csr(label_t label,
                                                    label_t neighbor_label,
                                                    label_t edge_label) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

const MutableCsrBase* MutablePropertyFragment::get_oe_csr(
    label_t label, label_t neighbor_label, label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

MutableCsrBase* MutablePropertyFragment::get_ie_csr(label_t label,
                                                    label_t neighbor_label,
                                                    label_t edge_label) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index];
}

const MutableCsrBase* MutablePropertyFragment::get_ie_csr(
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
  std::vector<Any> properties(col_num);
  for (size_t col_i = 0; col_i != col_num; ++col_i) {
    properties[col_i].type = property_types[col_i];
  }

  char line_buf[4096];
  oid_t oid;
  vid_t v_index;
  bool first_file = true;
  std::vector<Any> header(col_num + 1);
  for (auto& item : header) {
    item.type = PropertyType::kString;
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
        col_names[i] = std::string(header[i + 1].value.s.data(),
                                   header[i + 1].value.s.size());
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
