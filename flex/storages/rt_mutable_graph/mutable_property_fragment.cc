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

void get_header_row(const std::string& file_name, std::vector<Any>& header) {
  char line_buf[4096];
  FILE* fin = fopen(file_name.c_str(), "r");
  if (fgets(line_buf, 4096, fin) == NULL) {
    LOG(FATAL) << "Failed to read header from file: " << file_name;
    return;
  }
  preprocess_line(line_buf);
  ParseRecord(line_buf, header);
}

std::vector<std::pair<size_t, std::string>> generate_default_column_mapping(
    const std::string& file_name, std::string primary_key_name,
    const std::vector<std::string>& column_names) {
  std::vector<std::pair<size_t, std::string>> column_mapping;
  for (size_t i = 0; i < column_names.size(); ++i) {
    auto col_name = column_names[i];
    if (col_name != primary_key_name) {
      column_mapping.emplace_back(i, col_name);
    }
  }
  return column_mapping;
}

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

// vertex_column_mappings is a vector of pairs, each pair is (column_ind in
// file, the cooresponding property name in schema).
void MutablePropertyFragment::initVertices(
    label_t v_label_i, const std::vector<std::string>& filenames,
    const std::vector<std::pair<size_t, std::string>>& vertex_column_mappings) {
  // Check primary key num and type.
  auto primary_keys = schema_.get_vertex_primary_key(v_label_i);
  if (primary_keys.size() != 1) {
    LOG(FATAL) << "Only support one primary key for vertex.";
  }
  if (primary_keys[0].first != PropertyType::kInt64) {
    LOG(FATAL) << "Only support int64_t primary key for vertex.";
  }
  IdIndexer<oid_t, vid_t> indexer;
  std::string v_label_name = schema_.get_vertex_label_name(v_label_i);
  VLOG(10) << "Start init vertices for label " << v_label_i << " with "
           << filenames.size() << " files.";
  auto& table = vertex_data_[v_label_i];
  auto& property_types = schema_.get_vertex_properties(v_label_name);
  auto& property_names = schema_.get_vertex_property_names(v_label_name);

  // col num should be property num - 1, because one column will be used as
  // primary key
  CHECK(property_types.size() > 0);
  size_t col_num = property_types.size();

  // create real property_types_vec for table
  VLOG(10) << "Init table for table: " << v_label_name
           << ", with property num: " << col_num;
  table.init(property_names, property_types,
             schema_.get_vertex_storage_strategies(v_label_name),
             schema_.get_max_vnum(v_label_name));
  // Match the records read from the file with the schema
  parseVertexFiles(v_label_name, filenames, vertex_column_mappings, indexer);
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
  MutableCsrConstEdgeIterBase* edge_iter_raw(vid_t v) const override {
    return new TypedMutableCsrConstEdgeIter<EDATA_T>(
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

// each file name is tuple <src_column_ind, dst_column_id, edata(currently only
// one edata)> src_column_id indicate which column is src id, dst_column_id
// indicate which column is dst id default is 0, 1
template <typename EDATA_T>
std::pair<MutableCsrBase*, MutableCsrBase*> construct_csr(
    const Schema& schema, const std::vector<std::string>& filenames,
    size_t src_col_ind, size_t dst_col_ind,
    const std::vector<PropertyType>& property_types,
    const std::vector<std::pair<size_t, std::string>>& column_mappings,
    EdgeStrategy ie_strategy, EdgeStrategy oe_strategy,
    const LFIndexer<vid_t>& src_indexer, const LFIndexer<vid_t>& dst_indexer) {
  TypedMutableCsrBase<EDATA_T>* ie_csr = create_typed_csr<EDATA_T>(ie_strategy);
  TypedMutableCsrBase<EDATA_T>* oe_csr = create_typed_csr<EDATA_T>(oe_strategy);

  std::vector<int> odegree(src_indexer.size(), 0);
  std::vector<int> idegree(dst_indexer.size(), 0);

  std::vector<std::tuple<vid_t, vid_t, EDATA_T>> parsed_edges;
  vid_t src_index, dst_index;
  char line_buf[4096];
  oid_t src, dst;
  EDATA_T data;

  size_t col_num = property_types.size();
  std::vector<Any> header(col_num + 2);
  for (auto& item : header) {
    item.type = PropertyType::kString;
  }

  // fetch header first
  get_header_row(filenames[0], header);  // filenames must not be empty
  // check header matches schema

  for (auto filename : filenames) {
    VLOG(10) << "processing " << filename << " with src_col_id " << src_col_ind
             << " and dst_col_id " << dst_col_ind;
    FILE* fin = fopen(filename.c_str(), "r");
    if (fgets(line_buf, 4096, fin) == NULL) {
      continue;
    }
    preprocess_line(line_buf);  // do nothing

    // if match the default configuration, use ParseRecordX to fasten the
    // parsing
    if (src_col_ind == 0 && dst_col_ind == 1) {
      while (fgets(line_buf, 4096, fin) != NULL) {
        // ParseRecord src_id, dst_id, data from row.
        ParseRecordX(line_buf, src, dst, data);
        src_index = src_indexer.get_index(src);
        dst_index = dst_indexer.get_index(dst);
        ++idegree[dst_index];
        ++odegree[src_index];
        parsed_edges.emplace_back(src_index, dst_index, data);
      }
    } else {
      std::vector<Any> row(col_num + 2);
      CHECK(src_col_ind < row.size() && dst_col_ind < row.size());
      row[src_col_ind].type = PropertyType::kInt64;
      row[dst_col_ind].type = PropertyType::kInt64;
      int32_t data_col_id = -1;
      // the left column is must the edata.
      for (auto i = 0; i < row.size(); ++i) {
        if (row[i].type == PropertyType::kEmpty) {
          // The index 0's type must exists
          row[i].type = property_types[0];
          data_col_id = i;
          break;
        }
      }
      CHECK(data_col_id != -1);
      while (fgets(line_buf, 4096, fin) != NULL) {
        // ParseRecord src_id, dst_id, data from row.
        ParseRecord(line_buf, row);
        src_index = src_indexer.get_index(row[src_col_ind].AsInt64());
        dst_index = dst_indexer.get_index(row[dst_col_ind].AsInt64());
        ConvertAny<EDATA_T>::to(row[data_col_id], data);
        ++idegree[dst_index];
        ++odegree[src_index];
        parsed_edges.emplace_back(src_index, dst_index, data);
      }
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
    const std::vector<std::string>& filenames,
    const std::vector<std::pair<size_t, std::string>>& column_mappings,
    size_t src_col_ind, size_t dst_col_ind) {
  auto src_label_name = schema_.get_vertex_label_name(src_label_i);
  auto dst_label_name = schema_.get_vertex_label_name(dst_label_i);
  auto edge_label_name = schema_.get_edge_label_name(edge_label_i);
  if (filenames.size() <= 0) {
    LOG(FATAL) << "No edge files found for src label: " << src_label_name
               << " dst label: " << dst_label_name
               << " edge label: " << edge_label_name;
  }
  VLOG(10) << "Init edges src label: " << src_label_name
           << " dst label: " << dst_label_name
           << " edge label: " << edge_label_name
           << " filenames: " << filenames.size();
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

  {
    // check column mappings consistent,
    // TODO(zhanglei): Check column mappings after multiple property on edge is
    // supported
    if (column_mappings.size() > 1) {
      LOG(FATAL) << "Edge column mapping must be less than 1";
    }
    if (column_mappings.size() > 0) {
      auto& mapping = column_mappings[0];
      if (mapping.first == src_col_ind || mapping.first == dst_col_ind) {
        LOG(FATAL) << "Edge column mappings must not contain src_col_ind or "
                      "dst_col_ind";
      }
      // check property exists in schema
      if (!schema_.edge_has_property(src_label_name, dst_label_name,
                                     edge_label_name, mapping.second)) {
        LOG(FATAL) << "property " << mapping.second
                   << " not exists in schema for edge triplet "
                   << src_label_name << " -> " << edge_label_name << " -> "
                   << dst_label_name;
      }
    }
  }

  if (col_num == 0) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<grape::EmptyType>(ie_strtagy, oe_strtagy);
    } else {
      std::tie(ie_[index], oe_[index]) = construct_csr<grape::EmptyType>(
          schema_, filenames, src_col_ind, dst_col_ind, property_types,
          column_mappings, ie_strtagy, oe_strtagy, lf_indexers_[src_label_i],
          lf_indexers_[dst_label_i]);
    }
  } else if (property_types[0] == PropertyType::kDate) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<Date>(ie_strtagy, oe_strtagy);
    } else {
      std::tie(ie_[index], oe_[index]) = construct_csr<Date>(
          schema_, filenames, src_col_ind, dst_col_ind, property_types,
          column_mappings, ie_strtagy, oe_strtagy, lf_indexers_[src_label_i],
          lf_indexers_[dst_label_i]);
    }
  } else if (property_types[0] == PropertyType::kInt32) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<int>(ie_strtagy, oe_strtagy);
    } else {
      std::tie(ie_[index], oe_[index]) = construct_csr<int>(
          schema_, filenames, src_col_ind, dst_col_ind, property_types,
          column_mappings, ie_strtagy, oe_strtagy, lf_indexers_[src_label_i],
          lf_indexers_[dst_label_i]);
    }
  } else if (property_types[0] == PropertyType::kInt64) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<int64_t>(ie_strtagy, oe_strtagy);
    } else {
      std::tie(ie_[index], oe_[index]) = construct_csr<int64_t>(
          schema_, filenames, src_col_ind, dst_col_ind, property_types,
          column_mappings, ie_strtagy, oe_strtagy, lf_indexers_[src_label_i],
          lf_indexers_[dst_label_i]);
    }
  } else if (property_types[0] == PropertyType::kString) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<std::string>(ie_strtagy, oe_strtagy);
    } else {
      LOG(FATAL) << "Unsupported edge property type.";
    }
  } else if (property_types[0] == PropertyType::kDouble) {
    if (filenames.empty()) {
      std::tie(ie_[index], oe_[index]) =
          construct_empty_csr<double>(ie_strtagy, oe_strtagy);
    } else {
      std::tie(ie_[index], oe_[index]) = construct_csr<double>(
          schema_, filenames, src_col_ind, dst_col_ind, property_types,
          column_mappings, ie_strtagy, oe_strtagy, lf_indexers_[src_label_i],
          lf_indexers_[dst_label_i]);
    }
  } else {
    LOG(FATAL) << "Unsupported edge property type.";
  }
}

void MutablePropertyFragment::Init(const Schema& schema,
                                   const LoadingConfig& loading_config,
                                   int thread_num) {
  schema_ = schema;
  vertex_label_num_ = schema_.vertex_label_num();
  edge_label_num_ = schema_.edge_label_num();
  vertex_data_.resize(vertex_label_num_);
  ie_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  oe_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  lf_indexers_.resize(vertex_label_num_);

  auto& vertex_sources = loading_config.GetVertexLoadingMeta();
  auto& edge_sources = loading_config.GetEdgeLoadingMeta();

  if (thread_num == 1) {
    if (vertex_sources.empty()) {
      LOG(INFO) << "Skip loading vertices since no vertex source is specified.";
    } else {
      for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
           ++iter) {
        auto v_label_id = iter->first;
        auto v_files = iter->second;
        initVertices(v_label_id, v_files,
                     loading_config.GetVertexColumnMappings(v_label_id));
      }
    }

    if (edge_sources.empty()) {
      LOG(INFO) << "Skip loading edges since no edge source is specified.";
    } else {
      LOG(INFO) << "Loading edges...";
      for (auto iter = edge_sources.begin(); iter != edge_sources.end();
           ++iter) {
        // initEdges(iter->first, iter->second);
        auto& src_label_id = std::get<0>(iter->first);
        auto& dst_label_id = std::get<1>(iter->first);
        auto& e_label_id = std::get<2>(iter->first);
        auto& e_files = iter->second;
        auto src_dst_col_pair = loading_config.GetEdgeSrcDstCol(
            src_label_id, dst_label_id, e_label_id);
        // We currenly only support one src primary key and one dst primary key
        if (src_dst_col_pair.first.size() != 1 ||
            src_dst_col_pair.second.size() != 1) {
          LOG(FATAL) << "We currenly only support one src primary key and one "
                        "dst primary key";
        }
        initEdges(src_label_id, dst_label_id, e_label_id, e_files,
                  loading_config.GetEdgeColumnMappings(
                      src_label_id, dst_label_id, e_label_id),
                  src_dst_col_pair.first[0], src_dst_col_pair.second[0]);
      }
    }

  } else {
    // copy vertex_sources and edge sources to vector, since we need to
    // use multi-thread loading.
    std::vector<std::pair<label_t, std::vector<std::string>>> vertex_files;
    for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
         ++iter) {
      vertex_files.emplace_back(iter->first, iter->second);
    }
    std::vector<std::pair<typename LoadingConfig::edge_triplet_type,
                          std::vector<std::string>>>
        edge_files;
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      edge_files.emplace_back(iter->first, iter->second);
    }
    LOG(INFO) << "Parallel loading with " << thread_num << " threads, "
              << " " << vertex_files.size() << " vertex files, "
              << edge_files.size() << " edge files.";
    {
      if (vertex_sources.empty()) {
        LOG(INFO)
            << "Skip loading vertices since no vertex source is specified.";
      } else {
        std::atomic<size_t> v_ind(0);
        std::vector<std::thread> threads(thread_num);
        for (int i = 0; i < thread_num; ++i) {
          threads[i] = std::thread([&]() {
            while (true) {
              size_t cur = v_ind.fetch_add(1);
              if (cur >= vertex_files.size()) {
                break;
              }
              auto v_label_id = vertex_files[cur].first;
              initVertices(v_label_id, vertex_files[cur].second,
                           loading_config.GetVertexColumnMappings(v_label_id));
            }
          });
        }
        for (auto& thrd : threads) {
          thrd.join();
        }

        LOG(INFO) << "finished loading vertices";
      }
    }
    {
      if (edge_sources.empty()) {
        LOG(INFO) << "Skip loading edges since no edge source is specified.";
      } else {
        std::atomic<size_t> e_ind(0);
        std::vector<std::thread> threads(thread_num);
        for (int i = 0; i < thread_num; ++i) {
          threads[i] = std::thread([&]() {
            while (true) {
              size_t cur = e_ind.fetch_add(1);
              if (cur >= edge_files.size()) {
                break;
              }
              auto& edge_file = edge_files[cur];
              auto src_label_id = std::get<0>(edge_file.first);
              auto dst_label_id = std::get<1>(edge_file.first);
              auto e_label_id = std::get<2>(edge_file.first);
              auto& file_names = edge_file.second;
              auto src_dst_col_pair = loading_config.GetEdgeSrcDstCol(
                  src_label_id, dst_label_id, e_label_id);
              if (src_dst_col_pair.first.size() != 1 ||
                  src_dst_col_pair.second.size() != 1) {
                LOG(FATAL)
                    << "We currenly only support one src primary key and one "
                       "dst primary key";
              }
              initEdges(src_label_id, dst_label_id, e_label_id, file_names,
                        loading_config.GetEdgeColumnMappings(
                            src_label_id, dst_label_id, e_label_id),
                        src_dst_col_pair.first[0], src_dst_col_pair.second[0]);
            }
          });
        }
        for (auto& thrd : threads) {
          thrd.join();
        }
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
  } else if (properties[0] == PropertyType::kInt64) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<int64_t>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<int64_t>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<int64_t>();
    }
  } else if (properties[0] == PropertyType::kDouble) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<double>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<double>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<double>();
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

MutableCsrConstEdgeIterBase* MutablePropertyFragment::get_outgoing_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter_raw(u);
}

MutableCsrConstEdgeIterBase* MutablePropertyFragment::get_incoming_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter_raw(u);
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
    const std::vector<std::pair<size_t, std::string>>&
        in_vertex_column_mappings,
    IdIndexer<oid_t, vid_t>& indexer) {
  if (filenames.empty()) {
    return;
  }
  LOG(INFO) << "Parsing vertex files for label " << vertex_label;
  auto vertex_column_mappings = in_vertex_column_mappings;

  size_t label_index = schema_.get_vertex_label_id(vertex_label);
  auto& table = vertex_data_[label_index];
  auto& property_types = schema_.get_vertex_properties(vertex_label);
  size_t col_num = property_types.size();
  auto primary_key = schema_.get_vertex_primary_key(label_index)[0];
  auto primary_key_name = primary_key.second;

  // vertex_column_mappings can be empty, empty means the each column in the
  // file is mapped to the same column in the table.
  std::vector<Any> header(col_num + 1);
  {
    for (auto i = 0; i < header.size(); ++i) {
      header[i].type = PropertyType::kString;
    }
  }
  std::vector<Any> properties(col_num + 1);
  std::vector<std::string> column_names(col_num + 1);
  size_t primary_key_ind = col_num + 1;

  // First get header
  get_header_row(filenames[0], header);
  // construct column_names
  for (auto i = 0; i < header.size(); ++i) {
    column_names[i] =
        std::string(header[i].value.s.data(), header[i].value.s.size());
  }

  if (vertex_column_mappings.empty()) {
    vertex_column_mappings = generate_default_column_mapping(
        filenames[0], primary_key_name, column_names);
    VLOG(10) << "vertex_column_mappings is empty, "
                "generate_default_column_mapping returns "
             << vertex_column_mappings.size() << " mappings";
  }
  for (auto i = 0; i < properties.size(); ++i) {
    if (column_names[i] == primary_key_name) {
      primary_key_ind = i;
      break;
    }
    VLOG(10) << " compare: " << column_names[i] << " " << primary_key_name;
  }
  CHECK(primary_key_ind != col_num + 1);
  {
    // reset header of table with primary key removed
    std::vector<std::string> header_col_names;
    for (auto i = 0; i < column_names.size(); ++i) {
      if (i != primary_key_ind) {
        header_col_names.emplace_back(column_names[i]);
      }
    }
    table.reset_header(header_col_names);
    VLOG(10) << "reset header of table with primary key removed: "
             << header_col_names.size();
  }

  for (auto i = 0; i < properties.size(); ++i) {
    if (i < primary_key_ind) {
      properties[i].type = property_types[i];
    } else if (i > primary_key_ind) {
      properties[i].type = property_types[i - 1];
    } else {
      properties[i].type = primary_key.first;
    }
  }

  char line_buf[4096];
  // we can't assume oid will be the first column.
  oid_t oid;
  vid_t v_index;

  std::vector<int32_t> file_col_to_schema_col_ind;
  {
    // parse from vertex_column_mappings, vertex_column_mappings doesn't
    // contains primary key.
    size_t max_ind = 0;
    for (auto& pair : vertex_column_mappings) {
      max_ind = std::max(max_ind, pair.first);
    }
    file_col_to_schema_col_ind.resize(max_ind + 1, -1);
    for (auto& pair : vertex_column_mappings) {
      // if meet primary key, skip it.
      if (pair.second == primary_key_name) {
        VLOG(10) << "Skip primary key column " << pair.first << ", "
                 << pair.second;
        continue;
      }
      if (file_col_to_schema_col_ind[pair.first] == -1) {
        if (schema_.vertex_has_property(vertex_label, pair.second)) {
          auto& prop_names = schema_.get_vertex_property_names(vertex_label);
          // find index of pair.second in prop_names
          auto iter =
              std::find(prop_names.begin(), prop_names.end(), pair.second);
          // must be a valid iter.
          if (iter == prop_names.end()) {
            LOG(FATAL) << "Column " << pair.first << " is mapped to a column "
                       << "that does not exist in schema: " << pair.second;
          }
          file_col_to_schema_col_ind[pair.first] =
              std::distance(prop_names.begin(), iter);
          VLOG(10) << "Column " << std::to_string(pair.first)
                   << " is mapped to column " << pair.second << " in schema.: "
                   << std::to_string(file_col_to_schema_col_ind[pair.first]);
        } else {
          LOG(FATAL) << "Column " << pair.first << " is mapped to a column "
                     << "that does not exist in schema: " << pair.second;
        }
      } else {
        LOG(FATAL) << "Column " << pair.first << " is mapped to multiple "
                   << "columns in bulk loading file.";
      }
    }
  }

  for (auto filename : filenames) {
    VLOG(10) << "Processing file: " << filename;
    FILE* fin = fopen(filename.c_str(), "r");
    // Just read first line, and do nothing, the header of file is not needed.
    if (fgets(line_buf, 4096, fin) == NULL) {
      continue;
    }
    preprocess_line(line_buf);
    while (fgets(line_buf, 4096, fin) != NULL) {
      preprocess_line(line_buf);
      ParseRecord(line_buf, properties);
      oid = properties[primary_key_ind].AsInt64();
      if (indexer.add(oid, v_index)) {
        // insert properties except for primary_key_ind
        table.insert(v_index, properties, file_col_to_schema_col_ind);
      }
    }

    fclose(fin);
  }
}

}  // namespace gs
