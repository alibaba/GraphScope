#include "flex/storages/mutable_csr/fragment/ts_property_fragment.h"
#include "flex/storages/mutable_csr/fragment/graph_file_parsers.h"

namespace gs {

Schema::Schema() = default;
Schema::~Schema() = default;

void Schema::add_vertex_label(const std::string& label,
                              const std::vector<PropertyType>& properties,
                              const std::vector<StorageStrategy>& strategies,
                              size_t max_vnum) {
  label_t v_label_id = vertex_label_to_index(label);
  vproperties_[v_label_id] = properties;
  vprop_storage_[v_label_id] = strategies;
  vprop_storage_[v_label_id].resize(vproperties_[v_label_id].size(),
                                    StorageStrategy::kMem);
  max_vnum_[v_label_id] = max_vnum;
}

void Schema::add_edge_label(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& edge_label,
                            const std::vector<PropertyType>& properties,
                            EdgeStrategy oe, EdgeStrategy ie) {
  label_t src_label_id = vertex_label_to_index(src_label);
  label_t dst_label_id = vertex_label_to_index(dst_label);
  label_t edge_label_id = edge_label_to_index(edge_label);

  uint32_t label_id =
      generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  eproperties_[label_id] = properties;
  oe_strategy_[label_id] = oe;
  ie_strategy_[label_id] = ie;
  // add src and dst label of edge.
  elabel_ind_src_label_[edge_label_id] = src_label_id;
  elabel_ind_dst_label_[edge_label_id] = dst_label_id;
}

label_t Schema::vertex_label_num() const {
  return static_cast<label_t>(vlabel_indexer_.size());
}

label_t Schema::edge_label_num() const {
  return static_cast<label_t>(elabel_indexer_.size());
}

label_t Schema::get_vertex_label_id(const std::string& label) const {
  label_t ret;
  CHECK(vlabel_indexer_.get_index(label, ret));
  return ret;
}

void Schema::set_vertex_properties(
    label_t label_id, const std::vector<PropertyType>& types,
    const std::vector<StorageStrategy>& strategies) {
  vproperties_[label_id] = types;
  vprop_storage_[label_id] = strategies;
  vprop_storage_[label_id].resize(types.size(), StorageStrategy::kMem);
}

const std::vector<PropertyType>& Schema::get_vertex_properties(
    const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  return vproperties_[index];
}

const std::vector<PropertyType>& Schema::get_vertex_properties(
    label_t label) const {
  return vproperties_[label];
}

const std::vector<StorageStrategy>& Schema::get_vertex_storage_strategies(
    const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  return vprop_storage_[index];
}

size_t Schema::get_max_vnum(const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  return max_vnum_[index];
}

bool Schema::exist(const std::string& src_label, const std::string& dst_label,
                   const std::string& edge_label) {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(edge_label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.find(index) != eproperties_.end();
}

const std::vector<PropertyType>& Schema::get_edge_properties(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.at(index);
}

PropertyType Schema::get_edge_property(label_t src, label_t dst,
                                       label_t edge) const {
  uint32_t index = generate_edge_label(src, dst, edge);
  auto& vec = eproperties_.at(index);
  return vec.empty() ? PropertyType::kEmpty : vec[0];
}

bool Schema::valid_edge_property(const std::string& src_label,
                                 const std::string& dst_label,
                                 const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.find(index) != eproperties_.end();
}

EdgeStrategy Schema::get_outgoing_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return oe_strategy_.at(index);
}

EdgeStrategy Schema::get_incoming_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return ie_strategy_.at(index);
}

EdgeStrategy Schema::get_outgoing_edge_strategy(label_t& src, label_t& dst,
                                                label_t& edge) const {
  uint32_t index = generate_edge_label(src, dst, edge);
  return oe_strategy_.at(index);
}

EdgeStrategy Schema::get_incoming_edge_strategy(label_t& src, label_t& dst,
                                                label_t& edge) const {
  uint32_t index = generate_edge_label(src, dst, edge);
  return ie_strategy_.at(index);
}

label_t Schema::get_edge_label_id(const std::string& label) const {
  label_t ret;
  CHECK(elabel_indexer_.get_index(label, ret));
  return ret;
}

// Get the vertex label of this type of edge.
std::pair<label_t, label_t> Schema::get_edge_label_vertex_labels_id(
    const label_t& e_label) const {
  CHECK(e_label < elabel_ind_src_label_.size());
  return std::make_pair(elabel_ind_src_label_[e_label],
                        elabel_ind_dst_label_[e_label]);
}

std::string Schema::get_vertex_label_name(label_t index) const {
  std::string ret;
  vlabel_indexer_.get_key(index, ret);
  return ret;
}

std::string Schema::get_edge_label_name(label_t index) const {
  std::string ret;
  elabel_indexer_.get_key(index, ret);
  return ret;
}

void Schema::Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer) {
  vlabel_indexer_.Serialize(writer);
  elabel_indexer_.Serialize(writer);
  grape::InArchive arc;
  arc << vproperties_ << vprop_storage_ << eproperties_ << ie_strategy_
      << oe_strategy_ << elabel_ind_src_label_ << elabel_ind_dst_label_
      << max_vnum_;
  CHECK(writer->WriteArchive(arc));
}

void Schema::Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader) {
  vlabel_indexer_.Deserialize(reader);
  elabel_indexer_.Deserialize(reader);
  grape::OutArchive arc;
  CHECK(reader->ReadArchive(arc));
  arc >> vproperties_ >> vprop_storage_ >> eproperties_ >> ie_strategy_ >>
      oe_strategy_ >> elabel_ind_src_label_ >> elabel_ind_dst_label_ >>
      max_vnum_;
}

label_t Schema::vertex_label_to_index(const std::string& label) {
  label_t ret;
  vlabel_indexer_.add(label, ret);
  if (vproperties_.size() <= ret) {
    vproperties_.resize(ret + 1);
    vprop_storage_.resize(ret + 1);
    max_vnum_.resize(ret + 1);
  }
  return ret;
}

label_t Schema::edge_label_to_index(const std::string& label) {
  label_t ret;
  elabel_indexer_.add(label, ret);
  if (elabel_ind_dst_label_.size() <= ret) {
    elabel_ind_dst_label_.resize(ret + 1);
    elabel_ind_src_label_.resize(ret + 1);
  }
  return ret;
}

uint32_t Schema::generate_edge_label(label_t src, label_t dst,
                                     label_t edge) const {
  uint32_t ret = 0;
  ret |= src;
  ret <<= 8;
  ret |= dst;
  ret <<= 8;
  ret |= edge;
  return ret;
}

VertexStore::VertexStore(LFIndexer<vid_t>& indexer, Table& data)
    : indexer_(indexer), data_(data) {}

vid_t VertexStore::vertex_num() const { return indexer_.size(); }

vid_t VertexStore::get_vertex(oid_t id) const { return indexer_.get_index(id); }

bool VertexStore::get_vertex(oid_t id, vid_t& v) const {
  return indexer_.get_index(id, v);
}

vid_t VertexStore::add_vertex(oid_t id) { return indexer_.insert(id); }

vid_t VertexStore::add_vertex(oid_t id, const std::vector<Any>& properties) {
  vid_t index = indexer_.insert(id);
  data_.insert(index, properties);
  return index;
}

oid_t VertexStore::get_id(vid_t i) const { return indexer_.get_key(i); }

size_t VertexStore::vertex_properties_count() const { return data_.col_num(); }

Any VertexStore::get_property(vid_t v, size_t prop_id) const {
  return data_.at(v, prop_id);
}

std::shared_ptr<ColumnBase> VertexStore::get_property_column(int col_id) {
  return data_.get_column_by_id(col_id);
}

const std::shared_ptr<ColumnBase> VertexStore::get_property_column(
    int col_id) const {
  return data_.get_column_by_id(col_id);
}

std::shared_ptr<ColumnBase> VertexStore::get_property_column(
    const std::string& name) {
  return data_.get_column(name);
}

const std::shared_ptr<ColumnBase> VertexStore::get_property_column(
    const std::string& name) const {
  return data_.get_column(name);
}

void VertexStore::ingest(vid_t lid, grape::OutArchive& arc) {
  data_.ingest(lid, arc);
}

void VertexStore::set_properties(vid_t i, const std::vector<Any>& props) {
  data_.insert(i, props);
}

ConstVertexStore::ConstVertexStore(const LFIndexer<vid_t>& indexer,
                                   const Table& data)
    : indexer_(indexer), data_(data) {}

vid_t ConstVertexStore::vertex_num() const { return indexer_.size(); }

vid_t ConstVertexStore::get_vertex(oid_t id) const {
  return indexer_.get_index(id);
}

bool ConstVertexStore::get_vertex(oid_t id, vid_t& v) const {
  return indexer_.get_index(id, v);
}

oid_t ConstVertexStore::get_id(vid_t i) const { return indexer_.get_key(i); }

size_t ConstVertexStore::vertex_properties_count() const {
  return data_.col_num();
}

Any ConstVertexStore::get_property(vid_t v, size_t prop_id) const {
  return data_.at(v, prop_id);
}

const std::shared_ptr<ColumnBase> ConstVertexStore::get_property_column(
    int col_id) const {
  return data_.get_column_by_id(col_id);
}

const std::shared_ptr<ColumnBase> ConstVertexStore::get_property_column(
    const std::string& name) const {
  return data_.get_column(name);
}

TSPropertyFragment::TSPropertyFragment() {}

TSPropertyFragment::~TSPropertyFragment() {
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

void TSPropertyFragment::Init(
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
      IdIndexer<oid_t, vid_t> indexer;
      std::string v_label_name =
          schema_.get_vertex_label_name(static_cast<label_t>(v_label_i));
      std::vector<std::string> filenames;
      for (auto& pair : vertex_files) {
        if (pair.first == v_label_name) {
          filenames.push_back(pair.second);
        }
      }
      LOG(INFO) << "start loading vertex-" << v_label_name;
      parseVertexFiles(v_label_name, filenames, indexer);
      if (indexer.bucket_count() == 0) {
        indexer._rehash(1024);
      }
      build_lf_indexer(indexer, lf_indexers_[v_label_i]);
    }

    for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
         ++src_label_i) {
      std::string src_label_name = schema_.get_vertex_label_name(src_label_i);
      for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
           ++dst_label_i) {
        std::string dst_label_name = schema_.get_vertex_label_name(dst_label_i);
        for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
          std::string e_label_name = schema_.get_edge_label_name(e_label_i);
          std::vector<std::string> filenames;
          for (auto& tup : edge_files) {
            if (std::get<0>(tup) == src_label_name &&
                std::get<1>(tup) == dst_label_name &&
                std::get<2>(tup) == e_label_name) {
              filenames.push_back(std::get<3>(tup));
            }
          }
          if (!filenames.empty()) {
            LOG(INFO) << "start loading edge-" << src_label_name << "-"
                      << e_label_name << "-" << dst_label_name;
            parseEdgeFiles(src_label_name, dst_label_name, e_label_name,
                           filenames);
          }
        }
      }
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
            IdIndexer<oid_t, vid_t> indexer;
            std::string v_label_name =
                schema_.get_vertex_label_name(static_cast<label_t>(cur));
            std::vector<std::string> filenames;
            for (auto& pair : vertex_files) {
              if (pair.first == v_label_name) {
                filenames.push_back(pair.second);
              }
            }
            LOG(INFO) << "start loading vertex-" << v_label_name;
            parseVertexFiles(v_label_name, filenames, indexer);
            if (indexer.bucket_count() == 0) {
              indexer._rehash(1024);
            }
            build_lf_indexer(indexer, lf_indexers_[cur]);
          }
        });
      }
      for (auto& thrd : threads) {
        thrd.join();
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
            std::vector<std::string> filenames;
            for (auto& tup : edge_files) {
              if (std::get<0>(tup) == src_label_name &&
                  std::get<1>(tup) == dst_label_name &&
                  std::get<2>(tup) == e_label_name) {
                filenames.push_back(std::get<3>(tup));
              }
            }
            if (!filenames.empty()) {
              parseEdgeFiles(src_label_name, dst_label_name, e_label_name,
                             filenames);
            }
          }
        });
      }
      for (auto& thrd : threads) {
        thrd.join();
      }
    }
  }
}

template <typename EDATA_T>
class EmptyCsr : public TypedTSCsrBase<EDATA_T> {
 public:
  EmptyCsr() = default;
  ~EmptyCsr() = default;

  void batch_init(vid_t vnum, const std::vector<int>& degree) override {}

  void put_generic_edge(vid_t src, vid_t dst, const Any& data, timestamp_t ts,
                        ArenaAllocator& alloc) override {}

  void Serialize(const std::string& path) override {}

  void Deserialize(const std::string& path) override {}

  void batch_put_edge(vid_t src, vid_t dst, const EDATA_T& data,
                      timestamp_t ts = 0) override {}

  TSNbrSlice<EDATA_T> get_edges(vid_t v) const override {
    return TSNbrSlice<EDATA_T>::empty();
  }

  void ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc, timestamp_t ts,
                   ArenaAllocator& alloc) override {
    EDATA_T value;
    arc >> value;
  }

  void peek_ingest_edge(vid_t src, vid_t dst, grape::OutArchive& arc,
                        const timestamp_t ts, ArenaAllocator& alloc) override {}

  std::shared_ptr<TSCsrConstEdgeIterBase> edge_iter(vid_t v) const override {
    return std::make_shared<TypedTSCsrConstEdgeIter<EDATA_T>>(
        TSNbrSlice<EDATA_T>::empty());
  }

  std::shared_ptr<TSCsrEdgeIterBase> edge_iter_mut(vid_t v) override {
    return std::make_shared<TypedTSCsrEdgeIter<EDATA_T>>(
        TSNbrMutSlice<EDATA_T>::empty());
  }
};

template <typename EDATA_T>
TypedTSCsrBase<EDATA_T>* create_typed_csr(EdgeStrategy es) {
  if (es == EdgeStrategy::kSingle) {
    return new SingleTSCsr<EDATA_T>();
  } else if (es == EdgeStrategy::kMultiple) {
    return new TSCsr<EDATA_T>();
  } else if (es == EdgeStrategy::kNone) {
    return new EmptyCsr<EDATA_T>();
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
}

template <typename EDATA_T>
std::pair<TSCsrBase*, TSCsrBase*> construct_empty_csr(
    EdgeStrategy ie_strategy, EdgeStrategy oe_strategy) {
  TypedTSCsrBase<EDATA_T>* ie_csr = create_typed_csr<EDATA_T>(ie_strategy);
  TypedTSCsrBase<EDATA_T>* oe_csr = create_typed_csr<EDATA_T>(oe_strategy);
  ie_csr->batch_init(0, {});
  oe_csr->batch_init(0, {});
  return std::make_pair(ie_csr, oe_csr);
}

void TSPropertyFragment::Init(const Schema& schema) {
  schema_ = schema;
  vertex_label_num_ = schema_.vertex_label_num();
  edge_label_num_ = schema_.edge_label_num();

  vertex_data_.resize(vertex_label_num_);
  ie_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  oe_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  lf_indexers_.resize(vertex_label_num_);

  for (size_t v_label_i = 0; v_label_i != vertex_label_num_; ++v_label_i) {
    IdIndexer<oid_t, vid_t> indexer;
    std::string v_label_name = schema_.get_vertex_label_name(v_label_i);
    indexer._rehash(schema_.get_max_vnum(v_label_name));
    build_lf_indexer(indexer, lf_indexers_[v_label_i]);

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
          auto& property_types = schema_.get_edge_properties(
              src_label_name, dst_label_name, e_label_name);
          size_t col_num = property_types.size();
          CHECK_LE(col_num, 1);

          size_t src_label_index = schema_.get_vertex_label_id(src_label_name);
          size_t dst_label_index = schema_.get_vertex_label_id(dst_label_name);
          size_t edge_label_index = schema_.get_edge_label_id(e_label_name);
          size_t index = src_label_index * vertex_label_num_ * edge_label_num_ +
                         dst_label_index * edge_label_num_ + edge_label_index;

          EdgeStrategy oe_strtagy = schema_.get_outgoing_edge_strategy(
              src_label_name, dst_label_name, e_label_name);
          EdgeStrategy ie_strtagy = schema_.get_incoming_edge_strategy(
              src_label_name, dst_label_name, e_label_name);

          if (col_num == 0) {
            auto ret =
                construct_empty_csr<grape::EmptyType>(ie_strtagy, oe_strtagy);
            ie_[index] = ret.first;
            oe_[index] = ret.second;
          } else if (property_types[0] == PropertyType::kDate) {
            auto ret = construct_empty_csr<Date>(ie_strtagy, oe_strtagy);
            ie_[index] = ret.first;
            oe_[index] = ret.second;
          } else if (property_types[0] == PropertyType::kInt32) {
            auto ret = construct_empty_csr<int>(ie_strtagy, oe_strtagy);
            ie_[index] = ret.first;
            oe_[index] = ret.second;
          } else if (property_types[0] == PropertyType::kInt64) {
            auto ret = construct_empty_csr<int64_t>(ie_strtagy, oe_strtagy);
            ie_[index] = ret.first;
            oe_[index] = ret.second;
          } else if (property_types[0] == PropertyType::kString) {
            auto ret = construct_empty_csr<std::string>(ie_strtagy, oe_strtagy);
            ie_[index] = ret.first;
            oe_[index] = ret.second;
          } else {
            LOG(FATAL) << "Unexpected edge data type";
          }
        }
      }
    }
  }
}

void TSPropertyFragment::IngestEdge(label_t src_label, vid_t src_lid,
                                    label_t dst_label, vid_t dst_lid,
                                    label_t edge_label, timestamp_t ts,
                                    grape::OutArchive& arc,
                                    ArenaAllocator& alloc) {
  size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                 dst_label * edge_label_num_ + edge_label;
  ie_[index]->peek_ingest_edge(dst_lid, src_lid, arc, ts, alloc);
  oe_[index]->ingest_edge(src_lid, dst_lid, arc, ts, alloc);
}

VertexStore TSPropertyFragment::GetVertexStore(label_t label) {
  return VertexStore(lf_indexers_[label], vertex_data_[label]);
}

ConstVertexStore TSPropertyFragment::GetVertexStore(label_t label) const {
  return ConstVertexStore(lf_indexers_[label], vertex_data_[label]);
}

const Schema& TSPropertyFragment::schema() const { return schema_; }

void TSPropertyFragment::Serialize(const std::string& prefix) {
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

inline TSCsrBase* create_csr(EdgeStrategy es,
                             const std::vector<PropertyType>& properties) {
  if (properties.empty()) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleTSCsr<grape::EmptyType>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new TSCsr<grape::EmptyType>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<grape::EmptyType>();
    }
  } else if (properties[0] == PropertyType::kInt32) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleTSCsr<int>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new TSCsr<int>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<int>();
    }
  } else if (properties[0] == PropertyType::kDate) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleTSCsr<Date>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new TSCsr<Date>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<Date>();
    }
  } else if (properties[0] == PropertyType::kInt64) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleTSCsr<int64_t>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new TSCsr<int64_t>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<int64_t>();
    }
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
  return nullptr;
}

void TSPropertyFragment::Deserialize(const std::string& prefix) {
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

Table& TSPropertyFragment::get_vertex_table(label_t vertex_label) {
  return vertex_data_[vertex_label];
}

const Table& TSPropertyFragment::get_vertex_table(label_t vertex_label) const {
  return vertex_data_[vertex_label];
}

vid_t TSPropertyFragment::vertex_num(label_t vertex_label) const {
  return static_cast<vid_t>(lf_indexers_[vertex_label].size());
}

bool TSPropertyFragment::get_lid(label_t label, oid_t oid, vid_t& lid) const {
  return lf_indexers_[label].get_index(oid, lid);
}

oid_t TSPropertyFragment::get_oid(label_t label, vid_t lid) const {
  return lf_indexers_[label].get_key(lid);
}

const LFIndexer<vid_t>& TSPropertyFragment::get_const_indexer(
    label_t label) const {
  return lf_indexers_[label];
}

LFIndexer<vid_t>& TSPropertyFragment::get_indexer(label_t label) {
  return lf_indexers_[label];
}

std::shared_ptr<TSCsrConstEdgeIterBase> TSPropertyFragment::get_outgoing_edges(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter(u);
}

std::shared_ptr<TSCsrConstEdgeIterBase> TSPropertyFragment::get_incoming_edges(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter(u);
}

std::shared_ptr<TSCsrEdgeIterBase> TSPropertyFragment::get_outgoing_edges_mut(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter_mut(u);
}

std::shared_ptr<TSCsrEdgeIterBase> TSPropertyFragment::get_incoming_edges_mut(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter_mut(u);
}

TSCsrBase* TSPropertyFragment::get_oe_csr(label_t label, label_t neighbor_label,
                                          label_t edge_label) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

const TSCsrBase* TSPropertyFragment::get_oe_csr(label_t label,
                                                label_t neighbor_label,
                                                label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

TSCsrBase* TSPropertyFragment::get_ie_csr(label_t label, label_t neighbor_label,
                                          label_t edge_label) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index];
}

const TSCsrBase* TSPropertyFragment::get_ie_csr(label_t label,
                                                label_t neighbor_label,
                                                label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index];
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

void TSPropertyFragment::parseVertexFiles(
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
        //          col_names[i] = header[i + 1].value.s.to_string();
      }
      table.init(col_names, property_types,
                 schema_.get_vertex_storage_strategies(vertex_label),
                 schema_.get_max_vnum(vertex_label));
      first_file = false;
    }

#if 0
    if (vertex_label == "post") {
      process_line_post(fin, indexer, table);
    } else if (vertex_label == "comment") {
      process_line_comment(fin, indexer, table);
    } else if (vertex_label == "forum") {
      process_line_forum(fin, indexer, table);
    } else if (vertex_label == "person") {
      process_line_person(fin, indexer, table);
    } else {
      while (fgets(line_buf, 4096, fin) != NULL) {
        preprocess_line(line_buf);
        ParseRecord(line_buf, oid, properties);
        if (indexer.add(oid, v_index)) {
          table.insert(v_index, properties);
        }
      }
    }
#endif
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

template <typename EDATA_T>
std::pair<TSCsrBase*, TSCsrBase*> construct_csr(
    const std::vector<std::string>& filenames,
    const std::vector<PropertyType>& property_types, EdgeStrategy ie_strategy,
    EdgeStrategy oe_strategy, const LFIndexer<vid_t>& src_indexer,
    const LFIndexer<vid_t>& dst_indexer) {
  TypedTSCsrBase<EDATA_T>* ie_csr = create_typed_csr<EDATA_T>(ie_strategy);
  TypedTSCsrBase<EDATA_T>* oe_csr = create_typed_csr<EDATA_T>(oe_strategy);

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
        //          col_names[i] = header[i + 2].value.s.to_string();
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

void TSPropertyFragment::parseEdgeFiles(
    const std::string& src_label, const std::string& dst_label,
    const std::string& edge_label, const std::vector<std::string>& filenames) {
  if (filenames.empty()) {
    return;
  }

  auto& property_types =
      schema_.get_edge_properties(src_label, dst_label, edge_label);
  EdgeStrategy oe_strategy =
      schema_.get_outgoing_edge_strategy(src_label, dst_label, edge_label);
  EdgeStrategy ie_strategy =
      schema_.get_incoming_edge_strategy(src_label, dst_label, edge_label);
  size_t src_label_index = schema_.get_vertex_label_id(src_label);
  size_t dst_label_index = schema_.get_vertex_label_id(dst_label);
  size_t edge_label_index = schema_.get_edge_label_id(edge_label);
  size_t index = src_label_index * vertex_label_num_ * edge_label_num_ +
                 dst_label_index * edge_label_num_ + edge_label_index;
  size_t col_num = property_types.size();
  CHECK_LE(col_num, 1);

  if (col_num == 0) {
    LOG(INFO) << "EmptyType edge property";
    auto ret = construct_csr<grape::EmptyType>(
        filenames, property_types, ie_strategy, oe_strategy,
        lf_indexers_[src_label_index], lf_indexers_[dst_label_index]);
    ie_[index] = ret.first;
    oe_[index] = ret.second;
  } else if (property_types[0] == PropertyType::kDate) {
    LOG(INFO) << "Date edge property";
    auto ret = construct_csr<Date>(filenames, property_types, ie_strategy,
                                   oe_strategy, lf_indexers_[src_label_index],
                                   lf_indexers_[dst_label_index]);
    ie_[index] = ret.first;
    oe_[index] = ret.second;
  } else if (property_types[0] == PropertyType::kInt32) {
    LOG(INFO) << "int edge property";
    auto ret = construct_csr<int>(filenames, property_types, ie_strategy,
                                  oe_strategy, lf_indexers_[src_label_index],
                                  lf_indexers_[dst_label_index]);
    ie_[index] = ret.first;
    oe_[index] = ret.second;
  } else if (property_types[0] == PropertyType::kInt64) {
    LOG(INFO) << "int64 edge property";
    auto ret = construct_csr<int64_t>(filenames, property_types, ie_strategy,
                                  oe_strategy, lf_indexers_[src_label_index],
                                  lf_indexers_[dst_label_index]);
    ie_[index] = ret.first;
    oe_[index] = ret.second;
  } else {
    LOG(FATAL) << "Unexpected edge data type";
  }
}

}  // namespace gs
