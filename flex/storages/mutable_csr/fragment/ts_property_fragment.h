#ifndef GRAPHSCOPE_FRAGMENT_TS_PROPERTY_FRAGMENT_H_
#define GRAPHSCOPE_FRAGMENT_TS_PROPERTY_FRAGMENT_H_

#include <thread>
#include <tuple>
#include <vector>

#include "grape/io/local_io_adaptor.h"
#include "grape/serialization/out_archive.h"
#include "flex/storages/mutable_csr/graph/id_indexer.h"
#include "flex/storages/mutable_csr/graph/ts_csr.h"
#include "flex/storages/mutable_csr/property/table.h"
#include "flex/storages/mutable_csr/types.h"

namespace gs {

class Schema {
 public:
  Schema();
  ~Schema();

  void add_vertex_label(const std::string& label,
                        const std::vector<PropertyType>& properties,
                        const std::vector<StorageStrategy>& strategies = {},
                        size_t max_vnum = static_cast<size_t>(1) << 32);

  void add_edge_label(const std::string& src_label,
                      const std::string& dst_label,
                      const std::string& edge_label,
                      const std::vector<PropertyType>& properties,
                      EdgeStrategy oe = EdgeStrategy::kMultiple,
                      EdgeStrategy ie = EdgeStrategy::kMultiple);

  label_t vertex_label_num() const;

  label_t edge_label_num() const;

  label_t get_vertex_label_id(const std::string& label) const;

  void set_vertex_properties(
      label_t label_id, const std::vector<PropertyType>& types,
      const std::vector<StorageStrategy>& strategies = {});

  const std::vector<PropertyType>& get_vertex_properties(
      const std::string& label) const;

  const std::vector<PropertyType>& get_vertex_properties(label_t label) const;

  const std::vector<StorageStrategy>& get_vertex_storage_strategies(
      const std::string& label) const;

  size_t get_max_vnum(const std::string& label) const;

  bool exist(const std::string& src_label, const std::string& dst_label,
             const std::string& edge_label);

  const std::vector<PropertyType>& get_edge_properties(
      const std::string& src_label, const std::string& dst_label,
      const std::string& label) const;

  PropertyType get_edge_property(label_t src, label_t dst, label_t edge) const;

  bool valid_edge_property(const std::string& src_label,
                           const std::string& dst_label,
                           const std::string& label) const;

  EdgeStrategy get_outgoing_edge_strategy(const std::string& src_label,
                                          const std::string& dst_label,
                                          const std::string& label) const;

  EdgeStrategy get_incoming_edge_strategy(const std::string& src_label,
                                          const std::string& dst_label,
                                          const std::string& label) const;
  EdgeStrategy get_outgoing_edge_strategy(label_t& src_label,
                                          label_t& dst_label,
                                          label_t& label) const;

  EdgeStrategy get_incoming_edge_strategy(label_t& src_label,
                                          label_t& dst_label,
                                          label_t& label) const;

  label_t get_edge_label_id(const std::string& label) const;

  // Get the vertex label of this type of edge.
  // FIXME: one edge label can map to different vertex label tuples
  std::pair<label_t, label_t> get_edge_label_vertex_labels_id(
      const label_t& e_label) const;

  std::string get_vertex_label_name(label_t index) const;

  std::string get_edge_label_name(label_t index) const;

  void Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer);

  void Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader);

 private:
  label_t vertex_label_to_index(const std::string& label);

  label_t edge_label_to_index(const std::string& label);

  uint32_t generate_edge_label(label_t src, label_t dst, label_t edge) const;

  IdIndexer<std::string, label_t> vlabel_indexer_;
  IdIndexer<std::string, label_t> elabel_indexer_;
  std::vector<label_t> elabel_ind_src_label_;
  std::vector<label_t> elabel_ind_dst_label_;
  std::vector<std::vector<PropertyType>> vproperties_;
  std::vector<std::vector<StorageStrategy>> vprop_storage_;
  std::map<uint32_t, std::vector<PropertyType>> eproperties_;
  std::map<uint32_t, EdgeStrategy> oe_strategy_;
  std::map<uint32_t, EdgeStrategy> ie_strategy_;
  std::vector<size_t> max_vnum_;
};

std::string display_size(size_t size);

template <typename EDATA_T>
class TSSubGraph {
 public:
  TSSubGraph(TSCsr<EDATA_T>& csr, label_t src_label, label_t dst_label)
      : csr_(csr), src_label_(src_label), dst_label_(dst_label) {}

  TSSubGraph(const TSSubGraph<EDATA_T>& other)
      : csr_(other.csr_),
        src_label_(other.src_label_),
        dst_label_(other.dst_label_) {}

  TSNbrSlice<EDATA_T> get_edges(vid_t v) const { return csr_.get_edges(v); }

  const TSNbr<EDATA_T>* get_edge(vid_t v, eid_t inner_eid) const {
    return csr_.get_edge(v, inner_eid);
  }

  int degree(vid_t v) const { return csr_.degree(v); }

  inline std::shared_ptr<TypedTSCsrConstEdgeIter<EDATA_T>> edge_iter(vid_t v) {
    return csr_.edge_iter(v);
  }

  TSSubGraph& operator=(const TSSubGraph<EDATA_T>& other) {
    if (*this == other)
      return *this;
    this->csr_ = other.csr_;
    return *this;
  }

  bool operator==(const TSSubGraph<EDATA_T>& other) {
    return csr_ == other.csr_;
  }

  label_t GetSrcLabel() const { return src_label_; }
  label_t GetDstLabel() const { return dst_label_; }

 private:
  TSCsr<EDATA_T>& csr_;
  label_t src_label_, dst_label_;
};

template <typename EDATA_T>
class TSSingleSubGraph {
  using nbr_t = TSNbr<EDATA_T>;
  using nbr_slice_t = TSNbrSlice<EDATA_T>;

 public:
  TSSingleSubGraph(SingleTSCsr<EDATA_T>& csr, label_t src_label,
                   label_t dst_label)
      : csr_(csr), src_label_(src_label), dst_label_(dst_label) {}

  TSSingleSubGraph(const TSSingleSubGraph<EDATA_T>& other)
      : csr_(other.csr_),
        src_label_(other.src_label_),
        dst_label_(other.dst_label_) {}

  const nbr_t& get_edge(vid_t v) const { return csr_.get_edge(v); }

  inline nbr_slice_t get_edges(vid_t v) const { return csr_.get_edges(v); }

  bool valid(vid_t v) const { return csr_.valid(v); }

  TSSingleSubGraph& operator=(const TSSingleSubGraph<EDATA_T>& other) {
    if (*this == other)
      return *this;
    this->csr_ = other.csr_;
    return *this;
  }

  bool operator==(const TSSingleSubGraph<EDATA_T>& other) {
    return csr_ == other.csr_;
  }

  label_t GetSrcLabel() const { return src_label_; }
  label_t GetDstLabel() const { return dst_label_; }

 private:
  SingleTSCsr<EDATA_T>& csr_;
  label_t src_label_, dst_label_;
};

class VertexStore {
 public:
  VertexStore(LFIndexer<vid_t>& indexer, Table& data);

  vid_t vertex_num() const;

  vid_t get_vertex(oid_t id) const;

  bool get_vertex(oid_t id, vid_t& v) const;

  vid_t add_vertex(oid_t id);

  vid_t add_vertex(oid_t id, const std::vector<Any>& properties);

  oid_t get_id(vid_t i) const;

  size_t vertex_properties_count() const;

  Any get_property(vid_t v, size_t prop_id) const;

  std::shared_ptr<ColumnBase> get_property_column(int col_id);

  const std::shared_ptr<ColumnBase> get_property_column(int col_id) const;

  std::shared_ptr<ColumnBase> get_property_column(const std::string& name);

  const std::shared_ptr<ColumnBase> get_property_column(
      const std::string& name) const;

  void ingest(vid_t lid, grape::OutArchive& arc);

  void set_properties(vid_t i, const std::vector<Any>& props);

 private:
  LFIndexer<vid_t>& indexer_;
  Table& data_;
};

class ConstVertexStore {
 public:
  ConstVertexStore(const LFIndexer<vid_t>& indexer, const Table& data);

  vid_t vertex_num() const;

  vid_t get_vertex(oid_t id) const;

  bool get_vertex(oid_t id, vid_t& v) const;

  oid_t get_id(vid_t i) const;

  size_t vertex_properties_count() const;

  Any get_property(vid_t v, size_t prop_id) const;

  const std::shared_ptr<ColumnBase> get_property_column(int col_id) const;

  const std::shared_ptr<ColumnBase> get_property_column(
      const std::string& name) const;

 private:
  const LFIndexer<vid_t>& indexer_;
  const Table& data_;
};

class TSPropertyFragment {
 public:
  TSPropertyFragment();

  ~TSPropertyFragment();

  void Init(
      const Schema& schema,
      const std::vector<std::pair<std::string, std::string>>& vertex_files,
      const std::vector<std::tuple<std::string, std::string, std::string,
                                   std::string>>& edge_files,
      int thread_num = 1);

  void Init(const Schema& schema);

  void IngestEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                  vid_t dst_lid, label_t edge_label, timestamp_t ts,
                  grape::OutArchive& arc, ArenaAllocator& alloc);

  template <typename EDATA_T>
  TSSubGraph<EDATA_T> GetOutgoingSubGraph(label_t src_label, label_t dst_label,
                                          label_t edge_label) const {
    size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                   dst_label * edge_label_num_ + edge_label;
    TSCsr<EDATA_T>* oe_csr = dynamic_cast<TSCsr<EDATA_T>*>(oe_[index]);
    return TSSubGraph<EDATA_T>(*oe_csr, src_label, dst_label);
  }

  template <typename EDATA_T>
  TSSubGraph<EDATA_T> GetIncomingSubGraph(label_t src_label, label_t dst_label,
                                          label_t edge_label) const {
    size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                   dst_label * edge_label_num_ + edge_label;
    CHECK(ie_[index]);
    TSCsr<EDATA_T>* ie_csr = dynamic_cast<TSCsr<EDATA_T>*>(ie_[index]);
    CHECK(ie_csr);
    return TSSubGraph<EDATA_T>(*ie_csr, src_label, dst_label);
  }

  template <typename EDATA_T>
  TSSingleSubGraph<EDATA_T> GetOutgoingSingleSubGraph(
      label_t src_label, label_t dst_label, label_t edge_label) const {
    size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                   dst_label * edge_label_num_ + edge_label;
    SingleTSCsr<EDATA_T>* oe_csr =
        dynamic_cast<SingleTSCsr<EDATA_T>*>(oe_[index]);
    return TSSingleSubGraph<EDATA_T>(*oe_csr, src_label, dst_label);
  }

  template <typename EDATA_T>
  TSSingleSubGraph<EDATA_T> GetIncomingSingleSubGraph(
      label_t src_label, label_t dst_label, label_t edge_label) const {
    size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                   dst_label * edge_label_num_ + edge_label;
    SingleTSCsr<EDATA_T>* ie_csr =
        dynamic_cast<SingleTSCsr<EDATA_T>*>(ie_[index]);
    return TSSingleSubGraph<EDATA_T>(*ie_csr, src_label, dst_label);
  }

  VertexStore GetVertexStore(label_t label);

  ConstVertexStore GetVertexStore(label_t label) const;

  const Schema& schema() const;

  void Serialize(const std::string& prefix);

  void Deserialize(const std::string& prefix);

  Table& get_vertex_table(label_t vertex_label);

  const Table& get_vertex_table(label_t vertex_label) const;

  vid_t vertex_num(label_t vertex_label) const;

  bool get_lid(label_t label, oid_t oid, vid_t& lid) const;

  oid_t get_oid(label_t label, vid_t lid) const;

  const LFIndexer<vid_t>& get_const_indexer(label_t label) const;

  LFIndexer<vid_t>& get_indexer(label_t label);

  std::shared_ptr<TSCsrConstEdgeIterBase> get_outgoing_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<TSCsrConstEdgeIterBase> get_incoming_edges(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const;

  std::shared_ptr<TSCsrEdgeIterBase> get_outgoing_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  std::shared_ptr<TSCsrEdgeIterBase> get_incoming_edges_mut(
      label_t label, vid_t u, label_t neighbor_label, label_t edge_label);

  TSCsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                        label_t edge_label);

  const TSCsrBase* get_oe_csr(label_t label, label_t neighbor_label,
                              label_t edge_label) const;

  TSCsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                        label_t edge_label);

  const TSCsrBase* get_ie_csr(label_t label, label_t neighbor_label,
                              label_t edge_label) const;

  void parseVertexFiles(const std::string& vertex_label,
                        const std::vector<std::string>& filenames,
                        IdIndexer<oid_t, vid_t>& indexer);

  void parseEdgeFiles(const std::string& src_label,
                      const std::string& dst_label,
                      const std::string& edge_label,
                      const std::vector<std::string>& filenames);

  Schema schema_;
  std::vector<LFIndexer<vid_t>> lf_indexers_;
  std::vector<TSCsrBase*> ie_, oe_;
  std::vector<Table> vertex_data_;

  size_t vertex_label_num_, edge_label_num_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_TS_PROPERTY_FRAGMENT_H_
