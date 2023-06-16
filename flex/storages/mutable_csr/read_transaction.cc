#include "flex/storages/mutable_csr/read_transaction.h"

namespace gs {

ReadTransaction::ReadTransaction(const TSPropertyFragment& graph,
                                 VersionManager& vm, timestamp_t timestamp)
    : graph_(graph), vm_(vm), timestamp_(timestamp) {}
ReadTransaction::~ReadTransaction() { release(); }

timestamp_t ReadTransaction::timestamp() const { return timestamp_; }

void ReadTransaction::Commit() { release(); }

void ReadTransaction::Abort() { release(); }

ReadTransaction::vertex_iterator::vertex_iterator(
    vid_t cur, vid_t num, const Table& table, const LFIndexer<vid_t>& indexer)
    : cur_(cur), num_(num), table_(table), indexer_(indexer) {}
ReadTransaction::vertex_iterator::~vertex_iterator() = default;

bool ReadTransaction::vertex_iterator::IsValid() const { return cur_ < num_; }
void ReadTransaction::vertex_iterator::Next() { ++cur_; }
void ReadTransaction::vertex_iterator::Goto(vid_t target) {
  cur_ = std::min(target, num_);
}

oid_t ReadTransaction::vertex_iterator::GetId() const {
  return indexer_.get_key(cur_);
}
vid_t ReadTransaction::vertex_iterator::GetIndex() const { return cur_; }

Any ReadTransaction::vertex_iterator::GetField(int col_id) const {
  return table_.get_column_by_id(col_id)->get(cur_);
}

ReadTransaction::edge_iterator::edge_iterator(
    label_t neighbor_label, label_t edge_label,
    std::shared_ptr<TSCsrConstEdgeIterBase> iter)
    : neighbor_label_(neighbor_label),
      edge_label_(edge_label),
      iter_(std::move(iter)) {}
ReadTransaction::edge_iterator::~edge_iterator() = default;

Any ReadTransaction::edge_iterator::GetData() const {
  return iter_->get_data();
}

bool ReadTransaction::edge_iterator::IsValid() const {
  return iter_->is_valid();
}

void ReadTransaction::edge_iterator::Next() { iter_->next(); }

vid_t ReadTransaction::edge_iterator::GetNeighbor() const {
  return iter_->get_neighbor();
}

label_t ReadTransaction::edge_iterator::GetNeighborLabel() const {
  return neighbor_label_;
}

label_t ReadTransaction::edge_iterator::GetEdgeLabel() const {
  return edge_label_;
}

ReadTransaction::vertex_iterator ReadTransaction::GetVertexIterator(
    label_t label) const {
  return {0, graph_.vertex_num(label), graph_.get_vertex_table(label),
          graph_.get_const_indexer(label)};
}

ReadTransaction::vertex_iterator ReadTransaction::FindVertex(label_t label,
                                                             oid_t id) const {
  vid_t lid;
  if (graph_.get_lid(label, id, lid)) {
    return {label, graph_.vertex_num(label), graph_.get_vertex_table(label),
            graph_.get_const_indexer(label)};
  } else {
    return {graph_.vertex_num(label), graph_.vertex_num(label),
            graph_.get_vertex_table(label), graph_.get_const_indexer(label)};
  }
}

bool ReadTransaction::GetVertexIndex(label_t label, oid_t id,
                                     vid_t& index) const {
  return graph_.get_lid(label, id, index);
}

vid_t ReadTransaction::GetVertexNum(label_t label) const {
  return graph_.vertex_num(label);
}

oid_t ReadTransaction::GetVertexId(label_t label, vid_t index) const {
  return graph_.get_oid(label, index);
}

ReadTransaction::edge_iterator ReadTransaction::GetOutEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  return {neighnor_label, edge_label,
          graph_.get_outgoing_edges(label, u, neighnor_label, edge_label)};
}

ReadTransaction::edge_iterator ReadTransaction::GetInEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  return {neighnor_label, edge_label,
          graph_.get_incoming_edges(label, u, neighnor_label, edge_label)};
}

const Schema& ReadTransaction::schema() const { return graph_.schema(); }

void ReadTransaction::release() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    vm_.release_read_timestamp();
    timestamp_ = std::numeric_limits<timestamp_t>::max();
  }
}

}  // namespace gs
