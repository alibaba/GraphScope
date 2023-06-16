#ifndef GRAPHSCOPE_MUTABLE_CSR_READ_TRANSACTION_H_
#define GRAPHSCOPE_MUTABLE_CSR_READ_TRANSACTION_H_

#include <limits>
#include <utility>

#include "flex/storages/mutable_csr/fragment/ts_property_fragment.h"
#include "flex/storages/mutable_csr/version_manager.h"

namespace gs {

class ReadTransaction {
 public:
  ReadTransaction(const TSPropertyFragment& graph, VersionManager& vm,
                  timestamp_t timestamp);
  ~ReadTransaction();

  timestamp_t timestamp() const;

  void Commit();

  void Abort();

  class vertex_iterator {
   public:
    vertex_iterator(vid_t cur, vid_t num, const Table& table,
                    const LFIndexer<vid_t>& indexer);
    ~vertex_iterator();

    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    oid_t GetId() const;
    vid_t GetIndex() const;

    Any GetField(int col_id) const;

   private:
    vid_t cur_;
    vid_t num_;
    const Table& table_;
    const LFIndexer<vid_t>& indexer_;
  };

  class edge_iterator {
   public:
    edge_iterator(label_t neighbor_label, label_t edge_label,
                  std::shared_ptr<TSCsrConstEdgeIterBase> iter);
    ~edge_iterator();

    Any GetData() const;

    bool IsValid() const;

    void Next();

    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    label_t neighbor_label_;
    label_t edge_label_;

    std::shared_ptr<TSCsrConstEdgeIterBase> iter_;
  };

  vertex_iterator GetVertexIterator(label_t label) const;

  vertex_iterator FindVertex(label_t label, oid_t id) const;

  bool GetVertexIndex(label_t label, oid_t id, vid_t& index) const;

  vid_t GetVertexNum(label_t label) const;

  oid_t GetVertexId(label_t label, vid_t index) const;

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighnor_label,
                                   label_t edge_label) const;

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighnor_label,
                                  label_t edge_label) const;

  const Schema& schema() const;

 private:
  void release();

  const TSPropertyFragment& graph_;
  VersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_MUTABLE_CSR_READ_TRANSACTION_H_
