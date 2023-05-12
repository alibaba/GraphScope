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

#ifndef GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
#define GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_

#include <limits>
#include <utility>

#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

class MutablePropertyFragment;
class VersionManager;

class ReadTransaction {
 public:
  ReadTransaction(const MutablePropertyFragment& graph, VersionManager& vm,
                  timestamp_t timestamp);
  ~ReadTransaction();

  timestamp_t timestamp() const;

  void Commit();

  void Abort();

  class vertex_iterator {
   public:
    vertex_iterator(label_t label, vid_t cur, vid_t num,
                    const MutablePropertyFragment& graph);
    ~vertex_iterator();

    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    oid_t GetId() const;
    vid_t GetIndex() const;

    Property GetField(int col_id) const;
    int FieldNum() const;

   private:
    label_t label_;
    vid_t cur_;
    vid_t num_;
    const MutablePropertyFragment& graph_;
  };

  class edge_iterator {
   public:
    edge_iterator(label_t neighbor_label, label_t edge_label,
                  std::shared_ptr<GenericNbrIterator<vid_t>> iter);
    ~edge_iterator();

    Property GetData() const;

    bool IsValid() const;

    void Next();

    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    label_t neighbor_label_;
    label_t edge_label_;

    std::shared_ptr<GenericNbrIterator<vid_t>> iter_;
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

  template <typename EDATA_T>
  MutableCsrView<vid_t, EDATA_T, timestamp_t> GetOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<vid_t, EDATA_T, timestamp_t>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return csr->get_graph_view(timestamp_);
  }

  template <typename EDATA_T>
  MutableCsrView<vid_t, EDATA_T, timestamp_t> GetIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<vid_t, EDATA_T, timestamp_t>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return csr->get_graph_view(timestamp_);
  }

  template <typename EDATA_T>
  SingleMutableCsrView<vid_t, EDATA_T, timestamp_t> GetOutgoingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<vid_t, EDATA_T, timestamp_t>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return csr->get_graph_view(timestamp_);
  }

  template <typename EDATA_T>
  SingleMutableCsrView<vid_t, EDATA_T, timestamp_t> GetIncomingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<vid_t, EDATA_T, timestamp_t>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return csr->get_graph_view(timestamp_);
  }

  TableMutableCsrView<vid_t, timestamp_t> GetOutgoingTableGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const TableMutableCsr<vid_t, timestamp_t>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return csr->get_graph_view(timestamp_);
  }

  TableMutableCsrView<vid_t, timestamp_t> GetIncomingTableGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const TableMutableCsr<vid_t, timestamp_t>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return csr->get_graph_view(timestamp_);
  }

 private:
  void release();

  const MutablePropertyFragment& graph_;
  VersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
