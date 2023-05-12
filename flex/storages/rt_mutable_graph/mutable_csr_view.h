
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

#ifndef GRAPHSCOPE_GRAPH_MUTABLE_CSR_VIEW_H_
#define GRAPHSCOPE_GRAPH_MUTABLE_CSR_VIEW_H_

#include "flex/storages/rt_mutable_graph/mutable_csr_impl.h"
#include "flex/utils/property/column.h"
#include "flex/utils/property/table.h"
#include "flex/utils/property/types.h"

namespace gs {

template <typename VID_T>
class GenericNbrIterator {
 public:
  virtual bool IsValid() const = 0;
  virtual void Next() = 0;
  virtual VID_T GetNeighbor() const = 0;
  virtual Property GetGenericData() const = 0;
};

template <typename VID_T, typename EDATA_T>
class NbrIterator : public GenericNbrIterator<VID_T> {
 public:
  Property GetGenericData() const override {
    Property ret;
    ret.set_value(GetData());
    return ret;
  }
  virtual EDATA_T GetData() const = 0;
};

namespace mutable_csr_view {

template <typename VID_T, typename EDATA_T, typename TS_T>
class PackedNbrIterator : public NbrIterator<VID_T, EDATA_T> {
  using nbr_t = mutable_csr_impl::Nbr<VID_T, EDATA_T, TS_T>;

 public:
  PackedNbrIterator(const nbr_t* begin, const nbr_t* end, TS_T timestamp)
      : ptr_(begin), end_(end), timestamp_(timestamp) {
    while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
      ++ptr_;
    }
  }
  ~PackedNbrIterator() {}

  bool IsValid() const override { return (ptr_ != end_); }

  void Next() override {
    ++ptr_;
    while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
      ++ptr_;
    }
  }
  VID_T GetNeighbor() const override { return ptr_->neighbor; }
  EDATA_T GetData() const override { return ptr_->data; }

  int estimated_degree() const { return end_ - ptr_; }

 private:
  const nbr_t* ptr_;
  const nbr_t* end_;
  TS_T timestamp_;
};

template <typename VID_T, typename EDATA_T, typename TS_T>
class ColumnNbrIterator : public NbrIterator<VID_T, EDATA_T> {
  using nbr_t = mutable_csr_impl::Nbr<VID_T, size_t, TS_T>;

 public:
  ColumnNbrIterator(const PackedNbrIterator<VID_T, size_t, TS_T>& nbr_iter,
                    const TypedColumn<EDATA_T>* data)
      : nbr_iter_(nbr_iter), data_(data) {}
  ~ColumnNbrIterator() {}

  bool IsValid() const override { return nbr_iter_.IsValid(); }

  void Next() override { nbr_iter_.Next(); }
  VID_T GetNeighbor() const override { return nbr_iter_.GetNeighbor(); }
  EDATA_T GetData() const override {
    return data_->get_view(nbr_iter_.GetData());
  }

 private:
  PackedNbrIterator<VID_T, size_t, TS_T> nbr_iter_;
  const TypedColumn<EDATA_T>* data_;
};

template <typename VID_T, typename TS_T>
class StringNbrIterator : public NbrIterator<VID_T, std::string_view> {
  using nbr_t = mutable_csr_impl::Nbr<VID_T, size_t, TS_T>;

 public:
  StringNbrIterator(const PackedNbrIterator<VID_T, size_t, TS_T>& nbr_iter,
                    const StringColumn* col)
      : nbr_iter_(nbr_iter), column_(col) {}
  ~StringNbrIterator() {}

  bool IsValid() const override { return nbr_iter_.IsValid(); }

  void Next() override { nbr_iter_.Next(); }

  VID_T GetNeighbor() const override { return nbr_iter_.GetNeighbor(); }

  std::string_view GetData() const override {
    return column_->get_view(nbr_iter_.GetData());
  }

  int estimated_degree() const { return nbr_iter_.estimated_degree(); }

 private:
  PackedNbrIterator<VID_T, size_t, TS_T> nbr_iter_;
  const StringColumn* column_;
};

template <typename VID_T, typename TS_T>
class TableNbrIterator : public NbrIterator<VID_T, Property> {
  using nbr_t = mutable_csr_impl::Nbr<VID_T, size_t, TS_T>;

 public:
  TableNbrIterator(const PackedNbrIterator<VID_T, size_t, TS_T>& nbr_iter,
                   const Table* table)
      : nbr_iter_(nbr_iter), table_(table) {}
  ~TableNbrIterator() {}

  bool IsValid() const override { return nbr_iter_.IsValid(); }

  void Next() override { nbr_iter_.Next(); }

  VID_T GetNeighbor() const override { return nbr_iter_.GetNeighbor(); }

  Property GetData() const override {
    return table_->get_row(nbr_iter_.GetData());
  }

  Property GetData(size_t index) const {
    return table_->columns()[index]->get(nbr_iter_.GetData());
  }

  int estimated_degree() const { return nbr_iter_.estimated_degree(); }

 private:
  PackedNbrIterator<VID_T, size_t, TS_T> nbr_iter_;
  const Table* table_;
};

}  // namespace mutable_csr_view

template <typename VID_T>
class GenericMutableCsrViewBase {
 public:
  virtual std::shared_ptr<GenericNbrIterator<VID_T>> get_generic_basic_edges(
      VID_T src) const = 0;
};

template <typename VID_T, typename EDATA_T>
class MutableCsrViewBase : public GenericMutableCsrViewBase<VID_T> {
 public:
  virtual std::shared_ptr<NbrIterator<VID_T, EDATA_T>> get_basic_edges(
      VID_T src) const = 0;

  std::shared_ptr<GenericNbrIterator<VID_T>> get_generic_basic_edges(
      VID_T src) const override {
    return std::dynamic_pointer_cast<GenericNbrIterator<VID_T>>(
        get_basic_edges(src));
  }
};

}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_MUTABLE_CSR_VIEW_H_
