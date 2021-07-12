#ifndef ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_VERTEX_H_
#define ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_VERTEX_H_

#include <string>

#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/context/labeled_vertex_property_context.h"

namespace gs {

namespace gather_scatter {

template <typename ID_T>
class Vertex {
  using fragment_t = vineyard::ArrowFragment<ID_T, uint64_t>;
  using vertex_t = typename fragment_t::vertex_t;
  using context_t = LabeledVertexPropertyContext<fragment_t>;
  using label_id_t = typename fragment_t::label_id_t;
  using active_array_t = typename fragment_t::template vertex_array_t<bool>;

 public:
  Vertex(const fragment_t& fragment, context_t& context, label_id_t label)
      : fragment_(fragment), context_(context), label_(label) {
    std::shared_ptr<arrow::Table> table = fragment_.vertex_data_table(label_);
    immutable_property_num_ = table->num_columns();
    for (int i = 0; i < immutable_property_num_; ++i) {
      immutable_property_map_.emplace(table->schema()->field(i)->name(), i);
    }
    auto& ctx_vp = context_.vertex_properties()[label_];
    mutable_property_num_ = 0;
    for (auto column : ctx_vp) {
      immutable_property_map_.emplace(
          column->name(), mutable_property_num_ + immutable_property_num_);
      mutable_property_map_.emplace(column->name(), mutable_property_num_);
      ++mutable_property_num_;
    }
  }

  void resetVertex() { vertex_ = fragment_.InnerVertices(label_).begin(); }

  void nextVertex() { ++vertex_; }

  void setActiveArray(active_array_t& active_array) {
    active_array_ = &active_array;
  }

  ID_T GetId() const { return fragment_.GetId(); }

  int GetLabel() const { return label_; }

  template <typename T>
  T GetData(const std::string& name) const {
    auto iter = immutable_property_map_.find(name);
    if (iter != immutable_property_map_.end()) {
      int index = iter->second;
      return index < immutable_property_num_
                 ? fragment_.template GetData<T>(vertex_, index)
                 : context_
                       .template get_typed_column<T>(
                           label_, index - immutable_property_num_)
                       ->at(vertex_);
    } else {
      return T();
    }
  }

  template <typename T>
  void SetData(const std::string& name, const T& value) {
    auto iter = mutable_property_map_.find(name);
    if (iter != mutable_property_map_.end()) {
      int index = iter->second;
      context_.template get_typed_column<T>(label_, index)->at(vertex_) = value;
    } else {
      addColumn<T>(name);
      context_.template get_typed_column<T>(label_, mutable_property_num_ - 1)
          ->at(vertex_) = value;
    }
  }

  int InDegree() const {
    int id = 0;
    label_id_t e_label_num = fragment_.edge_label_num();
    for (label_id_t i = 0; i < e_label_num; ++i) {
      id += fragment_.GetLocalInDegree(vertex_, i);
    }
    return id;
  }

  int OutDegree() const {
    int od = 0;
    label_id_t e_label_num = fragment_.edge_label_num();
    for (label_id_t i = 0; i < e_label_num; ++i) {
      od += fragment_.GetLocalOutDegree(vertex_, i);
    }
    return od;
  }

  void SetActive(bool t) { (*active_array_)[vertex_] = t; }

  bool IsActive() const { return (*active_array_)[vertex_]; }

 private:
  template <typename T>
  void addColumn(const std::string& name) {
    int index = context_.add_column(label_, name, ContextTypeToEnum<T>::value);
    ++mutable_property_num_;
    CHECK_EQ(index, mutable_property_num_);
    immutable_property_map_.emplace(name, index + immutable_property_num_);
    mutable_property_map_.emplace(name, index);
  }

  const fragment_t& fragment_;
  context_t& context_;
  vertex_t vertex_;
  label_id_t label_;

  std::map<std::string, int> immutable_property_map_;
  int immutable_property_num_;
  std::map<std::string, int> mutable_property_map_;
  int mutable_property_num_;

  active_array_t* active_array_;
};

}  // namespace gather_scatter

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_VERTEX_H_
