
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_EDGE_DATA_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_EDGE_DATA_H_

#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"

#include "grape/grape.h"
#include "grape/utils/vertex_array.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/array.h"
#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/common/util/typename.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"
#include "vineyard/graph/utils/error.h"

#include "core/error.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/io/property_parser.h"
#include "core/java/graphx/local_vertex_map.h"

namespace gs {
template <typename VID_T, typename ED_T>
class EdgeData : public vineyard::Registered<EdgeData<VID_T, ED_T>> {
  using vid_t = VID_T;
  using edata_t = ED_T;
  using eid_t = uint64_t;
  using edata_array_t = typename vineyard::Array<edata_t>;
  using vertex_t = grape::Vertex<VID_T>;

 public:
  EdgeData() {}
  ~EdgeData() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<EdgeData<VID_T, ED_T>>{new EdgeData<VID_T, ED_T>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->edge_num_ = meta.GetKeyValue<eid_t>("edge_num");
    edatas_.Construct(meta.GetMemberMeta("edatas"));

    edatas_accessor_.Init(edatas_);
    VLOG(10) << "Finish construct edge data, edge num: " << edge_num_;
  }

  ED_T GetEdgeDataByEid(const eid_t& eid) { return edatas_accessor_[eid]; }

  eid_t GetEdgeNum() { return edge_num_; }

  gs::arrow_projected_fragment_impl::TypedArray<edata_t>& GetEdataArray() {
    return edatas_accessor_;
  }

 private:
  eid_t edge_num_;
  edata_array_t edatas_;
  gs::arrow_projected_fragment_impl::TypedArray<edata_t> edatas_accessor_;

  template <typename _VID_T, typename _ED_T>
  friend class EdgeDataBuilder;
};

template <typename VID_T>
class EdgeData<VID_T, std::string>
    : public vineyard::Registered<EdgeData<VID_T, std::string>> {
  using vid_t = VID_T;
  using edata_t = std::string;
  using eid_t = uint64_t;
  using edata_array_t = arrow::LargeStringArray;
  using vertex_t = grape::Vertex<VID_T>;

 public:
  EdgeData() {}
  ~EdgeData() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<EdgeData<VID_T, std::string>>{
            new EdgeData<VID_T, std::string>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->edge_num_ = meta.GetKeyValue<eid_t>("edge_num");
    vineyard::LargeStringArray vineyard_array;
    vineyard_array.Construct(meta.GetMemberMeta("edatas"));
    edatas_ = vineyard_array.GetArray();

    edatas_accessor_.Init(edatas_);
    VLOG(10) << "Finish construct edge data, edge nums: " << edge_num_;
  }

  arrow::util::string_view GetEdgeDataByEid(const eid_t& eid) {
    return edatas_->GetView(eid);
  }

  eid_t GetEdgeNum() { return edge_num_; }

  gs::arrow_projected_fragment_impl::TypedArray<edata_t>& GetEdataArray() {
    return edatas_accessor_;
  }

 private:
  eid_t edge_num_;
  std::shared_ptr<edata_array_t> edatas_;
  gs::arrow_projected_fragment_impl::TypedArray<std::string> edatas_accessor_;

  template <typename _VID_T, typename _ED_T>
  friend class EdgeDataBuilder;
};

template <typename VID_T, typename ED_T>
class EdgeDataBuilder : public vineyard::ObjectBuilder {
  using vid_t = VID_T;
  using edata_t = ED_T;
  using eid_t = uint64_t;
  using edata_array_builder_t = vineyard::ArrayBuilder<edata_t>;
  using edata_array_t = vineyard::Array<edata_t>;

 public:
  EdgeDataBuilder(vineyard::Client& client, std::vector<ED_T>& edata_array)
      : edata_builder_(client, edata_array) {
    edge_num_ = edata_array.size();
  }
  EdgeDataBuilder(vineyard::Client& client, size_t size)
      : edata_builder_(client, size) {
    edge_num_ = edata_builder_.size();
  }

  ~EdgeDataBuilder() {}

  edata_array_builder_t& GetArrayBuilder() { return edata_builder_; }

  std::shared_ptr<EdgeData<vid_t, edata_t>> MySeal(vineyard::Client& client) {
    return std::dynamic_pointer_cast<EdgeData<vid_t, edata_t>>(
        this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
    auto edge_data = std::make_shared<EdgeData<vid_t, edata_t>>();
    edge_data->meta_.SetTypeName(type_name<EdgeData<vid_t, edata_t>>());

    size_t nBytes = 0;
    edge_data->edatas_ = *edata_array_.get();
    edge_data->edge_num_ = edge_num_;
    edge_data->edatas_accessor_.Init(edge_data->edatas_);
    edge_data->meta_.AddKeyValue("edge_num", edge_num_);
    edge_data->meta_.AddMember("edatas", edata_array_->meta());
    nBytes += edata_array_->nbytes();
    edge_data->meta_.SetNBytes(nBytes);
    VINEYARD_CHECK_OK(client.CreateMetaData(edge_data->meta_, edge_data->id_));
    // mark the builder as sealed
    this->set_sealed(true);
    return std::static_pointer_cast<vineyard::Object>(edge_data);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    edata_array_ =
        std::dynamic_pointer_cast<edata_array_t>(edata_builder_.Seal(client));
    VLOG(10) << "Finish building edge data;";
    return vineyard::Status::OK();
  }

 private:
  eid_t edge_num_;
  edata_array_builder_t edata_builder_;
  std::shared_ptr<edata_array_t> edata_array_;
};  // namespace gs

template <typename VID_T>
class EdgeDataBuilder<VID_T, std::string> : public vineyard::ObjectBuilder {
  using vid_t = VID_T;
  using eid_t = uint64_t;
  using edata_array_builder_t = arrow::LargeStringBuilder;
  using edata_array_t = arrow::LargeStringArray;

 public:
  EdgeDataBuilder() {}
  ~EdgeDataBuilder() {}

  void Init(eid_t edge_num, std::vector<char>& edata_buffer,
            std::vector<int32_t>& lengths) {
    this->edge_num_ = edge_num;
    edata_array_builder_t builder;
    builder.Reserve(edge_num_);
    builder.ReserveData(edata_buffer.size());
    const char* ptr = edata_buffer.data();
    for (auto len : lengths) {
      builder.UnsafeAppend(ptr, len);
      ptr += len;
    }
    builder.Finish(&edata_array_);
    VLOG(10) << "Init edge data, num edges: " << edge_num_;
  }

  std::shared_ptr<EdgeData<vid_t, std::string>> MySeal(
      vineyard::Client& client) {
    return std::dynamic_pointer_cast<EdgeData<vid_t, std::string>>(
        this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
    auto edge_data = std::make_shared<EdgeData<vid_t, std::string>>();
    edge_data->meta_.SetTypeName(type_name<EdgeData<vid_t, std::string>>());

    size_t nBytes = 0;
    edge_data->edatas_ = vineyard_array.GetArray();
    edge_data->edge_num_ = edge_num_;
    edge_data->edatas_accessor_.Init(edge_data->edatas_);
    edge_data->meta_.AddKeyValue("edge_num", edge_num_);
    edge_data->meta_.AddMember("edatas", vineyard_array.meta());
    nBytes += vineyard_array.nbytes();
    edge_data->meta_.SetNBytes(nBytes);
    VINEYARD_CHECK_OK(client.CreateMetaData(edge_data->meta_, edge_data->id_));
    // mark the builder as sealed
    this->set_sealed(true);
    return std::static_pointer_cast<vineyard::Object>(edge_data);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    vineyard::LargeStringArrayBuilder edata_builder(client, this->edata_array_);
    vineyard_array = *std::dynamic_pointer_cast<vineyard::LargeStringArray>(
        edata_builder.Seal(client));
    VLOG(10) << "Finish building edge data;";
    return vineyard::Status::OK();
  }

 private:
  eid_t edge_num_;
  std::shared_ptr<edata_array_t> edata_array_;
  vineyard::LargeStringArray vineyard_array;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_EDGE_DATA_H_
