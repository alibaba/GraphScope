
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_RAW_DATA_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_RAW_DATA_H_

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
#include "core/java/utils.h"

namespace gs {
namespace graphx_raw_data_impl {
static inline std::shared_ptr<vineyard::Object> buildStringArray(
    vineyard::Client& client, std::vector<char>& buffer,
    std::vector<int32_t> offsets) {
  LOG(INFO) << "Building array of size: " << offsets.size();
  using arrow_builder_t =
      typename vineyard::ConvertToArrowType<std::string>::BuilderType;
  arrow_builder_t builder;
  // builder.AppendValues(raw_data);
  ARROW_CHECK_OK(builder.Reserve(offsets.size()));
  ARROW_CHECK_OK(builder.ReserveData(buffer.size()));
  const char* ptr = buffer.data();
  for (auto offset : offsets) {
    builder.UnsafeAppend(ptr, offset);
    ptr += offset;
  }
  LOG(INFO) << "Finish building arrow array";
  using arrow_array_t =
      typename vineyard::ConvertToArrowType<std::string>::ArrayType;
  std::shared_ptr<arrow_array_t> arrow_array;
  ARROW_CHECK_OK(builder.Finish(&arrow_array));
  using vineyard_builder_t =
      typename vineyard::InternalType<std::string>::vineyard_builder_type;
  vineyard_builder_t v6d_builder(client, arrow_array);
  return v6d_builder.Seal(client);
}
}  // namespace graphx_raw_data_impl

/**
 * @brief Temp storage of graphx raw data, used to send message to mpi process.
 *
 * @tparam VD_T
 * @tparam ED_T
 */
template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
class GraphXRawData
    : public vineyard::Registered<GraphXRawData<OID_T, VID_T, VD_T, ED_T>> {
  using vid_t = VID_T;
  using oid_t = OID_T;
  using edata_t = ED_T;
  using vdata_t = VD_T;
  using eid_t = uint64_t;
  using vineyard_edata_array_t = typename vineyard::NumericArray<edata_t>;
  using vineyard_vdata_array_t = typename vineyard::NumericArray<vdata_t>;
  using vineyard_oid_array_t = typename vineyard::NumericArray<oid_t>;
  using arrow_edata_array_t =
      typename vineyard::ConvertToArrowType<edata_t>::ArrayType;
  using arrow_vdata_array_t =
      typename vineyard::ConvertToArrowType<vdata_t>::ArrayType;
  using arrow_oid_array_t =
      typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

 public:
  GraphXRawData() {}
  ~GraphXRawData() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXRawData<OID_T, VID_T, VD_T, ED_T>>{
            new GraphXRawData<OID_T, VID_T, VD_T, ED_T>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->edge_num_ = meta.GetKeyValue<eid_t>("edge_num");
    this->vertex_num_ = meta.GetKeyValue<vid_t>("vertex_num");
    {
      vineyard_edata_array_t array;
      array.Construct(meta.GetMemberMeta("edatas"));
      edatas_ = array.GetArray();
    }
    {
      vineyard_vdata_array_t array;
      array.Construct(meta.GetMemberMeta("vdatas"));
      vdatas_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("oids"));
      oids_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("src_oids"));
      src_oids_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("dst_oids"));
      dst_oids_ = array.GetArray();
    }

    LOG(INFO) << "Finish construct raw data, edge num: " << this->edge_num_
              << ", vertex num: " << this->vertex_num_;
  }

  eid_t GetEdgeNum() { return edge_num_; }

  vid_t GetVertexNum() { return vertex_num_; }
  std::shared_ptr<arrow_vdata_array_t>& GetVdataArray() { return vdatas_; }

  std::shared_ptr<arrow_edata_array_t>& GetEdataArray() { return edatas_; }

  std::shared_ptr<arrow_oid_array_t>& GetOids() { return oids_; }
  std::shared_ptr<arrow_oid_array_t>& GetSrcOids() { return src_oids_; }
  std::shared_ptr<arrow_oid_array_t>& GetDstOids() { return dst_oids_; }

 private:
  eid_t edge_num_;
  vid_t vertex_num_;
  std::shared_ptr<arrow_edata_array_t> edatas_;
  std::shared_ptr<arrow_vdata_array_t> vdatas_;
  std::shared_ptr<arrow_oid_array_t> oids_, src_oids_, dst_oids_;

  template <typename _OID_T, typename _VID_T, typename _VD_T, typename _ED_T>
  friend class GraphXRawDataBuilder;
};

template <typename OID_T, typename VID_T, typename VD_T>
class GraphXRawData<OID_T, VID_T, VD_T, std::string>
    : public vineyard::Registered<
          GraphXRawData<OID_T, VID_T, VD_T, std::string>> {
  using vid_t = VID_T;
  using oid_t = OID_T;
  using edata_t = std::string;
  using vdata_t = VD_T;
  using eid_t = uint64_t;
  using vineyard_vdata_array_t =
      typename vineyard::InternalType<vdata_t>::vineyard_array_type;
  using vineyard_edata_array_t =
      typename vineyard::InternalType<edata_t>::vineyard_array_type;
  using vineyard_oid_array_t = typename vineyard::NumericArray<oid_t>;
  using arrow_edata_array_t =
      typename vineyard::ConvertToArrowType<edata_t>::ArrayType;
  using arrow_vdata_array_t =
      typename vineyard::ConvertToArrowType<vdata_t>::ArrayType;
  using arrow_oid_array_t =
      typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

 public:
  GraphXRawData() {}
  ~GraphXRawData() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXRawData<OID_T, VID_T, VD_T, std::string>>{
            new GraphXRawData<OID_T, VID_T, VD_T, std::string>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->edge_num_ = meta.GetKeyValue<eid_t>("edge_num");
    this->vertex_num_ = meta.GetKeyValue<vid_t>("vertex_num");
    {
      vineyard_edata_array_t array;
      array.Construct(meta.GetMemberMeta("edatas"));
      edatas_ = array.GetArray();
    }
    {
      vineyard_vdata_array_t array;
      array.Construct(meta.GetMemberMeta("vdatas"));
      vdatas_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("oids"));
      oids_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("src_oids"));
      src_oids_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("dst_oids"));
      dst_oids_ = array.GetArray();
    }

    LOG(INFO) << "Finish construct raw data, edge num: " << this->edge_num_
              << ", vertex num: " << this->vertex_num_;
  }

  eid_t GetEdgeNum() { return edge_num_; }

  vid_t GetVertexNum() { return vertex_num_; }
  std::shared_ptr<arrow_vdata_array_t>& GetVdataArray() { return vdatas_; }

  std::shared_ptr<arrow_edata_array_t>& GetEdataArray() { return edatas_; }

  std::shared_ptr<arrow_oid_array_t>& GetOids() { return oids_; }
  std::shared_ptr<arrow_oid_array_t>& GetSrcOids() { return src_oids_; }
  std::shared_ptr<arrow_oid_array_t>& GetDstOids() { return dst_oids_; }

 private:
  eid_t edge_num_;
  vid_t vertex_num_;
  std::shared_ptr<arrow_edata_array_t> edatas_;
  std::shared_ptr<arrow_vdata_array_t> vdatas_;
  std::shared_ptr<arrow_oid_array_t> oids_, src_oids_, dst_oids_;

  template <typename _OID_T, typename _VID_T, typename _VD_T, typename _ED_T>
  friend class GraphXRawDataBuilder;
};

template <typename OID_T, typename VID_T, typename ED_T>
class GraphXRawData<OID_T, VID_T, std::string, ED_T>
    : public vineyard::Registered<
          GraphXRawData<OID_T, VID_T, std::string, ED_T>> {
  using vid_t = VID_T;
  using oid_t = OID_T;
  using vdata_t = std::string;
  using edata_t = ED_T;
  using eid_t = uint64_t;
  using vineyard_vdata_array_t =
      typename vineyard::InternalType<vdata_t>::vineyard_array_type;
  using vineyard_edata_array_t =
      typename vineyard::InternalType<edata_t>::vineyard_array_type;
  using vineyard_oid_array_t = typename vineyard::NumericArray<oid_t>;
  using arrow_edata_array_t =
      typename vineyard::ConvertToArrowType<edata_t>::ArrayType;
  using arrow_vdata_array_t =
      typename vineyard::ConvertToArrowType<vdata_t>::ArrayType;
  using arrow_oid_array_t =
      typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

 public:
  GraphXRawData() {}
  ~GraphXRawData() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXRawData<OID_T, VID_T, std::string, ED_T>>{
            new GraphXRawData<OID_T, VID_T, std::string, ED_T>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->edge_num_ = meta.GetKeyValue<eid_t>("edge_num");
    this->vertex_num_ = meta.GetKeyValue<vid_t>("vertex_num");
    {
      vineyard_edata_array_t array;
      array.Construct(meta.GetMemberMeta("edatas"));
      edatas_ = array.GetArray();
    }
    {
      vineyard_vdata_array_t array;
      array.Construct(meta.GetMemberMeta("vdatas"));
      vdatas_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("oids"));
      oids_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("src_oids"));
      src_oids_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("dst_oids"));
      dst_oids_ = array.GetArray();
    }

    LOG(INFO) << "Finish construct raw data, edge num: " << this->edge_num_
              << ", vertex num: " << this->vertex_num_;
  }

  eid_t GetEdgeNum() { return edge_num_; }

  vid_t GetVertexNum() { return vertex_num_; }
  std::shared_ptr<arrow_vdata_array_t>& GetVdataArray() { return vdatas_; }

  std::shared_ptr<arrow_edata_array_t>& GetEdataArray() { return edatas_; }

  std::shared_ptr<arrow_oid_array_t>& GetOids() { return oids_; }
  std::shared_ptr<arrow_oid_array_t>& GetSrcOids() { return src_oids_; }
  std::shared_ptr<arrow_oid_array_t>& GetDstOids() { return dst_oids_; }

 private:
  eid_t edge_num_;
  vid_t vertex_num_;
  std::shared_ptr<arrow_edata_array_t> edatas_;
  std::shared_ptr<arrow_vdata_array_t> vdatas_;
  std::shared_ptr<arrow_oid_array_t> oids_, src_oids_, dst_oids_;

  template <typename _OID_T, typename _VID_T, typename _VD_T, typename _ED_T>
  friend class GraphXRawDataBuilder;
};

template <typename OID_T, typename VID_T>
class GraphXRawData<OID_T, VID_T, std::string, std::string>
    : public vineyard::Registered<
          GraphXRawData<OID_T, VID_T, std::string, std::string>> {
  using vid_t = VID_T;
  using oid_t = OID_T;
  using vdata_t = std::string;
  using edata_t = std::string;
  using eid_t = uint64_t;
  using vineyard_vdata_array_t =
      typename vineyard::InternalType<vdata_t>::vineyard_array_type;
  using vineyard_edata_array_t =
      typename vineyard::InternalType<edata_t>::vineyard_array_type;
  using vineyard_oid_array_t = typename vineyard::NumericArray<oid_t>;
  using arrow_edata_array_t =
      typename vineyard::ConvertToArrowType<edata_t>::ArrayType;
  using arrow_vdata_array_t =
      typename vineyard::ConvertToArrowType<vdata_t>::ArrayType;
  using arrow_oid_array_t =
      typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

 public:
  GraphXRawData() {}
  ~GraphXRawData() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXRawData<OID_T, VID_T, std::string, std::string>>{
            new GraphXRawData<OID_T, VID_T, std::string, std::string>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->edge_num_ = meta.GetKeyValue<eid_t>("edge_num");
    this->vertex_num_ = meta.GetKeyValue<vid_t>("vertex_num");
    {
      vineyard_edata_array_t array;
      array.Construct(meta.GetMemberMeta("edatas"));
      edatas_ = array.GetArray();
    }
    {
      vineyard_vdata_array_t array;
      array.Construct(meta.GetMemberMeta("vdatas"));
      vdatas_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("oids"));
      oids_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("src_oids"));
      src_oids_ = array.GetArray();
    }
    {
      vineyard_oid_array_t array;
      array.Construct(meta.GetMemberMeta("dst_oids"));
      dst_oids_ = array.GetArray();
    }

    LOG(INFO) << "Finish construct raw data, edge num: " << this->edge_num_
              << ", vertex num: " << this->vertex_num_;
  }

  eid_t GetEdgeNum() { return edge_num_; }

  vid_t GetVertexNum() { return vertex_num_; }
  std::shared_ptr<arrow_vdata_array_t>& GetVdataArray() { return vdatas_; }

  std::shared_ptr<arrow_edata_array_t>& GetEdataArray() { return edatas_; }

  std::shared_ptr<arrow_oid_array_t>& GetOids() { return oids_; }
  std::shared_ptr<arrow_oid_array_t>& GetSrcOids() { return src_oids_; }
  std::shared_ptr<arrow_oid_array_t>& GetDstOids() { return dst_oids_; }

 private:
  eid_t edge_num_;
  vid_t vertex_num_;
  std::shared_ptr<arrow_edata_array_t> edatas_;
  std::shared_ptr<arrow_vdata_array_t> vdatas_;
  std::shared_ptr<arrow_oid_array_t> oids_, src_oids_, dst_oids_;

  template <typename _OID_T, typename _VID_T, typename _VD_T, typename _ED_T>
  friend class GraphXRawDataBuilder;
};

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
class GraphXRawDataBuilder : public vineyard::ObjectBuilder {
  using vid_t = VID_T;
  using oid_t = OID_T;
  using vdata_t = VD_T;
  using edata_t = ED_T;
  using eid_t = uint64_t;
  using edata_array_builder_t = vineyard::NumericArrayBuilder<edata_t>;
  using vdata_array_builder_t = vineyard::NumericArrayBuilder<vdata_t>;
  using oid_array_builder_t = vineyard::NumericArrayBuilder<oid_t>;
  using edata_array_t = vineyard::NumericArray<edata_t>;
  using vdata_array_t = vineyard::NumericArray<vdata_t>;
  using oid_array_t = vineyard::NumericArray<oid_t>;

 public:
  GraphXRawDataBuilder(vineyard::Client& client, std::vector<oid_t>& oids,
                       std::vector<vdata_t>& vdatas,
                       std::vector<oid_t>& src_oids,
                       std::vector<oid_t>& dst_oids,
                       std::vector<edata_t>& edatas) {
    edge_num_ = edatas.size();
    vertex_num_ = vdatas.size();
    CHECK_EQ(edge_num_, src_oids.size());
    CHECK_EQ(edge_num_, dst_oids.size());
    CHECK_EQ(vertex_num_, oids.size());
    {
      this->oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, oids));
      this->src_oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, src_oids));
      this->dst_oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, dst_oids));
      this->vdata_array_ = std::dynamic_pointer_cast<vdata_array_t>(
          buildPrimitiveArray(client, vdatas));
      this->edata_array_ = std::dynamic_pointer_cast<edata_array_t>(
          buildPrimitiveArray(client, edatas));
      LOG(INFO) << "Finish Building all raw data";
    }
  }

  ~GraphXRawDataBuilder() {}

  std::shared_ptr<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>> MySeal(
      vineyard::Client& client) {
    return std::dynamic_pointer_cast<
        GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>(this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
    auto raw_data =
        std::make_shared<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>();
    raw_data->meta_.SetTypeName(
        type_name<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>());

    size_t nBytes = 0;
    raw_data->edatas_ = edata_array_->GetArray();
    raw_data->vdatas_ = vdata_array_->GetArray();
    raw_data->oids_ = oids_->GetArray();
    raw_data->src_oids_ = src_oids_->GetArray();
    raw_data->dst_oids_ = dst_oids_->GetArray();
    raw_data->edge_num_ = edge_num_;
    raw_data->vertex_num_ = vertex_num_;
    raw_data->meta_.AddKeyValue("edge_num", edge_num_);
    raw_data->meta_.AddKeyValue("vertex_num", vertex_num_);
    raw_data->meta_.AddMember("edatas", edata_array_->meta());
    raw_data->meta_.AddMember("vdatas", vdata_array_->meta());
    raw_data->meta_.AddMember("oids", oids_->meta());
    raw_data->meta_.AddMember("src_oids", src_oids_->meta());
    raw_data->meta_.AddMember("dst_oids", dst_oids_->meta());
    nBytes += edata_array_->nbytes();
    nBytes += vdata_array_->nbytes();
    nBytes += oids_->nbytes();
    nBytes += src_oids_->nbytes();
    nBytes += dst_oids_->nbytes();
    raw_data->meta_.SetNBytes(nBytes);
    VINEYARD_CHECK_OK(client.CreateMetaData(raw_data->meta_, raw_data->id_));
    // mark the builder as sealed
    this->set_sealed(true);
    return std::static_pointer_cast<vineyard::Object>(raw_data);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    VLOG(10) << "Finish building raw data;";
    return vineyard::Status::OK();
  }

 private:
  eid_t edge_num_;
  vid_t vertex_num_;
  std::shared_ptr<vdata_array_t> vdata_array_;
  std::shared_ptr<edata_array_t> edata_array_;
  std::shared_ptr<oid_array_t> oids_, src_oids_, dst_oids_;
};

template <typename OID_T, typename VID_T, typename VD_T>
class GraphXRawDataBuilder<OID_T, VID_T, VD_T, std::string>
    : public vineyard::ObjectBuilder {
  using vid_t = VID_T;
  using oid_t = OID_T;
  using vdata_t = VD_T;
  using edata_t = std::string;
  using eid_t = uint64_t;
  using edata_array_builder_t =
      typename vineyard::InternalType<edata_t>::vineyard_builder_type;
  using vdata_array_builder_t =
      typename vineyard::InternalType<vdata_t>::vineyard_builder_type;
  using oid_array_builder_t = vineyard::NumericArrayBuilder<oid_t>;
  using edata_array_t =
      typename vineyard::InternalType<edata_t>::vineyard_array_type;
  using vdata_array_t =
      typename vineyard::InternalType<vdata_t>::vineyard_array_type;
  using oid_array_t = vineyard::NumericArray<oid_t>;

 public:
  GraphXRawDataBuilder(vineyard::Client& client, std::vector<oid_t>& oids,
                       std::vector<vdata_t>& vdatas,
                       std::vector<oid_t>& src_oids,
                       std::vector<oid_t>& dst_oids,
                       std::vector<char>& edata_buffer,
                       std::vector<int32_t>& edata_offsets) {
    edge_num_ = edata_offsets.size();
    vertex_num_ = vdatas.size();
    CHECK_EQ(edge_num_, src_oids.size());
    CHECK_EQ(edge_num_, dst_oids.size());
    CHECK_EQ(vertex_num_, oids.size());
    {
      this->oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, oids));
      this->src_oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, src_oids));
      this->dst_oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, dst_oids));
      this->vdata_array_ = std::dynamic_pointer_cast<vdata_array_t>(
          buildPrimitiveArray(client, vdatas));
      this->edata_array_ = std::dynamic_pointer_cast<edata_array_t>(
          graphx_raw_data_impl::buildStringArray(client, edata_buffer,
                                                 edata_offsets));
      LOG(INFO) << "Finish Building all raw data";
    }
  }

  ~GraphXRawDataBuilder() {}

  std::shared_ptr<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>> MySeal(
      vineyard::Client& client) {
    return std::dynamic_pointer_cast<
        GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>(this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
    auto raw_data =
        std::make_shared<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>();
    raw_data->meta_.SetTypeName(
        type_name<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>());

    size_t nBytes = 0;
    raw_data->edatas_ = edata_array_->GetArray();
    raw_data->vdatas_ = vdata_array_->GetArray();
    raw_data->oids_ = oids_->GetArray();
    raw_data->src_oids_ = src_oids_->GetArray();
    raw_data->dst_oids_ = dst_oids_->GetArray();
    raw_data->edge_num_ = edge_num_;
    raw_data->vertex_num_ = vertex_num_;
    raw_data->meta_.AddKeyValue("edge_num", edge_num_);
    raw_data->meta_.AddKeyValue("vertex_num", vertex_num_);
    raw_data->meta_.AddMember("edatas", edata_array_->meta());
    raw_data->meta_.AddMember("vdatas", vdata_array_->meta());
    raw_data->meta_.AddMember("oids", oids_->meta());
    raw_data->meta_.AddMember("src_oids", src_oids_->meta());
    raw_data->meta_.AddMember("dst_oids", dst_oids_->meta());
    nBytes += edata_array_->nbytes();
    nBytes += vdata_array_->nbytes();
    nBytes += oids_->nbytes();
    nBytes += src_oids_->nbytes();
    nBytes += dst_oids_->nbytes();
    raw_data->meta_.SetNBytes(nBytes);
    VINEYARD_CHECK_OK(client.CreateMetaData(raw_data->meta_, raw_data->id_));
    // mark the builder as sealed
    this->set_sealed(true);
    return std::static_pointer_cast<vineyard::Object>(raw_data);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    VLOG(10) << "Finish building raw data;";
    return vineyard::Status::OK();
  }

 private:
  eid_t edge_num_;
  vid_t vertex_num_;
  std::shared_ptr<vdata_array_t> vdata_array_;
  std::shared_ptr<edata_array_t> edata_array_;
  std::shared_ptr<oid_array_t> oids_, src_oids_, dst_oids_;
};

template <typename OID_T, typename VID_T, typename ED_T>
class GraphXRawDataBuilder<OID_T, VID_T, std::string, ED_T>
    : public vineyard::ObjectBuilder {
  using vid_t = VID_T;
  using oid_t = OID_T;
  using vdata_t = std::string;
  using edata_t = ED_T;
  using eid_t = uint64_t;
  using edata_array_builder_t =
      typename vineyard::InternalType<edata_t>::vineyard_builder_type;
  using vdata_array_builder_t =
      typename vineyard::InternalType<vdata_t>::vineyard_builder_type;
  using oid_array_builder_t = vineyard::NumericArrayBuilder<oid_t>;
  using edata_array_t =
      typename vineyard::InternalType<edata_t>::vineyard_array_type;
  using vdata_array_t =
      typename vineyard::InternalType<vdata_t>::vineyard_array_type;
  using oid_array_t = vineyard::NumericArray<oid_t>;

 public:
  GraphXRawDataBuilder(vineyard::Client& client, std::vector<oid_t>& oids,
                       std::vector<char>& vdata_buffer,
                       std::vector<int32_t>& vdata_offsets,
                       std::vector<oid_t>& src_oids,
                       std::vector<oid_t>& dst_oids,
                       std::vector<edata_t>& edatas) {
    edge_num_ = edatas.size();
    vertex_num_ = vdata_offsets.size();
    CHECK_EQ(edge_num_, src_oids.size());
    CHECK_EQ(edge_num_, dst_oids.size());
    CHECK_EQ(vertex_num_, oids.size());
    {
      this->oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, oids));
      this->src_oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, src_oids));
      this->dst_oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, dst_oids));
      this->vdata_array_ = std::dynamic_pointer_cast<vdata_array_t>(
          graphx_raw_data_impl::buildStringArray(client, vdata_buffer,
                                                 vdata_offsets));
      this->edata_array_ = std::dynamic_pointer_cast<edata_array_t>(
          buildPrimitiveArray(client, edatas));
      LOG(INFO) << "Finish Building all raw data";
    }
  }

  ~GraphXRawDataBuilder() {}

  std::shared_ptr<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>> MySeal(
      vineyard::Client& client) {
    return std::dynamic_pointer_cast<
        GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>(this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
    auto raw_data =
        std::make_shared<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>();
    raw_data->meta_.SetTypeName(
        type_name<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>());

    size_t nBytes = 0;
    raw_data->edatas_ = edata_array_->GetArray();
    raw_data->vdatas_ = vdata_array_->GetArray();
    raw_data->oids_ = oids_->GetArray();
    raw_data->src_oids_ = src_oids_->GetArray();
    raw_data->dst_oids_ = dst_oids_->GetArray();
    raw_data->edge_num_ = edge_num_;
    raw_data->vertex_num_ = vertex_num_;
    raw_data->meta_.AddKeyValue("edge_num", edge_num_);
    raw_data->meta_.AddKeyValue("vertex_num", vertex_num_);
    raw_data->meta_.AddMember("edatas", edata_array_->meta());
    raw_data->meta_.AddMember("vdatas", vdata_array_->meta());
    raw_data->meta_.AddMember("oids", oids_->meta());
    raw_data->meta_.AddMember("src_oids", src_oids_->meta());
    raw_data->meta_.AddMember("dst_oids", dst_oids_->meta());
    nBytes += edata_array_->nbytes();
    nBytes += vdata_array_->nbytes();
    nBytes += oids_->nbytes();
    nBytes += src_oids_->nbytes();
    nBytes += dst_oids_->nbytes();
    raw_data->meta_.SetNBytes(nBytes);
    VINEYARD_CHECK_OK(client.CreateMetaData(raw_data->meta_, raw_data->id_));
    // mark the builder as sealed
    this->set_sealed(true);
    return std::static_pointer_cast<vineyard::Object>(raw_data);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    VLOG(10) << "Finish building raw data;";
    return vineyard::Status::OK();
  }

 private:
  eid_t edge_num_;
  vid_t vertex_num_;
  std::shared_ptr<vdata_array_t> vdata_array_;
  std::shared_ptr<edata_array_t> edata_array_;
  std::shared_ptr<oid_array_t> oids_, src_oids_, dst_oids_;
};

template <typename OID_T, typename VID_T>
class GraphXRawDataBuilder<OID_T, VID_T, std::string, std::string>
    : public vineyard::ObjectBuilder {
  using vid_t = VID_T;
  using oid_t = OID_T;
  using vdata_t = std::string;
  using edata_t = std::string;
  using eid_t = uint64_t;
  using edata_array_builder_t =
      typename vineyard::InternalType<edata_t>::vineyard_builder_type;
  using vdata_array_builder_t =
      typename vineyard::InternalType<vdata_t>::vineyard_builder_type;
  using oid_array_builder_t = vineyard::NumericArrayBuilder<oid_t>;
  using edata_array_t =
      typename vineyard::InternalType<edata_t>::vineyard_array_type;
  using vdata_array_t =
      typename vineyard::InternalType<vdata_t>::vineyard_array_type;
  using oid_array_t = vineyard::NumericArray<oid_t>;

 public:
  GraphXRawDataBuilder(vineyard::Client& client, std::vector<oid_t>& oids,
                       std::vector<char>& vdata_buffer,
                       std::vector<int32_t>& vdata_offsets,
                       std::vector<oid_t>& src_oids,
                       std::vector<oid_t>& dst_oids,
                       std::vector<char>& edata_buffer,
                       std::vector<int32_t>& edata_offsets) {
    edge_num_ = edata_offsets.size();
    vertex_num_ = vdata_offsets.size();
    CHECK_EQ(edge_num_, src_oids.size());
    CHECK_EQ(edge_num_, dst_oids.size());
    CHECK_EQ(vertex_num_, oids.size());
    {
      this->oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, oids));
      this->src_oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, src_oids));
      this->dst_oids_ = std::dynamic_pointer_cast<oid_array_t>(
          buildPrimitiveArray(client, dst_oids));
      this->vdata_array_ = std::dynamic_pointer_cast<vdata_array_t>(
          graphx_raw_data_impl::buildStringArray(client, vdata_buffer,
                                                 vdata_offsets));
      this->edata_array_ = std::dynamic_pointer_cast<edata_array_t>(
          graphx_raw_data_impl::buildStringArray(client, edata_buffer,
                                                 edata_offsets));
      LOG(INFO) << "Finish Building all raw data";
    }
  }

  ~GraphXRawDataBuilder() {}

  std::shared_ptr<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>> MySeal(
      vineyard::Client& client) {
    return std::dynamic_pointer_cast<
        GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>(this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
    auto raw_data =
        std::make_shared<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>();
    raw_data->meta_.SetTypeName(
        type_name<GraphXRawData<oid_t, vid_t, vdata_t, edata_t>>());

    size_t nBytes = 0;
    raw_data->edatas_ = edata_array_->GetArray();
    raw_data->vdatas_ = vdata_array_->GetArray();
    raw_data->oids_ = oids_->GetArray();
    raw_data->src_oids_ = src_oids_->GetArray();
    raw_data->dst_oids_ = dst_oids_->GetArray();
    raw_data->edge_num_ = edge_num_;
    raw_data->vertex_num_ = vertex_num_;
    raw_data->meta_.AddKeyValue("edge_num", edge_num_);
    raw_data->meta_.AddKeyValue("vertex_num", vertex_num_);
    raw_data->meta_.AddMember("edatas", edata_array_->meta());
    raw_data->meta_.AddMember("vdatas", vdata_array_->meta());
    raw_data->meta_.AddMember("oids", oids_->meta());
    raw_data->meta_.AddMember("src_oids", src_oids_->meta());
    raw_data->meta_.AddMember("dst_oids", dst_oids_->meta());
    nBytes += edata_array_->nbytes();
    nBytes += vdata_array_->nbytes();
    nBytes += oids_->nbytes();
    nBytes += src_oids_->nbytes();
    nBytes += dst_oids_->nbytes();
    raw_data->meta_.SetNBytes(nBytes);
    VINEYARD_CHECK_OK(client.CreateMetaData(raw_data->meta_, raw_data->id_));
    // mark the builder as sealed
    this->set_sealed(true);
    return std::static_pointer_cast<vineyard::Object>(raw_data);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    VLOG(10) << "Finish building raw data;";
    return vineyard::Status::OK();
  }

 private:
  eid_t edge_num_;
  vid_t vertex_num_;
  std::shared_ptr<vdata_array_t> vdata_array_;
  std::shared_ptr<edata_array_t> edata_array_;
  std::shared_ptr<oid_array_t> oids_, src_oids_, dst_oids_;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_RAW_DATA_H_
