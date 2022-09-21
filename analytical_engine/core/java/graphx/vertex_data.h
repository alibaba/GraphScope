
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_DATA_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_DATA_H_

#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"

#include "grape/grape.h"
#include "grape/utils/vertex_array.h"
#include "vineyard/basic/ds/array.h"
#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/client/client.h"

#include "core/config.h"
#include "core/fragment/arrow_projected_fragment.h"

namespace gs {
template <typename VID_T, typename VD_T>
class VertexData : public vineyard::Registered<VertexData<VID_T, VD_T>> {
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vdata_array_t = typename vineyard::Array<vdata_t>;
  using vertex_t = grape::Vertex<VID_T>;

 public:
  VertexData() {}
  ~VertexData() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<VertexData<VID_T, VD_T>>{
            new VertexData<VID_T, VD_T>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->frag_vnums_ = meta.GetKeyValue<fid_t>("frag_vnums");
    vdatas_.Construct(meta.GetMemberMeta("vdatas"));

    vdatas_accessor_.Init(vdatas_);
    VLOG(10) << "Finish construct vertex data, frag vnums: " << frag_vnums_;
  }

  vid_t VerticesNum() { return frag_vnums_; }

  VD_T GetData(const vid_t& lid) { return vdatas_accessor_[lid]; }

  VD_T GetData(const vertex_t& v) { return GetData(v.GetValue()); }

  gs::arrow_projected_fragment_impl::TypedArray<vdata_t>& GetVdataArray() {
    return vdatas_accessor_;
  }

 private:
  vid_t frag_vnums_;
  vdata_array_t vdatas_;
  gs::arrow_projected_fragment_impl::TypedArray<vdata_t> vdatas_accessor_;

  template <typename _VID_T, typename _VD_T>
  friend class VertexDataBuilder;
};

template <typename VID_T>
class VertexData<VID_T, std::string>
    : public vineyard::Registered<VertexData<VID_T, std::string>> {
  using vid_t = VID_T;
  using vdata_t = std::string;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;

  using vdata_array_t = arrow::LargeStringArray;
  using vertex_t = grape::Vertex<VID_T>;

 public:
  VertexData() {}
  ~VertexData() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<VertexData<VID_T, std::string>>{
            new VertexData<VID_T, std::string>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->frag_vnums_ = meta.GetKeyValue<fid_t>("frag_vnums");
    VLOG(10) << "frag_vnums: " << frag_vnums_;
    {
      vineyard::LargeStringArray vineyard_array;
      vineyard_array.Construct(meta.GetMemberMeta("vdatas"));
      vdatas_ = vineyard_array.GetArray();
    }

    vdatas_accessor_.Init(vdatas_);
    VLOG(10) << "Finish construct vertex data, frag vnums: " << frag_vnums_;
  }

  vid_t VerticesNum() { return frag_vnums_; }

  arrow::util::string_view GetData(const vid_t& lid) {
    return vdatas_->GetView(lid);
  }

  arrow::util::string_view GetData(const vertex_t& v) {
    return vdatas_->GetView(v.GetValue());
  }

  gs::arrow_projected_fragment_impl::TypedArray<vdata_t>& GetVdataArray() {
    return vdatas_accessor_;
  }

 private:
  vid_t frag_vnums_;
  std::shared_ptr<vdata_array_t> vdatas_;
  gs::arrow_projected_fragment_impl::TypedArray<std::string> vdatas_accessor_;

  template <typename _VID_T, typename _VD_T>
  friend class VertexDataBuilder;
};

template <typename VID_T, typename VD_T>
class VertexDataBuilder : public vineyard::ObjectBuilder {
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using vdata_array_builder_t = vineyard::ArrayBuilder<vdata_t>;
  using vdata_array_t = vineyard::Array<vdata_t>;

 public:
  VertexDataBuilder(vineyard::Client& client, vid_t frag_vnums,
                    vdata_t init_value)
      : frag_vnums_(frag_vnums), vdata_builder_(client, frag_vnums) {
    for (int64_t i = 0; i < static_cast<int64_t>(frag_vnums); ++i) {
      vdata_builder_[i] = init_value;
    }
    VLOG(10) << "Create vertex data size: " << frag_vnums
             << "init value: " << init_value;
  }
  VertexDataBuilder(vineyard::Client& client, vid_t frag_vnums)
      : frag_vnums_(frag_vnums), vdata_builder_(client, frag_vnums) {
    VLOG(10) << "Create vertex data size: " << frag_vnums
             << " with no init value";
  }

  VertexDataBuilder(vineyard::Client& client, std::vector<VD_T>& vec)
      : frag_vnums_(vec.size()), vdata_builder_(client, vec) {
    VLOG(10) << "Create vertex data with array " << vec.size();
  }
  ~VertexDataBuilder() {}

  vdata_array_builder_t& GetArrayBuilder() { return vdata_builder_; }

  std::shared_ptr<VertexData<vid_t, vdata_t>> MySeal(vineyard::Client& client) {
    return std::dynamic_pointer_cast<VertexData<vid_t, vdata_t>>(
        this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
    auto vertex_data = std::make_shared<VertexData<vid_t, vdata_t>>();
    vertex_data->meta_.SetTypeName(type_name<VertexData<vid_t, vdata_t>>());

    size_t nBytes = 0;
    vertex_data->vdatas_ = *vdata_array_.get();
    vertex_data->frag_vnums_ = frag_vnums_;
    vertex_data->vdatas_accessor_.Init(vertex_data->vdatas_);
    vertex_data->meta_.AddKeyValue("frag_vnums", frag_vnums_);
    vertex_data->meta_.AddMember("vdatas", vdata_array_->meta());
    nBytes += vdata_array_->nbytes();
    VLOG(10) << "total bytes: " << nBytes;
    vertex_data->meta_.SetNBytes(nBytes);
    VINEYARD_CHECK_OK(
        client.CreateMetaData(vertex_data->meta_, vertex_data->id_));
    // mark the builder as sealed
    this->set_sealed(true);
    return std::static_pointer_cast<vineyard::Object>(vertex_data);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    vdata_array_ =
        std::dynamic_pointer_cast<vdata_array_t>(vdata_builder_.Seal(client));
    VLOG(10) << "Finish building vertex data;";
    return vineyard::Status::OK();
  }

 private:
  vid_t frag_vnums_;
  std::shared_ptr<vdata_array_t> vdata_array_;
  vdata_array_builder_t vdata_builder_;
};

template <typename VID_T>
class VertexDataBuilder<VID_T, std::string> : public vineyard::ObjectBuilder {
  using vid_t = VID_T;
  using vdata_array_builder_t = arrow::LargeStringBuilder;
  using vdata_array_t = arrow::LargeStringArray;
  using bitset_words_t =
      typename vineyard::ConvertToArrowType<int64_t>::ArrayType;
  using bitset_words_builder_t =
      typename vineyard::ConvertToArrowType<int64_t>::BuilderType;

 public:
  VertexDataBuilder() {}
  ~VertexDataBuilder() {}

  void Init(vid_t frag_vnums, std::vector<char>& vdata_buffer,
            std::vector<int32_t>& offsets) {
    this->frag_vnums_ = frag_vnums;
    vdata_array_builder_t builder;
    VLOG(10) << "Vdata buffer has " << vdata_buffer.size() << " bytes";
    builder.Reserve(vdata_buffer.size());
    const char* ptr = vdata_buffer.data();
    for (auto offset : offsets) {
      builder.UnsafeAppend(ptr, offset);
      ptr += offset;
    }
    builder.Finish(&vdata_array_);
    VLOG(10) << "Init vertex data with " << frag_vnums_;
  }

  std::shared_ptr<VertexData<vid_t, std::string>> MySeal(
      vineyard::Client& client) {
    return std::dynamic_pointer_cast<VertexData<vid_t, std::string>>(
        this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
    auto vertex_data = std::make_shared<VertexData<vid_t, std::string>>();
    vertex_data->meta_.SetTypeName(type_name<VertexData<vid_t, std::string>>());

    size_t nBytes = 0;
    vertex_data->vdatas_ = vineyard_array.GetArray();
    vertex_data->frag_vnums_ = frag_vnums_;
    vertex_data->vdatas_accessor_.Init(vertex_data->vdatas_);
    vertex_data->meta_.AddKeyValue("frag_vnums", frag_vnums_);
    vertex_data->meta_.AddMember("vdatas", vineyard_array.meta());
    nBytes += vineyard_array.nbytes();
    VLOG(10) << "total bytes: " << nBytes;
    vertex_data->meta_.SetNBytes(nBytes);
    VINEYARD_CHECK_OK(
        client.CreateMetaData(vertex_data->meta_, vertex_data->id_));
    // mark the builder as sealed
    this->set_sealed(true);
    return std::static_pointer_cast<vineyard::Object>(vertex_data);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    vineyard::LargeStringArrayBuilder vdata_builder(client, this->vdata_array_);
    vineyard_array = *std::dynamic_pointer_cast<vineyard::LargeStringArray>(
        vdata_builder.Seal(client));

    VLOG(10) << "Finish building vertex data;";
    return vineyard::Status::OK();
  }

 private:
  vid_t frag_vnums_;
  std::shared_ptr<vdata_array_t> vdata_array_;
  vineyard::LargeStringArray vineyard_array;
};

template <typename VID_T, typename VD_T>
class VertexDataGetter {
  using vid_t = VID_T;
  using vdata_t = VD_T;

 public:
  VertexDataGetter() {}
  ~VertexDataGetter() {}
  std::shared_ptr<VertexData<vid_t, vdata_t>> Get(vineyard::Client& client,
                                                  vineyard::ObjectID id) {
    auto vertexData = std::dynamic_pointer_cast<VertexData<vid_t, vdata_t>>(
        client.GetObject(id));
    VLOG(10) << "Got VertexData: " << id
             << " frag vnum: " << vertexData->VerticesNum();
    return vertexData;
  }
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_DATA_H_
