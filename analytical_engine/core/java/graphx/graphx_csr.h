
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_CSR_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_CSR_H_

#define WITH_PROFILING
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"

#include "grape/grape.h"
#include "grape/graph/immutable_csr.h"
#include "grape/utils/bitset.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/graph/fragment/property_graph_utils.h"
#include "vineyard/graph/utils/error.h"
#include "vineyard/io/io/i_io_adaptor.h"
#include "vineyard/io/io/io_factory.h"

#include "core/config.h"
#include "core/error.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/io/property_parser.h"
#include "core/java/graphx/graphx_vertex_map.h"
/**
 * @brief Defines the RDD of edges. when data is feed into this, we assume it is
 * already shuffle and partitioned.
 *
 */
namespace gs {
struct int64_atomic {
  std::atomic<int64_t> atomic_{0};
  int64_atomic() : atomic_{0} {};
  int64_atomic(const int64_atomic& other) : atomic_(other.atomic_.load()) {}
  int64_atomic& operator=(const int64_atomic& other) {
    atomic_.store(other.atomic_.load());
    return *this;
  }
};

template <typename VID_T>
class GraphXCSR : public vineyard::Registered<GraphXCSR<VID_T>> {
 public:
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using vid_t = VID_T;
  using vineyard_offset_array_t =
      typename vineyard::InternalType<int64_t>::vineyard_array_type;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using nbr_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using vineyard_edges_array_t = vineyard::FixedSizeBinaryArray;

  GraphXCSR() {}
  ~GraphXCSR() {}

  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXCSR<VID_T>>{new GraphXCSR<VID_T>()});
  }

  int64_t GetInDegree(vid_t lid) {
    return GetIEOffset(lid + 1) - GetIEOffset(lid);
  }

  int64_t GetOutDegree(vid_t lid) {
    return GetOEOffset(lid + 1) - GetOEOffset(lid);
  }
  bool IsIEEmpty(vid_t lid) { return GetIEOffset(lid + 1) == GetIEOffset(lid); }
  bool IsOEEmpty(vid_t lid) { return GetOEOffset(lid + 1) == GetOEOffset(lid); }

  nbr_t* GetIEBegin(VID_T i) { return &in_edge_ptr_[GetIEOffset(i)]; }
  nbr_t* GetOEBegin(VID_T i) { return &out_edge_ptr_[GetOEOffset(i)]; }

  nbr_t* GetIEEnd(VID_T i) { return &in_edge_ptr_[GetIEOffset(i + 1)]; }
  nbr_t* GetOEEnd(VID_T i) { return &out_edge_ptr_[GetOEOffset(i + 1)]; }

  // inner verticesNum
  vid_t VertexNum() const { return local_vnum_; }

  int64_t GetInEdgesNum() const { return in_edges_num_; }

  int64_t GetOutEdgesNum() const { return out_edges_num_; }

  int64_t GetTotalEdgesNum() const { return total_edge_num_; }

  int64_t GetPartialInEdgesNum(vid_t from, vid_t end) const {
    return ie_offsets_->Value(static_cast<int64_t>(end)) -
           ie_offsets_->Value(static_cast<int64_t>(from));
  }
  int64_t GetPartialOutEdgesNum(vid_t from, vid_t end) const {
    return oe_offsets_->Value(static_cast<int64_t>(end)) -
           oe_offsets_->Value(static_cast<int64_t>(from));
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->total_edge_num_ = meta.GetKeyValue<eid_t>("total_edge_num");
    {
      vineyard_edges_array_t v6d_edges;
      v6d_edges.Construct(meta.GetMemberMeta("in_edges"));
      in_edges_ = v6d_edges.GetArray();
    }
    {
      vineyard_edges_array_t v6d_edges;
      v6d_edges.Construct(meta.GetMemberMeta("out_edges"));
      out_edges_ = v6d_edges.GetArray();
    }

    {
      vineyard_offset_array_t array;
      array.Construct(meta.GetMemberMeta("ie_offsets"));
      ie_offsets_ = array.GetArray();
      ie_offsets_accessor_.Init(ie_offsets_);
    }
    {
      vineyard_offset_array_t array;
      array.Construct(meta.GetMemberMeta("oe_offsets"));
      oe_offsets_ = array.GetArray();
      oe_offsets_accessor_.Init(oe_offsets_);
    }

    local_vnum_ = ie_offsets_->length() - 1;
    CHECK_GT(local_vnum_, 0);
    VLOG(10) << "In constructing graphx csr, local vnum: " << local_vnum_;
    out_edge_ptr_ = const_cast<nbr_t*>(
        reinterpret_cast<const nbr_t*>(out_edges_->GetValue(0)));
    in_edge_ptr_ = const_cast<nbr_t*>(
        reinterpret_cast<const nbr_t*>(in_edges_->GetValue(0)));
    in_edges_num_ = GetIEOffset(local_vnum_);
    out_edges_num_ = GetOEOffset(local_vnum_);
    VLOG(10) << "total in edges: " << in_edges_num_
             << " out edges : " << out_edges_num_;
    VLOG(10) << "Finish construct GraphXCSR: ";
  }
  inline int64_t GetIEOffset(vid_t lid) {
    return ie_offsets_->Value(static_cast<int64_t>(lid));
  }
  inline int64_t GetOEOffset(vid_t lid) {
    return oe_offsets_->Value(static_cast<int64_t>(lid));
  }

  inline gs::arrow_projected_fragment_impl::TypedArray<int64_t>&
  GetIEOffsetArray() {
    return ie_offsets_accessor_;
  }
  inline gs::arrow_projected_fragment_impl::TypedArray<int64_t>&
  GetOEOffsetArray() {
    return oe_offsets_accessor_;
  }

 private:
  vid_t local_vnum_;
  eid_t total_edge_num_;
  int64_t in_edges_num_, out_edges_num_;
  nbr_t *in_edge_ptr_, *out_edge_ptr_;
  std::shared_ptr<arrow::FixedSizeBinaryArray> in_edges_, out_edges_;
  std::shared_ptr<arrow::Int64Array> ie_offsets_, oe_offsets_;
  gs::arrow_projected_fragment_impl::TypedArray<int64_t> ie_offsets_accessor_,
      oe_offsets_accessor_;

  template <typename _VID_T>
  friend class GraphXCSRBuilder;
};

template <typename VID_T>
class GraphXCSRBuilder : public vineyard::ObjectBuilder {
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using vid_t = VID_T;
  using nbr_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;

 public:
  explicit GraphXCSRBuilder(vineyard::Client& client) : client_(client) {}

  void SetInEdges(const vineyard::FixedSizeBinaryArray& edges) {
    this->in_edges = edges;
  }
  void SetOutEdges(const vineyard::FixedSizeBinaryArray& edges) {
    this->out_edges = edges;
  }
  void SetIEOffsetArray(const vineyard::NumericArray<int64_t>& array) {
    this->ie_offsets = array;
  }
  void SetOEOffsetArray(const vineyard::NumericArray<int64_t>& array) {
    this->oe_offsets = array;
  }
  void SetTotalEdgesNum(eid_t edge_num) { this->total_edge_num_ = edge_num; }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));

    auto graphx_csr = std::make_shared<GraphXCSR<vid_t>>();
    graphx_csr->meta_.SetTypeName(type_name<GraphXCSR<vid_t>>());

    size_t nBytes = 0;
    graphx_csr->total_edge_num_ = total_edge_num_;
    graphx_csr->ie_offsets_ = ie_offsets.GetArray();
    graphx_csr->ie_offsets_accessor_.Init(graphx_csr->ie_offsets_);
    nBytes += ie_offsets.nbytes();
    graphx_csr->oe_offsets_ = oe_offsets.GetArray();
    graphx_csr->oe_offsets_accessor_.Init(graphx_csr->oe_offsets_);
    nBytes += oe_offsets.nbytes();
    graphx_csr->in_edges_ = in_edges.GetArray();
    nBytes += in_edges.nbytes();
    graphx_csr->out_edges_ = out_edges.GetArray();
    nBytes += out_edges.nbytes();
    VLOG(10) << "total bytes: " << nBytes;

    graphx_csr->in_edge_ptr_ = const_cast<nbr_t*>(
        reinterpret_cast<const nbr_t*>(graphx_csr->in_edges_->GetValue(0)));
    graphx_csr->out_edge_ptr_ = const_cast<nbr_t*>(
        reinterpret_cast<const nbr_t*>(graphx_csr->out_edges_->GetValue(0)));
    graphx_csr->local_vnum_ = graphx_csr->ie_offsets_->length() - 1;
    graphx_csr->in_edges_num_ =
        graphx_csr->GetIEOffset(graphx_csr->local_vnum_);
    graphx_csr->out_edges_num_ =
        graphx_csr->GetOEOffset(graphx_csr->local_vnum_);

    graphx_csr->meta_.AddMember("in_edges", in_edges.meta());
    graphx_csr->meta_.AddMember("out_edges", out_edges.meta());

    graphx_csr->meta_.AddMember("ie_offsets", ie_offsets.meta());
    graphx_csr->meta_.AddMember("oe_offsets", oe_offsets.meta());
    graphx_csr->meta_.AddKeyValue("total_edge_num", total_edge_num_);
    graphx_csr->meta_.SetNBytes(nBytes);

    VINEYARD_CHECK_OK(
        client.CreateMetaData(graphx_csr->meta_, graphx_csr->id_));
    this->set_sealed(true);

    return std::static_pointer_cast<vineyard::Object>(graphx_csr);
  }

 private:
  eid_t total_edge_num_;
  vineyard::Client& client_;
  vineyard::FixedSizeBinaryArray in_edges, out_edges;
  vineyard::NumericArray<int64_t> ie_offsets, oe_offsets;
};

template <typename OID_T, typename VID_T>
class BasicGraphXCSRBuilder : public GraphXCSRBuilder<VID_T> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using nbr_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using offset_array_builder_t =
      typename vineyard::ConvertToArrowType<int64_t>::BuilderType;
  using vineyard_offset_array_builder_t =
      typename vineyard::InternalType<int64_t>::vineyard_builder_type;
  using offset_array_t =
      typename vineyard::ConvertToArrowType<int64_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;
  using vid_array_builder_t =
      typename vineyard::ConvertToArrowType<vid_t>::BuilderType;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

 public:
  explicit BasicGraphXCSRBuilder(vineyard::Client& client)
      : GraphXCSRBuilder<vid_t>(client) {}

  boost::leaf::result<void> LoadEdges(
      std::vector<OID_T>& srcOids, std::vector<OID_T>& dstOids,
      GraphXVertexMap<oid_t, vid_t>& graphx_vertex_map, int local_num) {
    auto edges_num_ = srcOids.size();
    return LoadEdgesImpl(srcOids, dstOids, edges_num_, graphx_vertex_map,
                         local_num);
  }

  boost::leaf::result<void> LoadEdgesImpl(
      std::vector<OID_T>& src_oid_ptr, std::vector<OID_T>& dst_oid_ptr,
      int64_t edges_num_, GraphXVertexMap<oid_t, vid_t>& graphx_vertex_map,
      int local_num) {
    this->total_edge_num_ = edges_num_;
    vnum_ = graphx_vertex_map.GetInnerVertexSize();
    std::vector<vid_t> srcLids(edges_num_), dstLids(edges_num_);

    int thread_num =
        (std::thread::hardware_concurrency() + local_num - 1) / local_num;
    // int thread_num = 1;
    int64_t chunk_size = 8192;
    int64_t num_chunks = (edges_num_ + chunk_size - 1) / chunk_size;
    VLOG(10) << "edges nun: " << edges_num_ << " thread num " << thread_num
             << ", chunk size: " << chunk_size << "num chunks " << num_chunks;
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    {
      // int thread_num = 1;
      std::atomic<int> current_chunk(0);
      std::vector<std::thread> work_threads(thread_num);
      std::vector<int> cnt(thread_num);
      for (int i = 0; i < thread_num; ++i) {
        work_threads[i] = std::thread(
            [&](int tid) {
              int got;
              int64_t begin, end;
              while (true) {
                got = current_chunk.fetch_add(1, std::memory_order_relaxed);
                if (got >= num_chunks) {
                  break;
                }
                begin = std::min(edges_num_, got * chunk_size);
                end = std::min(edges_num_, begin + chunk_size);

                for (auto cur = begin; cur < end; ++cur) {
                  auto src_lid = graphx_vertex_map.GetLid(src_oid_ptr[cur]);
                  srcLids[cur] = src_lid;
                }
                for (auto cur = begin; cur < end; ++cur) {
                  auto dst_lid = graphx_vertex_map.GetLid(dst_oid_ptr[cur]);
                  dstLids[cur] = dst_lid;
                }
                cnt[tid] += (end - begin);
              }
            },
            i);
      }
      for (auto& thrd : work_threads) {
        thrd.join();
      }
    }
#if defined(WITH_PROFILING)
    auto build_lid_time = grape::GetCurrentTime();
    VLOG(10) << "Finish building lid arra edges cost"
             << (build_lid_time - start_ts) << " seconds";
#endif

    ie_degree_.resize(vnum_);
    oe_degree_.resize(vnum_);
    for (vid_t i = 0; i < vnum_; ++i) {
      ie_degree_[i].atomic_ = 0;
      oe_degree_[i].atomic_ = 0;
    }

    grape::Bitset in_edge_active, out_edge_active;
    build_degree_and_active(in_edge_active, out_edge_active, srcLids, dstLids,
                            edges_num_, thread_num, chunk_size, num_chunks);
#if defined(WITH_PROFILING)
    auto degree_time = grape::GetCurrentTime();
    VLOG(10) << "Finish building degree time cost"
             << (degree_time - build_lid_time) << " seconds";
#endif
    VLOG(10) << "Loading edges size " << edges_num_
             << "vertices num: " << vnum_;
    build_offsets();
    add_edges(vnum_, graphx_vertex_map.GetVertexSize(), local_num, srcLids,
              dstLids, in_edge_active, out_edge_active, edges_num_, chunk_size,
              num_chunks, thread_num);
    sort(thread_num);
    return {};
  }

  void build_degree_and_active(grape::Bitset& in_edge_active,
                               grape::Bitset& out_edge_active,
                               std::vector<vid_t>& srcLids,
                               std::vector<vid_t>& dstLids, int64_t edges_num_,
                               int thread_num, int64_t chunk_size,
                               int num_chunks) {
    in_edge_active.init(edges_num_);
    out_edge_active.init(edges_num_);
    std::atomic<int> current_chunk(0);
    std::vector<std::thread> work_threads(thread_num);
    for (int i = 0; i < thread_num; ++i) {
      work_threads[i] = std::thread([&]() {
        int got;
        int64_t begin, end;
        while (true) {
          got = current_chunk.fetch_add(1, std::memory_order_relaxed);
          if (got >= num_chunks) {
            break;
          }
          begin = std::min(edges_num_, got * chunk_size);
          end = std::min(edges_num_, begin + chunk_size);
          for (auto j = begin; j < end; ++j) {
            auto src_lid = srcLids[j];
            if (src_lid < vnum_) {
              oe_degree_[src_lid].atomic_.fetch_add(1,
                                                    std::memory_order_relaxed);
              out_edge_active.set_bit(j);
            }
          }
          for (auto j = begin; j < end; ++j) {
            auto dst_lid = dstLids[j];
            if (dst_lid < vnum_) {
              ie_degree_[dst_lid].atomic_.fetch_add(1,
                                                    std::memory_order_relaxed);
              in_edge_active.set_bit(j);
            }
          }
        }
      });
    }

    for (auto& thrd : work_threads) {
      thrd.join();
    }
  }

  vineyard::Status Build(vineyard::Client& client) override {
    this->SetTotalEdgesNum(total_edge_num_);
#if defined(WITH_PROFILING)
    auto time00 = grape::GetCurrentTime();
#endif
    std::vector<std::thread> threads(4);
    threads[0] = std::thread([&]() {
#if defined(WITH_PROFILING)
      auto time0 = grape::GetCurrentTime();
#endif
      std::shared_ptr<arrow::FixedSizeBinaryArray> edges;
      CHECK(in_edge_builder_.Finish(&edges).ok());

      vineyard::FixedSizeBinaryArrayBuilder edge_builder_v6d(client, edges);
      auto res = std::dynamic_pointer_cast<vineyard::FixedSizeBinaryArray>(
          edge_builder_v6d.Seal(client));
      this->SetInEdges(*res);
#if defined(WITH_PROFILING)
      auto time1 = grape::GetCurrentTime();
      VLOG(10) << "Building in edges cost" << (time1 - time0) << " seconds";
#endif
    });
    threads[1] = std::thread([&]() {
#if defined(WITH_PROFILING)
      auto time0 = grape::GetCurrentTime();
#endif
      std::shared_ptr<arrow::FixedSizeBinaryArray> edges;
      CHECK(out_edge_builder_.Finish(&edges).ok());

      vineyard::FixedSizeBinaryArrayBuilder edge_builder_v6d(client, edges);
      auto res = std::dynamic_pointer_cast<vineyard::FixedSizeBinaryArray>(
          edge_builder_v6d.Seal(client));
      this->SetOutEdges(*res);
#if defined(WITH_PROFILING)
      auto time1 = grape::GetCurrentTime();
      VLOG(10) << "Building out edges cost" << (time1 - time0) << " seconds";
#endif
    });

    threads[2] = std::thread([&]() {
#if defined(WITH_PROFILING)
      auto time0 = grape::GetCurrentTime();
#endif
      vineyard_offset_array_builder_t offset_array_builder(client,
                                                           ie_offset_array_);
      this->SetIEOffsetArray(
          *std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              offset_array_builder.Seal(client)));
#if defined(WITH_PROFILING)
      auto time1 = grape::GetCurrentTime();
      VLOG(10) << "Building ie offset cost" << (time1 - time0) << " seconds";
#endif
    });
    threads[3] = std::thread([&]() {
#if defined(WITH_PROFILING)
      auto time0 = grape::GetCurrentTime();
#endif
      vineyard_offset_array_builder_t offset_array_builder(client,
                                                           oe_offset_array_);
      this->SetOEOffsetArray(
          *std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              offset_array_builder.Seal(client)));
#if defined(WITH_PROFILING)
      auto time1 = grape::GetCurrentTime();
      VLOG(10) << "Building oe offset cost" << (time1 - time0) << " seconds";
#endif
    });
    for (auto& work_thread : threads) {
      work_thread.join();
    }
#if defined(WITH_PROFILING)
    auto time11 = grape::GetCurrentTime();
    VLOG(10) << "Building all cost" << (time11 - time00) << " seconds";
#endif
    return vineyard::Status::OK();
  }

  std::shared_ptr<GraphXCSR<vid_t>> MySeal(vineyard::Client& client) {
    return std::dynamic_pointer_cast<GraphXCSR<vid_t>>(this->Seal(client));
  }

 private:
  boost::leaf::result<void> build_offsets() {
    in_edges_num_ = 0;
    for (auto d : ie_degree_) {
      in_edges_num_ += d.atomic_.load();
    }
    ARROW_OK_OR_RAISE(in_edge_builder_.Resize(in_edges_num_));

    out_edges_num_ = 0;
    for (auto d : oe_degree_) {
      out_edges_num_ += d.atomic_.load();
    }
    ARROW_OK_OR_RAISE(out_edge_builder_.Resize(out_edges_num_));

    {
      ie_offsets_.resize(vnum_ + 1);
      ie_offsets_[0] = 0;
      for (VID_T i = 0; i < vnum_; ++i) {
        ie_offsets_[i + 1] = ie_offsets_[i] + ie_degree_[i].atomic_.load();
      }
      offset_array_builder_t builder;
      ARROW_OK_OR_RAISE(builder.AppendValues(ie_offsets_));
      ARROW_OK_OR_RAISE(builder.Finish(&ie_offset_array_));
    }
    {
      oe_offsets_.resize(vnum_ + 1);
      oe_offsets_[0] = 0;
      for (VID_T i = 0; i < vnum_; ++i) {
        oe_offsets_[i + 1] = oe_offsets_[i] + oe_degree_[i].atomic_.load();
      }
      offset_array_builder_t builder;
      ARROW_OK_OR_RAISE(builder.AppendValues(oe_offsets_));
      ARROW_OK_OR_RAISE(builder.Finish(&oe_offset_array_));
    }

    return {};
  }

  void add_edges(int vnum, int tvnum, int local_num,
                 const std::vector<vid_t>& src_accessor,
                 const std::vector<vid_t>& dst_accessor,
                 const grape::Bitset& in_edge_active,
                 const grape::Bitset& out_edge_active, int64_t edges_num_,
                 int64_t chunk_size, int64_t num_chunks, int64_t thread_num) {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif

    nbr_t* ie_mutable_ptr_begin = in_edge_builder_.MutablePointer(0);
    nbr_t* oe_mutable_ptr_begin = out_edge_builder_.MutablePointer(0);

    std::vector<int64_atomic> atomic_oe_offsets, atomic_ie_offsets;
    atomic_oe_offsets.resize(vnum);
    atomic_ie_offsets.resize(vnum);
    for (int i = 0; i < vnum; ++i) {
      atomic_oe_offsets[i].atomic_ = oe_offsets_[i];
      atomic_ie_offsets[i].atomic_ = ie_offsets_[i];
    }
    {
      std::atomic<int> current_chunk(0);
      VLOG(10) << "thread num " << thread_num << ", chunk size: " << chunk_size
               << "num chunks " << num_chunks;
      std::vector<std::thread> work_threads(thread_num);
      for (int i = 0; i < thread_num; ++i) {
        work_threads[i] = std::thread(
            [&](int tid) {
              int got;
              int64_t begin, end;
              while (true) {
                got = current_chunk.fetch_add(1, std::memory_order_relaxed);
                if (got >= num_chunks) {
                  break;
                }
                begin = std::min(edges_num_, got * chunk_size);
                end = std::min(edges_num_, begin + chunk_size);
                for (int64_t j = begin; j < end; ++j) {
                  vid_t srcLid = src_accessor[j];
                  vid_t dstLid = dst_accessor[j];
                  if (out_edge_active.get_bit(j)) {
                    int dstPos = atomic_oe_offsets[srcLid].atomic_.fetch_add(
                        1, std::memory_order_relaxed);
                    nbr_t* ptr = oe_mutable_ptr_begin + dstPos;
                    ptr->vid = dstLid;
                    ptr->eid = static_cast<eid_t>(j);
                  }
                  if (in_edge_active.get_bit(j)) {
                    int dstPos = atomic_ie_offsets[dstLid].atomic_.fetch_add(
                        1, std::memory_order_relaxed);
                    nbr_t* ptr = ie_mutable_ptr_begin + dstPos;
                    ptr->vid = srcLid;
                    ptr->eid = static_cast<eid_t>(j);
                  }
                }
              }
            },
            i);
      }
      for (auto& thrd : work_threads) {
        thrd.join();
      }
    }

#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    VLOG(10) << "Finish adding " << edges_num_ << "edges cost"
             << (finish_seal_ts - start_ts) << " seconds";
#endif
  }

  void sort(int64_t thread_num) {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    int64_t chunk_size = 8192;
    int64_t num_chunks = (vnum_ + chunk_size - 1) / chunk_size;
    std::atomic<int> current_chunk(0);
    VLOG(10) << "thread num " << thread_num << ", chunk size: " << chunk_size
             << "num chunks " << num_chunks;
    std::vector<std::thread> work_threads(thread_num);
    const int64_t* ie_offsets_ptr = ie_offset_array_->raw_values();
    const int64_t* oe_offsets_ptr = oe_offset_array_->raw_values();
    for (int i = 0; i < thread_num; ++i) {
      work_threads[i] = std::thread(
          [&](int tid) {
            int got;
            int64_t start, limit;
            nbr_t *begin, *end;
            while (true) {
              got = current_chunk.fetch_add(1, std::memory_order_relaxed);
              if (got >= num_chunks) {
                break;
              }
              start = std::min(static_cast<int64_t>(vnum_), got * chunk_size);
              limit = std::min(static_cast<int64_t>(vnum_), start + chunk_size);
              for (int64_t j = start; j < limit; ++j) {
                begin = in_edge_builder_.MutablePointer(ie_offsets_ptr[j]);
                end = in_edge_builder_.MutablePointer(ie_offsets_ptr[j + 1]);
                std::sort(begin, end, [](const nbr_t& lhs, const nbr_t& rhs) {
                  return lhs.vid < rhs.vid;
                });
                begin = out_edge_builder_.MutablePointer(oe_offsets_ptr[j]);
                end = out_edge_builder_.MutablePointer(oe_offsets_ptr[j + 1]);
                std::sort(begin, end, [](const nbr_t& lhs, const nbr_t& rhs) {
                  return lhs.vid < rhs.vid;
                });
              }
            }
          },
          i);
    }
    for (auto& thrd : work_threads) {
      thrd.join();
    }

#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    VLOG(10) << "Sort edges cost" << (finish_seal_ts - start_ts) << " seconds";
#endif
  }

  vid_t vnum_;
  eid_t total_edge_num_;
  int64_t in_edges_num_, out_edges_num_;

  std::vector<int64_atomic> ie_degree_, oe_degree_;
  vineyard::PodArrayBuilder<nbr_t> in_edge_builder_, out_edge_builder_;
  std::shared_ptr<offset_array_t> ie_offset_array_,
      oe_offset_array_;  // for output
  std::vector<int64_t> ie_offsets_,
      oe_offsets_;  // used for edge iterate in this
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_CSR_H_
