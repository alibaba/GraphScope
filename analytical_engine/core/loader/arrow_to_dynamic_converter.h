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

#ifndef ANALYTICAL_ENGINE_CORE_LOADER_ARROW_TO_DYNAMIC_CONVERTER_H_
#define ANALYTICAL_ENGINE_CORE_LOADER_ARROW_TO_DYNAMIC_CONVERTER_H_

#ifdef NETWORKX

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/fragment/dynamic_fragment.h"
#include "core/utils/convert_utils.h"

namespace bl = boost::leaf;

namespace gs {
/**
 * @brief A utility class to pack basic C++ data type to dynamic::Value
 * @tparam T
 */
template <typename T>
struct DynamicWrapper {
  static void to_dynamic(T s, dynamic::Value& t) { t = dynamic::Value(s); }

  static void to_dynamic_array(const std::string& label, T s,
                               dynamic::Value& t) {
    t.SetArray();
    t.PushBack(label).PushBack(s);
  }
};

/**
 * @brief This is a specialized DynamicWrapper for int64
 */
template <>
struct DynamicWrapper<int64_t> {
  static void to_dynamic(int64_t s, dynamic::Value& t) { t.SetInt64(s); }

  static void to_dynamic_array(const std::string& label, int64_t s,
                               dynamic::Value& t) {
    t.SetArray();
    t.PushBack(label).PushBack(s);
  }
};

/**
 * @brief This is a specialized DynamicWrapper for vineyard::arrow_string_view
 * type
 */
template <>
struct DynamicWrapper<std::string> {
  static void to_dynamic(vineyard::arrow_string_view s, dynamic::Value& t) {
    t.SetString(std::string(s));
  }

  static void to_dynamic_array(const std::string& label,
                               vineyard::arrow_string_view s,
                               dynamic::Value& t) {
    t.SetArray();
    t.PushBack(label).PushBack(std::string(s));
  }
};

/**
 * @brief A ArrowFragment to DynamicFragment converter. The conversion is
 * proceeded by traversing the source graph.
 * @tparam FRAG_T Fragment class
 */
template <typename FRAG_T>
class ArrowToDynamicConverter {
  using src_fragment_t = FRAG_T;
  using vertex_t = typename src_fragment_t::vertex_t;
  using oid_t = typename src_fragment_t::oid_t;
  using label_id_t = typename src_fragment_t::label_id_t;
  using dst_fragment_t = DynamicFragment;
  using vertex_map_t = typename dst_fragment_t::vertex_map_t;
  using vid_t = typename dst_fragment_t::vid_t;
  using internal_vertex_t = typename dst_fragment_t::internal_vertex_t;
  using edge_t = typename dst_fragment_t::edge_t;
  using vdata_t = typename dst_fragment_t::vdata_t;
  using edata_t = typename dst_fragment_t::edata_t;

 public:
  explicit ArrowToDynamicConverter(const grape::CommSpec& comm_spec,
                                   int default_label_id)
      : comm_spec_(comm_spec), default_label_id_(default_label_id) {}

  bl::result<std::shared_ptr<dst_fragment_t>> Convert(
      const std::shared_ptr<src_fragment_t>& arrow_frag) {
    arrow_vm_ptr_ = arrow_frag->GetVertexMap();
    CHECK(arrow_vm_ptr_->fnum() == comm_spec_.fnum());
    arrow_id_parser_.Init(comm_spec_.fnum(), arrow_vm_ptr_->label_num());
    dynamic_id_parser_.init(comm_spec_.fnum());

    BOOST_LEAF_AUTO(dynamic_vm, convertVertexMap(arrow_frag));
    BOOST_LEAF_AUTO(dynamic_frag, convertFragment(arrow_frag, dynamic_vm));
    return dynamic_frag;
  }

 private:
  bl::result<std::shared_ptr<vertex_map_t>> convertVertexMap(
      const std::shared_ptr<src_fragment_t>& arrow_frag) {
    const auto& schema = arrow_frag->schema();

    auto dst_vm_ptr = std::make_shared<vertex_map_t>(comm_spec_);
    dst_vm_ptr->Init();
    typename vertex_map_t::partitioner_t partitioner(comm_spec_.fnum());
    dst_vm_ptr->SetPartitioner(partitioner);
    dynamic::Value to_oid;

    for (label_id_t v_label = 0; v_label < arrow_vm_ptr_->label_num();
         v_label++) {
      if (v_label == default_label_id_) {
        for (fid_t fid = 0; fid < comm_spec_.fnum(); fid++) {
          for (vid_t offset = 0;
               offset < arrow_vm_ptr_->GetInnerVertexSize(fid, v_label);
               offset++) {
            auto gid = arrow_id_parser_.GenerateId(fid, v_label, offset);
            typename vineyard::InternalType<oid_t>::type oid;
            CHECK(arrow_vm_ptr_->GetOid(gid, oid));
            DynamicWrapper<oid_t>::to_dynamic(oid, to_oid);
            dst_vm_ptr->AddVertex(std::move(to_oid), gid);
          }
        }
      } else {
        std::string label_name = schema.GetVertexLabelName(v_label);
        for (fid_t fid = 0; fid < comm_spec_.fnum(); fid++) {
          for (vid_t offset = 0;
               offset < arrow_vm_ptr_->GetInnerVertexSize(fid, v_label);
               offset++) {
            auto gid = arrow_id_parser_.GenerateId(fid, v_label, offset);
            typename vineyard::InternalType<oid_t>::type oid;
            CHECK(arrow_vm_ptr_->GetOid(gid, oid));
            DynamicWrapper<oid_t>::to_dynamic_array(label_name, oid, to_oid);
            dst_vm_ptr->AddVertex(std::move(to_oid), gid);
          }
        }
      }
    }

    return dst_vm_ptr;
  }

  bl::result<std::shared_ptr<dst_fragment_t>> convertFragment(
      const std::shared_ptr<src_fragment_t>& src_frag,
      const std::shared_ptr<vertex_map_t>& dst_vm) {
    auto fid = src_frag->fid();

    auto dynamic_frag = std::make_shared<dst_fragment_t>(dst_vm);
    uint32_t thread_num =
        (std::thread::hardware_concurrency() + comm_spec_.local_num() - 1) /
        comm_spec_.local_num();

    // Init allocators for dynamic fragment
    dynamic_frag->allocators_ =
        std::make_shared<std::vector<dynamic::AllocatorT>>(thread_num);
    auto& allocators = dynamic_frag->allocators_;
    std::vector<std::vector<internal_vertex_t>> vertices(thread_num);
    std::vector<std::vector<edge_t>> edges(thread_num);
    vid_t ovnum = 0;
    for (label_id_t label = 0; label < src_frag->vertex_label_num(); ++label) {
      ovnum += src_frag->GetOuterVerticesNum(label);
    }

    // we record the degree messages here to avoid fetch these messages in
    // dynamic_frag.Init again.
    std::vector<int> inner_oe_degree(dst_vm->GetInnerVertexSize(fid), 0);
    std::vector<int> inner_ie_degree(dst_vm->GetInnerVertexSize(fid), 0);
    std::vector<int> outer_oe_degree(ovnum, 0);
    std::vector<int> outer_ie_degree(ovnum, 0);
    ska::flat_hash_map<vid_t, vid_t> ovg2i;
    vid_t ov_index = 0, index = 0;
    for (label_id_t v_label = 0; v_label < src_frag->vertex_label_num();
         v_label++) {
      auto inner_vertices = src_frag->InnerVertices(v_label);
      auto v_data = src_frag->vertex_data_table(v_label);

      parallel_for(
          inner_vertices.begin(), inner_vertices.end(),
          [&](uint32_t tid, vertex_t u) {
            vid_t u_gid = gid2Gid(src_frag->GetInnerVertexGid(u));
            vid_t lid = dynamic_id_parser_.get_local_id(u_gid);
            // extract vertex properties
            // N.B: th last column is id, we ignore it.
            dynamic::Value vertex_data(rapidjson::kObjectType);
            for (auto col_id = 0; col_id < v_data->num_columns() - 1;
                 col_id++) {
              auto column = v_data->column(col_id);
              auto& prop_key = v_data->field(col_id)->name();
              auto type = column->type();
              PropertyConverter<src_fragment_t>::NodeValue(
                  src_frag, u, type, prop_key, col_id, vertex_data,
                  (*allocators)[tid]);
            }
            vertices[tid].emplace_back(lid, std::move(vertex_data));

            // traverse edges and extract edge properties
            for (label_id_t e_label = 0; e_label < src_frag->edge_label_num();
                 e_label++) {
              auto e_data = src_frag->edge_data_table(e_label);
              auto oe = src_frag->GetOutgoingAdjList(u, e_label);
              inner_oe_degree[lid] += oe.Size();
              for (auto& e : oe) {
                auto v = e.get_neighbor();
                auto e_id = e.edge_id();
                vid_t v_gid = gid2Gid(src_frag->Vertex2Gid(v));
                if (src_frag->IsOuterVertex(v)) {
                  auto iter = ovg2i.find(v_gid);
                  if (iter != ovg2i.end()) {
                    index = iter->second;
                  } else {
                    ovg2i.emplace(v_gid, ov_index);
                    index = ov_index;
                    ++ov_index;
                  }
                  src_frag->directed() ? outer_ie_degree[index]++
                                       : outer_oe_degree[index]++;
                }
                dynamic::Value edge_data(rapidjson::kObjectType);
                PropertyConverter<src_fragment_t>::EdgeValue(
                    e_data, e_id, edge_data, (*allocators)[tid]);
                edges[tid].emplace_back(u_gid, v_gid, std::move(edge_data));
              }

              if (src_frag->directed()) {
                auto ie = src_frag->GetIncomingAdjList(u, e_label);
                inner_ie_degree[lid] += ie.Size();
                for (auto& e : ie) {
                  auto v = e.get_neighbor();
                  if (src_frag->IsOuterVertex(v)) {
                    auto e_id = e.edge_id();
                    vid_t v_gid = gid2Gid(src_frag->GetOuterVertexGid(v));
                    if (src_frag->IsOuterVertex(v)) {
                      auto iter = ovg2i.find(v_gid);
                      if (iter != ovg2i.end()) {
                        index = iter->second;
                      } else {
                        ovg2i.emplace(v_gid, ov_index);
                        index = ov_index;
                        ++ov_index;
                      }
                      outer_oe_degree[index]++;
                    }
                    dynamic::Value edge_data(rapidjson::kObjectType);
                    PropertyConverter<src_fragment_t>::EdgeValue(
                        e_data, e_id, edge_data, (*allocators)[tid]);
                    edges[tid].emplace_back(v_gid, u_gid, std::move(edge_data));
                  }
                }
              }
            }
          },
          thread_num);
    }

    dynamic_frag->Init(src_frag->fid(), src_frag->directed(), vertices, edges,
                       inner_oe_degree, outer_oe_degree, inner_ie_degree,
                       outer_ie_degree, thread_num);

    initFragmentSchema(dynamic_frag, src_frag->schema());

    return dynamic_frag;
  }

  /**
   * Convert arrow fragment gid of vertex to corresponding dynamic fragment gid.
   * In the covertVertexMap process, the insert order of vertex in dynamic
   * fragment vertex map is the same as arrow fragment vertex map.
   *
   * Params:
   *  - gid: the arrow fragment gid of vertex
   *
   * Returns:
   *  The corresponding dynamic fragment gid of the vertex
   */
  vid_t gid2Gid(const vid_t gid) const {
    auto fid = arrow_id_parser_.GetFid(gid);
    auto label_id = arrow_id_parser_.GetLabelId(gid);
    auto offset = arrow_id_parser_.GetOffset(gid);
    for (label_id_t i = 0; i < label_id; ++i) {
      offset += arrow_vm_ptr_->GetInnerVertexSize(fid, i);
    }
    return dynamic_id_parser_.generate_global_id(fid, offset);
  }

  void initFragmentSchema(std::shared_ptr<dst_fragment_t> frag,
                          const vineyard::PropertyGraphSchema& schema) {
    // init vertex properties schema
    for (size_t label_id = 0; label_id < schema.all_vertex_label_num();
         ++label_id) {
      for (auto& p : schema.GetVertexPropertyListByLabel(label_id)) {
        dynamic::Value key(p.first);
        frag->schema_["vertex"].AddMember(key, dynamic::Str2RpcType(p.second),
                                          dynamic::Value::allocator_);
      }
    }
    for (size_t label_id = 0; label_id < schema.all_edge_label_num();
         ++label_id) {
      for (auto& p : schema.GetEdgePropertyListByLabel(label_id)) {
        dynamic::Value key(p.first);
        frag->schema_["edge"].AddMember(key, dynamic::Str2RpcType(p.second),
                                        dynamic::Value::allocator_);
      }
    }
  }

  grape::CommSpec comm_spec_;
  label_id_t default_label_id_;
  std::shared_ptr<typename src_fragment_t::vertex_map_t> arrow_vm_ptr_;
  vineyard::IdParser<vid_t> arrow_id_parser_;
  grape::IdParser<vid_t> dynamic_id_parser_;
};

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_LOADER_ARROW_TO_DYNAMIC_CONVERTER_H_
