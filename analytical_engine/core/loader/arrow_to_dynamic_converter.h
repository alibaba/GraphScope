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
 * @brief This is a specialized DynamicWrapper for arrow::util::string_view type
 */
template <>
struct DynamicWrapper<std::string> {
  static void to_dynamic(arrow::util::string_view s, dynamic::Value& t) {
    t.SetString(s.to_string());
  }

  static void to_dynamic_array(const std::string& label,
                               arrow::util::string_view s, dynamic::Value& t) {
    t.SetArray();
    t.PushBack(label).PushBack(s.to_string());
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
  using oid_t = typename src_fragment_t::oid_t;
  using label_id_t = typename src_fragment_t::label_id_t;
  using dst_fragment_t = DynamicFragment;
  using vertex_map_t = typename dst_fragment_t::vertex_map_t;
  using vid_t = typename dst_fragment_t::vid_t;
  using internal_vertex_t = typename dst_fragment_t::internal_vertex_t;
  using vertex_t = typename src_fragment_t::vertex_t;
  using edge_t = typename dst_fragment_t::edge_t;
  using vdata_t = typename dst_fragment_t::vdata_t;
  using edata_t = typename dst_fragment_t::edata_t;

 public:
  explicit ArrowToDynamicConverter(const grape::CommSpec& comm_spec,
                                   int default_label_id)
      : comm_spec_(comm_spec), default_label_id_(default_label_id) {}

  bl::result<std::shared_ptr<dst_fragment_t>> Convert(
      const std::shared_ptr<src_fragment_t>& arrow_frag) {
    auto arrow_vm = arrow_frag->GetVertexMap();
    BOOST_LEAF_AUTO(dynamic_vm, convertVertexMap(arrow_frag));
    BOOST_LEAF_AUTO(dynamic_frag, convertFragment(arrow_frag, dynamic_vm));
    return dynamic_frag;
  }

 private:
  bl::result<std::shared_ptr<vertex_map_t>> convertVertexMap(
      const std::shared_ptr<src_fragment_t>& arrow_frag) {
    auto src_vm_ptr = arrow_frag->GetVertexMap();
    const auto& schema = arrow_frag->schema();
    auto fnum = src_vm_ptr->fnum();
    auto dst_vm_ptr = std::make_shared<vertex_map_t>(comm_spec_);
    vineyard::IdParser<vid_t> id_parser;

    CHECK(src_vm_ptr->fnum() == comm_spec_.fnum());
    dst_vm_ptr->Init();
    typename vertex_map_t::partitioner_t partitioner(comm_spec_.fnum());
    dst_vm_ptr->SetPartitioner(partitioner);
    id_parser.Init(fnum, src_vm_ptr->label_num());
    dynamic::Value to_oid;

    for (label_id_t v_label = 0; v_label < src_vm_ptr->label_num(); v_label++) {
      std::string label_name = schema.GetVertexLabelName(v_label);
      for (fid_t fid = 0; fid < fnum; fid++) {
        for (vid_t offset = 0;
             offset < src_vm_ptr->GetInnerVertexSize(fid, v_label); offset++) {
          auto gid = id_parser.GenerateId(fid, v_label, offset);
          typename vineyard::InternalType<oid_t>::type oid;

          CHECK(src_vm_ptr->GetOid(gid, oid));
          if (v_label == default_label_id_) {
            DynamicWrapper<oid_t>::to_dynamic(oid, to_oid);
            dst_vm_ptr->AddVertex(std::move(to_oid), gid);
          } else {
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
    const auto& schema = src_frag->schema();

    double start = grape::GetCurrentTime();
    uint32_t thread_num = std::thread::hardware_concurrency();
    size_t ivnum = dst_vm->GetInnerVertexSize(fid);
    std::vector<std::vector<internal_vertex_t>> vertices(thread_num);
    std::vector<std::vector<edge_t>> edges(thread_num);
    std::vector<int> oe_degree(ivnum, 0);
    std::vector<int> ie_degree(ivnum, 0);
    for (label_id_t v_label = 0; v_label < src_frag->vertex_label_num();
         v_label++) {
      auto inner_vertices = src_frag->InnerVertices(v_label);
      auto v_data = src_frag->vertex_data_table(v_label);
      std::string label_name = schema.GetVertexLabelName(v_label);

      parallel_for(inner_vertices.begin(), inner_vertices.end(),
                   [&](uint32_t tid, vertex_t u) {
        dynamic::Value u_oid(src_frag->GetId(u));
        vid_t u_gid, v_gid;

        CHECK(dst_vm->GetGid(fid, u_oid, u_gid));
        vid_t lid = dst_vm->GetLidFromGid(u_gid);
        dynamic::Value data(rapidjson::kObjectType);
        // N.B: th last column is id, we ignore it.
        for (auto col_id = 0; col_id < v_data->num_columns() - 1; col_id++) {
          auto column = v_data->column(col_id);
          auto& prop_key = v_data->field(col_id)->name();
          auto type = column->type();
          PropertyConverter<src_fragment_t>::NodeValue(src_frag, u, type,
                                                       prop_key, col_id, data);
        }
        vertices[tid].emplace_back(u_gid, std::move(data));

        // traverse edges and extract data
        for (label_id_t e_label = 0; e_label < src_frag->edge_label_num();
             e_label++) {
          auto e_data = src_frag->edge_data_table(e_label);
          auto oe = src_frag->GetOutgoingAdjList(u, e_label);
          oe_degree[lid] += oe.Size();
          for (auto& e : oe) {
            auto v = e.get_neighbor();
            auto e_id = e.edge_id();
            auto v_label_id = src_frag->vertex_label(v);
            dynamic::Value v_oid(src_frag->GetId(v));
            CHECK(dst_vm->GetGid(v_oid, v_gid));
            data = dynamic::Value(rapidjson::kObjectType);
            PropertyConverter<src_fragment_t>::EdgeValue(e_data, e_id, data);
            edges[tid].emplace_back(u_gid, v_gid, std::move(data));
          }

          if (src_frag->directed()) {
            auto ie = src_frag->GetIncomingAdjList(u, e_label);
            ie_degree[lid] += ie.Size();
            for (auto& e : ie) {
              auto v = e.get_neighbor();
              if (src_frag->IsOuterVertex(v)) {
                auto e_id = e.edge_id();
                auto v_label_id = src_frag->vertex_label(v);
                dynamic::Value v_oid(src_frag->GetId(v));
                CHECK(dst_vm->GetGid(v_oid, v_gid));
                data = dynamic::Value(rapidjson::kObjectType);
                PropertyConverter<src_fragment_t>::EdgeValue(e_data, e_id, data);
                edges[tid].emplace_back(v_gid, u_gid, std::move(data));
              }
            }
          }
        }
          }, thread_num);
    }
    for (int i = 0; i < edges.size();++i) {
      LOG(INFO) << "i=" << i << " size=" << edges[i].size();
    }
    LOG(INFO) << "Process vertices and Edges: " << grape::GetCurrentTime() - start;

    auto dynamic_frag = std::make_shared<dst_fragment_t>(dst_vm);
    start = grape::GetCurrentTime();
    dynamic_frag->Init(src_frag->fid(), src_frag->directed(), vertices, edges, oe_degree, ie_degree);
    LOG(INFO) << "Convert fragment: " << grape::GetCurrentTime() - start;

    // check the graph is consistent
    std::ofstream f("/Users/weibin/Dev/gstest/twitter_dy.e");
    auto inner_vertices = dynamic_frag->InnerVertices();
    for (auto v : inner_vertices) {
      auto oe = dynamic_frag->GetOutgoingAdjList(v);
      for (auto& e : oe) {
        f << dynamic_frag->GetId(v) << "\t" << dynamic_frag->GetId(e.get_neighbor()) << "\t" << e.get_data()["f2"].GetInt()  << "\n";
      }
    }
    f.close();
    return dynamic_frag;
  }

  grape::CommSpec comm_spec_;
  label_id_t default_label_id_;
};

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_LOADER_ARROW_TO_DYNAMIC_CONVERTER_H_
