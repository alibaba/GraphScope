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
#include <vector>

#include "vineyard/graph/fragment/arrow_fragment.h"

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
            dst_vm_ptr->AddVertex(to_oid, gid);
          } else {
            DynamicWrapper<oid_t>::to_dynamic_array(label_name, oid, to_oid);
            dst_vm_ptr->AddVertex(to_oid, gid);
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
    std::vector<DynamicFragment::internal_vertex_t> processed_vertices;
    std::vector<DynamicFragment::edge_t> processed_edges;

    for (label_id_t v_label = 0; v_label < src_frag->vertex_label_num();
         v_label++) {
      auto label_name = schema.GetVertexLabelName(v_label);
      auto v_data = src_frag->vertex_data_table(v_label);
      dynamic::Value u_oid, v_oid, data;
      vid_t u_gid, v_gid;

      // traverse vertices and extract data from ArrowFragment
      for (const auto& u : src_frag->InnerVertices(v_label)) {
        if (v_label == default_label_id_) {
          u_oid = dynamic::Value(src_frag->GetId(u));
        } else {
          u_oid = dynamic::Value(rapidjson::kArrayType);
          u_oid.PushBack(label_name).PushBack(src_frag->GetId(u));
        }

        CHECK(dst_vm->GetGid(fid, u_oid, u_gid));
        data = dynamic::Value(rapidjson::kObjectType);
        // N.B: th last column is id, we ignore it.
        for (auto col_id = 0; col_id < v_data->num_columns() - 1; col_id++) {
          auto column = v_data->column(col_id);
          auto prop_key = v_data->field(col_id)->name();
          auto type = column->type();
          PropertyConverter<src_fragment_t>::NodeValue(src_frag, u, type,
                                                       prop_key, col_id, data);
        }
        processed_vertices.emplace_back(u_gid, data);

        // traverse edges and extract data
        for (label_id_t e_label = 0; e_label < src_frag->edge_label_num();
             e_label++) {
          auto oe = src_frag->GetOutgoingAdjList(u, e_label);
          auto e_data = src_frag->edge_data_table(e_label);
          for (auto& e : oe) {
            auto v = e.neighbor();
            auto e_id = e.edge_id();
            auto v_label_id = src_frag->vertex_label(v);
            if (v_label_id == default_label_id_) {
              v_oid = dynamic::Value(src_frag->GetId(v));
            } else {
              v_oid = dynamic::Value(rapidjson::kArrayType);
              v_oid.PushBack(schema.GetVertexLabelName(v_label_id))
                  .PushBack(src_frag->GetId(v));
            }
            CHECK(dst_vm->GetGid(v_oid, v_gid));
            data = dynamic::Value(rapidjson::kObjectType);
            PropertyConverter<src_fragment_t>::EdgeValue(e_data, e_id, data);
            processed_edges.emplace_back(u_gid, v_gid, data);
          }

          if (src_frag->directed()) {
            auto ie = src_frag->GetIncomingAdjList(u, e_label);
            for (auto& e : ie) {
              auto v = e.neighbor();
              if (src_frag->IsOuterVertex(v)) {
                auto e_id = e.edge_id();
                auto v_label_id = src_frag->vertex_label(v);
                if (v_label_id == default_label_id_) {
                  v_oid = dynamic::Value(src_frag->GetId(v));
                } else {
                  v_oid = dynamic::Value(rapidjson::kArrayType);
                  v_oid.PushBack(schema.GetVertexLabelName(v_label_id))
                      .PushBack(src_frag->GetId(v));
                }
                CHECK(dst_vm->GetGid(v_oid, v_gid));
                data = dynamic::Value(rapidjson::kObjectType);
                PropertyConverter<src_fragment_t>::EdgeValue(e_data, e_id,
                                                             data);
                processed_edges.emplace_back(v_gid, u_gid, data);
              }
            }
          }
        }
      }
    }

    auto dynamic_frag = std::make_shared<dst_fragment_t>(dst_vm);
    dynamic_frag->Init(src_frag->fid(), processed_vertices, processed_edges,
                       src_frag->directed());
    return dynamic_frag;
  }

  grape::CommSpec comm_spec_;
  label_id_t default_label_id_;
};

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_LOADER_ARROW_TO_DYNAMIC_CONVERTER_H_
