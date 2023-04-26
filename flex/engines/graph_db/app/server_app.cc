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

#include "flex/engines/graph_db/app/server_app.h"

namespace gs {

static uint32_t get_vertex_vid(const gs::ReadTransaction& txn, uint8_t label,
                               int64_t id) {
  uint32_t vid = std::numeric_limits<uint32_t>::max();
  auto vit = txn.GetVertexIterator(label);
  for (; vit.IsValid(); vit.Next()) {
    if (vit.GetId() == id) {
      vid = vit.GetIndex();
      break;
    }
  }
  return vid;
}

void generate_label_tuples(
    const std::string& src_label, const std::string& dst_label,
    const std::string& edge_label, const gs::Schema& schema,
    std::vector<std::tuple<uint8_t, uint8_t, uint8_t>>& output) {
  output.clear();
  std::vector<uint8_t> src_labels, dst_labels, edge_labels;
  uint8_t vertex_label_num = schema.vertex_label_num();
  uint8_t edge_label_num = schema.edge_label_num();
  if (src_label == "_ANY_LABEL") {
    for (uint8_t k = 0; k != vertex_label_num; ++k) {
      src_labels.push_back(k);
    }
  } else {
    if (!schema.contains_vertex_label(src_label)) {
      return;
    }
    src_labels.push_back(schema.get_vertex_label_id(src_label));
  }
  if (dst_label == "_ANY_LABEL") {
    for (uint8_t k = 0; k != vertex_label_num; ++k) {
      dst_labels.push_back(k);
    }
  } else {
    if (!schema.contains_vertex_label(dst_label)) {
      return;
    }
    dst_labels.push_back(schema.get_vertex_label_id(dst_label));
  }
  if (edge_label == "_ANY_LABEL") {
    for (uint8_t k = 0; k != edge_label_num; ++k) {
      edge_labels.push_back(k);
    }
  } else {
    if (!schema.contains_edge_label(edge_label)) {
      return;
    }
    edge_labels.push_back(schema.get_edge_label_id(edge_label));
  }
  for (auto s : src_labels) {
    std::string s_name = schema.get_vertex_label_name(s);
    for (auto d : dst_labels) {
      std::string d_name = schema.get_vertex_label_name(d);
      for (auto e : edge_labels) {
        std::string e_name = schema.get_edge_label_name(e);
        if (schema.exist(s_name, d_name, e_name)) {
          output.emplace_back(s, d, e);
        }
      }
    }
  }
}

bool ServerApp::Query(Decoder& input, Encoder& output) {
  std::string op = std::string(input.get_string());
  for (auto& c : op) {
    c = toupper(c);
  }
  if (op == "SHOW_STORED_PROCEDURES") {
    CHECK(input.empty());
    graph_.GetAppInfo(output);
    return true;
  } else if (op == "QUERY_VERTEX") {
    std::string vertex_label = std::string(input.get_string());
    int64_t vertex_id = input.get_long();
    CHECK(input.empty());
    auto txn = graph_.GetReadTransaction();
    uint8_t vertex_label_id = txn.schema().get_vertex_label_id(vertex_label);
    auto vit = txn.GetVertexIterator(vertex_label_id);
    for (; vit.IsValid(); vit.Next()) {
      if (vit.GetId() == vertex_id) {
        output.put_int(1);
        int field_num = vit.FieldNum();
        for (int i = 0; i < field_num; ++i) {
          output.put_string(vit.GetField(i).to_string());
        }
        return true;
      }
    }

    output.put_int(0);
    return false;
  } else if (op == "QUERY_EDGE") {
    std::string src_label = std::string(input.get_string());
    int64_t src_id = input.get_long();
    std::string dst_label = std::string(input.get_string());
    int64_t dst_id = input.get_long();
    std::string edge_label = std::string(input.get_string());
    CHECK(input.empty());

    if (src_label != "_ANY_LABEL" && dst_label != "_ANY_LABEL" &&
        edge_label != "_ANY_LABEL" &&
        src_id != std::numeric_limits<int64_t>::max() &&
        dst_id != std::numeric_limits<int64_t>::max()) {
      auto txn = graph_.GetReadTransaction();
      uint8_t src_label_id, dst_label_id, edge_label_id;
      if (!txn.schema().contains_vertex_label(src_label)) {
        output.put_int(0);
        return false;
      }
      src_label_id = txn.schema().get_vertex_label_id(src_label);
      if (!txn.schema().contains_vertex_label(dst_label)) {
        output.put_int(0);
        return false;
      }
      dst_label_id = txn.schema().get_vertex_label_id(dst_label);
      if (!txn.schema().contains_edge_label(edge_label)) {
        output.put_int(0);
        return false;
      }
      edge_label_id = txn.schema().get_edge_label_id(edge_label);
      uint32_t src_vid = get_vertex_vid(txn, src_label_id, src_id);
      if (src_vid == std::numeric_limits<uint32_t>::max()) {
        output.put_int(0);
        return false;
      }
      uint32_t dst_vid = get_vertex_vid(txn, dst_label_id, dst_id);
      if (dst_vid == std::numeric_limits<uint32_t>::max()) {
        output.put_int(0);
        return false;
      }

      auto ieit = txn.GetInEdgeIterator(dst_label_id, dst_vid, src_label_id,
                                        edge_label_id);
      while (ieit.IsValid()) {
        if (ieit.GetNeighbor() == src_vid) {
          output.put_int(1);
          output.put_string(src_label);
          output.put_string(dst_label);
          output.put_string(edge_label);
          output.put_int(1);
          output.put_long(src_id);
          output.put_long(dst_id);
          output.put_string(ieit.GetData().to_string());
          return true;
        }
        ieit.Next();
      }

      auto oeit = txn.GetOutEdgeIterator(src_label_id, src_vid, dst_label_id,
                                         edge_label_id);
      while (oeit.IsValid()) {
        if (oeit.GetNeighbor() == dst_vid) {
          output.put_int(1);
          output.put_string(src_label);
          output.put_string(dst_label);
          output.put_string(edge_label);
          output.put_int(1);
          output.put_long(src_id);
          output.put_long(dst_id);
          output.put_string(oeit.GetData().to_string());
          return true;
        }
        oeit.Next();
      }

      output.put_int(0);
      return true;
    } else {
      auto txn = graph_.GetReadTransaction();
      std::vector<std::tuple<uint8_t, uint8_t, uint8_t>> label_tuples;
      generate_label_tuples(src_label, dst_label, edge_label, txn.schema(),
                            label_tuples);
      if (label_tuples.empty()) {
        output.put_int(0);
        return true;
      }
      output.put_int(1);
      size_t total_matched_edges = 0;
      for (auto& tup : label_tuples) {
        uint8_t src_label_id = std::get<0>(tup);
        uint8_t dst_label_id = std::get<1>(tup);
        uint8_t edge_label_id = std::get<2>(tup);

        vertex_range src_range(0, 0), dst_range(0, 0);

        if (src_id == std::numeric_limits<int64_t>::max()) {
          src_range.from = 0;
          src_range.to = txn.GetVertexNum(src_label_id);
        } else {
          uint32_t src_vid = get_vertex_vid(txn, src_label_id, src_id);
          if (src_vid != std::numeric_limits<uint32_t>::max()) {
            src_range.from = src_vid;
            src_range.to = src_vid + 1;
          }
        }

        if (dst_id == std::numeric_limits<int64_t>::max()) {
          dst_range.from = 0;
          dst_range.to = txn.GetVertexNum(dst_label_id);
        } else {
          uint32_t dst_vid = get_vertex_vid(txn, dst_label_id, dst_id);
          if (dst_vid != std::numeric_limits<uint32_t>::max()) {
            dst_range.from = dst_vid;
            dst_range.to = dst_vid + 1;
          }
        }

        if (src_range.empty() || dst_range.empty()) {
          continue;
        }

        std::vector<std::tuple<int64_t, int64_t, std::string>> match_edges;
        for (uint32_t v = dst_range.from; v != dst_range.to; ++v) {
          int64_t v_oid = txn.GetVertexId(dst_label_id, v);
          auto ieit = txn.GetInEdgeIterator(dst_label_id, v, src_label_id,
                                            edge_label_id);
          while (ieit.IsValid()) {
            uint32_t u = ieit.GetNeighbor();
            if (src_range.contains(u)) {
              int64_t u_oid = txn.GetVertexId(src_label_id, u);
              match_edges.emplace_back(u_oid, v_oid,
                                       ieit.GetData().to_string());
            }
            ieit.Next();
          }
        }
        if (match_edges.empty()) {
          for (uint32_t u = src_range.from; u != src_range.to; ++u) {
            int64_t u_oid = txn.GetVertexId(src_label_id, u);
            auto oeit = txn.GetOutEdgeIterator(src_label_id, u, dst_label_id,
                                               edge_label_id);
            while (oeit.IsValid()) {
              uint32_t v = oeit.GetNeighbor();
              if (dst_range.contains(v)) {
                int64_t v_oid = txn.GetVertexId(dst_label_id, v);
                match_edges.emplace_back(u_oid, v_oid,
                                         oeit.GetData().to_string());
              }
              oeit.Next();
            }
          }
        }
        if (!match_edges.empty()) {
          total_matched_edges += match_edges.size();
          if (total_matched_edges > 1000) {
            output.clear();
            output.put_int(2);
            return true;
          }
          std::string src_label_name =
              txn.schema().get_vertex_label_name(src_label_id);
          std::string dst_label_name =
              txn.schema().get_vertex_label_name(dst_label_id);
          std::string edge_label_name =
              txn.schema().get_edge_label_name(edge_label_id);
          output.put_string(src_label_name);
          output.put_string(dst_label_name);
          output.put_string(edge_label_name);
          output.put_int(match_edges.size());
          for (auto& e : match_edges) {
            output.put_long(std::get<0>(e));
            output.put_long(std::get<1>(e));
            output.put_string(std::get<2>(e));
          }
        }
      }

      if (total_matched_edges == 0) {
        output.clear();
        output.put_int(0);
      }
      return true;
    }
  }
  return false;
}

AppWrapper ServerAppFactory::CreateApp(GraphDBSession& graph) {
  AppBase* app = new ServerApp(graph);
  return AppWrapper(app, NULL);
}

}  // namespace gs
