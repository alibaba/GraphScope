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
#include "flex/engines/graph_db/runtime/execute/ops/update/load.h"
#include "flex/engines/graph_db/runtime/common/operators/update/load.h"

namespace gs {
namespace runtime {
namespace ops {

static PropertyType get_vertex_pk_type(const Schema& schema, label_t label) {
  const auto& pk_types = schema.get_vertex_primary_key(label);
  CHECK(pk_types.size() == 1) << "Only support one primary key";
  return std::get<0>(pk_types[0]);
}

class LoadSingleEdgeOpr : public IInsertOperator {
 public:
  LoadSingleEdgeOpr(int src_label_id, int dst_label_id, int edge_label_id,
                    PropertyType src_pk_type, PropertyType dst_pk_type,
                    PropertyType edge_prop_type, int src_index, int dst_index,
                    int prop_index = -1)
      : src_label_id(src_label_id),
        dst_label_id(dst_label_id),
        edge_label_id(edge_label_id),
        src_index(src_index),
        dst_index(dst_index),
        prop_index(prop_index),
        src_pk_type(src_pk_type),
        dst_pk_type(dst_pk_type),
        edge_prop_type(edge_prop_type) {}

  gs::runtime::WriteContext Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return Load::load_single_edge(graph, std::move(ctx), src_label_id,
                                  dst_label_id, edge_label_id, src_pk_type,
                                  dst_pk_type, edge_prop_type, src_index,
                                  dst_index, prop_index);
  }

 private:
  int src_label_id, dst_label_id, edge_label_id, src_index, dst_index,
      prop_index;
  PropertyType src_pk_type, dst_pk_type, edge_prop_type;
};

class LoadSingleVertexOpr : public IInsertOperator {
 public:
  LoadSingleVertexOpr(
      int vertex_label_id, int id_col, PropertyType pk_type,
      const std::vector<int>& properties,
      const std::vector<std::tuple<label_t, label_t, label_t, PropertyType,
                                   PropertyType, PropertyType, int, int, int>>&
          edges)
      : vertex_label_id(vertex_label_id),
        id_col(id_col),
        pk_type(pk_type),
        properties(properties),
        edges(edges) {}

  gs::runtime::WriteContext Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return Load::load_single_vertex(graph, std::move(ctx), vertex_label_id,
                                    pk_type, id_col, properties, edges);
  }

 private:
  int vertex_label_id, id_col;
  PropertyType pk_type;
  std::vector<int> properties;
  std::vector<std::tuple<label_t, label_t, label_t, PropertyType, PropertyType,
                         PropertyType, int, int, int>>
      edges;
};
class LoadOpr : public IInsertOperator {
 public:
  LoadOpr(
      const std::vector<std::tuple<label_t, int, PropertyType,
                                   std::vector<int>>>& vertex_mappings,
      const std::vector<std::tuple<label_t, label_t, label_t, PropertyType,
                                   PropertyType, PropertyType, int, int, int>>&
          edge_mappings)
      : vertex_mappings_(vertex_mappings), edge_mappings_(edge_mappings) {}

  gs::runtime::WriteContext Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return Load::load(graph, std::move(ctx), vertex_mappings_, edge_mappings_);
  }

 private:
  std::vector<std::tuple<label_t, int, PropertyType, std::vector<int>>>
      vertex_mappings_;
  std::vector<std::tuple<label_t, label_t, label_t, PropertyType, PropertyType,
                         PropertyType, int, int, int>>
      edge_mappings_;
};

static std::tuple<label_t, label_t, label_t, PropertyType, PropertyType,
                  PropertyType, int, int, int>
parse_edge_mapping(
    const Schema& schema,
    const cypher::Load_ColumnMappings_EdgeMapping& edge_mapping) {
  const auto& type_triplet = edge_mapping.type_triplet();
  const auto& src_label = type_triplet.source_vertex();
  const auto& dst_label = type_triplet.destination_vertex();
  const auto& edge_label = type_triplet.edge();

  const auto src_label_id = schema.get_vertex_label_id(src_label);
  const auto dst_label_id = schema.get_vertex_label_id(dst_label);
  const auto edge_label_id = schema.get_edge_label_id(edge_label);
  const auto& prop_names =
      schema.get_edge_property_names(src_label_id, dst_label_id, edge_label_id);
  const auto& prop_types =
      schema.get_edge_properties(src_label_id, dst_label_id, edge_label_id);
  CHECK(edge_mapping.source_vertex_mappings_size() == 1);
  CHECK(edge_mapping.destination_vertex_mappings_size() == 1);
  auto src_mapping = edge_mapping.source_vertex_mappings(0);
  auto dst_mapping = edge_mapping.destination_vertex_mappings(0);

  CHECK(src_mapping.property().key().name() == "id");
  CHECK(dst_mapping.property().key().name() == "id");

  auto src_pk_type = get_vertex_pk_type(schema, src_label_id);
  auto dst_pk_type = get_vertex_pk_type(schema, dst_label_id);
  const auto& edge_props = edge_mapping.column_mappings();
  CHECK(static_cast<size_t>(edge_mapping.column_mappings_size()) ==
        prop_types.size())
      << "Only support one property";

  CHECK(prop_names.size() < 2) << "Only support one property";
  PropertyType edge_prop_type = PropertyType::kEmpty;
  int prop_index = -1;
  int src_index = src_mapping.column().index();
  int dst_index = dst_mapping.column().index();
  if (prop_names.size() == 1) {
    const auto& edge_prop = edge_props[0];
    const auto& prop_name = edge_prop.property().key().name();
    CHECK(prop_name == prop_names[0]) << "property name not match";

    prop_index = edge_props[0].column().index();
    edge_prop_type = prop_types[0];
  }
  return std::make_tuple(src_label_id, dst_label_id, edge_label_id, src_pk_type,
                         dst_pk_type, edge_prop_type, src_index, dst_index,
                         prop_index);
}

static std::tuple<label_t, int, PropertyType, std::vector<int>>
parse_vertex_mapping(
    const Schema& schema,
    const cypher::Load_ColumnMappings_VertexMapping& vertex_mapping) {
  const auto& vertex_label = vertex_mapping.type_name();
  const auto& vertex_label_id = schema.get_vertex_label_id(vertex_label);
  auto pk_type = get_vertex_pk_type(schema, vertex_label_id);

  const auto& vertex_prop_types = schema.get_vertex_properties(vertex_label_id);
  const auto& prop_map =
      schema.get_vprop_name_to_type_and_index(vertex_label_id);

  const auto& props = vertex_mapping.column_mappings();
  size_t prop_size = vertex_mapping.column_mappings_size();
  std::vector<int> properties(vertex_prop_types.size());
  CHECK(prop_size == vertex_prop_types.size() + 1)
      << "Only support one primary key";
  int id_col = -1;
  for (size_t j = 0; j < prop_size; ++j) {
    const auto& prop = props[j];
    const auto& prop_name = prop.property().key().name();
    if (prop_name == "id") {
      id_col = prop.column().index();
    } else {
      const auto& prop_idx = prop_map.at(prop_name).second;
      properties[prop_idx] = prop.column().index();
    }
  }
  return std::make_tuple(vertex_label_id, id_col, pk_type, properties);
}
std::unique_ptr<IInsertOperator> LoadOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr().load();
  CHECK(opr.kind() == cypher::Load_Kind::Load_Kind_CREATE)
      << "Only support CREATE";
  auto& mappings = opr.mappings();
  int vertex_mapping_size = mappings.vertex_mappings_size();
  int edge_mapping_size = mappings.edge_mappings_size();
  if (vertex_mapping_size == 0 && edge_mapping_size == 1) {
    auto [src_label_id, dst_label_id, edge_label_id, src_pk_type, dst_pk_type,
          edge_prop_type, src_index, dst_index, prop_index] =
        parse_edge_mapping(schema, mappings.edge_mappings(0));

    return std::make_unique<LoadSingleEdgeOpr>(
        src_label_id, dst_label_id, edge_label_id, src_pk_type, dst_pk_type,
        edge_prop_type, src_index, dst_index, prop_index);
  } else if (vertex_mapping_size == 1) {
    auto [vertex_label_id, id_col, pk_type, properties] =
        parse_vertex_mapping(schema, mappings.vertex_mappings(0));
    std::vector<std::tuple<label_t, label_t, label_t, PropertyType,
                           PropertyType, PropertyType, int, int, int>>
        edges;
    for (int i = 0; i < edge_mapping_size; ++i) {
      auto [src_label_id, dst_label_id, edge_label_id, src_pk_type, dst_pk_type,
            edge_prop_type, src_index, dst_index, prop_index] =
          parse_edge_mapping(schema, mappings.edge_mappings(i));
      edges.emplace_back(src_label_id, dst_label_id, edge_label_id, src_pk_type,
                         dst_pk_type, edge_prop_type, src_index, dst_index,
                         prop_index);
    }
    return std::make_unique<LoadSingleVertexOpr>(vertex_label_id, id_col,
                                                 pk_type, properties, edges);
  } else {
    std::vector<std::tuple<label_t, int, PropertyType, std::vector<int>>>
        vertex_mappings;
    for (int i = 0; i < vertex_mapping_size; ++i) {
      auto [vertex_label_id, id_col, pk_type, properties] =
          parse_vertex_mapping(schema, mappings.vertex_mappings(i));
      vertex_mappings.emplace_back(vertex_label_id, id_col, pk_type,
                                   properties);
    }
    std::vector<std::tuple<label_t, label_t, label_t, PropertyType,
                           PropertyType, PropertyType, int, int, int>>
        edge_mappings;
    for (int i = 0; i < edge_mapping_size; ++i) {
      auto [src_label_id, dst_label_id, edge_label_id, src_pk_type, dst_pk_type,
            edge_prop_type, src_index, dst_index, prop_index] =
          parse_edge_mapping(schema, mappings.edge_mappings(i));
      edge_mappings.emplace_back(src_label_id, dst_label_id, edge_label_id,
                                 src_pk_type, dst_pk_type, edge_prop_type,
                                 src_index, dst_index, prop_index);
    }
    return std::make_unique<LoadOpr>(vertex_mappings, edge_mappings);
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs