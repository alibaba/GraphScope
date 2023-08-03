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

#ifndef GRAPHSCOPE_FRAGMENT_SCHEMA_H_
#define GRAPHSCOPE_FRAGMENT_SCHEMA_H_

#include "flex/storages/rt_mutable_graph/load_config.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/id_indexer.h"
#include "flex/utils/property/table.h"

namespace gs {

static constexpr const char* DT_SIGNED_INT32 = "DT_SIGNED_INT32";
static constexpr const char* DT_UINT64 = "DT_UINT64";
static constexpr const char* DT_STRING = "DT_STRING";
static constexpr const char* DT_SIGNED_INT64 = "DT_SIGNED_INT64";
static constexpr const char* DT_DOUBLE = "DT_DOUBLE";

class Schema {
 public:
  Schema();
  ~Schema();

  void add_vertex_label(
      const std::string& label, const std::vector<PropertyType>& property_types,
      const std::vector<std::string>& property_names,
      const std::tuple<PropertyType, std::string, int32_t>& primary_key,
      const std::vector<StorageStrategy>& strategies = {},
      size_t max_vnum = static_cast<size_t>(1) << 32);

  void add_edge_label(const std::string& src_label,
                      const std::string& dst_label,
                      const std::string& edge_label,
                      const std::vector<PropertyType>& properties,
                      const std::vector<std::string>& prop_names,
                      EdgeStrategy oe = EdgeStrategy::kMultiple,
                      EdgeStrategy ie = EdgeStrategy::kMultiple);

  label_t vertex_label_num() const;

  label_t edge_label_num() const;

  bool contains_vertex_label(const std::string& label) const;

  label_t get_vertex_label_id(const std::string& label) const;

  void set_vertex_properties(
      label_t label_id, const std::vector<PropertyType>& types,
      const std::vector<StorageStrategy>& strategies = {});

  const std::vector<PropertyType>& get_vertex_properties(
      const std::string& label) const;

  const std::vector<PropertyType>& get_vertex_properties(label_t label) const;

  const std::vector<StorageStrategy>& get_vertex_storage_strategies(
      const std::string& label) const;

  size_t get_max_vnum(const std::string& label) const;

  bool exist(const std::string& src_label, const std::string& dst_label,
             const std::string& edge_label) const;

  const std::vector<PropertyType>& get_edge_properties(
      const std::string& src_label, const std::string& dst_label,
      const std::string& label) const;

  PropertyType get_edge_property(label_t src, label_t dst, label_t edge) const;

  bool valid_edge_property(const std::string& src_label,
                           const std::string& dst_label,
                           const std::string& label) const;

  EdgeStrategy get_outgoing_edge_strategy(const std::string& src_label,
                                          const std::string& dst_label,
                                          const std::string& label) const;

  EdgeStrategy get_incoming_edge_strategy(const std::string& src_label,
                                          const std::string& dst_label,
                                          const std::string& label) const;

  bool contains_edge_label(const std::string& label) const;

  label_t get_edge_label_id(const std::string& label) const;

  std::string get_vertex_label_name(label_t index) const;

  std::string get_edge_label_name(label_t index) const;

  // get the index of primary key in the original table schema.
  int32_t get_vertex_primary_key_ind(label_t index) const;

  PropertyType get_vertex_primary_key_type(label_t index) const;

  void Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer);

  void Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader);

  // Returns:
  // 0. Schema
  // 1. vertex label name and file path
  // 2. src label , dst label, edge label, src_column_id, dst_column_id, file
  // path
  static std::tuple<
      Schema, std::vector<std::pair<std::string, std::string>>,
      std::vector<std::tuple<std::string, std::string, std::string, int32_t,
                             int32_t, std::string>>,
      std::vector<std::string>, LoadConfig>
  LoadFromYaml(const std::string& schema_config,
               const std::string& load_config);

  bool Equals(const Schema& other) const;

 private:
  label_t vertex_label_to_index(const std::string& label);

  label_t edge_label_to_index(const std::string& label);

  uint32_t generate_edge_label(label_t src, label_t dst, label_t edge) const;

  IdIndexer<std::string, label_t> vlabel_indexer_;
  IdIndexer<std::string, label_t> elabel_indexer_;
  std::vector<std::vector<PropertyType>> vproperties_;
  std::vector<std::vector<std::string>> vprop_names_;
  std::vector<std::tuple<PropertyType, std::string, int32_t>> v_primary_keys_;
  std::vector<std::vector<StorageStrategy>> vprop_storage_;
  std::map<uint32_t, std::vector<PropertyType>> eproperties_;
  std::map<uint32_t, std::vector<std::string>> eprop_names_;
  std::map<uint32_t, EdgeStrategy> oe_strategy_;
  std::map<uint32_t, EdgeStrategy> ie_strategy_;
  std::vector<size_t> max_vnum_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_SCHEMA_H_
