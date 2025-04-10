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

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/id_indexer.h"
#include "flex/utils/property/table.h"
#include "flex/utils/property/types.h"
#include "flex/utils/result.h"
#include "flex/utils/yaml_utils.h"

namespace gs {

class Schema {
 public:
  // How many built-in plugins are there.
  // Currently only one builtin plugin, SERVER_APP is supported.
  static constexpr uint8_t RESERVED_PLUGIN_NUM = 1;
  static constexpr uint8_t MAX_PLUGIN_ID = 245;
  static constexpr uint8_t ADHOC_READ_PLUGIN_ID = 253;
  static constexpr uint8_t HQPS_ADHOC_READ_PLUGIN_ID = 254;
  static constexpr uint8_t HQPS_ADHOC_WRITE_PLUGIN_ID = 255;

  static constexpr uint8_t CYPHER_READ_PLUGIN_ID = 248;
  static constexpr uint8_t CYPHER_WRITE_PLUGIN_ID = 247;
  static constexpr uint8_t CYPHER_READ_DEBUG_PLUGIN_ID = 246;
  static constexpr const char* HQPS_ADHOC_READ_PLUGIN_ID_STR = "\xFE";
  static constexpr const char* HQPS_ADHOC_WRITE_PLUGIN_ID_STR = "\xFF";
  static constexpr const char* ADHOC_READ_PLUGIN_ID_STR = "\xFD";
  static constexpr const char* CYPHER_READ_DEBUG_PLUGIN_ID_STR = "\xF6";
  static constexpr const char* PRIMITIVE_TYPE_KEY = "primitive_type";
  static constexpr const char* VARCHAR_KEY = "varchar";
  static constexpr const char* MAX_LENGTH_KEY = "max_length";

  // The builtin plugins are reserved for the system.
  static constexpr uint8_t BUILTIN_PLUGIN_NUM = 4;

  static constexpr uint8_t BUILTIN_COUNT_VERTICES_PLUGIN_ID = 252;
  static constexpr const char* BUILTIN_COUNT_VERTICES_PLUGIN_NAME =
      "count_vertices";
  static constexpr uint8_t BUILTIN_PAGERANK_PLUGIN_ID = 251;
  static constexpr const char* BUILTIN_PAGERANK_PLUGIN_NAME = "pagerank";
  static constexpr uint8_t BUILTIN_K_DEGREE_NEIGHBORS_PLUGIN_ID = 250;
  static constexpr const char* BUILTIN_K_DEGREE_NEIGHBORS_PLUGIN_NAME =
      "k_neighbors";
  static constexpr uint8_t BUILTIN_TVSP_PLUGIN_ID = 249;
  static constexpr const char* BUILTIN_TVSP_PLUGIN_NAME =
      "shortest_path_among_three";
  static constexpr const char* BUILTIN_PLUGIN_NAMES[BUILTIN_PLUGIN_NUM] = {
      BUILTIN_COUNT_VERTICES_PLUGIN_NAME, BUILTIN_PAGERANK_PLUGIN_NAME,
      BUILTIN_K_DEGREE_NEIGHBORS_PLUGIN_NAME, BUILTIN_TVSP_PLUGIN_NAME};
  static constexpr uint8_t BUILTIN_PLUGIN_IDS[BUILTIN_PLUGIN_NUM] = {
      BUILTIN_COUNT_VERTICES_PLUGIN_ID, BUILTIN_PAGERANK_PLUGIN_ID,
      BUILTIN_K_DEGREE_NEIGHBORS_PLUGIN_ID, BUILTIN_TVSP_PLUGIN_ID};

  // An array containing all compatible versions of schema.
  static const std::vector<std::string> COMPATIBLE_VERSIONS;
  static constexpr const char* DEFAULT_SCHEMA_VERSION = "v0.0";

  static bool IsBuiltinPlugin(const std::string& plugin_name);

  using label_type = label_t;
  Schema();
  ~Schema();

  static const std::vector<std::string>& GetCompatibleVersions();

  void Clear();

  void add_vertex_label(
      const std::string& label, const std::vector<PropertyType>& property_types,
      const std::vector<std::string>& property_names,
      const std::vector<std::tuple<PropertyType, std::string, size_t>>&
          primary_key,
      const std::vector<StorageStrategy>& strategies = {},
      size_t max_vnum = static_cast<size_t>(1) << 32,
      const std::string& description = "");

  void add_edge_label(const std::string& src_label,
                      const std::string& dst_label,
                      const std::string& edge_label,
                      const std::vector<PropertyType>& properties,
                      const std::vector<std::string>& prop_names,
                      EdgeStrategy oe = EdgeStrategy::kMultiple,
                      EdgeStrategy ie = EdgeStrategy::kMultiple,
                      bool oe_mutable = true, bool ie_mutable = true,
                      bool sort_on_compaction = false,
                      const std::string& description = "");

  label_t vertex_label_num() const;

  label_t edge_label_num() const;

  bool contains_vertex_label(const std::string& label) const;

  label_t get_vertex_label_id(const std::string& label) const;

  void set_vertex_properties(
      label_t label_id, const std::vector<PropertyType>& types,
      const std::vector<StorageStrategy>& strategies = {});

  const std::vector<PropertyType>& get_vertex_properties(
      const std::string& label) const;

  const std::vector<std::string>& get_vertex_property_names(
      const std::string& label) const;

  const std::string& get_vertex_description(const std::string& label) const;

  const std::vector<PropertyType>& get_vertex_properties(label_t label) const;

  const std::vector<std::string>& get_vertex_property_names(
      label_t label) const;

  const std::string& get_vertex_description(label_t label) const;

  const std::vector<StorageStrategy>& get_vertex_storage_strategies(
      const std::string& label) const;

  size_t get_max_vnum(const std::string& label) const;

  bool exist(const std::string& src_label, const std::string& dst_label,
             const std::string& edge_label) const;

  bool exist(label_type src_label, label_type dst_label,
             label_type edge_label) const;

  const std::vector<PropertyType>& get_edge_properties(
      const std::string& src_label, const std::string& dst_label,
      const std::string& label) const;

  const std::vector<PropertyType>& get_edge_properties(label_t src_label,
                                                       label_t dst_label,
                                                       label_t label) const;

  const std::string& get_compiler_path() const;

  void set_compiler_path(const std::string& compiler_path);

  std::string get_edge_description(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& label) const;

  std::string get_edge_description(label_t src_label, label_t dst_label,
                                   label_t label) const;

  PropertyType get_edge_property(label_t src, label_t dst, label_t edge) const;

  const std::vector<std::string>& get_edge_property_names(
      const std::string& src_label, const std::string& dst_label,
      const std::string& label) const;

  const std::vector<std::string>& get_edge_property_names(
      const label_t& src_label, const label_t& dst_label,
      const label_t& label) const;

  bool vertex_has_property(const std::string& label,
                           const std::string& prop) const;

  bool vertex_has_primary_key(const std::string& label,
                              const std::string& prop) const;

  bool edge_has_property(const std::string& src_label,
                         const std::string& dst_label,
                         const std::string& edge_label,
                         const std::string& prop) const;

  bool has_vertex_label(const std::string& label) const;

  bool has_edge_label(const std::string& src_label,
                      const std::string& dst_label,
                      const std::string& edge_label) const;

  bool has_edge_label(label_t src_label, label_t dst_label,
                      label_t edge_label) const;

  bool valid_edge_property(const std::string& src_label,
                           const std::string& dst_label,
                           const std::string& label) const;

  EdgeStrategy get_outgoing_edge_strategy(const std::string& src_label,
                                          const std::string& dst_label,
                                          const std::string& label) const;

  EdgeStrategy get_incoming_edge_strategy(const std::string& src_label,
                                          const std::string& dst_label,
                                          const std::string& label) const;

  inline EdgeStrategy get_outgoing_edge_strategy(label_t src_label,
                                                 label_t dst_label,
                                                 label_t label) const {
    uint32_t index = generate_edge_label(src_label, dst_label, label);
    return oe_strategy_.at(index);
  }

  inline EdgeStrategy get_incoming_edge_strategy(label_t src_label,
                                                 label_t dst_label,
                                                 label_t label) const {
    uint32_t index = generate_edge_label(src_label, dst_label, label);
    return ie_strategy_.at(index);
  }

  bool outgoing_edge_mutable(const std::string& src_label,
                             const std::string& dst_label,
                             const std::string& label) const;

  bool incoming_edge_mutable(const std::string& src_label,
                             const std::string& dst_label,
                             const std::string& label) const;

  bool get_sort_on_compaction(const std::string& src_label,
                              const std::string& dst_label,
                              const std::string& label) const;

  bool contains_edge_label(const std::string& label) const;

  label_t get_edge_label_id(const std::string& label) const;

  std::string get_vertex_label_name(label_t index) const;

  std::string get_edge_label_name(label_t index) const;

  const std::vector<std::tuple<PropertyType, std::string, size_t>>&
  get_vertex_primary_key(label_t index) const;

  const std::string& get_vertex_primary_key_name(label_t index) const;

  void Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer) const;

  void Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader);

  static Result<Schema> LoadFromYaml(const std::string& schema_config);

  static Result<Schema> LoadFromYamlNode(const YAML::Node& schema_node);

  bool Equals(const Schema& other) const;

  // Return the map from plugin name to plugin id
  const std::unordered_map<std::string, std::pair<std::string, uint8_t>>&
  GetPlugins() const;

  bool EmplacePlugins(
      const std::vector<std::pair<std::string, std::string>>& plugins);

  void SetPluginDir(const std::string& plugin_dir);

  void RemovePlugin(const std::string& plugin_name);

  std::string GetPluginDir() const;

  inline void SetGraphName(const std::string& name) { name_ = name; }

  inline void SetGraphId(const std::string& id) { id_ = id; }

  inline std::string GetGraphName() const { return name_; }

  inline std::string GetGraphId() const { return id_; }

  std::string GetDescription() const;

  void SetDescription(const std::string& description);

  void SetRemotePath(const std::string& remote_path);

  inline std::string GetRemotePath() const { return remote_path_; }

  void SetVersion(const std::string& version);

  std::string GetVersion() const;

  bool has_multi_props_edge() const;

  const std::unordered_map<std::string, std::pair<PropertyType, uint8_t>>&
  get_vprop_name_to_type_and_index(label_t label) const;

 private:
  label_t vertex_label_to_index(const std::string& label);

  label_t edge_label_to_index(const std::string& label);

  uint32_t generate_edge_label(label_t src, label_t dst, label_t edge) const;

  std::string name_, id_;
  IdIndexer<std::string, label_t> vlabel_indexer_;
  IdIndexer<std::string, label_t> elabel_indexer_;
  std::vector<std::vector<PropertyType>> vproperties_;
  std::vector<std::vector<std::string>> vprop_names_;
  std::vector<std::string> v_descriptions_;
  std::vector<std::vector<std::tuple<PropertyType, std::string, size_t>>>
      v_primary_keys_;  // the third element is the index of the property in the
                        // vertex property list
  std::vector<std::vector<StorageStrategy>> vprop_storage_;
  std::map<uint32_t, std::vector<PropertyType>> eproperties_;
  std::map<uint32_t, std::vector<std::string>> eprop_names_;
  std::map<uint32_t, std::string> e_descriptions_;
  std::map<uint32_t, EdgeStrategy> oe_strategy_;
  std::map<uint32_t, EdgeStrategy> ie_strategy_;
  std::map<uint32_t, bool> oe_mutability_;
  std::map<uint32_t, bool> ie_mutability_;
  std::map<uint32_t, bool> sort_on_compactions_;
  std::vector<std::unordered_map<std::string, std::pair<PropertyType, uint8_t>>>
      vprop_name_to_type_and_index_;
  std::vector<size_t> max_vnum_;
  std::unordered_map<std::string, std::pair<std::string, uint8_t>>
      plugin_name_to_path_and_id_;  // key is plugin_name, value is plugin_path
                                    // and plugin_id
  std::string plugin_dir_;
  std::string description_;
  std::string version_;
  std::string compiler_path_;
  std::string remote_path_;  // The path to the data on the remote storage
  bool has_multi_props_edge_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_FRAGMENT_SCHEMA_H_
