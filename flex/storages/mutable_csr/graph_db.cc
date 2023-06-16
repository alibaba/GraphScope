#include "flex/storages/mutable_csr/graph_db.h"

#include <yaml-cpp/yaml.h>

namespace gs {

static std::string gen_random_str(const int len) {
  static const char alpha[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::string tmp_s;
  tmp_s.reserve(len);
  for (int i = 0; i < len; ++i) {
    tmp_s += alpha[rand() % (sizeof(alpha) - 1)];
  }
  return tmp_s;
}

GraphDB::GraphDB() = default;
GraphDB::~GraphDB() = default;

void GraphDB::Init(const std::string& graph_dir, const std::string& data_dir,
                   int thread_num) {
  std::filesystem::path data_dir_path(data_dir);
  if (!std::filesystem::exists(data_dir_path)) {
    std::filesystem::create_directory(data_dir_path);
  }
  std::filesystem::path serial_path = data_dir_path / "init_snapshot.bin";
  std::vector<std::string> plugins;
  if (!std::filesystem::exists(serial_path)) {
    loadFromRawFiles(graph_dir, thread_num);
    graph_.Serialize(data_dir_path.string());
  } else {
    graph_.Deserialize(data_dir_path);
  }
}
ReadTransaction GraphDB::GetReadTransaction() {
  uint32_t ts = version_manager_.acquire_read_timestamp();
  return {graph_, version_manager_, ts};
}

const TSPropertyFragment& GraphDB::graph() const { return graph_; }
TSPropertyFragment& GraphDB::graph() { return graph_; }

const Schema& GraphDB::schema() const { return graph_.schema(); }

const std::shared_ptr<ColumnBase> GraphDB::get_vertex_property_column(
    uint8_t label, const std::string& col_name) const {
  return graph_.get_vertex_table(label).get_column(col_name);
}

// When met non-existing property, just return nullptr.
std::shared_ptr<RefColumnBase> GraphDB::get_vertex_property_column_x(
    uint8_t label, const std::string& col_name) const {
  if (col_name == "id" || col_name == "ID" || col_name == "Id" ||
      col_name == "iD") {
    return std::make_shared<TypedRefColumn<int64_t>>(
        graph_.lf_indexers_[label].keys(), StorageStrategy::kMem);
  } else if (col_name == "label" || col_name == "Label" ||
             col_name == "LABEL") {
    return std::make_shared<LabelRefColumn>(label);
  } else {
    auto ptr = graph_.get_vertex_table(label).get_column(col_name);
    if (ptr) {
      return CreateRefColumn(ptr);
    } else
      return nullptr;
  }
}

namespace config_parsing {

template <typename T>
bool get_scalar(YAML::Node node, const std::string& key, T& value) {
  YAML::Node cur = node[key];
  if (cur && cur.IsScalar()) {
    value = cur.as<T>();
    return true;
  }
  return false;
}

template <typename T>
bool get_sequence(YAML::Node node, const std::string& key,
                  std::vector<T>& seq) {
  YAML::Node cur = node[key];
  if (cur && cur.IsSequence()) {
    int num = cur.size();
    seq.clear();
    for (int i = 0; i < num; ++i) {
      seq.push_back(cur[i].as<T>());
    }
    return true;
  }
  return false;
}

static bool expect_config(YAML::Node root, const std::string& key,
                          const std::string& value) {
  std::string got;
  if (!get_scalar(root, key, got)) {
    LOG(ERROR) << key << " not set properly...";
    return false;
  }
  if (got != value) {
    LOG(ERROR) << key << " - " << got << " is not supported...";
    return false;
  }
  return true;
}

static PropertyType StringToPropertyType(const std::string& str) {
  if (str == "int32") {
    return PropertyType::kInt32;
  } else if (str == "Date") {
    return PropertyType::kDate;
  } else if (str == "String") {
    return PropertyType::kString;
  } else if (str == "Browser") {
    return PropertyType::kBrowser;
  } else if (str == "IpAddr") {
    return PropertyType::kIpAddr;
  } else if (str == "Gender") {
    return PropertyType::kGender;
  } else if (str == "Empty") {
    return PropertyType::kEmpty;
  } else if (str == "int64") {
    return PropertyType::kInt64;
  } else {
    return PropertyType::kEmpty;
  }
}

EdgeStrategy StringToEdgeStrategy(const std::string& str) {
  if (str == "None") {
    return EdgeStrategy::kNone;
  } else if (str == "Single") {
    return EdgeStrategy::kSingle;
  } else if (str == "Multiple") {
    return EdgeStrategy::kMultiple;
  } else {
    return EdgeStrategy::kMultiple;
  }
}

StorageStrategy StringToStorageStrategy(const std::string& str) {
  if (str == "None") {
    return StorageStrategy::kNone;
  } else if (str == "Mem") {
    return StorageStrategy::kMem;
  } else {
    return StorageStrategy::kMem;
  }
}

static bool parse_vertex_properties(YAML::Node node,
                                    const std::string& label_name,
                                    std::vector<PropertyType>& types,
                                    std::vector<StorageStrategy>& strategies) {
  if (!node || !node.IsSequence()) {
    LOG(ERROR) << "properties of vertex-" << label_name
               << " not set properly... ";
    return false;
  }

  int prop_num = node.size();
  if (prop_num == 0) {
    LOG(ERROR) << "properties of vertex-" << label_name
               << " not set properly... ";
    return false;
  }

  if (!expect_config(node[0], "name", "_ID") ||
      !expect_config(node[0], "type", "int64")) {
    LOG(ERROR) << "the first property of vertex-" << label_name
               << " should be _ID with type int64";
    return false;
  }

  for (int i = 1; i < prop_num; ++i) {
    std::string prop_type_str, strategy_str;
    if (!get_scalar(node[i], "type", prop_type_str)) {
      LOG(ERROR) << "type of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    get_scalar(node[i], "storage_strategy", strategy_str);
    types.push_back(StringToPropertyType(prop_type_str));
    strategies.push_back(StringToStorageStrategy(strategy_str));
  }

  return true;
}

static bool parse_vertex_schema(
    YAML::Node node, Schema& schema, const std::string& prefix,
    std::vector<std::pair<std::string, std::string>>& files) {
  std::string label_name;
  if (!get_scalar(node, "label_name", label_name)) {
    return false;
  }
  size_t max_num = ((size_t) 1) << 32;
  get_scalar(node, "max_vertex_num", max_num);
  std::vector<PropertyType> property_types;
  std::vector<StorageStrategy> strategies;
  if (!parse_vertex_properties(node["properties"], label_name, property_types,
                               strategies)) {
    return false;
  }
  schema.add_vertex_label(label_name, property_types, strategies, max_num);
  std::vector<std::string> files_got;
  get_sequence(node, "files", files_got);
  for (auto& f : files_got) {
    if (f[0] == '/') {
      files.emplace_back(label_name, f);
    } else {
      files.emplace_back(label_name, prefix + f);
    }
  }

  return true;
}

static bool parse_vertices_schema(
    YAML::Node node, Schema& schema, const std::string& prefix,
    std::vector<std::pair<std::string, std::string>>& files) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "vertex is not set properly";
    return false;
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    if (!parse_vertex_schema(node[i], schema, prefix, files)) {
      return false;
    }
  }
  return true;
}

static bool parse_edge_properties(YAML::Node node,
                                  const std::string& label_name,
                                  std::vector<PropertyType>& types) {
  if (!node || !node.IsSequence()) {
    LOG(ERROR) << "properties of vertex-" << label_name
               << " not set properly... ";
    return false;
  }

  int prop_num = node.size();
  if (prop_num <= 1) {
    LOG(ERROR) << "properties of edge-" << label_name
               << " not set properly... ";
    return false;
  }

  if (!expect_config(node[0], "name", "_SRC") ||
      !expect_config(node[0], "type", "int64")) {
    LOG(ERROR) << "the first property of edge-" << label_name
               << " should be _SRC with type int64";
    return false;
  }
  if (!expect_config(node[1], "name", "_DST") ||
      !expect_config(node[0], "type", "int64")) {
    LOG(ERROR) << "the second property of edge-" << label_name
               << " should be _DST with type int64";
    return false;
  }

  for (int i = 2; i < prop_num; ++i) {
    std::string prop_type_str, strategy_str;
    if (!get_scalar(node[i], "type", prop_type_str)) {
      LOG(ERROR) << "type of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    types.push_back(StringToPropertyType(prop_type_str));
  }

  return true;
}

static bool parse_edge_schema(
    YAML::Node node, Schema& schema, const std::string& prefix,
    std::vector<std::tuple<std::string, std::string, std::string, std::string>>&
        files) {
  std::string src_label_name, dst_label_name, edge_label_name;
  if (!get_scalar(node, "src_label_name", src_label_name)) {
    return false;
  }
  if (!get_scalar(node, "dst_label_name", dst_label_name)) {
    return false;
  }
  if (!get_scalar(node, "edge_label_name", edge_label_name)) {
    return false;
  }
  std::vector<PropertyType> property_types;
  if (!parse_edge_properties(node["properties"], edge_label_name,
                             property_types)) {
    return false;
  }
  EdgeStrategy ie = EdgeStrategy::kMultiple;
  EdgeStrategy oe = EdgeStrategy::kMultiple;
  std::string ie_str, oe_str;
  if (get_scalar(node, "outgoing_edge_strategy", oe_str)) {
    oe = StringToEdgeStrategy(oe_str);
  }
  if (get_scalar(node, "incoming_edge_strategy", ie_str)) {
    ie = StringToEdgeStrategy(ie_str);
  }
  schema.add_edge_label(src_label_name, dst_label_name, edge_label_name,
                        property_types, oe, ie);
  std::vector<std::string> files_got;
  get_sequence(node, "files", files_got);
  for (auto& f : files_got) {
    if (f[0] == '/') {
      files.emplace_back(src_label_name, dst_label_name, edge_label_name, f);
    } else {
      files.emplace_back(src_label_name, dst_label_name, edge_label_name,
                         prefix + f);
    }
  }

  return true;
}

static bool parse_edges_schema(
    YAML::Node node, Schema& schema, const std::string& prefix,
    std::vector<std::tuple<std::string, std::string, std::string, std::string>>&
        files) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "edge is not set properly";
    return false;
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    if (!parse_edge_schema(node[i], schema, prefix, files)) {
      return false;
    }
  }

  return true;
}

static bool parse_config_file(
    const std::string& path, Schema& schema,
    std::vector<std::pair<std::string, std::string>>& vertex_files,
    std::vector<std::tuple<std::string, std::string, std::string, std::string>>&
        edge_files) {
  YAML::Node root = YAML::LoadFile(path);
  YAML::Node graph_node = root["graph"];
  if (!graph_node || !graph_node.IsMap()) {
    LOG(ERROR) << "graph is not set properly";
    return false;
  }
  if (!expect_config(graph_node, "file_format", "ldbc_snb")) {
    return false;
  }
  if (!expect_config(graph_node, "graph_store", "mutable_csr")) {
    return false;
  }

  if (!graph_node["vertex"]) {
    LOG(ERROR) << "vertex is not set";
    return false;
  }

  std::string graph_data_prefix;
  get_scalar(root, "graph_dir", graph_data_prefix);
  if (!graph_data_prefix.empty()) {
    if (*graph_data_prefix.rbegin() != '/') {
      graph_data_prefix += '/';
    }
  }

  if (!parse_vertices_schema(graph_node["vertex"], schema, graph_data_prefix,
                             vertex_files)) {
    return false;
  }

  if (graph_node["edge"]) {
    if (!parse_edges_schema(graph_node["edge"], schema, graph_data_prefix,
                            edge_files)) {
      return false;
    }
  }

  return true;
}

}  // namespace config_parsing

void GraphDB::loadFromRawFiles(const std::string& graph_dir, int thread_num) {
  Schema schema;
  std::vector<std::pair<std::string, std::string>> vertex_files;
  std::vector<std::tuple<std::string, std::string, std::string, std::string>>
      edge_files;
  if (!config_parsing::parse_config_file(graph_dir, schema, vertex_files,
                                         edge_files)) {
    LOG(FATAL) << "Parsing config file: " << graph_dir << " failed...";
  }

  graph_.Init(schema, vertex_files, edge_files, thread_num);
}

}  // namespace gs
