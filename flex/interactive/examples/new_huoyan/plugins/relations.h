#include <string>
#include <unordered_map>
#include <vector>

#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/app/interactive_app_base.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/storages/rt_mutable_graph/types.h"

#include "nlohmann/json.hpp"

namespace gs {

// TODO: make sure the max vid is less than 2^30
inline vid_t encode_vid(label_t v_label, vid_t vid) {
  // vid_t is uint32_t, use the first 4 bits to store label id
  return ((vid_t) v_label << 30) | vid;
}

inline label_t decode_label(vid_t encoded_vid) { return encoded_vid >> 30; }

inline vid_t decode_vid(vid_t encoded_vid) { return encoded_vid & 0x3FFFFFFF; }

inline int64_t get_oid_from_encoded_vid(ReadTransaction& txn,
                                        vid_t encoded_vid) {
  auto label = decode_label(encoded_vid);
  auto vid = decode_vid(encoded_vid);
  return txn.GetVertexId(label, vid).AsInt64();
}

inline std::string rel_type_to_string(int64_t rel_type) {
  if (rel_type == 0) {
    return "invest";
  } else if (rel_type == 1) {
    return "shareholder";
  } else if (rel_type == 2) {
    return "shareholder_his";
  } else if (rel_type == 3) {
    return "legalperson";
  } else if (rel_type == 4) {
    return "legalperson_his";
  } else if (rel_type == 5) {
    return "executive";
  } else if (rel_type == 6) {
    return "executive_his";
  } else {
    LOG(ERROR) << "Unknown rel type: " << rel_type;
    return "unknown";
  }
}

struct Path {
  std::vector<uint32_t> vids;
  std::vector<int32_t> rel_types;
  std::vector<double> weights;
  std::vector<std::string_view> rel_infos;
  std::vector<Direction> directions;
  // The size of rel_types is one less than the size of vids.
};

// Contains all path to the end node.
struct Results {
  uint32_t start_node_id;
  std::unordered_map<uint32_t, std::vector<Path>> path_to_end_node;
};

struct ResultsCreator {
  label_t comp_label_id_;
  label_t person_label_id_;

  ResultsCreator(
      label_t comp_label_id, label_t person_label_id,
      std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col,
      std::shared_ptr<TypedColumn<int64_t>> typed_comp_status_col,
      std::shared_ptr<TypedColumn<std::string_view>> typed_comp_credit_code_col,
      std::shared_ptr<TypedColumn<std::string_view>>
          typed_comp_license_number_col,
      std::shared_ptr<TypedColumn<std::string_view>> typed_person_named_col)
      : comp_label_id_(comp_label_id),
        person_label_id_(person_label_id),
        typed_comp_named_col_(typed_comp_named_col),
        typed_comp_status_col_(typed_comp_status_col),
        typed_comp_credit_code_col_(typed_comp_credit_code_col),
        typed_comp_license_number_col_(typed_comp_license_number_col),
        typed_person_named_col_(typed_person_named_col) {}

  inline std::string get_vertex_label_str_from_encoded_vid(
      vid_t encoded_vid) const {
    auto label = decode_label(encoded_vid);
    if (label == comp_label_id_) {
      return "company";
    } else if (label == person_label_id_) {
      return "oc_person";
    } else {
      return "unknown";
    }
  }

  inline nlohmann::json get_vertex_properties_from_encoded_vid(
      ReadTransaction& txn, vid_t encoded_vid) const {
    auto label = decode_label(encoded_vid);
    auto vid = decode_vid(encoded_vid);
    auto oid = get_oid_from_encoded_vid(txn, encoded_vid);
    nlohmann::json properties;
    if (label == comp_label_id_) {
      properties["label"] = "company";
      properties["status"] = typed_comp_status_col_->get_view(vid);
      properties["credit_code"] = typed_comp_credit_code_col_->get_view(vid);
      properties["license_number"] =
          typed_comp_license_number_col_->get_view(vid);
    } else if (label == person_label_id_) {
      properties["label"] = "oc_person";
      auto person_name = typed_person_named_col_->get_view(vid);
      properties["status"] = "";
      properties["credit_code"] = "";
      properties["license_number"] = "";
    } else {
      throw std::runtime_error("Invalid label");
    }
    return properties;
  }

  inline std::string_view get_vertex_name_from_encoded_vid(
      vid_t encoded_vid) const {
    auto label = decode_label(encoded_vid);
    auto vid = decode_vid(encoded_vid);
    if (label == comp_label_id_) {
      auto comp_name = typed_comp_named_col_->get_view(vid);
      return comp_name;
    } else if (label == person_label_id_) {
      auto person_name = typed_person_named_col_->get_view(vid);
      return person_name;
    } else {
      throw std::runtime_error("Invalid label");
    }
  }

  // TODO: support multiple properties on edge.
  bool add_result(const std::vector<vid_t>& cur_path,
                  const std::vector<double>& weights,
                  const std::vector<int32_t>& rel_types,
                  const std::vector<std::string_view>& rel_infos,
                  const std::vector<Direction>& directions) {
    if (cur_path.size() < 2) {
      LOG(ERROR) << "Path size is less than 2";
      return false;
    }
    if (rel_types.size() + 1 != cur_path.size()) {
      LOG(ERROR) << "miss match between rel_types and cur_path size:"
                 << rel_types.size() << " " << cur_path.size();
    }
    if (directions.size() + 1 != cur_path.size()) {
      LOG(ERROR) << "miss match between directions and cur_path size:"
                 << directions.size() << " " << cur_path.size();
    }
    auto start_node_id = cur_path[0];
    auto end_node_id = cur_path.back();
    auto iter = results_.path_to_end_node.find(end_node_id);
    if (iter == results_.path_to_end_node.end()) {
      results_.path_to_end_node[end_node_id] = std::vector<Path>();
    }
    Path path;
    path.vids = cur_path;
    path.weights = weights;
    path.rel_types = rel_types;
    path.rel_infos = rel_infos;
    path.directions = directions;
    LOG(INFO) << "emplace path: " << gs::to_string(path.vids) << ", "
              << gs::to_string(path.weights) << ", "
              << gs::to_string(path.rel_types) << ", "
              << gs::to_string(path.rel_infos) << ", "
              << gs::to_string(path.directions);
    results_.path_to_end_node[end_node_id].push_back(path);
    return true;
  }

  inline std::string build_edge_id(uint32_t encoded_start_id,
                                   uint32_t end_vid) const {
    return std::to_string(encoded_start_id) + "->" + std::to_string(end_vid);
  }

  inline nlohmann::json get_edge_properties(
      double weight, int64_t rel_type, const std::string_view& rel_info) const {
    nlohmann::json properties;
    properties["type"] = rel_type_to_string(rel_type);
    properties["weight"] = weight;
    properties["rel_info"] = rel_info;
    return properties;
  }

  std::string get_result_as_json_string(ReadTransaction& txn) const {
    nlohmann::json json = nlohmann::json::array();
    auto start_node_name =
        get_vertex_name_from_encoded_vid(results_.start_node_id);
    for (const auto& [end_node_id, paths_vec] : results_.path_to_end_node) {
      auto end_node_name = get_vertex_name_from_encoded_vid(end_node_id);
      nlohmann::json end_node_json;
      end_node_json["endNodeName"] = end_node_name;
      end_node_json["startNodeName"] = start_node_name;
      uint32_t prev_oid;
      std::string prev_name;
      nlohmann::json paths = nlohmann::json::array();
      LOG(INFO) << "paths vec size:" << paths_vec.size();
      for (const auto& path : paths_vec) {
        nlohmann::json path_json = nlohmann::json::object();
        path_json["relations"] = nlohmann::json::array();
        path_json["nodes"] = nlohmann::json::array();
        for (size_t i = 0; i < path.vids.size(); i++) {
          nlohmann::json node_json;
          prev_oid = node_json["id"] =
              get_oid_from_encoded_vid(txn, path.vids[i]);
          prev_name = node_json["name"] =
              get_vertex_name_from_encoded_vid(path.vids[i]);

          node_json["label"] =
              get_vertex_label_str_from_encoded_vid(path.vids[i]);
          node_json["properties"] =
              get_vertex_properties_from_encoded_vid(txn, path.vids[i]);
          path_json["nodes"].push_back(node_json);
          VLOG(10) << "node_json: " << node_json.dump();

          if (i < path.rel_types.size()) {
            nlohmann::json rel_json;
            rel_json["type"] = path.rel_types[i];  // todo: rel type to string
            auto& dir = path.directions[i];
            if (dir == Direction::Out) {
              rel_json["startNode"] = prev_name;
              rel_json["id"] = build_edge_id(
                  prev_oid, get_oid_from_encoded_vid(txn, path.vids[i + 1]));
              rel_json["endNode"] =
                  get_vertex_name_from_encoded_vid(path.vids[i + 1]);
              rel_json["properties"] = get_edge_properties(
                  path.weights[i], path.rel_types[i], path.rel_infos[i]);
            } else {
              rel_json["startNode"] =
                  get_vertex_name_from_encoded_vid(path.vids[i + 1]);
              rel_json["id"] = build_edge_id(
                  get_oid_from_encoded_vid(txn, path.vids[i + 1]), prev_oid);
              rel_json["endNode"] = prev_name;
              rel_json["properties"] = get_edge_properties(
                  path.weights[i], path.rel_types[i], path.rel_infos[i]);
            }
            VLOG(10) << "rel json: " << rel_json.dump();
            path_json["relations"].push_back(rel_json);
          }
        }
        // json["paths"].push_back(path_json);
        paths.push_back(path_json);
        VLOG(10) << "path_json: " << path_json.dump();
      }
      end_node_json["paths"] = paths;
      json.push_back(end_node_json);
    }
    return json.dump();
  }

  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col_;
  std::shared_ptr<TypedColumn<int64_t>> typed_comp_status_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_credit_code_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_license_number_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_person_named_col_;

  Results results_;  // The results of the query.
};

}  // namespace gs
