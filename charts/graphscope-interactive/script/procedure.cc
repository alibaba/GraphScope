// #include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/app/interactive_app_base.h"
#include "grape/util.h"
#include "nlohmann/json.hpp"

namespace gs {

// TODO: make sure the max vid is less than 2^30
inline vid_t encode_vid(label_t v_label, vid_t vid) {
  // vid_t is uint32_t, use the first 4 bits to store label id
  return ((vid_t) v_label << 31) | vid;
}

inline label_t decode_label(vid_t encoded_vid) { return encoded_vid >> 31; }

inline vid_t decode_vid(vid_t encoded_vid) { return encoded_vid & 0x7FFFFFFF; }

inline int64_t get_oid_from_encoded_vid(ReadTransaction& txn,
                                        vid_t encoded_vid) {
  auto label = decode_label(encoded_vid);
  auto vid = decode_vid(encoded_vid);
  CHECK(encode_vid(label, vid) == encoded_vid)
      << "vid: " << encoded_vid << ", label " << (int32_t) label
      << ", local id " << vid;
  return txn.GetVertexId(label, vid).AsInt64();
}

inline std::string status_to_str(int64_t status_code) {
  if (status_code == 1) {
    return "在营";
  } else if (status_code == 0) {
    return "注销";
  } else
    return std::to_string(status_code);
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

  void clear() { path_to_end_node.clear(); }
};

struct ResultsCreator {
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

  void set_start_vid(uint32_t start_vid) { results_.start_node_id = start_vid; }

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
      properties["status"] =
          status_to_str(typed_comp_status_col_->get_view(vid));
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
    // LOG(INFO) << "emplace path: " << gs::to_string(path.vids) << ", "
    //            << gs::to_string(path.weights) << ", "
    //            << gs::to_string(path.rel_types) << ", "
    //            << gs::to_string(path.rel_infos) << ", "
    //            << gs::to_string(path.directions);
    results_.path_to_end_node[end_node_id].push_back(path);
    return true;
  }

  inline std::string build_edge_id(int64_t encoded_start_id,
                                   int64_t end_vid) const {
    return std::to_string(encoded_start_id) + "->" + std::to_string(end_vid);
  }

  inline nlohmann::json get_edge_properties(
      double weight, int64_t rel_type, const std::string_view& rel_info) const {
    nlohmann::json properties;
    properties["label"] = rel_type_to_string(rel_type);
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
      int64_t prev_oid;
      std::string prev_name;
      nlohmann::json paths = nlohmann::json::array();
      LOG(INFO) << "paths vec size:" << paths_vec.size();
      for (const auto& path : paths_vec) {
        nlohmann::json path_json = nlohmann::json::object();
        path_json["relationShips"] = nlohmann::json::array();
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

          if (i < path.rel_types.size()) {
            nlohmann::json rel_json;
            rel_json["type"] = rel_type_to_string(path.rel_types[i]);
            rel_json["name"] = rel_json["type"];
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
            path_json["relationShips"].push_back(rel_json);
          }
        }
        // json["paths"].push_back(path_json);
        paths.push_back(path_json);
        // VLOG(10) << "path_json: " << path_json.dump();
      }
      end_node_json["paths"] = paths;
      json.push_back(end_node_json);
    }
    return json.dump();
  }

  void clear() { results_.clear(); }

  label_t comp_label_id_;
  label_t person_label_id_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col_;
  std::shared_ptr<TypedColumn<int64_t>> typed_comp_status_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_credit_code_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_license_number_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_person_named_col_;

  Results results_;  // The results of the query.
};

/**
 *@brief Return the investigation path from the given company to the target.
 The input is 1 start company/person and a list of target companies.

  The rel_label(or rel_type) has the following mapping relation
  person-[]->company:
    1: shareholder；
    2: shareholder_his；
    3: legalperson；
    4: legalperson_his；
    5: executive；
    6: executive_his
  company-[]->company:
    0: invest
 */

class HuoYan : public WriteAppBase {
 public:
  static constexpr double timeout_sec = 60;
  static constexpr int32_t REL_TYPE_MAX = 8;  // 1 ~ 7
  HuoYan() : is_initialized_(false) {}
  ~HuoYan() {}
  bool is_simple(const std::vector<vid_t>& path) {
    // to check whether there are same vid in the path
    vis_.clear();
    for (auto& vid : path) {
      if (vis_.find(vid) != vis_.end()) {
        return false;
      }
      vis_.insert(vid);
    }
    return true;
  }

  bool edge_expand(gs::ReadTransaction& txn, const std::vector<vid_t>& vid_vec,
                   label_t dst_label_id, const AdjListView<RecordView>& edges,
                   const std::vector<bool>& valid_rel_type_ids, int32_t cur_ind,
                   std::vector<std::vector<vid_t>>& cur_paths,
                   std::vector<std::vector<double>>& cur_weights,
                   std::vector<std::vector<int32_t>>& cur_rel_types,
                   std::vector<std::vector<std::string_view>>& cur_rel_infos,
                   std::vector<std::vector<Direction>>& cur_directions,
                   std::vector<std::vector<vid_t>>& next_paths,
                   std::vector<std::vector<double>>& next_weights,
                   std::vector<std::vector<int32_t>>& next_rel_types,
                   std::vector<std::vector<std::string_view>>& next_rel_infos,
                   std::vector<std::vector<Direction>>& next_directions,
                   size_t& result_size, int result_limit, Encoder& output,
                   double& cur_time_left, Direction direction) {
    auto& cur_path = cur_paths[cur_ind];
    auto& cur_rel_type = cur_rel_types[cur_ind];
    auto& cur_weight = cur_weights[cur_ind];
    auto& cur_rel_info = cur_rel_infos[cur_ind];
    auto& cur_direction = cur_directions[cur_ind];
    double t0 = -grape::GetCurrentTime();
    // The direction is same for all edges.
    cur_direction.emplace_back(direction);
    for (auto& edge : edges) {
      auto dst = edge.get_neighbor();
      auto encoded_vid = encode_vid(dst_label_id, dst);
      auto data = edge.get_data();
      auto edge_rel_type = data[1].AsInt64();
      if (!valid_rel_type_ids[edge_rel_type]) {
        // filter edges by rel type
        continue;
      }
      cur_path.emplace_back(encoded_vid);
      CHECK(data.size() == 3) << "Expect 3 but got: " << data.size();
      // VLOG(10) << data[0].AsDouble() << "," << edge_rel_type << ","
      //          << data[2].AsStringView();
      cur_weight.emplace_back(data[0].AsDouble());
      cur_rel_type.emplace_back(edge_rel_type);
      cur_rel_info.emplace_back(data[2].AsStringView());

      // LOG(INFO) << "path is ? simple: " <<gs::to_string(cur_path) << "," <<
      // (int) is_simple(cur_path);
      if (is_simple(cur_path)) {
        next_paths.emplace_back(cur_path);
        next_weights.emplace_back(cur_weight);
        next_rel_types.emplace_back(cur_rel_type);
        next_rel_infos.emplace_back(cur_rel_info);

        next_directions.emplace_back(cur_direction);
        // LOG(INFO) << "label" << (int) dst_label_id << ",dst in valid comp: "
        // << (int) valid_comp_vids_[dst];
        if ((dst_label_id == comp_label_id_ && valid_comp_vids_[dst])) {
          // dst_label_id == person_label_id_
          // final_results.emplace_back(path);
          ++result_size;
          // output.put_int(cur_rel_type.size());
          if (cur_path.size() != cur_rel_type.size() + 1) {
            throw std::runtime_error("Error Internal state");
          }
          // VLOG(10) << "put path of size: " << cur_rel_type.size();
          if (!results_creator_->add_result(cur_path, cur_weight, cur_rel_type,
                                            cur_rel_info, cur_direction)) {
            LOG(ERROR) << "Failed to add result";
            return false;
          }
          // for (auto k = 0; k < cur_rel_type.size(); ++k) {
          //   output.put_long(get_oid_from_encoded_vid(txn, cur_path[k]));
          //   output.put_long(get_oid_from_encoded_vid(txn, cur_path[k + 1]));
          //   VLOG(10) << "put src id "
          //            << get_oid_from_encoded_vid(txn, cur_path[k])
          //            << ", dst id "
          //            << get_oid_from_encoded_vid(txn, cur_path[k + 1]);
          //   output.put_string_view(
          //       get_vertex_name_from_encoded_vid(cur_path[k]));
          //   output.put_string_view(
          //       get_vertex_name_from_encoded_vid(cur_path[k + 1]));
          //   VLOG(10) << "put name: "
          //            << get_vertex_name_from_encoded_vid(cur_path[k])
          //            << ", dst name "
          //            << get_vertex_name_from_encoded_vid(cur_path[k + 1]);
          //   output.put_int(cur_rel_type[k]);
          // }

          if (result_size >= result_limit) {
            // output.put_int_at(begin_loc, result_size);
            LOG(INFO) << "result limit exced: " << result_size;
            output.put_string(results_creator_->get_result_as_json_string(txn));
            txn.Commit();
            for (auto& vid : vid_vec) {
              valid_comp_vids_[vid] = false;
              results_creator_->clear();
            }
            return true;
          }
        }
      }
      cur_path.pop_back();
      cur_weight.pop_back();
      cur_rel_type.pop_back();
      cur_rel_info.pop_back();
    }
    cur_direction.pop_back();

    t0 += grape::GetCurrentTime();
    cur_time_left -= t0;
    if (cur_time_left < 0) {
      LOG(INFO) << "Timeout, result size: " << result_size;
      output.put_string(results_creator_->get_result_as_json_string(txn));
      txn.Commit();
      for (auto& vid : vid_vec) {
        valid_comp_vids_[vid] = false;
      }
      return true;
    }
    return false;
  }

  bool initialize(GraphDBSession& graph) {
    LOG(INFO) << "initializing...";
    comp_label_id_ = graph.schema().get_vertex_label_id("company");
    person_label_id_ = graph.schema().get_vertex_label_id("person");
    invest_label_id_ = graph.schema().get_edge_label_id("invest");
    person_invest_label_id_ = graph.schema().get_edge_label_id("personInvest");
    size_t num = graph.graph().vertex_num(comp_label_id_);
    valid_comp_vids_.resize(num, false);
    LOG(INFO) << "company num:" << num;
    LOG(INFO) << "person num: " << graph.graph().vertex_num(person_label_id_);
    auto comp_name_col =
        graph.get_vertex_property_column(comp_label_id_, "vertex_name");
    auto comp_status_col =
        graph.get_vertex_property_column(comp_label_id_, "status");
    auto comp_credit_code_col =
        graph.get_vertex_property_column(comp_label_id_, "credit_code");
    auto comp_license_number_col =
        graph.get_vertex_property_column(comp_label_id_, "license_number");
    auto person_name_col =
        graph.get_vertex_property_column(person_label_id_, "vertex_name");
    if (!comp_name_col) {
      LOG(ERROR) << "column vertex_name not found for company";
      return false;
    }
    if (!person_name_col) {
      LOG(ERROR) << "column vertex_name not found for person";
      return false;
    }
    if (!comp_status_col) {
      LOG(ERROR) << "column status not found for company";
      return false;
    }
    if (!comp_credit_code_col) {
      LOG(ERROR) << "column credit_code not found for company";
      return false;
    }
    if (!comp_license_number_col) {
      LOG(ERROR) << "column license_number not found for company";
      return false;
    }
    typed_comp_named_col_ =
        std::dynamic_pointer_cast<TypedColumn<std::string_view>>(comp_name_col);
    typed_comp_status_col_ =
        std::dynamic_pointer_cast<TypedColumn<int64_t>>(comp_status_col);
    typed_comp_credit_code_col_ =
        std::dynamic_pointer_cast<TypedColumn<std::string_view>>(
            comp_credit_code_col);
    typed_comp_license_number_col_ =
        std::dynamic_pointer_cast<TypedColumn<std::string_view>>(
            comp_license_number_col);
    typed_person_named_col_ =
        std::dynamic_pointer_cast<TypedColumn<std::string_view>>(
            person_name_col);
    if (!typed_comp_named_col_) {
      LOG(ERROR) << "column vertex_name is not string type for company";
    }
    if (!typed_person_named_col_) {
      LOG(ERROR) << "column vertex_name is not string type for person";
    }
    results_creator_ = std::make_shared<ResultsCreator>(
        comp_label_id_, person_label_id_, typed_comp_named_col_,
        typed_comp_status_col_, typed_comp_credit_code_col_,
        typed_comp_license_number_col_, typed_person_named_col_);
    is_initialized_.store(true);
    return true;
  }

#define DEBUG
  bool Query(GraphDBSession& graph, Decoder& input, Encoder& output) {
    //////////Initialization////////////////////////////
    if (!is_initialized_.load()) {
      if (!initialize(graph)) {
        LOG(ERROR) << "Failed to initialize";
        return false;
      } else {
        LOG(INFO) << "Successfully initialized";
      }
    } else {
      LOG(INFO) << "Already initialized";
    }
    ////////////Initialization///////////////////////////
    results_creator_->clear();
    double cur_time_left = timeout_sec;

    auto txn = graph.GetReadTransaction();
    int32_t hop_limit = input.get_int();
    int32_t result_limit = input.get_int();
    int32_t rel_type_num = input.get_int();
    LOG(INFO) << "result limit: " << result_limit
              << " rel type num: " << rel_type_num;
    // valid rel type ids
    std::vector<bool> valid_rel_type_ids(REL_TYPE_MAX, false);
    for (int i = 0; i < rel_type_num; ++i) {
      auto rel_type = input.get_int();
      if (rel_type < 0 || rel_type >= REL_TYPE_MAX) {
        LOG(ERROR) << "Invalid rel type id: " << rel_type;
        return false;
      }
      valid_rel_type_ids[rel_type] = true;
    }
    // Get the start node id.
    auto start_oid = input.get_long();
    LOG(INFO) << "Got start oid: " << start_oid;
    vid_t start_vid;
    if (!txn.GetVertexIndex(comp_label_id_, Any::From(start_oid), start_vid)) {
      LOG(ERROR) << "Start oid: " << start_oid << ", not found";
      return false;
    }
    results_creator_->set_start_vid(encode_vid(comp_label_id_, start_vid));
    LOG(INFO) << "start vid: " << start_vid;

    int32_t vec_size = input.get_int();
    LOG(INFO) << "Group Query: hop limit " << hop_limit << ", result limit "
              << result_limit << ", ids size " << vec_size;
    std::vector<vid_t> vid_vec;
    int count = 0;

    for (int i = 0; i < vec_size; ++i) {
      auto oid = input.get_long();
      // std::string_view oid = input.get_string();
      vid_t vid;
      if (!txn.GetVertexIndex(comp_label_id_, Any::From(oid), vid)) {
        LOG(INFO) << "Get oid: " << oid << ", not found";
        count++;
      } else {
        VLOG(10) << "Oid: " << oid << ", vid:" << vid;
        auto encoded_vid = encode_vid(comp_label_id_, vid);
#ifdef DEBUG
        auto label = decode_label(encoded_vid);
        auto vid2 = decode_vid(encoded_vid);
        CHECK_EQ(label, comp_label_id_);
        CHECK_EQ(vid, vid2);
#endif
        vid_vec.emplace_back(vid);
      }
    }
    LOG(INFO) << count << " out of " << vec_size << " vertices not found";
    for (auto& vid : vid_vec) {
      valid_comp_vids_[vid] = true;
    }

    auto cmp_invest_outgoing_view = txn.GetOutgoingGraphView<RecordView>(
        comp_label_id_, comp_label_id_, invest_label_id_);
    auto cmp_invest_incoming_view = txn.GetIncomingGraphView<RecordView>(
        comp_label_id_, comp_label_id_, invest_label_id_);

    auto person_invest_outgoing_view = txn.GetOutgoingGraphView<RecordView>(
        person_label_id_, comp_label_id_, person_invest_label_id_);
    auto person_invest_incoming_view = txn.GetIncomingGraphView<RecordView>(
        comp_label_id_, person_label_id_, person_invest_label_id_);

    // Expand from vid_vec, until end_vertex is valid, or hop limit is reached.
    std::vector<std::vector<vid_t>> cur_paths;
    std::vector<std::vector<double>> cur_weights;
    std::vector<std::vector<int32_t>> cur_rel_types;
    std::vector<std::vector<std::string_view>> cur_rel_infos;
    std::vector<std::vector<Direction>> cur_directions;

    std::vector<std::vector<vid_t>> next_paths;
    std::vector<std::vector<double>> next_weights;
    std::vector<std::vector<int32_t>> next_rel_types;
    std::vector<std::vector<std::string_view>> next_rel_infos;
    std::vector<std::vector<Direction>> next_directions;
    // init cur_paths
    cur_paths.emplace_back(
        std::vector<vid_t>{encode_vid(comp_label_id_, start_vid)});
    cur_rel_types.emplace_back(std::vector<int32_t>{});
    cur_weights.emplace_back(std::vector<double>{});
    cur_rel_infos.emplace_back(std::vector<std::string_view>{});
    cur_directions.emplace_back(std::vector<Direction>{});
    // size_t begin_loc = output.skip_int();
    size_t result_size = 0;
    for (auto i = 1; i <= hop_limit; ++i) {
      VLOG(10) << "hop: " << i;
      // possible edges:
      // company->company
      // person->company
      // company->person

      for (auto j = 0; j < cur_paths.size(); ++j) {
        // VLOG(10) << "path: " << gs::to_string(cur_paths[j]);
        auto last_vid_encoded = cur_paths[j].back();
        auto last_vid = decode_vid(last_vid_encoded);
        auto label = decode_label(last_vid_encoded);
        if (label == comp_label_id_) {
          const auto& oedges = cmp_invest_outgoing_view.get_edges(last_vid);
          if (edge_expand(
                  txn, vid_vec, comp_label_id_, oedges, valid_rel_type_ids, j,
                  cur_paths, cur_weights, cur_rel_types, cur_rel_infos,
                  cur_directions, next_paths, next_weights, next_rel_types,
                  next_rel_infos, next_directions, result_size, result_limit,
                  output, cur_time_left, Direction::Out)) {
            return true;  // early terminate.
          }

          const auto& iedges = cmp_invest_incoming_view.get_edges(last_vid);
          if (edge_expand(txn, vid_vec, comp_label_id_, iedges,
                          valid_rel_type_ids, j, cur_paths, cur_weights,
                          cur_rel_types, cur_rel_infos, cur_directions,
                          next_paths, next_weights, next_rel_types,
                          next_rel_infos, next_directions, result_size,
                          result_limit, output, cur_time_left, Direction::In)) {
            return true;  // early terminate.
          }

          const auto& oedges2 = person_invest_incoming_view.get_edges(last_vid);
          if (edge_expand(txn, vid_vec, person_label_id_, oedges2,
                          valid_rel_type_ids, j, cur_paths, cur_weights,
                          cur_rel_types, cur_rel_infos, cur_directions,
                          next_paths, next_weights, next_rel_types,
                          next_rel_infos, next_directions, result_size,
                          result_limit, output, cur_time_left, Direction::In)) {
            return true;  // early terminate.
          }
        } else if (label == person_label_id_) {
          const auto& oedges = person_invest_outgoing_view.get_edges(last_vid);
          double t0 = -grape::GetCurrentTime();
          if (edge_expand(
                  txn, vid_vec, comp_label_id_, oedges, valid_rel_type_ids, j,
                  cur_paths, cur_weights, cur_rel_types, cur_rel_infos,
                  cur_directions, next_paths, next_weights, next_rel_types,
                  next_rel_infos, next_directions, result_size, result_limit,
                  output, cur_time_left, Direction::Out)) {
            return true;  // early terminate.
          }
        } else {
          LOG(ERROR) << "Invalid label: " << label;
          return false;
        }
      }
      // LOG(INFO) << "Hop: " << i << ", result: " << final_results.size()
      //         << ", cur_paths: " << cur_paths.size()
      //       << ", next_paths: " << next_paths.size();
      cur_paths.swap(next_paths);
      cur_rel_types.swap(next_rel_types);
      cur_weights.swap(next_weights);
      cur_rel_infos.swap(next_rel_infos);
      cur_directions.swap(next_directions);
      next_paths.clear();
      next_rel_types.clear();
      next_weights.clear();
      next_rel_infos.clear();
      next_directions.clear();
    }

    output.put_string(results_creator_->get_result_as_json_string(txn));
    txn.Commit();
    for (auto& vid : vid_vec) {
      valid_comp_vids_[vid] = false;
      results_creator_->clear();
    }
    LOG(INFO) << "result size: " << result_size;

    return true;
  }

 private:
  std::atomic<bool> is_initialized_;
  label_t comp_label_id_;
  label_t person_label_id_;
  label_t invest_label_id_;
  label_t person_invest_label_id_;
  // std::vector<bool> comp_vis_;
  // std::vector<bool> person_vis_;
  std::unordered_set<vid_t> vis_;
  std::vector<bool> valid_comp_vids_;

  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col_;
  std::shared_ptr<TypedColumn<int64_t>> typed_comp_status_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_credit_code_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_license_number_col_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_person_named_col_;

  std::shared_ptr<ResultsCreator> results_creator_;
};

#undef DEBUG

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::HuoYan* app = new gs::HuoYan();
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::HuoYan* casted = static_cast<gs::HuoYan*>(app);
  delete casted;
}
}

