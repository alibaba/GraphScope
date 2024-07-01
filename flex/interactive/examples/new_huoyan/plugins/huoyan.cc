// #include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/app/interactive_app_base.h"
#include "grape/util.h"
#include "nlohmann/json.hpp"

#include "relations.h"

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
namespace gs {
class HuoYan : public WriteAppBase {
 public:
  static constexpr double timeout_sec = 15;
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
                   std::vector<std::vector<Direction>> cur_directions,
                   std::vector<std::vector<vid_t>>& next_paths,
                   std::vector<std::vector<double>>& next_weights,
                   std::vector<std::vector<int32_t>>& next_rel_types,
                   std::vector<std::vector<std::string_view>>& next_rel_infos,
                   std::vector<std::vector<Direction>> next_directions,
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
      VLOG(10) << data[0].AsDouble() << "," << edge_rel_type << ","
               << data[2].AsStringView();
      cur_weight.emplace_back(data[0].AsDouble());
      cur_rel_type.emplace_back();
      cur_rel_info.emplace_back(data[2].AsStringView());

      if (is_simple(cur_path)) {
        next_paths.emplace_back(cur_path);
        next_weights.emplace_back(cur_weight);
        next_rel_types.emplace_back(cur_rel_type);
        next_rel_infos.emplace_back(cur_rel_info);

        next_directions.emplace_back(cur_direction);
        if ((dst_label_id == comp_label_id_ && valid_comp_vids_[dst]) ||
            dst_label_id == person_label_id_) {
          // final_results.emplace_back(path);
          ++result_size;
          // output.put_int(cur_rel_type.size());
          if (cur_path.size() != cur_rel_type.size() + 1) {
            throw std::runtime_error("Error Internal state");
          }
          VLOG(10) << "put path of size: " << cur_rel_type.size();
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
            output.put_string(results_creator_->get_result_as_json_string(txn));
            txn.Commit();
            for (auto& vid : vid_vec) {
              valid_comp_vids_[vid] = false;
            }
            return true;
          }
        }
      }
      cur_path.pop_back();
      cur_rel_type.pop_back();
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

    LOG(INFO) << "company num:" << num;
    LOG(INFO) << "person num: " << graph.graph().vertex_num(person_label_id_);
    valid_comp_vids_.resize(num, false);
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

    double cur_time_left = timeout_sec;

    auto txn = graph.GetReadTransaction();
    int32_t hop_limit = input.get_int();
    int32_t result_limit = input.get_int();
    LOG(INFO) << "result limit: " << result_limit << "\n";
    int32_t rel_type_num = input.get_int();
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
    for (auto& vid : vid_vec) {
      cur_paths.emplace_back(
          std::vector<vid_t>{encode_vid(comp_label_id_, start_vid)});
      cur_rel_types.emplace_back(std::vector<int32_t>{});
      cur_weights.emplace_back(std::vector<double>{});
      cur_rel_infos.emplace_back(std::vector<std::string_view>{});
      cur_directions.emplace_back(std::vector<Direction>{});
    }
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
      cur_weights.swap(cur_weights);
      cur_rel_infos.swap(cur_rel_infos);
      cur_directions.swap(next_directions);
      next_paths.clear();
      next_rel_types.clear();
      next_weights.clear();
      next_rel_infos.clear();
      next_directions.clear();
    }

    LOG(INFO) << "result size: " << result_size;
    output.put_string(results_creator_->get_result_as_json_string(txn));
    txn.Commit();
    for (auto& vid : vid_vec) {
      valid_comp_vids_[vid] = false;
    }

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
