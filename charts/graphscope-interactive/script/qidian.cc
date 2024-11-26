#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "grape/util.h"

#define RAPIDJSON_HAS_STDSTRING 1

#include <rapidjson/document.h>
#include <rapidjson/pointer.h>
#include <rapidjson/rapidjson.h>
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace gs {

struct Path {
  std::vector<uint32_t> vids;
  std::vector<int64_t> rel_types;
  std::vector<double> weights;
  std::vector<Direction> directions;
  // The size of rel_types is one less than the size of vids.
};

struct CustomHash {
  size_t operator()(const std::pair<uint32_t, uint32_t>& key) const {
    return std::hash<uint32_t>()(key.first) ^ std::hash<uint32_t>()(key.second);
  }
};

struct Results {
  std::unordered_map<std::pair<uint32_t, uint32_t>, std::vector<Path>,
                     CustomHash>
      path_to_end_node;

  void clear() { path_to_end_node.clear(); }
};

std::string rel_type_to_string(int64_t rel_type_id) {
  if (rel_type_id == 0) {
    return "invest";
  } else if (rel_type_id == 1) {
    return "shareholder";
  } else if (rel_type_id == 2) {
    return "shareholder_his";
  } else if (rel_type_id == 3) {
    return "legalperson";
  } else if (rel_type_id == 4) {
    return "legalperson_his";
  } else if (rel_type_id == 5) {
    return "executive";
  } else if (rel_type_id == 6) {
    return "executive_his";
  } else {
    return "unknown";
  }
}

class ResultCreator {
 public:
  static constexpr int32_t VERTEX_LABEL_ID = 0;
  ResultCreator(const ReadTransaction& txn)
      : txn_(txn), page_id_(0), page_size_(0) {}

  void Init(
      int32_t page_id, int32_t page_size,
      std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col) {
    page_id_ = page_id;
    page_size_ = page_size;
    typed_comp_named_col_ = typed_comp_named_col;
  }

  bool AddPath(const std::vector<vid_t>& cur_path,
               const std::vector<int64_t>& cur_rel_type,
               const std::vector<double>& rel_weight,
               const std::vector<Direction>& directions) {
    if (cur_path.size() < 2) {
      LOG(ERROR) << "Path size is less than 2";
      return false;
    }
    if (cur_rel_type.size() + 1 != cur_path.size()) {
      LOG(ERROR) << "miss match between cur_rel_type and cur_path size:"
                 << cur_rel_type.size() << " " << cur_path.size();
      return false;
    }
    if (directions.size() + 1 != cur_path.size()) {
      LOG(ERROR) << "miss match between directions and cur_path size:"
                 << directions.size() << " " << cur_path.size();
      return false;
    }
    auto start_node_id = cur_path[0];
    auto end_node_id = cur_path.back();
    auto key = std::make_pair(start_node_id, end_node_id);
    auto iter = results_.path_to_end_node.find(key);
    if (iter == results_.path_to_end_node.end()) {
      results_.path_to_end_node[key] = std::vector<Path>();
    }
    Path path;
    path.vids = cur_path;
    path.weights = rel_weight;
    path.rel_types = cur_rel_type;
    path.directions = directions;
    results_.path_to_end_node[key].push_back(path);
    return true;
  }

  std::string Dump() const {
    rapidjson::Document document_;
    document_.AddMember("currentPage", page_id_, document_.GetAllocator());
    document_.AddMember("pageSize", page_size_, document_.GetAllocator());
    //
    // rapidjson::Document paths(rapidjson::kArrayType,
    // document_.GetAllocator());
    document_.AddMember("data", rapidjson::kArrayType,
                        document_.GetAllocator());
    for (auto& [key, path_list] : results_.path_to_end_node) {
      auto& src_vid = key.first;
      auto& dst_vid = key.second;
      rapidjson::Document paths_for_pair(rapidjson::kObjectType,
                                         &document_.GetAllocator());
      {
        paths_for_pair.AddMember("startNodeName", get_vertex_name(src_vid),
                                 document_.GetAllocator());
        paths_for_pair.AddMember("endNodeName", get_vertex_name(dst_vid),
                                 document_.GetAllocator());
        paths_for_pair.AddMember("startNodeId", get_vertex_id(src_vid),
                                 document_.GetAllocator());
        paths_for_pair.AddMember("endNodeId", get_vertex_id(dst_vid),
                                 document_.GetAllocator());
        paths_for_pair.AddMember("paths", rapidjson::kArrayType,
                                 document_.GetAllocator());
        for (auto& path : path_list) {
          paths_for_pair["paths"].PushBack(
              to_json(path, document_.GetAllocator()),
              document_.GetAllocator());
        }
      }

      document_["data"].PushBack(paths_for_pair, document_.GetAllocator());
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document_.Accept(writer);
    return buffer.GetString();
  }

  rapidjson::Document to_json(
      const Path& path, rapidjson::Document::AllocatorType& allocator) const {
    rapidjson::Document path_json(rapidjson::kObjectType, &allocator);
    path_json.AddMember("nodes", rapidjson::kArrayType, allocator);
    path_json.AddMember("relationships", rapidjson::kArrayType, allocator);
    for (auto i = 0; i < path.vids.size(); ++i) {
      rapidjson::Document node(rapidjson::kObjectType, &allocator);
      node.AddMember("id", get_vertex_id(path.vids[i]), allocator);
      node.AddMember("name", get_vertex_name(path.vids[i]), allocator);
      path_json["nodes"].PushBack(node, allocator);
      if (i < path.vids.size() - 1) {
        auto relation_id = generate_relation_id(path.vids[i], path.vids[i + 1],
                                                path.rel_types[i]);
        rapidjson::Document rel(rapidjson::kObjectType, &allocator);
        rel.AddMember("startNode", get_vertex_name(path.vids[i]), allocator);
        rel.AddMember("endNode", get_vertex_name(path.vids[i + 1]), allocator);
        rel.AddMember("type", get_rel_type_name(path.rel_types[i]), allocator);
        rel.AddMember("name", get_rel_type_name(path.rel_types[i]), allocator);
        rel.AddMember("id", relation_id, allocator);
        rel.AddMember("properties", rapidjson::kObjectType, allocator);
        rel["properties"].AddMember("weight", path.weights[i], allocator);
        rel["properties"].AddMember(
            "label", get_rel_type_name(path.rel_types[i]), allocator);
        rel["properties"].AddMember("id", relation_id, allocator);
        rel["properties"].AddMember(
            "label", get_rel_type_name(path.rel_types[i]), allocator);
        path_json["relationships"].PushBack(rel, allocator);
      }
    }
    return path_json;
  }

 private:
  inline std::string get_vertex_name(vid_t vid) const {
    return std::string(typed_comp_named_col_->get_view(vid));
  }

  inline int64_t get_vertex_id(vid_t vid) const {
    return txn_.GetVertexId(VERTEX_LABEL_ID, vid).AsInt64();
  }

  inline std::string get_rel_type_name(int64_t rel_type) const {
    return rel_type_to_string(rel_type);
  }

  inline std::string generate_relation_id(vid_t src, vid_t dst,
                                          int64_t rel_type) const {
    return std::to_string(get_vertex_id(src)) + "_" +
           get_rel_type_name(rel_type) + "_" +
           std::to_string(get_vertex_id(dst));
  }

  const ReadTransaction& txn_;
  int32_t page_id_, page_size_;
  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col_;

  Results results_;
};

class QiDian : public WriteAppBase {
 public:
  static constexpr double timeout_sec = 15;
  static constexpr int32_t REL_TYPE_MAX = 19;  // 0 ~ 18
  QiDian() {}

  void Init(GraphDBSession& graph) {
    vertex_label_id_ = graph.schema().get_vertex_label_id("vertex");
    invest_label_id_ = graph.schema().get_edge_label_id("invest");
    size_t num = graph.graph().vertex_num(vertex_label_id_);

    LOG(INFO) << "vertex num:" << num;
    valid_comp_vids_.resize(num, false);

    auto comp_name_col =
        graph.get_vertex_property_column(vertex_label_id_, "vertex_name");
    if (!comp_name_col) {
      LOG(ERROR) << "column vertex_name not found for company";
    }
    typed_comp_named_col_ =
        std::dynamic_pointer_cast<TypedColumn<std::string_view>>(comp_name_col);
    if (!typed_comp_named_col_) {
      LOG(ERROR) << "column vertex_name is not string type for company";
    }
  }
  ~QiDian() {}
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
                   const AdjListView<RecordView>& edges,
                   std::vector<bool>& valid_rel_type_ids, int32_t cur_ind,
                   std::vector<std::vector<vid_t>>& cur_paths,
                   std::vector<std::vector<int64_t>>& cur_rel_types,
                   std::vector<std::vector<double>>& cur_rel_weights,
                   std::vector<std::vector<Direction>>& cur_directions,
                   std::vector<std::vector<vid_t>>& next_paths,
                   std::vector<std::vector<int64_t>>& next_rel_types,
                   std::vector<std::vector<double>>& next_rel_weights,
                   std::vector<std::vector<Direction>>& next_directions,
                   size_t& result_size, int32_t left_bound, int32_t right_bound,
                   Encoder& output, ResultCreator& result_creator_,
                   Direction direction) {
    auto& cur_path = cur_paths[cur_ind];
    auto& cur_rel_type = cur_rel_types[cur_ind];
    auto& cur_rel_weight = cur_rel_weights[cur_ind];
    auto& cur_direction = cur_directions[cur_ind];

    for (auto& edge : edges) {
      auto dst = edge.get_neighbor();
      cur_path.emplace_back(dst);
      cur_rel_type.emplace_back(edge.get_data()[1].AsInt64());
      cur_rel_weight.emplace_back(edge.get_data()[0].AsDouble());
      cur_direction.emplace_back(direction);
      if (is_simple(cur_path)) {
        next_paths.emplace_back(cur_path);
        next_rel_types.emplace_back(cur_rel_type);
        next_rel_weights.emplace_back(cur_rel_weight);
        next_directions.emplace_back(cur_direction);
        if (valid_comp_vids_[dst]) {
          // final_results.emplace_back(path);
          if (result_size >= left_bound) {
            // output.put_int(cur_rel_type.size());
            if (cur_path.size() != cur_rel_type.size() + 1) {
              throw std::runtime_error("Error Internal state");
            }
            if (!result_creator_.AddPath(cur_path, cur_rel_type, cur_rel_weight,
                                         cur_direction)) {
              LOG(ERROR) << "Add path failed";
              return false;
            }
          }
          ++result_size;

          if (result_size >= right_bound) {
            // output.put_int_at(begin_loc, result_size - left_bound);
            output.put_string(result_creator_.Dump());
            return cleanUp(txn, vid_vec);
          }
        }
      }
      cur_path.pop_back();
      cur_rel_type.pop_back();
      cur_rel_weight.pop_back();
      cur_direction.pop_back();
    }
    return false;
  }

#define DEBUG
  bool Query(GraphDBSession& graph, Decoder& input, Encoder& output) {
    Init(graph);

    double cur_time_left = timeout_sec;

    auto txn = graph.GetReadTransaction();
    int32_t hop_limit = input.get_int();
    int32_t page_id = input.get_int();
    int32_t page_limit = input.get_int();
    int32_t left_bound = page_id * page_limit;
    int32_t right_bound = (page_id + 1) * page_limit;
    // LOG(INFO) << "result limit: " << page_limit << "\n";
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

    int32_t vec_size = input.get_int();
    LOG(INFO) << "Group Query: hop limit " << hop_limit << ", result limit "
              << page_limit << ", ids size " << vec_size
              << ", range: " << left_bound << ", " << right_bound;
    std::vector<vid_t> vid_vec;
    int count = 0;

    for (int i = 0; i < vec_size; ++i) {
      auto oid = input.get_long();
      // std::string_view oid = input.get_string();
      vid_t vid;
      if (!txn.GetVertexIndex(vertex_label_id_, Any::From(oid), vid)) {
        LOG(INFO) << "Get oid: " << oid << ", not found";
        count++;
      } else {
        VLOG(10) << "Oid: " << oid << ", vid:" << vid;
        vid_vec.emplace_back(vid);
      }
    }
    if (count > 0) {
      LOG(INFO) << count << " out of " << vec_size << " vertices are not found";
    }
    for (auto& vid : vid_vec) {
      valid_comp_vids_[vid] = true;
    }

#if 0
    auto cmp_invest_outgoing_view = txn.GetOutgoingGraphView<int64_t>(
        vertex_label_id_, vertex_label_id_, invest_label_id_);
    auto cmp_invest_incoming_view = txn.GetIncomingGraphView<int64_t>(
        vertex_label_id_, vertex_label_id_, invest_label_id_);
#else
    auto cmp_invest_outgoing_view = txn.GetOutgoingGraphView<RecordView>(
        vertex_label_id_, vertex_label_id_, invest_label_id_);
    auto cmp_invest_incoming_view = txn.GetIncomingGraphView<RecordView>(
        vertex_label_id_, vertex_label_id_, invest_label_id_);
#endif
    ResultCreator result_creator_(graph.GetReadTransaction());
    result_creator_.Init(page_id, page_limit, typed_comp_named_col_);

    // Expand from vid_vec, until end_vertex is valid, or hop limit is reached.
    std::vector<std::vector<vid_t>> cur_paths;
    std::vector<std::vector<int64_t>> cur_rel_types;
    std::vector<std::vector<double>> cur_rel_weights;
    std::vector<std::vector<Direction>> cur_directions;
    std::vector<std::vector<vid_t>> next_paths;
    std::vector<std::vector<int64_t>> next_rel_types;
    std::vector<std::vector<double>> next_rel_weights;
    std::vector<std::vector<Direction>> next_directions;
    // init cur_paths
    for (auto& vid : vid_vec) {
      cur_paths.emplace_back(std::vector<vid_t>{vid});
      cur_rel_types.emplace_back(std::vector<int64_t>{});
      cur_rel_weights.emplace_back(std::vector<double>{});
      cur_directions.emplace_back(std::vector<Direction>{});
    }
    // size_t begin_loc = output.skip_int();
    size_t result_size = 0;
    for (auto i = 1; i <= hop_limit; ++i) {
      VLOG(10) << "hop: " << i;

      for (auto j = 0; j < cur_paths.size(); ++j) {
        // VLOG(10) << "path: " << gs::to_string(cur_paths[j]);
        auto last_vid = cur_paths[j].back();
        const auto& oedges = cmp_invest_outgoing_view.get_edges(last_vid);
        double t0 = -grape::GetCurrentTime();
        if (edge_expand(txn, vid_vec, oedges, valid_rel_type_ids, j, cur_paths,
                        cur_rel_types, cur_rel_weights, cur_directions,
                        next_paths, next_rel_types, next_rel_weights,
                        next_directions, result_size, left_bound, right_bound,
                        output, result_creator_, Direction::Out)) {
          return true;  // early terminate.
        }
        t0 += grape::GetCurrentTime();
        cur_time_left -= t0;
        if (cur_time_left < 0) {
          LOG(INFO) << "Timeout, result size: " << result_size - left_bound;
          // output.put_int_at(begin_loc, result_size - left_bound);
          output.put_string(result_creator_.Dump());
          return cleanUp(txn, vid_vec);
        }
        double t1 = -grape::GetCurrentTime();
        const auto& iedges = cmp_invest_incoming_view.get_edges(last_vid);
        if (edge_expand(txn, vid_vec, iedges, valid_rel_type_ids, j, cur_paths,
                        cur_rel_types, cur_rel_weights, cur_directions,
                        next_paths, next_rel_types, next_rel_weights,
                        next_directions, result_size, left_bound, right_bound,
                        output, result_creator_, Direction::In)) {
          return true;  // early terminate.
        }
        t1 += grape::GetCurrentTime();
        cur_time_left -= t1;
        if (cur_time_left < 0) {
          LOG(INFO) << "Timeout, result size: " << result_size - left_bound;
          // output.put_int_at(begin_loc, result_size - left_bound);
          output.put_string(result_creator_.Dump());
          return cleanUp(txn, vid_vec);
        }
      }
      // LOG(INFO) << "Hop: " << i << ", result: " << final_results.size()
      //         << ", cur_paths: " << cur_paths.size()
      //       << ", next_paths: " << next_paths.size();
      cur_paths.swap(next_paths);
      cur_rel_types.swap(next_rel_types);
      cur_rel_weights.swap(next_rel_weights);
      cur_directions.swap(next_directions);
      next_paths.clear();
      next_rel_types.clear();
      next_rel_weights.clear();
      next_directions.clear();
    }

    // output.put_int_at(begin_loc, result_size - left_bound);
    output.put_string(result_creator_.Dump());
    return cleanUp(txn, vid_vec);
  }

  bool cleanUp(ReadTransaction& txn, const std::vector<vid_t>& vid_vec) {
    txn.Commit();
    for (auto& vid : vid_vec) {
      valid_comp_vids_[vid] = false;
    }
    return true;
  }

 private:
  label_t vertex_label_id_;
  label_t invest_label_id_;
  std::unordered_set<vid_t> vis_;
  std::vector<bool> valid_comp_vids_;

  std::shared_ptr<TypedColumn<std::string_view>> typed_comp_named_col_;
};

#undef DEBUG

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::QiDian* app = new gs::QiDian();
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::QiDian* casted = static_cast<gs::QiDian*>(app);
  delete casted;
}
}
