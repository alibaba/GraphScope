#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

namespace gs {
class Query0 : public AppBase {
 public:
  Query0(GraphDBSession& graph)
      : comp_label_id_(graph.schema().get_vertex_label_id("company")),
        invest_label_id_(graph.schema().get_edge_label_id("invest")),
        graph_(graph) {
    size_t num = graph_.graph().vertex_num(comp_label_id_);

    vis_.resize(num, false);
    valid_vids_.resize(num, false);
  }
  ~Query0() {}
  bool is_simple(const std::vector<vid_t>& path) {
    for (size_t i = 0; i < path.size(); ++i) {
      if (vis_[path[i]]) {
        for (size_t j = 0; j < i; ++j) {
          vis_[path[j]] = false;
        }
        return false;
      }
      vis_[path[i]] = true;
    }
    for (size_t i = 0; i < path.size(); ++i) {
      vis_[path[i]] = false;
    }
    return true;
  }
  bool Query(Decoder& input, Encoder& output) {
    auto txn = graph_.GetReadTransaction();
    int32_t hop_limit = input.get_int();
    int32_t result_limit = input.get_int();
    // LOG(INFO) << "result limit: " << result_limit << "\n";

    int32_t vec_size = input.get_int();
    LOG(INFO) << "Group Query: hop limit " << hop_limit << ", result limit "
              << result_limit << ", ids size " << vec_size;
    std::vector<vid_t> vid_vec;
    int count = 0;

    for (int i = 0; i < vec_size; ++i) {
      auto oid = input.get_long();
      vid_t vid;
      if (!txn.GetVertexIndex(comp_label_id_, Any::From(oid), vid)) {
        LOG(INFO) << "Get oid: " << oid << ", not found";
        count++;
      } else {
        // LOG(INFO) << "Get oid: " << oid << ", found: " << vid;
        vid_vec.emplace_back(vid);
      }
    }
    LOG(INFO) << count << " out of " << vec_size << " vertices not found";
    for (auto& vid : vid_vec) {
      valid_vids_[vid] = true;
    }

    auto outgoin_view = txn.GetOutgoingGraphView<int>(
        comp_label_id_, comp_label_id_, invest_label_id_);
    auto incoming_view = txn.GetIncomingGraphView<int>(
        comp_label_id_, comp_label_id_, invest_label_id_);
    // Expand from vid_vec, until end_vertex is valid, or hop limit is reached.
    std::vector<std::vector<vid_t>> cur_paths;
    std::vector<std::vector<vid_t>> next_paths;
    // init cur_paths
    for (auto& vid : vid_vec) {
      cur_paths.emplace_back(std::vector<vid_t>{vid});
    }
    size_t begin_loc = output.skip_int();
    size_t result_size = 0;
    for (auto i = 1; i <= hop_limit; ++i) {
      for (auto& path : cur_paths) {
        auto last_vid = path.back();
        const auto& oedges = outgoin_view.get_edges(last_vid);
        for (auto& edge : oedges) {
          auto& dst = edge.neighbor;
          path.emplace_back(dst);
          next_paths.emplace_back(path);
          if (valid_vids_[dst] && is_simple(path)) {
            // final_results.emplace_back(path);
            ++result_size;
            output.put_int(path.size());
            for (auto& vid : path) {
              output.put_long(txn.GetVertexId(comp_label_id_, vid).AsInt64());
            }

            if (result_size >= result_limit) {
              output.put_int_at(begin_loc, result_size);
              txn.Commit();
              for (auto& vid : vid_vec) {
                valid_vids_[vid] = false;
              }
              return true;
            }
          }
          path.pop_back();
        }
        const auto& iedges = incoming_view.get_edges(last_vid);
        for (auto& edge : iedges) {
          auto& dst = edge.neighbor;
          path.emplace_back(dst);
          next_paths.emplace_back(path);
          if (valid_vids_[dst] && is_simple(path)) {
            ++result_size;
            output.put_int(path.size());
            for (auto& vid : path) {
              output.put_long(txn.GetVertexId(comp_label_id_, vid).AsInt64());
            }
            if (result_size >= result_limit) {
              output.put_int_at(begin_loc, result_size);
              txn.Commit();
              for (auto& vid : vid_vec) {
                valid_vids_[vid] = false;
              }
              return true;
            }
          }
          path.pop_back();
        }
      }
      // LOG(INFO) << "Hop: " << i << ", result: " << final_results.size()
      //         << ", cur_paths: " << cur_paths.size()
      //       << ", next_paths: " << next_paths.size();
      cur_paths.swap(next_paths);
      next_paths.clear();
    }

    output.put_int_at(begin_loc, result_size);
    txn.Commit();
    for (auto& vid : vid_vec) {
      valid_vids_[vid] = false;
    }

    return true;
  }

 private:
  GraphDBSession& graph_;
  label_t comp_label_id_;
  label_t invest_label_id_;
  std::vector<bool> vis_;
  std::vector<bool> valid_vids_;
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::Query0* app = new gs::Query0(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::Query0* casted = static_cast<gs::Query0*>(app);
  delete casted;
}
}
