#include <queue>
#include <string_view>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/types.h"

namespace gs {
using oid_t = int64_t;

// Update vertex and edges.
class Query0 {
 public:
  Query0(GraphDBSession& graph)
      : medium_label_id_(graph.schema().get_vertex_label_id("MEDIUM")),
        center_label_id_(graph.schema().get_vertex_label_id("CENTER")),
        connect_label_id_(graph.schema().get_edge_label_id("CONNECT")),
        graph_(graph) {}

  bool Query(
      const std::tuple<gs::oid_t, std::string>& center,
      const std::tuple<gs::oid_t, std::string, double, std::string>& medium,
      const std::tuple<gs::oid_t, gs::oid_t, double>& connect) {
    auto txn = graph_.GetUpdateTransaction();
    auto& center_id = std::get<0>(center);
    auto& center_act_fee = std::get<1>(center);
    auto& medium_id = std::get<0>(medium);
    auto& medium_type = std::get<1>(medium);
    auto& medium_weight = std::get<2>(medium);
    auto& medium_src_type = std::get<3>(medium);
    auto& connect_src = std::get<0>(connect);
    auto& connect_dst = std::get<1>(connect);
    auto& connect_weight = std::get<2>(connect);

    // if center_id not exist, insert it
    if (!txn.AddVertex(center_label_id_, center_id,
                       {Any::From(center_act_fee)})) {
      txn.Abort();
      return false;
    }
    if (!txn.AddVertex(medium_label_id_, medium_id,
                       {Any::From(medium_type), Any::From(medium_weight),
                        Any::From(medium_src_type)})) {
      txn.Abort();
      return false;
    }
    if (!txn.AddEdge(center_label_id_, center_id, medium_label_id_, medium_id,
                     connect_label_id_, Any::From(connect_weight))) {
      txn.Abort();
      return false;
    }

    txn.Commit();
    return true;
  }

 private:
  GraphDBSession& graph_;
  label_t medium_label_id_;
  label_t center_label_id_;
  label_t connect_label_id_;
};

// Two hop query
class Query1 {
 public:
  Query1(GraphDBSession& graph)
      : medium_label_id_(graph.schema().get_vertex_label_id("MEDIUM")),
        center_label_id_(graph.schema().get_vertex_label_id("CENTER")),
        connect_label_id_(graph.schema().get_edge_label_id("CONNECT")),
        center_vnum_(graph.graph().vertex_num(center_label_id_)),
        graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) {
    auto txn = graph_.GetReadTransaction();
    // get center with center id.
    auto center_id = input.get_long();
    std::unordered_set<std::string_view> valid_types;
    int32_t medium_types_num = input.get_int();
    for (auto i = 0; i < medium_types_num; ++i) {
      valid_types.insert(input.get_string());
    }
    CHECK(input.empty());
    gs::vid_t center_vid;

    if (!txn.GetVertexIndex(center_label_id_, center_id, center_vid)) {
      txn.Abort();
      return false;
    }
    // get all medium with center id.
    std::vector<gs::vid_t> medium_vids;
    auto edge_iter = txn.GetOutEdgeIterator(
        center_label_id_, center_vid, medium_label_id_, connect_label_id_);
    auto medium_vertex_iter = txn.GetVertexIterator(medium_label_id_);
    while (edge_iter.IsValid()) {
      auto medium_vid = edge_iter.GetNeighbor();
      // get medium_vid's type property
      medium_vertex_iter.Goto(medium_vid);
      auto medium_type = medium_vertex_iter.GetField(0).AsStringView();
      if (valid_types.find(medium_type) != valid_types.end()) {
        medium_vids.push_back(medium_vid);
      }
      edge_iter.Next();
    }

    // reverse expand, need the results along each path.
    std::vector<std::tuple<int64_t, double, int64_t>> res_vec;
    auto reserver_edge_view = txn.GetIncomingGraphView<double>(
        center_label_id_, medium_label_id_, connect_label_id_);
    for (auto medium_vid : medium_vids) {
      // get oid
      auto oid = txn.GetVertexId(medium_label_id_, medium_vid);
      auto edges = reserver_edge_view.get_edges(medium_vid);
      for (auto iter : edges) {
        auto nbr_vid = iter.neighbor;
        if (nbr_vid != center_vid) {
          auto nbr_oid = txn.GetVertexId(center_label_id_, nbr_vid);
          res_vec.emplace_back(oid.AsInt64(), iter.data, nbr_oid.AsInt64());
        }
      }
    }
    txn.Commit();
    // write to output
    output.put_int(res_vec.size());
    LOG(INFO) << "Got res of size: " << res_vec.size();
    for (auto& res : res_vec) {
      output.put_long(std::get<0>(res));
      output.put_double(std::get<1>(res));
      output.put_long(std::get<2>(res));
    }

    return true;
  }

 private:
  GraphDBSession& graph_;
  label_t medium_label_id_;
  label_t center_label_id_;
  label_t connect_label_id_;
  size_t center_vnum_;
};

// One hop query
class Query2 {
 public:
  Query2(GraphDBSession& graph)
      : medium_label_id_(graph.schema().get_vertex_label_id("MEDIUM")),
        center_label_id_(graph.schema().get_vertex_label_id("CENTER")),
        connect_label_id_(graph.schema().get_edge_label_id("CONNECT")),
        graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) {
    auto txn = graph_.GetReadTransaction();
    // get center with center id.
    int medium_ids_num = input.get_int();
    std::vector<gs::vid_t> medium_vids(medium_ids_num, 0);
    std::vector<gs::oid_t> medium_oids(medium_ids_num, 0);
    for (auto i = 0; i < medium_ids_num; ++i) {
      auto medium_id = input.get_long();
      medium_oids.emplace_back(medium_id);
      if (!txn.GetVertexIndex(medium_label_id_, medium_id, medium_vids[i])) {
        txn.Abort();
        return false;
      }
    }
    // get center_vid
    gs::vid_t center_vid;
    auto center_oid = input.get_long();
    if (!txn.GetVertexIndex(center_label_id_, center_oid, center_vid)) {
      txn.Abort();
      return false;
    }

    // get all center_ids connected to medium
    std::vector<std::tuple<int64_t, double, int64_t>> res_vec;
    for (auto i = 0; i < medium_vids.size(); ++i) {
      auto medium_vid = medium_vids[i];
      auto edge_iter = txn.GetIncomingEdges<double>(
          center_label_id_, medium_vid, medium_label_id_, connect_label_id_);
      for (auto& edge : edge_iter) {
        auto cur_vid = edge.neighbor;
        if (cur_vid != center_vid) {
          auto cur_oid = txn.GetVertexId(center_label_id_, cur_vid);
          res_vec.emplace_back(medium_oids[i], edge.data, cur_oid.AsInt64());
        }
      }
    }
    txn.Abort();
    // write to output
    output.put_int(res_vec.size());
    LOG(INFO) << "Got res of size: " << res_vec.size();
    for (auto& res : res_vec) {
      output.put_long(std::get<0>(res));
      output.put_double(std::get<1>(res));
      output.put_long(std::get<2>(res));
    }

    return true;
  }

 private:
  GraphDBSession& graph_;
  label_t medium_label_id_;
  label_t center_label_id_;
  label_t connect_label_id_;
};

}  // namespace gs

int main(int argc, char** argv) {
  if (argc != 4) {
    LOG(ERROR) << "Usage: ./cro_test <graph_schema> "
                  "<bulk_load_yaml> <data_dir>";
    return 1;
  }
  auto graph_schema = std::string(argv[1]);
  auto bulk_load_yaml = std::string(argv[2]);
  auto data_dir = std::string(argv[3]);

  auto& db = gs::GraphDB::get();
  auto schema = gs::Schema::LoadFromYaml(graph_schema);
  auto loading_config =
      gs::LoadingConfig::ParseFromYaml(schema, bulk_load_yaml);
  db.Init(schema, loading_config, data_dir, 1);
  auto& sess = gs::GraphDB::get().GetSession(0);

  {
    std::tuple<gs::oid_t, std::string> center = {1L, "act1"};
    std::tuple<gs::oid_t, std::string, double, std::string> medium = {
        1L, "act1", 1.0, "phone"};
    std::tuple<gs::oid_t, gs::oid_t, double> connect = {1L, 1L, 1.0};
    gs::Query0 query0(sess);
    CHECK(query0.Query(center, medium, connect));
  }

  {
    // select centers
    std::vector<char> encoder_array;
    gs::Encoder input_encoder(encoder_array);
    input_encoder.put_long(1);
    input_encoder.put_int(1);
    input_encoder.put_string("phone");
    std::vector<char> output_array;
    gs::Encoder output(output_array);
    gs::Decoder input(encoder_array.data(), encoder_array.size());

    gs::Query1 query(sess);
    CHECK(query.Query(input, output));
  }

  return 0;
}