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

// #include <igraph/igraph.h>
#include <stdio.h>
#include "include/CPMVertexPartition.h"
#include "include/GraphHelper.h"
#include "include/ModularityVertexPartition.h"
#include "include/Optimiser.h"
#include "include/RBConfigurationVertexPartition.h"
#include "include/RBERVertexPartition.h"
#include "include/SignificanceVertexPartition.h"
#include "include/SurpriseVertexPartition.h"

#include <glog/logging.h>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

using std::cout;
using std::endl;

void graph_db_to_igraph(igraph_t* g, const gs::GraphDBSession& sess) {
  // Make sure only one vertex label and one edge label
  CHECK(sess.graph().schema().vertex_label_num() == 1 &&
        sess.graph().schema().edge_label_num() == 1);
  igraph_vector_int_t edges;
  igraph_vector_int_init(&edges, 0);
  auto& frag = sess.graph();
  size_t edges_cnt = 0;
  for (gs::vid_t v = 0; v < frag.vertex_num(0); ++v) {
    auto oe = frag.get_outgoing_edges_raw(0, v, 0, 0);
    while (oe->is_valid()) {
      igraph_vector_int_push_back(&edges, v);
      igraph_vector_int_push_back(&edges, oe->get_neighbor());
      oe->next();
      edges_cnt++;
    }
  }
  LOG(INFO) << "Edges count: " << edges_cnt;

  igraph_create(g, &edges, frag.vertex_num(0), true);
  igraph_vector_int_destroy(&edges);
}

void check_edge_id_same(const gs::GraphDBSession& sess) {
  igraph_t g;
  graph_db_to_igraph(&g, sess);
  IGraphProxy* igraph_proxy = new IGraphGraphProxy(&g);

  IGraphProxy* graph_db_proxy = new GraphDBGraphProxy(sess);

  CHECK(igraph_proxy->vertex_num() == graph_db_proxy->vertex_num());
  CHECK(igraph_proxy->edge_num() == graph_db_proxy->edge_num());
  for (size_t i = 0; i < igraph_proxy->edge_num(); i++) {
    size_t from_igraph, to_igraph;
    igraph_proxy->edge(i, from_igraph, to_igraph);
    size_t from_graph_db, to_graph_db;
    graph_db_proxy->edge(i, from_graph_db, to_graph_db);
    CHECK(from_igraph == from_graph_db);
    CHECK(to_igraph == to_graph_db);
  }
  for (size_t v = 0; v < graph_db_proxy->vertex_num(); ++v) {
    auto incident_graph_db = graph_db_proxy->incident(v, IGRAPH_ALL);
    auto incident_igraph = igraph_proxy->incident(v, IGRAPH_ALL);
    CHECK(incident_graph_db.size() == incident_igraph.size());
    sort(incident_igraph.begin(), incident_igraph.end());
    sort(incident_graph_db.begin(), incident_graph_db.end());
    for (size_t i = 0; i < incident_graph_db.size(); i++) {
      CHECK(incident_graph_db[i] == incident_igraph[i]);
      size_t from_igraph, to_igraph;
      igraph_proxy->edge(incident_igraph[i], from_igraph, to_igraph);
      size_t from_graph_db, to_graph_db;
      graph_db_proxy->edge(incident_graph_db[i], from_graph_db, to_graph_db);
      CHECK(from_igraph == from_graph_db);
      CHECK(to_igraph == to_graph_db);
    }
  }
  // test neighbors, degree
}

int main(int argc, char** argv) {
  // igraph_t g;
  // igraph_famous(&g, "Zachary");
  if (argc != 4) {
    LOG(ERROR) << "Usage: " << argv[0]
               << "<schema_path> <db_path> <output_path>";
    return 1;
  }
  std::string schema_path = argv[1];
  std::string db_path = argv[2];
  std::string output_path = argv[3];

  auto& db = gs::GraphDB::get();
  auto schema_res = gs::Schema::LoadFromYaml(schema_path);
  if (!schema_res.ok()) {
    LOG(FATAL) << "Fail to load graph schema from yaml file: " << schema_path;
  }
  auto load_res = db.Open(schema_res.value(), db_path, 1);
  if (!load_res.ok()) {
    LOG(FATAL) << "Failed to load graph from data directory: "
               << load_res.status().error_message();
  }
  auto& sess = db.GetSession(0);

  // igraph_t g;
  // graph_db_to_igraph(&g, sess);
  // IGraphProxy* proxy = new IGraphGraphProxy(&g);

  check_edge_id_same(sess);

  return 0;

  IGraphProxy* proxy = new GraphDBGraphProxy(sess);

  LOG(INFO) << "vertex num: " << proxy->vertex_num()
            << ", edge num: " << proxy->edge_num();
  LOG(INFO) << "directed: " << proxy->is_directed()
            << ", self loops: " << proxy->has_self_loops();

  // for (size_t i = 0; i < proxy->vertex_num(); i++) {
  //   auto neighbours = proxy->neighbors(i, IGRAPH_ALL);
  //   LOG(INFO) << "Vertex " << i << " has " << neighbours.size()
  //             << " neighbours";
  //   for (size_t j = 0; j < neighbours.size(); j++) {
  //     LOG(INFO) << "Neighbour " << j << ": " << neighbours[j];
  //   }
  //   LOG(INFO) << "=====================";
  // }
  // std::vector<std::vector<size_t>> incident_edges;
  // for (size_t i = 0; i < proxy->vertex_num(); i++) {
  //   auto neighbours = proxy->incident(i, IGRAPH_ALL);
  //   incident_edges.push_back(neighbours);
  //   LOG(INFO) << "Vertex " << i << " has " << neighbours.size()
  //             << " incident edges";
  //   for (size_t j = 0; j < neighbours.size(); j++) {
  //     VLOG(INFO) << "Incident edge " << j << ": " << neighbours[j];
  //   }
  //   LOG(INFO) << "=====================";
  // }

  // verify the incident edges
  // for (size_t i = 0; i < proxy->vertex_num(); i++) {
  //   auto edges = proxy->incident(i, IGRAPH_ALL);
  //   size_t from, to;
  //   for (size_t j = 0; j < edges.size(); j++) {
  //     proxy->edge(edges[j], from, to);
  //     LOG(INFO) << "edge: " << edges[j] << ", from: " << from << ", to: " <<
  //     to;
  //   }
  // }

  // LOG(INFO) << "Graph created, vcount: " << igraph_vcount(&g)
  // << ", ecount: " << igraph_ecount(&g);

  double t = -grape::GetCurrentTime();

  Graph graph(proxy);

  // CPMVertexPartition part(&graph, 0.0001);
  // SignificanceVertexPartition part(&graph);
  ModularityVertexPartition part(&graph);
  // RBERVertexPartition part(&graph, 0.02);
  // SurpriseVertexPartition part(&graph);

  Optimiser o(&graph);

  o.optimise_partition(&part);

  // write to file
  // /workspaces/GraphScope/flex/interactive/examples/wiki/leiden_out open file
  FILE* f = fopen(output_path.c_str(), "w");
  if (f == NULL) {
    printf("Error opening file!\n");
    exit(1);
  }
  // cout << "Node\tCommunity" << endl;
  cout << "Number of communities: " << part.n_communities() << endl;
  auto txn = sess.GetReadTransaction();
  for (size_t i = 0; i < graph.vcount(); i++) {
    // cout << i << "\t" << part.membership[i] << endl;
    fprintf(f, "%zu\t%zu\n", txn.GetVertexId(0, i).AsInt64(),
            part.membership()[i]);
  }
  fclose(f);

  // igraph_destroy(&g);

  t += grape::GetCurrentTime();
  LOG(INFO) << "Time: " << t << "s";
}