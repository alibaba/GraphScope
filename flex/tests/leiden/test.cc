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
#include "include/Optimiser.h"

#include <glog/logging.h>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

using std::cout;
using std::endl;

void graph_db_to_igraph(igraph_t* g, gs::GraphDBSession& sess) {
  // Make sure only one vertex label and one edge label
  CHECK(sess.graph().schema().vertex_label_num() == 1 &&
        sess.graph().schema().edge_label_num() == 1);
  igraph_vector_int_t edges;
  igraph_vector_int_init(&edges, 0);
  auto& frag = sess.graph();
  for (gs::vid_t v = 0; v < frag.vertex_num(0); ++v) {
    auto oe = frag.get_outgoing_edges_raw(0, v, 0, 0);
    while (oe->is_valid()) {
      igraph_vector_int_push_back(&edges, v);
      igraph_vector_int_push_back(&edges, oe->get_neighbor());
      oe->next();
    }
  }

  igraph_create(g, &edges, 0, true);
  igraph_vector_int_destroy(&edges);
}

int main(int argc, char** argv) {
  // igraph_t g;
  // igraph_famous(&g, "Zachary");
  if (argc != 3) {
    LOG(ERROR) << "Usage: " << argv[0] << "<schema_path> <db_path>";
    return 1;
  }
  std::string schema_path = argv[1];
  std::string db_path = argv[2];

  igraph_t g;

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

  graph_db_to_igraph(&g, sess);

  LOG(INFO) << "Graph created, vcount: " << igraph_vcount(&g)
            << ", ecount: " << igraph_ecount(&g);

  double t = -grape::GetCurrentTime();

  Graph graph(&g);

  CPMVertexPartition part(&graph, 0.5 /* resolution */);

  Optimiser o;

  o.optimise_partition(&part);

  cout << "Node\tCommunity" << endl;
  auto txn = sess.GetReadTransaction();
  for (size_t i = 0; i < graph.vcount(); i++) {
    cout << part.membership(i) << "," << txn.GetVertexId(0, i).to_string()
         << endl;
  }

  igraph_destroy(&g);

  t += grape::GetCurrentTime();
  LOG(INFO) << "Time: " << t << "s";
}