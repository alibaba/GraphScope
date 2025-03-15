/*  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 *
 */


 #include <graphlab.hpp>

 #define K 5
 
 struct VertexValue {
   std::vector<int> neighbors;
   long long kclique_num = 0;
   VertexValue& operator=(const VertexValue* other) {
     this->neighbors.clear();
     for (auto & item : other->neighbors) {
       this->neighbors.push_back(item);
     }
     this->kclique_num = other->kclique_num;
     return *this;
   }
   void save(graphlab::oarchive& oarc) const {
     oarc << neighbors << kclique_num;
   }
 
   void load(graphlab::iarchive& iarc) {
     iarc >> neighbors >> kclique_num;
   }
 };
 
 struct Message {
   std::map<int, std::vector<int>> neighboring_list;
 
   Message() {
   }
     
   Message& operator+=(const Message& other) {
     for (auto & list : other.neighboring_list) {
       this->neighboring_list[list.first] = std::vector<int>{};
       for (auto & item : list.second) {
         this->neighboring_list[list.first].push_back(item);
       }
     }
     return *this; 
   }
 
   void save(graphlab::oarchive& oarc) const {
     oarc << neighboring_list;
   }
 
   void load(graphlab::iarchive& iarc) {
     iarc >> neighboring_list;
   }
 };
 
 // The vertex data is its label 
 typedef std::string vertex_data_type;
 
 typedef Message gather_type;
  
 // The graph type is determined by the vertex and edge data types
 typedef graphlab::distributed_graph<VertexValue, graphlab::empty> graph_type;
 
 bool line_parser(graph_type& graph, const std::string& filename, const std::string& textline) {
   std::stringstream strm(textline);
   graphlab::vertex_id_type vid;
   std::string label;
   // first entry in the line is a vertex ID
   strm >> vid;
   strm >> label;  // useless
   // std::cout << "read line " << vid << " " << label << std::endl;
   
   // continue to read neighbors
   VertexValue vv;
   vv.kclique_num = 0;
   // while there are elements in the line, continue to read until we fail
   while(1){
     graphlab::vertex_id_type other_vid;
     strm >> other_vid;
     if (strm.fail())
       break;
     if (vid >= other_vid) continue;
     graph.add_edge(vid, other_vid);
     vv.neighbors.push_back(other_vid);
   }
   graph.add_vertex(vid, vv);
   return true;
 }
 
 class labelpropagation :
   public graphlab::ivertex_program<graph_type, gather_type>,
   public graphlab::IS_POD_TYPE {
 
   public:
 
     int KCliqueCounting(std::map<int, std::vector<int>> & subgraph, std::set<int> & cand, int lev) {
       if (lev == K - 1) {
         return cand.size();
       }
       int t = 0;
       for (auto &u : cand) {
         std::set<int> next_cand;
         for (auto &v : subgraph[u]) {
           if (cand.count(v)) {
             next_cand.insert(v);
           }
         }
         if (next_cand.size() >= K - lev - 1) {
           t = t + KCliqueCounting(subgraph, next_cand, lev + 1);
         }
       }
       return t;
     }
 
     edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const {
       return graphlab::ALL_EDGES;
     }
 
     gather_type gather(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
       if (vertex.id() == edge.target().id()) return Message{};
       // std::cout << "vertex id " << vertex.id() << " vertex data: " << vertex.data().kclique_num << "  edge " << edge.source().id() << " " << edge.target().id() << std::endl;
 
       Message msg;
       msg.neighboring_list[edge.target().id()] = std::vector<int>{};
       for (auto & neighbor : edge.target().data().neighbors) {
         msg.neighboring_list[edge.target().id()].push_back(neighbor);
       }
 
       return msg;
     }
 
     void apply(icontext_type& context, vertex_type& vertex, const gather_type& total) {
       std::map<int, std::vector<int>> subgraph;
       std::set<int> cand;
       std::set<int> nb;
       for (auto & u : vertex.data().neighbors) {
         nb.insert(u);
       }
 
       for (auto & pair : total.neighboring_list) {
         subgraph[vertex.id()].push_back(pair.first);
         cand.insert(pair.first);
         for (auto & u : pair.second) {
           if (nb.count(u) == 1) {
             subgraph[pair.first].push_back(u);
           }
         }
       }
 
       // std::cout << "subgraph of " << vertex.id() << std::endl;
       // for (auto &u : cand) {
       //   std::cout << u << " ";
       // }
       // std::cout << std::endl;
       // for (auto &u : subgraph) {
       //   for (auto &v : u.second) {
       //     std::cout << u.first << " " << v << std::endl;
       //   }
       // }
       vertex.data().kclique_num = KCliqueCounting(subgraph, cand, 1);
       // std::cout << "vertex " << vertex.id() << "     kclique num = " << vertex.data().kclique_num << std::endl;
     }
 
     edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const {
       return graphlab::NO_EDGES;
     }
 
     void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
       return;
     }
   };
 
 struct labelpropagation_writer {
   std::string save_vertex(graph_type::vertex_type v) {
     std::stringstream strm;
     strm << v.id() << "\t" << v.data().kclique_num << "\n";
     return strm.str();
   }
   std::string save_edge (graph_type::edge_type e) { return ""; }
 };
 
 size_t get_vertex_data(const graph_type::vertex_type& v) {
   return v.data().kclique_num;
 }
 
 
 int main(int argc, char** argv) {
   // Initialize control plain using mpi
   graphlab::mpi_tools::init(argc, argv);
   graphlab::distributed_control dc;
   global_logger().set_log_level(LOG_INFO);
   
   // Parse command line options -----------------------------------------------
   graphlab::command_line_options clopts("Label Propagation algorithm.");
   std::string graph_dir;
   std::string execution_type = "synchronous";
   clopts.attach_option("graph", graph_dir, "The graph file. Required ");
   clopts.add_positional("graph");
   clopts.attach_option("execution", execution_type, "Execution type (synchronous or asynchronous)");
 
   std::string saveprefix;
   clopts.attach_option("saveprefix", saveprefix,
                        "If set, will save the resultant pagerank to a "
                        "sequence of files with prefix saveprefix");
 
   if(!clopts.parse(argc, argv)) {
     dc.cout() << "Error in parsing command line arguments." << std::endl;
     return EXIT_FAILURE;
   }
   if (graph_dir == "") {
     dc.cout() << "Graph not specified. Cannot continue";
     return EXIT_FAILURE;
   }
  
   // Build the graph ----------------------------------------------------------
   graph_type graph(dc);
   dc.cout() << "Loading graph using line parser" << std::endl;
   graph.load(graph_dir, line_parser);
   // must call finalize before querying the graph
   graph.finalize();
 
   dc.cout() << "#vertices: " << graph.num_vertices() << " #edges:" << graph.num_edges() << std::endl;
 
   graphlab::omni_engine<labelpropagation> engine(dc, graph, execution_type, clopts);
 
   engine.signal_all();
   engine.start();
 
   const float runtime = engine.elapsed_seconds();
   dc.cout() << "Finished Running engine in " << runtime << " seconds." << std::endl;
 
   if (saveprefix != "") {
     graph.save(saveprefix, labelpropagation_writer(),
        false,  // do not gzip
        true,   //save vertices
        false); // do not save edges 
   }
   
   size_t count = graph.map_reduce_vertices<size_t>(get_vertex_data);
   std::cout << "Total k-clique number " << count << std::endl;
 
   graphlab::mpi_tools::finalize();
   return EXIT_SUCCESS;
 }
 