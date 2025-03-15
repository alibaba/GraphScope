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
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */

 #include <string>
 #include <iostream>
 #include <algorithm>
 #include <vector>
 #include <map>
 #include <boost/unordered_map.hpp>
 #include <time.h>
 
 #include <graphlab.hpp>
 #include <graphlab/graph/distributed_graph.hpp>
 
 struct vdata {
   uint64_t labelid;
   vdata() :
       labelid(0) {
   }
 
   void save(graphlab::oarchive& oarc) const {
     oarc << labelid;
   }
   void load(graphlab::iarchive& iarc) {
     iarc >> labelid;
   }
 };
 
 typedef graphlab::distributed_graph<vdata, graphlab::empty> graph_type;
 
 //set label id at vertex id
 void initialize_vertex(graph_type::vertex_type& v) {
   v.data().labelid = v.id();
 }
 
 //message where summation means minimum
 struct min_message {
   uint64_t value;
   explicit min_message(uint64_t v) :
       value(v) {
   }
   min_message() :
       value(std::numeric_limits<uint64_t>::max()) {
   }
   min_message& operator+=(const min_message& other) {
     value = std::min<uint64_t>(value, other.value);
     return *this;
   }
 
   void save(graphlab::oarchive& oarc) const {
     oarc << value;
   }
   void load(graphlab::iarchive& iarc) {
     iarc >> value;
   }
 };
 
 class label_propagation: public graphlab::ivertex_program<graph_type, size_t,
     min_message>, public graphlab::IS_POD_TYPE {
 private:
   size_t recieved_labelid;
   bool perform_scatter;
 public:
   label_propagation() {
     recieved_labelid = std::numeric_limits<size_t>::max();
     perform_scatter = false;
   }
 
   //receive messages
   void init(icontext_type& context, const vertex_type& vertex,
       const message_type& msg) {
     recieved_labelid = msg.value;
   }
 
   //do not gather
   edge_dir_type gather_edges(icontext_type& context,
       const vertex_type& vertex) const {
     return graphlab::NO_EDGES;
   }
   size_t gather(icontext_type& context, const vertex_type& vertex,
       edge_type& edge) const {
     return 0;
   }
 
   //update label id. If updated, scatter messages
   void apply(icontext_type& context, vertex_type& vertex,
       const gather_type& total) {
     if (recieved_labelid == std::numeric_limits<size_t>::max()) {
       perform_scatter = true;
     } else if (vertex.data().labelid > recieved_labelid) {
       perform_scatter = true;
       vertex.data().labelid = recieved_labelid;
     }
   }
 
   edge_dir_type scatter_edges(icontext_type& context,
       const vertex_type& vertex) const {
     if (perform_scatter)
       return graphlab::ALL_EDGES;
     else
       return graphlab::NO_EDGES;
   }
 
   //If a neighbor vertex has a bigger label id, send a massage
   void scatter(icontext_type& context, const vertex_type& vertex,
       edge_type& edge) const {
     if (edge.source().id() != vertex.id()
         && edge.source().data().labelid > vertex.data().labelid) {
       context.signal(edge.source(), min_message(vertex.data().labelid));
     }
     if (edge.target().id() != vertex.id()
         && edge.target().data().labelid > vertex.data().labelid) {
       context.signal(edge.target(), min_message(vertex.data().labelid));
     }
   }
 };
 
 class graph_writer {
 public:
   std::string save_vertex(graph_type::vertex_type v) {
     std::stringstream strm;
     strm << v.id() << "," << v.data().labelid << "\n";
     return strm.str();
   }
   std::string save_edge(graph_type::edge_type e) {
     return "";
   }
 };
 
 int main(int argc, char** argv) {
   std::cout << "Connected Component\n\n";
 
   graphlab::mpi_tools::init(argc, argv);
   graphlab::distributed_control dc;
   global_logger().set_log_level(LOG_DEBUG);
   //parse options
   graphlab::command_line_options clopts("Connected Component.");
   std::string graph_dir;
   std::string saveprefix;
   std::string format = "adj";
   std::string exec_type = "synchronous";
   clopts.attach_option("graph", graph_dir,
                        "The graph file. This is not optional");
   clopts.add_positional("graph");
   clopts.attach_option("format", format,
                        "The graph file format");
   clopts.attach_option("saveprefix", saveprefix,
                        "If set, will save the pairs of a vertex id and "
                        "a component id to a sequence of files with prefix "
                        "saveprefix");
   if (!clopts.parse(argc, argv)) {
     dc.cout() << "Error in parsing command line arguments." << std::endl;
     return EXIT_FAILURE;
   }
   if (graph_dir == "") {
     std::cout << "--graph is not optional\n";
     return EXIT_FAILURE;
   }
 
   graph_type graph(dc, clopts);
 
   //load graph
   dc.cout() << "Loading graph in format: "<< format << std::endl;
   graph.load_format(graph_dir, format);
   graphlab::timer ti;
   graph.finalize();
   dc.cout() << "Finalization in " << ti.current_time() << std::endl;
   graph.transform_vertices(initialize_vertex);
 
   //running the engine
   time_t start, end;
   graphlab::omni_engine<label_propagation> engine(dc, graph, exec_type, clopts);
   engine.signal_all();
   time(&start);
   engine.start();
 
   //write results
   if (saveprefix.size() > 0) {
     graph.save(saveprefix, graph_writer(),
         false, //set to true if each output file is to be gzipped
         true, //whether vertices are saved
         false); //whether edges are saved
   }
 
   graphlab::mpi_tools::finalize();
 
   return EXIT_SUCCESS;
 }
 
 