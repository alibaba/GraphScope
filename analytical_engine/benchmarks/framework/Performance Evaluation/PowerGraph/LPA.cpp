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

 struct label_counter {
   std::map<std::string, int> label_count;
 
   label_counter() {
   }
     
   label_counter& operator+=(const label_counter& other) { 
     for ( std::map<std::string, int>::const_iterator iter = other.label_count.begin();
               iter != other.label_count.end(); ++iter ) {
             label_count[iter->first] += iter->second;
     }
 
     return *this; 
   }
 
   void save(graphlab::oarchive& oarc) const {
     oarc << label_count;
   }
 
   void load(graphlab::iarchive& iarc) {
     iarc >> label_count;
   }
 };
 
 // The vertex data is its label 
 typedef std::string vertex_data_type;
 
 typedef label_counter gather_type;
  
 // The graph type is determined by the vertex and edge data types
 typedef graphlab::distributed_graph<std::string, graphlab::empty> graph_type;
 
 bool line_parser(graph_type& graph, const std::string& filename, const std::string& textline) {
   std::stringstream strm(textline);
   graphlab::vertex_id_type vid;
   std::string label;
   // first entry in the line is a vertex ID
   strm >> vid;
   strm >> label;
   // insert this vertex with its label 
   graph.add_vertex(vid, label);
   // while there are elements in the line, continue to read until we fail
   while(1){
     graphlab::vertex_id_type other_vid;
     strm >> other_vid;
     if (strm.fail())
       break;
     graph.add_edge(vid, other_vid);
   }
 
   return true;
 }
 
 class labelpropagation :
   public graphlab::ivertex_program<graph_type, gather_type>,
   public graphlab::IS_POD_TYPE {
     bool changed;
 
   public:
     edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const {
       return graphlab::ALL_EDGES;
     }
 
     gather_type gather(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
       // figure out which data to get from the edge.
       bool isEdgeSource = (vertex.id() == edge.source().id());
       std::string neighbor_label = isEdgeSource ? edge.target().data() : edge.source().data();
       // std::cout << "vertex " << vertex.id() << "  edge " << edge.source().id() << " " << edge.target().id() << std::endl;
 
       // make a label_counter and place the neighbor data in it
       label_counter counter;
       counter.label_count[neighbor_label] = 1;
 
 
       // gather_type is a label counter, so += will add neighbor counts to the
       // label_count map.
       return counter;
     }
 
     void apply(icontext_type& context, vertex_type& vertex, const gather_type& total) {
 
       int maxCount = 0;
 
       std::string maxLabel = vertex.data();
 
       // Figure out which label of the vertex's neighbors' labels is most common
       for ( std::map<std::string, int>::const_iterator iter = total.label_count.begin();
                 iter != total.label_count.end(); ++iter ) {
               if (iter->second > maxCount) {
                 maxCount = iter->second;
                 maxLabel = iter->first;
               }
       }
 
       
       // if maxLabel differs to vertex data, mark vertex as changed and update
       // its data.
       if ((vertex.data()).compare(maxLabel) != 0) {
         changed = true;
         vertex.data() = maxLabel;
       } else {
         changed = false;
       }
     }
 
     edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const {
       // if vertex data changes, scatter to all edges.
       if (changed) {
         return graphlab::ALL_EDGES;
       } else {
         return graphlab::NO_EDGES;
       }
     }
 
     void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
       bool isEdgeSource = (vertex.id() == edge.source().id());
 
       context.signal(isEdgeSource ? edge.target() : edge.source()); 
     }
   };
 
 struct labelpropagation_writer {
   std::string save_vertex(graph_type::vertex_type v) {
     std::stringstream strm;
     strm << v.id() << "\t" << v.data() << "\n";
     return strm.str();
   }
   std::string save_edge (graph_type::edge_type e) { return ""; }
 };
 
 
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
   
 
   graphlab::mpi_tools::finalize();
   return EXIT_SUCCESS;
 }
 