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


 #include <boost/unordered_set.hpp>
 #include <graphlab.hpp>
 #include <graphlab/ui/metrics_server.hpp>
 #include <graphlab/util/hopscotch_set.hpp>
 #include <graphlab/macros_def.hpp>
 /**
  *  
  * In this program we implement the "hash-set" version of the
  * "edge-iterator" algorithm described in
  * 
  *    T. Schank. Algorithmic Aspects of Triangle-Based Network Analysis.
  *    Phd in computer science, University Karlsruhe, 2007.
  *
  * The procedure is quite straightforward:
  *   - each vertex maintains a list of all of its neighbors in a hash set.
  *   - For each edge (u,v) in the graph, count the number of intersections
  *     of the neighbor set on u and the neighbor set on v.
  *   - We store the size of the intersection on the edge.
  * 
  * This will count every triangle exactly 3 times. Summing across all the
  * edges and dividing by 3 gives the desired result.
  *
  * The preprocessing stage take O(|E|) time, and it has been shown that this
  * algorithm takes $O(|E|^(3/2))$ time.
  *
  * If we only require total counts, we can introduce a optimization that is
  * similar to the "forward" algorithm
  * described in thesis above. Instead of maintaining a complete list of all
  * neighbors, each vertex only maintains a list of all neighbors with
  * ID greater than itself. This implicitly generates a topological sort
  * of the graph.
  *
  * Then you can see that each triangle
  *
  * \verbatim
   
      A----->C
      |     ^
      |   /
      v /
      B
    
  * \endverbatim
  * Must be counted only once. (Only when processing edge AB, can one
  * observe that A and B have intersecting out-neighbor sets).
  */
  
 
 // Radix sort implementation from https://github.com/gorset/radix
 // Thanks to Erik Gorset
 //
 /*
 Copyright 2011 Erik Gorset. All rights reserved.
 
 Redistribution and use in source and binary forms, with or without modification, are
 permitted provided that the following conditions are met:
 
 1. Redistributions of source code must retain the above copyright notice, this list of
 conditions and the following disclaimer.
 
 2. Redistributions in binary form must reproduce the above copyright notice, this list
 of conditions and the following disclaimer in the documentation and/or other materials
 provided with the distribution.
 
 THIS SOFTWARE IS PROVIDED BY Erik Gorset ``AS IS'' AND ANY EXPRESS OR IMPLIED
 WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL Erik Gorset OR
 CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 
 The views and conclusions contained in the software and documentation are those of the
 authors and should not be interpreted as representing official policies, either expressed
 or implied, of Erik Gorset.
 */
 void radix_sort(graphlab::vertex_id_type *array, int offset, int end, int shift) {
     int x, y;
     graphlab::vertex_id_type value, temp;
     int last[256] = { 0 }, pointer[256];
 
     for (x=offset; x<end; ++x) {
         ++last[(array[x] >> shift) & 0xFF];
     }
 
     last[0] += offset;
     pointer[0] = offset;
     for (x=1; x<256; ++x) {
         pointer[x] = last[x-1];
         last[x] += last[x-1];
     }
 
     for (x=0; x<256; ++x) {
         while (pointer[x] != last[x]) {
             value = array[pointer[x]];
             y = (value >> shift) & 0xFF;
             while (x != y) {
                 temp = array[pointer[y]];
                 array[pointer[y]++] = value;
                 value = temp;
                 y = (value >> shift) & 0xFF;
             }
             array[pointer[x]++] = value;
         }
     }
 
     if (shift > 0) {
         shift -= 8;
         for (x=0; x<256; ++x) {
             temp = x > 0 ? pointer[x] - pointer[x-1] : pointer[0] - offset;
             if (temp > 64) {
                 radix_sort(array, pointer[x] - temp, pointer[x], shift);
             } else if (temp > 1) {
                 std::sort(array + (pointer[x] - temp), array + pointer[x]);
                 //insertion_sort(array, pointer[x] - temp, pointer[x]);
             }
         }
     }
 }
 
 size_t HASH_THRESHOLD = 64;
 
 // We on each vertex, either a vector of sorted VIDs
 // or a hash set (cuckoo hash) of VIDs.
 // If the number of elements is greater than HASH_THRESHOLD,
 // the hash set is used. Otherwise the vector is used.
 struct vid_vector{
   std::vector<graphlab::vertex_id_type> vid_vec;
   graphlab::hopscotch_set<graphlab::vertex_id_type> *cset;
   vid_vector(): cset(NULL) { }
   vid_vector(const vid_vector& v):cset(NULL) {
     (*this) = v;
   }
 
   vid_vector& operator=(const vid_vector& v) {
     if (this == &v) return *this;
     vid_vec = v.vid_vec;
     if (v.cset != NULL) {
       // allocate the cuckoo set if the other side is using a cuckoo set
       // or clear if I alrady have one
       if (cset == NULL) {
         cset = new graphlab::hopscotch_set<graphlab::vertex_id_type>(HASH_THRESHOLD);
       }
       else {
         cset->clear();
       }
       (*cset) = *(v.cset);
     }
     else {
       // if the other side is not using a cuckoo set, lets not use a cuckoo set
       // either
       if (cset != NULL) {
         delete cset;
         cset = NULL;
       }
     }
     return *this;
   }
 
   ~vid_vector() {
     if (cset != NULL) delete cset;
   }
 
   // assigns a vector of vertex IDs to this storage.
   // this function will clear the contents of the vid_vector
   // and reconstruct it.
   // If the assigned values has length >= HASH_THRESHOLD,
   // we will allocate a cuckoo set to store it. Otherwise,
   // we just store a sorted vector
   void assign(const std::vector<graphlab::vertex_id_type>& vec) {
     clear();
     if (vec.size() >= HASH_THRESHOLD) {
         // move to cset
         cset = new graphlab::hopscotch_set<graphlab::vertex_id_type>(HASH_THRESHOLD);
         foreach (graphlab::vertex_id_type v, vec) {
           cset->insert(v);
         }
     }
     else {
       vid_vec = vec;
       if (vid_vec.size() > 64) {
         radix_sort(&(vid_vec[0]), 0, vid_vec.size(), 24);
       }
       else {
         std::sort(vid_vec.begin(), vid_vec.end());
       }
       std::vector<graphlab::vertex_id_type>::iterator new_end = std::unique(vid_vec.begin(),
                                                vid_vec.end());
       vid_vec.erase(new_end, vid_vec.end());
     }
   }
 
   void save(graphlab::oarchive& oarc) const {
     oarc << (cset != NULL);
     if (cset == NULL) oarc << vid_vec;
     else oarc << (*cset);
   }
 
 
   void clear() {
     vid_vec.clear();
     if (cset != NULL) {
       delete cset;
       cset = NULL;
     }
   }
 
   size_t size() const {
     return cset == NULL ? vid_vec.size() : cset->size();
   }
 
   void load(graphlab::iarchive& iarc) {
     clear();
     bool hascset;
     iarc >> hascset;
     if (!hascset) iarc >> vid_vec;
     else {
       cset = new graphlab::hopscotch_set<graphlab::vertex_id_type>(HASH_THRESHOLD);
       iarc >> (*cset);
     }
   }
 };
 
 /*
   A simple counting iterator which can be used as an insert iterator.
   but only counts the number of elements inserted. Useful for
   use with counting the size of an intersection using std::set_intersection
 */
 template <typename T>
 struct counting_inserter {
   size_t* i;
   counting_inserter(size_t* i):i(i) { }
   counting_inserter& operator++() {
     ++(*i);
     return *this;
   }
   void operator++(int) {
     ++(*i);
   }
 
   struct empty_val {
     empty_val operator=(const T&) { return empty_val(); }
   };
 
   empty_val operator*() {
     return empty_val();
   }
 
   typedef empty_val reference;
 };
 
 
 /*
  * Computes the size of the intersection of two vid_vector's
  */
 static uint32_t count_set_intersect(
              const vid_vector& smaller_set,
              const vid_vector& larger_set) {
 
   if (smaller_set.cset == NULL && larger_set.cset == NULL) {
     size_t i = 0;
     counting_inserter<graphlab::vertex_id_type> iter(&i);
     std::set_intersection(smaller_set.vid_vec.begin(), smaller_set.vid_vec.end(),
                           larger_set.vid_vec.begin(), larger_set.vid_vec.end(),
                           iter);
     return i;
   }
   else if (smaller_set.cset == NULL && larger_set.cset != NULL) {
     size_t i = 0;
     foreach(graphlab::vertex_id_type vid, smaller_set.vid_vec) {
       i += larger_set.cset->count(vid);
     }
     return i;
   }
   else if (smaller_set.cset != NULL && larger_set.cset == NULL) {
     size_t i = 0;
     foreach(graphlab::vertex_id_type vid, larger_set.vid_vec) {
       i += smaller_set.cset->count(vid);
     }
     return i;
   }
   else {
     size_t i = 0;
     foreach(graphlab::vertex_id_type vid, *(smaller_set.cset)) {
       i += larger_set.cset->count(vid);
     }
     return i;
 
   }
 }
 
 
 
 
 
 
 /*
  * Each vertex maintains a list of all its neighbors.
  * and a final count for the number of triangles it is involved in
  */
 struct vertex_data_type {
   vertex_data_type(): num_triangles(0){ }
   // A list of all its neighbors
   vid_vector vid_set;
   // The number of triangles this vertex is involved it.
   // only used if "per vertex counting" is used
   uint32_t num_triangles;
   void save(graphlab::oarchive &oarc) const {
     oarc << vid_set << num_triangles;
   }
   void load(graphlab::iarchive &iarc) {
     iarc >> vid_set >> num_triangles;
   }
 };
 
 
 /*
  * Each edge is simply a counter of triangles
  */
 typedef uint32_t edge_data_type;
 
 // To collect the set of neighbors, we need a message type which is
 // basically a set of vertex IDs
 
 bool PER_VERTEX_COUNT = false;
 
 
 /*
  * This is the gathering type which accumulates an array of
  * all neighboring vertices.
  * It is a simple wrapper around a vector with
  * an operator+= which simply performs a  +=
  */
 struct set_union_gather {
   graphlab::vertex_id_type v;
   std::vector<graphlab::vertex_id_type> vid_vec;
 
   set_union_gather():v(-1) {
   }
 
   size_t size() const {
     if (v == (graphlab::vertex_id_type)-1) return vid_vec.size();
     else return 1;
   }
   /*
    * Combining with another collection of vertices.
    * Union it into the current set.
    */
   set_union_gather& operator+=(const set_union_gather& other) {
     if (size() == 0) {
       (*this) = other;
       return (*this);
     }
     else if (other.size() == 0) {
       return *this;
     }
 
     if (vid_vec.size() == 0) {
       vid_vec.push_back(v);
       v = (graphlab::vertex_id_type)(-1);
     }
     if (other.vid_vec.size() > 0) {
       size_t ct = vid_vec.size();
       vid_vec.resize(vid_vec.size() + other.vid_vec.size());
       for (size_t i = 0; i < other.vid_vec.size(); ++i) {
         vid_vec[ct + i] = other.vid_vec[i];
       }
     }
     else if (other.v != (graphlab::vertex_id_type)-1) {
       vid_vec.push_back(other.v);
     }
     return *this;
   }
   
   // serialize
   void save(graphlab::oarchive& oarc) const {
     oarc << bool(vid_vec.size() == 0);
     if (vid_vec.size() == 0) oarc << v;
     else oarc << vid_vec;
   }
 
   // deserialize
   void load(graphlab::iarchive& iarc) {
     bool novvec;
     v = (graphlab::vertex_id_type)(-1);
     vid_vec.clear();
     iarc >> novvec;
     if (novvec) iarc >> v;
     else iarc >> vid_vec;
   }
 };
 
 /*
  * Define the type of the graph
  */
 typedef graphlab::distributed_graph<vertex_data_type,
                                     edge_data_type> graph_type;
 
 
 
 /*
  * This class implements the triangle counting algorithm as described in
  * the header. On gather, we accumulate a set of all adjacent vertices.
  * If per_vertex output is not necessary, we can use the optimization
  * where each vertex only accumulates neighbors with greater vertex IDs.
  */
 class triangle_count :
       public graphlab::ivertex_program<graph_type,
                                       set_union_gather>,
       /* I have no data. Just force it to POD */
       public graphlab::IS_POD_TYPE  {
 public:
   bool do_not_scatter;
 
   // Gather on all edges
   edge_dir_type gather_edges(icontext_type& context,
                              const vertex_type& vertex) const {
     return graphlab::ALL_EDGES;
   } 
 
   /*
    * For each edge, figure out the ID of the "other" vertex
    * and accumulate a set of the neighborhood vertex IDs.
    */
   gather_type gather(icontext_type& context,
                      const vertex_type& vertex,
                      edge_type& edge) const {
     set_union_gather gather;
     graphlab::vertex_id_type otherid = edge.target().id() == vertex.id() ?
                                        edge.source().id() : edge.target().id();
 
     size_t other_nbrs = (edge.target().id() == vertex.id()) ?
         (edge.source().num_in_edges() + edge.source().num_out_edges()): 
         (edge.target().num_in_edges() + edge.target().num_out_edges());
 
     size_t my_nbrs = vertex.num_in_edges() + vertex.num_out_edges();
 
     if (PER_VERTEX_COUNT || (other_nbrs > my_nbrs) || (other_nbrs == my_nbrs && otherid > vertex.id())) {
     //if (PER_VERTEX_COUNT || otherid > vertex.id()) {
      gather.v = otherid;
     } 
     return gather;
   }
 
   /*
    * the gather result now contains the vertex IDs in the neighborhood.
    * store it on the vertex. 
    */
   void apply(icontext_type& context, vertex_type& vertex,
              const gather_type& neighborhood) {
    do_not_scatter = false;
    if (neighborhood.vid_vec.size() == 0) {
      // neighborhood set may be empty or has only 1 element
      vertex.data().vid_set.clear();
      if (neighborhood.v != (graphlab::vertex_id_type(-1))) {
        vertex.data().vid_set.vid_vec.push_back(neighborhood.v);
      }
    }
    else {
      vertex.data().vid_set.assign(neighborhood.vid_vec);
    }
    do_not_scatter = vertex.data().vid_set.size() == 0;
   } // end of apply
 
   /*
    * Scatter over all edges to compute the intersection.
    * I only need to touch each edge once, so if I scatter just on the
    * out edges, that is sufficient.
    */
   edge_dir_type scatter_edges(icontext_type& context,
                               const vertex_type& vertex) const {
     if (do_not_scatter) return graphlab::NO_EDGES;
     else return graphlab::OUT_EDGES;
   }
 
 
   /*
    * For each edge, count the intersection of the neighborhood of the
    * adjacent vertices. This is the number of triangles this edge is involved
    * in.
    */
   void scatter(icontext_type& context,
               const vertex_type& vertex,
               edge_type& edge) const {
 
     //    vertex_type othervtx = edge.target();
     const vertex_data_type& srclist = edge.source().data();
     const vertex_data_type& targetlist = edge.target().data();
     if (targetlist.vid_set.size() < srclist.vid_set.size()) {
       edge.data() += count_set_intersect(targetlist.vid_set, srclist.vid_set);
     }
     else {
       edge.data() += count_set_intersect(srclist.vid_set, targetlist.vid_set);
     }
   }
 };
 
 /*
  * This class is used in a second engine call if per vertex counts are needed.
  * The number of triangles a vertex is involved in can be computed easily
  * by summing over the number of triangles each adjacent edge is involved in
  * and dividing by 2. 
  */
 class get_per_vertex_count :
       public graphlab::ivertex_program<graph_type, size_t>,
       /* I have no data. Just force it to POD */
       public graphlab::IS_POD_TYPE  {
 public:
   // Gather on all edges
   edge_dir_type gather_edges(icontext_type& context,
                              const vertex_type& vertex) const {
     return graphlab::ALL_EDGES;
   }
   // We gather the number of triangles each edge is involved in
   size_t gather(icontext_type& context,
                      const vertex_type& vertex,
                      edge_type& edge) const {
     return edge.data();
   }
 
   /* the gather result is the total sum of the number of triangles
    * each adjacent edge is involved in . Dividing by 2 gives the
    * desired result.
    */
   void apply(icontext_type& context, vertex_type& vertex,
              const gather_type& num_triangles) {
     vertex.data().vid_set.clear();
     vertex.data().num_triangles = num_triangles / 2;
   }
 
   // No scatter
   edge_dir_type scatter_edges(icontext_type& context,
                              const vertex_type& vertex) const {
     return graphlab::NO_EDGES;
   }
 
 
 };
 
 typedef graphlab::synchronous_engine<triangle_count> engine_type;
 
 /* Used to sum over all the edges in the graph in a
  * map_reduce_edges call
  * to get the total number of triangles
  */
 size_t get_edge_data(const graph_type::edge_type& e) {
   return e.data();
 }
 
 /*
  * A saver which saves a file where each line is a vid / # triangles pair
  */
 struct save_triangle_count{
   std::string save_vertex(graph_type::vertex_type v) { 
     double nt = v.data().num_triangles;
     double n_followed = v.num_out_edges();
     double n_following = v.num_in_edges();
 
     return graphlab::tostr(v.id()) + "\t" +
            graphlab::tostr(nt) + "\t" +
            graphlab::tostr(n_followed) + "\t" + 
            graphlab::tostr(n_following) + "\n";
   }
   std::string save_edge(graph_type::edge_type e) {
     return "";
   }
 };
 
 
 int main(int argc, char** argv) {
   std::cout << "This program counts the exact number of triangles in the "
             "provided graph.\n\n";
 
   graphlab::command_line_options clopts("Exact Triangle Counting. "
     "Given a graph, this program computes the total number of triangles "
     "in the graph. An option (per_vertex) is also provided which "
     "computes for each vertex, the number of triangles it is involved in."
     "The algorithm assumes that each undirected edge appears exactly once "
     "in the graph input. If edges may appear more than once, this procedure "
     "will over count.");
   std::string prefix, format;
   std::string per_vertex;
   clopts.attach_option("graph", prefix,
                        "Graph input. reads all graphs matching prefix*");
   clopts.attach_option("format", format,
                        "The graph format");
  clopts.attach_option("ht", HASH_THRESHOLD,
                        "Above this size, hash sets are used");
   clopts.attach_option("per_vertex", per_vertex,
                        "If not empty, will count the number of "
                        "triangles each vertex belongs to and "
                        "save to file with prefix \"[per_vertex]\". "
                        "The algorithm used is slightly different "
                        "and thus will be a little slower");
   if(!clopts.parse(argc, argv)) return EXIT_FAILURE;
   if (prefix == "") {
     std::cout << "--graph is not optional\n";
     clopts.print_description();
     return EXIT_FAILURE;
   }
   else if (format == "") {
     std::cout << "--format is not optional\n";
     clopts.print_description();
     return EXIT_FAILURE;
   }
 
 
   if (per_vertex != "") PER_VERTEX_COUNT = true;
   // Initialize control plane using mpi
   graphlab::mpi_tools::init(argc, argv);
   graphlab::distributed_control dc;
 
   graphlab::launch_metric_server();
   // load graph
   graph_type graph(dc, clopts);
   graph.load_format(prefix, format);
   graph.finalize();
   dc.cout() << "Number of vertices: " << graph.num_vertices() << std::endl
             << "Number of edges:    " << graph.num_edges() << std::endl;
 
   graphlab::timer ti;
   
   // create engine to count the number of triangles
   dc.cout() << "Counting Triangles..." << std::endl;
   engine_type engine(dc, graph, clopts);
   engine.signal_all();
   engine.start();
 
   dc.cout() << "Counted in " << ti.current_time() << " seconds" << std::endl;
 
   if (PER_VERTEX_COUNT == false) {
     size_t count = graph.map_reduce_edges<size_t>(get_edge_data);
     dc.cout() << count << " Triangles"  << std::endl;
   }
   else {
     graphlab::synchronous_engine<get_per_vertex_count> engine(dc, graph, clopts);
     engine.signal_all();
     engine.start();
     graph.save(per_vertex,
             save_triangle_count(),
             false, /* no compression */
             true, /* save vertex */
             false, /* do not save edge */
             clopts.get_ncpus()); /* one file per machine */
 
   }
   
   graphlab::stop_metric_server();
 
   graphlab::mpi_tools::finalize();
   return EXIT_SUCCESS;
 } // End of main
 
 