#ifndef GRAPHHELPER_INCLUDED
#define GRAPHHELPER_INCLUDED

#include <igraph/igraph.h>

// #include "libleidenalg_export.h"

#include <deque>
#include <exception>
#include <set>
#include <vector>

#include <cstdbool>

//#ifdef DEBUG
#include <iostream>
using std::cerr;
using std::endl;
//#endif

#include "flex/tests/leiden/include/GraphProxy.h"

class MutableVertexPartition;

using std::deque;
using std::make_pair;
using std::pair;
using std::set;
using std::vector;

vector<size_t> range(size_t n);

bool orderCSize(const size_t* A, const size_t* B);

double KL(double q, double p);
double KLL(double q, double p);

template <class T>
T sum(vector<T> vec) {
  T sum_of_elems = T();
  for (T x : vec)
    sum_of_elems += x;
  return sum_of_elems;
};

void shuffle(vector<size_t>& v, IGraphProxyRNG* rng);

class Graph {
 public:
  // Graph(igraph_t* graph, vector<double> const& edge_weights,
  //       vector<double> const& node_sizes,
  //       vector<double> const& node_self_weights, int correct_self_loops);
  // Graph(igraph_t* graph, vector<double> const& edge_weights,
  //       vector<double> const& node_sizes,
  //       vector<double> const& node_self_weights);
  // Graph(igraph_t* graph, vector<double> const& edge_weights,
  //       vector<double> const& node_sizes, int correct_self_loops);
  // Graph(igraph_t* graph, vector<double> const& edge_weights,
  //       vector<double> const& node_sizes);
  // Graph(igraph_t* graph, int correct_self_loops);
  // Graph(igraph_t* graph);
  Graph(IGraphProxy* graph, vector<double> const& edge_weights,
        vector<double> const& node_sizes,
        vector<double> const& node_self_weights, int correct_self_loops);
  Graph(IGraphProxy* graph, vector<double> const& edge_weights,
        vector<double> const& node_sizes,
        vector<double> const& node_self_weights);
  Graph(IGraphProxy* graph, vector<double> const& edge_weights,
        vector<double> const& node_sizes, int correct_self_loops);
  Graph(IGraphProxy* graph, vector<double> const& edge_weights,
        vector<double> const& node_sizes);
  Graph(IGraphProxy* graph, int correct_self_loops);
  Graph(IGraphProxy* graph);
  Graph();
  ~Graph();

  static Graph* GraphFromEdgeWeights(IGraphProxy* graph,
                                     vector<double> const& edge_weights,
                                     int correct_self_loops);
  static Graph* GraphFromEdgeWeights(IGraphProxy* graph,
                                     vector<double> const& edge_weights);
  static Graph* GraphFromNodeSizes(IGraphProxy* graph,
                                   vector<double> const& node_sizes,
                                   int correct_self_loops);
  static Graph* GraphFromNodeSizes(IGraphProxy* graph,
                                   vector<double> const& node_sizes);

  int has_self_loops();
  double possible_edges();
  double possible_edges(double n);

  Graph* collapse_graph(MutableVertexPartition* partition);

  vector<size_t> const& get_neighbour_edges(size_t v, igraph_neimode_t mode);
  vector<size_t> const& get_neighbours(size_t v, igraph_neimode_t mode);
  size_t get_random_neighbour(size_t v, igraph_neimode_t mode,
                              IGraphProxyRNG* rng);

  inline size_t get_random_node(IGraphProxyRNG* rng) {
    return get_random_int(0, this->vcount() - 1, rng);
  };

  inline IGraphProxyRNG* create_rng() { return this->_graph->create_rng(); };

  inline const IGraphProxy* get_igraph() { return this->_graph; };

  inline size_t vcount() { return this->_graph->vertex_num(); };
  inline size_t ecount() { return this->_graph->edge_num(); };
  inline double total_weight() { return this->_total_weight; };
  inline double total_size() { return this->_total_size; };
  inline int is_directed() { return this->_is_directed; };
  inline double density() { return this->_density; };
  inline int correct_self_loops() { return this->_correct_self_loops; };
  inline int is_weighted() { return this->_is_weighted; };

  inline double edge_weight(size_t e) {
#ifdef DEBUG
    if (e > this->_edge_weights.size())
      throw Exception("Edges outside of range of edge weights.");
#endif
    return this->_edge_weights[e];
  };

  inline void edge(size_t eid, size_t& from, size_t& to) {
    this->_graph->edge(eid, from, to);
  }

  inline vector<size_t> edge(size_t e) {
    vector<size_t> edge(2);
    this->edge(e, edge[0], edge[1]);
    return edge;
  }

  inline double node_size(size_t v) { return this->_node_sizes[v]; };

  inline double node_self_weight(size_t v) {
    return this->_node_self_weights[v];
  };

  inline size_t degree(size_t v, igraph_neimode_t mode) {
    return this->_graph->degree(v, mode);
  };

  inline double strength(size_t v, igraph_neimode_t mode) {
    if (mode == IGRAPH_IN || !this->is_directed())
      return this->_strength_in[v];
    else if (mode == IGRAPH_OUT)
      return this->_strength_out[v];
    else
      throw Exception("Incorrect mode specified.");
  };

 protected:
  int _remove_graph;

  const IGraphProxy* _graph;
  // igraph_vector_int_t _temp_igraph_vector;

  // Utility variables to easily access the strength of each node
  vector<double> _strength_in;
  vector<double> _strength_out;

  vector<double> _edge_weights;       // Used for the weight of the edges.
  vector<double> _node_sizes;         // Used for the size of the nodes.
  vector<double> _node_self_weights;  // Used for the self weight of the nodes.

  void cache_neighbours(size_t v, igraph_neimode_t mode);
  vector<size_t> _cached_neighs_from;
  size_t _current_node_cache_neigh_from;
  vector<size_t> _cached_neighs_to;
  size_t _current_node_cache_neigh_to;
  vector<size_t> _cached_neighs_all;
  size_t _current_node_cache_neigh_all;

  void cache_neighbour_edges(size_t v, igraph_neimode_t mode);
  vector<size_t> _cached_neigh_edges_from;
  size_t _current_node_cache_neigh_edges_from;
  vector<size_t> _cached_neigh_edges_to;
  size_t _current_node_cache_neigh_edges_to;
  vector<size_t> _cached_neigh_edges_all;
  size_t _current_node_cache_neigh_edges_all;

  double _total_weight;
  double _total_size;
  int _is_weighted;
  bool _is_directed;

  int _correct_self_loops;
  double _density;

  void init_admin();
  void set_defaults();
  void set_default_edge_weight();
  void set_default_node_size();
  void set_self_weights();
};

// We need this ugly way to include the MutableVertexPartition
// to overcome a circular linkage problem.
#include "MutableVertexPartition.h"

#endif  // GRAPHHELPER_INCLUDED
