#include "include/GraphProxy.h"
#include <igraph/igraph.h>
#include <queue>
#include <set>
#include <vector>

using std::deque;
using std::exception;
using std::make_pair;
using std::pair;
using std::set;
using std::vector;

IGraphGraphProxy::~IGraphGraphProxy() { igraph_destroy(graph_); }

size_t IGraphGraphProxy::vertex_num() const { return igraph_vcount(graph_); }

size_t IGraphGraphProxy::edge_num() const { return igraph_ecount(graph_); }

bool IGraphGraphProxy::is_directed() const {
  return igraph_is_directed(graph_);
}

bool IGraphGraphProxy::has_self_loops() const {
  igraph_bool_t has_self_loops;
  igraph_has_loop(graph_, &has_self_loops);
  return has_self_loops;
}

std::vector<size_t> IGraphGraphProxy::incident(size_t v,
                                               igraph_neimode_t mode) const {
  igraph_vector_int_t edges;
  size_t degree = this->degree(v, mode);
  igraph_vector_int_init(&edges, 0);
  igraph_incident(graph_, &edges, v, mode);
  std::vector<size_t> result;
  result.assign(igraph_vector_int_get_ptr(&edges, 0),
                igraph_vector_int_get_ptr(&edges, degree));
  igraph_vector_int_destroy(&edges);
  return result;
}

std::vector<size_t> IGraphGraphProxy::neighbors(size_t v,
                                                igraph_neimode_t mode) const {
  igraph_vector_int_t neighbours;
  size_t degree = this->degree(v, mode);
  igraph_vector_int_init(&neighbours, 0);
  igraph_neighbors(graph_, &neighbours, v, mode);
  std::vector<size_t> result;
  result.assign(igraph_vector_int_get_ptr(&neighbours, 0),
                igraph_vector_int_get_ptr(&neighbours, degree));
  igraph_vector_int_destroy(&neighbours);
  return result;
}

size_t IGraphGraphProxy::degree(size_t v, igraph_neimode_t mode) const {
  if (mode == IGRAPH_IN || !this->is_directed())
    return this->_degree_in[v];
  else if (mode == IGRAPH_OUT)
    return this->_degree_out[v];
  else if (mode == IGRAPH_ALL)
    return this->_degree_all[v];
  else
    throw Exception("Incorrect mode specified.");
}

size_t IGraphGraphProxy::get_random_neighbour(size_t v, igraph_neimode_t mode,
                                              IGraphProxyRNG* rng) const {
  size_t node = v;
  size_t rand_neigh = -1;

  if (this->degree(v, mode) <= 0) {
    throw Exception("Cannot select a random neighbour for an isolated node.");
  }

  if (this->is_directed() && mode != IGRAPH_ALL) {
    if (mode == IGRAPH_OUT) {
      // Get indices of where neighbours are
      size_t cum_degree_this_node = (size_t) VECTOR(this->graph_->os)[node];
      size_t cum_degree_next_node = (size_t) VECTOR(this->graph_->os)[node + 1];
      // Get a random index from them
      size_t rand_neigh_idx =
          get_random_int(cum_degree_this_node, cum_degree_next_node - 1, rng);
// Return the neighbour at that index
#ifdef DEBUG
      cerr << "Degree: " << this->degree(node, mode) << " diff in cumulative : "
           << cum_degree_next_node - cum_degree_this_node << endl;
#endif
      rand_neigh = VECTOR(
          this->graph_->to)[(size_t) VECTOR(this->graph_->oi)[rand_neigh_idx]];
    } else if (mode == IGRAPH_IN) {
      // Get indices of where neighbours are
      size_t cum_degree_this_node = (size_t) VECTOR(this->graph_->is)[node];
      size_t cum_degree_next_node = (size_t) VECTOR(this->graph_->is)[node + 1];
      // Get a random index from them
      size_t rand_neigh_idx =
          get_random_int(cum_degree_this_node, cum_degree_next_node - 1, rng);
#ifdef DEBUG
      cerr << "Degree: " << this->degree(node, mode) << " diff in cumulative : "
           << cum_degree_next_node - cum_degree_this_node << endl;
#endif
      // Return the neighbour at that index
      rand_neigh =
          VECTOR(this->graph_
                     ->from)[(size_t) VECTOR(this->graph_->ii)[rand_neigh_idx]];
    }
  } else {
    // both in- and out- neighbors in a directed graph.
    size_t cum_outdegree_this_node = (size_t) VECTOR(this->graph_->os)[node];
    size_t cum_indegree_this_node = (size_t) VECTOR(this->graph_->is)[node];

    size_t cum_outdegree_next_node =
        (size_t) VECTOR(this->graph_->os)[node + 1];
    size_t cum_indegree_next_node = (size_t) VECTOR(this->graph_->is)[node + 1];

    size_t total_outdegree = cum_outdegree_next_node - cum_outdegree_this_node;
    size_t total_indegree = cum_indegree_next_node - cum_indegree_this_node;

    size_t rand_idx =
        get_random_int(0, total_outdegree + total_indegree - 1, rng);

#ifdef DEBUG
    cerr << "Degree: " << this->degree(node, mode)
         << " diff in cumulative: " << total_outdegree + total_indegree << endl;
#endif
    // From among in or out neighbours?
    if (rand_idx < total_outdegree) {  // From among outgoing neighbours
      size_t rand_neigh_idx = cum_outdegree_this_node + rand_idx;
      rand_neigh = VECTOR(
          this->graph_->to)[(size_t) VECTOR(this->graph_->oi)[rand_neigh_idx]];
    } else {  // From among incoming neighbours
      size_t rand_neigh_idx =
          cum_indegree_this_node + rand_idx - total_outdegree;
      rand_neigh =
          VECTOR(this->graph_
                     ->from)[(size_t) VECTOR(this->graph_->ii)[rand_neigh_idx]];
    }
  }

  return rand_neigh;
}