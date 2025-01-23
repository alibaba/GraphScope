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

void IGraphGraphProxy::edge(size_t eid, size_t& from, size_t& to) const {
  from = IGRAPH_FROM(graph_, eid);
  to = IGRAPH_TO(graph_, eid);
}

////////////////////////////////////////GraphDBGraphProxy////////////////////////////////////////
// [Out,In]|[Out,In]
size_t GraphDBGraphProxy::generate_eid(size_t src, igraph_neimode_t mode,
                                       size_t offset, size_t dst) const {
  if (mode == IGRAPH_IN) {
    // start from dst, find the edge from dst to src
    auto oes = sess_.graph().get_outgoing_edges(0, dst, 0, 0);
    size_t cur_cnt = 0;
    while (oes->is_valid()) {
      if (oes->get_neighbor() == src) {
        break;
      }
      cur_cnt++;
      oes->next();
    }
    CHECK(oes->is_valid()) << "Cannot find edge from " << dst << " to " << src;
    return (edges_cnt_out_[dst] + cur_cnt);
  } else if (mode == IGRAPH_OUT) {
    return (edges_cnt_out_[src] + offset);
  } else {
    throw Exception("Incorrect mode specified.");
  }
}

size_t GraphDBGraphProxy::vertex_num() const { return sess_.vertex_num(); }
size_t GraphDBGraphProxy::edge_num() const { return sess_.edge_num(); }
bool GraphDBGraphProxy::is_directed() const { return true; }
bool GraphDBGraphProxy::has_self_loops() const { return has_self_loops_; }

void GraphDBGraphProxy::initialize() {
  if (initialized_) {
    return;
  }
  CHECK(sess_.graph().schema().vertex_label_num() == 1 &&
        sess_.graph().schema().edge_label_num() == 1);
  // Initialize has_self_loops_
  for (size_t i = 0; i < sess_.vertex_num(); ++i) {
    auto oes = sess_.graph().get_outgoing_edges(0, i, 0, 0);
    while (oes->is_valid()) {
      size_t dst = oes->get_neighbor();
      if (i == dst) {
        has_self_loops_ = true;
        break;
      }
      oes->next();
    }
    if (has_self_loops_) {
      break;
    }
  }
  // initialize edges_cnt_all_
  edges_cnt_all_.resize(sess_.vertex_num() + 1, 0);
  edges_cnt_in_.resize(sess_.vertex_num() + 1, 0);
  edges_cnt_out_.resize(sess_.vertex_num() + 1, 0);
  edges_cnt_all_[0] = 0;
  edges_cnt_in_[0] = 0;
  edges_cnt_out_[0] = 0;
  for (size_t i = 1; i <= sess_.vertex_num(); ++i) {
    auto oes = sess_.graph().get_outgoing_edges(0, i - 1, 0, 0);
    size_t cur_cnt = 0;
    while (oes->is_valid()) {
      cur_cnt++;
      oes->next();
    }
    edges_cnt_out_[i] = cur_cnt + edges_cnt_out_[i - 1];
    auto ies = sess_.graph().get_incoming_edges(0, i - 1, 0, 0);
    cur_cnt = 0;
    while (ies->is_valid()) {
      cur_cnt++;
      ies->next();
    }
    edges_cnt_in_[i] = cur_cnt + edges_cnt_in_[i - 1];
    edges_cnt_all_[i] = edges_cnt_out_[i] + edges_cnt_in_[i];
  }
  CHECK(edges_cnt_out_[sess_.vertex_num()] == sess_.edge_num())
      << "edges_cnt_out_[" << sess_.vertex_num()
      << "] = " << edges_cnt_out_[sess_.vertex_num()]
      << " != " << sess_.edge_num();
  CHECK(edges_cnt_all_[sess_.vertex_num()] == 2 * sess_.edge_num())
      << "edges_cnt_all_[" << sess_.vertex_num()
      << "] = " << edges_cnt_all_[sess_.vertex_num()]
      << " != " << 2 * sess_.edge_num();

  initialized_ = true;
}

std::vector<size_t> GraphDBGraphProxy::incident(size_t v,
                                                igraph_neimode_t mode) const {
  if (mode == IGRAPH_IN) {
    auto ies = sess_.graph().get_incoming_edges(0, v, 0, 0);
    std::vector<size_t> result;
    size_t offset = 0;
    while (ies->is_valid()) {
      result.push_back(generate_eid(v, mode, offset++, ies->get_neighbor()));
      ies->next();
    }
    return result;
  } else if (mode == IGRAPH_OUT) {
    auto oes = sess_.graph().get_outgoing_edges(0, v, 0, 0);
    std::vector<size_t> result;
    size_t offset = 0;
    while (oes->is_valid()) {
      result.push_back(generate_eid(v, mode, offset++, oes->get_neighbor()));
      oes->next();
    }
    return result;
  } else if (mode == IGRAPH_ALL) {
    auto ies = sess_.graph().get_incoming_edges(0, v, 0, 0);
    auto oes = sess_.graph().get_outgoing_edges(0, v, 0, 0);
    std::vector<size_t> result;
    size_t offset = 0;
    while (ies->is_valid()) {
      result.push_back(
          generate_eid(v, IGRAPH_IN, offset++, ies->get_neighbor()));
      ies->next();
    }
    offset = 0;
    while (oes->is_valid()) {
      result.push_back(
          generate_eid(v, IGRAPH_OUT, offset++, oes->get_neighbor()));
      oes->next();
    }
    return result;
  } else {
    throw Exception("Incorrect mode specified.");
  }
}

std::vector<size_t> GraphDBGraphProxy::neighbors(size_t v,
                                                 igraph_neimode_t mode) const {
  if (mode == IGRAPH_IN) {
    auto ies = sess_.graph().get_incoming_edges(0, v, 0, 0);
    std::vector<size_t> result;
    while (ies->is_valid()) {
      result.push_back(ies->get_neighbor());
      ies->next();
    }
    return result;
  } else if (mode == IGRAPH_OUT) {
    auto oes = sess_.graph().get_outgoing_edges(0, v, 0, 0);
    std::vector<size_t> result;
    while (oes->is_valid()) {
      result.push_back(oes->get_neighbor());
      oes->next();
    }
    return result;
  } else if (mode == IGRAPH_ALL) {
    auto ies = sess_.graph().get_incoming_edges(0, v, 0, 0);
    auto oes = sess_.graph().get_outgoing_edges(0, v, 0, 0);
    std::vector<size_t> result;
    while (ies->is_valid()) {
      result.push_back(ies->get_neighbor());
      ies->next();
    }
    while (oes->is_valid()) {
      result.push_back(oes->get_neighbor());
      oes->next();
    }
    return result;
  } else {
    throw Exception("Incorrect mode specified.");
  }
}

size_t GraphDBGraphProxy::get_random_neighbour(size_t v, igraph_neimode_t mode,
                                               IGraphProxyRNG* rng) const {
  if (this->degree(v, mode) <= 0) {
    throw Exception("Cannot select a random neighbour for an isolated node.");
  }
  if (mode == IGRAPH_ALL) {
    auto oe_degreee = degree(v, IGRAPH_OUT);
    auto ie_degreee = degree(v, IGRAPH_IN);
    size_t rand_idx = get_random_int(0, oe_degreee + ie_degreee - 1, rng);
    if (rand_idx >= oe_degreee) {
      auto ies = sess_.graph().get_incoming_edges(0, v, 0, 0);
      size_t offset = 0;
      while (ies->is_valid()) {
        if (offset == rand_idx - oe_degreee) {
          return ies->get_neighbor();
        }
        offset++;
        ies->next();
      }
      LOG(FATAL) << "Should not reach here.";
    } else {
      auto oes = sess_.graph().get_outgoing_edges(0, v, 0, 0);
      size_t offset = 0;
      while (oes->is_valid()) {
        if (offset == rand_idx) {
          return oes->get_neighbor();
        }
        offset++;
        oes->next();
      }
      LOG(FATAL) << "Should not reach here.";
    }
  } else if (mode == IGRAPH_OUT) {
    auto oes = sess_.graph().get_outgoing_edges(0, v, 0, 0);
    size_t offset = 0;
    size_t oe_degree = degree(v, IGRAPH_OUT);
    size_t rand_idx = get_random_int(0, oe_degree - 1, rng);
    while (oes->is_valid()) {
      if (offset == rand_idx) {
        return oes->get_neighbor();
      }
      offset++;
      oes->next();
    }
    LOG(FATAL) << "Should not reach here.";
  } else if (mode == IGRAPH_IN) {
    auto ies = sess_.graph().get_incoming_edges(0, v, 0, 0);
    size_t offset = 0;
    size_t ie_degree = degree(v, IGRAPH_IN);
    size_t rand_idx = get_random_int(0, ie_degree - 1, rng);
    while (ies->is_valid()) {
      if (offset == rand_idx) {
        return ies->get_neighbor();
      }
      offset++;
      ies->next();
    }
    LOG(FATAL) << "Should not reach here.";
  } else {
    throw Exception("Incorrect mode specified.");
  }
}

size_t GraphDBGraphProxy::degree(size_t v, igraph_neimode_t mode) const {
  if (mode == IGRAPH_IN) {
    return edges_cnt_in_[v + 1] - edges_cnt_in_[v];
  } else if (mode == IGRAPH_OUT) {
    return edges_cnt_out_[v + 1] - edges_cnt_out_[v];
  } else if (mode == IGRAPH_ALL) {
    return edges_cnt_all_[v + 1] - edges_cnt_all_[v];
  } else {
    throw Exception("Incorrect mode specified.");
  }
}

IGraphProxyRNG* GraphDBGraphProxy::create_rng() const {
  return new GraphDBGraphProxyRNG(vertex_num(), edge_num());
}

// When getting eid with src_node and dst_node, we only return the first edge.
int64_t GraphDBGraphProxy::get_eid(size_t src_node, size_t dst_node,
                                   bool directed) const {
  size_t offset = 0;
  auto oes = sess_.graph().get_outgoing_edges(0, src_node, 0, 0);
  offset = 0;
  while (oes->is_valid()) {
    if (oes->get_neighbor() == dst_node) {
      return generate_eid(src_node, IGRAPH_OUT, offset, dst_node);
    }
    offset++;
    oes->next();
  }
  if (!directed) {
    auto ies = sess_.graph().get_incoming_edges(0, dst_node, 0, 0);
    while (ies->is_valid()) {
      if (ies->get_neighbor() == src_node) {
        return generate_eid(dst_node, IGRAPH_IN, offset, src_node);
      }
      offset++;
      ies->next();
    }
  }

  return -1;
}

void GraphDBGraphProxy::edge(size_t eid, size_t& from, size_t& to) const {
  size_t offset = eid;
  size_t cur_src = 0;
  while (cur_src < vertex_num() && edges_cnt_out_[cur_src + 1] <= offset) {
    cur_src++;
  }
  offset = offset - edges_cnt_out_[cur_src];
  VLOG(10) << "getting from and to from edge:" << eid << " offset:" << offset
           << " cur_src:" << cur_src;
  auto out_degree = degree(cur_src, IGRAPH_OUT);
  CHECK(offset < out_degree)
      << "offset: " << offset << ", out_degree: " << out_degree;
  auto oes = sess_.graph().get_outgoing_edges(0, cur_src, 0, 0);
  size_t cur_offset = 0;
  while (oes->is_valid()) {
    if (cur_offset == offset) {
      VLOG(10) << "Found edge: " << cur_src << " -> " << oes->get_neighbor();
      from = cur_src;
      to = oes->get_neighbor();
      return;
    }
    cur_offset++;
    oes->next();
  }
  LOG(FATAL) << "Not found: " << cur_src << " -> " << offset;
}