#ifndef GRAPH_PROXY_INCLUDED
#define GRAPH_PROXY_INCLUDED

#include <igraph/igraph.h>
#include <vector>
#include "flex/engines/graph_db/database/graph_db_session.h"

using std::vector;

class IGraphProxyRNG {
 public:
  virtual size_t vertex_num() const = 0;
  virtual size_t edge_num() const = 0;
};

inline size_t graph_proxy_get_integer(IGraphProxyRNG* rng, size_t from,
                                      size_t to) {
  size_t random_seed = rng->vertex_num();
  // use random seed to generate number between [from, to]
  std::srand(random_seed);
  return std::rand() % (to - from + 1) + from;
}

inline size_t get_random_int(size_t from, size_t to, IGraphProxyRNG* rng) {
  // return igraph_rng_get_integer(rng, from, to);
  return graph_proxy_get_integer(rng, from, to);
};

class Exception : public std::exception {
 public:
  Exception(const char* str) { this->str = str; }

  virtual const char* what() const throw() { return this->str; }

 private:
  const char* str;
};

class IGraphProxy {
 public:
  IGraphProxy() {}
  virtual ~IGraphProxy() {}
  virtual size_t vertex_num() const = 0;
  virtual size_t edge_num() const = 0;
  virtual bool is_directed() const = 0;
  virtual bool has_self_loops() const = 0;
  // return incident edges.
  virtual std::vector<size_t> incident(size_t v,
                                       igraph_neimode_t mode) const = 0;
  virtual std::vector<size_t> neighbors(size_t v,
                                        igraph_neimode_t mode) const = 0;
  virtual size_t get_random_neighbour(size_t v, igraph_neimode_t mode,
                                      IGraphProxyRNG* rng) const = 0;
  virtual size_t degree(size_t v, igraph_neimode_t mode) const = 0;

  virtual IGraphProxyRNG* create_rng() const = 0;
  virtual int64_t get_eid(size_t src_node, size_t dst_node,
                          bool directed) const = 0;
  virtual void edge(size_t eid, size_t& from, size_t& to) const = 0;
};

// corresponding to igraph_rng_t

// inline size_t generate_edge_id(size_t src, size_t dst) {
//   return (src << 32) | dst;
// }

class IGraphGraphProxyRNG : public IGraphProxyRNG {
 public:
  IGraphGraphProxyRNG(igraph_t* g) : g_(g) {}
  ~IGraphGraphProxyRNG() {}
  size_t vertex_num() const override { return igraph_vcount(g_); }
  size_t edge_num() const override { return igraph_ecount(g_); }

 private:
  igraph_t* g_;
};

class GraphDBGraphProxyRNG : public IGraphProxyRNG {
 public:
  GraphDBGraphProxyRNG(size_t vertex_num, size_t edge_num)
      : vertex_num_(vertex_num), edge_num_(edge_num) {}
  ~GraphDBGraphProxyRNG() {}
  size_t vertex_num() const override { return vertex_num_; }
  size_t edge_num() const override { return edge_num_; }

 private:
  size_t vertex_num_, edge_num_;
};

class IGraphGraphProxy : public IGraphProxy {
 public:
  IGraphGraphProxy(igraph_t* graph) : graph_(graph) {
    auto n = igraph_vcount(graph);
    this->_degree_in.clear();
    this->_degree_in.resize(n, 0.0);

    if (this->is_directed()) {
      this->_degree_out.clear();
      this->_degree_out.resize(n, 0);

      this->_degree_all.clear();
      this->_degree_all.resize(n, 0);
    }

    auto m = edge_num();

    for (size_t e = 0; e < m; e++) {
      size_t from, to;
      this->edge(e, from, to);

      if (this->is_directed()) {
        this->_degree_in[to]++;
        this->_degree_out[from]++;
        this->_degree_all[to]++;
        this->_degree_all[from]++;
      } else {
        // recall that igraph ignores the mode for undirected graphs
        this->_degree_in[to]++;
        this->_degree_in[from]++;
      }
    }
  }
  ~IGraphGraphProxy();
  size_t vertex_num() const override;
  size_t edge_num() const override;
  bool is_directed() const override;
  bool has_self_loops() const override;
  // return incident edges.
  std::vector<size_t> incident(size_t v, igraph_neimode_t mode) const override;
  std::vector<size_t> neighbors(size_t v, igraph_neimode_t mode) const override;
  size_t get_random_neighbour(size_t v, igraph_neimode_t mode,
                              IGraphProxyRNG* rng) const override;
  size_t degree(size_t v, igraph_neimode_t mode) const override;

  IGraphProxyRNG* create_rng() const override {
    return new IGraphGraphProxyRNG(this->graph_);
  }

  int64_t get_eid(size_t src_node, size_t dst_node, bool directed) const {
    igraph_integer_t eid;
    igraph_bool_t error = false;
    igraph_get_eid(this->graph_, &eid, src_node, dst_node, directed, error);
    return eid;
  }

  void edge(size_t eid, size_t& from, size_t& to) const override;

 private:
  igraph_t* graph_;

  vector<size_t> _degree_in;
  vector<size_t> _degree_out;
  vector<size_t> _degree_all;
};

class GraphDBGraphProxy : public IGraphProxy {
 public:
  // We use size_t as edge_id, which should be uint64_t.
  // We use the top bit to denote the direction of the edge.
  static constexpr size_t kEdgeIdMask = 0x8000000000000000;
  // static constexpr size_t Dir_Out = 0x8000000000000000;
  // static constexpr size_t Dir_In = 0x0000000000000000;
  GraphDBGraphProxy(const gs::GraphDBSession& sess)
      : sess_(sess), initialized_(false), has_self_loops_(false) {
    initialize();
  }
  ~GraphDBGraphProxy() {}
  size_t vertex_num() const override;
  size_t edge_num() const override;
  bool is_directed() const override;
  bool has_self_loops() const override;
  // return incident edges.
  std::vector<size_t> incident(size_t v, igraph_neimode_t mode) const override;
  std::vector<size_t> neighbors(size_t v, igraph_neimode_t mode) const override;
  size_t get_random_neighbour(size_t v, igraph_neimode_t mode,
                              IGraphProxyRNG* rng) const override;
  size_t degree(size_t v, igraph_neimode_t mode) const override;

  IGraphProxyRNG* create_rng() const override;
  int64_t get_eid(size_t src_node, size_t dst_node,
                  bool directed) const override;
  void edge(size_t eid, size_t& from, size_t& to) const override;

 private:
  void initialize();
  size_t generate_eid(size_t src, igraph_neimode_t mode, size_t offset,
                      size_t dst) const;
  const gs::GraphDBSession& sess_;
  bool initialized_;
  bool has_self_loops_;
  // edges_cnt[vid] stores the number of edges that from i < vid.

  std::vector<size_t> edges_cnt_all_;
  std::vector<size_t> edges_cnt_out_, edges_cnt_in_;
};

#endif  // GRAPH_PROXY_INCLUDED