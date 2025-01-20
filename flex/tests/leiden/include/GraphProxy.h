#ifndef GRAPH_PROXY_INCLUDED
#define GRAPH_PROXY_INCLUDED

#include <igraph/igraph.h>
#include <vector>

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
};

// corresponding to igraph_rng_t

// Assuming vid is less than uint32_t, and only one vertex_label. We use first
// 32bits to store source vid, and the rest to store dst vid.
inline size_t get_source_vid_from_eid(size_t eid) { return eid >> 32; }

inline size_t get_dst_vid_from_eid(size_t eid) {
  size_t mask = 0xFFFFFFFF;  // 2^32 - 1
  return eid & mask;
}

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

 private:
  inline void edge(size_t eid, size_t& from, size_t& to) {
    // from = IGRAPH_FROM(this->get_igraph(), eid);
    from = get_source_vid_from_eid(eid);
    // to = IGRAPH_TO(this->get_igraph(), eid);
    to = get_dst_vid_from_eid(eid);
  }
  igraph_t* graph_;

  vector<size_t> _degree_in;
  vector<size_t> _degree_out;
  vector<size_t> _degree_all;
};

class GraphDBGraphProxy : public IGraphProxy {
 public:
  GraphDBGraphProxy(igraph_t* graph);
  ~GraphDBGraphProxy();
  size_t vertex_num() const override;
  size_t edge_num() const override;
  bool is_directed() const override;
  bool has_self_loops() const override;
  // return incident edges.
  std::vector<size_t> incident(size_t v, igraph_neimode_t mode) const override;
  std::vector<size_t> neighbors(size_t v, igraph_neimode_t mode) const override;
  size_t get_random_neighbour(size_t v, igraph_neimode_t mode,
                              IGraphProxyRNG* rng) const override;
};

#endif  // GRAPH_PROXY_INCLUDED