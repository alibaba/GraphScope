#ifndef ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_I_VERTEX_PROGRAM_H_
#define ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_I_VERTEX_PROGRAM_H_

#include <cstdint>
#include <map>
#include <string>

#include "apps/GatherScatter/Vertex.h"

namespace gs {

namespace gather_scatter {

enum class EdgeDir {
  IN_EDGES,
  OUT_EDGES,
  NO_EDGES,
  BOTH_EDGES,
};

class Context {
 public:
  Context() : iteration_(0) {}

  int max_iterations() const { return max_iterations_; }

  size_t num_vertices() const { return num_vertices_; }

  int procid() const { return procid_; }

  int num_procs() const { return num_procs_; }

  int iteration() const { return iteration_; }

  void AddColumn(const std::string& name, const std::string& type) {
    columns_.emplace(name, type);
  }

  void set_max_iterations(int max_iterations) {
    max_iterations_ = max_iterations;
  }

  void set_num_vertices(size_t num_vertices) { num_vertices_ = num_vertices; }

  void set_procid(int procid) { procid_ = procid; }

  void set_num_procs(int num_procs) { num_procs_ = num_procs; }

  void set_iteration(int iteration) { iteration_ = iteration; }

  const std::map<std::string, std::string> columns() const { return columns_; }

 private:
  int max_iterations_;
  size_t num_vertices_;
  int procid_;
  int num_procs_;

  int iteration_;

  std::map<std::string, std::string> columns_;
};

template <typename ID_T, typename GATHER_T, typename MESSAGE_T>
class IVertexProgram {
  using context_t = Context;
  using vertex_t = Vertex<ID_T>;

 public:
  using id_type = ID_T;
  using gather_type = GATHER_T;
  using message_type = MESSAGE_T;

  virtual void Setup(context_t& context) {}

  virtual void Init(const context_t& context, vertex_t& vertex) {}

  virtual void PreProcess(const context_t& context, vertex_t& vertex) {
    vertex.SetActive(true);
  }

  virtual void PostProcess(const context_t& context, vertex_t& vertex) {}

  virtual EdgeDir ScatterEdges(const context_t& context,
                               const vertex_t& vertex) const {
    return EdgeDir::OUT_EDGES;
  }

  virtual message_type ScatterValueSupplier(const context_t& context,
                                            const vertex_t& vertex) const = 0;

  virtual void Aggregate(gather_type& x, const message_type& y) const {}

  virtual gather_type GatherInit() const = 0;

  virtual std::string GatherIndex() const = 0;

  virtual int MaxIterations() const { return std::numeric_limits<int>::max(); }
};

}  // namespace gather_scatter

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_I_VERTEX_PROGRAM_H_
