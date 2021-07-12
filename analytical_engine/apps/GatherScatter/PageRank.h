#ifndef ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_PAGERANK_H_
#define ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_PAGERANK_H_

#include <cmath>
#include <cstdint>
#include <numeric>

#include "apps/GatherScatter/IVertexProgram.h"

namespace gs {

namespace gather_scatter {

class PageRank : public IVertexProgram<int64_t, double, double> {
  using context_t = Context;
  using vertex_t = Vertex<int64_t>;

 public:
  void Setup(context_t& context) {
    context.AddColumn("$pr", "double");
    context.AddColumn("$tmp", "double");
    context.AddColumn("$new", "double");
  }

  void Init(const context_t& context, vertex_t& vertex) {
    vertex.SetActive(true);
    vertex.SetData<double>("$pr", 1.0 / context.num_vertices());
  }

  void PreProcess(const context_t& context, vertex_t& vertex) {
    vertex.SetActive(true);
    vertex.SetData<double>("$tmp",
                           vertex.GetData<double>("$pr") / vertex.OutDegree());
  }

  void PostProcess(const context_t& context, vertex_t& vertex) {
    vertex.SetData<double>("$new", 0.15 / context.num_vertices() +
                                       0.85 * vertex.GetData<double>("$tmp"));

    if (std::abs(vertex.GetData<double>("$new") -
                 vertex.GetData<double>("$pr")) > 1e-10) {
      vertex.SetActive(true);
      vertex.SetData<double>("$pr", vertex.GetData<double>("$new"));
    } else {
      vertex.SetActive(false);
    }
  }

  double ScatterValueSupplier(const context_t& context,
                              const vertex_t& vertex) const {
    return vertex.GetData<double>("$tmp");
  }

  EdgeDir ScatterEdges(const context_t& context, const vertex_t& vertex) const {
    return EdgeDir::OUT_EDGES;
  }

  void Aggregate(double& a, const double& b) const { a = a + b; }

  double GatherInit() const { return 0.0; }

  std::string GatherIndex() const { return "$tmp"; }

  int MaxIterations() const { return std::numeric_limits<int>::max(); }
};

}  // namespace gather_scatter

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_GATHER_SCATTER_PAGERANK_H_
