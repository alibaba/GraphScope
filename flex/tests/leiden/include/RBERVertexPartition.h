#ifndef RBERVERTEXPARTITION_H
#define RBERVERTEXPARTITION_H

#include "LinearResolutionParameterVertexPartition.h"


class  RBERVertexPartition : public LinearResolutionParameterVertexPartition
{
  public:
    RBERVertexPartition(Graph* graph,
          vector<size_t> const& membership, double resolution_parameter);
    RBERVertexPartition(Graph* graph,
          vector<size_t> const& membership);
    RBERVertexPartition(Graph* graph,
      double resolution_parameter);
    RBERVertexPartition(Graph* graph);
    virtual ~RBERVertexPartition();
    virtual RBERVertexPartition* create(Graph* graph);
    virtual RBERVertexPartition* create(Graph* graph, vector<size_t> const& membership);

    virtual double diff_move(size_t v, size_t new_comm);
    virtual double quality(double resolution_parameter);

  protected:
  private:
};

#endif // RBERVERTEXPARTITION_H
