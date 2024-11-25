#ifndef LINEARRESOLUTIONPARAMETERVERTEXPARTITION_H
#define LINEARRESOLUTIONPARAMETERVERTEXPARTITION_H

#include "ResolutionParameterVertexPartition.h"

class  LinearResolutionParameterVertexPartition : public ResolutionParameterVertexPartition
{
  public:
    LinearResolutionParameterVertexPartition(Graph* graph,
          vector<size_t> membership, double resolution_parameter);
    LinearResolutionParameterVertexPartition(Graph* graph,
          vector<size_t> membership);
    LinearResolutionParameterVertexPartition(Graph* graph, double resolution_parameter);
    LinearResolutionParameterVertexPartition(Graph* graph);
    virtual ~LinearResolutionParameterVertexPartition();

  private:

};

#endif // RESOLUTIONPARAMETERVERTEXPARTITION_H
