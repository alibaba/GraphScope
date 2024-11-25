#include "LinearResolutionParameterVertexPartition.h"

LinearResolutionParameterVertexPartition::LinearResolutionParameterVertexPartition(Graph* graph,
      vector<size_t> membership, double resolution_parameter) :
        ResolutionParameterVertexPartition(graph,
        membership, resolution_parameter)
{ }

LinearResolutionParameterVertexPartition::LinearResolutionParameterVertexPartition(Graph* graph,
      vector<size_t> membership) :
        ResolutionParameterVertexPartition(graph,
        membership)
{ }

LinearResolutionParameterVertexPartition::LinearResolutionParameterVertexPartition(Graph* graph,
  double resolution_parameter) :
        ResolutionParameterVertexPartition(graph, resolution_parameter)
{ }

LinearResolutionParameterVertexPartition::LinearResolutionParameterVertexPartition(Graph* graph) :
        ResolutionParameterVertexPartition(graph)
{ }

LinearResolutionParameterVertexPartition::~LinearResolutionParameterVertexPartition()
{ }
