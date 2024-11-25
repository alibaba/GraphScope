#ifndef SIGNIFICANCEVERTEXPARTITION_H
#define SIGNIFICANCEVERTEXPARTITION_H

#include "MutableVertexPartition.h"


class  SignificanceVertexPartition : public MutableVertexPartition
{
  public:
    SignificanceVertexPartition(Graph* graph, vector<size_t> const& membership);
    SignificanceVertexPartition(Graph* graph);
    virtual ~SignificanceVertexPartition();
    virtual SignificanceVertexPartition* create(Graph* graph);
    virtual SignificanceVertexPartition* create(Graph* graph, vector<size_t> const& membership);

    virtual double diff_move(size_t v, size_t new_comm);
    virtual double quality();
  protected:
  private:
};

#endif // SIGNIFICANCEVERTEXPARTITION_H
