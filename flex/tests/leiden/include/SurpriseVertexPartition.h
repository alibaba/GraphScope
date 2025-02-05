#ifndef SURPRISEVERTEXPARTITION_H
#define SURPRISEVERTEXPARTITION_H

#include "MutableVertexPartition.h"
#include <iostream>
  using std::cerr;
  using std::endl;

class  SurpriseVertexPartition: public MutableVertexPartition
{
  public:
    SurpriseVertexPartition(Graph* graph, vector<size_t> const& membership);
    SurpriseVertexPartition(Graph* graph, SurpriseVertexPartition* partition);
    SurpriseVertexPartition(Graph* graph);
    virtual ~SurpriseVertexPartition();
    virtual SurpriseVertexPartition* create(Graph* graph);
    virtual SurpriseVertexPartition* create(Graph* graph, vector<size_t> const& membership);

    virtual double diff_move(size_t v, size_t new_comm);
    virtual double quality();
  protected:
  private:
};

#endif // SURPRISEVERTEXPARTITION_H
