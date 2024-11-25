#ifndef MUTABLEVERTEXPARTITION_H
#define MUTABLEVERTEXPARTITION_H

#include <string>
#include "GraphHelper.h"
#include <map>
#include <queue>
#include <utility>
#include <algorithm>

using std::string;
using std::map;
using std::make_pair;
using std::pair;
using std::sort;
using std::reverse;
using std::priority_queue;

/****************************************************************************
Contains a partition of graph.

This class contains the basic implementation for optimising a partition.
Specifically, it implements all the administration necessary to keep track of
the partition from various points of view. Internally, it keeps track of the
number of internal edges (or total weight), the size of the communities, the
total incoming degree (or weight) for a community, etc... When deriving from
this class, one can easily use this administration to provide their own
implementation.

In order to keep the administration up-to-date, all changes in partition
should be done through move_node. This function moves a node from one
community to another, and updates all the administration.

It is possible to manually update the membership vector, and then call
__init_admin() which completely refreshes all the administration. This is
only possible by updating the membership vector, not by changing some of the
other variables.

The basic idea is that diff_move computes the difference in the quality
function if we call move_node for the same move. Using this framework, the
Leiden method in the optimisation class can call these general functions in
order to optimise the quality function.
*****************************************************************************/

class  MutableVertexPartition
{
  public:
    MutableVertexPartition(Graph* graph,
        vector<size_t> const& membership);
    MutableVertexPartition(Graph* graph);
    virtual MutableVertexPartition* create(Graph* graph);
    virtual MutableVertexPartition* create(Graph* graph, vector<size_t> const& membership);

    virtual ~MutableVertexPartition();

    inline size_t membership(size_t v) { return this->_membership[v]; };
    inline vector<size_t> const& membership() const { return this->_membership; };

    double csize(size_t comm);
    size_t cnodes(size_t comm);
    vector<size_t> get_community(size_t comm);
    vector< vector<size_t> > get_communities();
    size_t n_communities();

    void move_node(size_t v,size_t new_comm);
    virtual double diff_move(size_t v, size_t new_comm)
    {
      throw Exception("Function not implemented. This should be implemented in a derived class, since the base class does not implement a specific method.");
    };
    virtual double quality()
    {
      throw Exception("Function not implemented. This should be implemented in a derived class, since the base class does not implement a specific method.");
    };

    inline Graph* get_graph() { return this->graph; };

    void renumber_communities();
    void renumber_communities(vector<size_t> const& fixed_nodes, vector<size_t> const& fixed_membership);
    void renumber_communities(vector<size_t> const& new_membership);
    void set_membership(vector<size_t> const& new_membership);
    void relabel_communities(vector<size_t> const& new_comm_id);
    vector<size_t> static rank_order_communities(vector<MutableVertexPartition*> partitions);
    size_t get_empty_community();
    size_t add_empty_community();
    void from_coarse_partition(vector<size_t> const& coarse_partition_membership);
    void from_coarse_partition(MutableVertexPartition* partition);
    void from_coarse_partition(MutableVertexPartition* partition, vector<size_t> const& coarser_membership);
    void from_coarse_partition(vector<size_t> const& coarse_partition_membership, vector<size_t> const& coarse_node);

    void from_partition(MutableVertexPartition* partition);

    inline double total_weight_in_comm(size_t comm)   { return comm < _n_communities ? this->_total_weight_in_comm[comm] : 0.0; };
    inline double total_weight_from_comm(size_t comm) { return comm < _n_communities ? this->_total_weight_from_comm[comm] : 0.0; };
    inline double total_weight_to_comm(size_t comm)   { return comm < _n_communities ? this->_total_weight_to_comm[comm] : 0.0; };

    inline double total_weight_in_all_comms()         { return this->_total_weight_in_all_comms; };
    inline size_t total_possible_edges_in_all_comms() { return this->_total_possible_edges_in_all_comms; };

    inline double weight_to_comm(size_t v, size_t comm)
    {
      if (this->_current_node_cache_community_to != v)
      {
        this->cache_neigh_communities(v, IGRAPH_OUT);
        this->_current_node_cache_community_to = v;
      }

      if (comm < this->_cached_weight_to_community.size())
        return this->_cached_weight_to_community[comm];
      else
        return 0.0;
    }

    inline double weight_from_comm(size_t v, size_t comm)
    {
      if (!this->graph->is_directed())
        return weight_to_comm(v, comm);

      if (this->_current_node_cache_community_from != v)
      {
        this->cache_neigh_communities(v, IGRAPH_IN);
        this->_current_node_cache_community_from = v;
      }

      if (comm < this->_cached_weight_from_community.size())
        return this->_cached_weight_from_community[comm];
      else
        return 0.0;
    }

    vector<size_t> const& get_neigh_comms(size_t v, igraph_neimode_t);
    vector<size_t> get_neigh_comms(size_t v, igraph_neimode_t mode, vector<size_t> const& constrained_membership);

    // By delegating the responsibility for deleting the graph to the partition,
    // we no longer have to worry about deleting this graph.
    int destructor_delete_graph;

  protected:

    void init_admin();

    vector<size_t> _membership; // Membership vector, i.e. \sigma_i = c means that node i is in community c

    Graph* graph;

    // Community size
    vector<double> _csize;

    // Number of nodes in community
    vector< size_t > _cnodes;

    double weight_vertex_tofrom_comm(size_t v, size_t comm, igraph_neimode_t mode);

    void set_default_attrs();

  private:

    // Keep track of the internal weight of each community
    vector<double> _total_weight_in_comm;
    // Keep track of the total weight to a community
    vector<double> _total_weight_to_comm;
    // Keep track of the total weight from a community
    vector<double> _total_weight_from_comm;
    // Keep track of the total internal weight
    double _total_weight_in_all_comms;
    size_t _total_possible_edges_in_all_comms;
    size_t _n_communities;

    vector<size_t> _empty_communities;

    void cache_neigh_communities(size_t v, igraph_neimode_t mode);

    size_t _current_node_cache_community_from; vector<double> _cached_weight_from_community; vector<size_t> _cached_neigh_comms_from;
    size_t _current_node_cache_community_to;   vector<double> _cached_weight_to_community;   vector<size_t> _cached_neigh_comms_to;
    size_t _current_node_cache_community_all;  vector<double> _cached_weight_all_community;  vector<size_t> _cached_neigh_comms_all;

    void clean_mem();
    void init_graph_admin();

    void update_n_communities();

};

#endif // MUTABLEVERTEXPARTITION_H
